package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	headerSize   = 9
	recordSet    = byte(1)
	recordDelete = byte(2)
	maxFieldSize = 64 << 20
)

var (
	ErrNotFound = errors.New("key not found")
	ErrClosed   = errors.New("store is closed")
)

type entry struct {
	valueOffset int64
	valueSize   uint32
}

type Store struct {
	mu     sync.RWMutex
	file   *os.File
	index  map[string]entry
	closed bool
}

func Open(path string) (*Store, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	s := &Store{file: f, index: make(map[string]entry)}
	if err := s.recover(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("recover database: %w", err)
	}
	return s, nil
}

func (s *Store) recover() error {
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	var offset int64
	header := make([]byte, headerSize)

	for offset < size {
		if size-offset < headerSize {
			return fmt.Errorf("incomplete record header at offset %d", offset)
		}
		if _, err := s.file.ReadAt(header, offset); err != nil {
			return err
		}

		kind := header[0]
		keySize := binary.LittleEndian.Uint32(header[1:5])
		valueSize := binary.LittleEndian.Uint32(header[5:9])

		if keySize > maxFieldSize || valueSize > maxFieldSize {
			return fmt.Errorf("record too large at offset %d", offset)
		}
		if kind != recordSet && kind != recordDelete {
			return fmt.Errorf("unknown record type %d at offset %d", kind, offset)
		}
		if kind == recordDelete && valueSize != 0 {
			return fmt.Errorf("invalid delete record at offset %d", offset)
		}

		recordSize := int64(headerSize) + int64(keySize) + int64(valueSize)
		if recordSize > size-offset {
			return fmt.Errorf("incomplete record at offset %d", offset)
		}

		keyBytes := make([]byte, int(keySize))
		if _, err := s.file.ReadAt(keyBytes, offset+headerSize); err != nil {
			return err
		}
		key := string(keyBytes)
		valueOffset := offset + headerSize + int64(keySize)

		if kind == recordDelete {
			delete(s.index, key)
		} else {
			s.index[key] = entry{valueOffset: valueOffset, valueSize: valueSize}
		}
		offset += recordSize
	}
	_, err = s.file.Seek(0, io.SeekEnd)
	return err
}

func (s *Store) appendRecord(kind byte, key, value string) error {
	if uint64(len(key)) > maxFieldSize || uint64(len(value)) > maxFieldSize {
		return errors.New("key or value exceeds 64 MiB")
	}
	if kind == recordDelete && value != "" {
		return errors.New("delete record cannot contain a value")
	}

	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	header := make([]byte, headerSize)
	header[0] = kind
	binary.LittleEndian.PutUint32(header[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(value)))

	record := make([]byte, 0, headerSize+len(key)+len(value))
	record = append(record, header...)
	record = append(record, key...)
	record = append(record, value...)

	n, err := s.file.Write(record)
	if err != nil {
		return err
	}
	if n != len(record) {
		return io.ErrShortWrite
	}
	if err := s.file.Sync(); err != nil {
		return err
	}

	if kind == recordDelete {
		delete(s.index, key)
	} else {
		s.index[key] = entry{
			valueOffset: offset + headerSize + int64(len(key)),
			valueSize:   uint32(len(value)),
		}
	}
	return nil
}

func (s *Store) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.appendRecord(recordSet, key, value)
}

func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return "", ErrClosed
	}

	e, ok := s.index[key]
	if !ok {
		return "", ErrNotFound
	}
	value := make([]byte, int(e.valueSize))
	if len(value) > 0 {
		if _, err := s.file.ReadAt(value, e.valueOffset); err != nil {
			return "", err
		}
	}
	return string(value), nil
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if _, ok := s.index[key]; !ok {
		return ErrNotFound
	}
	return s.appendRecord(recordDelete, key, "")
}

// Compact rewrites only the latest live values into a temporary file.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}

	oldFile := s.file
	path := oldFile.Name()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".diskkv-compact-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	replaced := false
	defer func() {
		if tmp != nil {
			_ = tmp.Close()
		}
		if !replaced {
			_ = os.Remove(tmpPath)
		}
	}()

	keys := make([]string, 0, len(s.index))
	for key := range s.index {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	newIndex := make(map[string]entry, len(s.index))
	var offset int64
	for _, key := range keys {
		e := s.index[key]
		value := make([]byte, int(e.valueSize))
		if len(value) > 0 {
			if _, err := oldFile.ReadAt(value, e.valueOffset); err != nil {
				return err
			}
		}

		header := make([]byte, headerSize)
		header[0] = recordSet
		binary.LittleEndian.PutUint32(header[1:5], uint32(len(key)))
		binary.LittleEndian.PutUint32(header[5:9], uint32(len(value)))

		record := make([]byte, 0, headerSize+len(key)+len(value))
		record = append(record, header...)
		record = append(record, key...)
		record = append(record, value...)

		n, err := tmp.Write(record)
		if err != nil {
			return err
		}
		if n != len(record) {
			return io.ErrShortWrite
		}
		newIndex[key] = entry{
			valueOffset: offset + headerSize + int64(len(key)),
			valueSize:   uint32(len(value)),
		}
		offset += int64(len(record))
	}

	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		tmp = nil
		return err
	}
	tmp = nil

	if err := oldFile.Close(); err != nil {
		s.closed = true
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		reopened, openErr := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0600)
		if openErr != nil {
			s.closed = true
			return fmt.Errorf("replace failed: %v; reopen failed: %w", err, openErr)
		}
		s.file = reopened
		return err
	}
	replaced = true

	newFile, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		s.closed = true
		return err
	}
	s.file = newFile
	s.index = newIndex
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}
