package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	headerSize     = 9
	recordSet      = byte(1) // V1 set record
	recordDelete   = byte(2) // V1 delete record
	recordSetV2    = byte(3) // V2 set record with checksum
	recordDeleteV2 = byte(4) // V2 delete record with checksum
	checksumSize   = 4
	maxFieldSize   = 64 << 20
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

	s := &Store{
		file:  f,
		index: make(map[string]entry),
	}

	if err := s.recover(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("recover database: %w", err)
	}

	return s, nil
}

// recover replays database records into the in-memory index.
// V1 records have no checksum; V2 records have a CRC32 checksum.
func (s *Store) recover() error {
	info, err := s.file.Stat()
	if err != nil {
		return err
	}

	size := info.Size()
	var offset int64
	header := make([]byte, headerSize)

	for offset < size {
		remaining := size - offset

		// An incomplete header at EOF is a torn final write.
		if remaining < headerSize {
			if err := s.file.Truncate(offset); err != nil {
				return fmt.Errorf("truncate incomplete header: %w", err)
			}
			break
		}

		if _, err := s.file.ReadAt(header, offset); err != nil {
			return fmt.Errorf("read header at offset %d: %w", offset, err)
		}

		kind := header[0]
		keySize := binary.LittleEndian.Uint32(header[1:5])
		valueSize := binary.LittleEndian.Uint32(header[5:9])

		if keySize > maxFieldSize || valueSize > maxFieldSize {
			return fmt.Errorf("record too large at offset %d", offset)
		}

		isV2 := kind == recordSetV2 || kind == recordDeleteV2
		isSet := kind == recordSet || kind == recordSetV2
		isDelete := kind == recordDelete || kind == recordDeleteV2

		if !isSet && !isDelete {
			return fmt.Errorf("unknown record type %d at offset %d", kind, offset)
		}

		if isDelete && valueSize != 0 {
			return fmt.Errorf("invalid delete record at offset %d", offset)
		}

		recordSize := int64(headerSize) + int64(keySize) + int64(valueSize)
		if isV2 {
			recordSize += checksumSize
		}

		// An incomplete final record is discarded during recovery.
		if recordSize > remaining {
			if err := s.file.Truncate(offset); err != nil {
				return fmt.Errorf("truncate incomplete record: %w", err)
			}
			break
		}

		payloadSize := int64(keySize) + int64(valueSize)
		payload := make([]byte, int(payloadSize))

		if len(payload) > 0 {
			if _, err := s.file.ReadAt(payload, offset+headerSize); err != nil {
				return fmt.Errorf("read payload at offset %d: %w", offset, err)
			}
		}

		if isV2 {
			checksumOffset := offset + int64(headerSize) + payloadSize
			checksumBytes := make([]byte, checksumSize)

			if _, err := s.file.ReadAt(checksumBytes, checksumOffset); err != nil {
				return fmt.Errorf("read checksum at offset %d: %w", offset, err)
			}

			recordWithoutChecksum := make(
				[]byte,
				0,
				headerSize+len(payload),
			)
			recordWithoutChecksum = append(recordWithoutChecksum, header...)
			recordWithoutChecksum = append(recordWithoutChecksum, payload...)

			want := binary.LittleEndian.Uint32(checksumBytes)
			got := crc32.ChecksumIEEE(recordWithoutChecksum)

			if got != want {
				return fmt.Errorf(
					"checksum mismatch at offset %d: got %08x, want %08x",
					offset,
					got,
					want,
				)
			}
		}

		key := string(payload[:int(keySize)])
		valueOffset := offset + int64(headerSize) + int64(keySize)

		if isDelete {
			delete(s.index, key)
		} else {
			s.index[key] = entry{
				valueOffset: valueOffset,
				valueSize:   valueSize,
			}
		}

		offset += recordSize
	}

	_, err = s.file.Seek(0, io.SeekEnd)
	return err
}

// appendRecord writes a checksummed V2 record and syncs it before
// updating the in-memory index.
func (s *Store) appendRecord(kind byte, key, value string) error {
	if uint64(len(key)) > maxFieldSize || uint64(len(value)) > maxFieldSize {
		return errors.New("key or value exceeds 64 MiB")
	}

	if kind != recordSet && kind != recordDelete {
		return fmt.Errorf("invalid record type %d", kind)
	}

	if kind == recordDelete && value != "" {
		return errors.New("delete record cannot contain a value")
	}

	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	v2Kind := recordSetV2
	if kind == recordDelete {
		v2Kind = recordDeleteV2
	}

	header := make([]byte, headerSize)
	header[0] = v2Kind
	binary.LittleEndian.PutUint32(header[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(value)))

	record := make([]byte, 0, headerSize+len(key)+len(value)+checksumSize)
	record = append(record, header...)
	record = append(record, key...)
	record = append(record, value...)

	checksum := crc32.ChecksumIEEE(record)
	checksumBytes := make([]byte, checksumSize)
	binary.LittleEndian.PutUint32(checksumBytes, checksum)
	record = append(record, checksumBytes...)

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

// Compact rewrites live values into a temporary database file.
// New compacted records use the checksummed V2 format.
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
				return fmt.Errorf("read value for key %q: %w", key, err)
			}
		}

		header := make([]byte, headerSize)
		header[0] = recordSetV2
		binary.LittleEndian.PutUint32(header[1:5], uint32(len(key)))
		binary.LittleEndian.PutUint32(header[5:9], uint32(len(value)))

		record := make(
			[]byte,
			0,
			headerSize+len(key)+len(value)+checksumSize,
		)
		record = append(record, header...)
		record = append(record, key...)
		record = append(record, value...)

		checksum := crc32.ChecksumIEEE(record)
		checksumBytes := make([]byte, checksumSize)
		binary.LittleEndian.PutUint32(checksumBytes, checksum)
		record = append(record, checksumBytes...)

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
		return fmt.Errorf("close old database before compaction: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		reopened, openErr := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0600)
		if openErr != nil {
			s.closed = true
			return fmt.Errorf(
				"replace failed: %v; reopen failed: %w",
				err,
				openErr,
			)
		}

		s.file = reopened
		return fmt.Errorf("replace compacted database: %w", err)
	}

	replaced = true

	newFile, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		s.closed = true
		return fmt.Errorf("reopen compacted database: %w", err)
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
