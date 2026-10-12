// Package storage implements diskkv, a small Bitcask-style key-value store:
// an append-only log on disk plus an in-memory index of live keys.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	headerSize     = 9
	recordSet      = byte(1) // V1 set record
	recordDelete   = byte(2) // V1 delete record
	recordSetV2    = byte(3) // V2 set record with checksum
	recordDeleteV2 = byte(4) // V2 delete record with checksum
	checksumSize   = 4
	maxFieldSize   = 64 << 20

	defaultCompactMinBytes   = 1 << 20
	defaultCompactStaleRatio = 0.5
)

var (
	ErrNotFound       = errors.New("key not found")
	ErrClosed         = errors.New("store is closed")
	ErrKeyTooLarge    = errors.New("key too large")
	ErrValueTooLarge  = errors.New("value too large")
	ErrInvalidOptions = errors.New("invalid options")
)

// SyncPolicy controls when appended records are flushed with fsync.
type SyncPolicy int

const (
	// SyncAlways fsyncs after every Set and Delete (default).
	SyncAlways SyncPolicy = iota
	// SyncNever leaves flushing to the operating system. Data is still
	// synced on Sync and Close. A crash may lose recent writes.
	SyncNever
)

// Options configures a Store. The zero value selects every default.
type Options struct {
	// MaxKeySize and MaxValueSize bound writes, in bytes. Zero means the
	// hard limit of 64 MiB. Limits apply to new writes only; existing files
	// are always readable up to the hard limit.
	MaxKeySize   int
	MaxValueSize int
	// SyncPolicy defaults to SyncAlways.
	SyncPolicy SyncPolicy
	// CompactMinBytes: CompactionRecommended is false while the file is
	// smaller than this. Zero means 1 MiB.
	CompactMinBytes int64
	// CompactStaleRatio in (0,1]: recommend compaction when stale bytes make
	// up at least this fraction of the file. Zero means 0.5.
	CompactStaleRatio float64
}

// DefaultOptions returns the options used by Open.
func DefaultOptions() Options {
	return Options{
		MaxKeySize:        maxFieldSize,
		MaxValueSize:      maxFieldSize,
		SyncPolicy:        SyncAlways,
		CompactMinBytes:   defaultCompactMinBytes,
		CompactStaleRatio: defaultCompactStaleRatio,
	}
}

func resolveOptions(o Options) (Options, error) {
	if o.MaxKeySize < 0 || o.MaxKeySize > maxFieldSize {
		return o, fmt.Errorf("%w: MaxKeySize %d out of range [0, %d]", ErrInvalidOptions, o.MaxKeySize, maxFieldSize)
	}
	if o.MaxValueSize < 0 || o.MaxValueSize > maxFieldSize {
		return o, fmt.Errorf("%w: MaxValueSize %d out of range [0, %d]", ErrInvalidOptions, o.MaxValueSize, maxFieldSize)
	}
	if o.SyncPolicy != SyncAlways && o.SyncPolicy != SyncNever {
		return o, fmt.Errorf("%w: unknown SyncPolicy %d", ErrInvalidOptions, o.SyncPolicy)
	}
	if o.CompactMinBytes < 0 {
		return o, fmt.Errorf("%w: CompactMinBytes %d is negative", ErrInvalidOptions, o.CompactMinBytes)
	}
	if math.IsNaN(o.CompactStaleRatio) || o.CompactStaleRatio < 0 || o.CompactStaleRatio > 1 {
		return o, fmt.Errorf("%w: CompactStaleRatio %v out of range [0, 1]", ErrInvalidOptions, o.CompactStaleRatio)
	}
	if o.MaxKeySize == 0 {
		o.MaxKeySize = maxFieldSize
	}
	if o.MaxValueSize == 0 {
		o.MaxValueSize = maxFieldSize
	}
	if o.CompactMinBytes == 0 {
		o.CompactMinBytes = defaultCompactMinBytes
	}
	if o.CompactStaleRatio == 0 {
		o.CompactStaleRatio = defaultCompactStaleRatio
	}
	return o, nil
}

// KeyValue is one result of ScanPrefix.
type KeyValue struct {
	Key   string
	Value string
}

// Stats is a snapshot of store metrics. Operation counters count successful
// operations since this Store was opened; they are not persisted. Size fields
// are tracked incrementally, so Stats never scans the file.
type Stats struct {
	LiveKeys     int    // keys currently readable
	FileSize     int64  // bytes in the database file
	LiveBytes    int64  // bytes of the newest record of each live key
	StaleBytes   int64  // FileSize - LiveBytes: overwritten records and tombstones
	Sets         uint64 // successful Set calls
	Gets         uint64 // Get calls that found the key
	Deletes      uint64 // successful Delete calls
	BytesWritten uint64 // bytes appended by Set/Delete (excludes compaction)
	Compactions  uint64 // successful Compact calls since Open

	LastCompaction          time.Time // zero if none since Open
	LastCompactionReclaimed int64     // bytes freed by the last compaction

	CompactionRecommended bool
}

type entry struct {
	valueOffset int64
	valueSize   uint32
	recordSize  int64
}

type Store struct {
	mu        sync.RWMutex
	file      *os.File
	opts      Options
	index     map[string]entry
	keys      []string // sorted copy of index keys, for prefix scans
	size      int64    // logical end of log; next record is written here
	liveBytes int64
	closed    bool

	sets         atomic.Uint64
	gets         atomic.Uint64
	deletes      atomic.Uint64
	bytesWritten atomic.Uint64

	compactions   uint64
	lastCompacted time.Time
	lastReclaimed int64
}

// Open opens or creates a database with DefaultOptions.
func Open(path string) (*Store, error) {
	return OpenWithOptions(path, Options{})
}

// OpenWithOptions opens or creates a database with the given options.
func OpenWithOptions(path string, opts Options) (*Store, error) {
	resolved, err := resolveOptions(opts)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	s := &Store{
		file:  f,
		opts:  resolved,
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
	var offset, live int64
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

			recordWithoutChecksum := make([]byte, 0, headerSize+len(payload))
			recordWithoutChecksum = append(recordWithoutChecksum, header...)
			recordWithoutChecksum = append(recordWithoutChecksum, payload...)

			want := binary.LittleEndian.Uint32(checksumBytes)
			got := crc32.ChecksumIEEE(recordWithoutChecksum)

			if got != want {
				return fmt.Errorf(
					"checksum mismatch at offset %d: got %08x, want %08x",
					offset, got, want,
				)
			}
		}

		key := string(payload[:int(keySize)])
		old, existed := s.index[key]

		if isDelete {
			if existed {
				live -= old.recordSize
				delete(s.index, key)
			}
		} else {
			if existed {
				live -= old.recordSize
			}
			s.index[key] = entry{
				valueOffset: offset + int64(headerSize) + int64(keySize),
				valueSize:   valueSize,
				recordSize:  recordSize,
			}
			live += recordSize
		}

		offset += recordSize
	}

	s.size = offset
	s.liveBytes = live
	s.keys = make([]string, 0, len(s.index))
	for key := range s.index {
		s.keys = append(s.keys, key)
	}
	sort.Strings(s.keys)

	return nil
}

func (s *Store) insertKey(key string) {
	i := sort.SearchStrings(s.keys, key)
	s.keys = append(s.keys, "")
	copy(s.keys[i+1:], s.keys[i:])
	s.keys[i] = key
}

func (s *Store) removeKey(key string) {
	i := sort.SearchStrings(s.keys, key)
	if i < len(s.keys) && s.keys[i] == key {
		s.keys = append(s.keys[:i], s.keys[i+1:]...)
	}
}

// appendRecord writes a checksummed V2 record at the logical end of the log.
// The in-memory index is updated only after the write (and the sync, under
// SyncAlways) succeeded. If the write or sync fails, the log is truncated
// back to its previous end so no partial record is left behind.
// The caller must hold s.mu for writing.
func (s *Store) appendRecord(kind byte, key, value string) error {
	if kind != recordSet && kind != recordDelete {
		return fmt.Errorf("invalid record type %d", kind)
	}
	if kind == recordDelete && value != "" {
		return errors.New("delete record cannot contain a value")
	}

	v2Kind := recordSetV2
	if kind == recordDelete {
		v2Kind = recordDeleteV2
	}

	rec := encodeV2(v2Kind, key, value)
	offset := s.size

	n, err := s.file.WriteAt(rec, offset)
	if err == nil && n != len(rec) {
		err = errors.New("short write")
	}
	if err == nil && s.opts.SyncPolicy == SyncAlways {
		err = s.file.Sync()
	}
	if err != nil {
		if tErr := s.file.Truncate(offset); tErr != nil {
			// The log may contain a partial record. Refuse further use;
			// reopening runs recovery, which discards a torn tail.
			s.closed = true
			_ = s.file.Close()
			return fmt.Errorf("append failed: %w (rollback failed: %v; reopen the database)", err, tErr)
		}
		return fmt.Errorf("append failed: %w", err)
	}

	recSize := int64(len(rec))
	s.size += recSize
	s.bytesWritten.Add(uint64(recSize))

	if kind == recordDelete {
		if old, ok := s.index[key]; ok {
			s.liveBytes -= old.recordSize
			delete(s.index, key)
			s.removeKey(key)
		}
		s.deletes.Add(1)
		return nil
	}

	old, existed := s.index[key]
	if existed {
		s.liveBytes -= old.recordSize
	} else {
		s.insertKey(key)
	}
	s.index[key] = entry{
		valueOffset: offset + int64(headerSize) + int64(len(key)),
		valueSize:   uint32(len(value)),
		recordSize:  recSize,
	}
	s.liveBytes += recSize
	s.sets.Add(1)

	return nil
}

func (s *Store) Set(key, value string) error {
	if len(key) > s.opts.MaxKeySize {
		return fmt.Errorf("%w: %d bytes exceeds limit %d", ErrKeyTooLarge, len(key), s.opts.MaxKeySize)
	}
	if len(value) > s.opts.MaxValueSize {
		return fmt.Errorf("%w: %d bytes exceeds limit %d", ErrValueTooLarge, len(value), s.opts.MaxValueSize)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	return s.appendRecord(recordSet, key, value)
}

// readValue reads the value for e from the log. The caller must hold s.mu.
func (s *Store) readValue(e entry) (string, error) {
	value := make([]byte, int(e.valueSize))
	if len(value) > 0 {
		if _, err := s.file.ReadAt(value, e.valueOffset); err != nil {
			return "", err
		}
	}
	return string(value), nil
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

	value, err := s.readValue(e)
	if err != nil {
		return "", err
	}
	s.gets.Add(1)
	return value, nil
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

// ScanPrefix returns every live key that starts with prefix, with its value,
// in ascending byte order. An empty prefix returns all keys.
//
// Cost: O(log n) to locate the first match in the sorted key list, then
// O(m) for the m matches, each needing one disk read for its value. Memory
// is O(m) for the result. The sorted key list adds O(n) memory; inserting a
// brand-new key into it costs O(n) (a slice shift), overwrites cost nothing.
func (s *Store) ScanPrefix(prefix string) ([]KeyValue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrClosed
	}

	out := []KeyValue{}
	for i := sort.SearchStrings(s.keys, prefix); i < len(s.keys) && strings.HasPrefix(s.keys[i], prefix); i++ {
		key := s.keys[i]
		value, err := s.readValue(s.index[key])
		if err != nil {
			return nil, fmt.Errorf("read value for key %q: %w", key, err)
		}
		out = append(out, KeyValue{Key: key, Value: value})
	}
	return out, nil
}

// Stats returns a snapshot of store metrics in O(1).
func (s *Store) Stats() (Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return Stats{}, ErrClosed
	}

	return Stats{
		LiveKeys:                len(s.index),
		FileSize:                s.size,
		LiveBytes:               s.liveBytes,
		StaleBytes:              s.size - s.liveBytes,
		Sets:                    s.sets.Load(),
		Gets:                    s.gets.Load(),
		Deletes:                 s.deletes.Load(),
		BytesWritten:            s.bytesWritten.Load(),
		Compactions:             s.compactions,
		LastCompaction:          s.lastCompacted,
		LastCompactionReclaimed: s.lastReclaimed,
		CompactionRecommended:   s.compactionRecommended(),
	}, nil
}

// CompactionRecommended reports whether stale bytes exceed the configured
// threshold. It never compacts by itself; call Compact to act on it.
func (s *Store) CompactionRecommended() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return !s.closed && s.compactionRecommended()
}

func (s *Store) compactionRecommended() bool {
	if s.size == 0 || s.size < s.opts.CompactMinBytes {
		return false
	}
	stale := float64(s.size - s.liveBytes)
	return stale/float64(s.size) >= s.opts.CompactStaleRatio
}

// Sync flushes the database file to stable storage.
func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	return s.file.Sync()
}

func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync() // best effort: some platforms cannot fsync directories
	_ = d.Close()
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
	sizeBefore := s.size

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

	newIndex := make(map[string]entry, len(s.index))
	var offset int64

	// s.keys is already sorted, so compacted files are written in key order.
	for _, key := range s.keys {
		e := s.index[key]

		value, err := s.readValue(e)
		if err != nil {
			return fmt.Errorf("read value for key %q: %w", key, err)
		}

		record := encodeV2(recordSetV2, key, value)

		n, err := tmp.Write(record)
		if err != nil {
			return err
		}
		if n != len(record) {
			return errors.New("short write during compaction")
		}

		newIndex[key] = entry{
			valueOffset: offset + int64(headerSize) + int64(len(key)),
			valueSize:   e.valueSize,
			recordSize:  int64(len(record)),
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
		// The original file is untouched; reopen it and keep the old index.
		reopened, openErr := os.OpenFile(path, os.O_RDWR, 0600)
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
	syncDir(filepath.Dir(path))

	newFile, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		s.closed = true
		return fmt.Errorf("reopen compacted database: %w", err)
	}

	s.file = newFile
	s.index = newIndex
	s.size = offset
	s.liveBytes = offset
	s.compactions++
	s.lastCompacted = time.Now()
	s.lastReclaimed = sizeBefore - offset

	return nil
}

// Close syncs and closes the database. Closing twice is a no-op.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	syncErr := s.file.Sync()
	closeErr := s.file.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
