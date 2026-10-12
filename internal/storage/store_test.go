package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPersistenceAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range [][2]string{{"name", "jojo"}, {"empty", ""}, {"name", "latest"}} {
		if err := s.Set(item[0], item[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Delete("name"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.Get("name"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key error = %v", err)
	}
	if got, err := s.Get("empty"); err != nil || got != "" {
		t.Fatalf("empty value: %q, %v", got, err)
	}
	if err := s.Set("name", "again"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get("name"); err != nil || got != "again" {
		t.Fatalf("reinserted key: %q, %v", got, err)
	}
}

func TestMissingKey(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get error = %v", err)
	}
	if err := s.Delete("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete error = %v", err)
	}
}

func TestCompact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Set("keep", "old"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("keep", "latest"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("remove", "temporary"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("remove"); err != nil {
		t.Fatal(err)
	}

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("compaction did not shrink file: before=%d after=%d", before.Size(), after.Size())
	}

	if got, err := s.Get("keep"); err != nil || got != "latest" {
		t.Fatalf("after compact: %q, %v", got, err)
	}
	if _, err := s.Get("remove"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key after compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if got, err := s.Get("keep"); err != nil || got != "latest" {
		t.Fatalf("after reopen: %q, %v", got, err)
	}
	if _, err := s.Get("remove"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key after reopen: %v", err)
	}
}

func TestRecoverTruncatedFinalHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Set("safe", "value"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.Write([]byte{1, 2, 3})
	closeErr := f.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatalf("recover truncated header: %v", err)
	}
	defer s.Close()

	if got, err := s.Get("safe"); err != nil || got != "value" {
		t.Fatalf("recovered value = %q, %v", got, err)
	}
}

func TestRecoverTruncatedFinalPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Set("safe", "value"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a set record claiming a 5-byte key and 5-byte value,
	// but write only part of its payload.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	torn := []byte{1, 5, 0, 0, 0, 5, 0, 0, 0, 'b', 'r'}
	_, writeErr := f.Write(torn)
	closeErr := f.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatalf("recover truncated payload: %v", err)
	}
	defer s.Close()

	if got, err := s.Get("safe"); err != nil || got != "value" {
		t.Fatalf("recovered value = %q, %v", got, err)
	}
	if _, err := s.Get("br"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial record must not be indexed: %v", err)
	}
}
func TestOpenDetectsChecksumCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Set("hello", "world"); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte in the stored key.
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	// The V2 header is 9 bytes, so byte 9 is the first key byte.
	if _, err := f.WriteAt([]byte{'j'}, 9); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("expected checksum corruption to be detected")
	}

	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got: %v", err)
	}
}

func TestCompactEmptyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact empty database: %v", err)
	}

	if got, err := s.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing key after compact: got %q, err %v", got, err)
	}
}

func TestCompactPreservesEmptyValuesAndRepeatedCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Set("empty", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("value", "first"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("value", "latest"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact #%d: %v", i+1, err)
		}
	}

	if got, err := s.Get("empty"); err != nil || got != "" {
		t.Fatalf("empty value after compaction: got %q, err %v", got, err)
	}
	if got, err := s.Get("value"); err != nil || got != "latest" {
		t.Fatalf("latest value after compaction: got %q, err %v", got, err)
	}
}

func TestCompactAfterClose(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := s.Compact(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Compact after Close: got %v, want %v", err, ErrClosed)
	}
}
