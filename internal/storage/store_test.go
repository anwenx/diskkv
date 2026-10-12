package storage

import (
	"errors"
	"os"
	"path/filepath"
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
