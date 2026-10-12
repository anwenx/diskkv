package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestPersistenceAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Set("name", "jojo"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("empty", ""); err != nil {
		t.Fatal(err)
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
		t.Fatalf("deleted key: got error %v, want ErrNotFound", err)
	}
	if got, err := s.Get("empty"); err != nil || got != "" {
		t.Fatalf("empty value: got %q, error %v", got, err)
	}
	if err := s.Set("name", "again"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get("name"); err != nil || got != "again" {
		t.Fatalf("reinserted key: got %q, error %v", got, err)
	}
}

func TestMissingKey(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got error %v, want ErrNotFound", err)
	}
	if err := s.Delete("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got error %v, want ErrNotFound", err)
	}
}
