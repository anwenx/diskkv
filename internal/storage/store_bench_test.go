package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	path := filepath.Join(b.TempDir(), "set.db")
	s, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := s.Set(key, "benchmark-value"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	path := filepath.Join(b.TempDir(), "get.db")
	s, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	if err := s.Set("benchmark-key", "benchmark-value"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Get("benchmark-key"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompact(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "compact.db")

	// Build a database with 100 keys and some overwritten records.
	s, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}

	for j := 0; j < 100; j++ {
		key := fmt.Sprintf("key-%d", j)

		if err := s.Set(key, "benchmark-value"); err != nil {
			_ = s.Close()
			b.Fatal(err)
		}

		if j%2 == 0 {
			if err := s.Set(key, "updated-value"); err != nil {
				_ = s.Close()
				b.Fatal(err)
			}
		}
	}

	if err := s.Close(); err != nil {
		b.Fatal(err)
	}

	// Preserve the original database so each iteration starts
	// with the same un-compacted data.
	original, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Restore the original database before each iteration.
		if err := os.WriteFile(path, original, 0600); err != nil {
			b.Fatal(err)
		}

		s, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()
		err = s.Compact()
		b.StopTimer()

		closeErr := s.Close()

		if err != nil {
			b.Fatal(err)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
	}
}
