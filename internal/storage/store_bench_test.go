package storage

import (
	"fmt"
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
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		path := filepath.Join(b.TempDir(), fmt.Sprintf("compact-%d.db", i))
		s, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}

		for j := 0; j < 100; j++ {
			key := fmt.Sprintf("key-%d", j)
			if err := s.Set(key, "benchmark-value"); err != nil {
				s.Close()
				b.Fatal(err)
			}
			if j%2 == 0 {
				if err := s.Set(key, "updated-value"); err != nil {
					s.Close()
					b.Fatal(err)
				}
			}
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
