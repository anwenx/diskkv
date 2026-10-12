// Command basic demonstrates the diskkv library API.
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/anwenx/diskkv/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "diskkv-example-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	s, err := storage.OpenWithOptions(filepath.Join(dir, "example.db"), storage.Options{
		SyncPolicy: storage.SyncNever, // faster; Close still syncs
	})
	if err != nil {
		return err
	}
	defer s.Close()

	for k, v := range map[string]string{"user:1": "alice", "user:2": "bob", "order:1": "book"} {
		if err := s.Set(k, v); err != nil {
			return err
		}
	}
	if err := s.Set("user:1", "alice-v2"); err != nil { // creates a stale record
		return err
	}

	kvs, err := s.ScanPrefix("user:")
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		fmt.Printf("%s = %s\n", kv.Key, kv.Value)
	}

	before, err := s.Stats()
	if err != nil {
		return err
	}
	if err := s.Compact(); err != nil {
		return err
	}
	after, err := s.Stats()
	if err != nil {
		return err
	}
	fmt.Printf("file size: %d -> %d bytes (stale %d -> %d)\n",
		before.FileSize, after.FileSize, before.StaleBytes, after.StaleBytes)
	return nil
}
