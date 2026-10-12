package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func fxOpen(t *testing.T, path string, opts Options) *Store {
	t.Helper()
	s, err := OpenWithOptions(path, opts)
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	return s
}

func fxClose(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func fxMustGet(t *testing.T, s *Store, key, want string) {
	t.Helper()
	got, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if got != want {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func fxMustSet(t *testing.T, s *Store, key, value string) {
	t.Helper()
	if err := s.Set(key, value); err != nil {
		t.Fatalf("Set(%q): %v", key, err)
	}
}

func fxKeys(kvs []KeyValue) []string {
	keys := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, kv.Key)
	}
	return keys
}

func fxEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// fxV1Record builds a legacy V1 record (no checksum).
func fxV1Record(kind byte, key, value string) []byte {
	b := make([]byte, headerSize, headerSize+len(key)+len(value))
	b[0] = kind
	binary.LittleEndian.PutUint32(b[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(b[5:9], uint32(len(value)))
	b = append(b, key...)
	b = append(b, value...)
	return b
}

func TestOptionsValidation(t *testing.T) {
	cases := map[string]Options{
		"negative key limit":   {MaxKeySize: -1},
		"huge key limit":       {MaxKeySize: maxFieldSize + 1},
		"negative value limit": {MaxValueSize: -1},
		"huge value limit":     {MaxValueSize: maxFieldSize + 1},
		"unknown sync policy":  {SyncPolicy: SyncPolicy(99)},
		"negative min bytes":   {CompactMinBytes: -1},
		"ratio above one":      {CompactStaleRatio: 1.5},
		"negative ratio":       {CompactStaleRatio: -0.1},
	}

	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			_, err := OpenWithOptions(path, opts)
			if !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("err = %v, want ErrInvalidOptions", err)
			}
			if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
				t.Fatalf("database file was created for invalid options")
			}
		})
	}
}

func TestSizeLimitsAndBoundaryLengths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := fxOpen(t, path, Options{MaxKeySize: 4, MaxValueSize: 8, SyncPolicy: SyncNever})
	defer s.Close()

	fxMustSet(t, s, "abcd", "12345678") // exactly at both limits
	fxMustSet(t, s, "", "")             // empty key and empty value are valid

	if err := s.Set("abcde", "x"); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("oversized key: err = %v, want ErrKeyTooLarge", err)
	}
	if err := s.Set("k", "123456789"); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("oversized value: err = %v, want ErrValueTooLarge", err)
	}

	fxMustGet(t, s, "abcd", "12345678")
	fxMustGet(t, s, "", "")

	st, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.LiveKeys != 2 {
		t.Fatalf("LiveKeys = %d, want 2 (rejected writes must not change the index)", st.LiveKeys)
	}
}

func TestStatsCounters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := fxOpen(t, path, Options{})

	fxMustSet(t, s, "a", "1")
	fxMustSet(t, s, "b", "2")
	fxMustSet(t, s, "a", "3")
	fxMustGet(t, s, "a", "3")
	if _, err := s.Get("zz"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing: %v", err)
	}
	if err := s.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("zz"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete missing: %v", err)
	}

	// V2 record sizes: set a/1 = 9+1+1+4 = 15 bytes, tombstone for b = 9+1+0+4 = 14.
	st, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Sets != 3 || st.Gets != 1 || st.Deletes != 1 || st.LiveKeys != 1 {
		t.Fatalf("counters wrong: %+v", st)
	}
	if st.FileSize != 59 || st.LiveBytes != 15 || st.StaleBytes != 44 || st.BytesWritten != 59 {
		t.Fatalf("size accounting wrong: %+v", st)
	}

	// Recovery must rebuild the same size accounting; counters restart.
	fxClose(t, s)
	s = fxOpen(t, path, Options{})
	defer s.Close()

	st, err = s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.FileSize != 59 || st.LiveBytes != 15 || st.StaleBytes != 44 || st.LiveKeys != 1 {
		t.Fatalf("after reopen: %+v", st)
	}
	if st.Sets != 0 || st.Gets != 0 || st.Deletes != 0 {
		t.Fatalf("counters should restart on Open: %+v", st)
	}

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	st, err = s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.FileSize != 15 || st.StaleBytes != 0 || st.LiveBytes != 15 {
		t.Fatalf("after compact: %+v", st)
	}
	if st.Compactions != 1 || st.LastCompactionReclaimed != 44 || st.LastCompaction.IsZero() {
		t.Fatalf("compaction info wrong: %+v", st)
	}
	fxMustGet(t, s, "a", "3")
}

func TestStatsAfterClose(t *testing.T) {
	s := fxOpen(t, filepath.Join(t.TempDir(), "db"), Options{})
	fxClose(t, s)

	if _, err := s.Stats(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Stats after Close: %v", err)
	}
	if _, err := s.ScanPrefix(""); !errors.Is(err, ErrClosed) {
		t.Fatalf("ScanPrefix after Close: %v", err)
	}
	if err := s.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync after Close: %v", err)
	}
	if s.CompactionRecommended() {
		t.Fatal("CompactionRecommended after Close should be false")
	}
}

func TestCompactionRecommended(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := fxOpen(t, path, Options{CompactMinBytes: 1, CompactStaleRatio: 0.5, SyncPolicy: SyncNever})
	defer s.Close()

	fxMustSet(t, s, "k", "v")
	if s.CompactionRecommended() {
		t.Fatal("a fresh single record has no stale bytes")
	}
	for i := 0; i < 4; i++ {
		fxMustSet(t, s, "k", "v")
	}
	if !s.CompactionRecommended() {
		t.Fatal("80% stale bytes should recommend compaction")
	}
	st, _ := s.Stats()
	if !st.CompactionRecommended {
		t.Fatal("Stats.CompactionRecommended disagrees with CompactionRecommended()")
	}

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if s.CompactionRecommended() {
		t.Fatal("compaction should clear the recommendation")
	}
}

func TestCompactionRecommendedRespectsMinimumSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := fxOpen(t, path, Options{SyncPolicy: SyncNever}) // default 1 MiB minimum
	defer s.Close()

	for i := 0; i < 5; i++ {
		fxMustSet(t, s, "k", "v")
	}
	if s.CompactionRecommended() {
		t.Fatal("a tiny file must not trigger the default recommendation")
	}
}

func TestScanPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	opts := Options{SyncPolicy: SyncNever}
	s := fxOpen(t, path, opts)

	for _, k := range []string{"user:2", "order:1", "user:1", "users", "user:10"} {
		fxMustSet(t, s, k, "v-"+k)
	}

	got, err := s.ScanPrefix("user:")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"user:1", "user:10", "user:2"}; !fxEqual(fxKeys(got), want) {
		t.Fatalf("ScanPrefix(user:) = %v, want %v", fxKeys(got), want)
	}
	if got[0].Value != "v-user:1" {
		t.Fatalf("value = %q", got[0].Value)
	}

	// Deletes and overwrites are reflected.
	if err := s.Delete("user:10"); err != nil {
		t.Fatal(err)
	}
	fxMustSet(t, s, "user:2", "new")

	check := func(s *Store) {
		t.Helper()
		got, err := s.ScanPrefix("user:")
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"user:1", "user:2"}; !fxEqual(fxKeys(got), want) {
			t.Fatalf("keys = %v, want %v", fxKeys(got), want)
		}
		if got[1].Value != "new" {
			t.Fatalf("user:2 = %q, want new", got[1].Value)
		}
		all, err := s.ScanPrefix("")
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"order:1", "user:1", "user:2", "users"}; !fxEqual(fxKeys(all), want) {
			t.Fatalf("all keys = %v, want %v", fxKeys(all), want)
		}
		none, err := s.ScanPrefix("zzz")
		if err != nil || len(none) != 0 {
			t.Fatalf("ScanPrefix(zzz) = %v, %v", none, err)
		}
	}

	check(s)

	fxClose(t, s) // recovery rebuilds the sorted key list
	s = fxOpen(t, path, opts)
	check(s)

	if err := s.Compact(); err != nil { // compaction keeps it correct
		t.Fatal(err)
	}
	check(s)
	fxClose(t, s)
}

func TestRepeatedUpdatesLatestWins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	opts := Options{SyncPolicy: SyncNever}
	s := fxOpen(t, path, opts)

	for i := 0; i < 100; i++ {
		fxMustSet(t, s, "k", fmt.Sprintf("v%d", i))
	}
	fxMustGet(t, s, "k", "v99")
	fxClose(t, s)

	s = fxOpen(t, path, opts)
	defer s.Close()
	fxMustGet(t, s, "k", "v99")
	st, _ := s.Stats()
	if st.LiveKeys != 1 {
		t.Fatalf("LiveKeys = %d, want 1", st.LiveKeys)
	}
}

func TestSyncNeverPersistsAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := fxOpen(t, path, Options{SyncPolicy: SyncNever})
	fxMustSet(t, s, "k", "v")
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	fxClose(t, s)

	s = fxOpen(t, path, Options{})
	defer s.Close()
	fxMustGet(t, s, "k", "v")
}

func TestAppendAfterTornTailRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	opts := Options{SyncPolicy: SyncNever}

	s := fxOpen(t, path, opts)
	fxMustSet(t, s, "a", "1")
	fxMustSet(t, s, "b", "2")
	fxClose(t, s)

	// Simulate a crash that left a partial record header at the end.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{recordSetV2, 1, 0}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	s = fxOpen(t, path, opts)
	fxMustGet(t, s, "a", "1")
	fxMustGet(t, s, "b", "2")
	fxMustSet(t, s, "c", "3") // must land right after the last good record
	fxClose(t, s)

	s = fxOpen(t, path, opts)
	defer s.Close()
	fxMustGet(t, s, "a", "1")
	fxMustGet(t, s, "b", "2")
	fxMustGet(t, s, "c", "3")

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 45 { // three 15-byte records, no leftover garbage
		t.Fatalf("file size = %d, want 45", info.Size())
	}
}

func TestLegacyV1FileStillReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")

	var data []byte
	data = append(data, fxV1Record(recordSet, "old", "1")...)
	data = append(data, fxV1Record(recordSet, "gone", "x")...)
	data = append(data, fxV1Record(recordDelete, "gone", "")...)
	data = append(data, fxV1Record(recordSet, "old", "2")...)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}

	s := fxOpen(t, path, Options{SyncPolicy: SyncNever})
	fxMustGet(t, s, "old", "2")
	if _, err := s.Get("gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted V1 key: %v", err)
	}
	fxMustSet(t, s, "new", "n") // V2 record appended after V1 records
	fxClose(t, s)

	s = fxOpen(t, path, Options{SyncPolicy: SyncNever})
	defer s.Close()
	fxMustGet(t, s, "old", "2")
	fxMustGet(t, s, "new", "n")

	if err := s.Compact(); err != nil { // rewrites everything as V2
		t.Fatal(err)
	}
	fxMustGet(t, s, "old", "2")
	fxMustGet(t, s, "new", "n")
	st, _ := s.Stats()
	if st.FileSize != 34 { // two V2 records of 9+3+1+4 = 17 bytes
		t.Fatalf("compacted size = %d, want 34", st.FileSize)
	}
}

func TestFailedWriteDoesNotUpdateIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := fxOpen(t, path, Options{SyncPolicy: SyncNever})
	fxMustSet(t, s, "a", "1")

	_ = s.file.Close() // simulate the file becoming unusable

	if err := s.Set("b", "2"); err == nil {
		t.Fatal("Set on a broken file returned nil")
	}
	if _, ok := s.index["b"]; ok {
		t.Fatal("index updated although the write failed")
	}
	if len(s.keys) != 1 || s.index["a"].recordSize != 15 {
		t.Fatalf("existing state damaged: keys=%v", s.keys)
	}
	if _, err := s.Get("a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("store should refuse further use after an unrecoverable write failure, got %v", err)
	}
}

func TestRandomizedOperationsMatchModel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	opts := Options{SyncPolicy: SyncNever}
	rng := rand.New(rand.NewSource(42))
	model := map[string]string{}
	s := fxOpen(t, path, opts)

	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("k%02d", rng.Intn(25))

		switch op := rng.Intn(100); {
		case op < 45:
			val := fmt.Sprintf("v%d", i)
			if rng.Intn(10) == 0 {
				val = ""
			}
			fxMustSet(t, s, key, val)
			model[key] = val
		case op < 65:
			err := s.Delete(key)
			if _, ok := model[key]; ok {
				if err != nil {
					t.Fatalf("op %d: Delete(%q): %v", i, key, err)
				}
				delete(model, key)
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatalf("op %d: Delete(%q) of missing key: %v", i, key, err)
			}
		case op < 90:
			got, err := s.Get(key)
			if want, ok := model[key]; ok {
				if err != nil || got != want {
					t.Fatalf("op %d: Get(%q) = %q, %v; want %q", i, key, got, err, want)
				}
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatalf("op %d: Get(%q) of missing key: %v", i, key, err)
			}
		case op < 95:
			if err := s.Compact(); err != nil {
				t.Fatalf("op %d: Compact: %v", i, err)
			}
		default:
			fxClose(t, s)
			s = fxOpen(t, path, opts)
		}
	}
	defer s.Close()

	wantKeys := make([]string, 0, len(model))
	for k := range model {
		wantKeys = append(wantKeys, k)
	}
	sort.Strings(wantKeys)

	got, err := s.ScanPrefix("")
	if err != nil {
		t.Fatal(err)
	}
	if !fxEqual(fxKeys(got), wantKeys) {
		t.Fatalf("final keys = %v, want %v", fxKeys(got), wantKeys)
	}
	for _, kv := range got {
		if kv.Value != model[kv.Key] {
			t.Fatalf("final value for %q = %q, want %q", kv.Key, kv.Value, model[kv.Key])
		}
	}
}

func TestConcurrentOperations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := fxOpen(t, path, Options{SyncPolicy: SyncNever})
	defer s.Close()

	const workers, perWorker = 8, 100
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("w%d-k%d", w, i)
				val := fmt.Sprintf("v%d", i)
				if err := s.Set(key, val); err != nil {
					t.Errorf("Set(%q): %v", key, err)
					return
				}
				got, err := s.Get(key)
				if err != nil || got != val {
					t.Errorf("Get(%q) = %q, %v; want %q", key, got, err, val)
					return
				}
				if i%25 == 0 {
					if _, err := s.Stats(); err != nil {
						t.Errorf("Stats: %v", err)
						return
					}
					if _, err := s.ScanPrefix(fmt.Sprintf("w%d-", w)); err != nil {
						t.Errorf("ScanPrefix: %v", err)
						return
					}
				}
			}
		}(w)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if err := s.Compact(); err != nil {
				t.Errorf("Compact: %v", err)
				return
			}
		}
	}()

	wg.Wait()

	st, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.LiveKeys != workers*perWorker || st.Sets != workers*perWorker {
		t.Fatalf("LiveKeys=%d Sets=%d, want %d each", st.LiveKeys, st.Sets, workers*perWorker)
	}
}

func BenchmarkScanPrefix(b *testing.B) {
	s, err := OpenWithOptions(filepath.Join(b.TempDir(), "scan.db"), Options{SyncPolicy: SyncNever})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	for i := 0; i < 10000; i++ {
		if err := s.Set(fmt.Sprintf("key-%05d", i), "value"); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := s.ScanPrefix("key-0001") // matches key-00010 .. key-00019
		if err != nil {
			b.Fatal(err)
		}
		if len(res) != 10 {
			b.Fatalf("got %d results, want 10", len(res))
		}
	}
}
