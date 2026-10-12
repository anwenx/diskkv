# diskkv

A small embedded key-value database in Go, built on an append-only log with an
in-memory index (the Bitcask design). Standard library only.

> Status: a learning/portfolio project. It is tested, but it is **not**
> production-ready; see [Known limitations](#known-limitations).

## Features

Inherited from the Bitcask-style design:

- Append-only log; writes never modify earlier bytes
- In-memory index of live keys (key -> value offset/size); one disk read per `Get`
- Tombstone records for deletes
- Recovery by replaying the log on `Open`
- Compaction that rewrites only live records

Implemented in this project:

- Record format V1 (legacy, still readable) and V2 with a CRC32 checksum
- Checksum validation and truncation of an incomplete final record on recovery
- Compaction through a temp file + atomic rename, always emitting V2 records
- **Options**: key/value size limits, `SyncAlways`/`SyncNever`, compaction thresholds (`OpenWithOptions`)
- **Stats()**: live keys, file/live/stale bytes, operation counters, compaction info, in O(1)
- **CompactionRecommended()**: advisory check based on tracked stale bytes (never compacts automatically)
- **ScanPrefix()**: ordered prefix queries over a sorted key list
- **Failed-write rollback**: a failed write/sync truncates the log back and leaves the index untouched
- CLI with `set`, `get`, `delete`, `scan`, `stats`, `compact` and a `-db` flag

Not implemented (future work): LRU value cache, hint files for faster startup,
background/automatic compaction, multi-process locking.

## Install, build, test

Requires the Go version listed in `go.mod`.

```bash
git clone https://github.com/anwenx/diskkv && cd diskkv
make build        # or: go build -o diskkv ./cmd/diskkv
make test         # go test ./...
make race         # go test -race ./...
make vet          # go vet ./...
make bench        # go test -bench . -run '^$' ./internal/storage
```

## CLI

```bash
./diskkv set language Go
./diskkv get language
./diskkv set user:1 alice
./diskkv set user:2 bob
./diskkv scan user:        # prints "key<TAB>value" lines in key order
./diskkv scan ""           # all keys
./diskkv stats
./diskkv delete language
./diskkv compact
./diskkv -db /tmp/my.db get user:1    # choose the database file
```

The default database file is `diskkv.db` in the current directory. Exit codes:
0 success, 1 operation failed (including "key not found"), 2 usage error.

## Library

```go
s, err := storage.Open("data.db") // default options
// or:
s, err = storage.OpenWithOptions("data.db", storage.Options{
    MaxValueSize:      1 << 20,         // reject values over 1 MiB
    SyncPolicy:        storage.SyncNever, // fsync only on Sync()/Close()
    CompactMinBytes:   4 << 20,
    CompactStaleRatio: 0.6,
})
if err != nil { /* handle */ }
defer s.Close()

_ = s.Set("user:1", "alice")
v, err := s.Get("user:1")              // storage.ErrNotFound if missing
kvs, _ := s.ScanPrefix("user:")        // []storage.KeyValue, sorted by key
st, _ := s.Stats()
if s.CompactionRecommended() { _ = s.Compact() }
```

Runnable demo: `go run ./examples/basic`.

API overview: `Open`, `OpenWithOptions`, `DefaultOptions`, `Set`, `Get`,
`Delete`, `ScanPrefix`, `Stats`, `CompactionRecommended`, `Compact`, `Sync`,
`Close`. Errors: `ErrNotFound`, `ErrClosed`, `ErrKeyTooLarge`,
`ErrValueTooLarge`, `ErrInvalidOptions`. Empty keys and empty values are valid.
The `Store` is safe for concurrent use (a `sync.RWMutex` guards it).

### Options

| Option | Default | Meaning |
|---|---|---|
| `MaxKeySize`, `MaxValueSize` | 64 MiB | Limits for new writes (0 = default, hard cap 64 MiB) |
| `SyncPolicy` | `SyncAlways` | fsync after each write, or only on `Sync`/`Close` |
| `CompactMinBytes` | 1 MiB | No compaction recommendation below this file size |
| `CompactStaleRatio` | 0.5 | Recommend compaction when stale bytes >= this fraction |

Invalid values return `ErrInvalidOptions` and no file is created.

### Stats fields

`LiveKeys`; `FileSize` (logical end of log); `LiveBytes` (newest record of each
live key); `StaleBytes = FileSize - LiveBytes` (overwritten records and
tombstones); `Sets`, `Gets` (found only), `Deletes`, `BytesWritten` (appends,
excluding compaction) and `Compactions` (counters since this `Open`, not
persisted); `LastCompaction`, `LastCompactionReclaimed`;
`CompactionRecommended`. Size fields are rebuilt during recovery and updated
incrementally, so `Stats` never scans the file.

## Architecture

```
            Set / Delete                         Get / ScanPrefix
                 |                                     |
        encode V2 record + CRC32                 index lookup (map)
                 |                                     |
   WriteAt(end of log) -> fsync (SyncAlways)           v
                 |                              ReadAt(value offset)
       success?  | no -> truncate back, index untouched
                 v yes
   update index map + sorted keys + byte accounting

   Open:    replay log -> validate CRC -> rebuild index, sorted keys, accounting
   Compact: write live records (key order) to temp file -> fsync -> rename over db
```

Layout: `internal/storage` (engine: `store.go`, `record.go`), `cmd/diskkv` (CLI),
`examples/basic` (library demo), `docs/VIVA.md` (explanations and Q&A).

## Record format

All integers are little-endian.

```
V1 (legacy, read-only): [type:1][key len:4][value len:4][key][value]
V2 (written today):     [type:1][key len:4][value len:4][key][value][crc32:4]
```

Types: 1 = V1 set, 2 = V1 delete, 3 = V2 set, 4 = V2 delete. The CRC32 (IEEE)
covers header + key + value. A delete record carries the key and a zero-length
value. Files may mix V1 and V2 records; new appends are always V2, and
`Compact` rewrites everything as V2.

## Recovery and checksums

`Open` replays the log from the start:

- A trailing partial header, or a final record whose declared length runs past
  the end of the file, is treated as a torn write and truncated.
- A V2 record whose CRC32 does not match makes `Open` fail with an error; the
  corrupted data is never silently accepted or dropped.
- Unknown record types, oversized length fields and malformed delete records
  fail `Open`.
- V1 records are accepted without a checksum (they have none).

## Compaction

`Compact` holds the write lock, writes every live record in key order to a
temp file in the same directory, fsyncs it, closes the old file, renames the
temp file over the database, then reopens it and swaps in the new index. If the
rename fails, the original file is reopened and the old index kept. Reads and
writes block while compaction runs. The directory fsync after the rename is
best effort.

## Durability

With `SyncAlways`, `Set`/`Delete` return success only after `write` and
`fsync` succeeded, so the record has been handed to stable storage as far as
the OS and disk report. That is not a guarantee against faulty hardware,
lying disk caches or filesystem bugs. With `SyncNever`, a successful return
means only that the OS accepted the write; recent writes can be lost on power
failure (but recovery still yields a consistent prefix, torn tail discarded).
If a write or sync fails, the log is truncated back to its previous end and the
in-memory index is not updated; if even that rollback fails, the store refuses
further use until reopened.

## Testing and benchmarks

Tests cover persistence, tombstones, truncated and corrupted files, V1
compatibility, compaction, repeated updates, empty values, size boundaries,
append-after-recovery, failed writes, prefix scans (also after reopen and
compaction), a seeded randomized run checked against a map model, concurrent
operations, and the CLI. Benchmarks: `BenchmarkSet`, `BenchmarkGet`,
`BenchmarkCompact`, `BenchmarkScanPrefix`. Run `make bench`; no performance
numbers are claimed here, so measure on your own machine.

## Design decisions

- Offsets are tracked in memory (`size`) and records are written with `WriteAt`,
  so a failed write can be rolled back by truncating to a known offset.
- The sorted key slice makes prefix scans O(log n + m); the price is an O(n)
  shift when a brand-new key is inserted and O(n) extra memory.
- Compaction is explicit; the store only reports whether it is advisable,
  avoiding background goroutines and their lock-ordering risks.
- Old V1 files stay readable; nothing rewrites them unless you call `Compact`.

## Known limitations

- The whole key set must fit in memory; startup time is proportional to log size (no hint file).
- Single process only; there is no file locking, so two processes on one file will corrupt it.
- `Get` does not re-verify checksums; they are checked during recovery only.
- Compaction blocks all operations and needs temporary space for the live data.
- No transactions, batching, TTLs or value cache. Keys and values are strings up to 64 MiB.
- A checksum mismatch anywhere makes `Open` fail; there is no repair tool.
- The CLI opens the database for each invocation and has no interactive mode.

## Acknowledgments

Based on the Bitcask log-structured design (Riak). The
[avinassh/go-caskdb](https://github.com/avinassh/go-caskdb) project was used as
a learning reference; this repository is an independent implementation.
