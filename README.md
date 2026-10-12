# diskkv

A small append-only key-value database written in Go.

## Features

- Set and retrieve string key-value pairs
- Logical deletion using tombstone records
- Persistent storage and index recovery on startup
- Standard-library-only implementation

## Requirements

Go 1.26 or a compatible newer Go release.

## Usage

```bash
go run ./cmd/diskkv set language Go
go run ./cmd/diskkv get language
go run ./cmd/diskkv delete language
```

The CLI stores data in `diskkv.db` in the current working directory.

## Library

```go
store, err := storage.Open("data.db")
if err != nil {
    panic(err)
}
defer store.Close()

if err := store.Set("language", "Go"); err != nil {
    panic(err)
}

value, err := store.Get("language")
if err != nil {
    panic(err)
}
println(value)
```

Missing keys return `storage.ErrNotFound`. Empty strings are valid values.

## Current limitations

- The index is held in memory.
- Old versions of updated and deleted records remain in the file.
- No compaction, checksums, or multi-process locking yet.
- A malformed or truncated record causes startup recovery to return an error.
- Each write is synced to disk, which favors durability over throughput.

## On-disk format

Version 1 records contain an operation type, little-endian key and value
lengths, and the key/value bytes. Delete records are tombstones.

This project is an educational implementation; do not use it as a production
database without further durability and corruption testing.
