# diskkv

A small append-only key-value database written in Go.

## Features

- Set and retrieve string key-value pairs
- Logical deletion using tombstone records
- Persistent storage with index recovery on startup
- Append-only record storage
- CRC32 checksums for newly written records
- Backward-compatible recovery of legacy V1 records
- Detection of checksum corruption during recovery
- Recovery from incomplete trailing records
- Compaction to remove overwritten and deleted records
- Standard-library-only implementation

## Usage

```bash
go run ./cmd/diskkv set language Go
go run ./cmd/diskkv get language
go run ./cmd/diskkv delete language
go run ./cmd/diskkv compact
```

The CLI stores data in `diskkv.db` in the current working directory.

## Library

```go
package main

import (
    "fmt"
    "github.com/anwenx/diskkv/internal/storage"
)

func main() {
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

    fmt.Println(value)
}
```

Missing keys return `storage.ErrNotFound`. Empty strings are valid values.

## Limitations

- The index is held in memory and rebuilt during recovery.
- Multiple processes writing to the same database are not coordinated.
- Full crash-safe replacement guarantees have not been established; durability depends on filesystem behavior and synchronization guarantees.
- A checksum mismatch causes recovery to return an error rather than silently accepting the corrupted record.