# diskkv

A small append-only key-value database written in Go.

## Features

- Set and retrieve string key-value pairs
- Logical deletion using tombstones
- Persistent storage and index recovery
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

## Limitations

The index is in memory. This educational implementation does not yet provide
checksums, multi-process locking, or full crash-safe replacement guarantees.
A malformed or truncated record causes recovery to return an error.
