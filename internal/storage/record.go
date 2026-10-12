package storage

// Records are encoded as:
// [type:1][key length:4][value length:4][key bytes][value bytes].
// Integer fields use little-endian encoding.
//
// Set records store a value. Delete records have an empty value and act as
// tombstones during recovery. The on-disk format is currently version 1.
