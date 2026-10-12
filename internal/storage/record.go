package storage

// Version 1 record layout:
// [type:1][key length:4][value length:4][key bytes][value bytes].
// Length fields use little-endian encoding. Delete records are tombstones.
