package storage

import (
	"encoding/binary"
	"hash/crc32"
)

// On-disk record layouts. All integers are little-endian.
//
// Version 1 (legacy, read-only; never written by this code):
//
//	[type:1][key length:4][value length:4][key bytes][value bytes]
//
// Version 2 (written by Set, Delete and Compact):
//
//	[type:1][key length:4][value length:4][key bytes][value bytes][crc32:4]
//
// The V2 CRC32 (IEEE) covers every byte of the record that precedes it
// (header, key and value). Delete records are tombstones: they have a key and
// a zero-length value. Type bytes: 1 = V1 set, 2 = V1 delete, 3 = V2 set,
// 4 = V2 delete.

// encodeV2 builds a complete checksummed V2 record for kind, which must be
// recordSetV2 or recordDeleteV2.
func encodeV2(kind byte, key, value string) []byte {
	rec := make([]byte, headerSize, headerSize+len(key)+len(value)+checksumSize)
	rec[0] = kind
	binary.LittleEndian.PutUint32(rec[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(rec[5:9], uint32(len(value)))
	rec = append(rec, key...)
	rec = append(rec, value...)

	var sum [checksumSize]byte
	binary.LittleEndian.PutUint32(sum[:], crc32.ChecksumIEEE(rec))
	return append(rec, sum[:]...)
}
