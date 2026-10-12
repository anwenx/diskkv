# diskkv: explanation and viva notes

## One-minute pitch
diskkv is a Bitcask-style key-value store. Every write appends a checksummed
record to one log file; an in-memory map says where the newest value of each key
lives, so a read is one map lookup and one `ReadAt`. Deletes append tombstones.
On startup the log is replayed to rebuild the index. `Compact` rewrites only
live records to reclaim space.

## What is inherited vs. mine
- Inherited design (Bitcask): append-only log, in-memory index, tombstones, replay, compaction.
- Already in my project before the upgrade: V1/V2 formats, CRC32, torn-tail recovery, compaction via temp file + rename, tests, benchmarks.
- Added in this upgrade: `Options`, `Stats`, `CompactionRecommended`, `ScanPrefix` with a sorted key list, failed-write rollback (tracked `size` + `WriteAt` + truncate), `Sync`, sync-on-`Close`, size accounting, CLI `scan`/`stats`/`-db`, new tests, README, example.

## Code tour (where to point)
- `record.go`: record layout comment and `encodeV2` (header + key + value + CRC32 of all preceding bytes).
- `store.go: recover`: replay loop; torn tail -> `Truncate`; CRC mismatch -> error; builds `index`, `keys`, `liveBytes`, `size`.
- `store.go: appendRecord`: encode, `WriteAt(size)`, fsync (SyncAlways), roll back on failure, and only then update index/accounting.
- `store.go: Compact`: sorted keys -> temp file -> fsync -> close old -> rename -> reopen -> swap index.
- `store.go: ScanPrefix`: `sort.SearchStrings` for the start, walk while `HasPrefix`.

## Likely questions
**Why append-only?** Sequential writes are fast and a crash can only damage the tail, which recovery can detect and drop. Old bytes are never modified.

**Why an in-memory index?** One lookup gives file offset and size, so `Get` needs one disk read. Cost: all keys must fit in RAM.

**What is a tombstone and why not just remove the record?** The log is append-only, so a delete is a new record. Replay applies it (`delete(index, key)`); compaction drops it along with the dead records it covered.

**How does recovery work?** Read records from offset 0. For each, check type and sizes, check the whole record fits in the file, verify the CRC for V2, then apply it to the index. If the last record is incomplete it is truncated.

**Why does a checksum mismatch fail `Open` instead of skipping the record?** Skipping could silently resurrect an older value of that key or lose a delete. I chose to fail loudly. Trade-off: no automatic repair.

**What does CRC32 protect against, and what not?** It detects accidental corruption (bit rot, torn bytes). It is not cryptographic and does not detect a deliberate attacker. V1 records have no checksum, so V1 corruption goes undetected.

**What is the V1/V2 difference and why keep V1?** V2 adds a trailing CRC32 and uses types 3/4. V1 (types 1/2) is still read so old files remain usable; new writes are V2 and `Compact` converts everything.

**What happens if `Set` fails halfway?** `appendRecord` truncates the file back to the previous end and leaves the index unchanged, so a later write cannot land after garbage. If truncation also fails the store marks itself closed; reopening runs recovery.

**Is `Set` durable?** With `SyncAlways`, success means `write` and `fsync` returned no error. That is not a hardware guarantee (disk caches, firmware). With `SyncNever` it is only in the OS cache until `Sync`/`Close`.

**How does compaction stay safe?** It writes a new file, fsyncs it, then `rename`s over the old one (atomic on POSIX). A crash before the rename leaves the old file intact; stray `.diskkv-compact-*` temp files are ignorable. Weak spots: it blocks all operations, and the directory fsync is best effort.

**How is concurrency handled?** One `sync.RWMutex`: `Get`/`ScanPrefix`/`Stats` take the read lock, `Set`/`Delete`/`Compact`/`Sync`/`Close` take the write lock. Counters are atomics because `Get` increments them under the read lock. It is single-process only.

**How is `ScanPrefix` efficient?** Keys are kept in a sorted slice. Binary search finds the first candidate in O(log n); matches are contiguous, so the scan is O(m) plus one disk read per match. Inserting a new key costs O(n) for the slice shift. Recovery sorts once, O(n log n).

**How does `Stats` avoid scanning the file?** `size` and `liveBytes` are updated on every append and rebuilt in recovery. Stale bytes = size - liveBytes.

**What is `CompactionRecommended`?** Advisory: file >= `CompactMinBytes` and stale/size >= `CompactStaleRatio`. I did not add background compaction to avoid goroutine and lock-ordering risk.

**What did you not build?** LRU cache (would need invalidation on Set/Delete/Compact and benchmarks showing a win), hint files (needs a validated, versioned format and stale-hint handling), multi-process locking, checksum verification on `Get`.

**How did you test it?** Table/regression tests for torn headers/payloads, flipped checksum bytes, V1 files, empty values, size limits, append-after-recovery; a seeded randomized sequence compared against a Go map (including compaction and reopen); a concurrent test run with `-race`; CLI tests.

**How do you benchmark honestly?** Setup happens before `b.ResetTimer()`, timed loops only do the operation, and results depend on fsync cost, so compare `SyncAlways` vs `SyncNever` explicitly. I report no numbers I did not measure.

## Demo script (3 minutes)
```bash
rm -f /tmp/demo.db
./diskkv -db /tmp/demo.db set user:1 alice
./diskkv -db /tmp/demo.db set user:2 bob
./diskkv -db /tmp/demo.db set user:1 alice-v2     # stale record created
./diskkv -db /tmp/demo.db scan user:              # ordered, latest values
./diskkv -db /tmp/demo.db stats                   # stale_bytes > 0
./diskkv -db /tmp/demo.db compact
./diskkv -db /tmp/demo.db stats                   # stale_bytes = 0, smaller file
./diskkv -db /tmp/demo.db delete user:2
./diskkv -db /tmp/demo.db get user:2              # key not found, exit code 1
```
Corruption demo: flip one byte with
`printf '\x00' | dd of=/tmp/demo.db bs=1 seek=12 conv=notrunc`, then run any
command and show the checksum error from `Open`.
