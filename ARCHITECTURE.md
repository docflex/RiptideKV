# RiptideKV — Architecture

This document describes the complete internal architecture of RiptideKV, a
**Log-Structured Merge (LSM) tree** key-value store written in Rust.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Crate Dependency Graph](#crate-dependency-graph)
3. [Data Flow — Write Path](#data-flow--write-path)
4. [Data Flow — Read Path](#data-flow--read-path)
5. [Data Flow — Flush (Memtable → SSTable)](#data-flow--flush)
6. [Data Flow — Compaction](#data-flow--compaction)
7. [Recovery (Cold Start)](#recovery-cold-start)
8. [On-Disk Layout](#on-disk-layout)
9. [Crate-by-Crate Deep Dive](#crate-by-crate-deep-dive)
   - [bloom](#bloom)
   - [memtable](#memtable)
   - [wal](#wal)
   - [sstable](#sstable)
   - [engine](#engine)
   - [cli](#cli)
10. [Sequence Numbers & Ordering](#sequence-numbers--ordering)
11. [Tombstones & Deletion Semantics](#tombstones--deletion-semantics)
12. [Crash Safety Guarantees](#crash-safety-guarantees)
13. [Test Coverage](#test-coverage)

---

## System Overview

```
            ┌─────────────────────────────────────────────┐
            │              CLI  (cargo run -p cli)        │
            │  SET / GET / DEL / SCAN / FLUSH / COMPACT   │
            └──────────────────┬──────────────────────────┘
                                │
                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│                           ENGINE  (orchestrator)                       │
│                                                                        │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌───────────┐            │
│  │ write.rs │   │ read.rs  │   │compact.rs│   │recovery.rs│            │
│  │ set/del  │   │ get/scan │   │ compact  │   │ WAL replay│            │
│  │ flush    │   │          │   │ GC       │   │ SST load  │            │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬──────┘            │
│       │              │              │              │                   │
│       ▼              ▼              ▼              ▼                   │
│  ┌─────────────────────────────────────────────────────────────┐       │
│  │                     Engine struct (lib.rs)                  │       │
│  │  mem: Memtable    l0_sstables    l1_sstables    manifest    │       │
│  │  wal_writer       seq            flush_threshold            │       │
│  └─────────────────────────────────────────────────────────────┘       │
└──────────┬──────────────┬──────────────┬───────────────────────────────┘
           │              │              │
           ▼              ▼              ▼
    ┌────────────┐  ┌──────────┐  ┌──────────────┐
    │  MEMTABLE  │  │   WAL    │  │   SSTABLE    │
    │  (BTreeMap)│  │ (append  │  │  (immutable  │
    │  in-memory │  │  log on  │  │   on-disk    │
    │  sorted    │  │  disk)   │  │   sorted)    │
    └────────────┘  └──────────┘  └──────┬───────┘
                                         │
                                    ┌────┴────┐
                                    │  BLOOM  │
                                    │ (filter │
                                    │ per SST)│
                                    └─────────┘
```

RiptideKV follows the classic LSM-tree design:

1. **Writes** go to an append-only WAL (for durability) and an in-memory
   Memtable (for fast reads).
2. When the Memtable grows past a threshold, it is **flushed** to an immutable
   SSTable on disk.
3. Over time, SSTables accumulate. **Compaction** merges them to reduce read
   amplification and reclaim space from deleted keys.
4. **Reads** check the Memtable first, then SSTables from newest to oldest.
   Bloom filters skip SSTables that definitely don't contain the key.

---

## Crate Dependency Graph

```
cli
 └── engine
      ├── memtable
      ├── wal
      └── sstable
           └── bloom
```

Each crate is independently testable. The `engine` crate is the only one that
ties them together. The `cli` crate is a thin interactive shell over the engine.

---

## Data Flow — Write Path

```
  Client: SET "user:1" "Alice"
       │
       ▼
  ┌─────────────────────────────────────────────────────────┐
  │ engine::set(key, value)                                 │
  │                                                         │
  │  1. seq += 1                    (monotonic counter)     │
  │  2. wal_writer.append(Put{seq, key, value})             │
  │     └─► [len][crc32][seq][op=0][key_len][key]           │
  │         [val_len][value]        ──► wal.log on disk     │
  │  3. mem.put(key, value, seq)    ──► BTreeMap insert     │
  │  4. if mem.approx_size() >= flush_threshold:            │
  │        flush()                  ──► new SSTable on disk │
  └─────────────────────────────────────────────────────────┘
```

**Why WAL before Memtable?** If the process crashes after the WAL append but
before the Memtable insert, the WAL replay on restart will reconstruct the
write. If we did it the other way around, a crash after the Memtable insert
would lose the write (Memtable is volatile).

---

## Data Flow — Read Path

```
  Client: GET "user:1"
       │
       ▼
  ┌──────────────────────────────────────────────────────────────┐
  │ engine::get(key)                                             │
  │                                                              │
  │  1. Check MEMTABLE ──────────────────────► Found? Return it  │
  │     (includes tombstones — if tombstone,   (freshest data)   │
  │      return None immediately)                                │
  │                          │ Not found                         │
  │                          ▼                                   │
  │  2. Check L0 SSTables (newest → oldest) ─► Found? Return it  │
  │     For each SSTable:                                        │
  │       a. bloom.may_contain(key)?  ── No ──► skip             │
  │       b. index lookup → offset                               │
  │       c. read record at offset                               │
  │       d. verify CRC32                                        │
  │       e. tombstone? → return None                            │
  │                          │ Not found                         │
  │                          ▼                                   │
  │  3. Check L1 SSTables (newest → oldest) ─► Found? Return it  │
  │     (same bloom → index → read → CRC flow)                   │
  │                          │ Not found                         │
  │                          ▼                                   │
  │  4. Return None (key does not exist)                         │
  └──────────────────────────────────────────────────────────────┘
```

**Why check L0 before L1?** L0 SSTables come from recent flushes and may
contain newer versions of keys that also exist in L1. The first match wins,
so checking L0 first ensures we always return the freshest data.

---

## Data Flow — Flush

```
  Memtable (sorted BTreeMap)
  ┌──────────────────────┐
  │ "a" → (seq=3, "val") │
  │ "b" → (seq=1, "val") │     SSTableWriter::write_from_memtable()
  │ "c" → (seq=2, None)  │ ──────────────────────────────────────────►
  └──────────────────────┘
                                    ┌─────────────────────────────┐
                                    │  sst-00000000000000003-     │
                                    │       1708600000000.sst     │
                                    │                             │
                                    │  DATA: a=val, b=val, c=tomb │
                                    │  BLOOM: {a, b, c}           │
                                    │  INDEX: a→0, b→45, c→88     │
                                    │  FOOTER: max_seq=3 | SST3   │
                                    └─────────────────────────────┘

  After flush:
    1. Manifest updated:  L0:sst-...-....sst
    2. WAL truncated to 0 bytes
    3. Memtable cleared
    4. New SSTableReader opened and inserted at l0_sstables[0]
    5. If l0_sstables.len() >= l0_compaction_trigger → auto-compact
```

**Atomic write**: The SSTable is first written to a `.sst.tmp` file, then
renamed to `.sst`. This ensures a crash during write never leaves a corrupt
SSTable — only a harmless `.tmp` file that is cleaned up on next startup.

---

## Data Flow — Compaction

```
  BEFORE                                    AFTER
  ──────                                    ─────

  L0: [SST-5] [SST-4] [SST-3]             L0: (empty)
       │        │        │
       └────────┼────────┘                 L1: [SST-6]  (single merged file)
                │                                 │
                ▼                                 │
       ┌─────────────────┐                        │
       │ MergeIterator    │                       │
       │ (min-heap on key │                       │
       │  + max-seq wins) │ ──── stream ────────► │
       │                  │                       │
       │ Tombstone GC:    │              ┌────────┴────────┐
       │ drop if no older │              │ New SSTable      │
       │ SST exists AND   │              │ with only live   │
       │ memtable doesn't │              │ keys (tombstones │
       │ reference key    │              │ GC'd away)       │
       └─────────────────┘              └─────────────────┘

  Old SST files deleted.
  Manifest updated: L1:sst-...-....sst
```

**Streaming compaction**: The `MergeIterator` walks all SSTables in sorted key
order using a min-heap. For each unique key, only the entry with the highest
sequence number is kept. The merged output is written directly to a new SSTable
via `write_from_iterator()` — the entire dataset is never materialized in RAM.

**Tombstone GC**: During a full compaction (all L0 + L1 → single L1), there
are no older SSTables that could contain shadowed values. Tombstones are
therefore safe to drop — unless the Memtable still references the key (the
Memtable is not part of compaction, so tombstones that shadow Memtable data
must be preserved).

---

## Recovery (Cold Start)

```
  Engine::new()
       │
       ▼
  ┌──────────────────────────────────────────────────────────────┐
  │ 1. Create SST directory if missing                           │
  │ 2. Clean up leftover .sst.tmp files                          │
  │ 3. Replay WAL → Memtable                                     │
  │    ┌──────────────────────────────────────────────────┐      │
  │    │ wal.log: [Put k=a seq=1] [Del k=b seq=2] [...]   │      │
  │    │          ──► mem.put(a, ..., 1)                  │      │
  │    │          ──► mem.delete(b, 2)                    │      │
  │    │          ──► max_seq = 2                         │      │
  │    └──────────────────────────────────────────────────┘      │
  │ 4. Open WAL writer in append mode                            │
  │ 5. Load MANIFEST → assign SSTables to L0/L1                  │
  │    ┌──────────────────────────────────────────────────┐      │
  │    │ MANIFEST:                                        │      │
  │    │   L0:sst-00000000000000000005-170860000.sst      │      │
  │    │   L1:sst-00000000000000000010-170860001.sst      │      │
  │    └──────────────────────────────────────────────────┘      │
  │ 6. Open each SSTable → extract max_seq from v3 footer        │
  │ 7. seq = max(wal_seq, sst_seq)                               │
  │                                                              │
  │ Result: Engine ready with Memtable + L0 + L1 + correct seq   │
  └──────────────────────────────────────────────────────────────┘
```

**Sequence number recovery**: The v3 SSTable footer stores `max_seq`, allowing
O(1) recovery of the highest sequence number without scanning all records.
For legacy v1/v2 SSTables, all keys are scanned as a fallback.

---

## On-Disk Layout

```
  data/
  ├── wal.log                          # Write-Ahead Log (binary)
  └── sst/
      ├── MANIFEST                     # Text file: L0/L1 assignments
      ├── sst-00000000000000000001-1708599999000.sst   # L0
      ├── sst-00000000000000000003-1708600000000.sst   # L0
      └── sst-00000000000000000010-1708600001000.sst   # L1 (compacted)
```

### SSTable Filename Convention

```
  sst-{seq:020}-{timestamp_ms}.sst
       │              │
       │              └── milliseconds since Unix epoch (uniqueness)
       └── zero-padded sequence number (sort order)
```

The 20-digit zero-padding ensures lexicographic sort matches numeric sort,
which is critical for loading SSTables in the correct newest-first order.

### SSTable File Layout (v3)

```
  ┌───────────────────────────────────────────────────────────────┐
  │                     DATA SECTION                              │
  │                                                               │
  │  For each record:                                             │
  │  ┌─────────┬─────────┬─────┬─────┬─────────┬─────────┬─────┐  │
  │  │ crc32   │ key_len │ key │ seq │ present │ val_len │ val │  │
  │  │ (u32)   │ (u32)   │     │(u64)│  (u8)   │ (u32)   │     │  │
  │  └─────────┴─────────┴─────┴─────┴─────────┴─────────┴─────┘  │
  │  CRC32 covers: key_len + key + seq + present [+ val_len + val]│
  │  present=1 → live value (val_len + val follow)                │
  │  present=0 → tombstone  (no val_len or val)                   │
  ├───────────────────────────────────────────────────────────────┤
  │                     BLOOM SECTION                             │
  │  ┌──────────┬────────────┬──────────┬───────────────────────┐ │
  │  │ num_bits │ num_hashes │ bits_len │ bits (byte array)     │ │
  │  │  (u64)   │   (u32)    │  (u32)   │                       │ │
  │  └──────────┴────────────┴──────────┴───────────────────────┘ │
  ├───────────────────────────────────────────────────────────────┤
  │                     INDEX SECTION                             │
  │  For each record:                                             │
  │  ┌─────────┬─────┬──────────────┐                             │
  │  │ key_len │ key │ data_offset  │                             │
  │  │ (u32)   │     │   (u64)      │                             │
  │  └─────────┴─────┴──────────────┘                             │
  ├───────────────────────────────────────────────────────────────┤
  │                     FOOTER (28 bytes)                         │
  │  ┌──────────┬──────────────┬──────────────┬─────────────────┐ │
  │  │ max_seq  │ bloom_offset │ index_offset │ magic ("SST3")  │ │
  │  │  (u64)   │    (u64)     │    (u64)     │    (u32)        │ │
  │  └──────────┴──────────────┴──────────────┴─────────────────┘ │
  └───────────────────────────────────────────────────────────────┘
```

### WAL Record Format

```
  ┌────────────┬──────────┬──────────────────────────────────────┐
  │ record_len │  crc32   │              body                    │
  │   (u32)    │  (u32)   │                                      │
  └────────────┴──────────┴──────────────────────────────────────┘

  Put body: [seq: u64][op=0: u8][key_len: u32][key][val_len: u32][value]
  Del body: [seq: u64][op=1: u8][key_len: u32][key]

  record_len includes the CRC but not itself.
  All integers are little-endian.
```

### Manifest Format

```
  # RiptideKV SSTable Manifest
  # Format: L<level>:<filename>
  L0:sst-00000000000000000005-1708600000000.sst
  L0:sst-00000000000000000003-1708599999000.sst
  L1:sst-00000000000000000010-1708600001000.sst
```

Written atomically via temp file + rename. Human-readable for debugging.

---

## Crate-by-Crate Deep Dive

### bloom

```
  Location: crates/bloom/src/lib.rs
  Purpose:  Space-efficient probabilistic set membership test
  Tests:    17
```

**What it does**: A bloom filter answers "is this key in the set?" with:
- **Definite NO** (no false negatives) — the key is guaranteed absent.
- **Probable YES** (possible false positives) — the key *might* be present.

**How it works**: Uses double hashing with FNV-1a:
```
  h(i) = h1 + i * h2   (mod num_bits)
```
where `h1` and `h2` are derived from two FNV-1a seeds. The optimal number of
hash functions `k` is computed from the desired false positive rate (default 1%).

**Role in the system**: Each SSTable embeds a serialized bloom filter built from
its keys. During point lookups (`GET`), the engine checks the bloom filter
before doing an index lookup + disk read. If the bloom filter says "no", the
entire SSTable is skipped — this is the primary optimization for read
performance in an LSM tree.

```
  GET "user:99"
       │
       ▼
  SSTable bloom filter: may_contain("user:99")?
       │                        │
      YES                      NO
       │                        │
       ▼                        ▼
  Index lookup + disk read    Skip entirely (saved I/O)
```

**Serialization**: The bloom filter is serialized into the SSTable's BLOOM
section as `[num_bits: u64][num_hashes: u32][bits_len: u32][bits: bytes]`.
A 128 MiB cap on deserialization prevents malicious or corrupt files from
causing OOM.

---

### memtable

```
  Location: crates/memtable/src/lib.rs
  Purpose:  In-memory sorted write buffer
  Tests:    43
```

**What it does**: The Memtable is a `BTreeMap<Vec<u8>, ValueEntry>` that holds
all recent writes before they are flushed to disk. It is the **fastest** layer
to read from (no disk I/O) and the **first** layer checked on every read.

**Key properties**:
- **Sorted order**: BTreeMap keeps keys in ascending byte order, which is
  exactly the order needed for SSTable flush (no sorting step required).
- **Sequence-gated writes**: Each write carries a sequence number. If a write
  arrives with a sequence number ≤ the existing entry's, it is silently
  rejected. This prevents stale WAL replays from overwriting newer data.
- **Tombstone support**: Deletes are stored as `ValueEntry { seq, value: None }`.
  The tombstone is flushed to SSTables and shadows older values during reads.
- **Approximate size tracking**: `approx_size()` tracks the cumulative byte
  size of keys + values. The engine uses this to decide when to flush.

```
  Memtable (BTreeMap)
  ┌──────────────────────────────────────────────────┐
  │  "apple"  → ValueEntry { seq: 5, Some("red") }   │
  │  "banana" → ValueEntry { seq: 3, Some("yellow")} │
  │  "cherry" → ValueEntry { seq: 7, None }          │  ← tombstone
  └──────────────────────────────────────────────────┘
       ▲                              │
       │                              ▼
   put/delete                   iter() → sorted
   (seq-gated)                  entries for flush
```

**Role in the system**: The Memtable absorbs all writes at memory speed. When
it exceeds `flush_threshold` bytes, the engine serializes it to a new SSTable
and clears it. The WAL ensures no data is lost if the process crashes before
the flush completes.

---

### wal

```
  Location: crates/wal/src/lib.rs
  Purpose:  Crash-safe durability via append-only binary log
  Tests:    22
```

**What it does**: The WAL (Write-Ahead Log) is an append-only binary file that
records every mutation **before** it is applied to the Memtable. On crash
recovery, the WAL is replayed to reconstruct the Memtable.

**Components**:
- **`WalWriter`**: Appends records to the log file. Uses a reusable internal
  buffer to minimize allocations. Optionally calls `fsync` after every append
  for maximum durability.
- **`WalReader`**: Reads and replays records from the log file. Tolerates
  truncated tails (partial writes from crashes) — it stops reading at the
  first incomplete record without returning an error.

**CRC32 integrity**: Each record includes a CRC32 checksum computed over the
body. On replay, the CRC is verified — if it doesn't match, the record is
treated as corruption and an error is returned.

```
  wal.log (append-only binary file)
  ┌─────────────────────────────────────────────────────────┐
  │ Record 1: [len=38][crc32][seq=1][PUT][key=a][val=hello] │
  │ Record 2: [len=22][crc32][seq=2][DEL][key=b]            │
  │ Record 3: [len=40][crc32][seq=3][PUT][key=c][val=world] │
  │ (truncated tail from crash — silently ignored)          │
  └─────────────────────────────────────────────────────────┘
```

**Role in the system**: The WAL is the **durability backbone**. Without it,
data in the Memtable would be lost on crash. The WAL is truncated to zero
bytes after every successful flush (because the data is now safely in an
SSTable). This keeps the WAL small and replay fast.

---

### sstable

```
  Location: crates/sstable/src/
  Purpose:  Immutable, sorted, on-disk key-value files
  Tests:    21
  Files:    lib.rs, reader.rs, writer.rs, merge.rs, format.rs
```

**What it does**: SSTables are the persistent storage layer. Each SSTable is a
single file containing sorted key-value records, a bloom filter, an index, and
a footer. Once written, an SSTable is **never modified** — it can only be
replaced during compaction.

**Sub-modules**:

| File | Responsibility |
|------|---------------|
| `format.rs` | Magic numbers, footer sizes, version constants |
| `writer.rs` | `write_from_memtable()`, `write_from_iterator()` (streaming) |
| `reader.rs` | `open()`, `get()`, `keys()`, `len()`, bloom filter checks |
| `merge.rs` | `MergeIterator` — min-heap merge of multiple SSTables |

**Writer flow**:
```
  Memtable or Iterator
       │
       ▼
  SSTableWriter
       │
       ├── 1. Write DATA records (sorted, with CRC32 per record)
       ├── 2. Build + write BLOOM filter
       ├── 3. Write INDEX (key → offset mapping)
       ├── 4. Write FOOTER (max_seq, bloom_offset, index_offset, magic)
       ├── 5. fsync the temp file
       └── 6. Rename .sst.tmp → .sst (atomic)
```

**Reader flow**:
```
  SSTableReader::open(path)
       │
       ├── 1. Read FOOTER (last 28 bytes) → get offsets + magic
       ├── 2. Read INDEX section → build in-memory key→offset map
       ├── 3. Read BLOOM section → deserialize bloom filter
       └── Ready for get() calls
            │
            ▼
       get(key):
         1. bloom.may_contain(key)? → No → return None
         2. index.get(key) → offset
         3. Seek to offset, read record
         4. Verify CRC32
         5. Return ValueEntry
```

**MergeIterator**: A min-heap that walks multiple SSTables in sorted key order.
For duplicate keys, the entry with the highest sequence number wins. Used by
compaction to produce a single merged output stream.

```
  SST-1: [a:1, c:3, e:5]
  SST-2: [b:2, c:4, d:6]     MergeIterator
  SST-3: [a:7, f:8]        ──────────────►  a:7, b:2, c:4, d:6, e:5, f:8
                                             (highest seq wins for dupes)
```

**Version compatibility**: The reader auto-detects v1/v2/v3 files by reading
the magic number from the footer. This allows seamless upgrades — old SSTables
continue to work alongside new ones.

---

### engine

```
  Location: crates/engine/src/
  Purpose:  Orchestrates all components into a complete storage engine
  Tests:    55
  Files:    lib.rs, write.rs, read.rs, compaction.rs, recovery.rs, manifest.rs
```

**What it does**: The engine crate is the **brain** of RiptideKV. It owns the
Memtable, WAL, and SSTables, and coordinates all operations between them.

**Module responsibilities**:

| File | What it does |
|------|-------------|
| `lib.rs` | `Engine` struct, constructor (`new`), accessors, `Debug`, `Drop` |
| `recovery.rs` | `replay_wal_and_build()`, `reader_max_seq()`, `cleanup_tmp_files()` |
| `write.rs` | `set()`, `del()`, `force_flush()`, internal `flush()` |
| `read.rs` | `get()`, `scan()` |
| `compaction.rs` | `compact()` with streaming merge + tombstone GC |
| `manifest.rs` | `Manifest` struct — load, save, add, replace (atomic file ops) |

**Public API**:

```rust
// Construction & recovery
Engine::new(wal_path, sst_dir, flush_threshold, wal_sync) -> Result<Engine>

// Write operations
engine.set(key, value) -> Result<()>
engine.del(key) -> Result<()>

// Read operations
engine.get(key) -> Result<Option<(seq, value)>>
engine.scan(start, end) -> Result<Vec<(key, value)>>

// Maintenance
engine.force_flush() -> Result<()>
engine.compact() -> Result<()>

// Introspection
engine.seq() -> u64
engine.sstable_count() -> usize
engine.l0_sstable_count() -> usize
engine.l1_sstable_count() -> usize
engine.flush_threshold() -> usize
engine.l0_compaction_trigger() -> usize

// Configuration
engine.set_flush_threshold(bytes)
engine.set_l0_compaction_trigger(count)  // 0 = disabled
```

**Level architecture**:

```
  ┌───────────────────────────────────────────────────┐
  │                    MEMTABLE                       │
  │  (freshest data, checked first on reads)          │
  ├───────────────────────────────────────────────────┤
  │                  L0 SSTables                      │
  │  (from flushes, may have overlapping key ranges)  │
  │  Ordered newest-first. Checked after memtable.    │
  ├───────────────────────────────────────────────────┤
  │                  L1 SSTables                      │
  │  (from compaction, non-overlapping key ranges)    │
  │  Ordered newest-first. Checked last.              │
  └───────────────────────────────────────────────────┘
```

**Auto-compaction**: After every flush, if `l0_sstables.len() >= l0_compaction_trigger`,
the engine automatically runs `compact()`. This keeps read amplification bounded
without requiring the caller to manually manage compaction. Set the trigger to
`0` to disable auto-compaction.

**Drop implementation**: When the `Engine` is dropped, any data remaining in
the Memtable is flushed to an SSTable as a best-effort operation. Errors are
silently ignored because `Drop` cannot propagate them — the data is still safe
in the WAL and will be recovered on the next startup.

---

### cli

```
  Location: crates/cli/src/main.rs
  Purpose:  Interactive command-line interface for the engine
  Tests:    5 (unit) + 4 criterion benchmarks
```

**What it does**: A REPL-style CLI that reads commands from stdin and executes
them against the engine. Designed for interactive use and scripted testing.

**Commands**:

| Command | Description |
|---------|-------------|
| `SET key value` | Insert or update a key-value pair |
| `GET key` | Look up a key (returns value or `(nil)`) |
| `DEL key` | Delete a key (writes a tombstone) |
| `SCAN [start] [end]` | Range scan (inclusive start, exclusive end) |
| `FLUSH` | Force flush memtable to SSTable |
| `COMPACT` | Trigger manual compaction |
| `STATS` | Print engine debug info (seq, counts, sizes) |
| `EXIT` / `QUIT` | Shut down gracefully |

**Configuration** (via environment variables):

| Variable | Default | Description |
|----------|---------|-------------|
| `RIPTIDE_WAL_PATH` | `wal.log` | WAL file path |
| `RIPTIDE_SST_DIR` | `data/sst` | SSTable directory |
| `RIPTIDE_FLUSH_KB` | `1024` | Flush threshold in KiB |
| `RIPTIDE_WAL_SYNC` | `true` | fsync every WAL append |
| `RIPTIDE_L0_TRIGGER` | `4` | L0 compaction trigger (0 = disabled) |

---

## Sequence Numbers & Ordering

Every write operation (SET or DEL) is assigned a **monotonically increasing
sequence number**. This is the single source of truth for ordering:

```
  Operation          Seq
  ─────────          ───
  SET a = "hello"     1
  SET b = "world"     2
  DEL a               3    ← tombstone with seq=3
  SET a = "back"      4    ← resurrects key with seq=4
```

**Conflict resolution**: When the same key appears in multiple places
(Memtable, L0, L1), the entry with the **highest sequence number wins**.
This is enforced in:
- `get()`: checks Memtable first (always has the highest seq for recent writes)
- `scan()`: merges all sources, keeps highest-seq entry per key
- `MergeIterator`: deduplicates by key, preferring highest seq
- `compact()`: writes only the winning entry to the output SSTable

**Recovery**: The sequence number is recovered as `max(wal_max_seq, sst_max_seq)`
on startup. The v3 SSTable footer stores `max_seq` for O(1) recovery.

---

## Tombstones & Deletion Semantics

Deletes in an LSM tree are not immediate erasures. Instead, a **tombstone**
marker is written:

```
  ValueEntry { seq: N, value: None }   ← this is a tombstone
```

**Why tombstones?** Because SSTables are immutable. You can't go back and
remove a key from an existing SSTable. Instead, the tombstone in a newer layer
**shadows** the older value:

```
  L0 SSTable (newer):  key="x", seq=5, value=None  (tombstone)
  L1 SSTable (older):  key="x", seq=2, value="hello"

  GET "x" → checks L0 first → finds tombstone → returns None
            (never reaches L1)
```

**Tombstone lifecycle**:
1. Written to Memtable on `DEL`
2. Flushed to SSTable with the Memtable
3. Preserved during compaction (to shadow older SSTables)
4. **Garbage collected** during full compaction when:
   - No older SSTables exist (all levels merged into one)
   - The Memtable doesn't reference the key

---

## Crash Safety Guarantees

| Scenario | What happens | Data safe? |
|----------|-------------|------------|
| Crash during SET (before WAL append) | Write lost | Yes (not acknowledged) |
| Crash during SET (after WAL, before Memtable) | WAL replayed on restart | Yes |
| Crash during flush (before rename) | `.sst.tmp` cleaned up on restart | Yes (WAL intact) |
| Crash during flush (after rename, before WAL truncate) | SSTable exists + WAL replayed (idempotent) | Yes |
| Crash during compaction | Old SSTables still exist, new `.tmp` cleaned up | Yes |
| Crash during manifest write | Atomic rename ensures old or new manifest | Yes |

**Key invariant**: Data is always recoverable from either the WAL or SSTables.
The WAL is only truncated **after** the SSTable is successfully written and the
manifest is updated.

---

## Test Coverage

```
  Crate      Tests   What's covered
  ─────      ─────   ──────────────
  bloom        17    Insert, lookup, false positives, serialization, edge cases
  memtable     43    Put, get, delete, seq gating, tombstones, iteration, size tracking
  wal          22    Append, replay, CRC verification, truncated tails, corruption
  sstable      21    Write, read, bloom integration, merge iterator, v1/v2/v3 compat
  engine       55    CRUD, flush, recovery, compaction, auto-compact, scan, manifest,
                     tombstone GC, force_flush, Drop, size limits, stress tests
  doctests      3    bloom, memtable, wal usage examples
  ─────────────────
  Total       161
```

All tests pass with zero warnings. CI runs `cargo fmt --check`, `cargo clippy`,
and `cargo test --workspace` on every push.
