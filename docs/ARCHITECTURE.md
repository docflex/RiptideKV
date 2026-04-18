# RiptideKV — Architecture Reference

> **Who is this for?**  
> A new engineer joining the project, or anyone wanting to understand exactly how
> every byte flows through the system.  No prior database internals knowledge
> required — concepts are introduced before code is referenced.

---

## Table of Contents

1. [Big Picture](#1-big-picture)
2. [Crate Map](#2-crate-map)
3. [Write Path — Step by Step](#3-write-path--step-by-step)
4. [Read Path — Step by Step](#4-read-path--step-by-step)
5. [Flush — Memtable → SSTable](#5-flush--memtable--sstable)
6. [Compaction — Merging SSTables](#6-compaction--merging-sstables)
7. [Recovery — Surviving a Crash](#7-recovery--surviving-a-crash)
8. [File Formats](#8-file-formats)
   - [WAL Record](#81-wal-record)
   - [SSTable (v3)](#82-sstable-v3)
   - [Manifest](#83-manifest)
9. [RESP2 Server Architecture](#9-resp2-server-architecture)
10. [Concurrency Model](#10-concurrency-model)
11. [Configuration Surface](#11-configuration-surface)
12. [Key Design Decisions & Trade-offs](#12-key-design-decisions--trade-offs)

---

## 1. Big Picture

RiptideKV is a **Log-Structured Merge (LSM) tree** key-value store exposed
over the **Redis Serialization Protocol (RESP2)**. Its architecture mirrors
the storage engines inside systems like LevelDB and RocksDB, but at a scale
that makes every piece easy to read and understand.

```
┌──────────────────────────────────────────────────────────────────┐
│                         Clients                                  │
│   redis-cli  /  Jedis (Java)  /  lettuce  /  telnet              │
└────────────────────────────┬─────────────────────────────────────┘
                             │  TCP (RESP2)
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                     crates/server                                │
│  TcpListener  ──►  per-connection Tokio task                     │
│                    RespReader  +  handler::dispatch()            │
│                    SharedDb (Arc<RwLock<DbState>>)               │
└────────────────────────────┬─────────────────────────────────────┘
                             │  engine API  (set / get / del / scan)
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                      crates/engine                               │
│                                                                  │
│   ┌──────────┐   WAL append    ┌────────────┐                   │
│   │ Memtable │ ◄──────────────  │    WAL     │                   │
│   │ (BTreeMap│   (durability)   │ (wal.log)  │                   │
│   │  sorted) │                 └────────────┘                   │
│   └────┬─────┘                                                   │
│        │ flush (threshold exceeded)                              │
│        ▼                                                         │
│   ┌──────────────────────────────────────────────────────┐      │
│   │                  SSTables on disk                    │      │
│   │   L0  (fresh flushes, may overlap in key range)      │      │
│   │   L1  (post-compaction, non-overlapping, one file)   │      │
│   └──────────────────────────────────────────────────────┘      │
│        │                                                         │
│        │ per-file index                                          │
│   ┌────┴──────────┐                                             │
│   │  Bloom Filter │  (fast "definitely not here" checks)        │
│   └───────────────┘                                             │
│        │                                                         │
│   ┌────┴──────────┐                                             │
│   │   Manifest    │  (which .sst file is L0 vs L1)              │
│   └───────────────┘                                             │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Crate Map

```
RiptideKV/
├── Cargo.toml          workspace root (resolver = "2")
└── crates/
    ├── bloom/          Probabilistic membership test (Bloom filter)
    │   └── src/lib.rs  BloomFilter struct, FNV-1a hashing
    │
    ├── memtable/       In-memory sorted write buffer
    │   └── src/lib.rs  Memtable, ValueEntry, sequence-gated writes
    │
    ├── wal/            Write-Ahead Log (durability)
    │   └── src/lib.rs  WalWriter, WalReader, WalRecord, CRC32
    │
    ├── sstable/        Immutable on-disk sorted tables
    │   └── src/
    │       ├── format.rs   File layout constants, footer read/write
    │       ├── writer.rs   SSTableWriter (atomic tmp+rename)
    │       ├── reader.rs   SSTableReader (index, bloom, CRC verify)
    │       └── merge.rs    MergeIterator (min-heap over N readers)
    │
    ├── engine/         Storage engine — orchestrates all of the above
    │   └── src/
    │       ├── lib.rs          Engine struct, constructor, accessors
    │       ├── write.rs        set(), del(), flush(), auto-compaction
    │       ├── read.rs         get(), scan()
    │       ├── compaction.rs   compact(), tombstone GC
    │       ├── recovery.rs     WAL replay, SSTable loading, tmp cleanup
    │       └── manifest.rs     Persistent L0/L1 level tracking
    │
    ├── server/         Async RESP2 TCP server (Tokio)
    │   └── src/
    │       ├── lib.rs      serve() — public library API
    │       ├── main.rs     Binary entry point (config from env vars)
    │       ├── resp.rs     RESP2 parser + serializer
    │       ├── db.rs       SharedDb — Engine + volatile TTL map
    │       └── handler.rs  Command dispatcher (55+ commands)
    │
    └── cli/            Interactive REPL + criterion benchmarks
        └── src/main.rs  SET/GET/DEL/SCAN/COMPACT/FLUSH/STATS REPL
```

**Dependency graph** (arrows = "depends on"):

```
cli ──────────────────────────────────────────► engine
server ───────────────────────────────────────► engine
engine ──► memtable
engine ──► wal
engine ──► sstable ──► bloom
```

Nothing depends on `server` or `cli`.  The storage engine is completely
independent and can be embedded in any Rust application.

---

## 3. Write Path — Step by Step

When a client sends `SET mykey myvalue`:

```
Client                 Server                  Engine
  │                      │                       │
  │─── *3\r\n           │                       │
  │─── $3\r\nSET\r\n    │                       │
  │─── $5\r\nmykey\r\n  │                       │
  │─── $7\r\nmyvalue\r\n│                       │
  │                      │                       │
  │            RespReader.read_value()           │
  │            dispatcher → cmd_set()           │
  │                      │                       │
  │              acquire RwLock write           │
  │                      │──── engine.set(k,v) ─►│
  │                      │                       │ seq += 1
  │                      │                       │ WAL.append(Put{seq,k,v})
  │                      │                       │   └─ write to wal.log
  │                      │                       │   └─ fsync (if wal_sync=true)
  │                      │                       │ memtable.put(k, v, seq)
  │                      │                       │ if mem.approx_size >= threshold:
  │                      │                       │   flush() ──► new .sst file
  │                      │                       │              truncate WAL
  │                      │                       │              fsync WAL truncation
  │                      │                       │              maybe compact()
  │              release RwLock                  │
  │                      │                       │
  │◄─── +OK\r\n          │                       │
```

**Code references:**
- WAL append: `crates/wal/src/lib.rs` → `WalWriter::append()`
- Memtable insert: `crates/memtable/src/lib.rs` → `Memtable::put()`
- Flush trigger: `crates/engine/src/write.rs` → `Engine::flush()`
- Command handler: `crates/server/src/handler.rs` → `cmd_set()`

### Why write to WAL before Memtable?

If the process crashes _after_ the WAL write but _before_ the Memtable insert,
the data is still safe — it will be replayed from WAL on the next startup.
If we did it the other way around, a crash after the Memtable insert but before
the WAL write would silently lose data.

### Sequence Numbers

Every write increments a global `u64` sequence number stored in `Engine.seq`.
Sequence numbers serve two purposes:

1. **Stale-write protection** in the Memtable: if you try to write a key with a
   sequence number older than the one already in the Memtable, the write is
   silently dropped (prevents WAL replay from overwriting newer data).

2. **Version resolution** during compaction: when two SSTables have the same
   key, the one with the higher sequence number wins.

---

## 4. Read Path — Step by Step

When a client sends `GET mykey`:

```
Engine.get("mykey")
  │
  ├── 1. Check Memtable
  │       memtable.get_entry("mykey")
  │       If found (value OR tombstone) → return immediately
  │       (Memtable always has the freshest data)
  │
  ├── 2. Check L0 SSTables (newest first)
  │       for sst in l0_sstables.iter().rev():
  │           bloom_filter.check("mykey")  ──►  "definitely not here" → skip
  │           index.binary_search("mykey") ──►  offset in file
  │           file.seek(offset)
  │           read record → verify CRC32 (v3 only)
  │           if found (value OR tombstone) → return
  │
  └── 3. Check L1 SSTables (newest first)
          same process as L0
          if nothing found → return None
```

**Key insight — tombstones:** A `DEL` operation writes a special "tombstone"
record (a key with `value = None`). When the read path encounters a tombstone
in any layer, it immediately returns `None` — the key is deleted. This means
you don't need to update or remove older SSTable entries on every delete;
the tombstone acts as a shadow.

**Code references:**
- `crates/engine/src/read.rs` → `Engine::get()`
- `crates/sstable/src/reader.rs` → `SSTableReader::get()` (bloom + index + CRC)
- `crates/memtable/src/lib.rs` → `Memtable::get_entry()`

### Bloom Filters

Each SSTable has an embedded Bloom filter. Before doing any disk I/O, the
reader checks the filter. If the filter says "definitely not in this file",
the reader skips that SSTable entirely — no disk read at all.

The filter uses **FNV-1a double hashing** (two independent hash functions
derived from one) and is sized for a **1% false-positive rate**. This means
1 in 100 "not present" queries will still do a disk seek unnecessarily, but
99% are saved.

A false positive means "the filter says the key might be here, but it's
actually not". The code then does a full disk read and returns `None` after
reading the record.  A false negative (filter says not here when it is) is
impossible.

---

## 5. Flush — Memtable → SSTable

When the Memtable's approximate size exceeds `flush_threshold`:

```
Engine::flush()
  │
  ├── SSTableWriter::write_from_memtable(tmp_path, &memtable)
  │     ├── iterate memtable in key order (BTreeMap is sorted)
  │     ├── for each entry:
  │     │     write key_len(u32) + key + seq(u64) + present(u8) + [val_len + val]
  │     │     compute CRC32 over this record → write to data section
  │     │     update bloom filter
  │     │     record (key, file_offset) in sparse index
  │     ├── write bloom filter section
  │     ├── write sparse index section
  │     └── write v3 footer (magic, offsets, max_seq, version)
  │
  ├── fsync the tmp file
  ├── rename(tmp_path, final_path)  ← atomic on POSIX
  ├── manifest.add(filename, level=0)
  ├── manifest.save()  ← atomic write+fsync+rename
  ├── truncate WAL to 0 bytes
  ├── fsync WAL truncation
  └── memtable.clear()
```

**Why tmp + rename?**  
A file rename on POSIX systems is atomic — either the old name or the new name
is visible, never a half-written state. This means a crash mid-write leaves
the `.sst.tmp` file, which is cleaned up on the next startup by
`cleanup_tmp_files()`. The SSTable is never visible to readers until the rename.

**Sparse index** — not every key is indexed, only one entry per N bytes
(based on a configurable block size). A lookup binary-searches the index for
the largest key ≤ the target, then scans forward from that offset. This keeps
the index small (fits in memory) while bounding the scan distance.

---

## 6. Compaction — Merging SSTables

Without compaction, L0 SSTable count grows without bound, making reads
progressively slower (more files to check). Compaction merges all L0 + L1
SSTables into a single new L1 SSTable.

```
Engine::compact()
  │
  ├── open SSTableReader for every L0 + L1 SSTable
  ├── create MergeIterator over all readers
  │     MergeIterator uses a BinaryHeap (min-heap)
  │     Each "slot" in the heap holds the current key from one reader
  │     Pop the smallest key → if duplicate keys across files, keep highest seq
  │
  ├── SSTableWriter::write_from_iterator(tmp_path, iter, &memtable)
  │     For each (key, entry) from the iterator:
  │       Skip tombstones where the key is NOT in the memtable
  │       (tombstone GC: dead tombstones can be dropped permanently)
  │       Otherwise write the entry to the new SSTable
  │
  ├── If the iterator produced no output (all keys were tombstone-GC'd):
  │     delete all old SSTable files
  │     clear manifest
  │     return
  │
  ├── fsync + rename new SSTable
  ├── manifest.replace_all([new_filename], level=1)
  ├── manifest.save()
  ├── delete all old SSTable files
  └── update engine's l0_sstables + l1_sstables lists
```

### Tombstone Garbage Collection

A tombstone can only be safely deleted if there is no older copy of the key
in any SSTable we're _not_ compacting. Since we compact everything in one
pass, there is no older layer — so any tombstone whose key is not in the
current Memtable can be dropped permanently.

The condition in `compaction.rs`:
```rust
// crates/engine/src/compaction.rs
if entry.value.is_none() && !mem_ref.contains_key(&key) {
    continue; // GC: dead tombstone — drop it
}
```

If the Memtable _does_ contain the key, there's a newer write in flight that
references this key — we keep the tombstone to avoid resurrecting the old value.

---

## 7. Recovery — Surviving a Crash

On every `Engine::new()` call:

```
1. cleanup_tmp_files(sst_dir)
     Remove any *.sst.tmp files left by interrupted flushes.

2. replay_wal_and_build(wal_path, &mut memtable)
     Open wal.log in read mode.
     For each WalRecord:
       Put  → memtable.put(key, value, seq)   [seq gating prevents double-apply]
       Del  → memtable.delete(key, seq)
     Return the highest seq seen.
     Truncated tail (last record partially written) → silently ignored.
     CRC32 mismatch → error (genuine corruption).

3. Open WalWriter in append mode (after replay, to avoid races).

4. Manifest::load_or_create(sst_dir)
     Read MANIFEST file → parse "filename level" lines.
     Open SSTableReader for each L0 and L1 filename.
     Extract max_seq from each v3 footer.

5. seq = max(wal_seq, max_sst_seq)
     Ensures the next write gets a sequence number higher than anything
     that already exists on disk.
```

**Why is WAL replay done before opening the WalWriter?**  
Opening a file in append mode on some operating systems truncates the existing
content or changes the seek position. By replaying first (read mode), we avoid
interfering with the unread bytes.

**What about the manifest?**  
The manifest is written atomically (tmp + fsync + rename), so it is always
consistent. Either the pre-flush or post-flush manifest is visible, never a
partial state.

---

## 8. File Formats

### 8.1 WAL Record

```
┌─────────────────────────────────────────────────────────────────┐
│  tag (1 byte)   0x01 = Put,  0x02 = Del                        │
│  seq (8 bytes)  little-endian u64                               │
│  key_len (4 bytes) little-endian u32                            │
│  key (key_len bytes)                                            │
│  [if Put:]                                                      │
│    val_len (4 bytes) little-endian u32                          │
│    val (val_len bytes)                                          │
│  crc32 (4 bytes) little-endian u32                              │
│    (CRC covers: tag + seq + key_len + key + [val_len + val])    │
└─────────────────────────────────────────────────────────────────┘
```

A truncated tail (process crashed mid-write) is detected by EOF before the
CRC field — the reader silently stops and returns what it has so far.
A CRC mismatch on a complete record is a genuine corruption and returns an error.

### 8.2 SSTable (v3)

```
┌───────────────────────────────────────────┐
│              DATA SECTION                  │
│  ┌─────────────────────────────────────┐  │
│  │ Record 0                            │  │
│  │  key_len (u32 LE)                   │  │
│  │  key     (bytes)                    │  │
│  │  seq     (u64 LE)                   │  │
│  │  present (u8: 1=value, 0=tombstone) │  │
│  │  [if present=1:]                    │  │
│  │    val_len (u32 LE)                 │  │
│  │    val     (bytes)                  │  │
│  │  crc32   (u32 LE)  ← v3 only       │  │
│  └─────────────────────────────────────┘  │
│  ┌─────────────────────────────────────┐  │
│  │ Record 1 ...                        │  │
│  └─────────────────────────────────────┘  │
│              BLOOM SECTION                 │
│  bit array + metadata (see bloom crate)   │
│              INDEX SECTION                 │
│  for each sampled key:                    │
│    key_len (u32) + key (bytes)            │
│    file_offset (u64)                      │
│              FOOTER (v3 = 56 bytes)        │
│  magic     (8 bytes) "RIPTIDE1"           │
│  bloom_off (u64 LE)  offset of bloom sec  │
│  bloom_len (u64 LE)  length of bloom sec  │
│  index_off (u64 LE)  offset of index sec  │
│  index_len (u64 LE)  length of index sec  │
│  max_seq   (u64 LE)  highest seq in file  │  ← v3 addition
│  version   (u8)      3                    │
└───────────────────────────────────────────┘
```

The `max_seq` field allows recovery to find the highest sequence number without
reading every record — it reads only the last 56 bytes of each SSTable.

### 8.3 Manifest

A plain UTF-8 text file, one line per SSTable:

```
sst-0000000001-1704067200000.sst L0
sst-0000000042-1704068400000.sst L1
```

Written atomically: `MANIFEST.tmp` → fsync → rename to `MANIFEST`.

---

## 9. RESP2 Server Architecture

```
TCP connection accepted
        │
        ▼
  Tokio task spawned  (one per connection, cheap green threads)
        │
        ▼
  handler::handle_connection(stream, db)
        │
        ├─► RespReader::read_value()      parsing
        │     │
        │     ├── *N\r\n  ──► read N items via read_item()
        │     │               (non-recursive: avoids Box::pin overhead)
        │     └── inline ──► split on whitespace, wrap as Array
        │
        └─► dispatch(cmd, args)           command routing
              │
              ├── acquire Arc::clone(&db.state).write/read().await
              │   (clone the Arc first so the guard doesn't borrow `conn`)
              │
              ├── execute command logic (mutation or query)
              │
              ├── drop guard (end of scoped block)
              │
              └── write response bytes to OwnedWriteHalf
```

### Shared State

```rust
// crates/server/src/db.rs
pub struct SharedDb {
    pub state: Arc<RwLock<DbState>>,  // Engine + TTL map
    pub start_time: Instant,
    pub connected_clients: Arc<AtomicI64>,
    pub total_commands: Arc<AtomicU64>,
    pub total_connections: Arc<AtomicU64>,
}

pub struct DbState {
    pub engine: Engine,
    pub ttl: HashMap<Vec<u8>, Instant>,  // volatile: lost on restart
}
```

`SharedDb` is `Clone` (all fields are `Arc`-wrapped), so cloning it for each
connection task is O(1) — it just bumps reference counts.

### TTL Implementation

TTL is stored as a `HashMap<Vec<u8>, Instant>` in memory. This is intentionally
**volatile** — TTLs are lost on server restart. Keys with expired TTLs are
**lazily evicted**: the TTL is checked on every read/write operation for that
key. There is no background eviction task.

This is the same approach Redis takes internally (active expiry + lazy eviction),
though Redis also has a periodic background sweep that we haven't added yet.

### Borrow-Checker Pattern

A subtle Rust lifetime issue: the `RwLockWriteGuard` returned by
`db.state.write().await` borrows `db.state`, which is a field of `conn`, so
the guard effectively borrows `conn`. You then can't call `&mut conn` methods
while the guard is alive.

The solution: clone the `Arc` into a local variable before `await`-ing:

```rust
// crates/server/src/handler.rs — the state_write! macro expands to:
let guard = Arc::clone(&conn.db.state).write_owned().await;
// Now `guard` borrows the local Arc clone, not `conn`.
// `conn` is free for mutable calls after `guard` drops.
```

---

## 10. Concurrency Model

| Layer | Concurrency |
|-------|-------------|
| TCP accept | Single Tokio task calls `listener.accept()` in a loop |
| Per-connection | Each connection runs in its own `tokio::spawn`-ed task |
| Engine access | Single `Arc<RwLock<DbState>>` — multiple readers OR one writer |
| Engine internals | Single-threaded `&mut self` — no internal locking |
| Stats counters | `AtomicI64` / `AtomicU64` — lock-free |

**Read/Write lock contention:** All commands that mutate state (SET, DEL, INCR,
etc.) hold a write lock for the duration of the command. Read-only commands
(GET, MGET, TTL, KEYS, etc.) hold a read lock. This means:
- Multiple GETs from different clients run concurrently.
- A SET blocks all other readers and writers until it completes.

This is a **coarse-grained** locking strategy — acceptable for a learning
project but a known bottleneck for high write throughput at scale. A production
system would use finer-grained locking or lock-free data structures.

---

## 11. Configuration Surface

All configuration is via environment variables — no config file required.

| Variable | Default | Where used |
|----------|---------|-----------|
| `RIPTIDE_BIND` | `0.0.0.0:6379` | `server/src/main.rs` |
| `RIPTIDE_WAL_PATH` | `wal.log` | `engine/src/lib.rs` via main |
| `RIPTIDE_SST_DIR` | `data/sst` | `engine/src/lib.rs` via main |
| `RIPTIDE_FLUSH_KB` | `1024` | `engine/src/write.rs` |
| `RIPTIDE_WAL_SYNC` | `true` | `wal/src/lib.rs` |
| `RIPTIDE_L0_TRIGGER` | `4` | `engine/src/write.rs` (CLI only) |

`RIPTIDE_L0_TRIGGER` is only exposed by the CLI binary; the server uses the
engine's default (`DEFAULT_L0_COMPACTION_TRIGGER = 4`). To change it from the
server, call `engine.set_l0_compaction_trigger(n)` programmatically.

---

## 12. Key Design Decisions & Trade-offs

### Decision: LSM tree over B-tree

LSM trees turn random writes into sequential appends. This makes writes very
fast (especially when WAL sync is enabled — only one sequential write per
operation). The trade-off is read amplification: a key might exist in the
Memtable, multiple L0 SSTables, and the L1 SSTable, so a read might have to
check many places.

### Decision: Single-level L1 compaction

We compact everything into a single L1 SSTable. This means:
- After compaction, there is exactly one L1 file.
- A key is guaranteed to appear at most once in L1.
- Reads only need to check one file at each level.

A production LSM (like RocksDB) has multiple levels (L1, L2, L3...) with size
amplification limits, which gives better space usage. Our single-file approach
is simpler to reason about.

### Decision: RESP2 over custom protocol

Using RESP2 means any existing Redis client library works with RiptideKV
out of the box — no custom client needed. Java (Jedis), Python (redis-py),
Go (go-redis), and redis-cli all speak RESP2.

### Decision: Volatile TTL (in-memory only)

Storing TTLs in a `HashMap<Vec<u8>, Instant>` is fast and simple but loses
TTLs on restart. A production system would persist expiry timestamps in the
WAL (as a special record type) or in SSTable metadata. We chose simplicity here.

### Decision: `Engine` takes `&mut self` (single-threaded)

The storage engine is inherently sequential — WAL appends must be ordered,
Memtable mutations are single-threaded, and compaction reads/writes files.
By using `&mut self`, we get compile-time exclusivity: only one caller can
mutate the engine at a time, which the `Arc<RwLock<>>` enforces at the server
level.
