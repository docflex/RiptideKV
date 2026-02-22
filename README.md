# RiptideKV

**RiptideKV** is a learning project to build a **Log-Structured Merge (LSM) key-value store** in Rust.
The goal is to understand storage engine internals by implementing them incrementally and correctly, not to ship a production database.

```
  Client ──► CLI ──► Engine ──┬── Memtable  (in-memory sorted buffer)
                              ├── WAL       (crash-safe append-only log)
                              └── SSTables  (immutable on-disk sorted files)
                                    └── Bloom Filters (fast negative lookups)
```

> **For the full architecture with ASCII diagrams, data flow, and per-crate
> deep dives, see [`ARCHITECTURE.md`](ARCHITECTURE.md).**

---

## Quick Start

```bash
# Build everything
cargo build --workspace

# Run the interactive CLI
cargo run -p cli

# Run all 161 tests
cargo test --workspace

# Run benchmarks
cargo bench -p cli
```

### CLI Usage

```
RiptideKV started (seq=0, wal=wal.log, sst_dir=data/sst, flush=1024KiB, l0_trigger=4)
Commands: SET key value | GET key | DEL key | SCAN [start] [end]
          COMPACT | FLUSH | STATS | EXIT
> SET name Alice
OK
> GET name
Alice
> DEL name
OK
> GET name
(nil)
```

### Configuration (Environment Variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `RIPTIDE_WAL_PATH` | `wal.log` | WAL file path |
| `RIPTIDE_SST_DIR` | `data/sst` | SSTable directory |
| `RIPTIDE_FLUSH_KB` | `1024` | Flush threshold in KiB (1024 = 1 MiB) |
| `RIPTIDE_WAL_SYNC` | `true` | fsync every WAL append |
| `RIPTIDE_L0_TRIGGER` | `4` | Auto-compaction trigger (0 = disabled) |

---

## Project Structure

```
RiptideKV/
├── ARCHITECTURE.md          # Detailed architecture documentation
├── Cargo.toml               # Workspace root
└── crates/
    ├── bloom/               # Probabilistic set membership (17 tests)
    ├── memtable/            # In-memory sorted write buffer (43 tests)
    ├── wal/                 # Write-Ahead Log for durability (22 tests)
    ├── sstable/             # Immutable on-disk sorted tables (21 tests)
    │   ├── reader.rs        #   Read + bloom check + CRC verify
    │   ├── writer.rs        #   Atomic write (tmp + rename)
    │   ├── merge.rs         #   Min-heap merge iterator
    │   └── format.rs        #   Magic numbers, footer sizes
    ├── engine/              # Storage engine orchestrator (55 tests)
    │   ├── lib.rs           #   Engine struct, constructor, accessors
    │   ├── write.rs         #   set(), del(), flush()
    │   ├── read.rs          #   get(), scan()
    │   ├── compaction.rs    #   compact(), tombstone GC
    │   ├── recovery.rs      #   WAL replay, SSTable loading
    │   ├── manifest.rs      #   Persistent L0/L1 level tracking
    │   └── tests/           #   Split into 4 focused test modules
    └── cli/                 #   Interactive REPL + benchmarks
```

**Dependency graph**: `cli → engine → {memtable, wal, sstable → bloom}`

---

## How It Works

### Write Path

1. Increment monotonic sequence number
2. Append record to WAL (durability)
3. Insert into Memtable (fast reads)
4. If Memtable exceeds threshold → flush to SSTable, truncate WAL

### Read Path

1. Check **Memtable** (freshest data)
2. Check **L0 SSTables** newest-first (bloom filter → index → disk read)
3. Check **L1 SSTables** newest-first
4. First match wins; tombstones shadow older values

### Compaction

Merges all L0 + L1 SSTables into a single L1 SSTable using a streaming
min-heap merge. Tombstones for keys with no older references are garbage
collected. Auto-triggers when L0 count reaches the configured threshold.

### Recovery

On startup: replay WAL → rebuild Memtable, load MANIFEST → assign SSTables
to L0/L1, recover sequence number from v3 footer (`max_seq`).

---

## Goals

- Learn Rust fundamentals in a systems programming context
- Incrementally build an LSM-style storage engine
- Practice testing, CI, and clean architecture

## Non-Goals (for now)

- Production-grade performance
- Distributed systems or consensus
- Concurrent read/write (currently single-threaded `&mut self`)

## Glossary

| Term | Definition |
|------|-----------|
| **LSM** | Log-Structured Merge tree; a write-optimized storage structure |
| **Memtable** | In-memory sorted buffer holding recent writes |
| **SSTable** | Sorted String Table; immutable on-disk sorted key-value file |
| **WAL** | Write-Ahead Log; append-only file for crash recovery |
| **Compaction** | Merging SSTables to remove duplicates and reclaim space |
| **Tombstone** | Marker indicating a key has been deleted |
| **Bloom Filter** | Probabilistic structure for fast "definitely not in set" checks |
| **L0** | Level 0; SSTables from memtable flushes (may overlap) |
| **L1** | Level 1; SSTables from compaction (non-overlapping) |
| **Manifest** | Text file tracking which SSTable belongs to which level |

---

## Development Phases

### Phase 0 — Rust fundamentals & repository setup [DELIVERED]

- Cargo workspace, CI (GitHub Actions), clippy, rustfmt
- Rust fundamentals: ownership, borrowing, traits, `Result`/`Option`

### Phase 1 — Core LSM (in-memory + basic on-disk) [DELIVERED]

- Ordered memtable with sequence-gated writes
- WAL with CRC32 per record, crash-safe replay
- SSTable v1 writer/reader with sparse index
- CLI with SET, GET, DEL

#### Write Path Demonstrations

| Demo | Description |
|------|-------------|
| ![Memtable → WAL](public/assets/memtable_wal.gif) | Writing to Memtable, then WAL |
| ![Flush to SSTable](public/assets/flush_to_sstable.gif) | Threshold exceeded → SSTable created → WAL flushed |
| ![New Memtable](public/assets/new_memtable_after_flush.gif) | New writes after flush |
| ![Delete](public/assets/delete_propagation.gif) | Deletions propagated via tombstones |

### Phase 2 — Reads, bloom filters, and compaction [DELIVERED]

- Read path: Memtable → L0 → L1 with bloom filter short-circuit
- Per-SSTable bloom filters (1% FPR, FNV-1a double hashing)
- Basic compaction: merge multiple SSTables, drop obsolete keys
- SSTable v2: bloom filter section in file layout

### Phase 3 — Robustness and production readiness [DELIVERED]

- **SSTable v3**: per-record CRC32 checksums, `max_seq` in footer
- **Manifest**: persistent L0/L1 tracking with atomic writes
- **Streaming compaction**: `write_from_iterator()` — bounded RAM usage
- **Range scan**: `Engine::scan(start, end)` merging all sources
- **Auto-compaction**: triggers when L0 count >= configurable threshold
- **Tombstone GC**: drops dead tombstones during full compaction
- **Graceful shutdown**: `Drop` impl flushes memtable, `force_flush()` API
- **CLI improvements**: env-var config, SCAN/COMPACT/FLUSH/STATS commands
- **SRP refactor**: engine split into 5 focused modules + 4 test modules
- **161 tests**, zero warnings

### Phase 4 — RESP server & Java compatibility (planned)

- RESP2 protocol server (GET, SET, DEL, PING, INFO)
- Async networking with Tokio
- Integration tests with Java Redis client (Jedis)

### Phase 5 — Performance, features, and polish (planned)

- Benchmarks and tuning (criterion)
- Optional: TTL, leveled compaction, compression, LRU block cache
- Structured logging (`tracing`), metrics, fuzzing

---

## Test Summary

| Crate | Tests | Coverage |
|-------|-------|----------|
| `bloom` | 17 | Insert, lookup, FP rate, serialization, edge cases |
| `memtable` | 43 | CRUD, seq gating, tombstones, iteration, size tracking |
| `wal` | 22 | Append, replay, CRC, truncated tails, corruption |
| `sstable` | 21 | Write, read, bloom, merge iterator, v1/v2/v3 compat |
| `engine` | 55 | CRUD, flush, recovery, compaction, scan, manifest, GC |
| doctests | 3 | Usage examples for bloom, memtable, wal |
| **Total** | **161** | |

CI: `cargo fmt --check` + `cargo clippy` + `cargo test --workspace`
