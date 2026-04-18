# RiptideKV

**RiptideKV** is a learning project that builds a **Log-Structured Merge (LSM) key-value store** in Rust — and exposes it as a **Redis-compatible TCP server**. The goal is to understand storage engine internals by implementing them incrementally and correctly.

```
  redis-cli / Jedis / redis-py
          │  TCP  (RESP2)
          ▼
  ┌──────────────────────────────────┐
  │  crates/server  (Tokio async)    │
  │  RESP2 parser · 55+ commands     │
  └───────────────┬──────────────────┘
                  │  engine API
                  ▼
  ┌──────────────────────────────────┐
  │  crates/engine  (LSM tree)       │
  │  Memtable · WAL · SSTables       │
  │  Bloom Filters · Compaction      │
  └──────────────────────────────────┘
```

---

## Documentation

| Document | What it covers |
|----------|----------------|
| **[docs/HOWTORUN.md](docs/HOWTORUN.md)** | Build, run CLI, run server, connect clients (redis-cli / Java / Python), benchmarks, troubleshooting |
| **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** | Write/read/recovery data flows, file formats, RESP2 server design, concurrency model, trade-offs |
| **[docs/GUIDE.md](docs/GUIDE.md)** | Learning guide — WAL, Memtable, SSTables, Bloom Filters, Compaction, RESP2, Tokio, with pitfalls |

---

## Quick Start

```bash
# Build everything
cargo build --workspace

# ── Option A: Interactive CLI (no network)
cargo run -p cli

# ── Option B: RESP2 TCP Server (Redis-compatible)
cargo run -p server --bin riptidekv-server
# → RiptideKV listening on 0.0.0.0:6379

# Connect with redis-cli (in another terminal)
redis-cli PING          # PONG
redis-cli SET foo bar   # OK
redis-cli GET foo       # "bar"

# Run all Rust tests (245)
cargo test --workspace

# Run all Java tests (150)
mvn test -f java/pom.xml

# Run benchmarks
cargo bench -p cli      # engine-level benchmarks
cargo bench -p server   # TCP server benchmarks
```

---

## Embedding in a Java / Maven project

The `riptidekv-server` JAR bundles the native server binary for all supported
platforms.  Your code starts the server as a subprocess and connects to it
with any Redis client.

### Add the dependency

```xml
<!-- 1. GitHub Packages repository (requires a GitHub PAT with read:packages) -->
<repositories>
  <repository>
    <id>github</id>
    <url>https://maven.pkg.github.com/YOUR_GITHUB_USERNAME/RiptideKV</url>
  </repository>
</repositories>

<!-- 2. Dependency -->
<dependency>
  <groupId>io.github.YOUR_GITHUB_USERNAME</groupId>
  <artifactId>riptidekv-server</artifactId>
  <version>1.0.0</version>
</dependency>
```

> **GitHub Packages authentication** — add to `~/.m2/settings.xml`:
> ```xml
> <server>
>   <id>github</id>
>   <username>YOUR_GITHUB_USERNAME</username>
>   <password>YOUR_GITHUB_PAT</password>   <!-- PAT with read:packages -->
> </server>
> ```

### Start the embedded server

```java
import io.riptidekv.RiptideKVConfig;
import io.riptidekv.RiptideKVServer;
import redis.clients.jedis.Jedis;
import java.nio.file.Paths;

RiptideKVConfig config = RiptideKVConfig.builder()
    .bind("127.0.0.1:6379")
    .dataDir(Paths.get("/var/lib/myapp/rkv"))  // WAL + SSTables stored here
    .flushKb(4096)                              // flush at 4 MiB
    .walSync(true)                              // durable writes
    .build();

try (RiptideKVServer server = new RiptideKVServer(config)) {
    server.start();  // extracts binary, starts process, blocks until ready

    try (Jedis jedis = new Jedis("127.0.0.1", server.getPort())) {
        jedis.set("hello", "world");
        System.out.println(jedis.get("hello")); // world

        jedis.setex("session:abc", 3600, "user_data");
        System.out.println(jedis.ttl("session:abc")); // ~3600
    }
} // server.close() sends SIGTERM, flushes memtable, exits cleanly
```

### Supported platforms

| Platform | Architecture |
|----------|-------------|
| Linux | x86_64, aarch64 |
| macOS | x86_64 (Intel), aarch64 (Apple Silicon) |
| Windows | x86_64 |

---

## Project Structure

```
RiptideKV/
├── ARCHITECTURE.md          # Legacy architecture overview (see docs/ for full version)
├── Cargo.toml               # Workspace root (resolver = "2")
├── docs/
│   ├── ARCHITECTURE.md      # Full system design — data flows, file formats, trade-offs
│   ├── GUIDE.md             # Linear learning guide — concepts, code refs, pitfalls
│   └── HOWTORUN.md          # Build, CLI, server, clients, benchmarks, troubleshooting
├── java/                    # Maven module — Java embedding library
│   ├── pom.xml              #   Published to GitHub Packages as riptidekv-server
│   └── src/
│       ├── main/java/io/riptidekv/
│       │   ├── RiptideKVConfig.java   # Fluent config builder (bind, dataDir, flushKb, walSync)
│       │   └── RiptideKVServer.java   # Extracts native binary + manages server subprocess
│       └── test/java/io/riptidekv/
│           ├── RespClient.java        # Minimal RESP2 client for tests
│           ├── RiptideKVConfigTest.java   # 20 config unit tests
│           ├── RiptideKVServerTest.java   # 14 lifecycle tests
│           └── RespCommandsTest.java      # 147 end-to-end command tests
└── crates/
    ├── bloom/               # Bloom filter  (17 tests)
    │   └── src/lib.rs       #   BloomFilter, FNV-1a double-hashing, serialization
    ├── memtable/            # In-memory sorted write buffer  (43 tests)
    │   └── src/lib.rs       #   Memtable (BTreeMap), sequence-gated writes, tombstones
    ├── wal/                 # Write-Ahead Log  (22 tests)
    │   └── src/lib.rs       #   WalWriter, WalReader, CRC32 per record
    ├── sstable/             # Immutable on-disk sorted tables  (21 tests)
    │   └── src/
    │       ├── format.rs    #   v1/v2/v3 footer layout, magic numbers
    │       ├── writer.rs    #   Atomic write (tmp → fsync → rename)
    │       ├── reader.rs    #   Bloom-filtered point lookup + CRC32 verify
    │       └── merge.rs     #   MergeIterator (min-heap k-way merge)
    ├── engine/              # Storage engine orchestrator  (55 tests)
    │   └── src/
    │       ├── lib.rs       #   Engine struct, constructor, public accessors
    │       ├── write.rs     #   set(), del(), flush(), auto-compaction trigger
    │       ├── read.rs      #   get(), scan()
    │       ├── compaction.rs#   compact(), tombstone GC
    │       ├── recovery.rs  #   WAL replay, SSTable loading, tmp cleanup
    │       └── manifest.rs  #   Persistent L0/L1 level tracking (atomic writes)
    ├── server/              # Async RESP2 TCP server  (84 integration tests)
    │   ├── src/
    │   │   ├── lib.rs       #   serve() — public library API (testable without subprocess)
    │   │   ├── main.rs      #   Binary entry point — env-var config + graceful shutdown
    │   │   ├── resp.rs      #   RESP2 parser (non-recursive) + response serializer
    │   │   ├── db.rs        #   SharedDb: Arc<RwLock<Engine>> + volatile TTL map
    │   │   └── handler.rs   #   55+ command dispatcher, per-connection state
    │   ├── benches/
    │   │   └── server_bench.rs  # Criterion: PING, SET, GET, pipeline, MSET throughput
    │   └── tests/
    │       └── integration.rs   # 84 end-to-end tests over real TCP sockets
    └── cli/                 # Interactive REPL + engine-level benchmarks
        ├── src/main.rs      #   SET/GET/DEL/SCAN/COMPACT/FLUSH/STATS REPL
        ├── benches/         #   Criterion: memtable, sstable, wal, engine benchmarks
        └── tests/           #   CLI integration tests
```

**Dependency graph** (arrows = "depends on"):

```
cli ──────────────────────────────────────► engine
server ───────────────────────────────────► engine
engine ──► memtable
engine ──► wal
engine ──► sstable ──► bloom
```

---

## How It Works

### Write Path

```
Client SET k v
  │
  ├─ 1. seq += 1
  ├─ 2. WAL.append(Put{seq, k, v})   — durable on disk
  ├─ 3. memtable.put(k, v, seq)      — fast in-memory
  └─ 4. if memtable.size >= threshold:
           flush to SSTable → truncate WAL → maybe compact
```

### Read Path

```
Client GET k
  │
  ├─ 1. memtable.get(k)           — newest, no disk I/O
  ├─ 2. L0 SSTables, newest first — bloom → index → disk read
  └─ 3. L1 SSTable                — bloom → index → disk read
         First hit (value or tombstone) wins.
```

### Recovery (on startup)

```
cleanup .sst.tmp → replay WAL → load Manifest → open SSTables → ready
```

---

## Supported Commands (Server)

```
Connection:   PING  ECHO  SELECT  QUIT  HELLO  CLIENT  INFO  CONFIG  COMMAND
Database:     DBSIZE  FLUSHDB  FLUSHALL  ACL  SLOWLOG  MEMORY  WAIT
Strings:      GET  SET  SETNX  SETEX  PSETEX  GETSET  GETDEL  GETEX
              MGET  MSET  MSETNX  APPEND  STRLEN
              INCR  INCRBY  INCRBYFLOAT  DECR  DECRBY  GETRANGE  SETRANGE
Keys:         DEL  UNLINK  EXISTS  TYPE  RENAME  RENAMENX  RANDOMKEY  TOUCH
              EXPIRE  PEXPIRE  EXPIREAT  PEXPIREAT  TTL  PTTL  PERSIST
              EXPIRETIME  PEXPIRETIME  KEYS  SCAN
```

---

## Glossary

| Term | Definition |
|------|-----------|
| **LSM** | Log-Structured Merge tree; a write-optimized storage structure |
| **Memtable** | In-memory sorted buffer holding recent writes |
| **SSTable** | Sorted String Table; immutable on-disk sorted key-value file |
| **WAL** | Write-Ahead Log; append-only file for crash recovery |
| **Compaction** | Merging SSTables to remove duplicates and reclaim space |
| **Tombstone** | A deletion marker — shadows older values in SSTables |
| **Bloom Filter** | Probabilistic structure for fast "definitely not in set" checks |
| **L0** | Level 0; SSTables from memtable flushes (may key-overlap) |
| **L1** | Level 1; single post-compaction SSTable (non-overlapping) |
| **Manifest** | Text file tracking which SSTable belongs to which level |
| **RESP2** | Redis Serialization Protocol v2 — the Redis wire format |

---

## Development Phases

| Phase | Status | Description |
|-------|--------|-------------|
| 0 | ✅ | Rust workspace, CI, clippy, rustfmt |
| 1 | ✅ | Memtable, WAL (CRC32), SSTable v1, CLI (SET/GET/DEL) |
| 2 | ✅ | Read path (Memtable→L0→L1), Bloom filters, Compaction |
| 3 | ✅ | SSTable v3 (CRC32 per record, max_seq), Manifest, streaming compaction, range scan, auto-compaction, tombstone GC |
| 4 | ✅ | RESP2 TCP server (Tokio), 55+ commands, TTL, Java/Python client compatibility, 84 integration tests |
| 5 | 📋 | Persistent TTL, tiered compaction, LRU block cache, compression, metrics |

---

## Known Limitations

RiptideKV is a learning project. The following are known differences from production Redis:

| Area | Behaviour | Note |
|------|-----------|------|
| **TTL persistence** | TTLs are stored in memory only — lost on server restart | Keys survive but their expiry times do not; planned in Phase 5 |
| **INCR on non-numeric** | Treats un-parseable values as `0` instead of returning an error | Intentional graceful degradation; differs from Redis |
| **Authentication** | No `AUTH` command — any client can connect | Bind to loopback (`127.0.0.1`) in production |
| **TLS** | Plaintext TCP only | Terminate TLS at a proxy (nginx, HAProxy) if needed |
| **Replication** | Single node only — no leader/follower | `WAIT` always returns 0 |
| **Compaction** | Only L0 → L1; L1 grows unboundedly | Tiered/levelled compaction planned in Phase 5 |
| **Block cache** | Every SSTable read goes to disk | LRU block cache planned in Phase 5 |
| **Linux aarch64** | Not in CI build matrix — binary is optional | Add cross-compilation to CI matrix to enable |
| **RESP3** | Not supported — returns `NOPROTO` error | Use RESP2 clients |

---

## Test Summary

| Crate | Tests | Coverage |
|-------|-------|----------|
| `bloom` | 17 | Insert, lookup, FP rate, serialization, edge cases |
| `memtable` | 43 | CRUD, seq gating, tombstones, iteration, size tracking |
| `wal` | 22 | Append, replay, CRC, truncated tails, corruption |
| `sstable` | 21 | Write, read, bloom, merge iterator, v1/v2/v3 compat |
| `engine` | 55 | CRUD, flush, recovery, compaction, scan, manifest, GC |
| `server` | 84 | All 55+ commands, TTL expiry, concurrent clients, pipelining, binary values |
| doctests | 3 | Usage examples for bloom, memtable, wal |
| **Total (Rust)** | **245** | |

**Java embedding library** (`mvn test -f java/pom.xml`):

| Test class | Tests | Coverage |
|---|---|---|
| `RiptideKVConfigTest` | 20 | Builder defaults, validation (null, blank, no colon, non-numeric port, out-of-range), port extraction, fluency |
| `RiptideKVServerTest` | 14 | start, stop, isRunning, close idempotency, port release, null config guard |
| `RespCommandsTest` | 116 | All 55+ commands over real TCP: Connection, Database, Strings, Keys, real-time expiry, pipelining, concurrent clients, binary safety |
| **Total (Java)** | **150** | |

CI: `cargo fmt --check` + `cargo clippy` + `cargo test --workspace` (245) + `mvn test -f java/pom.xml` (150)
