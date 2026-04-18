# RiptideKV — How To Run

Everything you need to build, run, test, and benchmark RiptideKV.

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Rust toolchain | 1.75+ | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| Cargo | ships with Rust | — |
| `redis-cli` (optional) | any | `brew install redis` (macOS) or `apt install redis-tools` |

Check your Rust version:

```bash
rustc --version   # should print 1.75.0 or newer
cargo --version
```

---

## 1 — Build

```bash
# Clone the repository
git clone <repo-url> RiptideKV
cd RiptideKV

# Build all crates in debug mode (fast compile, slower runtime)
cargo build --workspace

# Build in release mode (slow compile, fast runtime — use for benchmarks)
cargo build --workspace --release
```

Compiled binaries end up in:
```
target/debug/riptidekv-server    (server binary, debug)
target/debug/cli                 (CLI REPL, debug)
target/release/riptidekv-server  (server binary, release)
target/release/cli               (CLI REPL, release)
```

---

## 2 — Interactive CLI (REPL)

The CLI lets you interact with the storage engine directly — no network stack,
no RESP protocol, just raw engine calls.

```bash
cargo run -p cli
```

You will see a startup message, then a `>` prompt:

```
RiptideKV started (seq=0, wal=wal.log, sst_dir=data/sst, flush=1024KiB, l0_trigger=4)
Commands: SET key value | GET key | DEL key | SCAN [start] [end]
          COMPACT | FLUSH | STATS | EXIT
>
```

### CLI Commands

| Command | Description | Example |
|---------|-------------|---------|
| `SET key value` | Store a key-value pair | `SET name Alice` |
| `GET key` | Retrieve a value | `GET name` → `Alice` |
| `DEL key` | Delete a key | `DEL name` |
| `SCAN [start [end]]` | List keys in range | `SCAN a z` or `SCAN` (all) |
| `FLUSH` | Flush Memtable to SSTable now | `FLUSH` |
| `COMPACT` | Merge all SSTables | `COMPACT` |
| `STATS` | Show engine statistics | `STATS` |
| `EXIT` | Exit the REPL | `EXIT` |

### CLI Session Example

```
> SET user:1 Alice
OK
> SET user:2 Bob
OK
> GET user:1
Alice
> SCAN user: user;
user:1 → Alice
user:2 → Bob
> STATS
seq=2, memtable_keys=2, l0_sstables=0, l1_sstables=0, approx_mem=~0 KiB
> DEL user:1
OK
> GET user:1
(nil)
> COMPACT
Compacted.
> EXIT
```

### CLI Configuration (Environment Variables)

```bash
RIPTIDE_WAL_PATH=./mywal.log   \
RIPTIDE_SST_DIR=./my-sst       \
RIPTIDE_FLUSH_KB=512           \
RIPTIDE_WAL_SYNC=true          \
RIPTIDE_L0_TRIGGER=4           \
cargo run -p cli
```

| Variable | Default | Description |
|----------|---------|-------------|
| `RIPTIDE_WAL_PATH` | `wal.log` | Path to the WAL file |
| `RIPTIDE_SST_DIR` | `data/sst` | Directory for SSTable files |
| `RIPTIDE_FLUSH_KB` | `1024` | Memtable flush threshold in KiB (1024 = 1 MiB) |
| `RIPTIDE_WAL_SYNC` | `true` | `fsync` after each WAL write (`true`/`false`) |
| `RIPTIDE_L0_TRIGGER` | `4` | Auto-compact when L0 SSTable count reaches this |

---

## 3 — RESP2 Server

The server exposes the storage engine over TCP using the Redis Serialization
Protocol (RESP2). Any Redis client library can connect to it.

### Start the Server

```bash
# Default: binds to 0.0.0.0:6379, writes to ./wal.log and ./data/sst
cargo run -p server --bin riptidekv-server

# Or with release optimizations (recommended for real use):
cargo run -p server --bin riptidekv-server --release
```

On startup you will see:
```
2024-01-01T00:00:00.000000Z  INFO server: RiptideKV listening on 0.0.0.0:6379
```

Press **Ctrl-C** to gracefully shut down (pending writes are flushed).

### Server Configuration (Environment Variables)

```bash
RIPTIDE_BIND=127.0.0.1:6380    \
RIPTIDE_WAL_PATH=./data/wal.log \
RIPTIDE_SST_DIR=./data/sst      \
RIPTIDE_FLUSH_KB=4096           \
RIPTIDE_WAL_SYNC=true           \
cargo run -p server --bin riptidekv-server --release
```

| Variable | Default | Description |
|----------|---------|-------------|
| `RIPTIDE_BIND` | `0.0.0.0:6379` | TCP address to listen on |
| `RIPTIDE_WAL_PATH` | `wal.log` | Path to the WAL file |
| `RIPTIDE_SST_DIR` | `data/sst` | Directory for SSTable files |
| `RIPTIDE_FLUSH_KB` | `1024` | Memtable flush threshold in KiB |
| `RIPTIDE_WAL_SYNC` | `true` | `fsync` after each WAL write |

> **Tip:** Set `RIPTIDE_WAL_SYNC=false` to disable fsync for maximum write
> throughput in development or testing. Never do this if durability matters.

### Connect with redis-cli

If you have `redis-cli` installed:

```bash
redis-cli -h 127.0.0.1 -p 6379

127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET mykey "hello world"
OK
127.0.0.1:6379> GET mykey
"hello world"
127.0.0.1:6379> SET counter 0
OK
127.0.0.1:6379> INCR counter
(integer) 1
127.0.0.1:6379> INCR counter
(integer) 2
127.0.0.1:6379> TTL mykey
(integer) -1
127.0.0.1:6379> EXPIRE mykey 60
(integer) 1
127.0.0.1:6379> TTL mykey
(integer) 59
127.0.0.1:6379> KEYS *
1) "counter"
2) "mykey"
```

### Connect with raw TCP (netcat)

If you don't have `redis-cli`, use `nc` (netcat) to test the raw RESP2 protocol:

```bash
printf '*1\r\n$4\r\nPING\r\n' | nc 127.0.0.1 6379
# → +PONG

printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc 127.0.0.1 6379
# → +OK

printf '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' | nc 127.0.0.1 6379
# → $3
# → bar
```

### Supported Commands

RiptideKV implements **55+ Redis commands**. Here is the full list grouped by category:

#### Connection & Server
| Command | Description |
|---------|-------------|
| `PING [message]` | Returns PONG or echoes the message |
| `ECHO message` | Returns the message |
| `SELECT index` | Switch database (only 0 is supported) |
| `QUIT` | Close the connection |
| `RESET` | Reset connection state |
| `HELLO [version]` | Protocol negotiation (RESP2 only) |
| `CLIENT SETNAME name` | Set connection name |
| `CLIENT GETNAME` | Get connection name |
| `CLIENT ID` | Get connection ID |
| `CLIENT INFO` | Connection information |
| `CONFIG GET pattern` | Returns empty array (stub) |
| `CONFIG SET ...` | Returns OK (stub) |
| `INFO [section]` | Server information (server, clients, stats, keyspace, replication, memory) |
| `COMMAND COUNT` | Number of supported commands |
| `DBSIZE` | Number of keys in the database |
| `FLUSHDB` / `FLUSHALL` | Delete all keys |
| `DEBUG SLEEP seconds` | Sleep for N seconds |
| `WAIT` | Returns 0 (replica sync, not implemented) |
| `BGSAVE` / `SAVE` | Returns stub response |
| `ACL WHOAMI` | Returns "default" |
| `ACL LIST` | Returns default user entry |
| `SLOWLOG GET/LEN/RESET` | Returns stubs |
| `MEMORY USAGE key` | Returns nil |

#### String Operations
| Command | Syntax | Description |
|---------|--------|-------------|
| `SET` | `SET key value [EX s] [PX ms] [NX\|XX] [GET] [KEEPTTL]` | Set a key |
| `GET` | `GET key` | Get a value |
| `SETNX` | `SETNX key value` | Set only if absent |
| `SETEX` | `SETEX key seconds value` | Set with expiry in seconds |
| `PSETEX` | `PSETEX key ms value` | Set with expiry in milliseconds |
| `GETSET` | `GETSET key value` | Set and return old value |
| `GETDEL` | `GETDEL key` | Get and delete |
| `GETEX` | `GETEX key [EX s\|PX ms\|PERSIST]` | Get and modify expiry |
| `MGET` | `MGET key [key ...]` | Get multiple keys |
| `MSET` | `MSET k v [k v ...]` | Set multiple keys |
| `MSETNX` | `MSETNX k v [k v ...]` | Set multiple keys only if all absent |
| `APPEND` | `APPEND key value` | Append to string |
| `STRLEN` | `STRLEN key` | Length of string |
| `INCR` | `INCR key` | Increment integer by 1 |
| `INCRBY` | `INCRBY key delta` | Increment by delta |
| `INCRBYFLOAT` | `INCRBYFLOAT key delta` | Increment float by delta |
| `DECR` | `DECR key` | Decrement integer by 1 |
| `DECRBY` | `DECRBY key delta` | Decrement by delta |
| `GETRANGE` | `GETRANGE key start end` | Substring by byte index |
| `SETRANGE` | `SETRANGE key offset value` | Overwrite substring |

#### Key / Generic Operations
| Command | Syntax | Description |
|---------|--------|-------------|
| `DEL` | `DEL key [key ...]` | Delete one or more keys |
| `UNLINK` | `UNLINK key [key ...]` | Same as DEL |
| `EXISTS` | `EXISTS key [key ...]` | Count existing keys |
| `TYPE` | `TYPE key` | Returns `string` or `none` |
| `EXPIRE` | `EXPIRE key seconds` | Set TTL in seconds |
| `PEXPIRE` | `PEXPIRE key ms` | Set TTL in milliseconds |
| `EXPIREAT` | `EXPIREAT key unix-seconds` | Set expiry as Unix timestamp |
| `PEXPIREAT` | `PEXPIREAT key unix-ms` | Set expiry as Unix timestamp in ms |
| `TTL` | `TTL key` | Remaining TTL in seconds (-1 = no expiry, -2 = absent) |
| `PTTL` | `PTTL key` | Remaining TTL in milliseconds |
| `PERSIST` | `PERSIST key` | Remove expiry |
| `EXPIRETIME` | `EXPIRETIME key` | Absolute expiry as Unix timestamp |
| `PEXPIRETIME` | `PEXPIRETIME key` | Absolute expiry as Unix timestamp in ms |
| `KEYS` | `KEYS pattern` | List keys matching glob pattern (`*`, `?`, `[...]`) |
| `SCAN` | `SCAN cursor [MATCH pat] [COUNT n]` | Iterate keys |
| `RENAME` | `RENAME src dst` | Rename a key |
| `RENAMENX` | `RENAMENX src dst` | Rename only if dst absent |
| `RANDOMKEY` | `RANDOMKEY` | Random key from the database |
| `TOUCH` | `TOUCH key [key ...]` | Count existing keys (access-time update) |

---

## 4 — Running Tests

```bash
# Run all tests in the workspace
cargo test --workspace

# Run only the server integration tests
cargo test -p server

# Run only the engine tests
cargo test -p engine

# Run a specific test by name
cargo test -p server test_concurrent_incr_correctness

# Show test output (don't capture stdout)
cargo test --workspace -- --nocapture

# Run tests in parallel with N threads (default: number of CPUs)
cargo test --workspace -- --test-threads=4
```

### What Gets Tested

**Rust crates (`cargo test --workspace`):**

| Crate | Tests | What's covered |
|-------|-------|----------------|
| `bloom` | 17 | Insert, lookup, false-positive rate, serialization, edge cases |
| `memtable` | 43 | CRUD, sequence gating, tombstones, iteration, size tracking |
| `wal` | 22 | Append, CRC32, truncated tail, corruption detection, fsync |
| `sstable` | 21 | Write (atomic), read, bloom filter, merge iterator, v1/v2/v3 compat |
| `engine` | 55 | CRUD, flush, compaction, recovery, scan, manifest, tombstone GC |
| `server` | 84 | All 55+ commands, TTL expiry, concurrent clients, pipelining, binary values |
| doctests | 3 | Usage examples in bloom, memtable, wal |
| **Total (Rust)** | **245** | |

**Java embedding library (`mvn test -f java/pom.xml`):**

```bash
# Run all Java tests (requires the native binary in src/main/resources/native/)
mvn test -f java/pom.xml

# Run a specific test class
mvn test -f java/pom.xml -Dtest=RespCommandsTest
mvn test -f java/pom.xml -Dtest=RiptideKVConfigTest
mvn test -f java/pom.xml -Dtest=RiptideKVServerTest
```

| Test class | Tests | What's covered |
|---|---|---|
| `RiptideKVConfigTest` | 20 | Builder defaults, validation (null, blank, bad format, out-of-range port), fluency |
| `RiptideKVServerTest` | 14 | start, stop, isRunning, close idempotency, port release, null config guard |
| `RespCommandsTest` | 116 | All 55+ commands over real TCP — Connection, Database, Strings, Keys, expiry, pipelining, concurrent clients, binary safety |
| **Total (Java)** | **150** | |

> **Note:** Java tests require the native binary to be present in
> `java/src/main/resources/native/<os>-<arch>/riptidekv-server[.exe]`.
> Build it first with `cargo build --release -p server` and copy or symlink it.

---

## 5 — Running Benchmarks

Benchmarks use [Criterion.rs](https://bheisler.github.io/criterion.rs/book/)
for statistically rigorous measurements with warm-up, outlier detection, and
comparison across runs.

```bash
# Run all benchmarks (release build, takes several minutes)
cargo bench -p cli
cargo bench -p server

# Run a specific benchmark group
cargo bench -p cli -- engine_set_no_flush
cargo bench -p server -- server_ping_1k

# Save baseline for comparison
cargo bench -p server -- --save-baseline before_change

# Compare against saved baseline
cargo bench -p server -- --baseline before_change
```

Results are saved in `target/criterion/`. Open `target/criterion/report/index.html`
in a browser for graphs.

### Available Benchmarks

**`crates/cli/benches/` (storage engine level):**

| Benchmark | Measures |
|-----------|----------|
| `memtable_put_10k_sequential` | Memtable insert throughput |
| `memtable_get_hit_10k` | Memtable point lookup |
| `memtable_get_miss_10k` | Memtable miss (not found) |
| `sstable_write_from_memtable_10k` | Full flush to disk |
| `sstable_get_hit_10k` | SSTable point lookup (with bloom) |
| `sstable_get_miss_10k` | SSTable miss (bloom short-circuits) |
| `wal_append_nosync_5k` | WAL write without fsync |
| `engine_set_no_flush_1k` | End-to-end write (memtable only) |
| `engine_set_with_flush_1k` | End-to-end write (with flush) |
| `engine_get_memtable_hit_1k` | End-to-end read from memtable |
| `engine_get_sstable_hit_1k` | End-to-end read from SSTable |
| `engine_mixed_set_get_del_1k` | Mixed workload |

**`crates/server/benches/` (TCP server level):**

| Benchmark | Measures |
|-----------|----------|
| `server_ping_1k` | Round-trip latency for 1k PINGs |
| `server_set/set_1k_64b_values` | Write throughput over TCP |
| `server_get/get_1k_existing_keys` | Read throughput over TCP |
| `server_pipeline/pipeline_500_set_500_get` | Pipelined mixed workload |
| `server_mset_100_keys` | Batch write throughput |

---

## 6 — Persistent Data Storage

By default the server stores data in:
```
./wal.log      ← WAL (append-only log)
./data/sst/    ← SSTables (immutable on-disk sorted files)
               │   MANIFEST  (which file is L0 vs L1)
               │   sst-0000000001-<timestamp>.sst
               └── sst-0000000002-<timestamp>.sst
```

To use a specific data directory:

```bash
mkdir -p /var/lib/riptidekv
RIPTIDE_WAL_PATH=/var/lib/riptidekv/wal.log \
RIPTIDE_SST_DIR=/var/lib/riptidekv/sst      \
cargo run -p server --bin riptidekv-server --release
```

### Data Survives Restarts

Stop the server with Ctrl-C, then restart it pointing at the same data directory:

```bash
# Session 1
RIPTIDE_SST_DIR=/tmp/rkv-data cargo run -p server --bin riptidekv-server
# → SET persistent_key hello
# → Ctrl-C

# Session 2 (same data directory)
RIPTIDE_SST_DIR=/tmp/rkv-data cargo run -p server --bin riptidekv-server
# → GET persistent_key → "hello"   ✓ data survived
```

> **Note:** TTLs (EXPIRE/PEXPIRE) are **not** persisted across restarts.
> A key with a TTL will behave as if it has no expiry after a restart.

---

## 7 — Connecting from Java (Jedis)

Add Jedis to your Maven `pom.xml`:

```xml
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>5.1.0</version>
</dependency>
```

Connect to RiptideKV exactly as you would connect to Redis:

```java
import redis.clients.jedis.Jedis;

try (Jedis jedis = new Jedis("localhost", 6379)) {
    // String operations
    jedis.set("user:1", "Alice");
    String val = jedis.get("user:1");   // "Alice"

    // Counter
    jedis.set("counter", "0");
    jedis.incr("counter");              // 1
    jedis.incrBy("counter", 5);         // 6

    // TTL
    jedis.setex("session:abc", 3600, "user_data");  // expires in 1 hour
    long ttl = jedis.ttl("session:abc");             // ~3600

    // Bulk operations
    jedis.mset("k1", "v1", "k2", "v2");
    List<String> vals = jedis.mget("k1", "k2", "absent");
    // ["v1", "v2", null]

    // Pattern matching
    Set<String> keys = jedis.keys("user:*");
}
```

---

## 8 — Connecting from Python (redis-py)

```bash
pip install redis
```

```python
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

r.set('name', 'Alice')
print(r.get('name'))          # Alice

r.setex('session', 3600, 'data')
print(r.ttl('session'))       # ~3600

r.mset({'k1': 'v1', 'k2': 'v2'})
print(r.mget('k1', 'k2'))     # ['v1', 'v2']

print(r.keys('k*'))           # ['k1', 'k2']
```

---

## 9 — Troubleshooting

### "Address already in use"

Another process is using port 6379 (perhaps a local Redis server).
Use a different port:

```bash
RIPTIDE_BIND=127.0.0.1:6380 cargo run -p server --bin riptidekv-server
redis-cli -p 6380 PING
```

### Data Directory Errors on Startup

```
Error: No such file or directory (os error 2)
```

The SSTable directory doesn't exist. Create it or let the server create it:

```bash
mkdir -p data/sst
# or set a directory that exists:
RIPTIDE_SST_DIR=/tmp/rkv-sst cargo run -p server --bin riptidekv-server
```

### WAL Corruption Warning

```
WalReader: CRC mismatch at record, stopping replay
```

This indicates genuine data corruption (not a truncated tail, which is handled
silently). The engine will stop WAL replay at the corrupt record and continue
with what it successfully replayed.

### INCR on a non-numeric value returns 1 (not an error)

RiptideKV treats un-parseable values as `0` for INCR/DECR (graceful degradation).
This differs from Redis, which returns an error.  If strict Redis compatibility is
required, validate values before incrementing.

---

### "NOPROTO this server does not support RESP3"

You are connecting with a client configured for RESP3 (newer Redis protocol).
Disable RESP3 in your client:

```python
# redis-py: disable RESP3
r = redis.Redis(host='localhost', port=6379, protocol=2)
```

```java
// Jedis: use standard connection (defaults to RESP2)
Jedis jedis = new Jedis("localhost", 6379);  // fine as-is
```

### TTL Disappeared After Restart

TTLs are stored in memory only and are lost on server restart. This is a known
limitation. Keys survive, but their expiry times do not.

---

## 10 — Building a Release Binary

```bash
cargo build -p server --release

# The binary is at:
./target/release/riptidekv-server

# Run it:
RIPTIDE_BIND=0.0.0.0:6379 \
RIPTIDE_WAL_PATH=/var/lib/riptidekv/wal.log \
RIPTIDE_SST_DIR=/var/lib/riptidekv/sst \
./target/release/riptidekv-server
```

The release binary is roughly 10–20× faster than the debug build due to
LLVM optimizations and inlining.
