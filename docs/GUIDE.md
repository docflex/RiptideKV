# RiptideKV — Learning Guide

> **Who is this for?**  
> You built (or are studying) this project and want to understand not just
> _what_ the code does, but _why_ it does it that way. Each section introduces
> a concept from first principles and then shows exactly where it lives in the
> codebase, with real code, real examples, and the pitfalls that tripped us up
> along the way.

Read this linearly — each section builds on the previous one.

---

## Part 1 — Why Is a Key-Value Store Hard?

A key-value store sounds simple: store a key, get it back later. A `HashMap`
does this in memory. The moment you need **durability** — surviving power cuts,
process crashes, full disk failures — everything becomes interesting.

**The fundamental problem:** Memory is volatile. Disk is slow. How do you
get the performance of memory and the durability of disk?

RiptideKV's answer is the **Log-Structured Merge (LSM) tree**, the same
architecture used by LevelDB, RocksDB, Cassandra, and Pebble.

---

## Part 2 — The Write-Ahead Log (WAL)

### The Problem

If you write data to an in-memory structure and the process crashes before that
data reaches disk, it is gone forever. You need to persist _something_ to disk
on every write.

### Naïve Solution: Write to Disk Directly

Write each key-value pair to a file immediately. Problem: files on disk are
structured (databases, B-trees) — updating a structure on disk requires reading
a page, modifying it, and writing it back. Random writes to disk are slow (~100
operations/second on spinning disk, ~10,000 on SSD).

### Better Solution: Append to a Log

Appending to the end of a file is the fastest disk operation possible because
it's **sequential**. No seeking, no reading, just writing bytes at the end.

```
wal.log (grows rightward over time):
┌──────┬──────┬──────┬──────┐
│PUT k1│PUT k2│DEL k1│PUT k3│  ← each append is fast (sequential write)
└──────┴──────┴──────┴──────┘
```

This is the **Write-Ahead Log**: every mutation is appended to the log before
touching any in-memory structure. On crash, replay the log to reconstruct state.

### How RiptideKV Does It

```rust
// crates/wal/src/lib.rs — appending a record
pub fn append(&mut self, record: &WalRecord) -> Result<()> {
    // Serialize: tag(1) + seq(8) + key_len(4) + key + val_len(4) + val
    // Compute CRC32 over all bytes
    // Write to file (buffered)
    // If wal_sync=true, call file.sync_all() for durability
}
```

```rust
// crates/wal/src/lib.rs — replaying on restart
pub struct WalReader { ... }
impl WalReader {
    pub fn next(&mut self) -> Result<Option<WalRecord>> {
        // Read tag, seq, key_len, key...
        // If file ends early (truncated tail) → return Ok(None) — graceful
        // If CRC32 mismatches → return Err — genuine corruption
    }
}
```

**Pitfall — CRC32 vs truncated tail:** A crash can leave the last record
partially written. This looks like a CRC mismatch but is actually normal.
We distinguish by detecting EOF before the CRC field — that's a truncated
tail (safe to ignore). A CRC mismatch on a _complete_ record is genuine
corruption.

### WAL Sync Trade-off

`wal_sync = true`: `fsync()` after every write. **Durable** but ~10–100× slower.  
`wal_sync = false`: OS buffers writes. **Fast** (memory speed) but up to 1 second of data lost on crash.

The default for the server is `true`. The CLI default is `true`.  
Set `RIPTIDE_WAL_SYNC=false` for maximum write throughput in testing.

---

## Part 3 — The Memtable

### Why Not Just Replay the WAL Every Time?

Replaying the WAL on every read would be O(n) where n is every write ever made.
That's unusable. Instead, keep the recent writes in memory in a structure
optimised for both reads and writes.

### The Memtable

A **Memtable** is an in-memory sorted map. When we write a key-value pair, we:
1. Append to WAL (durability)
2. Insert into Memtable (fast access)

Reads check the Memtable first — if the key is there, we don't need to touch
disk at all.

```rust
// crates/memtable/src/lib.rs
pub struct Memtable {
    data: BTreeMap<Vec<u8>, ValueEntry>,  // sorted by key
    approx_size: usize,                   // tracks bytes for flush trigger
}

pub enum ValueEntry {
    Value { seq: u64, data: Vec<u8> },
    Tombstone { seq: u64 },              // deletion marker
}
```

Why `BTreeMap` and not `HashMap`? Because `BTreeMap` keeps keys sorted, which
matters enormously: when we flush to disk, we iterate in key order to produce
a **Sorted** String Table. Sorted order also makes range queries (`SCAN`) cheap
— binary search to the start key, then iterate.

### Sequence-Gated Writes

The Memtable only accepts a write if its sequence number is _newer_ than what's
already there:

```rust
// crates/memtable/src/lib.rs — Memtable::put()
pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, seq: u64) {
    match self.data.get(&key) {
        Some(existing) if existing.seq() >= seq => return,  // stale write, ignore
        _ => { /* insert */ }
    }
}
```

**Why is this important?** During WAL replay, we might replay a PUT for a key
that was later overwritten and flushed to an SSTable. Without sequence gating,
the replay would overwrite the newer data in the Memtable with the older value
from the WAL. Sequence gating prevents this.

### Approximate Size Tracking

The Memtable tracks `approx_size` (key bytes + value bytes + a small overhead
estimate). When this exceeds `flush_threshold`, the engine flushes the Memtable
to disk as a new SSTable.

---

## Part 4 — SSTables (Sorted String Tables)

### The Problem with the WAL as a Read Structure

The WAL is append-only and unsorted. Reading a specific key requires scanning
every record. For writes, this is fine. For reads, it's terrible.

### SSTables

When the Memtable grows too large, we flush it to disk as a **Sorted String
Table (SSTable)**. An SSTable is:
- **Immutable** — never modified after writing.
- **Sorted** — keys are in ascending order.
- **Compact** — all data is in one file with a footer pointing to sections.

Because it's sorted, looking up a key is fast: binary search the index, seek
to roughly the right place, scan forward. No need to read the whole file.

### How RiptideKV Writes an SSTable

```rust
// crates/sstable/src/writer.rs — atomic write
pub fn write_from_memtable(path: &Path, memtable: &Memtable) -> Result<()> {
    let tmp_path = path.with_extension("sst.tmp");
    let mut file = File::create(&tmp_path)?;

    // Write DATA section: one record per key-value pair
    for (key, entry) in memtable.iter() {
        write_record(&mut file, key, entry)?;
        update_bloom_filter(key);
        update_index(key, current_offset);
    }

    // Write BLOOM section
    // Write INDEX section
    // Write FOOTER (v3): offsets + lengths + max_seq + magic

    file.sync_all()?;           // durability before rename
    rename(tmp_path, path)?;    // atomic: readers see complete file or nothing
}
```

**Why tmp + rename?**  
`rename()` on POSIX is atomic — either the old filename or the new one is
visible, never a partially-written intermediate state. Without this, a crash
mid-write would leave a corrupt `.sst` file that gets loaded on restart.

### The Sparse Index

The index doesn't store every key — that would be as large as the data itself.
Instead, it stores one entry per ~4 KB of data ("sparse" index). A lookup:
1. Binary search the index for the largest key ≤ target.
2. Seek to that file offset.
3. Scan forward record by record until finding the key or overshooting.

This balances index memory usage (small) vs. per-lookup scan distance (bounded).

### Footer and Version History

```
v1 footer (40 bytes): magic + bloom_off + bloom_len + index_off + index_len + version
v2 footer (40 bytes): same structure, different magic? (compatible)
v3 footer (56 bytes): adds max_seq (8 bytes) — enables fast recovery
```

The version is stored in the last byte of the footer. This lets us evolve the
format over time while remaining backward compatible.

`max_seq` in v3 is crucial for recovery: instead of reading every record to
find the highest sequence number, we read just the last 56 bytes of each SSTable.

---

## Part 5 — Bloom Filters

### The Problem

The read path checks the Memtable, then all L0 SSTables, then L1. For a key
that doesn't exist, every SSTable would require a disk read (seek + read a few
hundred bytes) just to confirm "not found". With many SSTables, this is painful.

### Bloom Filters

A Bloom filter is a **probabilistic set membership** data structure. It answers:
- "Definitely not in this set" — 100% correct, zero false negatives.
- "Might be in this set" — 99% correct (1% false positive rate here).

It uses a bit array and multiple hash functions. Inserting a key sets several
bits; checking a key tests whether all those bits are set. If any bit is unset,
the key is definitely absent.

**The mathematical guarantee:** For our 1% false-positive rate with FNV-1a
double-hashing, we allocate ~10 bits per key in the filter. The exact formula:

```
bits = -n × ln(p) / (ln 2)²
hashes = bits/n × ln 2

where n = number of keys, p = desired false-positive rate
```

### How RiptideKV Uses Bloom Filters

```rust
// crates/bloom/src/lib.rs
pub struct BloomFilter {
    bits: Vec<u64>,     // the bit array (packed into u64 words)
    num_hashes: usize,  // k hash functions
}

impl BloomFilter {
    pub fn insert(&mut self, key: &[u8]) { /* set k bits */ }
    pub fn may_contain(&self, key: &[u8]) -> bool { /* check k bits */ }
}
```

```rust
// crates/sstable/src/reader.rs — read path
pub fn get(&self, key: &[u8]) -> Result<Option<ValueEntry>> {
    // Fast path: Bloom filter says "definitely not here"
    if let Some(bloom) = &self.bloom {
        if !bloom.may_contain(key) {
            return Ok(None);  // no disk I/O
        }
    }
    // Slow path: binary search index, seek, read record
    // ...
}
```

**Pitfall — FNV-1a double hashing:** Standard double hashing uses two
independent hash functions `h1(x)` and `h2(x)`, then computes
`gi(x) = h1(x) + i * h2(x) mod m` for each of the k hash positions.
We derive both h1 and h2 from the same FNV-1a hash over the key with
different seeds — a technique that gives good distribution without needing
a completely different hash algorithm.

---

## Part 6 — Compaction

### The Growing Problem

Every time the Memtable fills up, we flush to a new L0 SSTable. Over time:
- L0 has many SSTables (e.g., 50).
- Each GET might need to check all 50 files for a key.
- Disk space grows because old values/tombstones are never removed.

### Compaction: Merge Everything Into One

Compaction merges all existing SSTables into a single new one, resolving
version conflicts (keep the newest) and removing dead tombstones.

```
Before compaction:           After compaction:
L0: sst-001.sst  {k1:v1}    L1: sst-compact.sst  {k1:v3, k3:v2}
    sst-002.sst  {k1:v2}        (k2 was deleted, tombstone GC'd)
    sst-003.sst  {k2:v1}
L1: sst-000.sst  {k1:v0, k3:v2}
    sst-004.sst  {k2:tombstone}
```

### The Merge Iterator

The core data structure is a min-heap that treats N SSTable readers as N sorted
streams and merges them in order:

```rust
// crates/sstable/src/merge.rs
pub struct MergeIterator {
    heap: BinaryHeap<HeapEntry>,  // min-heap ordered by (key, seq desc)
    readers: Vec<SSTableReader>,
}

// The heap contains one "current entry" from each active reader.
// Pop the minimum key → if duplicate keys (same key in multiple SSTables),
// the one with higher seq wins (most recent write).
```

This is a classic **k-way merge** algorithm, the same one used in:
- Merge sort
- External sort (sorting data that doesn't fit in RAM)
- Database query engines for merging sorted result sets

### Tombstone Garbage Collection

```rust
// crates/engine/src/compaction.rs
for (key, entry) in merge_iter {
    // Is this a tombstone?
    if entry.value.is_none() {
        // Is the key still live in the current Memtable?
        if !memtable.contains_key(&key) {
            // Safe to drop: no newer data exists that this tombstone needs to hide.
            continue;
        }
    }
    // Otherwise: write this entry to the new SSTable.
    writer.write_entry(&key, &entry)?;
}
```

**Why the Memtable check?** If a key is in the Memtable, it means there's a
pending write (set or delete) that hasn't been flushed yet. If we GC the
tombstone, and then the Memtable is flushed, the old value in a pre-compaction
SSTable could resurface. The Memtable check prevents this.

### Auto-Compaction

When the L0 SSTable count reaches `l0_compaction_trigger` (default: 4), the
engine automatically compacts after the next flush:

```rust
// crates/engine/src/write.rs — inside flush()
if self.l0_sstables.len() >= self.l0_compaction_trigger {
    self.compact()?;
}
```

---

## Part 7 — Recovery After a Crash

### What Happens On Restart?

```
1. Remove any *.sst.tmp files (interrupted flush mid-write)
2. Replay WAL → rebuild Memtable
3. Load Manifest → know which .sst files belong to L0 vs L1
4. Open SSTableReaders for each file
5. Compute the maximum sequence number seen across all sources
6. Ready to serve requests
```

### What If the WAL Has a Partial Record at the End?

Normal. Crash during WAL append leaves the last record truncated. The WAL
reader detects EOF before reading a complete record and stops gracefully.
This means the last partial write is lost — which is correct: that write was
never acknowledged to the client.

### What If the Manifest Is Corrupt?

The Manifest is written atomically (write tmp → fsync → rename). It's either
the pre-flush version or the post-flush version, never a partial state. The
only way to get a corrupt manifest is hardware failure, at which point the WAL
provides recovery.

---

## Part 8 — The RESP2 Protocol

### Why RESP2?

Rather than inventing a custom protocol, RiptideKV speaks **Redis Serialization
Protocol 2** — the same wire format Redis has used since 2010. This means:
- **Any Redis client library works**: Jedis (Java), redis-py (Python),
  go-redis, lettuce, ioredis, redis-cli.
- **No custom client needed**: point your existing Redis code at RiptideKV's
  address and it works.

### RESP2 Format

RESP2 has 5 types, each identified by the first byte of each line:

```
+OK\r\n              Simple String  (short, no binary)
-ERR some msg\r\n    Error
:42\r\n              Integer
$5\r\nhello\r\n      Bulk String    (binary-safe, with length prefix)
*3\r\n...            Array          (N elements follow)
$-1\r\n              Null Bulk      (nil value)
*-1\r\n              Null Array     (nil array)
```

A command from the client is always an Array of Bulk Strings:

```
*3\r\n         ← array of 3 elements
$3\r\n         ← element 1: bulk string, 3 bytes
SET\r\n
$5\r\n         ← element 2: bulk string, 5 bytes
mykey\r\n
$7\r\n         ← element 3: bulk string, 7 bytes
myvalue\r\n
```

### Parsing Without Recursion

Arrays can contain any RESP value, including other arrays. A naïve recursive
parser hits Rust's async recursion limitation:

```
error[E0733]: recursion in an async fn requires boxing
```

RiptideKV solves this by never recursing. The top-level `read_value()` handles
the `*N` array header, then calls `read_item()` for each element. `read_item()`
calls `parse_scalar()` which handles `+`, `-`, `:`, `$` types but _not_ `*`.

```rust
// crates/server/src/resp.rs — non-recursive design
pub async fn read_value(&mut self) -> Result<Option<RespValue>> {
    // reads one line
    if starts_with('*') {
        // read N elements via read_item() — no recursion
        for _ in 0..count { items.push(self.read_item().await?); }
    } else {
        self.parse_scalar(&line).await   // +, -, :, $
    }
}

async fn read_item(&mut self) -> Result<RespValue> {
    // reads one line, calls parse_scalar — never calls read_value
    self.parse_scalar(&line).await
}
```

This handles the 99.9% case (arrays of bulk strings). Nested arrays (e.g.,
`EXEC` pipeline responses in Redis) would need a different approach, but
RESP2 commands from clients are always flat arrays.

### Inline Commands

Redis also supports "inline" commands — plain text without the array framing:

```
PING\r\n
SET foo bar\r\n
```

RiptideKV's parser handles these: if the first byte doesn't match a RESP2
type prefix, split the line on whitespace and wrap in an Array. This is what
lets you test the server with raw `nc` or `telnet`.

---

## Part 9 — The Async Server with Tokio

### What Is Tokio?

Tokio is Rust's most popular async runtime. It implements a **multi-threaded
work-stealing scheduler** for async tasks — lightweight green threads that
cooperate instead of preempting.

An async function returns a `Future` — a value representing a computation
that hasn't run yet. The runtime drives futures to completion by polling them.
When a future is "waiting" (e.g., for network data), it yields control and the
runtime runs other futures on the same OS thread.

### The Accept Loop

```rust
// crates/server/src/lib.rs
pub async fn serve(listener: TcpListener, db: SharedDb) -> anyhow::Result<()> {
    loop {
        let (stream, _peer) = listener.accept().await?;  // ← await yields here
        stream.set_nodelay(true)?;
        let db = db.clone();                             // O(1) Arc clone
        tokio::spawn(async move {
            handler::handle_connection(stream, db).await;
        });
        // The spawned task runs concurrently. We're back here immediately.
    }
}
```

Each `tokio::spawn` creates a new async task — effectively a green thread.
Thousands of concurrent connections use only a handful of OS threads (typically
one per CPU core).

`stream.set_nodelay(true)` disables Nagle's algorithm — it causes TCP to send
small packets immediately instead of buffering them. Critical for low-latency
request-response patterns like Redis.

### Why OwnedReadHalf / OwnedWriteHalf?

`TcpStream::into_split()` gives you independently-owned read and write halves.
This lets the RESP reader and writer live in separate places in the code without
Rust complaining about simultaneous borrows of the stream.

```rust
// crates/server/src/handler.rs
let (read_half, write_half) = stream.into_split();
let mut reader = RespReader::new(read_half);
let mut conn = Conn { writer: write_half, ... };
```

---

## Part 10 — Shared State and the Borrow Checker

### The Ownership Challenge

Multiple async tasks (one per connection) need to share the same database. In
Rust, shared mutable state requires explicit synchronization — the compiler
rejects unsafe sharing.

The solution: `Arc<RwLock<DbState>>`.

- `Arc<T>` — **atomically reference-counted** shared ownership. Clone it to
  get another handle; the underlying data lives until all handles are dropped.
- `RwLock<T>` — **readers-writer lock**. Multiple readers can hold the lock
  simultaneously; a writer gets exclusive access.

```rust
// crates/server/src/db.rs
pub struct SharedDb {
    pub state: Arc<RwLock<DbState>>,
    // ... stats counters
}

impl Clone for SharedDb {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),  // just bumps refcount
            // ...
        }
    }
}
```

### The Guard-and-Borrow Problem

A subtle Rust pitfall: `RwLockWriteGuard` borrows `Arc<RwLock<...>>`.
If the `Arc` is a field of `conn`, the guard borrows `conn`. You then can't
call `&mut conn` methods:

```rust
// ❌ This won't compile:
let guard = conn.db.state.write().await;  // borrows conn.db.state → borrows conn
conn.send(response).await;               // needs &mut conn — compile error!
```

Fix: clone the `Arc` into a local variable first. The guard then borrows the
local clone, not `conn`:

```rust
// ✅ This works:
let state_arc = Arc::clone(&conn.db.state);  // local clone
let guard = state_arc.write_owned().await;    // borrows local, not conn
conn.send(response).await;                   // conn is free
```

This pattern is encapsulated in the `state_write!` and `state_read!` macros
at the top of `handler.rs`.

---

## Part 11 — Testing Philosophy

### Unit Tests vs Integration Tests

RiptideKV has two levels of testing:

**Unit/integration tests per crate** (in `crates/*/src/tests/`):
- Test each component in isolation.
- Fast — no TCP, no server, no temp directories (mostly).
- Example: `write_tests.rs` tests `Engine::set`, `Engine::del`, flush triggering.

**Server integration tests** (in `crates/server/tests/integration.rs`):
- Spin up a real TCP server on a random port.
- Send real RESP2 bytes over a real socket.
- Assert real RESP2 responses.
- Cover the full system: RESP parsing → command dispatch → engine → response encoding.

```rust
// crates/server/tests/integration.rs — test helper
async fn start_server() -> (SocketAddr, SharedDb) {
    let engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        64 * 1024 * 1024,  // large — no auto-flush noise
        false,             // no fsync — tests are fast
    ).unwrap();
    let db = SharedDb::new(engine);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();  // OS picks port
    let addr = listener.local_addr().unwrap();
    tokio::spawn(server::serve(listener, db.clone()));
    (addr, db)
}
```

**Key test design decisions:**

1. **Port 0** — let the OS pick a free port. Avoids flaky tests from port
   conflicts when tests run in parallel.

2. **Large flush threshold** — prevents auto-flushes from interleaving with
   test assertions.

3. **`wal_sync = false`** — removes fsync latency from tests. Tests run ~10×
   faster.

4. **`Box::leak` for tempdir** — the `TempDir` guard must live as long as the
   server. Leaking it ensures the directory isn't cleaned up mid-test.
   (In production code you'd use proper lifetime management; this is fine for
   tests.)

### The Concurrency Test

```rust
// crates/server/tests/integration.rs
#[tokio::test]
async fn test_concurrent_incr_correctness() {
    // 50 tasks × 10 INCRs = 500 total increments
    let handles: Vec<_> = (0..50)
        .map(|_| tokio::spawn(async move {
            let mut c = TestClient::connect(addr).await;
            for _ in 0..10 { c.cmd(&["INCR", "shared_counter"]).await; }
        }))
        .collect();
    for h in handles { h.await.unwrap(); }
    // Final value must be exactly 500 — no lost updates
    let val: i64 = c.cmd(&["GET", "shared_counter"]).await.as_str().parse().unwrap();
    assert_eq!(val, 500);
}
```

This test confirms that the `RwLock` prevents lost updates when many clients
concurrently increment the same counter.

---

## Part 12 — Pitfalls and Lessons Learned

### Pitfall 1: Tombstone GC Predicate Inversion

**Bug:** After compaction, deleted keys were resurrecting.

**Cause:** The GC predicate was inverted:
```rust
// ❌ Wrong: keeps tombstone only if the key IS in memtable
if entry.value.is_none() && mem_ref.contains_key(&key) {
    continue;  // This was dropping tombstones that we needed!
}
```

**Fix:**
```rust
// ✅ Correct: GC the tombstone only if the key is NOT in memtable
if entry.value.is_none() && !mem_ref.contains_key(&key) {
    continue;  // Safe to GC: no live data in memtable references this key
}
```

**Lesson:** When writing GC predicates, double-check the direction of the
condition. Write a test that:
1. Writes a key.
2. Deletes it.
3. Compacts.
4. Restarts the engine.
5. Asserts the key is still gone.

### Pitfall 2: WAL Truncation Without fsync

**Bug:** On some filesystems (ext4 with barriers), a crash immediately after
`truncate()` but before any new write could leave the old WAL content visible
on restart, causing duplicate WAL replay.

**Fix:** Always `fsync()` after `truncate()`:
```rust
// crates/engine/src/write.rs
wal.truncate()?;
wal.sync()?;   // ← this line was missing
```

**Lesson:** Every crash-safety guarantee in storage engineering requires
explicit `fsync()` calls. Assume the OS will do the wrong thing unless
you tell it explicitly what must be durable before proceeding.

### Pitfall 3: Async Recursion in the RESP Parser

**Bug:** Rust rejected the recursive `read_value()` call inside arrays:
```
error[E0733]: recursion in an async fn requires boxing
```

**Why:** Each `async fn` compiles to a state machine whose size is computed
at compile time. Recursion makes the type infinitely large.

**Fix:** Split the parser into non-recursive functions (see Part 8). The
`read_item()` helper handles array elements but never calls `read_value()`.

### Pitfall 4: Holding RwLock Guards Across Await Points

**Bug:** Borrow checker rejected command handler code.

**Why:** The `RwLockWriteGuard` borrows `db.state`. If you hold the guard
and then call an `async` function (which yields to other tasks), Rust must
ensure the guard lives across the await point — but it also means the lock
is held while other tasks try to acquire it, causing a deadlock.

**Fix:** Always drop the guard before awaiting. Use a scoped block:
```rust
let result = {
    let mut state = db.write().await;
    // do all synchronous engine work here
    state.engine.set(key, val)?
    // state guard dropped here, at end of block
};
// Now await is safe — no lock held
conn.send(encode_ok()).await?;
```

### Pitfall 5: engine.get Returns (seq, value), Not Just value

RiptideKV's `Engine::get()` returns `Option<(u64, Vec<u8>)>` — a tuple of
`(sequence_number, value)`. Every call site that only needs the value must
destructure it:

```rust
// ✅ Correct
let val = engine.get(b"key")?.map(|(_, v)| v);

// ❌ Wrong — (u64, Vec<u8>) is not Vec<u8>
let val = engine.get(b"key")?;
```

The sequence number is exposed so callers can implement optimistic concurrency
control ("only update if seq matches X") — but most command implementations
don't need it.
