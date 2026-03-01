//! # ConcurrentEngine – Thread-Safe Wrapper for Fearless Concurrency
//!
//! Wraps [`Engine`] in `Arc<RwLock<Engine>>` so multiple threads (or Tokio
//! tasks via `spawn_blocking`) can safely share a single engine instance.
//!
//! ## Design Decisions
//!
//! | Decision | Rationale |
//! |----------|-----------|
//! | `std::sync::RwLock` (not Tokio) | Engine performs synchronous file I/O; an async lock would block the Tokio runtime. |
//! | `Arc` wrapper | Enables cheap cloning for distribution across tasks/threads. |
//! | Read lock for `get`/`scan` | Multiple readers can proceed concurrently (no I/O mutation). |
//! | Write lock for `set`/`del`/… | Mutations must be serialized to protect WAL + Memtable state. |
//! | `Clone` derives cheaply | `Arc::clone` is an atomic ref-count bump — negligible cost. |
//!
//! ## Usage with Tokio
//!
//! ```rust,ignore
//! let engine = ConcurrentEngine::new(cfg)?;
//! let engine_clone = engine.clone();
//!
//! tokio::task::spawn_blocking(move || {
//!     engine_clone.set(b"key".to_vec(), b"value".to_vec())
//! }).await??;
//! ```

use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use config::EngineConfig;

use crate::Engine;

/// A thread-safe handle to a shared [`Engine`].
///
/// ALL methods acquire the appropriate lock internally so callers never need
/// to manage locking themselves. The handle is cheaply cloneable (`Arc`).
#[derive(Clone)]
pub struct ConcurrentEngine {
    inner: Arc<RwLock<Engine>>,
}

impl ConcurrentEngine {
    // ─── Construction ───────────────────────────────────────────────────────

    /// Creates a new `ConcurrentEngine` from an [`EngineConfig`].
    pub fn new(cfg: EngineConfig) -> Result<Self> {
        let engine = Engine::new(cfg)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(engine)),
        })
    }

    /// Wraps an already-constructed [`Engine`] for concurrent access.
    pub fn from_engine(engine: Engine) -> Self {
        Self {
            inner: Arc::new(RwLock::new(engine)),
        }
    }

    // ─── Write operations (exclusive lock) ─────────────────────────────────

    /// Inserts a key-value pair. Acquires a **write** lock.
    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))
            .context("failed to acquire write lock for set")?
            .set(key, value)
    }

    /// Deletes a key (writes a tombstone). Acquires a **write** lock.
    pub fn del(&self, key: Vec<u8>) -> Result<()> {
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))
            .context("failed to acquire write lock for del")?
            .del(key)
    }

    /// Forces a memtable flush to SSTable. Acquires a **write** lock.
    pub fn force_flush(&self) -> Result<()> {
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))
            .context("failed to acquire write lock for force_flush")?
            .force_flush()
    }

    /// Runs compaction (merges all L0 + L1 into a single L1 SSTable).
    /// Acquires a **write** lock.
    pub fn compact(&self) -> Result<()> {
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))
            .context("failed to acquire write lock for compact")?
            .compact()
    }

    /// Updates the L0 compaction trigger. Acquires a **write** lock.
    pub fn set_l0_compaction_trigger(&self, trigger: usize) -> Result<()> {
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))
            .context("failed to acquire write lock for set_l0_compaction_trigger")?
            .set_l0_compaction_trigger(trigger);
        Ok(())
    }

    /// Updates the flush threshold. Acquires a **write** lock.
    pub fn set_flush_threshold(&self, threshold: usize) -> Result<()> {
        self.inner
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))
            .context("failed to acquire write lock for set_flush_threshold")?
            .set_flush_threshold(threshold);
        Ok(())
    }

    // ─── Read operations (shared lock) ─────────────────────────────────────

    /// Looks up a key. Acquires a **read** lock.
    pub fn get(&self, key: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        self.inner
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))
            .context("failed to acquire read lock for get")?
            .get(key)
    }

    /// Range scan. Acquires a **read** lock.
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))
            .context("failed to acquire read lock for scan")?
            .scan(start, end)
    }

    /// Returns the current sequence number. Acquires a **read** lock.
    pub fn seq(&self) -> Result<u64> {
        Ok(self
            .inner
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?
            .seq())
    }

    /// Returns the total SSTable count. Acquires a **read** lock.
    pub fn sstable_count(&self) -> Result<usize> {
        Ok(self
            .inner
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?
            .sstable_count())
    }

    /// Returns the L0 SSTable count. Acquires a **read** lock.
    pub fn l0_sstable_count(&self) -> Result<usize> {
        Ok(self
            .inner
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?
            .l0_sstable_count())
    }

    /// Returns the L1 SSTable count. Acquires a **read** lock.
    pub fn l1_sstable_count(&self) -> Result<usize> {
        Ok(self
            .inner
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?
            .l1_sstable_count())
    }

    /// Returns the flush threshold in bytes. Acquires a **read** lock.
    pub fn flush_threshold(&self) -> Result<usize> {
        Ok(self
            .inner
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?
            .flush_threshold())
    }

    /// Returns the L0 compaction trigger. Acquires a **read** lock.
    pub fn l0_compaction_trigger(&self) -> Result<usize> {
        Ok(self
            .inner
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?
            .l0_compaction_trigger())
    }
}

impl std::fmt::Debug for ConcurrentEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner.read() {
            Ok(engine) => write!(f, "ConcurrentEngine({:?})", &*engine),
            Err(_) => write!(f, "ConcurrentEngine(<poisoned>)"),
        }
    }
}
