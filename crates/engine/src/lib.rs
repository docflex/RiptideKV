//! # Engine - RiptideKV Storage Engine
//!
//! The central orchestrator that ties together the [`memtable`], [`wal`], and
//! [`sstable`] crates into a complete LSM-tree key-value store.
//!
//! ## Architecture
//!
//! ```text
//! Client
//!   |
//!   v
//! ┌───────────────────────────────────────────────┐
//! │                   ENGINE                      │
//! │                                               │
//! │ write.rs → WAL append → Memtable insert       │
//! │              |                                │
//! │              |  (threshold exceeded?)         │
//! │              |            yes                 │
//! │              v                                │
//! │           flush() → new SSTable               │
//! │              |                                │
//! │              |  (L0 count >= trigger?)        │
//! │              |            yes                 │
//! │              v                                │
//! │           compact() → merged L1 SST           │
//! │                                               │
//! │ read.rs → Memtable → L0 SSTs → L1 SSTs        │
//! │            (first match wins)                 │
//! └───────────────────────────────────────────────┘
//! ```
//!
//! ## Module Responsibilities
//!
//! | Module        | Purpose                                               |
//! |--------------|-------------------------------------------------------|
//! | [`lib.rs`]   | `Engine` struct, constructor, accessors, `Debug`, `Drop` |
//! | [`recovery`] | WAL replay, SSTable loading, tmp file cleanup          |
//! | [`write`]    | `set()`, `del()`, `force_flush()`, internal `flush()`   |
//! | [`read`]     | `get()`, `scan()`                                      |
//! | [`compaction`] | `compact()` with streaming merge + tombstone GC     |
//! | [`manifest`] | Persistent L0/L1 level tracking (atomic file ops)      |
//!
//! ## Levels
//!
//! ```text
//! ┌────────────────────────────┐  ← freshest, checked first
//! │ MEMTABLE                   │
//! ├────────────────────────────┤  ← from flushes (may overlap)
//! │ L0 SSTables                │
//! ├────────────────────────────┤  ← from compaction (no overlap)
//! │ L1 SSTables                │
//! └────────────────────────────┘
//! ```
//!
//! ## Crash Safety
//!
//! Every write is appended to the WAL **before** the Memtable update. The WAL
//! is only truncated **after** a successful flush + manifest update. SSTables
//! are written atomically via temp file + rename. The manifest uses the same
//! atomic write pattern. See [`ARCHITECTURE.md`] for the full crash matrix.
mod compaction;
mod manifest;
mod read;
mod recovery;
mod write;

use anyhow::Result;
use manifest::Manifest;
use memtable::Memtable;
pub use recovery::replay_wal_and_build;
use sstable::{MergeIterator, SSTableReader, SSTableWriter};
use std::path::{Path, PathBuf};
use wal::WalWriter;

/// Maximum allowed key size in bytes (64 KiB).
pub const MAX_KEY_SIZE: usize = 64 * 1024;
/// Maximum allowed value size in bytes (10 MiB).
pub const MAX_VALUE_SIZE: usize = 10 * 1024 * 1024;

/// Default number of L0 SSTables that triggers automatic compaction.
///
/// When the L0 count reaches this threshold after a flush, the engine
/// automatically runs compaction to merge L0 + L1 into a single L1 SSTable.
/// Set to `0` to disable auto-compaction.
pub const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4;

/// The central storage engine orchestrating Memtable, WAL, and SSTables.
///
/// # Write Path
///
/// 1. Increment the monotonic sequence number.
/// 2. Append the record to the WAL (crash-safe durability).
/// 3. Apply the mutation to the in-memory Memtable.
/// 4. If `approx_size >= flush_threshold`, flush the Memtable to a new SSTable,
///    truncate the WAL, and reset the Memtable.
///
/// # Read Path
///
/// 1. Check the Memtable (freshest data, includes tombstones).
/// 2. Check SSTables from newest to oldest.
/// 3. First match wins; tombstones shadow older values.
///
/// # Recovery
///
/// On construction ([`Engine::new`]), the WAL is replayed into a fresh Memtable
/// and existing `.sst` files are loaded from the SST directory.
pub struct Engine {
    pub(crate) mem: Memtable,
    /// Level 0: SSTables from memtable flushes (may have overlapping key ranges).
    /// Ordered newest-first.
    pub(crate) l0_sstables: Vec<SSTableReader>,
    /// Level 1: SSTables from compaction (non-overlapping key ranges).
    /// Ordered newest-first.
    pub(crate) l1_sstables: Vec<SSTableReader>,
    pub(crate) wal_path: PathBuf,
    pub(crate) sst_dir: PathBuf,
    pub(crate) wal_writer: WalWriter,
    /// Persistent manifest tracking which SSTable files belong to which level.
    /// Updated atomically on flush and compaction so that L0/L1 assignments
    /// survive restarts.
    pub(crate) manifest: Manifest,

    /// Current monotonic sequence number.
    pub(crate) seq: u64,

    /// Memtable byte-size threshold that triggers a flush to SSTable.
    pub(crate) flush_threshold: usize,

    /// Number of L0 SSTables that triggers automatic compaction after a flush.
    /// Set to `0` to disable auto-compaction (caller must invoke `compact()`).
    pub(crate) l0_compaction_trigger: usize,

    /// If `true`, every WAL append is followed by `fsync` for durability.
    pub(crate) wal_sync: bool,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("seq", &self.seq)
            .field("flush_threshold", &self.flush_threshold)
            .field("wal_sync", &self.wal_sync)
            .field("wal_path", &self.wal_path)
            .field("sst_dir", &self.sst_dir)
            .field("memtable_size", &self.mem.approx_size())
            .field("memtable_entries", &self.mem.len())
            .field("l0_sstable_count", &self.l0_sstables.len())
            .field("l1_sstable_count", &self.l1_sstables.len())
            .field("l0_compaction_trigger", &self.l0_compaction_trigger)
            .finish()
    }
}

impl Engine {
    /// Creates a new engine, performing full recovery from the WAL and existing
    /// SSTable files.
    ///
    /// # Arguments
    ///
    /// * `wal_path` — path to the write-ahead log file.
    /// * `sst_dir` — directory where SSTable files are stored.
    /// * `flush_threshold` — memtable byte-size threshold that triggers flush.
    /// * `wal_sync` — if `true`, every WAL append calls `fsync`.
    ///
    /// # Recovery Steps
    ///
    /// 1. Create the SST directory if it does not exist.
    /// 2. Clean up leftover `.sst.tmp` files from interrupted flushes.
    /// 3. Replay the WAL into a fresh Memtable.
    /// 4. Open the WAL writer in append mode.
    /// 5. Load SSTables from the manifest (or scan directory for legacy DBs).
    /// 6. Determine the highest sequence number across WAL and SSTables.
    pub fn new<P1: AsRef<Path>, P2: AsRef<Path>>(
        wal_path: P1,
        sst_dir: P2,
        flush_threshold: usize,
        wal_sync: bool,
    ) -> Result<Self> {
        let wal_path = wal_path.as_ref().to_path_buf();
        let sst_dir = sst_dir.as_ref().to_path_buf();

        // ensure sst dir exists
        std::fs::create_dir_all(&sst_dir)?;

        // clean up any leftover .sst.tmp files from interrupted flushes
        Self::cleanup_tmp_files(&sst_dir);

        // replay wal into memtable and obtain last seq
        // (must happen BEFORE opening the writer to avoid file-sharing conflicts on Windows)
        let mut mem = Memtable::new();
        let seq = replay_wal_and_build(&wal_path, &mut mem)?;

        // open wal writer in append mode (after replay is done)
        let wal_writer = WalWriter::create(&wal_path, wal_sync)?;

        // Load or create the manifest to determine L0/L1 assignments.
        let mut manifest = Manifest::load_or_create(&sst_dir)?;

        let mut l0_sstables = Vec::new();
        let mut l1_sstables = Vec::new();
        let mut max_sst_seq = 0u64;

        // If the manifest has entries, use it to load SSTables into the
        // correct levels. This preserves L0/L1 assignments across restarts.
        if !manifest.entries.is_empty() {
            for filename in manifest.l0_filenames() {
                let path = sst_dir.join(filename);
                if path.exists() {
                    let reader = SSTableReader::open(&path)?;
                    max_sst_seq = max_sst_seq.max(Self::reader_max_seq(&reader));
                    l0_sstables.push(reader);
                }
            }
            for filename in manifest.l1_filenames() {
                let path = sst_dir.join(filename);
                if path.exists() {
                    let reader = SSTableReader::open(&path)?;
                    max_sst_seq = max_sst_seq.max(Self::reader_max_seq(&reader));
                    l1_sstables.push(reader);
                }
            }
        } else {
            // No manifest yet (fresh DB or pre-manifest upgrade).
            // Fall back to scanning the directory and loading all SSTables
            // into L0 (conservative - compaction will sort them out).
            let mut paths: Vec<_> = std::fs::read_dir(&sst_dir)?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map(|e| e == "sst").unwrap_or(false))
                .collect();

            // newest first (filename contains seq + timestamp)
            paths.sort();
            paths.reverse();

            for path in &paths {
                let reader = SSTableReader::open(path)?;
                max_sst_seq = max_sst_seq.max(Self::reader_max_seq(&reader));
                l0_sstables.push(reader);
            }

            // Bootstrap the manifest from the discovered files.
            if !paths.is_empty() {
                for path in &paths {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        manifest.add(name.to_string(), 0);
                    }
                }
                manifest.save()?;
            }
        }

        // seq must be the max of WAL seq and SSTable seq
        let seq = seq.max(max_sst_seq);

        Ok(Self {
            mem,
            l0_sstables,
            l1_sstables,
            wal_path,
            sst_dir,
            wal_writer,
            manifest,
            seq,
            flush_threshold,
            l0_compaction_trigger: DEFAULT_L0_COMPACTION_TRIGGER,
            wal_sync,
        })
    }

    /// Returns the current monotonic sequence number.
    #[must_use]
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Returns the current flush threshold in bytes.
    #[must_use]
    pub fn flush_threshold(&self) -> usize {
        self.flush_threshold
    }

    /// Updates the flush threshold. Useful for testing or runtime tuning.
    pub fn set_flush_threshold(&mut self, threshold: usize) {
        self.flush_threshold = threshold;
    }

    /// Returns the current L0 compaction trigger threshold.
    ///
    /// When the number of L0 SSTables reaches this value after a flush,
    /// compaction is triggered automatically. A value of 0 disables
    /// auto-compaction.
    #[must_use]
    pub fn l0_compaction_trigger(&self) -> usize {
        self.l0_compaction_trigger
    }

    /// Updates the L0 compaction trigger. Set to `0` to disable auto-compaction.
    pub fn set_l0_compaction_trigger(&mut self, trigger: usize) {
        self.l0_compaction_trigger = trigger;
    }

    /// Returns the total number of SSTables across all levels.
    #[must_use]
    pub fn sstable_count(&self) -> usize {
        self.l0_sstables.len() + self.l1_sstables.len()
    }

    /// Returns the number of L0 SSTables (from memtable flushes).
    #[must_use]
    pub fn l0_sstable_count(&self) -> usize {
        self.l0_sstables.len()
    }

    /// Returns the number of L1 SSTables (from compaction).
    #[must_use]
    pub fn l1_sstable_count(&self) -> usize {
        self.l1_sstables.len()
    }
}

/// Best-effort flush on drop.
///
/// When the `Engine` is dropped, any data remaining in the memtable is flushed
/// to an SSTable so it is not lost. Errors during the flush are silently
/// ignored because Drop cannot propagate errors — the data is still safe in
/// the WAL and will be recovered on the next startup.
impl Drop for Engine {
    fn drop(&mut self) {
        if !self.mem.is_empty() {
            let _ = self.flush();
        }
    }
}

#[cfg(test)]
mod tests;
