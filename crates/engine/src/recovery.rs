/// WAL replay and SSTable recovery logic.
///
/// This module handles the cold-start path: replaying the WAL into a fresh
/// memtable, loading existing SSTables from disk, and bootstrapping the
/// manifest when upgrading from a pre-manifest database.
use anyhow::Result;
use memtable::Memtable;
use std::path::Path;
use wal::{WalReader, WalRecord};

use crate::{Engine, SSTableReader};

/// Replays a WAL file into the given memtable, returning the highest sequence
/// number encountered.
///
/// If the WAL file does not exist, returns `Ok(0)` (fresh start).
///
/// # Errors
///
/// Propagates any I/O or corruption error from [`WalReader::replay`].
pub fn replay_wal_and_build<P: AsRef<Path>>(path: P, mem: &mut Memtable) -> Result<u64> {
    match WalReader::open(path.as_ref()) {
        Ok(mut reader) => {
            let mut max_seq = 0u64;

            reader.replay(|r| match r {
                WalRecord::Put { seq, key, value } => {
                    mem.put(key, value, seq);
                    max_seq = max_seq.max(seq);
                }
                WalRecord::Del { seq, key } => {
                    mem.delete(key, seq);
                    max_seq = max_seq.max(seq);
                }
            })?;

            Ok(max_seq)
        }
        Err(e) => {
            // File doesn't exist yet -> fresh start
            if matches!(e, wal::WalError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::NotFound)
            {
                Ok(0)
            } else {
                Err(anyhow::anyhow!(e).context("failed to open WAL for replay"))
            }
        }
    }
}

impl Engine {
    /// Extracts the max sequence number from an SSTable reader.
    ///
    /// Uses the v3 footer's `max_seq` for O(1) access when available.
    /// Falls back to scanning all keys for legacy v1/v2 SSTables.
    pub(crate) fn reader_max_seq(reader: &SSTableReader) -> u64 {
        if let Some(seq) = reader.max_seq() {
            return seq;
        }
        let mut max = 0u64;
        for key in reader.keys() {
            if let Ok(Some(entry)) = reader.get(key) {
                max = max.max(entry.seq);
            }
        }
        max
    }

    /// Cleans up leftover `.sst.tmp` files from interrupted flushes.
    pub(crate) fn cleanup_tmp_files(sst_dir: &Path) {
        if let Ok(entries) = std::fs::read_dir(sst_dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                    if name.ends_with(".sst.tmp") {
                        let _ = std::fs::remove_file(&p);
                    }
                }
            }
        }
    }
}
