/// Write path: `set()`, `del()`, `force_flush()`, and the internal `flush()`.
///
/// All mutations flow through this module. Each write is first appended to the
/// WAL for durability, then applied to the in-memory Memtable. When the
/// Memtable exceeds the configured flush threshold, it is persisted to a new
/// SSTable on disk.
use anyhow::Result;
use std::fs::OpenOptions;
use std::time::{SystemTime, UNIX_EPOCH};
use wal::{WalRecord, WalWriter};

use crate::{Engine, SSTableReader, SSTableWriter, MAX_KEY_SIZE, MAX_VALUE_SIZE};

impl Engine {
    /// Inserts a key-value pair (the `SET` command).
    ///
    /// The operation is first appended to the WAL, then applied to the
    /// Memtable. If the Memtable exceeds the flush threshold, it is
    /// automatically flushed to a new SSTable.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        anyhow::ensure!(!key.is_empty(), "key must not be empty");
        anyhow::ensure!(
            key.len() <= MAX_KEY_SIZE,
            "key too large: {} bytes (max {})",
            key.len(),
            MAX_KEY_SIZE
        );
        anyhow::ensure!(
            value.len() <= MAX_VALUE_SIZE,
            "value too large: {} bytes (max {})",
            value.len(),
            MAX_VALUE_SIZE
        );

        self.seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("sequence number overflow (u64::MAX reached)"))?;
        let seq = self.seq;

        // Append to WAL first
        self.wal_writer.append(&WalRecord::Put {
            seq,
            key: key.clone(),
            value: value.clone(),
        })?;

        // Apply to memtable
        self.mem.put(key, value, seq);

        // Maybe flush memtable to SSTable
        if self.mem.approx_size() >= self.flush_threshold {
            self.flush()?;
        }

        Ok(())
    }

    /// Deletes a key by writing a tombstone (the `DEL` command).
    ///
    /// A tombstone record is appended to the WAL and inserted into the
    /// Memtable. The tombstone shadows any older value in SSTables.
    pub fn del(&mut self, key: Vec<u8>) -> Result<()> {
        anyhow::ensure!(!key.is_empty(), "key must not be empty");
        anyhow::ensure!(
            key.len() <= MAX_KEY_SIZE,
            "key too large: {} bytes (max {})",
            key.len(),
            MAX_KEY_SIZE
        );

        self.seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("sequence number overflow (u64::MAX reached)"))?;
        let seq = self.seq;

        self.wal_writer.append(&WalRecord::Del {
            seq,
            key: key.clone(),
        })?;

        self.mem.delete(key, seq);

        if self.mem.approx_size() >= self.flush_threshold {
            self.flush()?;
        }

        Ok(())
    }

    /// Forces a flush of the current Memtable to a new SSTable.
    ///
    /// This is a no-op if the memtable is empty. After flushing, the WAL is
    /// truncated and the memtable is reset. If auto-compaction is enabled and
    /// the L0 count reaches the trigger, compaction runs automatically.
    ///
    /// # Errors
    ///
    /// Returns an error on I/O failure during SSTable write, manifest update,
    /// or WAL truncation.
    pub fn force_flush(&mut self) -> Result<()> {
        if self.mem.is_empty() {
            return Ok(());
        }
        self.flush()
    }

    /// Internal flush implementation. Callers should use [`force_flush`] for
    /// the public API or rely on the automatic flush in `set`/`del`.
    ///
    /// # Steps
    ///
    /// 1. Generate a unique filename: `sst-{seq}-{timestamp_ms}.sst`.
    /// 2. Write the SSTable via [`SSTableWriter::write_from_memtable`]
    ///    (atomic temp + rename).
    /// 3. Update the manifest atomically.
    /// 4. Truncate the WAL to zero bytes.
    /// 5. Create a fresh [`WalWriter`] in append mode.
    /// 6. Replace the Memtable with an empty one.
    /// 7. Open the new SSTable and insert it at position 0 (newest).
    /// 8. Trigger auto-compaction if the L0 count reaches the threshold.
    pub(crate) fn flush(&mut self) -> Result<()> {
        // choose filename using current seq and timestamp so it's monotonic
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();

        let sst_name = format!("sst-{:020}-{}.sst", self.seq, ts);
        let sst_path = self.sst_dir.join(&sst_name);

        // write sstable (this writes to temp and rename inside)
        SSTableWriter::write_from_memtable(&sst_path, &self.mem)?;

        // Record the new SSTable in the manifest and persist atomically.
        self.manifest.add(sst_name, 0);
        self.manifest.save()?;

        // Successfully wrote SSTable and manifest; now safely truncate the WAL.
        let _f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.wal_path)?;

        // create a fresh WalWriter (append mode)
        self.wal_writer = WalWriter::create(&self.wal_path, self.wal_sync)?;

        // reset memtable (reuses existing allocation)
        self.mem.clear();

        let reader = SSTableReader::open(&sst_path)?;
        self.l0_sstables.insert(0, reader);

        // Auto-compaction: if the L0 count has reached the trigger threshold,
        // merge all L0 + L1 SSTables into a single L1 SSTable. This keeps
        // read amplification bounded without requiring the caller to manually
        // invoke compact().
        if self.l0_compaction_trigger > 0 && self.l0_sstables.len() >= self.l0_compaction_trigger {
            self.compact()?;
        }

        Ok(())
    }
}
