use anyhow::Result;
use bloom::BloomFilter;
use byteorder::{LittleEndian, WriteBytesExt};
use crc32fast::Hasher as Crc32;
use memtable::{Memtable, ValueEntry};
use std::fs::{rename, OpenOptions};
use std::io::{BufWriter, Seek, Write};
use std::path::Path;

use crate::format::write_footer_v3;

/// Default bloom filter false positive rate (1%).
const BLOOM_FPR: f64 = 0.01;

/// Writes a [`Memtable`] to disk as an immutable SSTable file.
///
/// The writer is stateless — all work happens inside the single static method
/// [`write_from_memtable`](SSTableWriter::write_from_memtable). The write is
/// crash-safe: data is first written to a temporary file, fsynced, and then
/// atomically renamed to the final path.
pub struct SSTableWriter {}

impl SSTableWriter {
    /// Flushes `mem` to a new SSTable file at `path`.
    ///
    /// # File Layout (v3)
    ///
    /// ```text
    /// [DATA]  repeated: crc32(u32) | key_len(u32) | key | seq(u64) | present(u8) | [val_len(u32) | val]
    /// [BLOOM] serialized BloomFilter (num_bits + num_hashes + bits)
    /// [INDEX] repeated: key_len(u32) | key | data_offset(u64)
    /// [FOOTER] max_seq(u64) | bloom_offset(u64) | index_offset(u64) | magic(u32 = "SST3")
    /// ```
    ///
    /// The CRC32 covers everything after itself in the record (key_len through
    /// end of value). This detects silent disk corruption on reads.
    ///
    /// # Crash Safety
    ///
    /// Writes to `path.sst.tmp`, calls `sync_all()`, then atomically renames.
    /// If the process crashes mid-write the temp file is left behind and
    /// ignored on recovery.
    ///
    /// # Errors
    ///
    /// Returns an error if the memtable is empty (writing an empty SSTable is
    /// not useful and likely indicates a logic bug) or on any I/O failure.
    pub fn write_from_memtable(path: &Path, mem: &Memtable) -> Result<()> {
        if mem.is_empty() {
            anyhow::bail!("refusing to write an empty SSTable (empty memtable)");
        }
        let iter = mem.iter().map(|(k, v)| (k.to_vec(), v.clone()));
        Self::write_internal(path, mem.len(), iter)
    }

        /// Writes an SSTable from an iterator of `(key, ValueEntry)` pairs.
    ///
    /// This is the **streaming compaction** entry point. Unlike
    /// [`write_from_memtable`](SSTableWriter::write_from_memtable), this method
    /// does not require the entire dataset to be materialized in a `Memtable`.
    /// Entries are consumed one at a time and written directly to disk, keeping
    /// memory usage proportional to the bloom filter + index (not the data).
    ///
    /// # Arguments
    ///
    /// * `path` – destination `.sst` file path.
    /// * `expected_count` – estimated number of entries (used to size the bloom
    ///   filter). Over-estimating is safe; under-estimating increases FPR.
    /// * `iter` – an iterator yielding `(key, ValueEntry)` in **sorted key
    ///   order** (ascending). The caller is responsible for deduplication.
    ///
    /// # Errors
    ///
    /// Returns an error if the iterator yields zero entries or on I/O failure.
    pub fn write_from_iterator<I>(path: &Path, expected_count: usize, iter: I) -> Result<()>
    where
        I: Iterator<Item = (Vec<u8>, ValueEntry)>,
    {
        Self::write_internal(path, expected_count.max(1), iter)
    }

    /// Internal write implementation shared by both `write_from_memtable` and
    /// `write_from_iterator`.
    ///
    /// Accepts any iterator of `(Vec<u8>, ValueEntry)` pairs. The iterator
    /// must yield entries in ascending key order.
    fn write_internal<I>(path: &Path, expected_count: usize, iter: I) -> Result<()>
    where
        I: Iterator<Item = (Vec<u8>, ValueEntry)>,
    {
        // Create temporary file next to target for atomic rename later
        let tmp_path = path.with_extension("sst.tmp");
        let raw_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut file = BufWriter::new(raw_file);

        // Build bloom filter from all keys
        let mut bloom = BloomFilter::new(expected_count.max(1), BLOOM_FPR);

        // Keep an in-memory index: (key, offset)
        let mut index: Vec<(Vec<u8>, u64)> = Vec::new();

        // Track max sequence number for the v3 footer.
        let mut max_seq: u64 = 0;

        // Reusable buffer for computing per-record CRC32 checksums.
        let mut record_buf: Vec<u8> = Vec::with_capacity(256);

                // Write DATA section
        for (key, entry) in iter {
            max_seq = max_seq.max(entry.seq);

            // Build the record body into a buffer so we can CRC it.
            record_buf.clear();
            record_buf.write_u32::<LittleEndian>(key.len() as u32)?;
            record_buf.extend_from_slice(&key);
            record_buf.write_u64::<LittleEndian>(entry.seq)?;
            match &entry.value {
                Some(v) => {
                    record_buf.write_u8(1)?;
                    record_buf.write_u32::<LittleEndian>(v.len() as u32)?;
                    record_buf.extend_from_slice(v);
                }
                None => {
                    record_buf.write_u8(0)?;
                }
            }

            // Compute CRC32 over the record body.
            let mut hasher = Crc32::new();
            hasher.update(&record_buf);
            let crc = hasher.finalize();

            // Write: [crc32][record body]
            let offset = file.stream_position()?;
            file.write_u32::<LittleEndian>(crc)?;
            file.write_all(&record_buf)?;

            // Insert key into bloom filter
            bloom.insert(&key);

            // record in index (offset points to the CRC prefix)
            index.push((key, offset));
        }

        if index.is_empty() {
            // Clean up the temp file and bail — nothing to write.
            drop(file);
            let _ = std::fs::remove_file(&tmp_path);
            anyhow::bail!("refusing to write an empty SSTable (no entries)");
        }

                // Write BLOOM section
        let bloom_offset = file.stream_position()?;
        bloom.write_to(&mut file)?;

        // Write INDEX section and remember its offset
        let index_offset = file.stream_position()?;

        for (key, data_offset) in &index {
            file.write_u32::<LittleEndian>(key.len() as u32)?;
            file.write_all(key)?;
            file.write_u64::<LittleEndian>(*data_offset)?;
        }

        // Write v3 FOOTER (max_seq + bloom_offset + index_offset + magic)
        write_footer_v3(&mut file, max_seq, bloom_offset, index_offset)?;

        // Flush BufWriter, then sync the underlying file
        file.flush()?;
        file.into_inner()?.sync_all()?;

        // Atomically move into place
        rename(&tmp_path, path)?;

        // Fsync the parent directory to ensure the rename is durable.
        // On NTFS this is a no-op (metadata is journaled), but on ext4/XFS
        // a crash after rename but before dir sync can lose the entry.
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }

        Ok(())
    }
}

// #[cfg(test)]
// #[path = "writer_tests.rs"]
// mod tests;
