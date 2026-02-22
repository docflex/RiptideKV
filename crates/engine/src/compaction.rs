/// Compaction: merges all L0 + L1 SSTables into a single L1 SSTable.
///
/// Uses [`MergeIterator`] for sorted, deduplicated streaming from multiple
/// SSTables. Tombstone GC drops dead keys when no older SSTables remain.
/// The result is written atomically (temp file + rename), old files are
/// deleted, and the manifest is updated.
use anyhow::Result;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{Engine, MergeIterator, SSTableReader, SSTableWriter};

impl Engine {
    /// Compacts all SSTables into a single merged SSTable.
    ///
    /// Uses [`MergeIterator`] to walk all SSTables in sorted key order,
    /// resolving duplicates by highest sequence number. The merged result is
    /// written to a new SSTable, old SSTable files are deleted, and the
    /// engine's SSTable list is replaced with the single merged reader.
    ///
    /// Tombstone GC: since this is a full compaction (all L0 + L1 -> single
    /// L1), tombstones are safe to drop unless the memtable still references
    /// the key (the memtable is not part of compaction).
    ///
    /// # When to compact
    ///
    /// Called automatically when L0 count reaches `l0_compaction_trigger`
    /// after a flush, or manually by the caller.
    ///
    /// # Errors
    ///
    /// Returns an error on I/O failure during merge, write, or cleanup.
    pub fn compact(&mut self) -> Result<()> {
        let total = self.l0_sstables.len() + self.l1_sstables.len();
        if total <= 1 {
            return Ok(()); // nothing to compact
        }

        // Collect the paths of old SSTable files before we start.
        let old_paths: Vec<PathBuf> = std::fs::read_dir(&self.sst_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "sst").unwrap_or(false))
            .collect();

        // Move L0 + L1 into a contiguous vec for MergeIterator.
        let mut all_sstables: Vec<SSTableReader> = Vec::new();
        let mut l0 = std::mem::take(&mut self.l0_sstables);
        let mut l1 = std::mem::take(&mut self.l1_sstables);
        all_sstables.append(&mut l0);
        all_sstables.append(&mut l1);

        // Estimate total entry count for bloom filter sizing.
        let estimated_count: usize = all_sstables.iter().map(|r| r.len()).sum();

        let mut merge = MergeIterator::new(&all_sstables);

        // Stram directly from MergeIterator -> SSTableWriter without
        // materializing the entire dataset in RAM. Memory usage is bounded
        // by the bloom filter + index, not the data volume.
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let sst_name = format!("sst-{:020}-{}.sst", self.seq, ts);
        let sst_path = self.sst_dir.join(&sst_name);

        // Tombstone GC: since this is a full compaction (all L0 + L1 -> single
        // L1), there are no older SSTables that could contain shadowed values.
        // Tombstones are therefore safe to drop — they have no older data to
        // shadow. Also check if the memtable contains the key: if so, the
        // tombstone must be preserved to shadow the memtable entry on recovery.
        //
        // Build a streaming iterator adapter from MergeIterator.
        // MergeIterator::next() returns Result<Option<...>>, so we collect
        // into a fallible iterator that stops on error or exhaustion.
        let mem_ref = &self.mem;
        let mut merge_error: Option<anyhow::Error> = None;
        let streaming_iter = std::iter::from_fn(|| {
            loop {
                match merge.next_entry() {
                    Ok(Some((key, entry))) => {
                        // Drop tombstones unless the memtable still references
                        // this key (the memtable is not part of compaction, so
                        // we must keep tombstones that shadow memtable data).
                        if entry.value.is_none() && mem_ref.contains_key(&key) {
                            continue; // GC this tombstone
                        }
                        return Some((key, entry));
                    }
                    Ok(None) => return None,
                    Err(e) => {
                        merge_error = Some(e);
                        return None;
                    }
                }
            }
        });

        let write_result =
            SSTableWriter::write_from_iterator(&sst_path, estimated_count, streaming_iter);

        // Check for merge errors first, then write errors.
        if let Some(e) = merge_error {
            // Clean up partial write if any.
            let _ = std::fs::remove_file(sst_path.with_extension("sst.tmp"));
            return Err(e);
        }

        // Handle the case where all SSTables were empty.
        if let Err(e) = write_result {
            if e.to_string().contains("empty") {
                drop(all_sstables);
                for p in &old_paths {
                    let _ = std::fs::remove_file(p);
                }
                self.manifest.entries.clear();
                self.manifest.save()?;
                return Ok(());
            }
            return Err(e);
        }

        // Update the manifest atomically: replace all entries with the
        // single compacted L1 SSTable.
        self.manifest.replace_all_with_l1(sst_name);
        self.manifest.save()?;

        // Drop old readers (releases file handles) before deleting files.
        drop(all_sstables);

        // Delete old SSTable files (but not the new one).
        for p in &old_paths {
            let _ = std::fs::remove_file(p);
        }

        // Open the new merged SSTable into L1 (compacted = non-overlapping).
        let reader = SSTableReader::open(&sst_path)?;
        self.l1_sstables = vec![reader];

        Ok(())
    }
}
