/// Read path: get() and scan().
///
/// Point lookups check the memtable first (freshest data), then L0 SSTables
/// (newest-first, may overlap), then L1 SSTables (newest-first, non-overlapping).
/// The first match wins; tombstones shadow older values.
///
/// Range scans merge data from all sources, deduplicate by highest sequence
/// number, and filter out tombstones before returning sorted results.

use anyhow::Result;
use memtable::ValueEntry;
use std::collections::BTreeMap;

use crate::Engine;

impl Engine {
    /// Looks up a key, returning `Some((seq, value))` if found and live.
    ///
    /// The read path checks the Memtable first, then SSTables from newest to
    /// oldest. Tombstones in any layer shadow older values, causing `None` to
    /// be returned.
    ///
    /// # Errors
    ///
    /// Returns an error if any SSTable read fails (e.g. corruption, I/O).
    pub fn get(&self, key: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        // 1. Check memtable FIRST (and respect tombstones)
        if let Some(entry) = self.mem.get_entry(key) {
            return Ok(entry.value.as_ref().map(|v| (entry.seq, v.clone())));
        }

        // 2. Check L0 SSTables (newest -> oldest, may overlap)
        for sst in &self.l0_sstables {
            match sst.get(key) {
                Ok(Some(entry)) => {
                    return Ok(match entry.value {
                        Some(v) => Some((entry.seq, v)),
                        None => None, // tombstone hides older values
                    });
                }
                Ok(None) => continue,
                Err(e) => return Err(e),
            }
        }

        // 3. Check L1 SSTables (newest -> oldest, non-overlapping)
        for sst in &self.l1_sstables {
            match sst.get(key) {
                Ok(Some(entry)) => {
                    return Ok(match entry.value {
                        Some(v) => Some((entry.seq, v)),
                        None => None,
                    });
                }
                Ok(None) => continue,
                Err(e) => return Err(e),
            }
        }

        // 4. Not found anywhere
        Ok(None)
    }

    /// Scans a range of keys, returning all live key-value pairs in ascending
    /// key order.
    ///
    /// The scan merges data from the memtable and all SSTable levels, resolving
    /// duplicates by keeping the entry with the highest sequence number.
    /// Tombstones are filtered out — only live values are returned.
    ///
    /// # Arguments
    ///
    /// * `start` — inclusive lower bound of the key range. Pass `b""` to start
    ///   from the beginning.
    /// * `end` — exclusive upper bound of the key range. Pass `b""` to scan to
    ///   the end.
    ///
    /// # Returns
    ///
    /// A `Vec<(Vec<u8>, Vec<u8>)>` of `(key, value)` pairs in ascending key
    /// order. Empty if no live keys exist in the range.
    ///
    /// # Errors
    ///
    /// Returns an error if any SSTable read fails.
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Collect the best (highest-seq) entry per key across all sources.
        // BTreeMap ensures ascending key order in the output.
        let mut merged: BTreeMap<Vec<u8>, ValueEntry> = BTreeMap::new();

        // Helper: insert only if this entry has a higher seq than any existing one.
        let mut merge_entry = |key: Vec<u8>, entry: ValueEntry| {
            match merged.get(&key) {
                Some(existing) if existing.seq >= entry.seq => {}
                _ => {
                    merged.insert(key, entry);
                }
            }
        };

        // 1. Memtable entries (highest priority — freshest data).
        for (key, entry) in self.mem.iter() {
            if !start.is_empty() && key < start {
                continue;
            }
            if !end.is_empty() && key >= end {
                continue;
            }
            merge_entry(key.to_vec(), entry.clone());
        }

        // 2. L0 SSTables (newest first, may overlap).
        for sst in &self.l0_sstables {
            for key_ref in sst.keys() {
                if !start.is_empty() && key_ref < start {
                    continue;
                }
                if !end.is_empty() && key_ref >= end {
                    continue;
                }
                if let Ok(Some(entry)) = sst.get(key_ref) {
                    merge_entry(key_ref.to_vec(), entry);
                }
            }
        }

        // 3. L1 SSTables (newest first, non-overlapping).
        for sst in &self.l1_sstables {
            for key_ref in sst.keys() {
                if !start.is_empty() && key_ref < start {
                    continue;
                }
                if !end.is_empty() && key_ref >= end {
                    continue;
                }
                if let Ok(Some(entry)) = sst.get(key_ref) {
                    merge_entry(key_ref.to_vec(), entry);
                }
            }
        }

        // Filter out tombstones and collect live values.
        let result: Vec<(Vec<u8>, Vec<u8>)> = merged
            .into_iter()
            .filter_map(|(key, entry)| entry.value.map(|v| (key, v)))
            .collect();

        Ok(result)
    }
}
