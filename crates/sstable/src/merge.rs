//! Merge iterator over multiple [`SSTableReader`]s.
//!
//! Produces `(key, ValueEntry)` pairs in ascending key order. When the same
//! key appears in multiple SSTables, only the entry with the **highest
//! sequence number** is emitted (newest wins).
//!
//! This is the core primitive for compaction: walk N input SSTables in sorted
//! order, deduplicate by seq, and write the result to a new SSTable.

use anyhow::Result;
use memtable::ValueEntry;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::SSTableReader;

/// A pending key from one SSTable source, used for heap-based merge ordering.
///
/// Only the `key` and `source` are stored — the actual [`ValueEntry`] is read
/// lazily from disk when the key reaches the top of the heap. This keeps heap
/// entries lightweight.
struct HeapEntry {
    key: Vec<u8>,
    /// Index into the `readers` / `key_iters` arrays.
    source: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source == other.source
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; we want the *smallest* key first,
        // so reverse the key comparison. On tie, prefer the entry from
        // the source with the lower index (arbitrary but deterministic).
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.source.cmp(&self.source))
    }
}

/// Merges multiple SSTables into a single sorted stream of `(key, ValueEntry)`.
///
/// Duplicate keys are resolved by keeping only the entry with the highest
/// sequence number. The iterator is lazy — it reads one key at a time from
/// each source SSTable.
pub struct MergeIterator<'a> {
    readers: &'a [SSTableReader],
    /// Per-reader: sorted keys remaining to be yielded.
    key_iters: Vec<std::vec::IntoIter<Vec<u8>>>,
    heap: BinaryHeap<HeapEntry>,
}

impl<'a> MergeIterator<'a> {
    /// Creates a new merge iterator over the given SSTable readers.
    ///
    /// Each reader's keys are loaded into memory (they're already in the
    /// in-memory index). The first key from each reader is pushed onto a
    /// min-heap.
    pub fn new(readers: &'a [SSTableReader]) -> Self {
        let mut key_iters: Vec<std::vec::IntoIter<Vec<u8>>> = Vec::with_capacity(readers.len());
        let mut heap = BinaryHeap::new();

        for (i, reader) in readers.iter().enumerate() {
            let keys: Vec<Vec<u8>> = reader.keys().map(|k| k.to_vec()).collect();
            let mut iter = keys.into_iter();
            if let Some(first_key) = iter.next() {
                heap.push(HeapEntry {
                    key: first_key,
                    source: i,
                });
            }
            key_iters.push(iter);
        }

        Self {
            readers,
            key_iters,
            heap,
        }
    }

    /// Returns the next `(key, ValueEntry)` in sorted order, or `None` when
    /// all sources are exhausted.
    ///
    /// Duplicate keys (same key from multiple SSTables) are resolved by
    /// keeping only the entry with the highest sequence number.
    pub fn next_entry(&mut self) -> Result<Option<(Vec<u8>, ValueEntry)>> {
        loop {
            let top = match self.heap.pop() {
                Some(e) => e,
                None => return Ok(None),
            };

            // Read the actual entry from disk
            let entry = self.readers[top.source].get(&top.key)?;

            // Advance this source's iterator.
            if let Some(next_key) = self.key_iters[top.source].next() {
                self.heap.push(HeapEntry {
                    key: next_key,
                    source: top.source,
                });
            }

            let entry = match entry {
                Some(e) => e,
                None => continue, // shouldn't happen, but skip
            };

            // Skip duplicates: drain all heap entries with the same key,
            // keeping only the one with the highest seq.
            let best_key = top.key;
            let mut best_entry = entry;

            while let Some(peek) = self.heap.peek() {
                if peek.key != best_key {
                    break;
                }
                let dup = self.heap.pop().unwrap();

                // Read entry for the duplicate
                if let Ok(Some(dup_entry)) = self.readers[dup.source].get(&dup.key) {
                    if dup_entry.seq > best_entry.seq {
                        best_entry = dup_entry;
                    }
                }

                // Advance this source's iterator.
                if let Some(next_key) = self.key_iters[dup.source].next() {
                    self.heap.push(HeapEntry {
                        key: next_key,
                        source: dup.source,
                    });
                }
            }

            return Ok(Some((best_key, best_entry)));
        }
    }

    /// Collects all remaining entries into a `Vec`.
    ///
    /// Useful for testing and for building a merged memtable for compaction.
    pub fn collect_all(&mut self) -> Result<Vec<(Vec<u8>, ValueEntry)>> {
        let mut result = Vec::new();
        while let Some(pair) = self.next_entry()? {
            result.push(pair);
        }
        Ok(result)
    }
}
