//! # Memtable
//!
//! An in-memory, sorted, mutable write buffer for the RiptideKV storage engine.
//!
//! The memtable is the first point of contact for every write operation. It buffers
//! recent `PUT` and `DELETE` operations in a sorted structure (`BTreeMap`) before
//! they are flushed to immutable on-disk SSTables.
//!
//! ## Key properties
//! - **Sorted order**: entries are always in ascending key order (required for SSTable flush).
//! - **Sequence-number gated**: stale writes (lower sequence number) are silently rejected.
//! - **Tombstone support**: deletes are recorded as `ValueEntry { value: None }` markers.
//! - **Approximate size tracking**: tracks the byte size of keys + values for flush threshold decisions.
//!
//! ## Example
//! ```rust
//! use memtable::Memtable;
//!
//! let mut m = Memtable::new();
//! m.put(b"hello".to_vec(), b"world".to_vec(), 1);
//! assert_eq!(m.get(b"hello").unwrap().1, b"world".to_vec());
//!
//! m.delete(b"hello".to_vec(), 2);
//! assert!(m.get(b"hello").is_none());
//! ```

use std::collections::BTreeMap;

/// A single entry in the memtable, pairing a sequence number with an optional value.
///
/// - `value == Some(bytes)` — the key holds a live value.
/// - `value == None` — the key has been deleted (tombstone).
///
/// Tombstones are retained in the memtable and flushed to SSTables so that
/// older values in lower levels are correctly shadowed during reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueEntry {
    /// Monotonically increasing sequence number assigned at write time.
    pub seq: u64,
    /// `Some(bytes)` for live values, `None` for tombstones (deletes).
    pub value: Option<Vec<u8>>,
}

/// An ordered, in-memory write buffer backed by a `BTreeMap`.
///
/// The memtable tracks an approximate byte size (keys + values) so the engine
/// can decide when to flush to an SSTable. Sequence numbers gate every mutation:
/// a write with a sequence number <= the existing entry's sequence is silently
/// dropped, ensuring consistency during WAL replay and concurrent recovery.
#[derive(Debug)]
pub struct Memtable {
    map: BTreeMap<Vec<u8>, ValueEntry>,
    approx_size: usize,
}

impl Memtable {
    /// Creates a new, empty memtable.
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            approx_size: 0,
        }
    }

    /// Inserts a key-value pair with the given sequence number.
    ///
    /// If the key already exists with a **newer or equal** sequence number, the
    /// write is silently ignored (stale-write protection). Otherwise the old
    /// entry is replaced and `approx_size` is adjusted accordingly.
    ///
    /// # Arguments
    ///
    /// * `key` - the lookup key (ownership transferred to the memtable).
    /// * `value` - the payload bytes (ownership transferred).
    /// * `seq` - monotonically increasing sequence number.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, seq: u64) {
        match self.map.get(&key) {
            Some(old) if old.seq >= seq => {
                // stale or equal write, ignore
                return;
            }
            Some(old) => {
                // Replace existing entry: remove old value bytes from approx_size if present.
                if let Some(ref ov) = old.value {
                    self.approx_size = self.approx_size.saturating_sub(ov.len());
                }
                // Key bytes already counted; do not subtract key length here.
            }
            None => {
                // New key: count key bytes
                self.approx_size = self.approx_size.saturating_add(key.len());
            }
        }

        // Add new value bytes
        self.approx_size = self.approx_size.saturating_add(value.len());

        self.map.insert(
            key,
            ValueEntry {
                seq,
                value: Some(value),
            },
        );
    }

    /// Records a tombstone (delete marker) for the given key.
    ///
    /// A tombstone is stored as `ValueEntry { seq, value: None }`. It shadows
    /// any older value both in the memtable and in SSTables during reads.
    ///
    /// Stale-write protection applies: if the key already has a newer or equal
    /// sequence number, the delete is silently ignored.
    pub fn delete(&mut self, key: Vec<u8>, seq: u64) {
        match self.map.get(&key) {
            Some(old) if old.seq >= seq => {
                // existing newer or equal entry; ignore
                return;
            }
            Some(old) => {
                // If there was a live value, subtract its size (key stays counted)
                if let Some(ref ov) = old.value {
                    self.approx_size = self.approx_size.saturating_sub(ov.len());
                }
                // Leave key bytes counted (they were already counted when the key first appeared)
            }
            None => {
                // New tombstone for a key we haven't seen — count the key bytes
                self.approx_size = self.approx_size.saturating_add(key.len());
            }
        }

        self.map.insert(key, ValueEntry { seq, value: None });
    }

    /// Returns a borrowed reference to the value for the given key if it exists
    /// and is **not** a tombstone.
    ///
    /// Returns `Some((seq, value_bytes))` for live entries, `None` for missing
    /// keys or tombstones. Callers should `.clone()` only when ownership is needed.
    ///
    /// **Prefer [`get_entry`](Memtable::get_entry)** when you need to distinguish
    /// between "key not found" and "key was deleted" (tombstone).
    pub fn get(&self, key: &[u8]) -> Option<(u64, &[u8])> {
        self.map
            .get(key)
            .and_then(|e| e.value.as_deref().map(|v| (e.seq, v)))
    }

    /// Returns an iterator over all entries in **ascending key order**.
    ///
    /// This includes tombstones. The ordering guarantee is provided by the
    /// underlying `BTreeMap` and is required for correct SSTable flush.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &ValueEntry)> {
        self.map.iter().map(|(k, v)| (k.as_slice(), v))
    }

    /// Returns the number of entries (including tombstones).
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns the approximate byte size of all keys and values stored.
    ///
    /// This is used by the engine to decide when to flush the memtable to an
    /// SSTable. The size tracks key bytes + value bytes but does **not** include
    /// `BTreeMap` node overhead.
    #[must_use]
    pub fn approx_size(&self) -> usize {
        self.approx_size
    }

    /// Returns `true` if the memtable contains zero entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns the raw [`ValueEntry`] for the given key, if present.
    ///
    /// Unlike [`get`], this does **not** filter out tombstones. The engine uses
    /// this to distinguish between "key not found" (returns `None`) and
    /// "key was deleted" (returns `Some(ValueEntry { value: None })`).
    pub fn get_entry(&self, key: &[u8]) -> Option<&ValueEntry> {
        self.map.get(key)
    }

    /// Returns `true` if the memtable contains the given key (including tombstones).
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.map.contains_key(key)
    }

    /// Removes all entries and resets `approx_size` to zero.
    ///
    /// This is semantically equivalent to replacing the memtable with
    /// `Memtable::new()`, but reuses the existing allocations.
    pub fn clear(&mut self) {
        self.map.clear();
        self.approx_size = 0;
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
