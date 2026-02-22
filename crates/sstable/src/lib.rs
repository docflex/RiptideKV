//! # SSTable - Sorted String Table
//!
//! Immutable, on-disk storage files for the RiptideKV storage engine.
//!
//! When the in-memory [`memtable::Memtable`] exceeds its size threshold the
//! engine flushes it to disk as an SSTable. SSTables are *write-once,
//! read-many* — once created they are never modified (only replaced during
//! compaction).
//!
//! ## File layout (v3 – current)
//!
//! ```text
//! ┌───────────────────────────────────────────────────────────────┐
//! │ DATA SECTION (sorted key/value records)                        │
//! │                                                               │
//! │ crc32 (u32) | key_len (u32) | key | seq (u64)                 │
//! │ present (u8) | [val_len (u32) | val]                           │
//! │                                                               │
//! │ ... repeated for each entry ...                                │
//! │                                                               │
//! │ The CRC32 covers everything after itself in the               │
//! │ record (key_len through end of value). This detects           │
//! │ silent disk corruption on reads.                              │
//! ├───────────────────────────────────────────────────────────────┤
//! │ BLOOM SECTION (serialized BloomFilter)                         │
//! │                                                               │
//! │ num_bits (u64) | num_hashes (u32)                              │
//! │ bits_len (u32) | bits (bytes)                                 │
//! ├───────────────────────────────────────────────────────────────┤
//! │ INDEX SECTION (key -> data_offset mapping)                     │
//! │                                                               │
//! │ key_len (u32) | key | data_offset (u64)                        │
//! │                                                               │
//! │ ... repeated for each entry ...                                │
//! ├───────────────────────────────────────────────────────────────┤
//! │ FOOTER (always last 28 bytes)                                  │
//! │                                                               │
//! │ max_seq (u64 LE) | bloom_offset (u64 LE)                       │
//! │ index_offset (u64 LE) | magic (u32 LE) "SST3"                 │
//! └───────────────────────────────────────────────────────────────┘
//! ```
//!
//! All integers are little-endian. The magic value `0x5353_5433` ("SST3")
//! identifies v3. The reader also supports v1 files (magic `SST1`, 12-byte
//! footer, no bloom/CRC) and v2 files (magic `SST2`, 20-byte footer, bloom
//! but no CRC) for backward compatibility.
//!
//! ## Version history
//!
//! | Version | Magic | Footer | Features                          |
//! |---------|-------|--------|-----------------------------------|
//! | v1      | `SST1`| 12 B   | Basic DATA + INDEX                |
//! | v2      | `SST2`| 20 B   | + Bloom filter section             |
//! | v3      | `SST3`| 28 B   | + Per-record CRC32, max_seq in footer |

mod format;
mod merge;
mod reader;
mod writer;

pub use format::{FOOTER_BYTES, FOOTER_BYTES_V2, FOOTER_BYTES_V3, SSTABLE_MAGIC, SSTABLE_MAGIC_V2, SSTABLE_MAGIC_V3};
pub use merge::MergeIterator;
pub use reader::SSTableReader;
pub use writer::SSTableWriter;

#[cfg(test)]
mod tests;
