//! Minimal SSTable writer (v1).
//! This crate implements a simple, correct SSTable writer that writes sorted
//! key/ValueEntry records and a small in-memory index in the footer.
//!
//! The format is intentionally simple for Phase 1:
//! [DATA RECORDS][INDEX][FOOTER]
//!
//! FOOTER = u64 index_offset, u32 magic("SST1")

mod format;
mod reader;
mod writer;

pub use reader::SsTableReader;
pub use writer::SsTableWriter;
