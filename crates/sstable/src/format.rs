//! SSTable binary format constants and footer read/write helpers.
//!
//! ## v1 footer (12 bytes) - magic `SST1` (`0x5353_5431`)
//!
//! ```text
//! [index_offset: u64 LE][magic: u32 LE]
//! ```
//!
//! ## v2 footer (20 bytes) - magic `SST2` (`0x5353_5432`)
//!
//! ```text
//! [bloom_offset: u64 LE][index_offset: u64 LE][magic: u32 LE]
//! ```
//!
//! ## v3 footer (28 bytes) - magic `SST3` (`0x5353_5433`)
//!
//! ```text
//! [max_seq: u64 LE][bloom_offset: u64 LE][index_offset: u64 LE][magic: u32 LE]
//! ```
//!
//! v3 also adds a CRC32 checksum per data record for end-to-end integrity.
//!
//! The reader detects the version by reading the last 4 bytes (magic) first,
//! then seeking back to read the appropriate footer size.

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};

/// Magic number identifying SSTable v1 files (ASCII "SST1").
pub const SSTABLE_MAGIC_V1: u32 = 0x5353_5431;

/// Magic number identifying SSTable v2 files (ASCII "SST2").
pub const SSTABLE_MAGIC_V2: u32 = 0x5353_5432;

/// Magic number identifying SSTable v3 files (ASCII "SST3").
/// 
/// v3 adds per-record CRC32 checksums in the data section and stores
/// `max_seq` in the footer for O(1) sequence-number recovery.
pub const SSTABLE_MAGIC_V3: u32 = 0x5353_5433;

/// Size of the v1 footer in bytes: 8 (`index_offset`) + 4 (`magic`).
pub const FOOTER_BYTES_V1: u64 = 8 + 4;

/// Size of the v2 footer in bytes: 8 (`bloom_offset`) + 8 (`index_offset`) + 4 (`magic`).
pub const FOOTER_BYTES_V2: u64 = 8 + 8 + 4;

/// Size of the v3 footer in bytes: 8 (`max_seq`) + 8 (`bloom_offset`) + 8 (`index_offset`) + 4 (`magic`).
pub const FOOTER_BYTES_V3: u64 = 8 + 8 + 8 + 4;

/// Backwards-compatible alias used by existing code.
pub const SSTABLE_MAGIC: u32 = SSTABLE_MAGIC_V1;

/// Backwards-compatible alias used by existing code.
pub const FOOTER_BYTES: u64 = FOOTER_BYTES_V1;

/// Returns the byte offset where the v1 footer starts: `filesize - 12`.
///
/// Uses [`u64::saturating_sub`] so files smaller than 12 bytes return 0
/// rather than underflowing.
#[allow(dead_code)]
pub fn footer_pos(filesize: u64) -> u64 {
    filesize.saturating_sub(FOOTER_BYTES_V1)
}

/// Writes a v2 SSTable footer (`bloom_offset` + `index_offset` + `magic`) to `w`
#[allow(dead_code)]
pub fn write_footer_v2<W: Write>(
    w: &mut W,
    bloom_offset: u64,
    index_offset: u64,
) -> IoResult<()> {
    w.write_u64::<LittleEndian>(bloom_offset)?;
    w.write_u64::<LittleEndian>(index_offset)?;
    w.write_u32::<LittleEndian>(SSTABLE_MAGIC_V2)?;
    Ok(())
}

/// Writes a v3 SSTable footer to `w`.
///
/// Layout: `[max_seq: u64][bloom_offset: u64][index_offset: u64][magic: u32 = "SST3"]`
pub fn write_footer_v3<W: Write>(
    w: &mut W,
    max_seq: u64,
    bloom_offset: u64,
    index_offset: u64,
) -> IoResult<()> {
    w.write_u64::<LittleEndian>(max_seq)?;
    w.write_u64::<LittleEndian>(bloom_offset)?;
    w.write_u64::<LittleEndian>(index_offset)?;
    w.write_u32::<LittleEndian>(SSTABLE_MAGIC_V3)?;
    Ok(())
}

/// Writes a v1 SSTable footer (`index_offset` + `magic`) to `w`.
#[allow(dead_code)]
pub fn write_footer<W: Write>(w: &mut W, index_offset: u64) -> IoResult<()> {
    w.write_u64::<LittleEndian>(index_offset)?;
    w.write_u32::<LittleEndian>(SSTABLE_MAGIC_V1)?;
    Ok(())
}

/// Parsed SSTable footer, version-aware.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Footer {
    /// v1: no bloom filter.
    V1 {
        index_offset: u64,
    },
    /// v2: includes bloom filter offset.
    V2 {
        bloom_offset: u64,
        index_offset: u64,
    },
    /// v3: adds per-record CRC32 and stores max_seq for O(1) recovery.
    V3 {
        max_seq: u64,
        bloom_offset: u64,
        index_offset: u64,
    },
}

impl Footer {
    /// Returns the index offset regardless of version.
    #[must_use]
    pub fn index_offset(&self) -> u64 {
        match self {
            Footer::V1 { index_offset } => *index_offset,
            Footer::V2 { index_offset, .. } => *index_offset,
            Footer::V3 { index_offset, .. } => *index_offset,
        }
    }

    /// Returns the bloom offset if present (v2+), `None` for v1.
    #[must_use]
    pub fn bloom_offset(&self) -> Option<u64> {
        match self {
            Footer::V1 { .. } => None,
            Footer::V2 { bloom_offset, .. } => Some(*bloom_offset),
            Footer::V3 { bloom_offset, .. } => Some(*bloom_offset),
        }
    }

    /// Returns the max sequence number stored in the footer (v3+).
    ///
    /// For v1/v2 files this returns `None`, and the caller must scan
    /// all keys to determine the max seq (legacy recovery path).
    #[must_use]
    pub fn max_seq(&self) -> Option<u64> {
        match self {
            Footer::V1 { .. } | Footer::V2 { .. } => None,
            Footer::V3 { max_seq, .. } => Some(*max_seq),
        }
    }

    /// Returns `true` if this is a v3 SSTable (has per-record CRC32).
    #[must_use]
    pub fn has_checksums(&self) -> bool {
        matches!(self, Footer::V3 { .. })
    }

    /// Returns the magic number for this footer version.
    #[must_use]
    #[allow(dead_code)]
    pub fn magic(&self) -> u32 {
        match self {
            Footer::V1 { .. } => SSTABLE_MAGIC_V1,
            Footer::V2 { .. } => SSTABLE_MAGIC_V2,
            Footer::V3 { .. } => SSTABLE_MAGIC_V3,
        }
    }

    /// Returns the footer size in bytes for this version.
    #[must_use]
    pub fn footer_size(&self) -> u64 {
        match self {
            Footer::V1 { .. } => FOOTER_BYTES_V1,
            Footer::V2 { .. } => FOOTER_BYTES_V2,
            Footer::V3 { .. } => FOOTER_BYTES_V3,
        }
    }
}


/// Reads the SSTable footer from `r`, auto-detecting v1 or v2.
/// Strategy: read the last 4 bytes to determine the magic, then seek back
/// to read the full footer for that version.
pub fn read_footer_versioned<R: Read + Seek>(r: &mut R) -> IoResult<Footer> {
    let filesize = r.seek(SeekFrom::End(0))?;

    if filesize < FOOTER_BYTES_V1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file too small for SSTable footer",
        ));
    }

    // Read magic (last 4 bytes)
    r.seek(SeekFrom::End(-4))?;
    let magic = r.read_u32::<LittleEndian>()?;

    match magic {
        SSTABLE_MAGIC_V3 => {
            if filesize < FOOTER_BYTES_V3 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "file too small for v3 footer",
                ));
            }
            r.seek(SeekFrom::End(-(FOOTER_BYTES_V3 as i64)))?;
            let max_seq = r.read_u64::<LittleEndian>()?;
            let bloom_offset = r.read_u64::<LittleEndian>()?;
            let index_offset = r.read_u64::<LittleEndian>()?;
            let _magic = r.read_u32::<LittleEndian>()?;
            Ok(Footer::V3 {
                max_seq,
                bloom_offset,
                index_offset,
            })
        }
        SSTABLE_MAGIC_V2 => {
            if filesize < FOOTER_BYTES_V2 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "file too small for v2 footer",
                ));
            }
            r.seek(SeekFrom::End(-(FOOTER_BYTES_V2 as i64)))?;
            let bloom_offset = r.read_u64::<LittleEndian>()?;
            let index_offset = r.read_u64::<LittleEndian>()?;
            let _magic = r.read_u32::<LittleEndian>()?;
            Ok(Footer::V2 {
                bloom_offset,
                index_offset,
            })
        }
        SSTABLE_MAGIC_V1 => {
            r.seek(SeekFrom::End(-(FOOTER_BYTES_V1 as i64)))?;
            let index_offset = r.read_u64::<LittleEndian>()?;
            let _magic = r.read_u32::<LittleEndian>()?;
            Ok(Footer::V1 { index_offset })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown SSTable magic: {:#x}", magic),
        )),
    }
}

/// Reads the SSTable footer from `r`, returning `(index_offset, magic)`.
///
/// **Legacy v1 reader.** Prefer [`read_footer_versioned`] for new code.
///
/// The reader is seeked to the end to determine file size, then to the
/// footer position. After this call the cursor is at the end of the file.
#[allow(dead_code)]
pub fn read_footer<R: Read + Seek>(r: &mut R) -> IoResult<(u64, u32)> {
    let filesize = r.seek(SeekFrom::End(0))?;
    r.seek(SeekFrom::Start(footer_pos(filesize)))?;
    let index_offset = r.read_u64::<LittleEndian>()?;
    let magic = r.read_u32::<LittleEndian>()?;
    Ok((index_offset, magic))
}

use std::io;
