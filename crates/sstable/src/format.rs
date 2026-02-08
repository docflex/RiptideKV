use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};

pub const SSTABLE_MAGIC: u32 = 0x5353_5431; // "SST1"
pub const FOOTER_BYTES: u64 = 8 /*index_offset*/ + 4 /*magic*/;

/// returns position where footer starts (filesize - FOOTER_BYTES)
pub fn footer_pos(filesize: u64) -> u64 {
    filesize.saturating_sub(FOOTER_BYTES)
}

/// Convenience: write footer (index_offset + magic)
pub fn write_footer<W: Write>(w: &mut W, index_offset: u64) -> IoResult<()> {
    w.write_u64::<LittleEndian>(index_offset)?;
    w.write_u32::<LittleEndian>(SSTABLE_MAGIC)?;
    Ok(())
}

/// Convenience: read footer (index_offset, magic)
pub fn read_footer<R: Read + Seek>(r: &mut R) -> IoResult<(u64, u32)> {
    let filesize = r.seek(SeekFrom::End(0))?;
    r.seek(SeekFrom::Start(footer_pos(filesize)))?;
    let index_offset = r.read_u64::<LittleEndian>()?;
    let magic = r.read_u32::<LittleEndian>()?;
    Ok((index_offset, magic))
}
