use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use memtable::Memtable;
use std::fs::{rename, OpenOptions};
use std::io::{Seek, Write};
use std::path::Path;

/// Magic identifying our SSTable v1
// const SSTABLE_MAGIC: u32 = 0x5353_5431; // ASCII "SST1"
use crate::format::write_footer;

pub struct SsTableWriter {}

impl SsTableWriter {
    /// Write an SSTable at `path` from the given memtable.
    ///
    /// Format (v1, minimal):
    /// [DATA] repeated entries:
    ///   u32 key_len
    ///   key bytes
    ///   u64 seq
    ///   u8 value_present (0/1)
    ///   if value_present == 1:
    ///       u32 value_len
    ///       value bytes
    ///
    /// [INDEX] repeated:
    ///   u32 key_len
    ///   key bytes
    ///   u64 data_offset
    ///
    /// [FOOTER]:
    ///   u64 index_offset
    ///   u32 magic ("SST1")
    ///
    /// Implementation notes:
    /// - Writes to a temp file in the same directory, fsyncs, then atomically renames.
    /// - Index is kept in memory while writing (vector of (key, offset)).
    pub fn write_from_memtable(path: &Path, mem: &Memtable) -> Result<()> {
        // Create temporary file next to target for atomic rename later
        let tmp_path = path.with_extension("sst.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;

        // Keep an in-memory index: (key, offset)
        let mut index: Vec<(Vec<u8>, u64)> = Vec::new();

        // Write DATA section
        for (key, entry) in mem.iter() {
            // get current offset
            let offset = file.stream_position()?;

            // key
            file.write_u32::<LittleEndian>(key.len() as u32)?;
            file.write_all(key)?;

            // seq
            file.write_u64::<LittleEndian>(entry.seq)?;

            // value present flag and value bytes if present
            match &entry.value {
                Some(v) => {
                    file.write_u8(1)?;
                    file.write_u32::<LittleEndian>(v.len() as u32)?;
                    file.write_all(v)?;
                }
                None => {
                    file.write_u8(0)?;
                }
            }

            // record in index (first key of this record points to offset)
            index.push((key.clone(), offset));
        }

        // Write INDEX section and remember its offset
        let index_offset = file.stream_position()?;

        for (key, data_offset) in &index {
            file.write_u32::<LittleEndian>(key.len() as u32)?;
            file.write_all(key)?;
            file.write_u64::<LittleEndian>(*data_offset)?;
        }

        // Write FOOTER (index offset + magic) using format.rs helper
        write_footer(&mut file, index_offset)?;

        // Flush and sync
        file.flush()?;
        file.sync_all()?;

        // Atomically move into place
        rename(tmp_path, path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::SSTABLE_MAGIC;
    use memtable::Memtable;
    use std::io::Read;
    use tempfile::tempdir;

    fn make_sample_memtable() -> Memtable {
        let mut m = Memtable::new();
        // Keys purposely inserted in order for BTreeMap but mem.iter guarantees sorted order
        m.put(b"a".to_vec(), b"apple".to_vec(), 1);
        m.put(b"b".to_vec(), b"banana".to_vec(), 2);
        m.put(b"c".to_vec(), b"".to_vec(), 3); // present but empty string
        m.delete(b"d".to_vec(), 4); // tombstone
        m
    }

    #[test]
    fn write_and_inspect_sstable_footer() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.sst");

        let mem = make_sample_memtable();
        SsTableWriter::write_from_memtable(&path, &mem)?;

        // File should exist and be non-empty
        let meta = std::fs::metadata(&path)?;
        assert!(meta.len() > 0);

        // Read footer (last 12 bytes) and verify magic/index offset
        let mut f = std::fs::File::open(&path)?;
        let filesize = f.metadata()?.len();
        assert!(filesize >= 12, "file too small to contain footer");

        f.seek(std::io::SeekFrom::Start(filesize - 12))?;
        let index_offset = byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut f)?;
        let magic = byteorder::ReadBytesExt::read_u32::<LittleEndian>(&mut f)?;
        assert_eq!(magic, SSTABLE_MAGIC);

        // Basic sanity: index_offset must point inside file
        assert!(index_offset < filesize);

        // Read a few first bytes to ensure data was written (smoke)
        f.seek(std::io::SeekFrom::Start(0))?;
        let mut buf = [0u8; 8];
        let n = f.read(&mut buf)?;
        assert!(n > 0);

        Ok(())
    }
}
