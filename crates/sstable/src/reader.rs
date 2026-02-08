use anyhow::{bail, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use memtable::ValueEntry;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::format::{read_footer, SSTABLE_MAGIC};

/// Simple reader that loads the index into memory for fast point lookups.
/// It stores a map: key -> data_offset. When a key is requested,
/// the reader opens the file, seeks to the offset, parses the record and returns it.
pub struct SsTableReader {
    path: PathBuf,
    index: BTreeMap<Vec<u8>, u64>,
}

impl SsTableReader {
    /// Open an SSTable and load its index into memory.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let mut f = File::open(&path_buf)?;
        let metadata = f.metadata()?;
        let filesize = metadata.len();

        if filesize < crate::format::FOOTER_BYTES {
            bail!("sstable file too small");
        }

        // read footer via helper
        let (index_offset, magic) = read_footer(&mut f)?;
        if magic != SSTABLE_MAGIC {
            bail!("invalid sstable magic: {:x}", magic);
        }
        if index_offset >= filesize {
            bail!("invalid index_offset");
        }

        // Read index entries from index_offset up to footer start
        f.seek(SeekFrom::Start(index_offset))?;
        let mut index = BTreeMap::new();

        // Read until we reach footer (filesize - FOOTER_BYTES)
        while f.stream_position()? < (filesize - crate::format::FOOTER_BYTES) {
            // key_len (u32) + key bytes + data_offset (u64)
            let key_len = f.read_u32::<LittleEndian>()? as usize;
            let mut key = vec![0u8; key_len];
            f.read_exact(&mut key)?;
            let data_offset = f.read_u64::<LittleEndian>()?;
            index.insert(key, data_offset);
        }

        Ok(Self {
            path: path_buf,
            index,
        })
    }

    /// Point lookup. Returns the raw ValueEntry (seq + optional value) if present in this SSTable.
    ///
    /// Note: this reads the record from disk for the matching offset and returns it.
    /// If the key is not present in the index, returns Ok(None).
    pub fn get(&self, key: &[u8]) -> Result<Option<ValueEntry>> {
        let maybe_offset = self.index.get(key);
        if maybe_offset.is_none() {
            return Ok(None);
        }
        let offset = *maybe_offset.unwrap();

        // Open file each time to keep API & ownership simple and avoid mutable File in struct.
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(offset))?;

        // Parse record at offset:
        // u32 key_len
        // key bytes
        // u64 seq
        // u8 value_present
        // if present == 1:
        //   u32 value_len
        //   value bytes
        let key_len = f.read_u32::<LittleEndian>()? as usize;
        let mut key_buf = vec![0u8; key_len];
        f.read_exact(&mut key_buf)?;

        // Sanity: the key read should match the requested key
        if key_buf.as_slice() != key {
            bail!("index pointed to mismatching key at offset");
        }

        let seq = f.read_u64::<LittleEndian>()?;
        let present = f.read_u8()?;
        if present == 1 {
            let val_len = f.read_u32::<LittleEndian>()? as usize;
            let mut val = vec![0u8; val_len];
            f.read_exact(&mut val)?;
            Ok(Some(ValueEntry {
                seq,
                value: Some(val),
            }))
        } else {
            Ok(Some(ValueEntry { seq, value: None }))
        }
    }

    /// Expose the loaded keys for debugging / tests.
    pub fn keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.index.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SsTableWriter;
    use memtable::Memtable;
    use tempfile::tempdir;

    fn make_sample_memtable() -> Memtable {
        let mut m = Memtable::new();
        m.put(b"a".to_vec(), b"apple".to_vec(), 1);
        m.put(b"b".to_vec(), b"banana".to_vec(), 2);
        m.put(b"c".to_vec(), b"".to_vec(), 3);
        m.delete(b"d".to_vec(), 4);
        m
    }

    #[test]
    fn open_and_get_entries() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("sample.sst");

        let mem = make_sample_memtable();
        SsTableWriter::write_from_memtable(&path, &mem)?;

        // Open reader and verify values
        let reader = SsTableReader::open(&path)?;

        // Check keys exist in index
        let keys: Vec<_> = reader.keys().cloned().collect();
        assert!(keys.contains(&b"a".to_vec()));
        assert!(keys.contains(&b"b".to_vec()));
        assert!(keys.contains(&b"c".to_vec()));
        assert!(keys.contains(&b"d".to_vec()));

        // Get 'a'
        let a = reader.get(b"a")?.expect("a must exist");
        assert_eq!(a.seq, 1);
        assert_eq!(a.value, Some(b"apple".to_vec()));

        // Get 'b'
        let b = reader.get(b"b")?.expect("b must exist");
        assert_eq!(b.seq, 2);
        assert_eq!(b.value, Some(b"banana".to_vec()));

        // Get 'c' (empty but present)
        let c = reader.get(b"c")?.expect("c must exist");
        assert_eq!(c.seq, 3);
        assert_eq!(c.value, Some(b"".to_vec()));

        // Get 'd' (tombstone)
        let d = reader.get(b"d")?.expect("d must exist");
        assert_eq!(d.seq, 4);
        assert_eq!(d.value, None);

        // Non-existent key
        assert!(reader.get(b"nope")?.is_none());

        Ok(())
    }
}
