use crate::format::{read_footer_versioned, Footer, SSTABLE_MAGIC_V3};
use crate::*;
use anyhow::Result;
use memtable::Memtable;
use std::io::Read;
use std::io::Seek;
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
fn write_empty_memtable_is_rejected() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.sst");
    let mem = Memtable::new(); // empty
    let result = SSTableWriter::write_from_memtable(&path, &mem);
    assert!(result.is_err(), "writing an empty memtable should fail");
    assert!(
        result.unwrap_err().to_string().contains("empty"),
        "error message should mention 'empty'"
    );
    // No file should have been created
    assert!(
        !path.exists(),
        "no .sst file should be created for empty memtable"
    );
}

#[test]
fn write_and_inspect_sstable_v3_footer() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("test.sst");

    let mem = make_sample_memtable();
    SSTableWriter::write_from_memtable(&path, &mem)?;

    // File should exist and be non-empty
    let meta = std::fs::metadata(&path)?;
    assert!(meta.len() > 0);

    // Read versioned footer and verify it's v3
    let mut f = std::fs::File::open(&path)?;
    let filesize = f.metadata()?.len();
    assert!(filesize >= 28, "file too small to contain v3 footer");

    let footer = read_footer_versioned(&mut f)?;
    assert_eq!(footer.magic(), SSTABLE_MAGIC_V3);

    match &footer {
        Footer::V3 {
            max_seq,
            bloom_offset,
            index_offset,
        } => {
            // max_seq should be 4 (highest seq in our sample memtable)
            assert_eq!(*max_seq, 4);
            // bloom_offset must be before index_offset
            assert!(*bloom_offset < *index_offset);
            // index_offset must point inside file
            assert!(*index_offset < filesize);
        }
        _ => panic!("expected v3 Footer"),
    }

    // Read a few first bytes to ensure data was written (smoke)
    f.seek(std::io::SeekFrom::Start(0))?;
    let mut buf = [0u8; 8];
    let n = f.read(&mut buf)?;
    assert!(n > 0);

    Ok(())
}
