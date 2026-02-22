use crate::*;
use crate::SSTableWriter;
use memtable::Memtable;
use tempfile::tempdir;
use anyhow::Result;

fn make_sample_memtable() -> Memtable {
    let mut m = Memtable::new();
    m.put(b"a".to_vec(), b"apple".to_vec(), 1);
    m.put(b"b".to_vec(), b"banana".to_vec(), 2);
    m.put(b"c".to_vec(), b"".to_vec(), 3);
    m.delete(b"d".to_vec(), 4);
    m
}

// -------------------- Basic open & get --------------------

#[test]
fn open_and_get_entries() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("sample.sst");

    let mem = make_sample_memtable();
    SSTableWriter::write_from_memtable(&path, &mem)?;
    let reader = SSTableReader::open(&path)?;

    // Check keys exist in index
    let keys: Vec<&[u8]> = reader.keys().collect();
    assert!(keys.contains(&b"a".as_slice()));
    assert!(keys.contains(&b"b".as_slice()));
    assert!(keys.contains(&b"c".as_slice()));
    assert!(keys.contains(&b"d".as_slice()));

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

// -------------------- len / is_empty --------------------

#[test]
fn len_and_is_empty() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("len.sst");

    let mem = make_sample_memtable();
    SSTableWriter::write_from_memtable(&path, &mem)?;

    let reader = SSTableReader::open(&path)?;
    assert_eq!(reader.len(), 4);
    assert!(!reader.is_empty());

    Ok(())
}

// -------------------- Large values --------------------

#[test]
fn large_value_roundtrip() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("bigval.sst");

    let mut mem = Memtable::new();
    let big = vec![b'x'; 500_000];
    mem.put(b"big".to_vec(), big.clone(), 1);
    SSTableWriter::write_from_memtable(&path, &mem)?;

    let reader = SSTableReader::open(&path)?;
    let entry = reader.get(b"big")?.unwrap();
    assert_eq!(entry.value.unwrap().len(), 500_000);

    Ok(())
}

// -------------------- Bloom filter --------------------

#[test]
fn v2_sstable_has_bloom_filter() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("bloom.sst");

    let mem = make_sample_memtable();
    SSTableWriter::write_from_memtable(&path, &mem)?;

    let reader = SSTableReader::open(&path)?;
    assert!(reader.has_bloom(), "v2 SSTable should have a bloom filter");

    Ok(())
}

#[test]
fn bloom_filter_finds_all_inserted_keys() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("bloom_hit.sst");

    let mut mem = Memtable::new();
    for i in 0..500u64 {
        mem.put(format!("key{:04}", i).into_bytes(), b"v".to_vec(), i);
    }
    SSTableWriter::write_from_memtable(&path, &mem)?;

    let reader = SSTableReader::open(&path)?;
    assert!(reader.has_bloom());

    // Every inserted key must be found
    for i in 0..500u64 {
        let key = format!("key{:04}", i).into_bytes();
        let entry = reader.get(&key)?;
        assert!(entry.is_some(), "key{:04} should exist", i);
    }

    Ok(())
}

#[test]
fn bloom_filter_rejects_missing_keys() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("bloom_miss.sst");

    let mut mem = Memtable::new();
    for i in 0..100u64 {
        mem.put(format!("exist{:04}", i).into_bytes(), b"v".to_vec(), i);
    }
    SSTableWriter::write_from_memtable(&path, &mem)?;

    let reader = SSTableReader::open(&path)?;
    assert!(reader.has_bloom());

    // Keys that were NOT inserted should mostly return None
    // (bloom filter may have false positives, but never false negatives)
    let mut misses = 0;
    for i in 0..100u64 {
        let key = format!("missing{:04}", i).into_bytes();
        if reader.get(&key)?.is_none() {
            misses += 1;
        }
    }
    // With 1% FPR, we expect ~99 misses out of 100
    assert!(
        misses > 90,
        "bloom filter should reject most missing keys, got {} misses out of 100",
        misses
    );

    Ok(())
}

// -------------------- Validation errors --------------------

#[test]
fn open_file_too_small() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("tiny.sst");
    std::fs::write(&path, b"short").unwrap();

    let result = SSTableReader::open(&path);
    assert!(result.is_err());
}

#[test]
fn open_bad_magic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("badmagic.sst");

    // 12 bytes: 8 for index_offset + 4 for wrong magic
    let mut data = vec![0u8; 8]; // index_offset = 0
    data.extend_from_slice(&[0xBA, 0xAD, 0xF0, 0x0D]); // wrong magic
    std::fs::write(&path, &data).unwrap();

    let result = SSTableReader::open(&path);
    assert!(result.is_err());
}

#[test]
fn open_nonexistent_file() {
    let result = SSTableReader::open("/tmp/no_such_file_riptide.sst");
    assert!(result.is_err());
}

// -------------------- Keys iterator ordering --------------------

#[test]
fn keys_are_sorted() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("sorted.sst");

    let mut mem = Memtable::new();
    mem.put(b"z".to_vec(), b"1".to_vec(), 1);
    mem.put(b"a".to_vec(), b"2".to_vec(), 2);
    mem.put(b"m".to_vec(), b"3".to_vec(), 3);
    SSTableWriter::write_from_memtable(&path, &mem)?;

    let reader = SSTableReader::open(&path)?;
    let keys: Vec<&[u8]> = reader.keys().collect();
    assert_eq!(keys, vec![b"a".as_slice(), b"m".as_slice(), b"z".as_slice()]);

    Ok(())
}

// -------------------- Multiple gets on same reader --------------------

#[test]
fn multiple_gets_same_reader() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("multi.sst");

    let mut mem = Memtable::new();
    for i in 0..100u64 {
        mem.put(format!("k{:03}", i).into_bytes(), b"v".to_vec(), i);
    }
    SSTableWriter::write_from_memtable(&path, &mem)?;

    let reader = SSTableReader::open(&path)?;
    // Read all keys twice to ensure re-opening the file works
    for _ in 0..2 {
        for i in 0..100u64 {
            let key = format!("k{:03}", i).into_bytes();
            let entry = reader.get(&key)?.unwrap();
            assert_eq!(entry.seq, i);
        }
    }

    Ok(())
}
