use crate::*;
use anyhow::Result;
use tempfile::tempdir;

// --------------------- Scan (range query) ---------------------

#[test]
fn scan_full_range() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    engine.set(b"a".to_vec(), b"1".to_vec())?;
    engine.set(b"b".to_vec(), b"2".to_vec())?;
    engine.set(b"c".to_vec(), b"3".to_vec())?;

    let results = engine.scan(b"", b"")?;
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], (b"a".to_vec(), b"1".to_vec()));
    assert_eq!(results[1], (b"b".to_vec(), b"2".to_vec()));
    assert_eq!(results[2], (b"c".to_vec(), b"3".to_vec()));
    Ok(())
}

#[test]
fn scan_bounded_range() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    for c in b'a'..=b'z' {
        engine.set(vec![c], vec![c])?;
    }

    // Scan [b, e) - should return b, c, d
    let results = engine.scan(b"b", b"e")?;
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, b"b".to_vec());
    assert_eq!(results[2].0, b"d".to_vec());
    Ok(())
}

#[test]
fn scan_across_memtable_and_sstables() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        64,
        false,
    )?;
    engine.set_l0_compaction_trigger(0);

    // Write enough to trigger flushes
    for i in 0..20u64 {
        engine.set(format!("k{:04}", i).into_bytes(), b"val".to_vec())?;
    }

    // Some data in SSTables, some in memtable - scan should merge all
    let results = engine.scan(b"", b"")?;
    assert_eq!(results.len(), 20);
    // Should be sorted
    for i in 0..19 {
        assert!(results[i].0 < results[i + 1].0);
    }
    Ok(())
}

#[test]
fn scan_respects_tombstones() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    engine.set(b"a".to_vec(), b"1".to_vec())?;
    engine.set(b"b".to_vec(), b"2".to_vec())?;
    engine.set(b"c".to_vec(), b"3".to_vec())?;
    engine.del(b"b".to_vec())?;

    let results = engine.scan(b"", b"")?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b"a".to_vec());
    assert_eq!(results[1].0, b"c".to_vec());
    Ok(())
}

#[test]
fn scan_empty_range() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    engine.set(b"a".to_vec(), b"1".to_vec())?;

    // Range [x, z) - no keys in this range
    let results = engine.scan(b"x", b"z")?;
    assert!(results.is_empty());
    Ok(())
}

// --------------------- Read path priority ---------------------

#[test]
fn read_path_prefers_l0_over_l1() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        64,
        false,
    )?;
    engine.set_l0_compaction_trigger(0);

    // Write initial data and force multiple flushes so compact has work
    engine.set(b"key".to_vec(), b"old".to_vec())?;
    for i in 0..30u64 {
        engine.set(format!("pad{:04}", i).into_bytes(), b"x".to_vec())?;
    }

    assert!(
        engine.l0_sstable_count() > 1,
        "need multiple L0 SSTables for compact"
    );
    engine.compact()?;
    assert_eq!(engine.l1_sstable_count(), 1);
    assert_eq!(engine.l0_sstable_count(), 0);

    // Write newer value - will be in memtable or L0 after flush
    engine.set(b"key".to_vec(), b"new".to_vec())?;

    // key "new" is in memtable (or L0 if flushed), "old" is in L1
    let (_, val) = engine.get(b"key")?.expect("key should exist");
    assert_eq!(val, b"new", "memtable/L0 should shadow L1");
    Ok(())
}
