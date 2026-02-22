use super::helpers::count_sst_files;
use crate::*;
use anyhow::Result;
use std::fs;
use tempfile::tempdir;

// --------------------- Basic set / get / del ---------------------

#[test]
fn set_and_get() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    engine.set(b"name".to_vec(), b"alice".to_vec())?;
    let (seq, val) = engine.get(b"name")?.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(val, b"alice");
    Ok(())
}

#[test]
fn get_missing_key() -> Result<()> {
    let dir = tempdir()?;
    let engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    assert!(engine.get(b"nope")?.is_none());
    Ok(())
}

#[test]
fn del_removes_key() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    engine.set(b"k".to_vec(), b"v".to_vec())?;
    assert!(engine.get(b"k")?.is_some());

    engine.del(b"k".to_vec())?;
    assert!(engine.get(b"k")?.is_none());
    Ok(())
}

#[test]
fn overwrite_key() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    engine.set(b"k".to_vec(), b"v1".to_vec())?;
    engine.set(b"k".to_vec(), b"v2".to_vec())?;
    assert_eq!(engine.get(b"k")?.unwrap().1, b"v2".to_vec());
    Ok(())
}

#[test]
fn set_after_del_resurrects() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    engine.set(b"k".to_vec(), b"v1".to_vec())?;
    engine.del(b"k".to_vec())?;
    engine.set(b"k".to_vec(), b"v2".to_vec())?;
    assert_eq!(engine.get(b"k")?.unwrap().1, b"v2".to_vec());
    Ok(())
}

#[test]
fn newest_sstable_wins_on_read() -> Result<()> {
    let dir = tempdir()?;
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");

    let mut engine = Engine::new(&wal_path, &sst_dir, 1, false)?;

    // Write k=v1, flush
    engine.set(b"k".to_vec(), b"v1".to_vec())?;
    std::thread::sleep(std::time::Duration::from_millis(2));

    // Write k=v2, flush (newer SSTable)
    engine.set(b"k".to_vec(), b"v2".to_vec())?;

    // Should read v2 from the newest SSTable
    assert_eq!(engine.get(b"k")?.unwrap().1, b"v2".to_vec());
    Ok(())
}

// --------------------- force_flush ---------------------

#[test]
fn force_flush_empty_memtable_is_noop() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    let count_before = engine.sstable_count();
    engine.force_flush()?;
    assert_eq!(
        engine.sstable_count(),
        count_before,
        "empty flush should be noop"
    );
    Ok(())
}

#[test]
fn force_flush_persists_memtable_data() -> Result<()> {
    let dir = tempdir()?;
    let wal = dir.path().join("wal.log");
    let sst = dir.path().join("sst");

    {
        let mut engine = Engine::new(&wal, &sst, 1024 * 1024, false)?;
        engine.set(b"key".to_vec(), b"value".to_vec())?;
        engine.force_flush()?;
        assert_eq!(engine.l0_sstable_count(), 1);
    }

    // Reopen - data should be in SSTable, not WAL
    let engine = Engine::new(&wal, &sst, 1024 * 1024, false)?;
    let (_, val) = engine.get(b"key")?.expect("key should survive");
    assert_eq!(val, b"value");
    Ok(())
}

// --------------------- Drop flushes memtable ---------------------

#[test]
fn drop_flushes_memtable_to_sstable() -> Result<()> {
    let dir = tempdir()?;
    let wal = dir.path().join("wal.log");
    let sst = dir.path().join("sst");

    {
        let mut engine = Engine::new(&wal, &sst, 1024 * 1024, false)?;
        engine.set(b"drop_key".to_vec(), b"drop_val".to_vec())?;
        // Engine drops here - should flush memtable
    }

    // Reopen - data should be in SSTable from the Drop flush
    let engine = Engine::new(&wal, &sst, 1024 * 1024, false)?;
    let (_, val) = engine.get(b"drop_key")?.expect("key should survive drop");
    assert_eq!(val, b"drop_val");
    assert!(engine.sstable_count() >= 1);
    Ok(())
}

#[test]
fn set_rejects_oversized_value() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    let big_val = vec![b'v'; MAX_VALUE_SIZE + 1];
    let result = engine.set(b"k".to_vec(), big_val);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("value too large"));
    assert_eq!(engine.seq(), 0);
    Ok(())
}

#[test]
fn set_accepts_max_key_size() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024 * 1024, // huge threshold to avoid flush
        false,
    )?;

    let max_key = vec![b'k'; MAX_KEY_SIZE];
    engine.set(max_key.clone(), b"v".to_vec())?;
    let (_, val) = engine
        .get(&max_key)?
        .expect("max-size key should be readable");
    assert_eq!(val, b"v");
    Ok(())
}

#[test]
fn del_rejects_oversized_key() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    let big_key = vec![b'k'; MAX_KEY_SIZE + 1];
    let result = engine.del(big_key);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("key too large"));
    assert_eq!(engine.seq(), 0);
    Ok(())
}

// --------------------- Multiple flushes ---------------------

#[test]
fn multiple_flushes_create_multiple_sstables() -> Result<()> {
    let dir = tempdir()?;
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");

    let mut engine = Engine::new(&wal_path, &sst_dir, 1, false)?;
    // Disable auto-compaction so all L0 SSTables remain on disk.
    engine.set_l0_compaction_trigger(0);

    for i in 0..5u64 {
        engine.set(format!("k{}", i).into_bytes(), b"v".to_vec())?;
        // Each set triggers a flush due to threshold=1
        // Small sleep to ensure unique timestamps in filenames
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    let sst_count = count_sst_files(&sst_dir);
    assert!(
        sst_count >= 5,
        "expected multiple SSTable files, got {}",
        sst_count
    );

    // All keys should be readable
    for i in 0..5u64 {
        let key = format!("k{}", i).into_bytes();
        assert!(engine.get(&key)?.is_some(), "key {} should be readable", i);
    }
    Ok(())
}

// --------------------- Sequence number ---------------------

#[test]
fn seq_increments_on_every_operation() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    assert_eq!(engine.seq(), 0);
    engine.set(b"a".to_vec(), b"1".to_vec())?;
    assert_eq!(engine.seq(), 1);
    engine.set(b"b".to_vec(), b"2".to_vec())?;
    assert_eq!(engine.seq(), 2);
    engine.del(b"a".to_vec())?;
    assert_eq!(engine.seq(), 3);
    Ok(())
}

// --------------------- Key/value size limits ---------------------

#[test]
fn set_rejects_empty_key() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    let result = engine.set(vec![], b"value".to_vec());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("empty"));
    // seq should not have incremented
    assert_eq!(engine.seq(), 0);
    Ok(())
}

#[test]
fn del_rejects_empty_key() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    let result = engine.del(vec![]);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("empty"));
    assert_eq!(engine.seq(), 0);
    Ok(())
}

#[test]
fn set_rejects_oversized_key() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    let big_key = vec![b'k'; MAX_KEY_SIZE + 1];
    let result = engine.set(big_key, b"v".to_vec());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("key too large"));
    assert_eq!(engine.seq(), 0);
    Ok(())
}

// --------------------- Flush mechanics ---------------------

#[test]
fn flush_writes_sstable_and_truncates_wal() -> Result<()> {
    let dir = tempdir()?;
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");

    let mut engine = Engine::new(&wal_path, &sst_dir, 1, true)?;
    engine.set(b"key1".to_vec(), b"value1".to_vec())?;

    assert!(
        count_sst_files(&sst_dir) >= 1,
        "expected at least one .sst file"
    );

    let wal_meta = fs::metadata(&wal_path)?;
    assert_eq!(wal_meta.len(), 0, "expected wal to be truncated to 0 bytes");
    Ok(())
}

#[test]
fn flush_triggers_at_threshold() -> Result<()> {
    let dir = tempdir()?;
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    let threshold = 4 * 1024; // 4 KB for fast test

    let mut engine = Engine::new(&wal_path, &sst_dir, threshold, false)?;
    let value = vec![b'x'; 512];
    let writes = (threshold / value.len()) + 5;
    for i in 0..writes {
        engine.set(format!("key{}", i).into_bytes(), value.clone())?;
    }

    assert!(
        count_sst_files(&sst_dir) >= 1,
        "expected at least one SSTable after crossing threshold"
    );
    Ok(())
}

// --------------------- Read from SSTables after flush ---------------------

#[test]
fn get_reads_from_sstable_after_flush() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1, // tiny threshold - every set triggers flush
        false,
    )?;

    engine.set(b"k1".to_vec(), b"v1".to_vec())?;
    // After flush, memtable is empty; k1 is only in SSTable
    assert_eq!(engine.get(b"k1")?.unwrap().1, b"v1".to_vec());
    Ok(())
}

#[test]
fn tombstone_in_sstable_shadows_older_value() -> Result<()> {
    let dir = tempdir()?;
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");

    // Large threshold so we control flushes manually
    let mut engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, false)?;

    // Write k=v, then force flush by lowering threshold temporarily
    engine.set(b"k".to_vec(), b"old_value".to_vec())?;
    engine.set_flush_threshold(1);
    // Trigger flush with a dummy write
    engine.set(b"dummy".to_vec(), b"x".to_vec())?;

    // Now SSTable #1 has {k: old_value, dummy: x}
    // Reset threshold high
    engine.set_flush_threshold(1024 * 1024);

    // Delete k (goes into memtable as tombstone)
    engine.del(b"k".to_vec())?;

    // Memtable tombstone should shadow SSTable value
    assert!(engine.get(b"k")?.is_none());
    Ok(())
}
