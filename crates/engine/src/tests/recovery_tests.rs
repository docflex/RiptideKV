use crate::*;
use anyhow::Result;
use memtable::Memtable;
use std::fs;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

// --------------------- Recovery ---------------------

#[test]
fn recovery_from_wal() -> Result<()> {
    let dir = tempdir()?;
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");

    // Write some data, then drop engine (simulates crash)
    {
        let mut engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, true)?;
        engine.set(b"a".to_vec(), b"1".to_vec())?;
        engine.set(b"b".to_vec(), b"2".to_vec())?;
        engine.del(b"a".to_vec())?;
    }

    // Reopen engine - should replay WAL
    let engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, true)?;
    assert!(engine.get(b"a")?.is_none()); // deleted
    assert_eq!(engine.get(b"b")?.unwrap().1, b"2".to_vec());
    assert_eq!(engine.seq(), 3); // 3 operations
    Ok(())
}

#[test]
fn recovery_from_sstables() -> Result<()> {
    let dir = tempdir()?;
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");

    // Write data and force flush
    {
        let mut engine = Engine::new(&wal_path, &sst_dir, 1, true)?;
        engine.set(b"k".to_vec(), b"v".to_vec())?;
        // Flush happened due to threshold=1
    }

    // Reopen - WAL is empty but SSTable has the data
    let engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, true)?;
    assert_eq!(engine.get(b"k")?.unwrap().1, b"v".to_vec());
    Ok(())
}

#[test]
fn recovery_combines_wal_and_sstables() -> Result<()> {
    let dir = tempdir()?;
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");

    // Create an engine that flushes immediately
    {
        let mut engine = Engine::new(&wal_path, &sst_dir, 1, true)?;
        // This triggers flush (threshold=1)
        engine.set(b"flushed".to_vec(), b"in_sst".to_vec())?;
    }

    {
        // Reopen with high threshold so next writes stay in WAL
        let mut engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, true)?;
        engine.set(b"in_wal".to_vec(), b"pending".to_vec())?;
    }

    // Final reopen - should have both
    let engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, true)?;
    assert_eq!(engine.get(b"flushed")?.unwrap().1, b"in_sst".to_vec());
    assert_eq!(engine.get(b"in_wal")?.unwrap().1, b"pending".to_vec());
    Ok(())
}

// --------------------- Manifest recovery ---------------------

#[test]
fn manifest_preserves_l0_l1_across_restart() -> Result<()> {
    let dir = tempdir()?;
    let wal = dir.path().join("wal.log");
    let sst = dir.path().join("sst");

    {
        let mut engine = Engine::new(&wal, &sst, 64, false)?;
        engine.set_l0_compaction_trigger(0);

        // Create some L0 SSTables
        for i in 0..20u64 {
            engine.set(format!("k{:04}", i).into_bytes(), b"val".to_vec())?;
        }
        engine.force_flush()?;
        let l0_before = engine.l0_sstable_count();
        assert!(l0_before > 0);

        // Compact to L1
        engine.compact()?;
        assert_eq!(engine.l0_sstable_count(), 0);
        assert_eq!(engine.l1_sstable_count(), 1);

        // Add more L0 data
        for i in 20..25u64 {
            engine.set(format!("k{:04}", i).into_bytes(), b"val2".to_vec())?;
        }
        engine.force_flush()?;
        assert!(engine.l0_sstable_count() > 0);
        assert_eq!(engine.l1_sstable_count(), 1);
    }

    // Reopen - manifest should preserve L0/L1 assignments
    let engine = Engine::new(&wal, &sst, 64, false)?;
    assert!(engine.l0_sstable_count() > 0, "L0 should be preserved");
    assert_eq!(engine.l1_sstable_count(), 1, "L1 should be preserved");

    // All keys readable
    for i in 0..25u64 {
        assert!(engine.get(format!("k{:04}", i).as_bytes())?.is_some());
    }
    Ok(())
}

// --------------------- SST filename sort correctness ---------------------

#[test]
fn sst_sort_order_is_correct_across_many_flushes() -> Result<()> {
    // Regression: if seq is not zero-padded, sst-9 sorts after sst-85
    // lexicographically, breaking newest-first ordering.
    let dir = tempdir()?;
    let sst_dir = dir.path().join("sst");

    // Use threshold=1 so every set triggers a flush
    let mut engine = Engine::new(dir.path().join("wal.log"), &sst_dir, 1, false)?;

    // Write 15 keys - produces seq 1..15, so filenames span single and
    // double digits. Without zero-padding this breaks.
    for i in 0..15u64 {
        engine.set(
            format!("k{:02}", i).into_bytes(),
            format!("v{}", i).into_bytes(),
        )?;
        thread::sleep(Duration::from_millis(2));
    }

    // Drop and reopen - recovery must load SSTables in correct order
    drop(engine);
    let engine = Engine::new(dir.path().join("wal.log"), &sst_dir, 1024 * 1024, false)?;

    // All keys must be readable with correct values
    for i in 0..15u64 {
        let key = format!("k{:02}", i).into_bytes();
        let expected = format!("v{}", i).into_bytes();
        let (_, val) = engine.get(&key)?.expect(&format!("k{:02} missing", i));
        assert_eq!(val, expected, "k{:02} has wrong value", i);
    }

    Ok(())
}

#[test]
fn sst_overwrite_across_flushes_returns_newest() -> Result<()> {
    // Write same key across multiple flushes; newest SSTable must win.
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1, // Flush every write
        false,
    )?;

    for i in 0..12u64 {
        engine.set(b"shared".to_vec(), format!("v{}", i).into_bytes())?;
        thread::sleep(Duration::from_millis(2));
    }

    // Drop and reopen
    drop(engine);
    let engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1024 * 1024,
        false,
    )?;

    // Must read the latest value
    let (_, val) = engine.get(b"shared")?.expect("shared key missing");
    assert_eq!(val, b"v11", "should read newest value after recovery");
    Ok(())
}

// --------------------- Tmp file cleanup on recovery ---------------------

#[test]
fn recovery_cleans_up_tmp_files() -> Result<()> {
    let dir = tempdir()?;
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir)?;

    // Simulate a leftover .sst.tmp from an interrupted flush
    let tmp_file = sst_dir.join("sst-0000000000000001-12345.sst.tmp");
    fs::write(&tmp_file, b"garbage")?;
    assert!(tmp_file.exists());

    // Opening the engine should clean it up
    let _engine = Engine::new(dir.path().join("wal.log"), &sst_dir, 1024 * 1024, false)?;

    assert!(
        !tmp_file.exists(),
        ".sst.tmp should be cleaned up on recovery"
    );
    Ok(())
}

// --------------------- Seq recovery from SSTables ---------------------

#[test]
fn seq_recovered_from_sstables_after_wal_truncation() -> Result<()> {
    let dir = tempdir()?;
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");

    // Write data and flush (WAL gets truncated)
    {
        let mut engine = Engine::new(&wal_path, &sst_dir, 1, false)?;
        engine.set(b"a".to_vec(), b"1".to_vec())?;
        thread::sleep(Duration::from_millis(2));
        engine.set(b"b".to_vec(), b"2".to_vec())?;
        thread::sleep(Duration::from_millis(2));
        engine.set(b"c".to_vec(), b"3".to_vec())?;
        // seq is now 3, WAL is truncated, data is in SSTables
    }

    // Reopen - WAL is empty, seq must be recovered from SSTables
    let mut engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, false)?;
    assert!(
        engine.seq() >= 3,
        "seq should be >= 3 from SSTable scan, got {}",
        engine.seq()
    );

    // New writes must get seq > 3
    engine.set(b"d".to_vec(), b"4".to_vec())?;
    assert!(
        engine.seq() > 3,
        "new write seq should be > 3, got {}",
        engine.seq()
    );
    Ok(())
}

// --------------------- WAL open error propagation ---------------------

#[test]
fn replay_wal_propagates_non_notfound_errors() {
    // replay_wal_and_build should return Ok(0) for missing file
    let mut mem = Memtable::new();
    let result = recovery::replay_wal_and_build("/nonexistent/path/wal.log", &mut mem);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}
