use crate::*;
use anyhow::Result;
use std::fs;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

// --------------------- Compaction & Levels ---------------------

#[test]
fn flush_goes_to_l0() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        64,
        false,
    )?;
    engine.set_l0_compaction_trigger(0);

    for i in 0..20u64 {
        engine.set(format!("k{:04}", i).into_bytes(), b"val".to_vec())?;
    }

    assert!(engine.l0_sstable_count() > 0, "flushes should go to L0");
    assert_eq!(engine.l1_sstable_count(), 0, "L1 should be empty before compact");
    Ok(())
}

#[test]
fn compact_moves_l0_to_l1() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        64,
        false,
    )?;
    engine.set_l0_compaction_trigger(0);

    for i in 0..50u64 {
        engine.set(format!("k{:04}", i).into_bytes(), b"val".to_vec())?;
    }

    assert!(engine.l0_sstable_count() > 1, "should have multiple L0 SSTables");

    engine.compact()?;
    assert_eq!(engine.l0_sstable_count(), 0, "L0 should be empty after compact");
    assert_eq!(engine.l1_sstable_count(), 1, "L1 should have exactly 1 SSTable after compact");
    assert_eq!(engine.sstable_count(), 1, "total should be 1");

    // All keys still readable
    for i in 0..50u64 {
        let key = format!("k{:04}", i).into_bytes();
        let (_, val) = engine.get(&key)?.expect("key should exist after compact");
        assert_eq!(val, b"val");
    }
    Ok(())
}

#[test]
fn compact_preserves_newest_value() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        32,
        false,
    )?;

    engine.set(b"key".to_vec(), b"v1".to_vec())?;
    engine.set(b"key".to_vec(), b"v2".to_vec())?;
    engine.set(b"key".to_vec(), b"v3".to_vec())?;

    engine.compact()?;

    let (_, val) = engine.get(b"key")?.expect("key should exist");
    assert_eq!(val, b"v3", "newest value should survive compaction");
    Ok(())
}

// --------------------- Stress ---------------------

#[test]
fn many_keys_with_flushes() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        4096, // 4 KB threshold
        false,
    )?;

    for i in 0..500u64 {
        let key = format!("key{:04}", i).into_bytes();
        let val = vec![b'v'; 64];
        engine.set(key, val)?;
    }

    // Verify all keys readable
    for i in 0..500u64 {
        let key = format!("key{:04}", i).into_bytes();
        assert!(
            engine.get(&key)?.is_some(),
            "key{:04} should be readable",
            i
        );
    }

    // Delete half
    for i in (0..500u64).step_by(2) {
        let key = format!("key{:04}", i).into_bytes();
        engine.del(key)?;
    }

    // Verify deletes
    for i in 0..500u64 {
        let key = format!("key{:04}", i).into_bytes();
        if i % 2 == 0 {
            assert!(engine.get(&key)?.is_none(), "key{:04} should be deleted", i);
        } else {
            assert!(
                engine.get(&key)?.is_some(),
                "key{:04} should still exist",
                i
            );
        }
    }

    Ok(())
}

// --------------------- Auto-compaction ---------------------

#[test]
fn auto_compaction_triggers_at_l0_threshold() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1, // threshold=1 -> every set triggers a flush
        false,
    )?;
    engine.set_l0_compaction_trigger(3);

    // Write 3 keys -> 3 flushes -> triggers auto-compaction at L0=3
    for i in 0..3u64 {
        engine.set(format!("k{}", i).into_bytes(), b"v".to_vec())?;
        thread::sleep(Duration::from_millis(2));
    }

    // After auto-compaction: L0 should be 0, L1 should be 1
    assert_eq!(engine.l0_sstable_count(), 0);
    assert_eq!(engine.l1_sstable_count(), 1);

    // All keys still readable
    for i in 0..3u64 {
        assert!(engine.get(format!("k{}", i).as_bytes())?.is_some());
    }
    Ok(())
}

#[test]
fn auto_compaction_disabled_when_trigger_is_zero() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        1,
        false,
    )?;
    engine.set_l0_compaction_trigger(0);

    for i in 0..5u64 {
        engine.set(format!("k{}", i).into_bytes(), b"v".to_vec())?;
        thread::sleep(Duration::from_millis(2));
    }

    // No auto-compaction -> all in L0
    assert!(engine.l0_sstable_count() >= 5);
    assert_eq!(engine.l1_sstable_count(), 0);
    Ok(())
}

// --------------------- Tombstone GC ---------------------

#[test]
fn tombstone_gc_removes_dead_keys_during_compaction() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        32,
        false,
    )?;
    engine.set_l0_compaction_trigger(0);

    // Write and delete a key, then flush so it's in SSTables
    engine.set(b"alive".to_vec(), b"yes".to_vec())?;
    engine.set(b"dead".to_vec(), b"soon".to_vec())?;
    engine.del(b"dead".to_vec())?;
    engine.force_flush()?;

    // Before compaction: "dead" tombstone exists in SSTable
    assert!(engine.get(b"dead")?.is_none());

    // Compact – tombstone GC should remove "dead" entirely
    engine.compact()?;

    // After compaction: "dead" should still return None (GC’d)
    assert!(engine.get(b"dead")?.is_none());
    // "alive" should still be present
    assert!(engine.get(b"alive")?.is_some());
    Ok(())
}

#[test]
fn compact_reduces_sst_file_count() -> Result<()> {
    let dir = tempdir()?;
    let sst_dir = dir.path().join("sst");
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        &sst_dir,
        64,
        false,
    )?;
    engine.set_l0_compaction_trigger(0);

    for i in 0..50u64 {
        engine.set(format!("k{:04}", i).into_bytes(), b"val".to_vec())?;
    }

    let files_before: Vec<_> = fs::read_dir(&sst_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "sst").unwrap_or(false))
        .collect();
    assert!(files_before.len() > 1, "should have multiple .sst files");

    engine.compact()?;

    let files_after: Vec<_> = fs::read_dir(&sst_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "sst").unwrap_or(false))
        .collect();
    assert_eq!(files_after.len(), 1, "should have exactly 1 .sst file after compact");
    Ok(())
}

#[test]
fn l0_flush_then_compact_then_more_flushes() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        64,
        false,
    )?;
    engine.set_l0_compaction_trigger(0);

    // Phase 1: flush some data into L0
    for i in 0..20u64 {
        engine.set(format!("k{:04}", i).into_bytes(), b"v1".to_vec())?;
    }
    let l0_before = engine.l0_sstable_count();
    assert!(l0_before > 0);

    // Phase 2: compact L0 -> L1
    engine.compact()?;
    assert_eq!(engine.l0_sstable_count(), 0);
    assert_eq!(engine.l1_sstable_count(), 1);

    // Phase 3: more flushes go to L0 again
    for i in 20..40u64 {
        engine.set(format!("k{:04}", i).into_bytes(), b"v2".to_vec())?;
    }
    assert!(engine.l0_sstable_count() > 0, "new flushes should go to L0");
    assert_eq!(engine.l1_sstable_count(), 1, "L1 should still have 1");

    // All keys readable (from L0 + L1 + memtable)
    for i in 0..40u64 {
        let key = format!("k{:04}", i).into_bytes();
        assert!(engine.get(&key)?.is_some(), "key {} should exist", i);
    }

    // Phase 4: compact again (L0 + L1 -> new L1)
    engine.compact()?;
    assert_eq!(engine.l0_sstable_count(), 0);
    assert_eq!(engine.l1_sstable_count(), 1);

    // All keys still readable
    for i in 0..40u64 {
        let key = format!("k{:04}", i).into_bytes();
        assert!(
            engine.get(&key)?.is_some(),
            "key {} should exist after second compact",
            i
        );
    }
    Ok(())
}

#[test]
fn compact_preserves_tombstones() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        32,
        false,
    )?;

    engine.set(b"alive".to_vec(), b"yes".to_vec())?;
    engine.set(b"dead".to_vec(), b"soon".to_vec())?;
    engine.del(b"dead".to_vec())?;

    engine.compact()?;

    assert!(engine.get(b"alive")?.is_some(), "alive key should survive");
    assert!(engine.get(b"dead")?.is_none(), "deleted key should stay deleted after compact");
    Ok(())
}

#[test]
fn compact_single_sstable_is_noop() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        64,
        false,
    )?;

    for i in 0..5u64 {
        engine.set(format!("k{}", i).into_bytes(), b"v".to_vec())?;
    }
    engine.set(format!("k{}", 5).into_bytes(), b"v".to_vec())?;

    let count_before = engine.sstable_count();
    engine.compact()?;
    let count_after = engine.sstable_count();

    if count_before <= 1 {
        assert_eq!(count_after, count_before);
    } else {
        assert_eq!(count_after, 1);
    }
    Ok(())
}

#[test]
fn compact_then_recovery_works() -> Result<()> {
    let dir = tempdir()?;
    let wal = dir.path().join("wal.log");
    let sst = dir.path().join("sst");

    {
        let mut engine = Engine::new(&wal, &sst, 64, false)?;
        engine.set_l0_compaction_trigger(0);
        for i in 0..30u64 {
            engine.set(format!("k{:04}", i).into_bytes(), b"val".to_vec())?;
        }
        // Flush any remaining memtable data so compact sees everything.
        engine.force_flush()?;
        assert!(engine.sstable_count() > 1);
        engine.compact()?;
        assert_eq!(engine.sstable_count(), 1);
    }

    // Reopen engine – should recover from the single compacted SSTable
    let engine = Engine::new(&wal, &sst, 64, false)?;
    assert_eq!(engine.sstable_count(), 1);

    for i in 0..30u64 {
        let key = format!("k{:04}", i).into_bytes();
        let (_, val) = engine.get(&key)?.expect("key should survive recovery after compact");
        assert_eq!(val, b"val");
    }
    Ok(())
}
