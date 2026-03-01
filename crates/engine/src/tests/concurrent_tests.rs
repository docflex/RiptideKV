use anyhow::Result;
use config::EngineConfig;
use std::thread;
use tempfile::tempdir;

use crate::ConcurrentEngine;

fn test_config(dir: &std::path::Path) -> EngineConfig {
    EngineConfig::builder()
        .wal_path(dir.join("wal.log"))
        .sst_dir(dir.join("sst"))
        .flush_threshold_bytes(1024 * 1024)
        .wal_sync(false)
        .build()
}

#[test]
fn concurrent_reads_do_not_block_each_other() -> Result<()> {
    let dir = tempdir()?;
    let engine = ConcurrentEngine::new(test_config(dir.path()))?;
    engine.set(b"k".to_vec(), b"v".to_vec())?;

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let e = engine.clone();
            thread::spawn(move || e.get(b"k"))
        })
        .collect();

    for h in handles {
        let result = h.join().expect("thread panicked")?;
        assert_eq!(result.unwrap().1, b"v");
    }
    Ok(())
}

#[test]
fn concurrent_writes_are_serialized() -> Result<()> {
    let dir = tempdir()?;
    let engine = ConcurrentEngine::new(test_config(dir.path()))?;

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let e = engine.clone();
            thread::spawn(move || e.set(format!("k{}", i).into_bytes(), b"v".to_vec()))
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked")?;
    }

    // All 10 keys should exist
    for i in 0..10 {
        let key = format!("k{}", i).into_bytes();
        assert!(engine.get(&key)?.is_some(), "key k{} missing", i);
    }

    assert_eq!(engine.seq()?, 10);
    Ok(())
}

#[test]
fn mixed_reads_and_writes() -> Result<()> {
    let dir = tempdir()?;
    let engine = ConcurrentEngine::new(test_config(dir.path()))?;

    // Pre-populate
    for i in 0..5 {
        engine.set(format!("k{}", i).into_bytes(), b"init".to_vec())?;
    }

    let mut handles = Vec::new();

    // Spawn readers
    for i in 0..5 {
        let e = engine.clone();
        handles.push(thread::spawn(move || {
            let key = format!("k{}", i).into_bytes();
            for _ in 0..100 {
                e.get(&key).expect("get failed");
            }
        }));
    }

    // Spawn writers
    for i in 5..10 {
        let e = engine.clone();
        handles.push(thread::spawn(move || {
            e.set(format!("k{}", i).into_bytes(), b"new".to_vec())
                .expect("set failed");
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }

    // All 10 keys shoudl exist
    for i in 0..10 {
        assert!(
            engine.get(format!("k{}", i).as_bytes())?.is_some(),
            "k{} missing",
            i
        )
    }
    Ok(())
}

#[test]
fn clone_shares_same_engine() -> Result<()> {
    let dir = tempdir()?;
    let e1 = ConcurrentEngine::new(test_config(dir.path()))?;
    let e2 = e1.clone();

    e1.set(b"shared".to_vec(), b"data".to_vec())?;
    assert_eq!(e2.get(b"shared")?.unwrap().1, b"data");
    Ok(())
}

#[test]
fn debug_output_works() -> Result<()> {
    let dir = tempdir()?;
    let engine = ConcurrentEngine::new(test_config(dir.path()))?;
    let debug_str = format!("{:?}", engine);
    assert!(debug_str.contains("ConcurrentEngine"));
    Ok(())
}
