use crate::SSTableWriter;
use crate::*;
use anyhow::Result;
use memtable::Memtable;
use tempfile::tempdir;

/// Helper: write a memtable to an SSTable and open a reader.
fn write_and_open(
    dir: &std::path::Path,
    name: &str,
    entries: &[(&[u8], Option<&[u8]>, u64)],
) -> Result<SSTableReader> {
    let path = dir.join(name);
    let mut mem = Memtable::new();
    for &(key, val, seq) in entries {
        match val {
            Some(v) => mem.put(key.to_vec(), v.to_vec(), seq),
            None => mem.delete(key.to_vec(), seq),
        }
    }
    SSTableWriter::write_from_memtable(&path, &mem)?;
    SSTableReader::open(&path)
}

// -------------------- Basic merge --------------------

#[test]
fn merge_single_sstable() -> Result<()> {
    let dir = tempdir()?;
    let r = write_and_open(
        dir.path(),
        "a.sst",
        &[
            (b"a", Some(b"1"), 1),
            (b"b", Some(b"2"), 2),
            (b"c", Some(b"3"), 3),
        ],
    )?;

    let readers = vec![r];
    let mut iter = MergeIterator::new(&readers);
    let result = iter.collect_all()?;

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].0, b"a");
    assert_eq!(result[1].0, b"b");
    assert_eq!(result[2].0, b"c");
    Ok(())
}

#[test]
fn merge_two_non_overlapping() -> Result<()> {
    let dir = tempdir()?;
    let r1 = write_and_open(
        dir.path(),
        "a.sst",
        &[(b"a", Some(b"1"), 1), (b"b", Some(b"2"), 2)],
    )?;
    let r2 = write_and_open(
        dir.path(),
        "b.sst",
        &[(b"c", Some(b"3"), 3), (b"d", Some(b"4"), 4)],
    )?;

    let readers = vec![r1, r2];
    let mut iter = MergeIterator::new(&readers);
    let result = iter.collect_all()?;

    assert_eq!(result.len(), 4);
    assert_eq!(result[0].0, b"a");
    assert_eq!(result[1].0, b"b");
    assert_eq!(result[2].0, b"c");
    assert_eq!(result[3].0, b"d");
    Ok(())
}

// -------------------- Many keys --------------------

#[test]
fn merge_many_keys_across_sstables() -> Result<()> {
    let dir = tempdir()?;

    // 3 SSTables, each with 100 keys, some overlapping
    let r1 = write_and_open(
        dir.path(),
        "1.sst",
        &(0..100u64)
            .map(|i| {
                let key = format!("key{:04}", i);
                (key.as_bytes().to_vec(), Some(b"v1".to_vec()), i)
            })
            .collect::<Vec<_>>()
            .iter()
            .map(|(k, v, s)| (k.as_slice(), v.as_deref(), *s))
            .collect::<Vec<_>>()
            .as_slice(),
    )?;

    let r2 = write_and_open(
        dir.path(),
        "2.sst",
        &(50..150u64)
            .map(|i| {
                let key = format!("key{:04}", i);
                (key.as_bytes().to_vec(), Some(b"v2".to_vec()), i + 100)
            })
            .collect::<Vec<_>>()
            .iter()
            .map(|(k, v, s)| (k.as_slice(), v.as_deref(), *s))
            .collect::<Vec<_>>()
            .as_slice(),
    )?;

    let readers = vec![r1, r2];
    let mut iter = MergeIterator::new(&readers);
    let result = iter.collect_all()?;

    // keys 0..150 = 150 unique keys
    assert_eq!(result.len(), 150);

    // Overlapping keys (50..100) should have seq from r2 (higher)
    for (key, entry) in &result {
        let key_str = String::from_utf8_lossy(key);
        if let Ok(num) = key_str.trim_start_matches("key").parse::<u64>() {
            if num >= 50 && num < 100 {
                // Should be from r2 (seq = num + 100)
                assert_eq!(entry.seq, num + 100, "key {} should have seq from r2", num);
                assert_eq!(entry.value, Some(b"v2".to_vec()));
            }
        }
    }

    // Output must be sorted
    let keys: Vec<&[u8]> = result.iter().map(|(k, _)| k.as_slice()).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);

    Ok(())
}

// -------------------- Three-way merge --------------------

#[test]
fn merge_three_sstables_with_overlap() -> Result<()> {
    let dir = tempdir()?;
    let r1 = write_and_open(
        dir.path(),
        "1.sst",
        &[(b"a", Some(b"v1"), 1), (b"c", Some(b"v1"), 1)],
    )?;
    let r2 = write_and_open(
        dir.path(),
        "2.sst",
        &[(b"b", Some(b"v2"), 2), (b"c", Some(b"v2"), 2)],
    )?;
    let r3 = write_and_open(
        dir.path(),
        "3.sst",
        &[(b"c", Some(b"v3"), 3), (b"d", Some(b"v3"), 3)],
    )?;

    let readers = vec![r1, r2, r3];
    let mut iter = MergeIterator::new(&readers);
    let result = iter.collect_all()?;

    // a, b, c (deduped), d
    assert_eq!(result.len(), 4);
    assert_eq!(result[0].0, b"a");
    assert_eq!(result[1].0, b"b");
    assert_eq!(result[2].0, b"c");
    assert_eq!(result[2].1.seq, 3); // highest seq wins
    assert_eq!(result[2].1.value, Some(b"v3".to_vec()));
    assert_eq!(result[3].0, b"d");

    Ok(())
}

// -------------------- Empty inputs --------------------

#[test]
fn merge_no_readers() -> Result<()> {
    let readers: Vec<SSTableReader> = vec![];
    let mut iter = MergeIterator::new(&readers);
    let result = iter.collect_all()?;
    assert!(result.is_empty());
    Ok(())
}

// -------------------- Sorted output --------------------

#[test]
fn merge_output_is_sorted() -> Result<()> {
    let dir = tempdir()?;
    let r1 = write_and_open(
        dir.path(),
        "1.sst",
        &[
            (b"z", Some(b"1"), 1),
            (b"m", Some(b"2"), 2),
            (b"a", Some(b"3"), 3),
        ],
    )?;
    let r2 = write_and_open(
        dir.path(),
        "2.sst",
        &[(b"x", Some(b"4"), 4), (b"b", Some(b"5"), 5)],
    )?;

    let readers = vec![r1, r2];
    let mut iter = MergeIterator::new(&readers);
    let result = iter.collect_all()?;

    let keys: Vec<&[u8]> = result.iter().map(|(k, _)| k.as_slice()).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);

    Ok(())
}

// -------------------- Deduplication --------------------

#[test]
fn merge_overlapping_keys_highest_seq_wins() -> Result<()> {
    let dir = tempdir()?;

    // Older SSTable
    let r1 = write_and_open(dir.path(), "old.sst", &[(b"key", Some(b"old_value"), 1)])?;

    // Newer SSTable
    let r2 = write_and_open(dir.path(), "new.sst", &[(b"key", Some(b"new_value"), 5)])?;

    let readers = vec![r1, r2];
    let mut iter = MergeIterator::new(&readers);
    let result = iter.collect_all()?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, b"key");
    assert_eq!(result[0].1.seq, 5);
    assert_eq!(result[0].1.value, Some(b"new_value".to_vec()));

    Ok(())
}

#[test]
fn merge_tombstone_wins_over_older_value() -> Result<()> {
    let dir = tempdir()?;
    let r1 = write_and_open(dir.path(), "old.sst", &[(b"key", Some(b"alive"), 1)])?;
    let r2 = write_and_open(
        dir.path(),
        "new.sst",
        &[(b"key", None, 5)], // tombstone
    )?;

    let readers = vec![r1, r2];
    let mut iter = MergeIterator::new(&readers);
    let result = iter.collect_all()?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1.seq, 5);
    assert_eq!(result[0].1.value, None); // tombstone wins

    Ok(())
}
