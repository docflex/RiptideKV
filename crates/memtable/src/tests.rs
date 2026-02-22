use super::*;

// -------------------- Basic CRUD --------------------

#[test]
fn put_and_get_single_key() {
    let mut m = Memtable::new();
    m.put(b"k1".to_vec(), b"v1".to_vec(), 1);
    assert_eq!(m.len(), 1);
    let (seq, val) = m.get(b"k1").unwrap();
    assert_eq!(seq, 1);
    assert_eq!(val, b"v1");
}

#[test]
fn put_overwrites_with_newer_seq() {
    let mut m = Memtable::new();
    m.put(b"k1".to_vec(), b"v1".to_vec(), 1);
    m.put(b"k1".to_vec(), b"v2".to_vec(), 2);
    assert_eq!(m.get(b"k1").unwrap().1, b"v2");
}

#[test]
fn put_ignores_stale_seq() {
    let mut m = Memtable::new();
    m.put(b"k1".to_vec(), b"v2".to_vec(), 5);
    m.put(b"k1".to_vec(), b"v-old".to_vec(), 3);
    assert_eq!(m.get(b"k1").unwrap().1, b"v2");
}

#[test]
fn put_ignores_equal_seq() {
    let mut m = Memtable::new();
    m.put(b"k".to_vec(), b"first".to_vec(), 1);
    m.put(b"k".to_vec(), b"second".to_vec(), 1);
    // Equal seq is treated as stale -> first write wins
    assert_eq!(m.get(b"k").unwrap().1, b"first");
}

#[test]
fn get_missing_key_returns_none() {
    let m = Memtable::new();
    assert!(m.get(b"nonexistent").is_none());
}

#[test]
fn delete_creates_tombstone() {
    let mut m = Memtable::new();
    m.put(b"k1".to_vec(), b"v1".to_vec(), 1);
    m.delete(b"k1".to_vec(), 2);
    assert!(m.get(b"k1").is_none());
    assert_eq!(m.len(), 1); // tombstone still present
}

// -------------------- Load / write tests --------------------

#[test]
fn write_load_10k_unique_keys() {
    let mut m = Memtable::new();
    for i in 0..10_000u64 {
        let key = format!("key{}", i).into_bytes();
        let val = vec![b'x'; 100];
        m.put(key, val, i);
    }
    assert_eq!(m.len(), 10_000);
}

#[test]
fn write_load_with_key_reuse() {
    let mut m = Memtable::new();
    let mut seq = 0u64;
    for i in 0..100_000u64 {
        seq += 1;
        let key = format!("key{}", i % 1_000).into_bytes();
        m.put(key, vec![b'x'; 50], seq);
    }
    assert_eq!(m.len(), 1_000);
}

// -------------------- Iterator ordering --------------------

#[test]
fn iter_yields_sorted_keys() {
    let mut m = Memtable::new();
    m.put(b"c".to_vec(), b"3".to_vec(), 3);
    m.put(b"a".to_vec(), b"1".to_vec(), 1);
    m.put(b"b".to_vec(), b"2".to_vec(), 2);

    let keys: Vec<&[u8]> = m.iter().map(|(k, _)| k).collect();
    assert_eq!(
        keys,
        vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
    );
}

#[test]
fn iter_includes_tombstones() {
    let mut m = Memtable::new();
    m.put(b"a".to_vec(), b"1".to_vec(), 1);
    m.delete(b"b".to_vec(), 2);
    m.put(b"c".to_vec(), b"3".to_vec(), 3);

    let entries: Vec<_> = m.iter().collect();
    assert_eq!(entries.len(), 3);
    assert!(entries[1].1.value.is_none()); // "b" is tombstone
}

#[test]
fn iter_empty_memtable() {
    let m = Memtable::new();
    assert_eq!(m.iter().count(), 0);
}

// -------------------- contains_key --------------------

#[test]
fn contains_key_live_value() {
    let mut m = Memtable::new();
    m.put(b"k".to_vec(), b"v".to_vec(), 1);
    assert!(m.contains_key(b"k"));
}

#[test]
fn contains_key_tombstone() {
    let mut m = Memtable::new();
    m.delete(b"k".to_vec(), 1);
    assert!(m.contains_key(b"k"));
}

#[test]
fn contains_key_missing() {
    let m = Memtable::new();
    assert!(!m.contains_key(b"k"));
}

// -------------------- approx_size tracking --------------------

#[test]
fn approx_size_includes_key_and_value() {
    let mut m = Memtable::new();
    assert_eq!(m.approx_size(), 0);
    // key="ab" (2) + value="ccc" (3) = 5
    m.put(b"ab".to_vec(), b"ccc".to_vec(), 1);
    assert_eq!(m.approx_size(), 5);
}

#[test]
fn approx_size_adjusts_on_overwrite() {
    let mut m = Memtable::new();
    m.put(b"a".to_vec(), b"aaa".to_vec(), 1); // key=1 + val=3 = 4
    assert_eq!(m.approx_size(), 4);
    m.put(b"a".to_vec(), b"bb".to_vec(), 2); // key=1 + val=2 = 3
    assert_eq!(m.approx_size(), 3);
}

#[test]
fn approx_size_adjusts_on_delete() {
    let mut m = Memtable::new();
    m.put(b"a".to_vec(), b"aaa".to_vec(), 1); // 1+3=4
    m.delete(b"a".to_vec(), 2); // value removed, key stays -> 1
    assert_eq!(m.approx_size(), 1);
}

#[test]
fn seq_max_u64() {
    let mut m = Memtable::new();
    m.put(b"k".to_vec(), b"v".to_vec(), u64::MAX);
    assert_eq!(m.get(b"k").unwrap().0, u64::MAX);
}

// -------------------- Clear --------------------

#[test]
fn clear_resets_everything() {
    let mut m = Memtable::new();
    m.put(b"a".to_vec(), b"1".to_vec(), 1);
    m.put(b"b".to_vec(), b"2".to_vec(), 2);
    assert!(!m.is_empty());
    assert!(m.approx_size() > 0);

    m.clear();
    assert_eq!(m.len(), 0);
    assert_eq!(m.approx_size(), 0);
    assert!(m.is_empty());
    assert!(m.get(b"a").is_none());
}

#[test]
fn clear_then_reuse() {
    let mut m = Memtable::new();
    m.put(b"old".to_vec(), b"data".to_vec(), 1);
    m.clear();
    // Seq 1 should work fine after clear (no stale-write issue)
    m.put(b"new".to_vec(), b"data".to_vec(), 1);
    assert_eq!(m.get(b"new").unwrap().0, 1);
    assert!(m.get(b"old").is_none());
}

// -------------------- len / is_empty --------------------

#[test]
fn len_counts_tombstones() {
    let mut m = Memtable::new();
    m.put(b"a".to_vec(), b"1".to_vec(), 1);
    m.delete(b"b".to_vec(), 2);
    assert_eq!(m.len(), 2);
}

#[test]
fn is_empty_on_new() {
    let m = Memtable::new();
    assert!(m.is_empty());
}

#[test]
fn is_empty_after_insert() {
    let mut m = Memtable::new();
    m.put(b"k".to_vec(), b"v".to_vec(), 1);
    assert!(!m.is_empty());
}

#[test]
fn default_creates_empty() {
    let m = Memtable::default();
    assert!(m.is_empty());
    assert_eq!(m.approx_size(), 0);
}

// -------------------- Many / stress tests --------------------

#[test]
fn many_distinct_keys() {
    let mut m = Memtable::new();
    for i in 0u64..1000 {
        m.put(format!("key{:04}", i).into_bytes(), b"v".to_vec(), i);
    }
    assert_eq!(m.len(), 1000);
    // Verify sorted order
    let keys: Vec<&[u8]> = m.iter().map(|(k, _)| k).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
}

#[test]
fn overwrite_same_key_many_times() {
    let mut m = Memtable::new();
    for seq in 1..=10_000u64 {
        m.put(b"k".to_vec(), format!("v{}", seq).into_bytes(), seq);
    }
    assert_eq!(m.len(), 1);
    assert_eq!(m.get(b"k").unwrap().0, 10_000);
}

#[test]
fn alternating_put_delete() {
    let mut m = Memtable::new();
    for i in 0..1_000u64 {
        let seq = i * 2 + 1;
        m.put(b"k".to_vec(), b"v".to_vec(), seq);
        m.delete(b"k".to_vec(), seq + 1);
    }
    assert!(m.get(b"k").is_none());
    assert_eq!(m.len(), 1);
}

#[test]
fn delete_heavy_workload() {
    let mut m = Memtable::new();
    let mut seq = 0u64;
    for _ in 0..10_000 {
        seq += 1;
        m.put(b"k".to_vec(), b"v".to_vec(), seq);
        seq += 1;
        m.delete(b"k".to_vec(), seq);
    }
    assert!(m.get(b"k").is_none());
    assert_eq!(m.len(), 1);
}

// -------------------- Edge cases --------------------

#[test]
fn empty_key() {
    let mut m = Memtable::new();
    m.put(b"".to_vec(), b"val".to_vec(), 1);
    assert_eq!(m.get(b"").unwrap().1, b"val");
}

#[test]
fn empty_value() {
    let mut m = Memtable::new();
    m.put(b"k".to_vec(), b"".to_vec(), 1);
    let (_s, v) = m.get(b"k").unwrap();
    assert!(v.is_empty());
}

#[test]
fn binary_key_and_value() {
    let mut m = Memtable::new();
    let key = vec![0x00, 0xFF, 0x80, 0x01];
    let val = vec![0xDE, 0xAD, 0xBE, 0xEF];
    m.put(key.clone(), val.clone(), 1);
    assert_eq!(m.get(&key).unwrap().1, val);
}

#[test]
fn large_value() {
    let mut m = Memtable::new();
    let val = vec![b'x'; 1_000_000]; // 1 MB
    m.put(b"big".to_vec(), val.clone(), 1);
    assert_eq!(m.get(b"big").unwrap().1.len(), 1_000_000);
    assert_eq!(m.approx_size(), 3 + 1_000_000); // key len (3) + value len
}

#[test]
fn seq_zero_is_valid() {
    let mut m = Memtable::new();
    m.put(b"k".to_vec(), b"v".to_vec(), 0);
    assert_eq!(m.get(b"k").unwrap().0, 0);
}

#[test]
fn approx_size_for_new_tombstone() {
    let mut m = Memtable::new();
    m.delete(b"key".to_vec(), 1); // key=3, no value -> 3
    assert_eq!(m.approx_size(), 3);
}

#[test]
fn approx_size_stale_write_no_change() {
    let mut m = Memtable::new();
    m.put(b"k".to_vec(), b"v".to_vec(), 5); // 1+1=2
    let before = m.approx_size();
    m.put(b"k".to_vec(), b"vvvv".to_vec(), 3); // stale, ignored
    assert_eq!(m.approx_size(), before);
}

#[test]
fn approx_size_multiple_keys() {
    let mut m = Memtable::new();
    m.put(b"a".to_vec(), b"1".to_vec(), 1); // 1+1=2
    m.put(b"bb".to_vec(), b"22".to_vec(), 2); // 2+2=4
    m.put(b"ccc".to_vec(), b"333".to_vec(), 3); // 3+3=6
    assert_eq!(m.approx_size(), 12);
}

#[test]
fn delete_with_stale_seq_ignored() {
    let mut m = Memtable::new();
    m.put(b"k1".to_vec(), b"v1".to_vec(), 5);
    m.delete(b"k1".to_vec(), 3);
    assert_eq!(m.get(b"k1").unwrap().1, b"v1");
}

#[test]
fn delete_nonexistent_key_creates_tombstone() {
    let mut m = Memtable::new();
    m.delete(b"k".to_vec(), 1);
    assert_eq!(m.len(), 1);
    assert!(m.get(b"k").is_none());
    assert!(m.contains_key(b"k"));
}

#[test]
fn put_after_delete_with_higher_seq_resurrects_key() {
    let mut m = Memtable::new();
    m.put(b"k".to_vec(), b"v1".to_vec(), 1);
    m.delete(b"k".to_vec(), 2);
    assert!(m.get(b"k").is_none());

    m.put(b"k".to_vec(), b"v2".to_vec(), 3);
    assert_eq!(m.get(b"k").unwrap().1, b"v2");
}

#[test]
fn put_after_delete_with_lower_seq_ignored() {
    let mut m = Memtable::new();
    m.delete(b"k".to_vec(), 5);
    m.put(b"k".to_vec(), b"v".to_vec(), 3);
    assert!(m.get(b"k").is_none());
}

// -------------------- get_entry & tombstones --------------------

#[test]
fn get_entry_returns_tombstone() {
    let mut m = Memtable::new();
    m.delete(b"k".to_vec(), 1);
    let entry = m.get_entry(b"k").unwrap();
    assert_eq!(entry.seq, 1);
    assert!(entry.value.is_none());
}

#[test]
fn get_entry_returns_none_for_missing_key() {
    let m = Memtable::new();
    assert!(m.get_entry(b"nope").is_none());
}

#[test]
fn get_entry_returns_live_value() {
    let mut m = Memtable::new();
    m.put(b"k".to_vec(), b"v".to_vec(), 1);
    let entry = m.get_entry(b"k").unwrap();
    assert_eq!(entry.value.as_deref(), Some(b"v".as_slice()));
}
