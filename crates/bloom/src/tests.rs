use super::*;
use std::io::Cursor;

// -------------------- Construction --------------------

#[test]
fn new_creates_valid_filter() {
    let bf = BloomFilter::new(100, 0.01);
    assert!(bf.num_bits() > 0);
    assert!(bf.num_hashes() > 0);
    assert!(!bf.bits.is_empty());
}

#[test]
#[should_panic(expected = "expected_items must be > 0")]
fn new_panics_on_zero_items() {
    BloomFilter::new(0, 0.01);
}

#[test]
#[should_panic(expected = "false_positive_rate must be in (0, 1)")]
fn new_panics_on_zero_fpr() {
    BloomFilter::new(100, 0.0);
}

#[test]
#[should_panic(expected = "false_positive_rate must be in (0, 1)")]
fn new_panics_on_one_fpr() {
    BloomFilter::new(100, 1.0);
}

// -------------------- Insert / Contains --------------------

#[test]
fn inserted_key_is_found() {
    let mut bf = BloomFilter::new(100, 0.01);
    bf.insert(b"hello");
    assert!(bf.may_contain(b"hello"));
}

#[test]
fn missing_key_is_not_found() {
    let bf = BloomFilter::new(100, 0.01);
    assert!(!bf.may_contain(b"hello"));
}

#[test]
fn many_keys_all_found() {
    let mut bf = BloomFilter::new(1000, 0.01);
    for i in 0..1000u64 {
        bf.insert(&i.to_le_bytes());
    }
    for i in 0..1000u64 {
        assert!(
            bf.may_contain(&i.to_le_bytes()),
            "key {} should be found",
            i
        );
    }
}

#[test]
fn false_positive_rate_is_reasonable() {
    let n = 10_000;
    let fpr = 0.01;
    let mut bf = BloomFilter::new(n, fpr);

    // Insert n keys
    for i in 0..n as u64 {
        bf.insert(&i.to_le_bytes());
    }

    // Test n keys that were NOT inserted
    let mut false_positives = 0;
    let test_count = 10_000;
    for i in (n as u64)..(n as u64 + test_count) {
        if bf.may_contain(&i.to_le_bytes()) {
            false_positives += 1;
        }
    }

    let actual_fpr = false_positives as f64 / test_count as f64;
    // Allow up to 3x the target FPR (statistical variance)
    assert!(
        actual_fpr < fpr * 3.0,
        "FPR too high: {:.4} (target {:.4})",
        actual_fpr,
        fpr
    );
}

#[test]
fn empty_key() {
    let mut bf = BloomFilter::new(10, 0.01);
    bf.insert(b"");
    assert!(bf.may_contain(b""));
}

#[test]
fn binary_key() {
    let mut bf = BloomFilter::new(10, 0.01);
    let key = vec![0u8, 1, 2, 255, 254, 253];
    bf.insert(&key);
    assert!(bf.may_contain(&key));
}

// -------------------- Serialization --------------------

#[test]
fn roundtrip_serialize_deserialize() {
    let mut bf = BloomFilter::new(500, 0.01);
    for i in 0..500u64 {
        bf.insert(&i.to_le_bytes());
    }

    // Serialize
    let mut buf = Vec::new();
    bf.write_to(&mut buf).unwrap();
    assert_eq!(buf.len(), bf.serialized_size());

    // Deserialize
    let mut cursor = Cursor::new(&buf);
    let bf2 = BloomFilter::read_from(&mut cursor).unwrap();

    assert_eq!(bf2.num_bits(), bf.num_bits());
    assert_eq!(bf2.num_hashes(), bf.num_hashes());
    assert_eq!(bf2.bits, bf.bits);

    // All inserted keys still found
    for i in 0..500u64 {
        assert!(
            bf2.may_contain(&i.to_le_bytes()),
            "key {} missing after roundtrip",
            i
        );
    }
}

#[test]
fn serialized_size_is_correct() {
    let bf = BloomFilter::new(100, 0.05);
    // 8 (num_bits) + 4 (num_hashes) + 4 (bits_len) + bits.len()
    assert_eq!(bf.serialized_size(), 16 + bf.bits.len());
}

#[test]
fn deserialize_rejects_oversized_bloom() {
    // Craft a bloom with bits_len = 256 MiB (exceeds 128 MiB cap)
    let mut buf = Vec::new();
    buf.extend_from_slice(&64u64.to_le_bytes()); // num_bits
    buf.extend_from_slice(&3u32.to_le_bytes()); // num_hashes
    buf.extend_from_slice(&(256 * 1024 * 1024u32).to_le_bytes()); // bits_len = 256 MiB

    let mut cursor = Cursor::new(&buf);
    let result = BloomFilter::read_from(&mut cursor);
    assert!(result.is_err());
}

// -------------------- Debug --------------------

#[test]
fn debug_impl_works() {
    let bf = BloomFilter::new(100, 0.01);
    let debug = format!("{:?}", bf);
    assert!(debug.contains("BloomFilter"));
    assert!(debug.contains("num_bits"));
    assert!(debug.contains("num_hashes"));
}

// -------------------- Edge cases --------------------

#[test]
fn single_item_filter() {
    let mut bf = BloomFilter::new(1, 0.01);
    bf.insert(b"only");
    assert!(bf.may_contain(b"only"));
}

#[test]
fn very_low_fpr() {
    let bf = BloomFilter::new(100, 0.0001);
    // Should have many bits and hashes
    assert!(bf.num_bits() > 1000);
    assert!(bf.num_hashes() > 5);
}

#[test]
fn high_fpr_still_works() {
    let mut bf = BloomFilter::new(100, 0.5);
    bf.insert(b"test");
    assert!(bf.may_contain(b"test"));
}
