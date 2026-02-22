use super::*;
use std::fs;
use std::io::Cursor;
use tempfile::tempdir;
// use anyhow::Result;

// -------------------- Helpers --------------------

fn make_put(seq: u64, key: &[u8], value: &[u8]) -> WalRecord {
    WalRecord::Put {
        seq,
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

fn make_del(seq: u64, key: &[u8]) -> WalRecord {
    WalRecord::Del {
        seq,
        key: key.to_vec(),
    }
}

fn replay_all(path: &std::path::Path) -> Result<Vec<WalRecord>, WalError> {
    let mut reader = WalReader::open(path)?;
    let mut recs = Vec::new();
    reader.replay(|r| recs.push(r))?;
    Ok(recs)
}

fn replay_from_bytes(data: &[u8]) -> Result<Vec<WalRecord>, WalError> {
    let cursor = Cursor::new(data.to_vec());
    let mut reader = WalReader::from_reader(cursor);
    let mut recs = Vec::new();
    reader.replay(|r| recs.push(r))?;
    Ok(recs)
}

// -------------------- Basic write & replay --------------------

#[test]
fn write_and_replay_put_and_del() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&make_put(1, b"k", b"v1")).unwrap();
        w.append(&make_put(2, b"k2", b"v2")).unwrap();
        w.append(&make_del(3, b"k")).unwrap();
    }

    let recs = replay_all(&path).unwrap();
    assert_eq!(
        recs,
        vec![
            make_put(1, b"k", b"v1"),
            make_put(2, b"k2", b"v2"),
            make_del(3, b"k"),
        ]
    );
}

// -------------------- Truncated tail tolerance --------------------

#[test]
fn truncated_tail_after_valid_records() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&make_put(1, b"k1", b"v1")).unwrap();
        w.append(&make_put(2, b"k2", b"v2")).unwrap();
    }

    // Append a partial record (just the record_len header, no body)
    let mut data = fs::read(&path).unwrap();
    data.extend_from_slice(&[0x20, 0x00, 0x00, 0x00]); // record_len = 32
    fs::write(&path, &data).unwrap();

    // Should recover the two valid records and ignore the truncated tail
    let recs = replay_all(&path).unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0], make_put(1, b"k1", b"v1"));
    assert_eq!(recs[1], make_put(2, b"k2", b"v2"));
}

// -------------------- Single-roundtrip helpers --------------------

#[test]
fn single_put_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&make_put(42, b"hello", b"world")).unwrap();
    }

    let recs = replay_all(&path).unwrap();
    assert_eq!(recs, vec![make_put(42, b"hello", b"world")]);
}

#[test]
fn single_del_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&make_del(7, b"gone")).unwrap();
    }

    let recs = replay_all(&path).unwrap();
    assert_eq!(recs, vec![make_del(7, b"gone")]);
}

// -------------------- Empty WAL --------------------

#[test]
fn replay_empty_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    fs::write(&path, b"").unwrap();

    let recs = replay_all(&path).unwrap();
    assert!(recs.is_empty());
}

#[test]
fn replay_empty_in_memory() {
    let recs = replay_from_bytes(b"").unwrap();
    assert!(recs.is_empty());
}

#[test]
fn truncated_tail_is_ok() {
    let result = replay_from_bytes(&[0, 1, 2, 3, 4, 5, 6, 7]);
    assert!(result.is_ok());
}

// -------------------- File Not Found --------------------

#[test]
fn open_non_existent_file_return_error() {
    let result = WalReader::open("/tmp/non_existent_wal.log");
    assert!(matches!(result, Err(WalError::Io(_))));
}

#[test]
fn sync_to_disk_does_not_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    let mut w = WalWriter::create(&path, false).unwrap();
    w.append(&make_put(1, b"k", b"v")).unwrap();
    w.sync_to_disk().unwrap();
}

#[test]
fn empty_key_and_value() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&make_put(1, b"", b"")).unwrap();
    }

    let recs = replay_all(&path).unwrap();
    assert_eq!(recs, vec![make_put(1, b"", b"")]);
}

// -------------------- Corruption detection --------------------

#[test]
fn corrupt_crc_mismatch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&make_put(1, b"k", b"v")).unwrap();
    }

    // Flip a byte in the body to corrupt the CRC
    let mut data = fs::read(&path).unwrap();
    let last = data.len() - 1;
    data[last] ^= 0xFF;
    fs::write(&path, &data).unwrap();

    let result = replay_all(&path);
    assert!(matches!(result, Err(WalError::Corrupt)));
}

#[test]
fn crc_mismatch_is_corruption() {
    let mut body = Vec::new();
    body.extend_from_slice(&1u64.to_le_bytes()); // seq
    body.push(0); // op = Put
    body.extend_from_slice(&1u32.to_le_bytes()); // key_len
    body.extend_from_slice(b"k");
    body.extend_from_slice(&1u32.to_le_bytes()); // val_len
    body.extend_from_slice(b"v");

    let record_len = (body.len() + 4) as u32;

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&record_len.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes()); // WRONG CRC
    bytes.extend_from_slice(&body);

    let result = replay_from_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn corrupt_record_len_zero() {
    // record_len = 0 is invalid (must be > 4 for CRC)
    let data: Vec<u8> = vec![0, 0, 0, 0];
    let result = replay_from_bytes(&data);
    assert!(matches!(result, Err(WalError::Corrupt)));
}

#[test]
fn corrupt_record_len_too_small() {
    // record_len = 3 is invalid (must be > 4)
    let data: Vec<u8> = vec![3, 0, 0, 0];
    let result = replay_from_bytes(&data);
    assert!(matches!(result, Err(WalError::Corrupt)));
}

#[test]
fn large_value_record() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let big_val = vec![b'x'; 1_000_000]; // 1 MB

    {
        let mut w = WalWriter::create(&path, false).unwrap();
        w.append(&WalRecord::Put {
            seq: 1,
            key: b"big".to_vec(),
            value: big_val.clone(),
        })
        .unwrap();
    }

    let recs = replay_all(&path).unwrap();
    assert_eq!(recs.len(), 1);
    if let WalRecord::Put { value, .. } = &recs[0] {
        assert_eq!(value.len(), 1_000_000);
    } else {
        panic!("expected Put");
    }
}

#[test]
fn truncated_body_after_crc() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&make_put(1, b"k", b"v")).unwrap();
    }

    // Append a partial record: record_len + crc but truncated body
    let mut data = fs::read(&path).unwrap();
    data.extend_from_slice(&[0x20, 0x00, 0x00, 0x00]); // record_len = 32
    data.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD]); // crc
    data.extend_from_slice(&[0x01, 0x02]); // partial body (too short)
    fs::write(&path, &data).unwrap();

    let recs = replay_all(&path).unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0], make_put(1, b"k", b"v"));
}

#[test]
fn append_to_existing_wal() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&make_put(1, b"a", b"1")).unwrap();
    }
    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&make_put(2, b"b", b"2")).unwrap();
    }

    let recs = replay_all(&path).unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0], make_put(1, b"a", b"1"));
    assert_eq!(recs[1], make_put(2, b"b", b"2"));
}

// -------------------- Edge tests --------------------

#[test]
fn seq_zero_and_max() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&make_put(0, b"min", b"v")).unwrap();
        w.append(&make_put(u64::MAX, b"max", b"v")).unwrap();
    }

    let recs = replay_all(&path).unwrap();
    assert_eq!(recs.len(), 2);
    if let WalRecord::Put { seq, .. } = &recs[0] {
        assert_eq!(*seq, 0);
    } else {
        panic!("expected Put");
    }
    if let WalRecord::Put { seq, .. } = &recs[1] {
        assert_eq!(*seq, u64::MAX);
    } else {
        panic!("expected Put");
    }
}

#[test]
fn from_reader_in_memory() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&make_put(1, b"k", b"v")).unwrap();
        w.append(&make_del(2, b"k")).unwrap();
    }

    let data = fs::read(&path).unwrap();
    let recs = replay_from_bytes(&data).unwrap();
    assert_eq!(recs.len(), 2);
}

#[test]
fn binary_key_and_value() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let key = vec![0x00u8, 0xFF, 0x80];
    let val = vec![0xDEu8, 0xAD, 0xBE, 0xEF];

    {
        let mut w = WalWriter::create(&path, true).unwrap();
        w.append(&WalRecord::Put {
            seq: 1,
            key: key.clone(),
            value: val.clone(),
        })
        .unwrap();
    }

    let recs = replay_all(&path).unwrap();
    assert_eq!(recs.len(), 1);
    if let WalRecord::Put {
        seq,
        key: k,
        value: v,
    } = &recs[0]
    {
        assert_eq!(*seq, 1);
        assert_eq!(k, &key);
        assert_eq!(v, &val);
    } else {
        panic!("expected Put");
    }
}

// -------------------- Stress tests --------------------

#[test]
fn many_records_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    let n = 5_000usize;
    {
        let mut w = WalWriter::create(&path, false).unwrap();
        for i in 0..n {
            let key = format!("key{}", i).into_bytes();
            let val = format!("val{}", i).into_bytes();
            w.append(&WalRecord::Put {
                seq: i as u64,
                key,
                value: val,
            })
            .unwrap();
        }
        w.sync_to_disk().unwrap();
    }

    let recs = replay_all(&path).unwrap();
    assert_eq!(recs.len(), n);
    for (i, rec) in recs.iter().enumerate() {
        let expected_key = format!("key{}", i).into_bytes();
        let expected_val = format!("val{}", i).into_bytes();
        assert_eq!(
            rec,
            &WalRecord::Put {
                seq: i as u64,
                key: expected_key,
                value: expected_val,
            }
        );
    }
}

#[test]
fn interleaved_puts_and_dels() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut w = WalWriter::create(&path, false).unwrap();
        for i in 0u64..1000 {
            if i % 3 == 0 {
                w.append(&make_del(i, format!("k{}", i).as_bytes()))
                    .unwrap();
            } else {
                w.append(&make_put(i, format!("k{}", i).as_bytes(), b"v"))
                    .unwrap();
            }
        }
    }

    let recs = replay_all(&path).unwrap();
    assert_eq!(recs.len(), 1000);

    let del_count = recs
        .iter()
        .filter(|r| matches!(r, WalRecord::Del { .. }))
        .count();
    let put_count = recs.len() - del_count;
    // 0,3,6,...,999 -> ceil(1000/3) = 334
    assert_eq!(del_count, 334);
    assert_eq!(put_count, 666);
}
