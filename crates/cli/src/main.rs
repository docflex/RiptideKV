///! # CLI - RiptideKV Interactive Shell
///!
///! A REPL-style command-line interface for the RiptideKV storage engine.
///! Reads commands from stdin, executes them against the engine, and prints
///! results to stdout. Designed for both interactive use and scripted testing
///! (pipe commands via stdin).
///!
///! ## Commands
///!
///! ```text
///! SET key value      Insert or update a key-value pair
///! GET key            Look up a key (prints value or "(nil)")
///! DEL key            Delete a key (writes a tombstone)
///! SCAN [start] [end] Range scan (inclusive start, exclusive end)
///! FLUSH              Force flush memtable to SSTable
///! COMPACT            Trigger manual compaction (L0 + L1 -> L1)
///! STATS              Print engine debug info
///! EXIT / QUIT        Shut down gracefully
///! ```
///!
///! ## Configuration
///!
///! All settings are controlled via environment variables:
///!
///! ```text
///! RIPTIDE_WAL_PATH   WAL file path           (default: "wal.log")
///! RIPTIDE_SST_DIR    SSTable directory       (default: "data/sst")
///! RIPTIDE_FLUSH_KB   Flush threshold in KiB  (default: 1024 = 1 MiB)
///! RIPTIDE_WAL_SYNC   fsync every WAL append  (default: "true")
///! RIPTIDE_L0_TRIGGER L0 compaction trigger   (default: 4, 0 = disabled)
///! ```
///!
///! ## Example
///!
///! ```text
///! $ cargo run -p cli
///! RiptideKV started (seq=0, wal=wal.log, sst_dir=data/sst, flush=1024KiB, l0_trigger=4)
///! > SET name Alice
///! OK
///! > GET name
///! Alice
///! > SCAN
///! name -> Alice
///! (1 entries)
///! > EXIT
///! bye
///! ```

use anyhow::Result;
use engine::Engine;
use std::io::{self, BufRead, Write};

/// Reads a configuration value from the environment, falling back to `default`.
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn main() -> Result<()> {
    // Configuration via environment variables with sensible defaults.
    //
    //  RIPTIDE_WAL_PATH   - WAL file path           (default: "wal.log")
    //  RIPTIDE_SST_DIR    - SSTable directory       (default: "data/sst")
    //  RIPTIDE_FLUSH_KB   - flush threshold in KiB  (default: 1024 = 1 MiB)
    //  RIPTIDE_WAL_SYNC   - fsync every WAL append  (default: "true")
    //  RIPTIDE_L0_TRIGGER - L0 compaction trigger   (default: 4, 0 = disabled)
    let wal_path = env_or("RIPTIDE_WAL_PATH", "wal.log");
    let sst_dir = env_or("RIPTIDE_SST_DIR", "data/sst");
    let flush_kb: usize = env_or("RIPTIDE_FLUSH_KB", "1024").parse().unwrap_or(1024);
    let flush_threshold = flush_kb * 1024;
    let wal_sync: bool = env_or("RIPTIDE_WAL_SYNC", "true").parse().unwrap_or(true);
    let l0_trigger: usize = env_or("RIPTIDE_L0_TRIGGER", "4").parse().unwrap_or(4);

    let mut engine = Engine::new(&wal_path, &sst_dir, flush_threshold, wal_sync)?;
    engine.set_l0_compaction_trigger(l0_trigger);

    println!(
        "RiptideKV started (seq={}, wal={}, sst_dir={}, flush={}KiB, l0_trigger={})",
        engine.seq(),
        wal_path,
        sst_dir,
        flush_kb,
        l0_trigger
    );
    println!("Commands: SET key value | GET key | DEL key | SCAN [start] [end]");
    println!("          COMPACT | FLUSH | STATS | EXIT");
    print!("> ");
    io::stdout().flush().ok();

    let stdin = io::stdin();

    for line in stdin.lock().lines() {
        let line = line?;
        let mut parts = line.split_whitespace();
        if let Some(cmd) = parts.next() {
            match cmd.to_uppercase().as_str() {
                "SET" => {
                    if let Some(k) = parts.next() {
                        let v: String = parts.collect::<Vec<&str>>().join(" ");
                        if v.is_empty() {
                            println!("ERR usage: SET key value");
                        } else {
                            match engine.set(k.as_bytes().to_vec(), v.as_bytes().to_vec()) {
                                Ok(()) => println!("OK"),
                                Err(e) => println!("ERR set failed: {}", e),
                            }
                        }
                    } else {
                        println!("ERR usage: SET key value");
                    }
                }
                "GET" => {
                    if let Some(k) = parts.next() {
                        match engine.get(k.as_bytes()) {
                            Ok(Some((_seq, v))) => println!("{}", String::from_utf8_lossy(&v)),
                            Ok(None) => println!("(nil)"),
                            Err(e) => println!("ERR read failed: {}", e),
                        }
                    } else {
                        println!("ERR usage: GET key");
                    }
                }
                "DEL" => {
                    if let Some(k) = parts.next() {
                        match engine.del(k.as_bytes().to_vec()) {
                            Ok(()) => println!("OK"),
                            Err(e) => println!("ERR del failed: {}", e),
                        }
                    } else {
                        println!("ERR usage: DEL key");
                    }
                }
                "SCAN" => {
                    let start = parts.next().unwrap_or("").as_bytes();
                    let end = parts.next().unwrap_or("").as_bytes();
                    match engine.scan(start, end) {
                        Ok(results) => {
                            if results.is_empty() {
                                println!("(empty)");
                            } else {
                                for (k, v) in &results {
                                    println!(
                                        "{} -> {}",
                                        String::from_utf8_lossy(k),
                                        String::from_utf8_lossy(v)
                                    );
                                }
                                println!("({} entries)", results.len());
                            }
                        }
                        Err(e) => println!("ERR scan failed: {}", e),
                    }
                }
                "COMPACT" => match engine.compact() {
                    Ok(()) => println!(
                        "OK (L0={}, L1={})",
                        engine.l0_sstable_count(),engine.l1_sstable_count()
                    ),
                    Err(e) => println!("ERR compact failed: {}", e),
                },
                "FLUSH" => match engine.force_flush() {
                    Ok(()) => println!(
                        "OK (L0={}, L1={})",
                        engine.l0_sstable_count(),engine.l1_sstable_count()
                    ),
                    Err(e) => println!("ERR flush failed: {}", e),
                },
                "STATS" => {
                    println!("{:?}", engine);
                }
                "EXIT" | "QUIT" => {
                    println!("bye");
                    break;
                }
                other => {
                    println!("unknown command: {}", other);
                }
            }
        }

        print!("> ");
        io::stdout().flush().ok();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use engine::replay_wal_and_build;
    use memtable::Memtable;
    use wal::{WalRecord, WalWriter};

    #[test]
    fn wal_replay_rebuilds_memtable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&WalRecord::Put {
                seq: 1,
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            })
            .unwrap();
            w.append(&WalRecord::Del {
                seq: 2,
                key: b"a".to_vec(),
            })
            .unwrap();
            w.append(&WalRecord::Put {
                seq: 3,
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            })
            .unwrap();
        }

        let mut mem = Memtable::new();
        let max_seq = replay_wal_and_build(path.to_str().unwrap(), &mut mem).unwrap();

        assert_eq!(max_seq, 3);
        assert!(mem.get(b"a").is_none());
        assert_eq!(mem.get(b"b").unwrap().1, b"2");
    }

    #[test]
    fn wal_durability_without_memtable_update() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&WalRecord::Put {
                seq: 1,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            })
            .unwrap();
            // crash here: memtable never updated
        }

        let mut mem = Memtable::new();
        replay_wal_and_build(path.to_str().unwrap(), &mut mem).unwrap();

        assert_eq!(mem.get(b"k").unwrap().1, b"v");
    }

    #[test]
    fn wal_crc_detects_corruption() {
        use byteorder::{LittleEndian, WriteBytesExt};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        // Build a complete body: seq=1, op=Put(0), key_len=1, key='k', val_len=1, val='v'
        let mut body = Vec::new();
        body.write_u64::<LittleEndian>(1).unwrap();
        body.write_u8(0).unwrap(); // op = Put
        body.write_u32::<LittleEndian>(1).unwrap(); // key_len
        body.extend_from_slice(b"k");
        body.write_u32::<LittleEndian>(1).unwrap(); // val_len
        body.extend_from_slice(b"v");

        let record_len = (body.len() + 4) as u32; // body + crc

        // Intentionally write a bogus CRC (0) so verify will fail
        let mut file_bytes = Vec::new();
        file_bytes.write_u32::<LittleEndian>(record_len).unwrap();
        file_bytes.write_u32::<LittleEndian>(0).unwrap(); // bogus CRC
        file_bytes.extend_from_slice(&body);

        std::fs::write(&path, &file_bytes).unwrap();

        let mut mem = Memtable::new();
        let res = replay_wal_and_build(path.to_str().unwrap(), &mut mem);

        assert!(res.is_err());
    }
}

#[cfg(test)]
mod load_test {
    use memtable::Memtable;

    #[test]
    fn write_load_test() {
        let mut mem = Memtable::new();
        let mut seq = 0;

        for i in 0..1_000_000 {
            seq += 1;
            let key = format!("key{}", i % 10_000).into_bytes();
            let val = vec![b'x'; 100];
            mem.put(key, val, seq);
        }

        assert!(mem.len() <= 10_000);
    }

    #[test]
    fn delete_heavy_workload() {
        let mut mem = Memtable::new();
        let mut seq = 0;

        for _i in 0..100_000 {
            seq += 1;
            mem.put(b"k".to_vec(), b"v".to_vec(), seq);
            seq += 1;
            mem.delete(b"k".to_vec(), seq);
        }

        assert!(mem.get(b"k").is_none());
        assert_eq!(mem.len(), 1);
    }
}
