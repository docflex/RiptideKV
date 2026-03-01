//! # CLI - RiptideKV Interactive Shell
//!
//! A REPL-style command-line interface for the RiptideKV storage engine.
//! Reads commands from stdin, executes them against the engine, and prints
//! results to stdout. Designed for both interactive use and scripted testing
//! (pipe commands via stdin).
//!
//! ## Commands
//!
//! ```text
//! SET key value      Insert or update a key-value pair
//! GET key            Look up a key (prints value or "(nil)")
//! DEL key            Delete a key (writes a tombstone)
//! SCAN [start] [end] Range scan (inclusive start, exclusive end)
//! FLUSH              Force flush memtable to SSTable
//! COMPACT            Trigger manual compaction (L0 + L1 -> L1)
//! STATS              Print engine debug info
//! EXIT / QUIT        Shut down gracefully
//! ```
//!
//! ## Configuration
//!
//! All settings are controlled via environment variables:
//!
//! ```text
//! RIPTIDE_WAL_PATH   WAL file path           (default: "wal.log")
//! RIPTIDE_SST_DIR    SSTable directory       (default: "data/sst")
//! RIPTIDE_FLUSH_KB   Flush threshold in KiB  (default: 1024 = 1 MiB)
//! RIPTIDE_WAL_SYNC   fsync every WAL append  (default: "true")
//! RIPTIDE_L0_TRIGGER L0 compaction trigger   (default: 4, 0 = disabled)
//! ```
//!
//! ## Example
//!
//! ```text
//! $ cargo run -p cli
//! RiptideKV started (seq=0, wal=wal.log, sst_dir=data/sst, flush=1024KiB, l0_trigger=4)
//! > SET name Alice
//! OK
//! > GET name
//! Alice
//! > SCAN
//! name -> Alice
//! (1 entries)
//! > EXIT
//! bye
//! ```
use anyhow::Result;
use config::EngineConfig;
use engine::Engine;
use std::io::{self, BufRead, Write};

fn main() -> Result<()> {
    // Load all configuration from environment variables (single source of truth).
    // See `config::EngineConfig::from_env()` for the full list of supported
    // variables and their defaults.
    let cfg = EngineConfig::from_env();

    let flush_kb = cfg.flush_threshold_bytes / 1024;
    let wal_path_display = cfg.wal_path.display().to_string();
    let sst_path_display = cfg.sst_dir.display().to_string();
    let l0_trigger = cfg.l0_compaction_trigger;

    let mut engine = Engine::new(cfg)?;

    println!(
        "RiptideKV started (seq={}, wal={}, sst_dir={}, flush={}KiB, l0_trigger={})",
        engine.seq(),
        wal_path_display,
        sst_path_display,
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
                        engine.l0_sstable_count(),
                        engine.l1_sstable_count()
                    ),
                    Err(e) => println!("ERR compact failed: {}", e),
                },
                "FLUSH" => match engine.force_flush() {
                    Ok(()) => println!(
                        "OK (L0={}, L1={})",
                        engine.l0_sstable_count(),
                        engine.l1_sstable_count()
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
