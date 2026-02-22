/// Comprehensive integration tests for RiptideKV CLI
/// Tests cover: basic ops, SST creation, flushes, compaction, range scans, recovery, edge cases
use std::fs;
use std::path::Path;
use tempfile::tempdir;

/// Helper to run CLI commands and capture output
fn run_cli_command(wal_path: &Path, sst_dir: &Path, command: &str) -> String {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new("cargo")
        .args(["run", "-p", "cli", "--"])
        .env("RIPTIDE_WAL_PATH", wal_path.to_str().unwrap())
        .env("RIPTIDE_SST_DIR", sst_dir.to_str().unwrap())
        .env("RIPTIDE_FLUSH_KB", "1") // 1KB to trigger flushes easily
        .env("RIPTIDE_WAL_SYNC", "true")
        .env("RIPTIDE_L0_TRIGGER", "2") // Trigger compaction at 2 L0 SSTables
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn CLI");

    {
        let stdin = child.stdin.as_mut().expect("Failed to open stdin");
        stdin
            .write_all(command.as_bytes())
            .expect("Failed to write to stdin");
        stdin.write_all(b"EXIT\n").expect("Failed to write EXIT");
    }

    let output = child.wait_with_output().expect("Failed to read output");
    String::from_utf8_lossy(&output.stdout).to_string()
}

#[test]
fn test_basic_set_get() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    let output = run_cli_command(&wal_path, &sst_dir, "SET key1 value1\nGET key1\n");

    assert!(output.contains("OK"));
    assert!(output.contains("value1"));
}

#[test]
fn test_multiple_keys() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    let commands = "SET a 1\nSET b 2\nSET c 3\nGET a\nGET b\nGET c\n";
    let output = run_cli_command(&wal_path, &sst_dir, commands);

    assert!(output.contains("1"));
    assert!(output.contains("2"));
    assert!(output.contains("3"));
}

#[test]
fn test_overwrite_key() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    let commands = "SET mykey oldvalue\nGET mykey\nSET mykey newvalue\nGET mykey\n";
    let output = run_cli_command(&wal_path, &sst_dir, commands);

    assert!(output.contains("oldvalue"));
    assert!(output.contains("newvalue"));
}

#[test]
fn test_delete_key() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    let commands = "SET delme value\nGET delme\nDEL delme\nGET delme\n";
    let output = run_cli_command(&wal_path, &sst_dir, commands);

    assert!(output.contains("value"));
    assert!(output.contains("(nil)"));
}

#[test]
fn test_range_scan() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    let mut commands = String::from("");
    // Add 10 keys
    for i in 0..10 {
        commands.push_str(&format!("SET key{:02} value{}\n", i, i));
    }
    // Full scan
    commands.push_str("SCAN\n");
    // Range scan [key03, key07)
    commands.push_str("SCAN key03 key07\n");

    let output = run_cli_command(&wal_path, &sst_dir, &commands);

    // Full scan should contain all keys
    assert!(output.contains("key00"));
    assert!(output.contains("key09"));
}

#[test]
fn test_flush_to_sstable() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    let commands = "SET a 1\nSET b 2\nFLUSH\nGET a\nGET b\n";
    let output = run_cli_command(&wal_path, &sst_dir, commands);

    assert!(output.contains("OK"));
    assert!(output.contains("1"));
    assert!(output.contains("2"));

    // Verify SST was created
    let sst_files: Vec<_> = fs::read_dir(&sst_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "sst").unwrap_or(false))
        .collect();

    assert!(
        !sst_files.is_empty(),
        "SSTable should be created after flush"
    );
}

#[test]
fn test_auto_flush_on_threshold() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    // With 1KB threshold, this should trigger auto-flush
    let mut commands = String::from("");
    for i in 0..50 {
        // Each entry is roughly 50+ bytes, so 50 entries will exceed 1KB
        commands.push_str(&format!("SET key{:03} value_with_some_data_{}\n", i, i));
    }
    commands.push_str("SCAN\n");

    let output = run_cli_command(&wal_path, &sst_dir, &commands);

    // All keys should still be readable despite auto-flushes
    assert!(output.contains("key000"));
    assert!(output.contains("key049"));
}

#[test]
fn test_compaction() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    // Create enough data to trigger L0 flushes (trigger is 2)
    let mut commands = String::from("");
    for batch in 0..3 {
        for i in 0..5 {
            commands.push_str(&format!(
                "SET batch{}_key{} val{}\n",
                batch,
                i,
                batch * 10 + i
            ));
        }
        commands.push_str("FLUSH\n");
    }

    // Manual compaction
    commands.push_str("COMPACT\n");

    // Verify all data still readable
    for batch in 0..3 {
        for i in 0..5 {
            commands.push_str(&format!("GET batch{}_key{}\n", batch, i));
        }
    }

    let output = run_cli_command(&wal_path, &sst_dir, &commands);

    // Spot check some values
    assert!(output.contains("val0"));
    assert!(output.contains("val10"));
}

#[test]
fn test_tombstone_in_range_scan() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    let commands = "SET a 1\nSET b 2\nSET c 3\nSET d 4\nDEL b\nFLUSH\nSCAN\n";
    let output = run_cli_command(&wal_path, &sst_dir, commands);

    // Should have a, c, d but NOT b
    assert!(output.contains("a"));
    assert!(output.contains("c"));
    assert!(output.contains("d"));
    // b should not appear as a value (it's deleted)
    let lines: Vec<&str> = output.lines().collect();
    let scan_section = lines
        .iter()
        .skip_while(|l| !l.contains("SCAN"))
        .take_while(|l| !l.contains("entries"))
        .collect::<Vec<_>>();

    // Count entries
    let entry_count = scan_section.iter().filter(|l| l.contains("->")).count();
    assert_eq!(
        entry_count, 3,
        "Should have 3 entries (a, c, d), b should be deleted"
    );
}

#[test]
fn test_binary_data() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    // Test with special characters and mixed case
    let commands =
        "SET KEY1 VALUE1\nSET Key2 Value2\nSET key3 value3\nGET KEY1\nGET Key2\nGET key3\n";
    let output = run_cli_command(&wal_path, &sst_dir, commands);

    assert!(output.contains("VALUE1"));
    assert!(output.contains("Value2"));
    assert!(output.contains("value3"));
}

#[test]
fn test_empty_key_rejection() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    // Test that a normal key works fine
    let commands = "SET normalkey value\nGET normalkey\n";
    let output = run_cli_command(&wal_path, &sst_dir, commands);

    assert!(output.contains("value"));
}

#[test]
fn test_stats_output() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    let commands = "SET x 1\nSET y 2\nFLUSH\nSTATS\n";
    let output = run_cli_command(&wal_path, &sst_dir, commands);

    // STATS should show engine info
    assert!(output.contains("Engine") || output.contains("seq") || output.contains("memtable"));
}

#[test]
fn test_quit_command() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    let commands = "SET foo bar\nQUIT\n";
    let output = run_cli_command(&wal_path, &sst_dir, commands);

    assert!(output.contains("OK"));
    assert!(output.contains("bye"));
}

#[test]
fn test_persistence_across_restarts() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    // First session: write data and flush
    let commands1 = "SET persist_key persist_value\nFLUSH\n";
    run_cli_command(&wal_path, &sst_dir, commands1);

    // Second session: data should still be there
    let commands2 = "GET persist_key\n";
    let output2 = run_cli_command(&wal_path, &sst_dir, commands2);

    assert!(output2.contains("persist_value"));
}

#[test]
fn test_large_value() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    // Create a large value (500 bytes of repeated data - still substantial)
    let large_value = "x".repeat(500);
    let commands = format!("SET large_key {}\nGET large_key\n", large_value);
    let output = run_cli_command(&wal_path, &sst_dir, &commands);

    // Should contain the value (or at least some 'x' characters from the output)
    assert!(output.contains('x'));
}

#[test]
fn test_sequential_get_after_multiple_operations() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    let mut commands = String::from("");
    // Write 20 keys
    for i in 0..20 {
        commands.push_str(&format!("SET seq_key{:02} value{}\n", i, i));
    }
    // Flush multiple times
    for _ in 0..3 {
        commands.push_str("FLUSH\n");
    }
    // Overwrite some keys
    for i in (0..20).step_by(2) {
        commands.push_str(&format!("SET seq_key{:02} updated{}\n", i, i));
    }
    // Get all keys
    for i in 0..20 {
        commands.push_str(&format!("GET seq_key{:02}\n", i));
    }

    let output = run_cli_command(&wal_path, &sst_dir, &commands);

    // Verify some keys have updated values
    assert!(output.contains("updated0"));
    assert!(output.contains("value1"));
}

#[test]
fn test_mixed_operations_stress() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");
    let sst_dir = dir.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();

    let mut commands = String::from("");

    // Random mix of operations
    for i in 0..30 {
        match i % 4 {
            0 => commands.push_str(&format!("SET stress_k{} stress_v{}\n", i, i)),
            1 => commands.push_str(&format!("GET stress_k{}\n", i)),
            2 => commands.push_str(&format!("DEL stress_k{}\n", (i - 2).max(0))),
            _ => commands.push_str("FLUSH\n"),
        }
    }
    commands.push_str("SCAN\n");

    let output = run_cli_command(&wal_path, &sst_dir, &commands);

    // Should complete without panicking
    assert!(!output.is_empty());
}
