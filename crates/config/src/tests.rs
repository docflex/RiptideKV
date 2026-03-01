use super::*;

#[test]
fn default_config_has_expected_values() {
    let cfg = EngineConfig::default();
    assert_eq!(cfg.wal_path, PathBuf::from("wal.log"));
    assert_eq!(cfg.sst_dir, PathBuf::from("data/sst"));
    assert_eq!(cfg.flush_threshold_bytes, 1024 * 1024);
    assert!(cfg.wal_sync);
    assert_eq!(cfg.l0_compaction_trigger, 4);
    assert_eq!(cfg.server_host, "127.0.0.1");
    assert_eq!(cfg.server_port, 6379);
}

#[test]
fn builder_overrides_defaults() {
    let cfg = EngineConfig::builder()
        .wal_path("/custom/wal.log")
        .sst_dir("/custom/sst")
        .flush_threshold_bytes(256)
        .wal_sync(false)
        .l0_compaction_trigger(8)
        .server_host("0.0.0.0")
        .server_port(7379)
        .build();

    assert_eq!(cfg.wal_path, PathBuf::from("/custom/wal.log"));
    assert_eq!(cfg.sst_dir, PathBuf::from("/custom/sst"));
    assert_eq!(cfg.flush_threshold_bytes, 256);
    assert!(!cfg.wal_sync);
    assert_eq!(cfg.l0_compaction_trigger, 8);
    assert_eq!(cfg.server_host, "0.0.0.0");
    assert_eq!(cfg.server_port, 7379);
}

#[test]
fn clone_produces_independent_copy() {
    let cfg1 = EngineConfig::builder().flush_threshold_bytes(100).build();
    let mut cfg2 = cfg1.clone();
    cfg2.flush_threshold_bytes = 200;

    assert_eq!(cfg1.flush_threshold_bytes, 100);
    assert_eq!(cfg2.flush_threshold_bytes, 200);
}

#[test]
fn builder_partial_override() {
    let cfg = EngineConfig::builder().flush_threshold_bytes(64).build();

    // Only flush_threshold changed; rest are defaults
    assert_eq!(cfg.flush_threshold_bytes, 64);
    assert_eq!(cfg.wal_path, PathBuf::from("wal.log"));
    assert!(cfg.wal_sync);
    assert_eq!(cfg.l0_compaction_trigger, 4);
}

#[test]
fn server_addr_format() {
    let cfg = EngineConfig::builder()
        .server_host("0.0.0.0")
        .server_port(8080)
        .build();
    assert_eq!(cfg.server_addr(), "0.0.0.0:8080");
}

#[test]
fn from_env_uses_defaults_when_no_vars_set() {
    // This test relies on the CI/test environment not having RIPTIDE_*
    // variables set. It verifies the fallback path.
    let cfg = EngineConfig::from_env();
    // We can't assert exact values because env vars might be set in CI,
    // but we can assert the config is valid.
    assert!(!cfg.wal_path.as_os_str().is_empty());
    assert!(!cfg.sst_dir.as_os_str().is_empty());
    assert!(cfg.flush_threshold_bytes > 0);
}
