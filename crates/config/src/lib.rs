//! # Config – Unified Configuration for RiptideKV
//!
//! This crate provides a single source of truth for all RiptideKV configuration.
//! Every component — the CLI, the RESP server, tests — constructs an [`EngineConfig`]
//! and passes it to the engine. This eliminates duplicated defaults, scattered
//! env-var parsing, and positional-argument constructors.
//!
//! ## Design Decisions
//!
//! - **Single struct, not scattered args**: `Engine::new()` previously took 4
//!   positional arguments (`wal_path`, `sst_dir`, `flush_threshold`, `wal_sync`)
//!   plus a post-construction setter for `l0_compaction_trigger`. This was fragile
//!   and easy to misconfigure. A single config struct is self-documenting.
//!
//! - **Builder pattern**: Tests need custom configs (tiny flush thresholds, sync
//!   disabled, etc.) while production uses env vars. The builder pattern supports
//!   both ergonomically.
//!
//! - **from_env() factory**: Both the CLI and the RESP server need to load
//!   config from environment variables. Centralizing this avoids duplication.
//!
//! - **Defaults defined once**: Constants like `DEFAULT_FLUSH_THRESHOLD_KB` and
//!   `DEFAULT_L0_COMPACTION_TRIGGER` live here, not scattered across crates.
//!
//! ## Example
//!
//! ```rust
//! use config::EngineConfig;
//!
//! // Production: load from environment variables
//! let cfg = EngineConfig::from_env();
//!
//! // Tests: use builder for precise control
//! let cfg = EngineConfig::builder()
//!     .wal_path("/tmp/test/wal.log")
//!     .sst_dir("/tmp/test/sst")
//!     .flush_threshold_bytes(64)
//!     .wal_sync(false)
//!     .l0_compaction_trigger(0)
//!     .build();
//! ```

use std::path::PathBuf;

// ─── Default constants (single source of truth) ─────────────────────────────

/// Default WAL file path.
pub const DEFAULT_WAL_PATH: &str = "wal.log";

/// Default SSTable directory.
pub const DEFAULT_SST_DIR: &str = "data/sst";

/// Default flush threshold in KiB. The memtable is flushed to an SSTable
/// when its approximate byte size reaches `flush_threshold_kb * 1024`.
pub const DEFAULT_FLUSH_THRESHOLD_KB: usize = 1024;

/// Default number of L0 SSTables that triggers automatic compaction.
/// Set to `0` to disable auto-compaction.
pub const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4;

/// Default WAL sync mode. When `true`, every WAL append is followed by
/// `fsync` for maximum durability.
pub const DEFAULT_WAL_SYNC: bool = true;

/// Default host address for the RESP server.
pub const DEFAULT_SERVER_HOST: &str = "127.0.0.1";

/// Default port for the RESP server.
pub const DEFAULT_SERVER_PORT: u16 = 6379;

// ─── EngineConfig ───────────────────────────────────────────────────────────

/// Unified configuration for the RiptideKV storage engine.
///
/// This struct is the **single source of truth** for all engine parameters.
/// It is consumed by `Engine::new()`, the CLI, the RESP server, and tests.
///
/// # Fields
///
/// | Field | Default | Env Variable |
/// |-------|----------|--------------|
/// | `wal_path` | `wal.log` | `RIPTIDE_WAL_PATH` |
/// | `sst_dir` | `data/sst` | `RIPTIDE_SST_DIR` |
/// | `flush_threshold_bytes` | `1048576` (1 MiB) | `RIPTIDE_FLUSH_KB` (in KiB) |
/// | `wal_sync` | `true` | `RIPTIDE_WAL_SYNC` |
/// | `l0_compaction_trigger` | `4` | `RIPTIDE_L0_TRIGGER` |
/// | `server_host` | `127.0.0.1` | `RIPTIDE_HOST` |
/// | `server_port` | `6379` | `RIPTIDE_PORT` |
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Path to the write-ahead log file.
    pub wal_path: PathBuf,

    /// Directory where SSTable files are stored.
    pub sst_dir: PathBuf,

    /// Memtable byte-size threshold that triggers a flush to SSTable.
    /// When `memtable.approx_size() >= flush_threshold_bytes`, the memtable
    /// is flushed to a new SSTable on disk.
    pub flush_threshold_bytes: usize,

    /// If `true`, every WAL append is followed by `fsync` for durability.
    /// Setting this to `false` batches writes for better throughput at the
    /// cost of losing the most recent writes on crash.
    pub wal_sync: bool,

    /// Number of L0 SSTables that triggers automatic compaction after a flush.
    /// Set to `0` to disable auto-compaction (caller must invoke `compact()`
    /// manually).
    pub l0_compaction_trigger: usize,

    /// Host address for the RESP server to bind to.
    pub server_host: String,

    /// Port for the RESP server to listen on.
    pub server_port: u16,
}

impl Default for EngineConfig {
    /// Returns the default configuration with sensible production values.
    ///
    /// These defaults match the values previously hard-coded across the CLI
    /// and engine crates.
    fn default() -> Self {
        Self {
            wal_path: PathBuf::from(DEFAULT_WAL_PATH),
            sst_dir: PathBuf::from(DEFAULT_SST_DIR),
            flush_threshold_bytes: DEFAULT_FLUSH_THRESHOLD_KB * 1024,
            wal_sync: DEFAULT_WAL_SYNC,
            l0_compaction_trigger: DEFAULT_L0_COMPACTION_TRIGGER,
            server_host: DEFAULT_SERVER_HOST.to_string(),
            server_port: DEFAULT_SERVER_PORT,
        }
    }
}

impl EngineConfig {
    /// Creates a configuration by reading environment variables, falling back
    /// to defaults for any variable that is not set or cannot be parsed.
    ///
    /// # Environment Variables
    ///
    /// | Variable | Type | Default |
    /// |----------|------|---------|
    /// | `RIPTIDE_WAL_PATH` | `String` | `wal.log` |
    /// | `RIPTIDE_SST_DIR` | `String` | `data/sst` |
    /// | `RIPTIDE_FLUSH_KB` | `usize` (KiB) | `1024` |
    /// | `RIPTIDE_WAL_SYNC` | `bool` | `true` |
    /// | `RIPTIDE_L0_TRIGGER` | `usize` | `4` |
    /// | `RIPTIDE_HOST` | `String` | `127.0.0.1` |
    /// | `RIPTIDE_PORT` | `u16` | `6379` |
    pub fn from_env() -> Self {
        let defaults = Self::default();

        let wal_path = env_or("RIPTIDE_WAL_PATH", DEFAULT_WAL_PATH);
        let sst_dir = env_or("RIPTIDE_SST_DIR", DEFAULT_SST_DIR);

        let flush_kb: usize = env_or("RIPTIDE_FLUSH_KB", &DEFAULT_FLUSH_THRESHOLD_KB.to_string())
            .parse()
            .unwrap_or(DEFAULT_FLUSH_THRESHOLD_KB);

        let wal_sync: bool = env_or("RIPTIDE_WAL_SYNC", &DEFAULT_WAL_SYNC.to_string())
            .parse()
            .unwrap_or(DEFAULT_WAL_SYNC);

        let l0_trigger: usize = env_or(
            "RIPTIDE_L0_TRIGGER",
            &DEFAULT_L0_COMPACTION_TRIGGER.to_string(),
        )
        .parse()
        .unwrap_or(DEFAULT_L0_COMPACTION_TRIGGER);

        let server_host = env_or("RIPTIDE_HOST", DEFAULT_SERVER_HOST);

        let server_port: u16 = env_or("RIPTIDE_PORT", &DEFAULT_SERVER_PORT.to_string())
            .parse()
            .unwrap_or(defaults.server_port);

        Self {
            wal_path: PathBuf::from(wal_path),
            sst_dir: PathBuf::from(sst_dir),
            flush_threshold_bytes: flush_kb * 1024,
            wal_sync,
            l0_compaction_trigger: l0_trigger,
            server_host,
            server_port,
        }
    }

    /// Returns a [`ConfigBuilder`] for ergonomic construction in tests and
    /// programmatic usage.
    ///
    /// All fields start with their default values. Override only what you need.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder {
            config: Self::default(),
        }
    }

    /// Returns the `host:port` address string for the RESP server.
    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.server_host, self.server_port)
    }
}

// ─── ConfigBuilder ──────────────────────────────────────────────────────────

/// Builder for [`EngineConfig`] with a fluent API.
///
/// All fields start with their default values. Call setters to override, then
/// call [`build()`](ConfigBuilder::build) to produce the final config.
///
/// # Example
///
/// ```rust
/// use config::EngineConfig;
///
/// let cfg = EngineConfig::builder()
///     .wal_path("/tmp/wal.log")
///     .sst_dir("/tmp/sst")
///     .flush_threshold_bytes(256)
///     .wal_sync(false)
///     .build();
///
/// assert_eq!(cfg.flush_threshold_bytes, 256);
/// assert!(!cfg.wal_sync);
/// ```
pub struct ConfigBuilder {
    config: EngineConfig,
}

impl ConfigBuilder {
    /// Sets the WAL file path.
    pub fn wal_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.wal_path = path.into();
        self
    }

    /// Sets the SSTable directory.
    pub fn sst_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.sst_dir = dir.into();
        self
    }

    /// Sets the flush threshold in bytes (not KiB).
    ///
    /// This is the raw byte value. For KiB, multiply by 1024 before passing.
    pub fn flush_threshold_bytes(mut self, bytes: usize) -> Self {
        self.config.flush_threshold_bytes = bytes;
        self
    }

    /// Sets the WAL sync mode.
    pub fn wal_sync(mut self, sync: bool) -> Self {
        self.config.wal_sync = sync;
        self
    }

    /// Sets the L0 compaction trigger. Pass `0` to disable auto-compaction.
    pub fn l0_compaction_trigger(mut self, trigger: usize) -> Self {
        self.config.l0_compaction_trigger = trigger;
        self
    }

    /// Sets the RESP server host address.
    pub fn server_host(mut self, host: impl Into<String>) -> Self {
        self.config.server_host = host.into();
        self
    }

    /// Sets the RESP server port.
    pub fn server_port(mut self, port: u16) -> Self {
        self.config.server_port = port;
        self
    }

    /// Consumes the builder and returns the final [`EngineConfig`].
    pub fn build(self) -> EngineConfig {
        self.config
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Reads an environment variable, falling back to `default` if not set.
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests;
