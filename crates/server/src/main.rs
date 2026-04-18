//! RiptideKV — RESP2 server entry point.
//!
//! This is a thin configuration wrapper around `server::serve()`.
//!
//! Configuration via environment variables:
//!   RIPTIDE_BIND       bind address (default 0.0.0.0:6379)
//!   RIPTIDE_WAL_PATH   WAL file path (default wal.log)
//!   RIPTIDE_SST_DIR    SSTable directory (default data/sst)
//!   RIPTIDE_FLUSH_KB   memtable flush threshold KiB (default 1024)
//!   RIPTIDE_WAL_SYNC   fsync after every WAL write (default true)

use engine::Engine;
use server::db::SharedDb;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("server=info".parse()?))
        .init();

    let bind = std::env::var("RIPTIDE_BIND").unwrap_or_else(|_| "0.0.0.0:6379".into());
    let wal_path = std::env::var("RIPTIDE_WAL_PATH").unwrap_or_else(|_| "wal.log".into());
    let sst_dir = std::env::var("RIPTIDE_SST_DIR").unwrap_or_else(|_| "data/sst".into());
    let flush_kb: usize = std::env::var("RIPTIDE_FLUSH_KB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024);
    let wal_sync: bool = std::env::var("RIPTIDE_WAL_SYNC")
        .ok()
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);

    std::fs::create_dir_all(&sst_dir)?;

    let engine = Engine::new(&wal_path, &sst_dir, flush_kb * 1024, wal_sync)?;

    let db = SharedDb::new(engine);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    info!("RiptideKV listening on {bind}");

    tokio::select! {
        res = server::serve(listener, db) => {
            if let Err(e) = res { error!("server error: {e}"); }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down (SIGINT)");
        }
        _ = sigterm() => {
            info!("shutting down (SIGTERM)");
        }
    }

    Ok(())
}

/// Resolves when SIGTERM is received on Unix; never resolves on other platforms.
#[cfg(unix)]
async fn sigterm() {
    use tokio::signal::unix::{signal, SignalKind};
    signal(SignalKind::terminate())
        .expect("failed to install SIGTERM handler")
        .recv()
        .await;
}

#[cfg(not(unix))]
async fn sigterm() {
    std::future::pending::<()>().await;
}
