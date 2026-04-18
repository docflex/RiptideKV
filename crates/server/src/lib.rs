//! RiptideKV server library.
//!
//! The library interface makes the server independently testable without
//! launching a subprocess. Tests can call [`serve`] with a pre-bound listener.
//!
//! # Public surface
//!
//! - [`serve`]  — accept loop (cancellation via dropping the listener)
//! - [`db`]     — shared database state (`SharedDb`, `DbState`)
//! - [`resp`]   — RESP2 parser / serializer helpers
//! - [`handler`] — per-connection dispatcher (all Redis commands)

pub mod db;
pub mod handler;
pub mod resp;

use db::SharedDb;
use tokio::net::TcpListener;
use tracing::error;

/// Run the accept loop on `listener` until it is dropped or returns an error.
///
/// Each accepted connection is handled in its own Tokio task. The loop
/// continues until `listener.accept()` fails (e.g. the listener is closed).
pub async fn serve(listener: TcpListener, db: SharedDb) -> anyhow::Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                stream.set_nodelay(true)?;
                let db = db.clone();
                tokio::spawn(async move {
                    handler::handle_connection(stream, db).await;
                });
            }
            Err(e) => {
                error!("accept error: {e}");
                return Err(e.into());
            }
        }
    }
}
