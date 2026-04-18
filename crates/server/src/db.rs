//! Shared database state: Engine + volatile TTL map + server statistics.

use engine::Engine;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

// ─── Inner state ─────────────────────────────────────────────────────────────

pub struct DbState {
    pub engine: Engine,
    /// Volatile expiry times (lost on restart; graceful degradation: keys survive, just un-expire).
    pub ttl: HashMap<Vec<u8>, Instant>,
}

impl DbState {
    /// Returns `true` if key has an expiry and it has passed.
    pub fn is_expired(&self, key: &[u8]) -> bool {
        self.ttl
            .get(key)
            .map(|t| Instant::now() >= *t)
            .unwrap_or(false)
    }

    /// Lazily evict an expired key. Returns `true` if the key was evicted.
    pub fn evict_if_expired(&mut self, key: &[u8]) -> anyhow::Result<bool> {
        if self.is_expired(key) {
            self.engine.del(key.to_vec())?;
            self.ttl.remove(key);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Set (or overwrite) a TTL.
    pub fn set_expiry(&mut self, key: &[u8], from_now: Duration) {
        self.ttl.insert(key.to_vec(), Instant::now() + from_now);
    }

    /// Remove any TTL for this key.
    pub fn clear_expiry(&mut self, key: &[u8]) {
        self.ttl.remove(key);
    }

    /// Remaining TTL in milliseconds, or `None` if no expiry, or `Some(-2)` if expired.
    pub fn ttl_ms(&self, key: &[u8]) -> Option<i64> {
        let deadline = self.ttl.get(key)?;
        let now = Instant::now();
        if now >= *deadline {
            Some(-2)
        } else {
            Some(deadline.duration_since(now).as_millis() as i64)
        }
    }
}

// ─── Shared wrapper ──────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SharedDb {
    pub state: Arc<RwLock<DbState>>,
    pub start_time: Instant,
    pub connected_clients: Arc<AtomicI64>,
    pub total_commands: Arc<AtomicU64>,
    pub total_connections: Arc<AtomicU64>,
}

impl SharedDb {
    pub fn new(engine: Engine) -> Self {
        Self {
            state: Arc::new(RwLock::new(DbState {
                engine,
                ttl: HashMap::new(),
            })),
            start_time: Instant::now(),
            connected_clients: Arc::new(AtomicI64::new(0)),
            total_commands: Arc::new(AtomicU64::new(0)),
            total_connections: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn client_connected(&self) {
        self.connected_clients.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn client_disconnected(&self) {
        self.connected_clients.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_commands(&self) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
    }

    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn num_clients(&self) -> i64 {
        self.connected_clients.load(Ordering::Relaxed)
    }

    pub fn total_commands_processed(&self) -> u64 {
        self.total_commands.load(Ordering::Relaxed)
    }
}
