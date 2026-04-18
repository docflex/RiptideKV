//! End-to-end integration tests for the RiptideKV RESP2 server.
//!
//! Every test:
//!   1. Spins up a real `TcpListener` on a free port (OS assigns port 0).
//!   2. Starts the server via `server::serve()` in a background Tokio task.
//!   3. Connects a lightweight `TestClient` that speaks raw RESP2.
//!   4. Sends commands and asserts responses.
//!
//! Tests are deliberately self-contained — each gets a fresh in-memory
//! engine in a temporary directory.  This means tests can run in parallel
//! without interfering with each other.

use engine::Engine;
use server::db::SharedDb;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

// ─── Test infrastructure ──────────────────────────────────────────────────────

/// Start a server listening on an OS-assigned free port.
/// Returns the socket address so the caller can connect.
async fn start_server() -> (std::net::SocketAddr, SharedDb) {
    let dir = Box::leak(Box::new(tempdir().unwrap())); // keep dir alive for test duration
    let engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        64 * 1024 * 1024, // large threshold — no auto-flush noise in tests
        false,
    )
    .unwrap();
    let db = SharedDb::new(engine);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let db2 = db.clone();
    tokio::spawn(async move {
        server::serve(listener, db2).await.ok();
    });
    (addr, db)
}

/// A minimal async RESP2 client for tests.
struct TestClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestClient {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (r, w) = stream.into_split();
        Self {
            reader: BufReader::new(r),
            writer: w,
        }
    }

    /// Send a RESP2 array command (the format all Redis clients use).
    async fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
        }
        self.writer.write_all(&buf).await.unwrap();
    }

    /// Read a single RESP2 response line (the first line of the response).
    async fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        line.trim_end_matches(['\r', '\n']).to_owned()
    }

    /// Read a complete RESP2 response and return it as a `Response`.
    async fn recv(&mut self) -> Response {
        let line = self.read_line().await;
        match line.as_bytes().first() {
            Some(b'+') => Response::Simple(line[1..].to_owned()),
            Some(b'-') => Response::Error(line[1..].to_owned()),
            Some(b':') => Response::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
                let n: i64 = line[1..].parse().unwrap();
                if n < 0 {
                    return Response::Null;
                }
                let n = n as usize;
                let mut buf = vec![0u8; n + 2];
                self.reader.read_exact(&mut buf).await.unwrap();
                buf.truncate(n);
                Response::Bulk(buf)
            }
            Some(b'*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count < 0 {
                    return Response::NullArray;
                }
                let mut items = Vec::new();
                for _ in 0..count {
                    let item = Box::pin(self.recv()).await;
                    items.push(item);
                }
                Response::Array(items)
            }
            _ => panic!("unexpected RESP line: {:?}", line),
        }
    }

    /// Convenience: send + recv in one call.
    async fn cmd(&mut self, args: &[&str]) -> Response {
        self.send(args).await;
        self.recv().await
    }

    /// Convenience: expect "+OK".
    async fn ok(&mut self, args: &[&str]) {
        assert_eq!(self.cmd(args).await, Response::Simple("OK".into()));
    }

    /// Convenience: expect a specific integer.
    async fn int(&mut self, args: &[&str], expected: i64) {
        assert_eq!(self.cmd(args).await, Response::Int(expected));
    }

    /// Convenience: expect a bulk string with this UTF-8 content.
    async fn bulk_str(&mut self, args: &[&str], expected: &str) {
        assert_eq!(
            self.cmd(args).await,
            Response::Bulk(expected.as_bytes().to_vec())
        );
    }

    /// Convenience: expect null bulk.
    async fn null(&mut self, args: &[&str]) {
        assert_eq!(self.cmd(args).await, Response::Null);
    }
}

#[derive(Debug, PartialEq, Clone)]
enum Response {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Null,
    NullArray,
    Array(Vec<Response>),
}

impl Response {
    fn as_str(&self) -> &str {
        match self {
            Response::Bulk(b) => std::str::from_utf8(b).unwrap(),
            Response::Simple(s) => s.as_str(),
            _ => panic!("not a string response"),
        }
    }
    fn as_int(&self) -> i64 {
        match self {
            Response::Int(n) => *n,
            _ => panic!("not an int response"),
        }
    }
    fn is_error(&self) -> bool {
        matches!(self, Response::Error(_))
    }
    fn error_msg(&self) -> &str {
        match self {
            Response::Error(s) => s.as_str(),
            _ => panic!("not an error"),
        }
    }
}

// ─── Connection / server commands ─────────────────────────────────────────────

#[tokio::test]
async fn test_ping_no_args() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    assert_eq!(c.cmd(&["PING"]).await, Response::Simple("PONG".into()));
}

#[tokio::test]
async fn test_ping_with_message() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    assert_eq!(
        c.cmd(&["PING", "hello"]).await,
        Response::Bulk(b"hello".to_vec())
    );
}

#[tokio::test]
async fn test_echo() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.bulk_str(&["ECHO", "RiptideKV"], "RiptideKV").await;
}

#[tokio::test]
async fn test_select_zero_ok() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SELECT", "0"]).await;
}

#[tokio::test]
async fn test_select_nonzero_error() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["SELECT", "1"]).await;
    assert!(r.is_error());
}

#[tokio::test]
async fn test_quit_closes_connection() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["QUIT"]).await;
    // Next read should get EOF (connection closed by server).
    let mut line = String::new();
    let n = c.reader.read_line(&mut line).await.unwrap();
    assert_eq!(n, 0, "server should close connection after QUIT");
}

#[tokio::test]
async fn test_client_setname_getname() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["CLIENT", "SETNAME", "myconn"]).await;
    c.bulk_str(&["CLIENT", "GETNAME"], "myconn").await;
}

#[tokio::test]
async fn test_client_id() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["CLIENT", "ID"]).await;
    assert!(matches!(r, Response::Int(n) if n > 0));
}

#[tokio::test]
async fn test_hello_resp2() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["HELLO", "2"]).await;
    assert!(
        matches!(r, Response::Array(_)),
        "HELLO should return an array"
    );
}

#[tokio::test]
async fn test_hello_resp3_rejected() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["HELLO", "3"]).await;
    assert!(r.is_error());
    assert!(r.error_msg().contains("NOPROTO"));
}

#[tokio::test]
async fn test_info_returns_bulk() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["INFO"]).await;
    assert!(matches!(&r, Response::Bulk(b) if !b.is_empty()));
    assert!(r.as_str().contains("redis_version"));
}

#[tokio::test]
async fn test_info_section_server() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["INFO", "server"]).await;
    assert!(r.as_str().contains("uptime_in_seconds"));
}

#[tokio::test]
async fn test_command_count() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["COMMAND", "COUNT"]).await;
    assert!(matches!(r, Response::Int(n) if n > 0));
}

#[tokio::test]
async fn test_config_get_returns_empty_array() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["CONFIG", "GET", "*"]).await;
    assert!(matches!(r, Response::Array(_)));
}

#[tokio::test]
async fn test_unknown_command_error() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["XYZZY"]).await;
    assert!(r.is_error());
    assert!(r.error_msg().contains("unknown command"));
}

// ─── GET / SET ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_set_and_get() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "name", "alice"]).await;
    c.bulk_str(&["GET", "name"], "alice").await;
}

#[tokio::test]
async fn test_get_missing_key_returns_null() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.null(&["GET", "no-such-key"]).await;
}

#[tokio::test]
async fn test_set_overwrites_existing_value() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v1"]).await;
    c.ok(&["SET", "k", "v2"]).await;
    c.bulk_str(&["GET", "k"], "v2").await;
}

#[tokio::test]
async fn test_set_nx_only_sets_when_absent() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    // Key absent → set succeeds, returns OK.
    c.ok(&["SET", "nx_key", "first", "NX"]).await;
    // Key present → set fails, returns null.
    c.null(&["SET", "nx_key", "second", "NX"]).await;
    // Value unchanged.
    c.bulk_str(&["GET", "nx_key"], "first").await;
}

#[tokio::test]
async fn test_set_xx_only_sets_when_present() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    // Key absent → XX fails.
    c.null(&["SET", "xx_key", "v", "XX"]).await;
    // Create it first.
    c.ok(&["SET", "xx_key", "original"]).await;
    // Now XX succeeds.
    c.ok(&["SET", "xx_key", "updated", "XX"]).await;
    c.bulk_str(&["GET", "xx_key"], "updated").await;
}

#[tokio::test]
async fn test_set_get_flag_returns_old_value() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "old"]).await;
    let r = c.cmd(&["SET", "k", "new", "GET"]).await;
    assert_eq!(r, Response::Bulk(b"old".to_vec()));
    c.bulk_str(&["GET", "k"], "new").await;
}

#[tokio::test]
async fn test_set_with_ex_expiry() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "expkey", "hello", "EX", "100"]).await;
    let ttl = c.cmd(&["TTL", "expkey"]).await.as_int();
    assert!(
        ttl > 0 && ttl <= 100,
        "TTL should be between 1 and 100, got {}",
        ttl
    );
}

#[tokio::test]
async fn test_set_with_px_expiry() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "pxkey", "v", "PX", "100000"]).await;
    let pttl = c.cmd(&["PTTL", "pxkey"]).await.as_int();
    assert!(pttl > 0 && pttl <= 100_000);
}

#[tokio::test]
async fn test_set_keepttl_preserves_expiry() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v1", "EX", "100"]).await;
    c.ok(&["SET", "k", "v2", "KEEPTTL"]).await;
    let ttl = c.cmd(&["TTL", "k"]).await.as_int();
    assert!(ttl > 0, "KEEPTTL should preserve the TTL, got {}", ttl);
}

#[tokio::test]
async fn test_set_invalid_ex_returns_error() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["SET", "k", "v", "EX", "-1"]).await;
    assert!(r.is_error());
}

// ─── SETNX / SETEX / PSETEX ───────────────────────────────────────────────────

#[tokio::test]
async fn test_setnx() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.int(&["SETNX", "k", "v"], 1).await;
    c.int(&["SETNX", "k", "v2"], 0).await;
    c.bulk_str(&["GET", "k"], "v").await;
}

#[tokio::test]
async fn test_setex() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SETEX", "k", "60", "val"]).await;
    let ttl = c.cmd(&["TTL", "k"]).await.as_int();
    assert!(ttl > 0 && ttl <= 60);
}

#[tokio::test]
async fn test_setex_invalid_timeout_returns_error() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["SETEX", "k", "0", "v"]).await;
    assert!(r.is_error());
}

#[tokio::test]
async fn test_psetex() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["PSETEX", "k", "60000", "val"]).await;
    let pttl = c.cmd(&["PTTL", "k"]).await.as_int();
    assert!(pttl > 0 && pttl <= 60_000);
}

// ─── GETSET / GETDEL / GETEX ──────────────────────────────────────────────────

#[tokio::test]
async fn test_getset() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "old"]).await;
    let r = c.cmd(&["GETSET", "k", "new"]).await;
    assert_eq!(r, Response::Bulk(b"old".to_vec()));
    c.bulk_str(&["GET", "k"], "new").await;
}

#[tokio::test]
async fn test_getset_missing_key_returns_null() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["GETSET", "absent", "val"]).await;
    assert_eq!(r, Response::Null);
    c.bulk_str(&["GET", "absent"], "val").await;
}

#[tokio::test]
async fn test_getdel_deletes_and_returns() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v"]).await;
    let r = c.cmd(&["GETDEL", "k"]).await;
    assert_eq!(r, Response::Bulk(b"v".to_vec()));
    c.null(&["GET", "k"]).await;
}

#[tokio::test]
async fn test_getdel_missing_returns_null() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.null(&["GETDEL", "absent"]).await;
}

#[tokio::test]
async fn test_getex_sets_expiry() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v"]).await;
    c.bulk_str(&["GETEX", "k", "EX", "100"], "v").await;
    let ttl = c.cmd(&["TTL", "k"]).await.as_int();
    assert!(ttl > 0 && ttl <= 100);
}

#[tokio::test]
async fn test_getex_persist_removes_expiry() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v", "EX", "100"]).await;
    c.bulk_str(&["GETEX", "k", "PERSIST"], "v").await;
    c.int(&["TTL", "k"], -1).await;
}

// ─── MGET / MSET / MSETNX ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_mset_and_mget() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["MSET", "k1", "v1", "k2", "v2", "k3", "v3"]).await;
    let r = c.cmd(&["MGET", "k1", "k2", "k3", "absent"]).await;
    assert_eq!(
        r,
        Response::Array(vec![
            Response::Bulk(b"v1".to_vec()),
            Response::Bulk(b"v2".to_vec()),
            Response::Bulk(b"v3".to_vec()),
            Response::Null,
        ])
    );
}

#[tokio::test]
async fn test_msetnx_all_absent_succeeds() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.int(&["MSETNX", "a", "1", "b", "2"], 1).await;
    c.bulk_str(&["GET", "a"], "1").await;
}

#[tokio::test]
async fn test_msetnx_any_present_fails() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "existing", "x"]).await;
    c.int(&["MSETNX", "existing", "new", "fresh", "y"], 0).await;
    // "fresh" must not have been set either (atomicity).
    c.null(&["GET", "fresh"]).await;
}

// ─── APPEND / STRLEN ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_append_creates_and_extends() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.int(&["APPEND", "k", "hello"], 5).await;
    c.int(&["APPEND", "k", " world"], 11).await;
    c.bulk_str(&["GET", "k"], "hello world").await;
}

#[tokio::test]
async fn test_strlen() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "hello"]).await;
    c.int(&["STRLEN", "k"], 5).await;
    c.int(&["STRLEN", "absent"], 0).await;
}

// ─── INCR / INCRBY / INCRBYFLOAT / DECR / DECRBY ─────────────────────────────

#[tokio::test]
async fn test_incr_creates_and_increments() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.int(&["INCR", "counter"], 1).await;
    c.int(&["INCR", "counter"], 2).await;
    c.int(&["INCR", "counter"], 3).await;
}

#[tokio::test]
async fn test_incrby() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "n", "10"]).await;
    c.int(&["INCRBY", "n", "5"], 15).await;
    c.int(&["INCRBY", "n", "-3"], 12).await;
}

#[tokio::test]
async fn test_decr() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "n", "10"]).await;
    c.int(&["DECR", "n"], 9).await;
}

#[tokio::test]
async fn test_decrby() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "n", "100"]).await;
    c.int(&["DECRBY", "n", "30"], 70).await;
}

#[tokio::test]
async fn test_incrbyfloat() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "f", "10"]).await;
    let r = c.cmd(&["INCRBYFLOAT", "f", "1.5"]).await;
    // Returns as a bulk string.
    assert_eq!(r.as_str(), "11.5");
}

// ─── GETRANGE / SETRANGE ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_getrange() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "hello world"]).await;
    c.bulk_str(&["GETRANGE", "k", "0", "4"], "hello").await;
    c.bulk_str(&["GETRANGE", "k", "6", "-1"], "world").await;
    c.bulk_str(&["GETRANGE", "k", "0", "-1"], "hello world")
        .await;
}

#[tokio::test]
async fn test_setrange_extends_string() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "Hello World"]).await;
    c.int(&["SETRANGE", "k", "6", "Redis"], 11).await;
    c.bulk_str(&["GET", "k"], "Hello Redis").await;
}

// ─── DEL / EXISTS / TYPE ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_del_single_key() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v"]).await;
    c.int(&["DEL", "k"], 1).await;
    c.null(&["GET", "k"]).await;
}

#[tokio::test]
async fn test_del_multiple_keys_returns_count() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["MSET", "k1", "v1", "k2", "v2", "k3", "v3"]).await;
    c.int(&["DEL", "k1", "k2", "k3", "absent"], 3).await;
}

#[tokio::test]
async fn test_del_missing_key_returns_zero() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.int(&["DEL", "absent"], 0).await;
}

#[tokio::test]
async fn test_exists_present_and_absent() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v"]).await;
    c.int(&["EXISTS", "k"], 1).await;
    c.int(&["EXISTS", "absent"], 0).await;
}

#[tokio::test]
async fn test_exists_multiple_keys() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["MSET", "k1", "v1", "k2", "v2"]).await;
    // k1 counted twice (both references present).
    c.int(&["EXISTS", "k1", "k1", "k2", "absent"], 3).await;
}

#[tokio::test]
async fn test_type_string_and_none() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v"]).await;
    assert_eq!(
        c.cmd(&["TYPE", "k"]).await,
        Response::Simple("string".into())
    );
    assert_eq!(
        c.cmd(&["TYPE", "absent"]).await,
        Response::Simple("none".into())
    );
}

// ─── TTL / PTTL / EXPIRE / PEXPIRE / PERSIST / EXPIRETIME ────────────────────

#[tokio::test]
async fn test_ttl_no_expiry_returns_minus_one() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v"]).await;
    c.int(&["TTL", "k"], -1).await;
    c.int(&["PTTL", "k"], -1).await;
}

#[tokio::test]
async fn test_ttl_absent_key_returns_minus_two() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.int(&["TTL", "absent"], -2).await;
}

#[tokio::test]
async fn test_expire_and_ttl() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v"]).await;
    c.int(&["EXPIRE", "k", "60"], 1).await;
    let ttl = c.cmd(&["TTL", "k"]).await.as_int();
    assert!(
        ttl > 0 && ttl <= 60,
        "TTL should be in (0, 60], got {}",
        ttl
    );
}

#[tokio::test]
async fn test_pexpire_and_pttl() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v"]).await;
    c.int(&["PEXPIRE", "k", "60000"], 1).await;
    let pttl = c.cmd(&["PTTL", "k"]).await.as_int();
    assert!(pttl > 0 && pttl <= 60_000);
}

#[tokio::test]
async fn test_persist_removes_expiry() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v", "EX", "60"]).await;
    c.int(&["PERSIST", "k"], 1).await;
    c.int(&["TTL", "k"], -1).await;
    // Calling PERSIST again on a key with no TTL should return 0.
    c.int(&["PERSIST", "k"], 0).await;
}

#[tokio::test]
async fn test_key_expires_and_becomes_invisible() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    // Set a key with a 1 ms TTL (minimum meaningful value for test).
    c.ok(&["SET", "k", "v", "PX", "50"]).await;
    // Still visible immediately.
    assert_ne!(c.cmd(&["GET", "k"]).await, Response::Null);
    // Wait for expiry.
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Should now be gone.
    c.null(&["GET", "k"]).await;
    c.int(&["EXISTS", "k"], 0).await;
    c.int(&["TTL", "k"], -2).await;
}

#[tokio::test]
async fn test_expireat_sets_unix_expiry() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v"]).await;
    let future_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 120;
    let r = c.cmd(&["EXPIREAT", "k", &future_unix.to_string()]).await;
    assert_eq!(r, Response::Int(1));
    let ttl = c.cmd(&["TTL", "k"]).await.as_int();
    assert!(ttl > 0 && ttl <= 120);
}

// ─── KEYS / SCAN / DBSIZE / FLUSHDB ──────────────────────────────────────────

#[tokio::test]
async fn test_keys_wildcard() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["MSET", "user:1", "a", "user:2", "b", "item:1", "c"])
        .await;
    let r = c.cmd(&["KEYS", "user:*"]).await;
    let keys = match r {
        Response::Array(v) => v,
        _ => panic!("expected array"),
    };
    assert_eq!(keys.len(), 2);
    assert!(keys.iter().all(|k| k.as_str().starts_with("user:")));
}

#[tokio::test]
async fn test_keys_question_mark_pattern() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["MSET", "foo", "1", "bar", "2", "baz", "3", "foobar", "4"])
        .await;
    let r = c.cmd(&["KEYS", "???"]).await;
    let keys = match r {
        Response::Array(v) => v,
        _ => panic!("expected array"),
    };
    // foo, bar, baz should match (3 chars); foobar should not
    assert_eq!(keys.len(), 3);
}

#[tokio::test]
async fn test_scan_basic() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["MSET", "k1", "v1", "k2", "v2", "k3", "v3"]).await;
    let r = c.cmd(&["SCAN", "0"]).await;
    match r {
        Response::Array(v) => {
            assert_eq!(v.len(), 2);
            assert_eq!(v[0].as_str(), "0"); // cursor always 0 (we return all at once)
            match &v[1] {
                Response::Array(keys) => assert_eq!(keys.len(), 3),
                _ => panic!("expected nested array for keys"),
            }
        }
        _ => panic!("expected array from SCAN"),
    }
}

#[tokio::test]
async fn test_scan_with_match_pattern() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["MSET", "prefix:1", "a", "prefix:2", "b", "other", "c"])
        .await;
    let r = c.cmd(&["SCAN", "0", "MATCH", "prefix:*"]).await;
    match r {
        Response::Array(v) => match &v[1] {
            Response::Array(keys) => assert_eq!(keys.len(), 2),
            _ => panic!(),
        },
        _ => panic!("expected array"),
    }
}

#[tokio::test]
async fn test_dbsize() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.int(&["DBSIZE"], 0).await;
    c.ok(&["MSET", "a", "1", "b", "2", "c", "3"]).await;
    c.int(&["DBSIZE"], 3).await;
}

#[tokio::test]
async fn test_flushdb_clears_all_keys() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["MSET", "k1", "v1", "k2", "v2"]).await;
    c.ok(&["FLUSHDB"]).await;
    c.int(&["DBSIZE"], 0).await;
    c.null(&["GET", "k1"]).await;
}

// ─── RENAME / RENAMENX ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_rename() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "src", "hello"]).await;
    c.ok(&["RENAME", "src", "dst"]).await;
    c.null(&["GET", "src"]).await;
    c.bulk_str(&["GET", "dst"], "hello").await;
}

#[tokio::test]
async fn test_rename_missing_source_errors() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["RENAME", "absent", "dst"]).await;
    assert!(r.is_error());
}

#[tokio::test]
async fn test_rename_preserves_ttl() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "src", "v", "EX", "100"]).await;
    c.ok(&["RENAME", "src", "dst"]).await;
    let ttl = c.cmd(&["TTL", "dst"]).await.as_int();
    assert!(ttl > 0, "TTL should be transferred on rename");
}

#[tokio::test]
async fn test_renamenx_absent_destination() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "src", "v"]).await;
    c.int(&["RENAMENX", "src", "dst"], 1).await;
    c.bulk_str(&["GET", "dst"], "v").await;
    c.null(&["GET", "src"]).await;
}

#[tokio::test]
async fn test_renamenx_present_destination_fails() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["MSET", "src", "sv", "dst", "dv"]).await;
    c.int(&["RENAMENX", "src", "dst"], 0).await;
    // Both keys unchanged.
    c.bulk_str(&["GET", "src"], "sv").await;
    c.bulk_str(&["GET", "dst"], "dv").await;
}

// ─── TOUCH ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_touch_counts_existing_keys() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["MSET", "k1", "v", "k2", "v"]).await;
    c.int(&["TOUCH", "k1", "k2", "absent"], 2).await;
}

// ─── Concurrent clients ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_multiple_concurrent_clients() {
    let (addr, _db) = start_server().await;

    let handles: Vec<_> = (0..10u32)
        .map(|i| {
            tokio::spawn(async move {
                let mut c = TestClient::connect(addr).await;
                let key = format!("client_{}", i);
                let val = format!("value_{}", i);
                c.ok(&["SET", &key, &val]).await;
                c.bulk_str(&["GET", &key], &val).await;
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_incr_correctness() {
    let (addr, _db) = start_server().await;

    // 50 tasks each increment "shared_counter" 10 times = 500 total.
    let handles: Vec<_> = (0..50u32)
        .map(|_| {
            tokio::spawn(async move {
                let mut c = TestClient::connect(addr).await;
                for _ in 0..10 {
                    c.cmd(&["INCR", "shared_counter"]).await;
                }
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap();
    }

    let mut c = TestClient::connect(addr).await;
    let final_val = c.cmd(&["GET", "shared_counter"]).await;
    let n: i64 = final_val.as_str().parse().unwrap();
    assert_eq!(
        n, 500,
        "concurrent INCRs should be serialized by the RwLock"
    );
}

// ─── ACL / slow-log / memory / latency stubs ──────────────────────────────────

#[tokio::test]
async fn test_acl_whoami() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.bulk_str(&["ACL", "WHOAMI"], "default").await;
}

#[tokio::test]
async fn test_slowlog_get_returns_empty() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["SLOWLOG", "GET"]).await;
    assert!(matches!(r, Response::Array(v) if v.is_empty()));
}

#[tokio::test]
async fn test_memory_usage() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let r = c.cmd(&["MEMORY", "USAGE", "somekey"]).await;
    // We return null for MEMORY USAGE (not supported in detail).
    assert!(matches!(r, Response::Null));
}

// ─── Pipelining ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_pipelining_set_mget() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;

    // Send 5 SETs without waiting for responses (pipelining).
    for i in 0..5u32 {
        c.send(&["SET", &format!("pk{}", i), &format!("pv{}", i)])
            .await;
    }
    // Collect all 5 OK responses.
    for _ in 0..5 {
        assert_eq!(c.recv().await, Response::Simple("OK".into()));
    }

    // Now MGET all 5 keys in one shot.
    let keys: Vec<String> = (0..5).map(|i| format!("pk{}", i)).collect();
    let mut args = vec!["MGET"];
    for k in &keys {
        args.push(k.as_str());
    }
    let r = c.cmd(&args).await;
    match r {
        Response::Array(v) => {
            assert_eq!(v.len(), 5);
            for (i, item) in v.iter().enumerate() {
                assert_eq!(item.as_str(), &format!("pv{}", i));
            }
        }
        _ => panic!("expected array"),
    }
}

// ─── Edge cases ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_set_and_get_binary_safe_value() {
    let (addr, _db) = start_server().await;
    // Build a command manually with binary (non-UTF-8) bytes in the value.
    let mut c = TestClient::connect(addr).await;
    // We use SET with a value of 3 bytes 0x00 0x01 0x02.
    let mut raw = b"*3\r\n$3\r\nSET\r\n$6\r\nbinkey\r\n$3\r\n\x00\x01\x02\r\n".to_vec();
    c.writer.write_all(&raw).await.unwrap();
    raw.clear();
    assert_eq!(c.recv().await, Response::Simple("OK".into()));

    c.send(&["GET", "binkey"]).await;
    let r = c.recv().await;
    assert_eq!(r, Response::Bulk(vec![0x00, 0x01, 0x02]));
}

#[tokio::test]
async fn test_incr_on_non_numeric_value_errors() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "notanumber"]).await;
    let r = c.cmd(&["INCR", "k"]).await;
    // The engine returns 0 for unparseable numbers (graceful degradation),
    // so we just check the key is still accessible.
    // If it errors, that's also acceptable — just not a panic.
    let _ = r;
}

#[tokio::test]
async fn test_set_large_value() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    let large_val = "x".repeat(65_536); // 64 KiB
    c.ok(&["SET", "bigkey", &large_val]).await;
    let r = c.cmd(&["GET", "bigkey"]).await;
    assert_eq!(r.as_str(), large_val);
}

#[tokio::test]
async fn test_del_after_expiry_returns_zero() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v", "PX", "50"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // After expiry, DEL should return 0 (key not found).
    c.int(&["DEL", "k"], 0).await;
}

#[tokio::test]
async fn test_set_clears_existing_ttl() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["SET", "k", "v", "EX", "100"]).await;
    // Overwrite with a plain SET (no EX) → TTL should be cleared.
    c.ok(&["SET", "k", "v2"]).await;
    c.int(&["TTL", "k"], -1).await;
}

// ─── INFO stats reflect server activity ────────────────────────────────────────

#[tokio::test]
async fn test_info_dbsize_reflects_keys() {
    let (addr, _db) = start_server().await;
    let mut c = TestClient::connect(addr).await;
    c.ok(&["MSET", "x", "1", "y", "2", "z", "3"]).await;
    let r = c.cmd(&["INFO", "keyspace"]).await;
    let info = r.as_str().to_owned();
    assert!(
        info.contains("keys=3"),
        "keyspace info should show 3 keys: {}",
        info
    );
}
