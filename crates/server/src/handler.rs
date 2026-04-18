//! Per-connection command dispatcher.
//!
//! Design notes:
//! - `engine.set / del` take *owned* `Vec<u8>`; always `.clone()` or move.
//! - `engine.get` returns `Option<(seq, Vec<u8>)>`; use `.map(|(_, v)| v)`.
//! - Lock guards borrow `db.state` (Arc); clone the Arc before awaiting so
//!   `conn` is free for mutable calls after the guard is dropped.

use crate::db::SharedDb;
use crate::resp::{
    encode_array, encode_bulk, encode_error, encode_int, encode_null_array, encode_simple, ok,
    RespReader, RespValue,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tracing::{debug, warn};

// ─── Connection state ────────────────────────────────────────────────────────

struct Conn {
    db: SharedDb,
    writer: OwnedWriteHalf,
    name: String,
    id: u64,
    db_index: u32,
}

impl Conn {
    async fn send(&mut self, data: Vec<u8>) -> anyhow::Result<()> {
        self.writer.write_all(&data).await?;
        Ok(())
    }
    async fn ok(&mut self) -> anyhow::Result<()> {
        self.send(ok()).await
    }
    async fn err(&mut self, msg: &str) -> anyhow::Result<()> {
        self.send(encode_error(msg)).await
    }
    async fn int(&mut self, n: i64) -> anyhow::Result<()> {
        self.send(encode_int(n)).await
    }
    async fn bulk(&mut self, data: Option<&[u8]>) -> anyhow::Result<()> {
        self.send(encode_bulk(data)).await
    }
}

// ─── Entry point ─────────────────────────────────────────────────────────────

static CONN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub async fn handle_connection(stream: TcpStream, db: SharedDb) {
    let id = CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let peer = stream.peer_addr().ok();
    debug!(?peer, id, "client connected");
    db.client_connected();

    let (read_half, write_half) = stream.into_split();
    let mut reader = RespReader::new(read_half);
    let mut conn = Conn {
        db: db.clone(),
        writer: write_half,
        name: String::new(),
        id,
        db_index: 0,
    };

    loop {
        let value = match reader.read_value().await {
            Ok(Some(v)) => v,
            Ok(None) => break,
            Err(e) => {
                warn!(?peer, id, "parse error: {e}");
                break;
            }
        };
        db.inc_commands();

        let args = match value {
            RespValue::Array(Some(arr)) => arr,
            _ => {
                let _ = conn.err("ERR protocol error: expected array").await;
                break;
            }
        };
        if args.is_empty() {
            let _ = conn.err("ERR empty command").await;
            continue;
        }

        let cmd = match args[0].as_str() {
            Some(s) => s.to_ascii_uppercase(),
            None => {
                let _ = conn.err("ERR command must be a string").await;
                continue;
            }
        };

        match dispatch(&mut conn, &cmd, &args[1..]).await {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => {
                warn!(id, "error in {cmd}: {e}");
                let _ = conn.err(&format!("ERR internal: {e}")).await;
            }
        }
    }

    debug!(?peer, id, "client disconnected");
    db.client_disconnected();
}

// ─── Dispatcher ──────────────────────────────────────────────────────────────

async fn dispatch(conn: &mut Conn, cmd: &str, args: &[RespValue]) -> anyhow::Result<bool> {
    match cmd {
        "PING" => cmd_ping(conn, args).await?,
        "ECHO" => cmd_echo(conn, args).await?,
        "SELECT" => cmd_select(conn, args).await?,
        "QUIT" | "SHUTDOWN" => {
            conn.ok().await?;
            return Ok(false);
        }
        "RESET" => {
            conn.name.clear();
            conn.db_index = 0;
            conn.send(encode_simple("RESET")).await?;
        }
        "HELLO" => cmd_hello(conn, args).await?,
        "CLIENT" => cmd_client(conn, args).await?,
        "CONFIG" => cmd_config(conn, args).await?,
        "INFO" => cmd_info(conn, args).await?,
        "COMMAND" => cmd_command(conn, args).await?,
        "DBSIZE" => cmd_dbsize(conn).await?,
        "FLUSHDB" | "FLUSHALL" => cmd_flushdb(conn).await?,
        "DEBUG" => cmd_debug(conn, args).await?,
        "OBJECT" => cmd_object(conn, args).await?,
        "MEMORY" => cmd_memory(conn, args).await?,
        "SLOWLOG" => cmd_slowlog(conn, args).await?,
        "LATENCY" => cmd_latency(conn, args).await?,
        "ACL" => cmd_acl(conn, args).await?,
        "WAIT" => conn.int(0).await?,
        "LOLWUT" => {
            conn.send(encode_simple("RiptideKV -- made with Rust"))
                .await?
        }
        "FAILOVER" | "REPLICAOF" | "SLAVEOF" => conn.ok().await?,
        "BGSAVE" | "BGREWRITEAOF" | "SAVE" => {
            conn.send(encode_simple("Background saving started"))
                .await?
        }
        "LASTSAVE" => conn.int(0).await?,

        "GET" => cmd_get(conn, args).await?,
        "SET" => cmd_set(conn, args).await?,
        "SETNX" => cmd_setnx(conn, args).await?,
        "SETEX" => cmd_setex(conn, args).await?,
        "PSETEX" => cmd_psetex(conn, args).await?,
        "GETSET" => cmd_getset(conn, args).await?,
        "GETDEL" => cmd_getdel(conn, args).await?,
        "GETEX" => cmd_getex(conn, args).await?,
        "MGET" => cmd_mget(conn, args).await?,
        "MSET" => cmd_mset(conn, args).await?,
        "MSETNX" => cmd_msetnx(conn, args).await?,
        "APPEND" => cmd_append(conn, args).await?,
        "STRLEN" => cmd_strlen(conn, args).await?,
        "INCR" => cmd_incr(conn, args, 1).await?,
        "INCRBY" => cmd_incrby(conn, args).await?,
        "INCRBYFLOAT" => cmd_incrbyfloat(conn, args).await?,
        "DECR" => cmd_incr(conn, args, -1).await?,
        "DECRBY" => cmd_decrby(conn, args).await?,
        "GETRANGE" | "SUBSTR" => cmd_getrange(conn, args).await?,
        "SETRANGE" => cmd_setrange(conn, args).await?,

        "DEL" | "UNLINK" => cmd_del(conn, args).await?,
        "EXISTS" => cmd_exists(conn, args).await?,
        "TYPE" => cmd_type(conn, args).await?,
        "TTL" => cmd_ttl(conn, args, false).await?,
        "PTTL" => cmd_ttl(conn, args, true).await?,
        "EXPIRE" => cmd_expire(conn, args, false).await?,
        "PEXPIRE" => cmd_expire(conn, args, true).await?,
        "EXPIREAT" => cmd_expireat(conn, args, false).await?,
        "PEXPIREAT" => cmd_expireat(conn, args, true).await?,
        "PERSIST" => cmd_persist(conn, args).await?,
        "EXPIRETIME" => cmd_expiretime(conn, args, false).await?,
        "PEXPIRETIME" => cmd_expiretime(conn, args, true).await?,
        "KEYS" => cmd_keys(conn, args).await?,
        "SCAN" => cmd_scan(conn, args).await?,
        "RENAME" => cmd_rename(conn, args).await?,
        "RENAMENX" => cmd_renamenx(conn, args).await?,
        "RANDOMKEY" => cmd_randomkey(conn).await?,
        "TOUCH" => cmd_touch(conn, args).await?,
        "DUMP" => conn.bulk(None).await?,
        "RESTORE" => conn.err("ERR RESTORE not supported").await?,
        "COPY" => conn.err("ERR COPY not supported").await?,
        "MOVE" => conn.err("ERR MOVE not supported (single db)").await?,
        "SORT" | "SORT_RO" => conn.err("ERR SORT not supported").await?,

        _ => {
            conn.err(&format!(
                "ERR unknown command `{}`, with args beginning with: {}",
                cmd,
                args.iter()
                    .filter_map(|a| a.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
            .await?
        }
    }
    Ok(true)
}

// ─── Helper: clone Arc so guard doesn't borrow `conn` ────────────────────────

macro_rules! state_write {
    ($conn:expr) => {
        Arc::clone(&$conn.db.state).write_owned().await
    };
}
macro_rules! state_read {
    ($conn:expr) => {
        Arc::clone(&$conn.db.state).read_owned().await
    };
}

// ─── Server / connection commands ────────────────────────────────────────────

async fn cmd_ping(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.is_empty() {
        conn.send(encode_simple("PONG")).await
    } else {
        conn.bulk(args[0].as_bytes()).await
    }
}

async fn cmd_echo(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 1 {
        return conn
            .err("ERR wrong number of arguments for 'echo' command")
            .await;
    }
    conn.bulk(args[0].as_bytes()).await
}

async fn cmd_select(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let idx: u32 = match args
        .first()
        .and_then(|a| a.as_str())
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => {
            return conn
                .err("ERR value is not an integer or out of range")
                .await
        }
    };
    if idx != 0 {
        return conn.err("ERR DB index is out of range").await;
    }
    conn.db_index = 0;
    conn.ok().await
}

async fn cmd_hello(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let proto = args
        .first()
        .and_then(|a| a.as_str())
        .and_then(|s| s.parse::<u8>().ok());
    if matches!(proto, Some(3)) {
        return conn.err("NOPROTO this server does not support RESP3").await;
    }
    let items: Vec<Vec<u8>> = vec![
        encode_bulk(Some(b"server")),
        encode_bulk(Some(b"RiptideKV")),
        encode_bulk(Some(b"version")),
        encode_bulk(Some(b"7.0.0")),
        encode_bulk(Some(b"proto")),
        encode_int(2),
        encode_bulk(Some(b"id")),
        encode_int(conn.id as i64),
        encode_bulk(Some(b"mode")),
        encode_bulk(Some(b"standalone")),
        encode_bulk(Some(b"role")),
        encode_bulk(Some(b"master")),
        encode_bulk(Some(b"modules")),
        encode_array(&[]),
    ];
    conn.send(encode_array(&items)).await
}

async fn cmd_client(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let sub = args
        .first()
        .and_then(|a| a.as_str())
        .map(|s| s.to_ascii_uppercase());
    match sub.as_deref() {
        Some("SETNAME") => {
            let name = match args.get(1).and_then(|a| a.as_str()) {
                Some(n) => n.to_owned(),
                None => return conn.err("ERR syntax error").await,
            };
            if name.contains(' ') {
                return conn
                    .err("ERR Client names cannot contain spaces, newlines or special characters.")
                    .await;
            }
            conn.name = name;
            conn.ok().await
        }
        Some("GETNAME") => {
            let n = if conn.name.is_empty() {
                None
            } else {
                Some(conn.name.as_bytes().to_vec())
            };
            conn.bulk(n.as_deref()).await
        }
        Some("ID") => conn.int(conn.id as i64).await,
        Some("INFO") | Some("LIST") => {
            let info = format!("id={} name={} db={}\n", conn.id, conn.name, conn.db_index);
            conn.bulk(Some(info.as_bytes())).await
        }
        Some("NO-EVICT") | Some("NO-TOUCH") | Some("REPLY") | Some("PAUSE") | Some("UNPAUSE")
        | Some("KILL") | Some("CACHING") | Some("RESET") => conn.ok().await,
        _ => conn.err("ERR unknown CLIENT subcommand").await,
    }
}

async fn cmd_config(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let sub = args
        .first()
        .and_then(|a| a.as_str())
        .map(|s| s.to_ascii_uppercase());
    match sub.as_deref() {
        Some("GET") => conn.send(encode_array(&[])).await,
        Some("SET") => conn.ok().await,
        Some("RESETSTAT") => conn.ok().await,
        Some("REWRITE") => conn.err("ERR CONFIG REWRITE not supported").await,
        _ => conn.err("ERR unknown CONFIG subcommand").await,
    }
}

async fn cmd_info(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let section = args
        .first()
        .and_then(|a| a.as_str())
        .map(|s| s.to_ascii_lowercase());
    let uptime = conn.db.uptime_secs();
    let clients = conn.db.num_clients();
    let cmds = conn.db.total_commands_processed();
    let dbsize = {
        let state = state_read!(conn);
        state.engine.scan(b"", b"").map(|v| v.len()).unwrap_or(0)
    };
    let server  = format!("# Server\r\nredis_version:7.0.0\r\nredis_mode:standalone\r\nuptime_in_seconds:{uptime}\r\n");
    let cli_s = format!("# Clients\r\nconnected_clients:{clients}\r\n");
    let stats = format!("# Stats\r\ntotal_commands_processed:{cmds}\r\n");
    let ks = format!("# Keyspace\r\ndb0:keys={dbsize},expires=0,avg_ttl=0\r\n");
    let repl = "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n".to_owned();
    let mem = "# Memory\r\nused_memory:0\r\nused_memory_human:0B\r\n".to_owned();
    let full = match section.as_deref() {
        Some("server") => server,
        Some("clients") => cli_s,
        Some("stats") => stats,
        Some("keyspace") => ks,
        Some("replication") => repl,
        Some("memory") => mem,
        _ => format!("{server}{cli_s}{stats}{ks}{repl}{mem}"),
    };
    conn.bulk(Some(full.as_bytes())).await
}

async fn cmd_command(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let sub = args
        .first()
        .and_then(|a| a.as_str())
        .map(|s| s.to_ascii_uppercase());
    match sub.as_deref() {
        Some("COUNT") => conn.int(50).await,
        Some("DOCS") | Some("INFO") | Some("LIST") | Some("GETKEYS") | None => {
            conn.send(encode_array(&[])).await
        }
        _ => conn.err("ERR unknown COMMAND subcommand").await,
    }
}

async fn cmd_dbsize(conn: &mut Conn) -> anyhow::Result<()> {
    let n = {
        let s = state_read!(conn);
        s.engine.scan(b"", b"").map(|v| v.len() as i64).unwrap_or(0)
    };
    conn.int(n).await
}

async fn cmd_flushdb(conn: &mut Conn) -> anyhow::Result<()> {
    let keys: Vec<Vec<u8>> = {
        let s = state_read!(conn);
        s.engine
            .scan(b"", b"")
            .unwrap_or_default()
            .into_iter()
            .map(|(k, _)| k)
            .collect()
    };
    {
        let mut s = state_write!(conn);
        for k in &keys {
            let _ = s.engine.del(k.clone());
        }
        s.ttl.clear();
    }
    conn.ok().await
}

async fn cmd_debug(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let sub = args
        .first()
        .and_then(|a| a.as_str())
        .map(|s| s.to_ascii_uppercase());
    match sub.as_deref() {
        Some("SLEEP") => {
            let secs: f64 = args
                .get(1)
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            tokio::time::sleep(Duration::from_secs_f64(secs)).await;
            conn.ok().await
        }
        Some("OBJECT") => {
            conn.bulk(Some(
                b"encoding:raw serializedlength:0 lru:0 lru_seconds_idle:0",
            ))
            .await
        }
        _ => conn.ok().await,
    }
}

async fn cmd_object(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let sub = args
        .first()
        .and_then(|a| a.as_str())
        .map(|s| s.to_ascii_uppercase());
    match sub.as_deref() {
        Some("ENCODING") => conn.bulk(Some(b"raw")).await,
        Some("IDLETIME") => conn.int(0).await,
        Some("REFCOUNT") => conn.int(1).await,
        Some("FREQ") => conn.int(0).await,
        Some("HELP") => {
            let items = vec![
                encode_bulk(Some(b"OBJECT ENCODING <key>")),
                encode_bulk(Some(b"OBJECT IDLETIME <key>")),
                encode_bulk(Some(b"OBJECT REFCOUNT <key>")),
                encode_bulk(Some(b"OBJECT FREQ <key>")),
            ];
            conn.send(encode_array(&items)).await
        }
        _ => conn.err("ERR unknown OBJECT subcommand").await,
    }
}

async fn cmd_memory(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let sub = args
        .first()
        .and_then(|a| a.as_str())
        .map(|s| s.to_ascii_uppercase());
    match sub.as_deref() {
        Some("USAGE") => conn.bulk(None).await,
        Some("MALLOC-STATS") | Some("DOCTOR") | Some("STATS") => {
            conn.bulk(Some(b"not available")).await
        }
        Some("HELP") => conn.send(encode_array(&[])).await,
        Some("PURGE") => conn.ok().await,
        _ => conn.err("ERR unknown MEMORY subcommand").await,
    }
}

async fn cmd_slowlog(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let sub = args
        .first()
        .and_then(|a| a.as_str())
        .map(|s| s.to_ascii_uppercase());
    match sub.as_deref() {
        Some("GET") => conn.send(encode_array(&[])).await,
        Some("LEN") => conn.int(0).await,
        Some("RESET") => conn.ok().await,
        _ => conn.err("ERR unknown SLOWLOG subcommand").await,
    }
}

async fn cmd_latency(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let sub = args
        .first()
        .and_then(|a| a.as_str())
        .map(|s| s.to_ascii_uppercase());
    match sub.as_deref() {
        Some("LATEST") | Some("HISTORY") => conn.send(encode_array(&[])).await,
        Some("RESET") => conn.int(0).await,
        _ => conn.err("ERR unknown LATENCY subcommand").await,
    }
}

async fn cmd_acl(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let sub = args
        .first()
        .and_then(|a| a.as_str())
        .map(|s| s.to_ascii_uppercase());
    match sub.as_deref() {
        Some("WHOAMI") => conn.bulk(Some(b"default")).await,
        Some("CAT") => {
            let cats = vec![
                encode_bulk(Some(b"all")),
                encode_bulk(Some(b"read")),
                encode_bulk(Some(b"write")),
            ];
            conn.send(encode_array(&cats)).await
        }
        Some("LIST") => {
            conn.send(encode_array(&[encode_bulk(Some(
                b"user default on nopass ~* &* +@all",
            ))]))
            .await
        }
        Some("USERS") => {
            conn.send(encode_array(&[encode_bulk(Some(b"default"))]))
                .await
        }
        Some("LOG") => conn.send(encode_array(&[])).await,
        Some("GETUSER") => conn.send(encode_null_array()).await,
        Some("SETUSER") | Some("DELUSER") | Some("SAVE") | Some("LOAD") => conn.ok().await,
        Some("GENPASS") => {
            conn.bulk(Some(
                b"0000000000000000000000000000000000000000000000000000000000000000",
            ))
            .await
        }
        Some("INFO") => conn.send(encode_array(&[])).await,
        _ => conn.err("ERR unknown ACL subcommand").await,
    }
}

// ─── String commands ─────────────────────────────────────────────────────────

async fn cmd_get(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let key = req_key(args, 0, "get")?;
    let val = {
        let mut s = state_write!(conn);
        if s.evict_if_expired(&key)? {
            None
        } else {
            s.engine.get(&key)?.map(|(_, v)| v)
        }
    };
    conn.bulk(val.as_deref()).await
}

async fn cmd_set(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() < 2 {
        return conn
            .err("ERR wrong number of arguments for 'set' command")
            .await;
    }
    let key = req_key(args, 0, "set")?;
    let value = match args[1].as_bytes() {
        Some(v) => v.to_vec(),
        None => return conn.err("ERR value must be a string").await,
    };

    let mut ttl_ms: Option<i64> = None;
    let mut nx = false;
    let mut xx = false;
    let mut keepttl = false;
    let mut get_flag = false;
    let mut i = 2usize;
    while i < args.len() {
        let opt = match args[i].as_str() {
            Some(s) => s.to_ascii_uppercase(),
            None => return conn.err("ERR syntax error").await,
        };
        match opt.as_str() {
            "EX" => {
                i += 1;
                let s: i64 = parse_int(args.get(i))?;
                if s <= 0 {
                    return conn.err("ERR invalid expire time in 'set' command").await;
                }
                ttl_ms = Some(s * 1000);
            }
            "PX" => {
                i += 1;
                let ms: i64 = parse_int(args.get(i))?;
                if ms <= 0 {
                    return conn.err("ERR invalid expire time in 'set' command").await;
                }
                ttl_ms = Some(ms);
            }
            "EXAT" => {
                i += 1;
                let unix: i64 = parse_int(args.get(i))?;
                let now_s = unix_now_secs();
                ttl_ms = Some((unix - now_s) * 1000);
            }
            "PXAT" => {
                i += 1;
                let ums: i64 = parse_int(args.get(i))?;
                let now_ms = unix_now_ms();
                ttl_ms = Some(ums - now_ms);
            }
            "NX" => nx = true,
            "XX" => xx = true,
            "KEEPTTL" => keepttl = true,
            "GET" => get_flag = true,
            _ => return conn.err("ERR syntax error").await,
        }
        i += 1;
    }

    let result: SetResult = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        let prev = s.engine.get(&key)?.map(|(_, v)| v);
        if nx && prev.is_some() {
            SetResult::Nx(prev)
        } else if xx && prev.is_none() {
            SetResult::Xx
        } else {
            s.engine.set(key.clone(), value)?;
            if !keepttl {
                match ttl_ms {
                    Some(ms) if ms > 0 => s.set_expiry(&key, Duration::from_millis(ms as u64)),
                    _ => s.clear_expiry(&key),
                }
            }
            SetResult::Ok(prev)
        }
    };
    match result {
        SetResult::Nx(prev) => {
            if get_flag {
                conn.bulk(prev.as_deref()).await
            } else {
                conn.bulk(None).await
            }
        }
        SetResult::Xx => conn.bulk(None).await,
        SetResult::Ok(prev) => {
            if get_flag {
                conn.bulk(prev.as_deref()).await
            } else {
                conn.ok().await
            }
        }
    }
}

enum SetResult {
    Ok(Option<Vec<u8>>),
    Nx(Option<Vec<u8>>),
    Xx,
}

async fn cmd_setnx(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 2 {
        return conn
            .err("ERR wrong number of arguments for 'setnx' command")
            .await;
    }
    let key = req_key(args, 0, "setnx")?;
    let val = req_bytes(args, 1)?;
    let set = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        if s.engine.get(&key)?.is_some() {
            false
        } else {
            s.engine.set(key, val)?;
            true
        }
    };
    conn.int(if set { 1 } else { 0 }).await
}

async fn cmd_setex(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 3 {
        return conn
            .err("ERR wrong number of arguments for 'setex' command")
            .await;
    }
    let key = req_key(args, 0, "setex")?;
    let secs: i64 = parse_int(args.get(1))?;
    if secs <= 0 {
        return conn.err("ERR invalid expire time in 'setex' command").await;
    }
    let val = req_bytes(args, 2)?;
    {
        let mut s = state_write!(conn);
        s.engine.set(key.clone(), val)?;
        s.set_expiry(&key, Duration::from_secs(secs as u64));
    }
    conn.ok().await
}

async fn cmd_psetex(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 3 {
        return conn
            .err("ERR wrong number of arguments for 'psetex' command")
            .await;
    }
    let key = req_key(args, 0, "psetex")?;
    let ms: i64 = parse_int(args.get(1))?;
    if ms <= 0 {
        return conn
            .err("ERR invalid expire time in 'psetex' command")
            .await;
    }
    let val = req_bytes(args, 2)?;
    {
        let mut s = state_write!(conn);
        s.engine.set(key.clone(), val)?;
        s.set_expiry(&key, Duration::from_millis(ms as u64));
    }
    conn.ok().await
}

async fn cmd_getset(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 2 {
        return conn
            .err("ERR wrong number of arguments for 'getset' command")
            .await;
    }
    let key = req_key(args, 0, "getset")?;
    let val = req_bytes(args, 1)?;
    let prev = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        let p = s.engine.get(&key)?.map(|(_, v)| v);
        s.engine.set(key.clone(), val)?;
        s.clear_expiry(&key);
        p
    };
    conn.bulk(prev.as_deref()).await
}

async fn cmd_getdel(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 1 {
        return conn
            .err("ERR wrong number of arguments for 'getdel' command")
            .await;
    }
    let key = req_key(args, 0, "getdel")?;
    let prev = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        let p = s.engine.get(&key)?.map(|(_, v)| v);
        if p.is_some() {
            s.engine.del(key.clone())?;
            s.clear_expiry(&key);
        }
        p
    };
    conn.bulk(prev.as_deref()).await
}

async fn cmd_getex(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.is_empty() {
        return conn
            .err("ERR wrong number of arguments for 'getex' command")
            .await;
    }
    let key = req_key(args, 0, "getex")?;
    let val = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        let v = s.engine.get(&key)?.map(|(_, v)| v);
        if v.is_some() {
            let mut i = 1usize;
            while i < args.len() {
                let opt = args[i]
                    .as_str()
                    .map(|x| x.to_ascii_uppercase())
                    .unwrap_or_default();
                match opt.as_str() {
                    "EX" => {
                        i += 1;
                        let sec: u64 = parse_int(args.get(i)).unwrap_or(0) as u64;
                        s.set_expiry(&key, Duration::from_secs(sec));
                    }
                    "PX" => {
                        i += 1;
                        let ms: u64 = parse_int(args.get(i)).unwrap_or(0) as u64;
                        s.set_expiry(&key, Duration::from_millis(ms));
                    }
                    "PERSIST" => {
                        s.clear_expiry(&key);
                    }
                    _ => {}
                }
                i += 1;
            }
        }
        v
    };
    conn.bulk(val.as_deref()).await
}

async fn cmd_mget(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.is_empty() {
        return conn
            .err("ERR wrong number of arguments for 'mget' command")
            .await;
    }
    let keys: Vec<Vec<u8>> = args
        .iter()
        .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
        .collect();
    let items = {
        let mut s = state_write!(conn);
        keys.iter()
            .map(|k| {
                let _ = s.evict_if_expired(k);
                let v = s.engine.get(k).ok().flatten().map(|(_, v)| v);
                encode_bulk(v.as_deref())
            })
            .collect::<Vec<_>>()
    };
    conn.send(encode_array(&items)).await
}

async fn cmd_mset(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return conn
            .err("ERR wrong number of arguments for 'mset' command")
            .await;
    }
    {
        let mut s = state_write!(conn);
        let mut i = 0usize;
        while i < args.len() {
            let k = req_key(args, i, "mset")?;
            let v = req_bytes(args, i + 1)?;
            s.engine.set(k.clone(), v)?;
            s.clear_expiry(&k);
            i += 2;
        }
    }
    conn.ok().await
}

async fn cmd_msetnx(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return conn
            .err("ERR wrong number of arguments for 'msetnx' command")
            .await;
    }
    let set = {
        let mut s = state_write!(conn);
        let mut all_missing = true;
        for i in (0..args.len()).step_by(2) {
            if let Some(k) = args[i].as_bytes() {
                if s.engine.get(k)?.is_some() {
                    all_missing = false;
                    break;
                }
            }
        }
        if all_missing {
            let mut i = 0usize;
            while i < args.len() {
                let k = req_key(args, i, "msetnx")?;
                let v = req_bytes(args, i + 1)?;
                s.engine.set(k, v)?;
                i += 2;
            }
        }
        all_missing
    };
    conn.int(if set { 1 } else { 0 }).await
}

async fn cmd_append(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 2 {
        return conn
            .err("ERR wrong number of arguments for 'append' command")
            .await;
    }
    let key = req_key(args, 0, "append")?;
    let suffix = req_bytes(args, 1)?;
    let len = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        let mut cur = s.engine.get(&key)?.map(|(_, v)| v).unwrap_or_default();
        cur.extend_from_slice(&suffix);
        let l = cur.len() as i64;
        s.engine.set(key, cur)?;
        l
    };
    conn.int(len).await
}

async fn cmd_strlen(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 1 {
        return conn
            .err("ERR wrong number of arguments for 'strlen' command")
            .await;
    }
    let key = req_key(args, 0, "strlen")?;
    let len = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        s.engine
            .get(&key)?
            .map(|(_, v)| v.len() as i64)
            .unwrap_or(0)
    };
    conn.int(len).await
}

async fn cmd_incr(conn: &mut Conn, args: &[RespValue], delta: i64) -> anyhow::Result<()> {
    let key = req_key(args, 0, "incr")?;
    let next = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        let cur: i64 = s
            .engine
            .get(&key)?
            .map(|(_, v)| v)
            .as_deref()
            .map(|b| {
                std::str::from_utf8(b)
                    .ok()
                    .and_then(|x| x.parse().ok())
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        let n = cur
            .checked_add(delta)
            .ok_or_else(|| anyhow::anyhow!("ERR increment or decrement would overflow"))?;
        s.engine.set(key, n.to_string().into_bytes())?;
        n
    };
    conn.int(next).await
}

async fn cmd_incrby(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 2 {
        return conn
            .err("ERR wrong number of arguments for 'incrby' command")
            .await;
    }
    let delta: i64 = parse_int(args.get(1))?;
    cmd_incr(conn, &args[..1], delta).await
}

async fn cmd_decrby(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 2 {
        return conn
            .err("ERR wrong number of arguments for 'decrby' command")
            .await;
    }
    let delta: i64 = parse_int(args.get(1))?;
    cmd_incr(conn, &args[..1], -delta).await
}

async fn cmd_incrbyfloat(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 2 {
        return conn
            .err("ERR wrong number of arguments for 'incrbyfloat' command")
            .await;
    }
    let key = req_key(args, 0, "incrbyfloat")?;
    let delta: f64 = args[1]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("ERR not a float"))?;
    let result = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        let cur: f64 = s
            .engine
            .get(&key)?
            .map(|(_, v)| v)
            .as_deref()
            .map(|b| {
                std::str::from_utf8(b)
                    .ok()
                    .and_then(|x| x.parse().ok())
                    .unwrap_or(0.0)
            })
            .unwrap_or(0.0);
        let next = cur + delta;
        if next.is_nan() || next.is_infinite() {
            return Err(anyhow::anyhow!(
                "ERR increment would produce NaN or Infinity"
            ));
        }
        let repr = format_float(next);
        s.engine.set(key, repr.as_bytes().to_vec())?;
        repr
    };
    conn.bulk(Some(result.as_bytes())).await
}

async fn cmd_getrange(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 3 {
        return conn
            .err("ERR wrong number of arguments for 'getrange' command")
            .await;
    }
    let key = req_key(args, 0, "getrange")?;
    let start: i64 = parse_int(args.get(1)).unwrap_or(0);
    let end: i64 = parse_int(args.get(2)).unwrap_or(-1);
    let slice = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        let val = s.engine.get(&key)?.map(|(_, v)| v).unwrap_or_default();
        let len = val.len() as i64;
        let st = if start >= 0 {
            start as usize
        } else {
            (len + start).max(0) as usize
        };
        let en = if end >= 0 {
            (end as usize + 1).min(val.len())
        } else {
            (len + end + 1).max(0) as usize
        };
        if st >= val.len() || st >= en {
            vec![]
        } else {
            val[st..en].to_vec()
        }
    };
    conn.bulk(Some(&slice)).await
}

async fn cmd_setrange(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 3 {
        return conn
            .err("ERR wrong number of arguments for 'setrange' command")
            .await;
    }
    let key = req_key(args, 0, "setrange")?;
    let offset: usize = parse_int(args.get(1))? as usize;
    let patch = req_bytes(args, 2)?;
    let len = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        let mut val = s.engine.get(&key)?.map(|(_, v)| v).unwrap_or_default();
        let needed = offset + patch.len();
        if val.len() < needed {
            val.resize(needed, 0);
        }
        val[offset..offset + patch.len()].copy_from_slice(&patch);
        let l = val.len() as i64;
        s.engine.set(key, val)?;
        l
    };
    conn.int(len).await
}

// ─── Key / generic commands ──────────────────────────────────────────────────

async fn cmd_del(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.is_empty() {
        return conn
            .err("ERR wrong number of arguments for 'del' command")
            .await;
    }
    let count = {
        let mut s = state_write!(conn);
        let mut c = 0i64;
        for a in args {
            if let Some(k) = a.as_bytes() {
                let expired = s.evict_if_expired(k)?;
                if !expired && s.engine.get(k)?.is_some() {
                    s.engine.del(k.to_vec())?;
                    s.ttl.remove(k);
                    c += 1;
                }
            }
        }
        c
    };
    conn.int(count).await
}

async fn cmd_exists(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.is_empty() {
        return conn
            .err("ERR wrong number of arguments for 'exists' command")
            .await;
    }
    let count = {
        let mut s = state_write!(conn);
        let mut c = 0i64;
        for a in args {
            if let Some(k) = a.as_bytes() {
                let _ = s.evict_if_expired(k);
                if s.engine.get(k)?.is_some() {
                    c += 1;
                }
            }
        }
        c
    };
    conn.int(count).await
}

async fn cmd_type(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 1 {
        return conn
            .err("ERR wrong number of arguments for 'type' command")
            .await;
    }
    let key = req_key(args, 0, "type")?;
    let t = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        if s.engine.get(&key)?.is_some() {
            "string"
        } else {
            "none"
        }
    };
    conn.send(encode_simple(t)).await
}

async fn cmd_ttl(conn: &mut Conn, args: &[RespValue], millis: bool) -> anyhow::Result<()> {
    if args.len() != 1 {
        return conn.err("ERR wrong number of arguments for command").await;
    }
    let key = req_key(args, 0, "ttl")?;
    let n = {
        let mut s = state_write!(conn);
        if s.evict_if_expired(&key)? || s.engine.get(&key)?.is_none() {
            -2
        } else {
            match s.ttl_ms(&key) {
                None => -1,
                Some(-2) => {
                    let _ = s.engine.del(key.clone());
                    s.ttl.remove(&key);
                    -2
                }
                Some(ms) => {
                    if millis {
                        ms
                    } else {
                        ms / 1000
                    }
                }
            }
        }
    };
    conn.int(n).await
}

async fn cmd_expire(conn: &mut Conn, args: &[RespValue], millis: bool) -> anyhow::Result<()> {
    if args.len() < 2 {
        return conn.err("ERR wrong number of arguments for command").await;
    }
    let key = req_key(args, 0, "expire")?;
    let t: i64 = parse_int(args.get(1))?;
    if t <= 0 {
        return conn.err("ERR invalid expire time in command").await;
    }
    let r = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        if s.engine.get(&key)?.is_none() {
            0i64
        } else {
            let dur = if millis {
                Duration::from_millis(t as u64)
            } else {
                Duration::from_secs(t as u64)
            };
            s.set_expiry(&key, dur);
            1
        }
    };
    conn.int(r).await
}

async fn cmd_expireat(conn: &mut Conn, args: &[RespValue], millis: bool) -> anyhow::Result<()> {
    if args.len() < 2 {
        return conn.err("ERR wrong number of arguments for command").await;
    }
    let key = req_key(args, 0, "expireat")?;
    let unix: i64 = parse_int(args.get(1))?;
    let now_ms = unix_now_ms();
    let delta_ms = if millis {
        unix - now_ms
    } else {
        (unix * 1000) - now_ms
    };
    let r = {
        let mut s = state_write!(conn);
        if delta_ms <= 0 {
            let _ = s.engine.del(key.clone());
            s.ttl.remove(&key);
            1i64
        } else {
            s.evict_if_expired(&key)?;
            if s.engine.get(&key)?.is_none() {
                0
            } else {
                s.set_expiry(&key, Duration::from_millis(delta_ms as u64));
                1
            }
        }
    };
    conn.int(r).await
}

async fn cmd_expiretime(conn: &mut Conn, args: &[RespValue], millis: bool) -> anyhow::Result<()> {
    if args.len() != 1 {
        return conn.err("ERR wrong number of arguments for command").await;
    }
    let key = req_key(args, 0, "expiretime")?;
    let n = {
        let mut s = state_write!(conn);
        if s.evict_if_expired(&key)? || s.engine.get(&key)?.is_none() {
            -2
        } else {
            match s.ttl_ms(&key) {
                None => -1,
                Some(ms) => {
                    let now_ms = unix_now_ms();
                    let unix_ms = now_ms + ms;
                    if millis {
                        unix_ms
                    } else {
                        unix_ms / 1000
                    }
                }
            }
        }
    };
    conn.int(n).await
}

async fn cmd_persist(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 1 {
        return conn
            .err("ERR wrong number of arguments for 'persist' command")
            .await;
    }
    let key = req_key(args, 0, "persist")?;
    let r = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&key)?;
        if s.engine.get(&key)?.is_none() {
            0i64
        } else if s.ttl.remove(&key).is_some() {
            1
        } else {
            0
        }
    };
    conn.int(r).await
}

async fn cmd_keys(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let pattern = match args.first().and_then(|a| a.as_bytes()) {
        Some(p) => String::from_utf8_lossy(p).into_owned(),
        None => {
            return conn
                .err("ERR wrong number of arguments for 'keys' command")
                .await
        }
    };
    let items = {
        let s = state_read!(conn);
        s.engine
            .scan(b"", b"")
            .unwrap_or_default()
            .into_iter()
            .filter(|(k, _)| !s.is_expired(k))
            .filter(|(k, _)| glob_match(&pattern, &String::from_utf8_lossy(k)))
            .map(|(k, _)| encode_bulk(Some(&k)))
            .collect::<Vec<_>>()
    };
    conn.send(encode_array(&items)).await
}

async fn cmd_scan(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    let mut pattern = "*".to_owned();
    let mut i = 1usize;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                pattern = args
                    .get(i)
                    .and_then(|a| a.as_str())
                    .unwrap_or("*")
                    .to_owned();
            }
            "COUNT" | "TYPE" => {
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }
    let items = {
        let s = state_read!(conn);
        s.engine
            .scan(b"", b"")
            .unwrap_or_default()
            .into_iter()
            .filter(|(k, _)| !s.is_expired(k))
            .filter(|(k, _)| glob_match(&pattern, &String::from_utf8_lossy(k)))
            .map(|(k, _)| encode_bulk(Some(&k)))
            .collect::<Vec<_>>()
    };
    let resp = encode_array(&[encode_bulk(Some(b"0")), encode_array(&items)]);
    conn.send(resp).await
}

async fn cmd_rename(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 2 {
        return conn
            .err("ERR wrong number of arguments for 'rename' command")
            .await;
    }
    let src = req_key(args, 0, "rename")?;
    let dst = req_key(args, 1, "rename")?;
    {
        let mut s = state_write!(conn);
        s.evict_if_expired(&src)?;
        let val = s
            .engine
            .get(&src)?
            .map(|(_, v)| v)
            .ok_or_else(|| anyhow::anyhow!("ERR no such key"))?;
        let ttl = s.ttl.get(&src).cloned();
        s.engine.set(dst.clone(), val)?;
        s.engine.del(src.clone())?;
        s.ttl.remove(&src);
        match ttl {
            Some(t) => {
                s.ttl.insert(dst, t);
            }
            None => {
                s.ttl.remove(&dst);
            }
        }
    }
    conn.ok().await
}

async fn cmd_renamenx(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.len() != 2 {
        return conn
            .err("ERR wrong number of arguments for 'renamenx' command")
            .await;
    }
    let src = req_key(args, 0, "renamenx")?;
    let dst = req_key(args, 1, "renamenx")?;
    let r = {
        let mut s = state_write!(conn);
        s.evict_if_expired(&src)?;
        s.evict_if_expired(&dst)?;
        let val = s
            .engine
            .get(&src)?
            .map(|(_, v)| v)
            .ok_or_else(|| anyhow::anyhow!("ERR no such key"))?;
        if s.engine.get(&dst)?.is_some() {
            0i64
        } else {
            s.engine.set(dst.clone(), val)?;
            s.engine.del(src.clone())?;
            s.ttl.remove(&src);
            s.ttl.remove(&dst);
            1
        }
    };
    conn.int(r).await
}

async fn cmd_randomkey(conn: &mut Conn) -> anyhow::Result<()> {
    let result = {
        let s = state_read!(conn);
        let all = s.engine.scan(b"", b"").unwrap_or_default();
        let live: Vec<_> = all.iter().filter(|(k, _)| !s.is_expired(k)).collect();
        if live.is_empty() {
            None
        } else {
            let idx = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as usize % live.len())
                .unwrap_or(0);
            Some(live[idx].0.clone())
        }
    };
    conn.bulk(result.as_deref()).await
}

async fn cmd_touch(conn: &mut Conn, args: &[RespValue]) -> anyhow::Result<()> {
    if args.is_empty() {
        return conn
            .err("ERR wrong number of arguments for 'touch' command")
            .await;
    }
    let count = {
        let s = state_write!(conn);
        let mut c = 0i64;
        for a in args {
            if let Some(k) = a.as_bytes() {
                if !s.is_expired(k) && s.engine.get(k)?.is_some() {
                    c += 1;
                }
            }
        }
        c
    };
    conn.int(count).await
}

// ─── Small helpers ───────────────────────────────────────────────────────────

fn req_key(args: &[RespValue], i: usize, _cmd: &str) -> anyhow::Result<Vec<u8>> {
    args.get(i)
        .and_then(|a| a.as_bytes())
        .map(|b| b.to_vec())
        .ok_or_else(|| anyhow::anyhow!("ERR key must be a string"))
}

fn req_bytes(args: &[RespValue], i: usize) -> anyhow::Result<Vec<u8>> {
    args.get(i)
        .and_then(|a| a.as_bytes())
        .map(|b| b.to_vec())
        .ok_or_else(|| anyhow::anyhow!("ERR argument must be a string"))
}

fn parse_int(v: Option<&RespValue>) -> anyhow::Result<i64> {
    v.and_then(|a| a.as_str())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("ERR value is not an integer or out of range"))
}

fn unix_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn format_float(f: f64) -> String {
    if f == f.floor() && f.abs() < 1e15 {
        format!("{}", f as i64)
    } else {
        format!("{}", f)
    }
}

// ─── Glob matching ───────────────────────────────────────────────────────────

fn glob_match(pattern: &str, s: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let str: Vec<char> = s.chars().collect();
    glob_inner(&pat, &str)
}

fn glob_inner(pat: &[char], s: &[char]) -> bool {
    match (pat.first(), s.first()) {
        (None, None) => true,
        (Some(&'*'), _) => glob_inner(&pat[1..], s) || (!s.is_empty() && glob_inner(pat, &s[1..])),
        (Some(&'?'), Some(_)) => glob_inner(&pat[1..], &s[1..]),
        (Some(p), Some(c)) if p == c => glob_inner(&pat[1..], &s[1..]),
        _ => false,
    }
}
