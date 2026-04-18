//! RESP2 protocol parser and serializer.
//!
//! Supports all five RESP2 types plus inline command parsing (for redis-cli
//! and telnet clients that don't use the full framing).

use anyhow::{bail, Result};
use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;

/// A RESP2 value.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    /// Convenience: unwrap a bulk-string or simple-string as UTF-8 bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            RespValue::BulkString(Some(b)) => Some(b),
            RespValue::SimpleString(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// Convenience: unwrap as a UTF-8 string slice.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            RespValue::BulkString(Some(b)) => std::str::from_utf8(b).ok(),
            RespValue::SimpleString(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

// ─── Async parser ────────────────────────────────────────────────────────────

pub struct RespReader {
    inner: BufReader<OwnedReadHalf>,
}

impl RespReader {
    pub fn new(read_half: OwnedReadHalf) -> Self {
        Self {
            inner: BufReader::with_capacity(8 * 1024, read_half),
        }
    }

    /// Read one RESP value (or inline command) from the stream.
    /// Returns `Ok(None)` on clean EOF.
    ///
    /// Arrays are parsed by reading each element with `read_item()` to avoid
    /// recursive async functions (which require `Box::pin` indirection).
    pub async fn read_value(&mut self) -> Result<Option<RespValue>> {
        let mut line = String::new();
        let n = self.inner.read_line(&mut line).await?;
        if n == 0 {
            return Ok(None);
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);

        if let Some(rest) = trimmed.strip_prefix('*') {
            let count: i64 = rest.parse()?;
            if count < 0 {
                return Ok(Some(RespValue::Array(None)));
            }
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                items.push(self.read_item().await?);
            }
            return Ok(Some(RespValue::Array(Some(items))));
        }

        // Non-array top-level value or inline command.
        self.parse_scalar(trimmed).await.map(Some)
    }

    /// Read exactly one scalar (non-array) RESP item from the stream.
    /// Used to read the elements of an array without recursion.
    async fn read_item(&mut self) -> Result<RespValue> {
        let mut line = String::new();
        let n = self.inner.read_line(&mut line).await?;
        if n == 0 {
            bail!("unexpected EOF reading RESP item");
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        self.parse_scalar(trimmed).await
    }

    /// Parse a single non-array RESP line into a value.
    async fn parse_scalar(&mut self, line: &str) -> Result<RespValue> {
        match line.as_bytes().first() {
            Some(b'+') => Ok(RespValue::SimpleString(line[1..].to_owned())),
            Some(b'-') => Ok(RespValue::Error(line[1..].to_owned())),
            Some(b':') => Ok(RespValue::Integer(line[1..].parse()?)),
            Some(b'$') => {
                let len: i64 = line[1..].parse()?;
                if len < 0 {
                    return Ok(RespValue::BulkString(None));
                }
                let len = len as usize;
                let mut buf = vec![0u8; len + 2]; // +2 for \r\n
                self.inner.read_exact(&mut buf).await?;
                buf.truncate(len);
                Ok(RespValue::BulkString(Some(Bytes::from(buf))))
            }
            _ => {
                // Inline command: space-separated tokens
                let parts: Vec<RespValue> = line
                    .split_ascii_whitespace()
                    .map(|s| RespValue::BulkString(Some(Bytes::from(s.as_bytes().to_vec()))))
                    .collect();
                if parts.is_empty() {
                    bail!("empty inline command");
                }
                // Wrap inline tokens as an array so the dispatcher sees the same shape.
                Ok(RespValue::Array(Some(parts)))
            }
        }
    }
}

// ─── Serializer ──────────────────────────────────────────────────────────────

/// Encode a RESP2 simple string.
#[inline]
pub fn encode_simple(s: &str) -> Vec<u8> {
    format!("+{}\r\n", s).into_bytes()
}

/// Encode a RESP2 error.
#[inline]
pub fn encode_error(msg: &str) -> Vec<u8> {
    format!("-{}\r\n", msg).into_bytes()
}

/// Encode a RESP2 integer.
#[inline]
pub fn encode_int(n: i64) -> Vec<u8> {
    format!(":{}\r\n", n).into_bytes()
}

/// Encode a RESP2 bulk string (or null).
pub fn encode_bulk(data: Option<&[u8]>) -> Vec<u8> {
    match data {
        None => b"$-1\r\n".to_vec(),
        Some(b) => {
            let mut out = format!("${}\r\n", b.len()).into_bytes();
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
            out
        }
    }
}

/// Encode a RESP2 array (or null).
pub fn encode_array(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", items.len()).into_bytes();
    for item in items {
        out.extend_from_slice(item);
    }
    out
}

/// Null array.
#[inline]
pub fn encode_null_array() -> Vec<u8> {
    b"*-1\r\n".to_vec()
}

/// OK simple string.
#[inline]
pub fn ok() -> Vec<u8> {
    b"+OK\r\n".to_vec()
}
