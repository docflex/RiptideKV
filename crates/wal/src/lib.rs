//! # WAL — Write-Ahead Log
//!
//! Provides crash-safe durability for the RiptideKV storage engine.
//!
//! Every mutation (`PUT` or `DELETE`) is serialized into a binary record and
//! appended to the WAL **before** the corresponding in-memory update. On
//! restart the WAL is replayed to reconstruct the memtable, guaranteeing that
//! no acknowledged write is lost.
//!
//! ## Binary Record Format
//!
//! ```text
//! [record_len: u32 LE][crc32: u32 LE][body ...]
//! ```
//!
//! Body (Put): `[seq: u64][op=0: u8][key_len: u32][key][val_len: u32][value]`
//! Body (Del): `[seq: u64][op=1: u8][key_len: u32][key]`
//!
//! `record_len` includes the 4-byte CRC but **not** itself.
//!
//! ## Example
//!
//! ```rust,no_run
//! use wal::{WalWriter, WalReader, WalRecord};
//!
//! let mut w = WalWriter::create("wal.log", true).unwrap();
//! w.append(&WalRecord::Put {
//!     seq: 1,
//!     key: b"hello".to_vec(),
//!     value: b"world".to_vec(),
//! }).unwrap();
//! drop(w);
//!
//! let mut r = WalReader::open("wal.log").unwrap();
//! r.replay(|rec| println!("{:?}", rec)).unwrap();
//! ```

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher as Crc32;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::Path;

use thiserror::Error;

/// A single WAL record representing either a key-value insertion or a deletion.
///
/// Each record carries a monotonically increasing **sequence number** that the
/// engine uses for ordering, conflict resolution, and (in later phases) snapshot reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    /// A key-value insertion.
    Put {
        /// Sequence number assigned by the engine.
        seq: u64,
        /// The lookup key.
        key: Vec<u8>,
        /// The payload value.
        value: Vec<u8>,
    },
    /// A key deletion (tombstone).
    Del {
        /// Sequence number assigned by the engine.
        seq: u64,
        /// The key to delete.
        key: Vec<u8>,
    },
}

/// Errors that can occur during WAL operations.
#[derive(Debug, Error)]
pub enum WalError {
    /// An underlying I/O error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// A record failed CRC validation or contained an unknown op code.
    #[error("corrupt record")]
    Corrupt,
}

/// Append-only WAL writer.
///
/// Records are serialized into an in-memory buffer, CRC-checksummed, and then
/// written to the underlying file in a single `write_all` call. When `sync` is
/// `true`, every append is followed by `sync_all()` (fsync) to guarantee the
/// record is durable on disk before the call returns.
pub struct WalWriter {
    file: File,
    sync: bool,
    /// Reusable scratch buffer to avoid allocation on every append.
    buf: Vec<u8>,
}

impl WalWriter {
    /// Opens (or creates) a WAL file in append mode.
    ///
    /// # Arguments
    ///
    /// * `path` - file system path for the WAL (created if it does not exist).
    /// * `sync` - if true, every `append` call is followed by `fsync`.
    pub fn create<P: AsRef<Path>>(path: P, sync: bool) -> Result<Self, WalError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Self {
            file,
            sync,
            buf: Vec::with_capacity(256),
        })
    }

    /// Serializes `record` and appends it to the WAL file.
    ///
    /// Layout:
    /// [record_len: u32 LE][crc32: u32 LE][body bytes...]
    pub fn append(&mut self, record: &WalRecord) -> Result<(), WalError> {
        // Reuse the internal buffer — clear but keep the allocation
        self.buf.clear();

        // Reserve 8 bytes for the frame header (record_len + crc), filled later
        self.buf.extend_from_slice(&[0u8; 8]);

        // Write body into buf starting at offset 8
        match record {
            WalRecord::Put { seq, key, value } => {
                self.buf.write_u64::<LittleEndian>(*seq)?;
                self.buf.write_u8(0)?; // op = put
                self.buf.write_u32::<LittleEndian>(key.len() as u32)?;
                self.buf.extend_from_slice(key);
                self.buf.write_u32::<LittleEndian>(value.len() as u32)?;
                self.buf.extend_from_slice(value);
            }
            WalRecord::Del { seq, key } => {
                self.buf.write_u64::<LittleEndian>(*seq)?;
                self.buf.write_u8(1)?; // op = del
                self.buf.write_u32::<LittleEndian>(key.len() as u32)?;
                self.buf.extend_from_slice(key);
            }
        }

        // Body is buf[8..]
        let body = &self.buf[8..];

        // Compute CRC over the body
        let mut hasher = Crc32::new();
        hasher.update(body);
        let crc = hasher.finalize();

        // record_len = body.len() + 4 (CRC), must fit in u32
        let record_len = (body.len() as u64) + 4;
        if record_len > (u32::MAX as u64) {
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL record too large (exceeds u32::MAX bytes)",
            )));
        }

        // Fill in the 8-byte header: record_len(u32) + crc(u32)
        let header = (record_len as u32).to_le_bytes();
        let crc_bytes = crc.to_le_bytes();
        self.buf[0..4].copy_from_slice(&header);
        self.buf[4..8].copy_from_slice(&crc_bytes);

        // Single write call for the entire frame
        self.file.write_all(&self.buf)?;
        self.file.flush()?;

        if self.sync {
            self.file.sync_all()?;
        }

        Ok(())
    }

    /// Forces all buffered data to be written to disk via `sync_all()`.
    ///
    /// Useful when `sync` is `false` (batched mode) and the caller wants to
    /// ensure durability at a specific point (e.g., before acknowledging a batch).
    pub fn sync_to_disk(&mut self) -> Result<(), WalError> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

/// Sequential WAL reader that yields valid records.
///
/// The reader is generic over any `Read` implementor, allowing it to be used
/// with real files (`WalReader<File>`) or in-memory buffers for testing.
///
/// During replay, each record's CRC32 is verified. A truncated tail record
/// (e.g., from a crash mid-write) is treated as a clean EOF — all fully-written
/// records before it are still returned.
pub struct WalReader<R: Read> {
    rdr: BufReader<R>,
}

impl WalReader<File> {
    /// Opens an existing WAL file for sequential replay.
    ///
    /// Returns `WalError::Io` if the file cannot be opened.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<WalReader<File>, WalError> {
        let f = File::open(path)?;
        Ok(WalReader {
            rdr: BufReader::new(f),
        })
    }
}

impl<R: Read> WalReader<R> {
    /// Constructs a reader from any `Read` implementor.
    ///
    /// Useful for unit tests that supply an in-memory buffer (e.g., `Cursor<Vec<u8>>`).
    pub fn from_reader(reader: R) -> Self {
        WalReader {
            rdr: BufReader::new(reader),
        }
    }

    /// Replays every valid record in the WAL, calling `apply` for each one.
    ///
    /// # Termination
    ///
    /// - **Clean EOF** (no more bytes) -> returns `Ok(())`.
    /// - **Truncated tail** (partial record at end, e.g., crash mid-write) ->
    ///   returns `Ok(())` after yielding all complete records before it.
    /// - **CRC mismatch** -> returns `Err(WalError::Corrupt)`.
    /// - **Unknown op code** -> returns `Err(WalError::Corrupt)`.
    /// - **I/O error** -> returns `Err(WalError::Io(...))`.
    pub fn replay<F>(&mut self, mut apply: F) -> Result<(), WalError>
    where
        F: FnMut(WalRecord),
    {
        // Reusable buffer to avoid allocation per record
        let mut body = Vec::with_capacity(256);

        loop {
            // read record_len
            let record_len = match self.rdr.read_u32::<LittleEndian>() {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(WalError::Io(e)),
            };

            // record_len includes CRC (4 bytes) but not itself
            // Reject absurd sizes -> corruption
            const MAX_RECORD_SIZE: u32 = 64 * 1024 * 1024; // 64MB safety cap
            if record_len <= 4 || record_len > MAX_RECORD_SIZE {
                return Err(WalError::Corrupt);
            }

            // read crc (handle truncated tail)
            let crc = match self.rdr.read_u32::<LittleEndian>() {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(WalError::Io(e)),
            };

            // read body (record_len - 4 bytes), reusing the buffer
            let body_len = (record_len - 4) as usize;
            body.clear();
            body.resize(body_len, 0);
            match self.rdr.read_exact(&mut body) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // truncated tail — treat as EOF
                    return Ok(());
                }
                Err(e) => return Err(WalError::Io(e)),
            }

            // verify crc (only after we've successfully read the full body)
            let mut hasher = Crc32::new();
            hasher.update(&body);
            if hasher.finalize() != crc {
                return Err(WalError::Corrupt);
            }

            // parse body (single read)
            let mut br = &body[..];
            let seq = br.read_u64::<LittleEndian>()?;
            let op = br.read_u8()?;
            let key_len = br.read_u32::<LittleEndian>()? as usize;
            if key_len > body_len {
                return Err(WalError::Corrupt);
            }
            let mut key = vec![0u8; key_len];
            br.read_exact(&mut key)?;

            match op {
                0 => {
                    let val_len = br.read_u32::<LittleEndian>()? as usize;
                    if val_len > body_len {
                        return Err(WalError::Corrupt);
                    }
                    let mut val = vec![0u8; val_len];
                    br.read_exact(&mut val)?;
                    apply(WalRecord::Put {
                        seq,
                        key,
                        value: val,
                    });
                }
                1 => {
                    apply(WalRecord::Del { seq, key });
                }
                _ => return Err(WalError::Corrupt),
            }
        }
    }
}

#[cfg(test)]
mod tests;
