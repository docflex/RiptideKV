use anyhow::{bail, Result};
use bloom::BloomFilter;
use byteorder::{LittleEndian, ReadBytesExt};
use crc32fast::Hasher as Crc32;
use memtable::ValueEntry;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::format::{read_footer_versioned, Footer, FOOTER_BYTES_V1};

/// Maximum key size we'll allocate during reads (64 KiB). Prevents OOM on corrupt files.
const MAX_KEY_BYTES: usize = 64 * 1024;
/// Maximum value size we'll allocate during reads (10 MiB). Prevents OOM on corrupt files.
const MAX_VALUE_BYTES: usize = 10 * 1024 * 1024;

/// Reads an SSTable file for point lookups.
///
/// On [`open`](SSTableReader::open) the entire **index** is loaded into memory
/// as a `BTreeMap<Vec<u8>, u64>` (key → data-section byte offset). If the file
/// is v2, the bloom filter is also loaded for fast negative lookups.
///
/// A persistent file handle is kept open for the lifetime of the reader,
/// wrapped in a `Mutex` so that `get` can be called through a shared `&self`
/// reference.
///
/// Point lookups require only a single seek + read per call (no file open/close).
pub struct SSTableReader {
    /// Path to the `.sst` file on disk (kept for diagnostics).
    #[allow(dead_code)]
    path: PathBuf,
    /// In-memory index mapping each key to its byte offset in the data section.
    index: BTreeMap<Vec<u8>, u64>,
    /// Optional bloom filter (present for v2+ SSTables).
    bloom: Option<BloomFilter>,
    /// Persistent file handle, wrapped in Mutex for interior mutability.
    file: Mutex<BufReader<File>>,
    /// Parsed footer — used to determine version-specific read behaviour
    /// (e.g. whether to verify CRC32 on reads, or to expose max_seq).
    footer: Footer,
}

impl SSTableReader {
    /// Opens an SSTable file and loads its index (and bloom filter, if v2)
    /// into memory.
    ///
    /// # Validation
    ///
    /// - The file must be at least 12 bytes (v1 footer) or 20 bytes (v2 footer).
    /// - The footer magic must be `SST1` or `SST2`.
    /// - The `index_offset` must point inside the file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file is too small, the magic is wrong, or any
    /// I/O operation fails.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let mut f = File::open(&path_buf)?;
        let metadata = f.metadata()?;
        let filesize = metadata.len();

        if filesize < FOOTER_BYTES_V1 {
            bail!("sstable file too small");
        }

        // Auto-detect v1 or v2 footer
        let footer = read_footer_versioned(&mut f)?;
        let index_offset = footer.index_offset();

        if index_offset >= filesize {
            bail!("invalid index_offset");
        }

        // Determine where the index section ends (footer start)
        let footer_size = footer.footer_size();

        // Load bloom filter if v2
        let bloom = if let Some(bloom_offset) = footer.bloom_offset() {
            f.seek(SeekFrom::Start(bloom_offset))?;
            Some(BloomFilter::read_from(&mut f)?)
        } else {
            None
        };

        // Read index entries from index_offset up to footer start
        f.seek(SeekFrom::Start(index_offset))?;
        let mut index = BTreeMap::new();

        while f.stream_position()? < (filesize - footer_size) {
            let key_len = f.read_u32::<LittleEndian>()? as usize;
            if key_len > MAX_KEY_BYTES {
                bail!("corrupt index: key_len {} exceeds maximum {}", key_len, MAX_KEY_BYTES);
            }
            let mut key = vec![0u8; key_len];
            f.read_exact(&mut key)?;
            let data_offset = f.read_u64::<LittleEndian>()?;
            index.insert(key, data_offset);
        }

        // Rewind to start for future reads
        f.seek(SeekFrom::Start(0))?;

        Ok(Self {
            path: path_buf,
            index,
            bloom,
            file: Mutex::new(BufReader::new(f)),
            footer,
        })
    }

    /// Point lookup for a single key.
    ///
    /// If a bloom filter is present (v2), it is checked first. A negative
    /// result means the key is **definitely not** in this SSTable, avoiding
    /// an index lookup and disk I/O entirely.
    ///
    /// Returns `Ok(Some(entry))` if the key exists in this SSTable (the entry
    /// may be a tombstone with `value: None`). Returns `Ok(None)` if the key
    /// is not present in the index.
    ///
    /// Uses the persistent file handle with a seek + read (no file open/close).
    ///
    /// # Errors
    ///
    /// Returns an error on I/O failure or if the on-disk key does not match
    /// the requested key (index corruption).
    pub fn get(&self, key: &[u8]) -> Result<Option<ValueEntry>> {
        // Fast path: bloom filter says "definitely not here"
        if let Some(ref bf) = self.bloom {
            if !bf.may_contain(key) {
                return Ok(None);
            }
        }

        let offset = match self.index.get(key) {
            Some(&o) => o,
            None => return Ok(None),
        };

        let has_crc = self.footer.has_checksums();

        let mut f = self.file.lock().map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        f.seek(SeekFrom::Start(offset))?;

        // v3 record layout: [crc32: u32][key_len: u32][key][seq: u64][present: u8][val_len: u32][val]
        // v1/v2 layout:    [key_len: u32][key][seq: u64][present: u8][val_len: u32][val]
        //
        // For v3, read the stored CRC first, then read the body and verify.
        let stored_crc = if has_crc {
            Some(f.read_u32::<LittleEndian>()?)
        } else {
            None
        };

        // Read the record body (everything after the CRC prefix).
        let key_len = f.read_u32::<LittleEndian>()? as usize;
        if key_len > MAX_KEY_BYTES {
            bail!("corrupt data: key_len {} exceeds maximum {}", key_len, MAX_KEY_BYTES);
        }
        let mut key_buf = vec![0u8; key_len];
        f.read_exact(&mut key_buf)?;

        // Sanity: the key read should match the requested key
        if key_buf.as_slice() != key {
            bail!("index pointed to mismatching key at offset");
        }

        let seq = f.read_u64::<LittleEndian>()?;
        let present = f.read_u8()?;
        let (value, val_bytes) = if present == 1 {
            let val_len = f.read_u32::<LittleEndian>()? as usize;
            if val_len > MAX_VALUE_BYTES {
                bail!("corrupt data: val_len {} exceeds maximum {}", val_len, MAX_VALUE_BYTES);
            }
            let mut val = vec![0u8; val_len];
            f.read_exact(&mut val)?;
            (Some(val.clone()), Some(val))
        } else {
            (None, None)
        };

        // Verify CRC32 for v3 SSTables.
        if let Some(expected_crc) = stored_crc {
            let mut hasher = Crc32::new();
            // Reconstruct the body that was checksummed: key_len + key + seq + present + [val_len + val]
            hasher.update(&(key_len as u32).to_le_bytes());
            hasher.update(&key_buf);
            hasher.update(&seq.to_le_bytes());
            hasher.update(&[present]);
            if let Some(ref vb) = val_bytes {
                hasher.update(&(vb.len() as u32).to_le_bytes());
                hasher.update(vb);
            }
            let actual_crc = hasher.finalize();
            if actual_crc != expected_crc {
                bail!(
                    "CRC32 mismatch at offset {}: expected {:#010x}, got {:#010x} (data corruption)",
                    offset, expected_crc, actual_crc
                );
            }
        }

        Ok(Some(ValueEntry { seq, value }))
    }

    /// Returns `true` if this SSTable has a bloom filter loaded (v2+ format).
    #[must_use]
    pub fn has_bloom(&self) -> bool {
        self.bloom.is_some()
    }

    /// Returns the max sequence number stored in the SSTable footer (v3+).
    ///
    /// For v1/v2 files this returns `None`, and the caller must scan all
    /// keys to determine the max seq (legacy recovery path).
    #[must_use]
    pub fn max_seq(&self) -> Option<u64> {
        self.footer.max_seq()
    }

    /// Returns `true` if this SSTable has per-record CRC32 checksums (v3+).
    #[must_use]
    pub fn has_checksums(&self) -> bool {
        self.footer.has_checksums()
    }

    /// Returns the number of entries in the in-memory index.
    #[must_use]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns `true` if the SSTable contains zero entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Returns an iterator over all keys in the in-memory index.
    ///
    /// Keys are yielded in ascending sorted order (guaranteed by `BTreeMap`).
    ///
    /// Useful for debugging, testing, and future range-scan support.
    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.index.keys().map(|k| k.as_slice())
    }
}

// #[cfg(test)]
// #[path ="reader_tests.rs"]
// mod tests;
