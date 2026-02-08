use anyhow::Result;
use memtable::Memtable;
use sstable::SsTableReader;
use sstable::SsTableWriter;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use wal::{WalReader, WalRecord, WalWriter};

pub fn replay_wal_and_build(path: &str, mem: &mut Memtable) -> Result<u64> {
    if let Ok(mut reader) = WalReader::open(path) {
        let mut max_seq = 0u64;
        reader.replay(|r| match r {
            WalRecord::Put { seq, key, value } => {
                mem.put(key, value, seq);
                max_seq = max_seq.max(seq);
            }
            WalRecord::Del { seq, key } => {
                mem.delete(key, seq);
                max_seq = max_seq.max(seq);
            }
        })?;
        Ok(max_seq)
    } else {
        Ok(0)
    }
}
pub struct Engine {
    mem: Memtable,
    sstables: Vec<SsTableReader>,
    wal_path: PathBuf,
    sst_dir: PathBuf,
    wal_writer: WalWriter,
    pub seq: u64,
    pub flush_threshold: usize,
    /// fsync WAL for durability (true means sync on every append)
    pub wal_sync: bool,
}

impl Engine {
    /// Create engine; replay WAL into memtable and return engine.
    pub fn new<P: AsRef<Path>>(
        wal_path: P,
        sst_dir: P,
        flush_threshold: usize,
        wal_sync: bool,
    ) -> Result<Self> {
        let wal_path = wal_path.as_ref().to_path_buf();
        let sst_dir = sst_dir.as_ref().to_path_buf();

        // ensure sst dir exists
        std::fs::create_dir_all(&sst_dir)?;

        // create/open wal writer (we will use create to append)
        let wal_writer = WalWriter::create(&wal_path, wal_sync)?;

        // replay wal into memtable and obtain last seq
        let mut mem = Memtable::new();
        let seq = replay_wal_and_build(wal_path.to_str().unwrap(), &mut mem)?;

        let mut sstables = Vec::new();

        let mut paths: Vec<_> = std::fs::read_dir(&sst_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "sst").unwrap_or(false))
            .collect();

        // newest first (filename contains seq + timestamp)
        paths.sort();
        paths.reverse();

        for path in paths {
            sstables.push(SsTableReader::open(&path)?);
        }

        Ok(Self {
            mem,
            sstables,
            wal_path,
            sst_dir,
            wal_writer,
            seq,
            flush_threshold,
            wal_sync,
        })
    }

    /// SET equivalent: record in WAL then update memtable. May flush.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.seq = self.seq.saturating_add(1);
        let seq = self.seq;
        // Append to WAL first
        self.wal_writer.append(&WalRecord::Put {
            seq,
            key: key.clone(),
            value: value.clone(),
        })?;
        // Apply to memtable
        self.mem.put(key, value, seq);

        // Maybe flush memtable to SSTable
        if self.mem.approx_size() >= self.flush_threshold {
            self.flush()?;
        }

        Ok(())
    }

    /// GET equivalent: only checks memtable for now (we'll add SSTable reads later)
    pub fn get(&self, key: &[u8]) -> Option<(u64, Vec<u8>)> {
        // 1. Check memtable FIRST (and respect tombstones)
        if let Some(entry) = self.mem.get_entry(key) {
            return entry.value.as_ref().map(|v| (entry.seq, v.clone()));
        }

        // 2. Check SSTables (newest → oldest)
        for sst in &self.sstables {
            match sst.get(key) {
                Ok(Some(entry)) => {
                    return match entry.value {
                        Some(v) => Some((entry.seq, v)),
                        None => None, // tombstone hides older values
                    };
                }
                Ok(None) => continue,
                Err(_) => continue,
            }
        }

        // 3. Not found anywhere
        None
    }

    /// DEL equivalent: append tombstone then apply. May flush.
    pub fn del(&mut self, key: Vec<u8>) -> Result<()> {
        self.seq = self.seq.saturating_add(1);
        let seq = self.seq;

        self.wal_writer.append(&WalRecord::Del {
            seq,
            key: key.clone(),
        })?;
        self.mem.delete(key, seq);

        if self.mem.approx_size() >= self.flush_threshold {
            self.flush()?;
        }

        Ok(())
    }

    /// Flush current memtable to an SSTable, atomically, then reset memtable and truncate WAL.
    /// Simple approach:
    /// 1. Pick sst_path = sst_dir / format!("sst-{}.sst", seq)
    /// 2. Call SsTableWriter::write_from_memtable(sst_path, &mem)
    /// 3. On success, truncate wal.log to zero length (safe because SSTable contains all entries)
    /// 4. Replace memtable with a fresh one
    fn flush(&mut self) -> Result<()> {
        // choose filename using current seq and timestamp so it's monotonic
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let sst_name = format!("sst-{}-{}.sst", self.seq, ts);
        let sst_path = self.sst_dir.join(&sst_name);

        // write sstable (this writes to temp and rename inside)
        SsTableWriter::write_from_memtable(&sst_path, &self.mem)?;

        // Successfully wrote SSTable; now safely truncate the WAL
        // We ensure the SSTable is fsynced by the writer; now we can truncate wal.log
        // Truncate by opening with truncate(true)
        let mut _f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.wal_path)?;
        // (dropping _f closes it)

        // create a fresh WalWriter (append mode)
        self.wal_writer = WalWriter::create(&self.wal_path, self.wal_sync)?;

        // reset memtable
        self.mem = Memtable::new();

        let reader = SsTableReader::open(&sst_path)?;
        self.sstables.insert(0, reader);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*; // brings Engine, replay_wal_and_build, etc.
    use anyhow::Result;
    use std::fs;
    use tempfile::tempdir;

    // Small integration-style test: create engine with tiny threshold, call set and verify:
    // - an .sst file appears in sst_dir
    // - wal file is truncated to zero-length
    #[test]
    fn flush_writes_sstable_and_truncates_wal() -> Result<()> {
        // create a temporary directory for WAL + SST storage
        let dir = tempdir()?;
        let wal_path = dir.path().join("wal.log");
        let sst_dir = dir.path().join("sst");

        // Build engine with tiny flush threshold (1 byte) and force wal sync = true
        let mut engine = Engine::new(&wal_path, &sst_dir, 1, true)?;

        // SET a key; since threshold is 1, this should trigger flush inline
        engine.set(b"key1".to_vec(), b"value1".to_vec())?;

        // Ensure sst_dir exists and has at least one .sst file
        let entries = fs::read_dir(&sst_dir)?
            .filter_map(|e| e.ok())
            .collect::<Vec<_>>();

        // There must be at least one .sst file in the directory
        let found_sst = entries.iter().any(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|ext| ext == "sst")
                .unwrap_or(false)
        });
        assert!(
            found_sst,
            "expected at least one .sst file in {:?}",
            sst_dir
        );

        // WAL should be truncated to zero bytes after successful flush
        let wal_meta = fs::metadata(&wal_path)?;
        assert_eq!(wal_meta.len(), 0, "expected wal to be truncated to 0 bytes");

        Ok(())
    }

    #[test]
    fn flush_triggers_at_4mb_threshold() -> anyhow::Result<()> {
        use std::fs;
        use tempfile::tempdir;

        // Arrange
        let dir = tempdir()?;
        let wal_path = dir.path().join("wal.log");
        let sst_dir = dir.path().join("sst");

        // 4 MB threshold
        let threshold = 4 * 1024 * 1024;

        let mut engine = Engine::new(&wal_path, &sst_dir, threshold, false)?;

        // Each value is 1 KB
        let value = vec![b'x'; 1024];

        // Act
        // Write slightly more than 4 MB
        let writes = (threshold / value.len()) + 10;
        for i in 0..writes {
            let key = format!("key{}", i).into_bytes();
            engine.set(key, value.clone())?;
        }

        // Assert: SSTable directory exists
        assert!(sst_dir.exists());

        let entries: Vec<_> = fs::read_dir(&sst_dir)?.filter_map(|e| e.ok()).collect();

        // Exactly one SSTable should exist
        let sst_files: Vec<_> = entries
            .iter()
            .filter(|e| e.path().extension().map(|e| e == "sst").unwrap_or(false))
            .collect();

        assert_eq!(
            sst_files.len(),
            1,
            "expected exactly one SSTable after crossing threshold"
        );

        // WAL should be truncated after flush
        let wal_meta = fs::metadata(&wal_path)?;
        assert!(
            wal_meta.len() < threshold as u64,
            "WAL should only contain post-flush entries"
        );

        Ok(())
    }
}
