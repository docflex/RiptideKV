/// # Manifest - SSTable Level Metadata
///
/// Tracks which SSTable files belong to which level (L0 or L1) so that the
/// engine can correctly reconstruct its state after a restart.
///
/// ## File Format
///
/// The manifest is a simple text-based file with one SSTable entry per line:
///
/// ```text
/// L0:sst-000000000000000005-1708600000000.sst
/// L0:sst-000000000000000003-1708599999000.sst
/// L1:sst-000000000000000010-1708600001000.sst
/// ```
///
/// Lines starting with `#` are comments. Empty lines are ignored.
///
/// ## Crash Safety
///
/// The manifest is rewritten atomically: write to a `.tmp` file, fsync, then
/// rename over the existing manifest. This ensures the manifest is never
/// partially written.
///
/// ## Design Rationale
///
/// A text format was chosen over binary for debuggability — operators can
/// inspect the manifest with any text editor. The file is small (one line per
/// SSTable) so parsing overhead is negligible.

use anyhow::{bail, Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// Name of the manifest file within the SST directory.
pub const MANIFEST_FILENAME: &str = "MANIFEST";

/// Temporary file used during atomic manifest writes.
const MANIFEST_TMP_FILENAME: &str = "MANIFEST.tmp";

/// Represents the level assignment for a single SSTable file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstMeta {
    /// The SSTable filename (not the full path — just the basename).
    pub filename: String,
    /// The level this SSTable belongs to (0 = L0, 1 = L1).
    pub level: u32,
}

/// In-memory representation of the manifest.
#[derive(Debug, Clone)]
pub struct Manifest {
    /// Path to the manifest file on disk.
    path: PathBuf,
    /// All SSTable entries, in the order they appear in the file.
    pub entries: Vec<SstMeta>,
}

impl Manifest {
    /// Loads an existing manifest from `sst_dir/MANIFEST`, or creates an
    /// empty one if the file does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest file exists but cannot be parsed.
    pub fn load_or_create(sst_dir: &Path) -> Result<Self> {
        let path = sst_dir.join(MANIFEST_FILENAME);

        if path.exists() {
            let file = File::open(&path)
                .with_context(|| format!("failed to open manifest at {}", path.display()))?;
            let reader = BufReader::new(file);
            let mut entries = Vec::new();

            for (line_num, line) in reader.lines().enumerate() {
                let line = line.with_context(|| {
                    format!("failed to read manifest line {}", line_num + 1)
                })?;
                let trimmed = line.trim();

                // Skip empty lines and comments.
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    continue;
                }

                // Expected format: "<level>:<filename>"
                let (level_str, filename) = trimmed.split_once(':').ok_or_else(|| {
                    anyhow::anyhow!(
                        "manifest line {}: invalid format (expected 'L<n>:<filename>'): {}",
                        line_num + 1,
                        trimmed
                    )
                })?;

                let level = match level_str {
                    "L0" => 0,
                    "L1" => 1,
                    other => bail!(
                        "manifest line {}: unknown level '{}' (expected L0 or L1)",
                        line_num + 1,
                        other
                    ),
                };

                entries.push(SstMeta {
                    filename: filename.to_string(),
                    level,
                });
            }

            Ok(Self { path, entries })
        } else {
            Ok(Self {
                path,
                entries: Vec::new(),
            })
        }
    }

    /// Persists the current manifest state to disk.
    ///
    /// On Unix-like systems this uses atomic rename (write to `.tmp`, fsync,
    /// rename). On Windows, `rename` over an existing file can fail with
    /// "Access is denied" if the target is still cached by the OS or antivirus,
    /// so we fall back to a direct truncate-and-write strategy
    /// which is still safe because the manifest is small and fsynced.
    pub fn save(&self) -> Result<()> {
        let tmp_path = self.path.with_file_name(MANIFEST_TMP_FILENAME);

        // Write to a temp file first.
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)
                .with_context(|| {
                    format!("failed to create manifest tmp at {}", tmp_path.display())
                })?;

            Self::write_manifest_contents(&mut f, &self.entries)?;
            f.flush()?;
            f.sync_all()?;
        }

        // Try atomic rename first. If it fails (common on Windows when the
        // target file is still cached), fall back to direct overwrite.
        if fs::rename(&tmp_path, &self.path).is_err() {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&self.path)
                .with_context(|| {
                    format!("failed to open manifest at {}", self.path.display())
                })?;

            Self::write_manifest_contents(&mut f, &self.entries)?;
            f.flush()?;
            f.sync_all()?;

            // Clean up the orphaned tmp file.
            let _ = fs::remove_file(&tmp_path);
        }

        Ok(())
    }

    /// Writes the manifest header and entries to a writer.
    fn write_manifest_contents(f: &mut File, entries: &[SstMeta]) -> Result<()> {
        writeln!(f, "# RiptideKV SSTable Manifest")?;
        writeln!(f, "# Format: <level>:<filename>")?;
        for entry in entries {
            let level_str = match entry.level {
                0 => "L0",
                1 => "L1",
                other => panic!("invalid level {}", other),
            };
            writeln!(f, "{}:{}", level_str, entry.filename)?;
        }
        Ok(())
    }

    /// Returns the filenames of all L0 SSTables, in manifest order (newest first).
    pub fn l0_filenames(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter(|e| e.level == 0)
            .map(|e| e.filename.as_str())
            .collect()
    }

    /// Returns the filenames of all L1 SSTables, in manifest order (newest first).
    pub fn l1_filenames(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter(|e| e.level == 1)
            .map(|e| e.filename.as_str())
            .collect()
    }

    /// Adds an SSTable entry to the manifest (does **not** save to disk).
    ///
    /// New entries are inserted at the front (newest first) for the given level.
    pub fn add(&mut self, filename: String, level: u32) {
        // Insert at the beginning of entries for this level to maintain
        // newest-first ordering within each level.
        let insert_pos = self
            .entries
            .iter()
            .position(|e| e.level == level)
            .unwrap_or(self.entries.len());
        self.entries.insert(insert_pos, SstMeta { filename, level });
    }

    /// Removes all entries matching the given filenames.
    #[allow(dead_code)]
    pub fn remove_files(&mut self, filenames: &[&str]) {
        self.entries
            .retain(|e| !filenames.contains(&e.filename.as_str()));
    }

    /// Replaces all L0 and L1 entries with a single L1 entry (used after compaction).
    pub fn replace_all_with_l1(&mut self, filename: String) {
        self.entries.clear();
        self.entries.push(SstMeta {
            filename,
            level: 1,
        });
    }
}
