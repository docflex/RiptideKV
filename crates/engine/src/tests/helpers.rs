use std::fs;
use std::path::Path;

pub fn count_sst_files(dir: &Path) -> usize {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|ext| ext == "sst")
                .unwrap_or(false)
        })
        .count()
}
