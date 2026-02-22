use crate::*;
use tempfile::tempdir;
use std::fs;

use manifest::MANIFEST_FILENAME;

#[test]
fn create_empty_manifest() -> Result<()> {
    let dir = tempdir()?;
    let m = Manifest::load_or_create(dir.path())?;
    assert!(m.entries.is_empty());
    assert!(m.l0_filenames().is_empty());
    assert!(m.l1_filenames().is_empty());
    Ok(())
}

#[test]
fn save_and_reload() -> Result<()> {
    let dir = tempdir()?;
    let mut m = Manifest::load_or_create(dir.path())?;
    m.add("sst-001.sst".to_string(), 0);
    m.add("sst-002.sst".to_string(), 0);
    m.add("sst-003.sst".to_string(), 1);
    m.save()?;

    let m2 = Manifest::load_or_create(dir.path())?;
    assert_eq!(m2.l0_filenames(), vec!["sst-002.sst", "sst-001.sst"]);
    assert_eq!(m2.l1_filenames(), vec!["sst-003.sst"]);
    Ok(())
}

#[test]
fn remove_files() -> Result<()> {
    let dir = tempdir()?;
    let mut m = Manifest::load_or_create(dir.path())?;
    m.add("a.sst".to_string(), 0);
    m.add("b.sst".to_string(), 0);
    m.add("c.sst".to_string(), 1);
    m.remove_files(&["a.sst", "c.sst"]);
    assert_eq!(m.entries.len(), 1);
    assert_eq!(m.entries[0].filename, "b.sst");
    Ok(())
}

#[test]
fn replace_all_with_l1() -> Result<()> {
    let dir = tempdir()?;
    let mut m = Manifest::load_or_create(dir.path())?;
    m.add("old1.sst".to_string(), 0);
    m.add("old2.sst".to_string(), 1);
    m.replace_all_with_l1("merged.sst".to_string());
    assert_eq!(m.entries.len(), 1);
    assert_eq!(m.entries[0].filename, "merged.sst");
    assert_eq!(m.entries[0].level, 1);
    Ok(())
}

#[test]
fn comments_and_blank_lines_ignored() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join(MANIFEST_FILENAME);
    fs::write(
        &path,
        "# comment\n\nL0:a.sst\n\n# another comment\nL1:b.sst\n",
    )?;
    let m = Manifest::load_or_create(dir.path())?;
    assert_eq!(m.l0_filenames(), vec!["a.sst"]);
    assert_eq!(m.l1_filenames(), vec!["b.sst"]);
    Ok(())
}

#[test]
fn invalid_format_returns_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join(MANIFEST_FILENAME);
    fs::write(&path, "bad-line-no-colon\n").unwrap();
    let result = Manifest::load_or_create(dir.path());
    assert!(result.is_err());
}

#[test]
fn unknown_level_returns_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join(MANIFEST_FILENAME);
    fs::write(&path, "L9:file.sst\n").unwrap();
    let result = Manifest::load_or_create(dir.path());
    assert!(result.is_err());
}