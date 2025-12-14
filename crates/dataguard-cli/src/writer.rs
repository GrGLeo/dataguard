use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

pub fn resolve_file_path(path: &Option<String>, timestamp: &str) -> Result<PathBuf> {
    let base_path = path.as_deref().unwrap_or(".");
    let path = Path::new(base_path);
    let filename = format!("validation_{}.json", timestamp);

    let output_path = if path.exists() {
        if path.is_dir() {
            path.join(&filename)
        } else {
            path.to_path_buf()
        }
    } else {
        let path_str = base_path;
        if path_str.ends_with('/') || path_str.ends_with('\\') {
            fs::create_dir_all(path)
                .with_context(|| format!("Failed to create directory: {}", path.display()))?;
            path.join(filename)
        } else {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() && !parent.exists() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!("Failed to create directory: {}", path.display())
                    })?;
                }
            }
            path.to_path_buf()
        }
    };
    Ok(output_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;
    #[test]
    fn test_resolve_file_path_no_argument_uses_current_dir() {
        let result = resolve_file_path(&None, "20251214-153045").unwrap();
        assert_eq!(
            result.file_name().unwrap(),
            "validation_20251214-153045.json"
        );
        assert!(result.starts_with("."));
    }
    #[test]
    fn test_resolve_file_path_existing_directory() {
        let temp_dir = TempDir::new().unwrap();
        let path_str = temp_dir.path().to_str().unwrap().to_string();

        let result = resolve_file_path(&Some(path_str), "20251214-153045").unwrap();

        assert_eq!(
            result.file_name().unwrap(),
            "validation_20251214-153045.json"
        );
        assert!(result.starts_with(temp_dir.path()));
    }
    #[test]
    fn test_resolve_file_path_existing_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("existing.json");
        fs::write(&file_path, "test").unwrap();

        let path_str = file_path.to_str().unwrap().to_string();
        let result = resolve_file_path(&Some(path_str.clone()), "20251214-153045").unwrap();

        assert_eq!(result, file_path);
    }
    #[test]
    fn test_resolve_file_path_nonexistent_dir_with_trailing_slash() {
        let temp_dir = TempDir::new().unwrap();
        let new_dir = temp_dir.path().join("new_dir/");
        let path_str = new_dir.to_str().unwrap().to_string();

        let result = resolve_file_path(&Some(path_str), "20251214-153045").unwrap();

        assert!(new_dir.exists());
        assert!(new_dir.is_dir());
        assert_eq!(
            result.file_name().unwrap(),
            "validation_20251214-153045.json"
        );
    }
    #[test]
    fn test_resolve_file_path_nonexistent_file_creates_parent() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("subdir/report.json");
        let path_str = file_path.to_str().unwrap().to_string();

        let result = resolve_file_path(&Some(path_str.clone()), "20251214-153045").unwrap();

        assert!(file_path.parent().unwrap().exists());
        assert_eq!(result, file_path);
    }
    #[test]
    fn test_resolve_file_path_dot_path() {
        let result = resolve_file_path(&Some(".".to_string()), "20251214-153045").unwrap();

        assert_eq!(
            result.file_name().unwrap(),
            "validation_20251214-153045.json"
        );
    }
}
