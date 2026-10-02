//! Data-dir resolution for the adapter, mirroring `tachyon-app`'s
//! documented config precedence (env > config file > platform
//! default) so a `data_dir` set in the config file is honored by the
//! adapter exactly as the CLI gateway honors it.
//!
//! `tachyon-app` is a binary crate the adapter cannot depend on, so
//! the SAME config source is parsed in-crate with `serde_json`: one
//! `FileConfig`-shaped read of `TACHYON_CONFIG` (else the platform
//! default `$XDG_CONFIG_HOME/tachyon/config.json`, else
//! `~/.config/tachyon/config.json`) extracting only `data_dir`.
//! Unknown JSON fields are ignored, exactly like `tachyon-app`'s
//! `FileConfig`. The shared-source duplication is a logged decision
//! (see `docs/agents/auto-workflow/decisions.md`).

use std::path::PathBuf;

/// Resolves the data dir holding `gateway.json`, with the same
/// precedence `tachyon-app` applies for `Config::data_dir` minus the
/// CLI layer (the adapter has no flags): `TACHYON_DATA_DIR` if set,
/// else the config file's `data_dir`, else the platform default.
#[must_use]
pub fn resolve_data_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("TACHYON_DATA_DIR") {
        return PathBuf::from(dir);
    }
    config_file_data_dir().unwrap_or_else(default_data_dir)
}

/// The `data_dir` recorded in the config file `tachyon-app` would
/// load: `TACHYON_CONFIG` if set, else the platform default path.
/// `None` when no file exists (silent for the default path — a
/// missing default config is the normal case; warned when
/// `TACHYON_CONFIG` names it explicitly, where `tachyon-app` would
/// fail its own load) or when it cannot be read/parsed (warned). The
/// adapter warns and falls through instead of failing its own load: it
/// must keep serving typed JSON-RPC errors rather than die before
/// answering `initialize`.
fn config_file_data_dir() -> Option<PathBuf> {
    let path = config_file_path()?;
    match std::fs::read(&path) {
        Ok(bytes) => {
            let data_dir = file_data_dir(&bytes);
            if data_dir.is_none() {
                tracing::warn!(
                    path = %path.display(),
                    "config file carries no usable data_dir; falling back to the platform default"
                );
            }
            data_dir
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if std::env::var_os("TACHYON_CONFIG").is_some() {
                tracing::warn!(
                    path = %path.display(),
                    "TACHYON_CONFIG names a config file that does not exist; falling back to the platform default data dir"
                );
            }
            None
        }
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                %error,
                "cannot read the config file; falling back to the platform default data dir"
            );
            None
        }
    }
}

/// The config file path: `TACHYON_CONFIG` (the adapter's only config
/// knob — no `--config` flag), else the platform default path.
fn config_file_path() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("TACHYON_CONFIG").map(PathBuf::from) {
        return Some(path);
    }
    default_config_path()
}

/// Platform config directory: `$XDG_CONFIG_HOME`, else `~/.config`
/// (mirrors `tachyon-app`'s `default_config_path`).
fn default_config_path() -> Option<PathBuf> {
    config_base_dir().map(|base| base.join("tachyon").join("config.json"))
}

fn config_base_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from) {
        return Some(dir);
    }
    home_dir().map(|home| home.join(".config"))
}

/// The `data_dir` field of one config-file body, ignoring every other
/// field (same lenient parse `tachyon-app`'s `FileConfig` applies).
/// `None` when the bytes are not a JSON object or carry no `data_dir`.
#[must_use]
pub fn file_data_dir(bytes: &[u8]) -> Option<PathBuf> {
    #[derive(serde::Deserialize)]
    struct FileConfig {
        data_dir: Option<PathBuf>,
    }
    serde_json::from_slice::<FileConfig>(bytes)
        .ok()
        .and_then(|file| file.data_dir)
}

/// Platform data directory: `$XDG_DATA_HOME/tachyon`, else
/// `~/.local/share/tachyon` (byte-for-byte `tachyon-app`'s
/// `default_data_dir`, including the relative fallback).
#[must_use]
pub fn default_data_dir() -> PathBuf {
    if let Some(base) = std::env::var_os("XDG_DATA_HOME") {
        return PathBuf::from(base).join("tachyon");
    }
    home_dir().map_or_else(
        || PathBuf::from(".tachyon-data"),
        |home| home.join(".local").join("share").join("tachyon"),
    )
}

#[cfg(unix)]
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

#[cfg(windows)]
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("USERPROFILE").map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::file_data_dir;

    /// The config file is the SAME JSON source `tachyon-app` reads:
    /// `data_dir` is extracted, unknown fields are ignored (the CLI's
    /// `FileConfig` is lenient), and anything that is not a JSON
    /// object carrying a `data_dir` yields `None` (fall through to the
    /// platform default).
    #[test]
    fn config_file_data_dir_parses_the_cli_config_source() {
        assert_eq!(
            file_data_dir(br#"{"data_dir": "/from-config"}"#),
            Some(std::path::PathBuf::from("/from-config"))
        );
        assert_eq!(
            file_data_dir(br#"{"log_level": "info", "data_dir": "/x", "evidence_grace_ms": 10}"#),
            Some(std::path::PathBuf::from("/x")),
            "sibling fields never shadow data_dir"
        );
        assert_eq!(file_data_dir(br#"{"log_level": "info"}"#), None);
        assert_eq!(file_data_dir(b"not json at all"), None);
        assert_eq!(file_data_dir(b"[1, 2, 3]"), None);
        assert_eq!(file_data_dir(br#"{"data_dir": 42}"#), None);
    }
}
