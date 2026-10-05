//! Local runtime endpoint: directory, socket path, endpoint file, and
//! stale-instance detection (spec §37).
//!
//! The runtime directory is user-only (0o700). The endpoint file carries
//! connection metadata only — never secrets. A live socket behind a stale
//! endpoint file means another gateway owns the runtime; a dead socket
//! means the previous owner is gone and its files may be replaced.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tachyon_protocol::PROTOCOL_VERSION;
use tachyon_types::Timestamp;
use thiserror::Error;

use crate::transport::connect;

/// Errors from endpoint claim/setup.
#[derive(Debug, Error)]
pub enum EndpointError {
    /// Filesystem failure.
    #[error("endpoint I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Endpoint file is not valid JSON.
    #[error("endpoint file is corrupt: {0}")]
    Corrupt(#[from] serde_json::Error),
    /// Another gateway answered on the recorded socket.
    #[error("gateway already running (pid {pid})")]
    AlreadyRunning {
        /// Pid recorded by the live owner.
        pid: u32,
    },
}

/// Connection metadata for one running gateway.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EndpointInfo {
    /// Socket clients connect to.
    pub socket_path: PathBuf,
    /// Pid of the owning process.
    pub pid: u32,
    /// When the owner started (micros since epoch).
    pub started_at_micros: i64,
    /// Wire protocol version the owner speaks.
    pub protocol_version: u16,
}

/// Paths claimed for one gateway instance.
#[derive(Clone, Debug)]
pub struct ClaimPaths {
    /// Directory holding socket, endpoint file, and state.
    pub dir: PathBuf,
    /// Unix socket path.
    pub socket: PathBuf,
    /// Endpoint metadata file.
    pub endpoint_file: PathBuf,
}

/// True when `pid` likely names a live process.
///
/// Unix checks `/proc/<pid>` plus the current process; Windows is
/// conservative and treats any recorded pid as live so a stale
/// socket probe alone never steals a live owner's directory.
fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    if pid == std::process::id() {
        return true;
    }
    #[cfg(unix)]
    {
        Path::new(&format!("/proc/{pid}")).exists()
    }
    #[cfg(windows)]
    {
        true
    }
}

/// Moves a corrupt endpoint file aside so the dir can rebind.
/// Keeps the bytes for forensics; never deletes a live socket.
fn quarantine_corrupt(endpoint_file: &Path) {
    let micros = Timestamp::now().as_micros();
    let name = format!("gateway.corrupt.{micros}.json", micros = micros.max(0));
    let dest = endpoint_file.with_file_name(name);
    let _ = std::fs::rename(endpoint_file, dest);
}

/// Ensures the runtime dir exists with user-only permissions and evicts a
/// stale previous owner. Fails with [`EndpointError::AlreadyRunning`] when
/// a live gateway answers or the recorded pid is still alive.
///
/// A corrupt/truncated endpoint file is quarantined (renamed aside) and
/// the dir rebinds unless a live socket answers, which means another
/// gateway owns the runtime right now.
pub async fn claim_runtime_dir(data_dir: &Path) -> Result<ClaimPaths, EndpointError> {
    std::fs::create_dir_all(data_dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(data_dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let paths = ClaimPaths {
        dir: data_dir.to_owned(),
        socket: data_dir.join("gateway.sock"),
        endpoint_file: data_dir.join("gateway.json"),
    };
    if paths.endpoint_file.exists() {
        match read_endpoint(&paths.endpoint_file) {
            Ok(info) => {
                if probe_socket(&live_address(&paths)).await {
                    return Err(EndpointError::AlreadyRunning { pid: info.pid });
                }
                if pid_alive(info.pid) {
                    return Err(EndpointError::AlreadyRunning { pid: info.pid });
                }
                let _ = std::fs::remove_file(&paths.socket);
                let _ = std::fs::remove_file(&paths.endpoint_file);
            }
            Err(EndpointError::Corrupt(_)) => {
                if probe_socket(&live_address(&paths)).await {
                    return Err(EndpointError::AlreadyRunning { pid: 0 });
                }
                quarantine_corrupt(&paths.endpoint_file);
                let _ = std::fs::remove_file(&paths.socket);
            }
            Err(EndpointError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                // Raced with another claimer deleting it: treat as missing.
            }
            Err(other) => return Err(other),
        }
    }
    if paths.socket.exists() {
        if probe_socket(&live_address(&paths)).await {
            return Err(EndpointError::AlreadyRunning { pid: 0 });
        }
        // Socket file without endpoint metadata: leftover of a crash.
        let _ = std::fs::remove_file(&paths.socket);
    }
    Ok(paths)
}

/// Address a live owner listens on: the socket file on Unix, the derived
/// pipe name on Windows.
fn live_address(paths: &ClaimPaths) -> PathBuf {
    #[cfg(unix)]
    {
        paths.socket.clone()
    }
    #[cfg(windows)]
    {
        crate::transport::pipe_name_for(&paths.dir)
    }
}

/// Writes endpoint metadata for the bound `address` (socket path on Unix,
/// pipe name on Windows) after a successful bind.
///
/// Atomic publish: bytes go to a `0600` temp file in the same directory
/// and are renamed over `gateway.json`, so readers never see a
/// truncated file. The runtime dir is `0700` before any write.
pub fn write_endpoint(paths: &ClaimPaths, address: &Path) -> Result<EndpointInfo, EndpointError> {
    let info = EndpointInfo {
        socket_path: address.to_owned(),
        pid: std::process::id(),
        started_at_micros: Timestamp::now().as_micros(),
        protocol_version: PROTOCOL_VERSION,
    };
    let bytes = serde_json::to_vec_pretty(&info)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&paths.dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let tmp = paths
        .endpoint_file
        .with_file_name(format!("gateway.json.tmp.{}", std::process::id()));
    {
        use std::io::Write as _;
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o600)
                .open(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all().map_err(EndpointError::Io)?;
        }
        #[cfg(not(unix))]
        {
            std::fs::write(&tmp, &bytes)?;
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, &paths.endpoint_file)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&paths.endpoint_file, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(info)
}

/// Removes socket and endpoint files. Best-effort; missing files are fine.
pub fn release_runtime_dir(paths: &ClaimPaths) {
    let _ = std::fs::remove_file(&paths.socket);
    let _ = std::fs::remove_file(&paths.endpoint_file);
}

fn read_endpoint(path: &Path) -> Result<EndpointInfo, EndpointError> {
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(EndpointError::from)
}

/// Reads endpoint metadata written by a running gateway.
pub fn read_endpoint_info(path: &Path) -> Result<EndpointInfo, EndpointError> {
    read_endpoint(path)
}

/// True when something accepts connections on `address` (socket or pipe).
async fn probe_socket(address: &Path) -> bool {
    tokio::time::timeout(std::time::Duration::from_secs(2), connect(address))
        .await
        .is_ok_and(|result| result.is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "t25-{}-{}-{}",
            tag,
            std::process::id(),
            tachyon_types::EventId::generate()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Missing endpoint + missing socket claims cleanly.
    #[tokio::test]
    async fn missing_endpoint_claims_cleanly() {
        let dir = fresh_dir("missing");
        let paths = claim_runtime_dir(&dir).await.expect("missing claims");
        assert_eq!(paths.dir, dir);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Truncated endpoint is quarantined, never returned as corrupt,
    /// and the dir rebinds when no live socket answers.
    #[tokio::test]
    async fn truncated_endpoint_is_quarantined_and_rebinds() {
        let dir = fresh_dir("trunc");
        std::fs::create_dir_all(&dir).expect("mkdir");
        let endpoint = dir.join("gateway.json");
        std::fs::write(&endpoint, b"{truncated").expect("writes trunc");
        let paths = claim_runtime_dir(&dir)
            .await
            .expect("truncated quarantines and claims");
        assert_eq!(paths.dir, dir);
        assert!(!endpoint.exists(), "truncated file is moved away, not left");
        let quarantined = std::fs::read_dir(&dir)
            .expect("reads dir")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .any(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.contains("corrupt"))
            });
        assert!(quarantined, "a quarantine file remains");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Two concurrent claimers on an empty dir leave a consistent
    /// state: neither panics and the dir is claimable afterwards.
    #[tokio::test]
    async fn two_concurrent_claimers_leave_consistent_state() {
        let dir = fresh_dir("race");
        let (first, second) = tokio::join!(claim_runtime_dir(&dir), claim_runtime_dir(&dir));
        assert!(
            first.is_ok() || second.is_ok(),
            "at least one claimer wins the empty dir"
        );
        let again = claim_runtime_dir(&dir).await;
        assert!(
            again.is_ok(),
            "the dir stays claimable after the race: {again:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Stale pid (dead owner, dead socket) rebinds; a live pid is
    /// never stolen even when the socket probe is dead.
    #[tokio::test]
    async fn stale_pid_rebinds_but_live_pid_is_never_stolen() {
        // Stale: pid that cannot be alive on this host.
        let stale_dir = fresh_dir("stale");
        std::fs::create_dir_all(&stale_dir).expect("mkdir");
        let stale_info = EndpointInfo {
            socket_path: stale_dir.join("gateway.sock"),
            pid: 999_999_999,
            started_at_micros: Timestamp::now().as_micros(),
            protocol_version: PROTOCOL_VERSION,
        };
        std::fs::write(
            stale_dir.join("gateway.json"),
            serde_json::to_vec_pretty(&stale_info).expect("json"),
        )
        .expect("writes stale");
        claim_runtime_dir(&stale_dir)
            .await
            .expect("dead owner rebinds");
        let _ = std::fs::remove_dir_all(&stale_dir);

        // Live: our own pid is alive, so the dir must be refused
        // even though no socket answers.
        let live_dir = fresh_dir("live");
        std::fs::create_dir_all(&live_dir).expect("mkdir");
        let live_info = EndpointInfo {
            socket_path: live_dir.join("gateway.sock"),
            pid: std::process::id(),
            started_at_micros: Timestamp::now().as_micros(),
            protocol_version: PROTOCOL_VERSION,
        };
        std::fs::write(
            live_dir.join("gateway.json"),
            serde_json::to_vec_pretty(&live_info).expect("json"),
        )
        .expect("writes live");
        let err = claim_runtime_dir(&live_dir)
            .await
            .expect_err("live pid is never stolen");
        assert!(
            matches!(err, EndpointError::AlreadyRunning { .. }),
            "live pid maps to AlreadyRunning: {err:?}"
        );
        let _ = std::fs::remove_dir_all(&live_dir);
    }
}
