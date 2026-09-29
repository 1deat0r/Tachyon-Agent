//! Content-addressed artifact store (spec §21).
//!
//! Layout: `$root/<first-two-hash-chars>/<blake3>`. Payloads above
//! [`COMPRESSION_THRESHOLD`] are zstd-compressed on disk (an optimization,
//! not an invariant — [`fetch`] detects the framing). Large stdout/model
//! blobs live here, never in event rows. Metadata in SQLite arrives with
//! the store milestone; the on-disk layout is already final.

use crate::ToolError;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tachyon_types::ArtifactId;

/// Payloads at or above this size are compressed. Spec candidate: 64 KiB.
pub const COMPRESSION_THRESHOLD: usize = 64 * 1024;

/// Largest zstd window log accepted by the decoder API.
const MAX_DECODER_WINDOW_LOG: u32 = 31;

/// Default bound for general fetches that do not provide a durable receipt.
pub const DEFAULT_FETCH_LIMIT: usize = 64 * 1024 * 1024;

static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

/// Magic prefix marking a zstd-compressed artifact payload.
const COMPRESSED_MAGIC: &[u8] = b"TACHYON-ZSTD1\n";

/// Content-addressed spool rooted at `$data/artifacts`.
#[derive(Clone, Debug)]
pub struct ArtifactSpool {
    root: PathBuf,
}

impl ArtifactSpool {
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn path_for(&self, id: &ArtifactId) -> PathBuf {
        self.root.join(&id.0[..2.min(id.0.len())]).join(&id.0)
    }

    fn validate_id(id: &ArtifactId) -> Result<(), ToolError> {
        if id.0.len() != 64
            || !id
                .0
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(ToolError::InvalidArgs(
                "artifact id must be 64 lowercase hexadecimal characters".to_owned(),
            ));
        }
        Ok(())
    }

    /// Stores `bytes`, returning its BLAKE3 id. Idempotent: identical
    /// bytes hash identically and short-circuit the write.
    pub fn store(&self, bytes: &[u8]) -> Result<ArtifactId, ToolError> {
        let id = ArtifactId(blake3::hash(bytes).to_hex().to_string());
        let path = self.path_for(&id);
        let parent = path
            .parent()
            .ok_or_else(|| ToolError::InvalidArgs("artifact path has no parent".to_owned()))?;
        ensure_private_directory(&self.root)?;
        ensure_private_directory(parent)?;
        sync_directory(&self.root)?;
        if self.fetch_verified(&id, bytes.len(), bytes.len()).is_ok() {
            set_private_file_permissions(&path)?;
            sync_existing_file(&path)?;
            sync_directory(parent)?;
            return Ok(id);
        }

        let payload = if bytes.len() >= COMPRESSION_THRESHOLD {
            let mut framed = COMPRESSED_MAGIC.to_vec();
            framed.extend_from_slice(&zstd::encode_all(bytes, 3).map_err(|error| {
                ToolError::InvalidArgs(format!("zstd compress failed: {error}"))
            })?);
            framed
        } else {
            bytes.to_vec()
        };
        let (tmp, mut file) = create_temp_file(parent, &id)?;
        let _cleanup = TempArtifact(tmp.clone());
        if let Err(error) = set_private_file_permissions(&tmp) {
            drop(file);
            return Err(error);
        }
        let write_result = file.write_all(&payload).and_then(|()| file.sync_all());
        drop(file);
        write_result?;
        publish_temp_file(&tmp, &path)?;
        sync_directory(parent)?;
        Ok(id)
    }

    /// Fetches artifact `id` with the default byte bound, verifying its
    /// content hash. Evidence consumers should prefer [`Self::fetch_verified`]
    /// with a durable receipt length.
    pub fn fetch(&self, id: &ArtifactId) -> Result<Vec<u8>, ToolError> {
        self.fetch_inner(id, None, DEFAULT_FETCH_LIMIT)
    }

    /// Fetches an artifact for trusted recovery of legacy records that do not
    /// persist its byte length. This path has no application output ceiling;
    /// never use it for client or model-facing retrieval. New evidence paths
    /// must use [`Self::fetch_verified`] with a durable receipt and limit.
    pub fn fetch_legacy_unbounded(&self, id: &ArtifactId) -> Result<Vec<u8>, ToolError> {
        self.fetch_inner(id, None, usize::MAX)
    }

    /// Fetches and verifies an artifact without allowing decompression to
    /// produce more than `max_bytes`.
    pub fn fetch_bounded(&self, id: &ArtifactId, max_bytes: usize) -> Result<Vec<u8>, ToolError> {
        self.fetch_inner(id, None, max_bytes)
    }

    /// Fetches and verifies an artifact against its durable receipt length and
    /// the caller's output bound.
    pub fn fetch_verified(
        &self,
        id: &ArtifactId,
        expected_bytes: usize,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ToolError> {
        if expected_bytes > max_bytes {
            return Err(ToolError::InvalidArgs(format!(
                "artifact receipt length {expected_bytes} exceeds fetch limit {max_bytes}"
            )));
        }
        self.fetch_inner(id, Some(expected_bytes), max_bytes)
    }

    fn fetch_inner(
        &self,
        id: &ArtifactId,
        expected_bytes: Option<usize>,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ToolError> {
        Self::validate_id(id)?;
        let path = self.path_for(id);
        let parent = path
            .parent()
            .ok_or_else(|| ToolError::InvalidArgs("artifact path has no parent".to_owned()))?;
        set_private_directory_permissions(&self.root)?;
        set_private_directory_permissions(parent)?;
        set_private_file_permissions(&path)?;
        let file = std::fs::File::open(&path)
            .map_err(|_| ToolError::InvalidArgs(format!("unknown artifact: {}", id.0)))?;
        let mut reader = BufReader::new(file);
        let bytes = if reader.fill_buf()?.starts_with(COMPRESSED_MAGIC) {
            reader.consume(COMPRESSED_MAGIC.len());
            let mut decoder = zstd::stream::read::Decoder::with_buffer(reader)?;
            decoder.window_log_max(decoder_window_log(max_bytes))?;
            read_limited(decoder, max_bytes)?
        } else {
            read_limited(reader, max_bytes)?
        };
        if bytes.len() > max_bytes {
            return Err(ToolError::InvalidArgs(format!(
                "artifact exceeds fetch limit of {max_bytes} bytes"
            )));
        }
        if expected_bytes.is_some_and(|expected| bytes.len() != expected) {
            return Err(ToolError::InvalidArgs(format!(
                "artifact length mismatch: expected {}, got {}",
                expected_bytes.unwrap_or_default(),
                bytes.len()
            )));
        }
        let actual_id = blake3::hash(&bytes).to_hex().to_string();
        if actual_id != id.0 {
            return Err(ToolError::InvalidArgs(format!(
                "artifact integrity mismatch: requested {}, content hashes to {actual_id}",
                id.0
            )));
        }
        Ok(bytes)
    }
}

struct TempArtifact(PathBuf);

impl Drop for TempArtifact {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn create_temp_file(parent: &Path, id: &ArtifactId) -> Result<(PathBuf, File), ToolError> {
    for _ in 0..128 {
        let sequence = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(".{}.tmp-{}-{sequence}", id.0, std::process::id()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate a unique artifact temporary file",
    )
    .into())
}

fn ensure_private_directory(path: &Path) -> Result<(), ToolError> {
    fs::create_dir_all(path)?;
    set_private_directory_permissions(path)?;
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

fn require_spool_object_type(path: &Path, directory: bool) -> Result<(), ToolError> {
    let metadata = fs::symlink_metadata(path)?;
    let file_type = metadata.file_type();
    let expected_type = if directory {
        file_type.is_dir()
    } else {
        file_type.is_file()
    };
    if file_type.is_symlink() || !expected_type {
        return Err(ToolError::InvalidArgs(format!(
            "artifact spool path is not a regular {}: {}",
            if directory { "directory" } else { "file" },
            path.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<(), ToolError> {
    use std::os::unix::fs::PermissionsExt as _;
    require_spool_object_type(path, true)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(windows)]
fn set_private_directory_permissions(path: &Path) -> Result<(), ToolError> {
    require_spool_object_type(path, true)?;
    set_windows_private_acl(path, true)
}

#[cfg(not(any(unix, windows)))]
fn set_private_directory_permissions(_path: &Path) -> Result<(), ToolError> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "owner-only artifact directory permissions are unsupported on this platform",
    )
    .into())
}

fn sync_existing_file(path: &Path) -> Result<(), ToolError> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<(), ToolError> {
    use std::os::unix::fs::PermissionsExt as _;
    require_spool_object_type(path, false)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(windows)]
fn set_private_file_permissions(path: &Path) -> Result<(), ToolError> {
    require_spool_object_type(path, false)?;
    set_windows_private_acl(path, false)
}

#[cfg(not(any(unix, windows)))]
fn set_private_file_permissions(_path: &Path) -> Result<(), ToolError> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "owner-only artifact file permissions are unsupported on this platform",
    )
    .into())
}

#[cfg(windows)]
struct TokenHandle(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
#[allow(unsafe_code)]
impl Drop for TokenHandle {
    fn drop(&mut self) {
        // SAFETY: this handle was returned by OpenProcessToken and is owned by
        // this guard until Drop.
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}

#[cfg(windows)]
struct LocalAcl(*mut windows_sys::Win32::Security::ACL);

#[cfg(windows)]
#[allow(unsafe_code)]
impl Drop for LocalAcl {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: SetEntriesInAclW returns this allocation for LocalFree.
            unsafe { windows_sys::Win32::Foundation::LocalFree(self.0.cast()) };
        }
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn set_windows_private_acl(path: &Path, directory: bool) -> Result<(), ToolError> {
    use std::iter::once;
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::Foundation::{
        ERROR_INSUFFICIENT_BUFFER, GENERIC_ALL, GetLastError, HANDLE,
    };
    use windows_sys::Win32::Security::Authorization::{
        EXPLICIT_ACCESS_W, SE_FILE_OBJECT, SET_ACCESS, SetEntriesInAclW, SetNamedSecurityInfoW,
        TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, GetTokenInformation, PROTECTED_DACL_SECURITY_INFORMATION,
        SUB_CONTAINERS_AND_OBJECTS_INHERIT, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    let mut raw_token: HANDLE = std::ptr::null_mut();
    // SAFETY: GetCurrentProcess returns a pseudo-handle; OpenProcessToken
    // writes a new owned token handle to raw_token.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut raw_token) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let token = TokenHandle(raw_token);

    let mut token_bytes = 0u32;
    // SAFETY: this sizing call intentionally supplies no output buffer.
    let sizing_result = unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            std::ptr::null_mut(),
            0,
            &raw mut token_bytes,
        )
    };
    if sizing_result != 0 || unsafe { GetLastError() } != ERROR_INSUFFICIENT_BUFFER {
        return Err(std::io::Error::last_os_error().into());
    }
    let word_count = usize::try_from(token_bytes)
        .unwrap_or(0)
        .div_ceil(std::mem::size_of::<usize>());
    let mut token_buffer = vec![0usize; word_count];
    // SAFETY: token_buffer has at least the size reported by the sizing call
    // and is word-aligned for TOKEN_USER.
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            token_buffer.as_mut_ptr().cast(),
            token_bytes,
            &raw mut token_bytes,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: GetTokenInformation populated the buffer with a TOKEN_USER.
    let token_user = unsafe { &*token_buffer.as_ptr().cast::<TOKEN_USER>() };
    let sid = token_user.User.Sid;
    if sid.is_null() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "current Windows token has no user SID",
        )
        .into());
    }

    let access = EXPLICIT_ACCESS_W {
        grfAccessPermissions: GENERIC_ALL,
        grfAccessMode: SET_ACCESS,
        grfInheritance: if directory {
            SUB_CONTAINERS_AND_OBJECTS_INHERIT
        } else {
            0
        },
        Trustee: TRUSTEE_W {
            TrusteeForm: TRUSTEE_IS_SID,
            TrusteeType: TRUSTEE_IS_USER,
            ptstrName: sid.cast(),
            ..TRUSTEE_W::default()
        },
    };

    let mut raw_acl = std::ptr::null_mut();
    // SAFETY: `access` references the live token SID, and raw_acl receives the
    // LocalAlloc-compatible ACL owned by LocalAcl below.
    let acl_status =
        unsafe { SetEntriesInAclW(1, &raw const access, std::ptr::null(), &raw mut raw_acl) };
    if acl_status != 0 {
        return Err(std::io::Error::from_raw_os_error(acl_status.cast_signed()).into());
    }
    let acl = LocalAcl(raw_acl);

    let wide_path: Vec<u16> = path.as_os_str().encode_wide().chain(once(0)).collect();
    // SAFETY: the path is NUL-terminated and remains alive for the call. The
    // DACL grants full access only to the current token user and protects it
    // from permissive inherited entries; directory ACEs remain inheritable.
    let status = unsafe {
        SetNamedSecurityInfoW(
            wide_path.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            acl.0,
            std::ptr::null_mut(),
        )
    };
    if status != 0 {
        return Err(std::io::Error::from_raw_os_error(status.cast_signed()).into());
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), ToolError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)]
fn sync_directory(_path: &Path) -> Result<(), ToolError> {
    // `MoveFileExW(MOVEFILE_WRITE_THROUGH)` below is the Windows platform
    // equivalent for making the same-directory publication durable.
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn sync_directory(_path: &Path) -> Result<(), ToolError> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "durable artifact publication is unsupported on this platform",
    )
    .into())
}

#[cfg(unix)]
fn publish_temp_file(temp: &Path, destination: &Path) -> Result<(), ToolError> {
    fs::rename(temp, destination)?;
    Ok(())
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn publish_temp_file(temp: &Path, destination: &Path) -> Result<(), ToolError> {
    use std::iter::once;
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let temp_wide: Vec<u16> = temp.as_os_str().encode_wide().chain(once(0)).collect();
    let destination_wide: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(once(0))
        .collect();
    // SAFETY: both paths are NUL-terminated UTF-16 buffers that remain alive
    // for the call; flags request same-volume replace and write-through.
    let result = unsafe {
        MoveFileExW(
            temp_wide.as_ptr(),
            destination_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn publish_temp_file(_temp: &Path, _destination: &Path) -> Result<(), ToolError> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "durable artifact publication is unsupported on this platform",
    )
    .into())
}

fn read_limited(mut reader: impl Read, limit: usize) -> Result<Vec<u8>, ToolError> {
    let read_limit = u64::try_from(limit.saturating_add(1)).unwrap_or(u64::MAX);
    let mut bytes = Vec::new();
    reader.by_ref().take(read_limit).read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn decoder_window_log(max_bytes: usize) -> u32 {
    let required = if max_bytes <= 1 {
        0
    } else {
        usize::BITS - (max_bytes - 1).leading_zeros()
    };
    required.clamp(10, MAX_DECODER_WINDOW_LOG)
}
