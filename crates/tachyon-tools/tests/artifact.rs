use std::path::PathBuf;

use tachyon_tools::artifact::{ArtifactSpool, DEFAULT_FETCH_LIMIT};
use tachyon_types::{ArtifactId, TaskId};

fn scratch() -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "tachyon-artifact-{}-{}",
        std::process::id(),
        TaskId::generate()
    ));
    std::fs::create_dir_all(&root).expect("create scratch directory");
    root
}

#[test]
fn fetch_rejects_bytes_that_do_not_match_the_artifact_id() {
    let root = scratch();
    let spool_root = root.join("artifacts");
    let spool = ArtifactSpool::new(spool_root.clone());
    let id = spool.store(b"expected payload").expect("store artifact");

    let path = spool_root.join(&id.0[..2]).join(&id.0);
    std::fs::write(path, b"different payload").expect("corrupt stored artifact");

    assert!(
        spool.fetch(&id).is_err(),
        "fetch must reject bytes whose BLAKE3 id does not match"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fetch_rejects_malformed_artifact_ids_even_if_a_matching_path_exists() {
    let root = scratch();
    let spool_root = root.join("artifacts");
    let spool = ArtifactSpool::new(spool_root.clone());
    let id = ArtifactId("not-a-blake3-id".to_owned());
    let path = spool_root.join(&id.0[..2]).join(&id.0);
    std::fs::create_dir_all(path.parent().expect("artifact parent"))
        .expect("create artifact shard");
    std::fs::write(path, b"payload").expect("create malformed artifact");

    assert!(
        spool.fetch(&id).is_err(),
        "fetch must validate the id before accepting an on-disk blob"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn bounded_fetch_rejects_compressed_payloads_larger_than_the_limit() {
    let root = scratch();
    let spool = ArtifactSpool::new(root.join("artifacts"));
    let payload = vec![b'x'; 100_000];
    let id = spool.store(&payload).expect("store compressed artifact");

    assert!(
        spool.fetch_bounded(&id, 1_024).is_err(),
        "bounded fetch must stop decompression at its byte limit"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn legacy_recovery_fetch_preserves_artifacts_larger_than_the_default_limit() {
    let root = scratch();
    let spool = ArtifactSpool::new(root.join("artifacts"));
    let payload = vec![b'x'; DEFAULT_FETCH_LIMIT + 1];
    let id = spool.store(&payload).expect("store large artifact");

    assert!(spool.fetch(&id).is_err());
    assert_eq!(spool.fetch_legacy_unbounded(&id).unwrap(), payload);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn verified_fetch_requires_the_receipt_length_to_match() {
    let root = scratch();
    let spool = ArtifactSpool::new(root.join("artifacts"));
    let payload = b"receipt-bound content";
    let id = spool.store(payload).expect("store artifact");

    assert!(
        spool.fetch_verified(&id, payload.len() + 1, 1_024).is_err(),
        "verified fetch must reject a mismatched durable receipt length"
    );
    assert_eq!(
        spool.fetch_verified(&id, payload.len(), 1_024).unwrap(),
        payload
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn compressed_fetch_supports_large_artifacts() {
    let root = scratch();
    let spool = ArtifactSpool::new(root.join("artifacts"));
    let payload = vec![b'x'; 16 * 1024 * 1024];
    let id = spool.store(&payload).expect("store large artifact");

    assert_eq!(spool.fetch(&id).unwrap(), payload);
    assert_eq!(spool.fetch_bounded(&id, payload.len()).unwrap(), payload);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn storing_existing_content_repairs_a_corrupt_blob_before_returning_its_id() {
    let root = scratch();
    let spool_root = root.join("artifacts");
    let spool = ArtifactSpool::new(spool_root.clone());
    let payload = b"durable content";
    let id = spool.store(payload).expect("store artifact");
    std::fs::write(spool_root.join(&id.0[..2]).join(&id.0), b"corrupted")
        .expect("corrupt artifact");

    assert_eq!(spool.store(payload).unwrap(), id);
    assert_eq!(spool.fetch(&id).unwrap(), payload);
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn stored_artifacts_use_owner_only_directory_and_file_permissions() {
    use std::os::unix::fs::PermissionsExt as _;

    let root = scratch();
    let spool_root = root.join("artifacts");
    let spool = ArtifactSpool::new(spool_root.clone());
    let id = spool.store(b"private evidence").expect("store artifact");

    let shard = spool_root.join(&id.0[..2]);
    let blob = shard.join(&id.0);
    assert_eq!(
        std::fs::metadata(&spool_root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&shard).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&blob).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn storing_existing_valid_artifact_restores_owner_only_file_permissions() {
    use std::os::unix::fs::PermissionsExt as _;

    let root = scratch();
    let spool_root = root.join("artifacts");
    let spool = ArtifactSpool::new(spool_root.clone());
    let payload = b"existing valid artifact";
    let id = spool.store(payload).expect("store artifact");
    let blob = spool_root.join(&id.0[..2]).join(&id.0);
    std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o644))
        .expect("make existing artifact permissive");

    assert_eq!(spool.store(payload).unwrap(), id);
    assert_eq!(
        std::fs::metadata(&blob).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn fetch_hardens_existing_spool_directories_and_file_before_reading() {
    use std::os::unix::fs::PermissionsExt as _;

    let root = scratch();
    let spool_root = root.join("artifacts");
    let spool = ArtifactSpool::new(spool_root.clone());
    let payload = b"legacy artifact";
    let id = spool.store(payload).expect("store artifact");
    let shard = spool_root.join(&id.0[..2]);
    let blob = shard.join(&id.0);
    std::fs::set_permissions(&spool_root, std::fs::Permissions::from_mode(0o755))
        .expect("make artifact root permissive");
    std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o755))
        .expect("make artifact shard permissive");
    std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o644))
        .expect("make artifact file permissive");

    assert_eq!(spool.fetch(&id).unwrap(), payload);
    assert_eq!(
        std::fs::metadata(&spool_root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&shard).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&blob).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn fetch_rejects_symlinked_spool_objects() {
    let root = scratch();
    let spool_root = root.join("artifacts");
    let spool = ArtifactSpool::new(spool_root.clone());
    let payload = b"private target";
    let id = spool.store(payload).expect("store artifact");
    let shard = spool_root.join(&id.0[..2]);
    let blob = shard.join(&id.0);
    let target = root.join("outside");
    std::fs::rename(&blob, &target).expect("move artifact outside shard");
    std::os::unix::fs::symlink(&target, &blob).expect("replace artifact with symlink");

    assert!(spool.fetch(&id).is_err());
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(windows)]
#[test]
fn windows_spool_entries_have_protected_current_user_only_dacls() {
    let root = scratch();
    let spool_root = root.join("artifacts");
    let spool = ArtifactSpool::new(spool_root.clone());
    let id = spool
        .store(b"private Windows artifact")
        .expect("store artifact");
    let shard = spool_root.join(&id.0[..2]);
    let blob = shard.join(&id.0);

    for path in [&spool_root, &shard, &blob] {
        assert_current_user_only_dacl(path);
    }
    let _ = std::fs::remove_dir_all(root);
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
struct SecurityDescriptor(*mut std::ffi::c_void);

#[cfg(windows)]
#[allow(unsafe_code)]
impl Drop for SecurityDescriptor {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: GetNamedSecurityInfoW returns this allocation for LocalFree.
            unsafe { windows_sys::Win32::Foundation::LocalFree(self.0) };
        }
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn assert_current_user_only_dacl(path: &std::path::Path) {
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, GetLastError};
    use windows_sys::Win32::Security::Authorization::{GetNamedSecurityInfoW, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACL_SIZE_INFORMATION, AclSizeInformation, DACL_SECURITY_INFORMATION,
        EqualSid, GetAce, GetAclInformation, GetSecurityDescriptorControl,
        GetSecurityDescriptorDacl, SE_DACL_PROTECTED,
    };
    use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser};
    use windows_sys::Win32::System::SystemServices::ACCESS_ALLOWED_ACE_TYPE;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    let mut raw_token = std::ptr::null_mut();
    // SAFETY: GetCurrentProcess returns a pseudo-handle; OpenProcessToken
    // writes a new owned token handle to raw_token.
    assert_ne!(
        unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut raw_token) },
        0,
        "open current process token"
    );
    let token = TokenHandle(raw_token);

    let mut token_bytes = 0u32;
    // SAFETY: this sizing call intentionally supplies no output buffer.
    assert_eq!(
        unsafe {
            GetTokenInformation(
                token.0,
                TokenUser,
                std::ptr::null_mut(),
                0,
                &raw mut token_bytes,
            )
        },
        0,
        "size current token user information"
    );
    // SAFETY: GetLastError reports the sizing call's documented buffer error.
    assert_eq!(unsafe { GetLastError() }, ERROR_INSUFFICIENT_BUFFER);
    let word_count = usize::try_from(token_bytes)
        .expect("token user size fits usize")
        .div_ceil(std::mem::size_of::<usize>());
    let mut token_buffer = vec![0usize; word_count];
    // SAFETY: token_buffer is word-aligned and at least token_bytes long.
    assert_ne!(
        unsafe {
            GetTokenInformation(
                token.0,
                TokenUser,
                token_buffer.as_mut_ptr().cast(),
                token_bytes,
                &raw mut token_bytes,
            )
        },
        0,
        "read current token user information"
    );
    // SAFETY: GetTokenInformation populated token_buffer with a TOKEN_USER.
    let token_user = unsafe { &*token_buffer.as_ptr().cast::<TOKEN_USER>() };
    let user_sid = token_user.User.Sid;
    assert!(
        !user_sid.is_null(),
        "current process token user SID is present"
    );

    let wide_path: Vec<u16> = path.as_os_str().encode_wide().chain([0]).collect();
    let mut dacl = std::ptr::null_mut();
    let mut raw_descriptor = std::ptr::null_mut();
    // SAFETY: all out-pointers are valid for the call and the path is
    // NUL-terminated UTF-16 for the duration of the call. The security
    // descriptor owner can differ from TOKEN_USER for elevated processes, so
    // validate the DACL against the user SID that the spool grants access to.
    let status = unsafe {
        GetNamedSecurityInfoW(
            wide_path.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &raw mut dacl,
            std::ptr::null_mut(),
            &raw mut raw_descriptor,
        )
    };
    assert_eq!(status, 0, "read DACL for {}", path.display());
    let descriptor = SecurityDescriptor(raw_descriptor);

    let mut control = 0u16;
    let mut revision = 0u32;
    // SAFETY: descriptor is the valid security descriptor returned above.
    assert_ne!(
        unsafe { GetSecurityDescriptorControl(descriptor.0, &raw mut control, &raw mut revision) },
        0
    );
    assert_ne!(control & SE_DACL_PROTECTED, 0, "DACL must be protected");

    let mut dacl_present = 0;
    let mut descriptor_dacl = std::ptr::null_mut();
    let mut dacl_defaulted = 0;
    // SAFETY: descriptor and all output pointers are valid.
    assert_ne!(
        unsafe {
            GetSecurityDescriptorDacl(
                descriptor.0,
                &raw mut dacl_present,
                &raw mut descriptor_dacl,
                &raw mut dacl_defaulted,
            )
        },
        0
    );
    assert_ne!(dacl_present, 0);
    assert!(!dacl.is_null());
    assert_eq!(dacl, descriptor_dacl);

    let mut size_info = ACL_SIZE_INFORMATION::default();
    // SAFETY: the DACL pointer is owned by descriptor and size_info is writable.
    assert_ne!(
        unsafe {
            GetAclInformation(
                dacl,
                (&raw mut size_info).cast(),
                u32::try_from(std::mem::size_of::<ACL_SIZE_INFORMATION>())
                    .expect("ACL size fits in DWORD"),
                AclSizeInformation,
            )
        },
        0
    );
    assert_ne!(
        size_info.AceCount, 0,
        "DACL must grant the current user access"
    );
    for index in 0..size_info.AceCount {
        let mut raw_ace = std::ptr::null_mut();
        // SAFETY: index is below AceCount and raw_ace is a valid out-pointer.
        assert_ne!(unsafe { GetAce(dacl, index, &raw mut raw_ace) }, 0);
        let ace = raw_ace.cast::<ACCESS_ALLOWED_ACE>();
        // SAFETY: every allowed ACE has the ACCESS_ALLOWED_ACE layout.
        assert_eq!(
            u32::from(unsafe { (*ace).Header.AceType }),
            ACCESS_ALLOWED_ACE_TYPE,
            "DACL contains a non-allow ACE for {}",
            path.display()
        );
        // SAFETY: SidStart is the first aligned byte of the SID embedded after
        // ACCESS_ALLOWED_ACE's fixed fields.
        let ace_sid = unsafe { std::ptr::addr_of!((*ace).SidStart).cast_mut().cast() };
        // SAFETY: user_sid is owned by token_buffer and ace_sid is in the DACL.
        assert_ne!(
            unsafe { EqualSid(user_sid, ace_sid) },
            0,
            "ACE {index} grants access to a different user on {}",
            path.display()
        );
    }
}

#[test]
fn concurrent_identical_stores_publish_one_valid_blob_without_temp_files() {
    let root = scratch();
    let spool_root = root.join("artifacts");
    let spool = ArtifactSpool::new(spool_root.clone());
    let payload = vec![b'z'; 100_000];

    let ids = std::thread::scope(|scope| {
        (0..8)
            .map(|_| {
                let spool = spool.clone();
                let payload = &payload;
                scope.spawn(move || spool.store(payload).expect("store concurrent artifact"))
            })
            .map(|worker| worker.join().expect("store worker"))
            .collect::<Vec<_>>()
    });

    assert!(ids.iter().all(|id| id == &ids[0]));
    assert_eq!(spool.fetch(&ids[0]).unwrap(), payload);
    let shard = spool_root.join(&ids[0].0[..2]);
    assert_eq!(std::fs::read_dir(shard).unwrap().count(), 1);
    let _ = std::fs::remove_dir_all(root);
}
