use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use crate::{
    error::{AppError, DestinationPreflightFailure},
    transfer_policy::{DEFAULT_MAX_FILE_BYTES, TRANSFER_CHUNK_BYTES},
};

const DESTINATION_SPACE_RESERVE_CHUNKS: u64 = 16;
const FAT32_MAX_FILE_BYTES: u64 = (4 * 1024 * 1024 * 1024) - 1;
const WRITE_PROBE_ATTEMPTS: u8 = 3;
pub(crate) const DESTINATION_PREFLIGHT_PAUSE_MARKER: &str =
    "[weline-localnet:destination-preflight:v1]";

pub(crate) fn destination_preflight_pause_error(failure: DestinationPreflightFailure) -> String {
    format!(
        "{DESTINATION_PREFLIGHT_PAUSE_MARKER}{}",
        failure.marker_token()
    )
}

pub(crate) fn destination_preflight_failure_from_pause_error(
    error: &str,
) -> Option<DestinationPreflightFailure> {
    let token = error.strip_prefix(DESTINATION_PREFLIGHT_PAUSE_MARKER)?;
    DestinationPreflightFailure::from_marker_token(token)
}

pub(crate) fn sanitize_destination_preflight_error(directory: &Path, error: AppError) -> AppError {
    if matches!(error, AppError::DestinationPreflight(_)) {
        return error;
    }
    let failure = DestinationPreflightFailure::from_app_error(&error);
    tracing::warn!(
        directory = %directory.display(),
        raw_error = %error,
        category = failure.marker_token(),
        "destination preflight failed"
    );
    AppError::DestinationPreflight(failure)
}

fn destination_io_error(directory: &Path, operation: &str, error: std::io::Error) -> AppError {
    let failure = DestinationPreflightFailure::from_io_error(&error);
    tracing::warn!(
        directory = %directory.display(),
        %operation,
        raw_error = %error,
        category = failure.marker_token(),
        "destination filesystem operation failed"
    );
    AppError::DestinationPreflight(failure)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeSnapshot {
    pub filesystem: String,
    pub available_bytes: u64,
    pub max_file_bytes: Option<u64>,
}

impl VolumeSnapshot {
    pub fn known(
        filesystem: impl AsRef<str>,
        available_bytes: u64,
        max_file_bytes: Option<u64>,
    ) -> Self {
        Self {
            filesystem: normalize_filesystem_name(filesystem.as_ref()),
            available_bytes,
            max_file_bytes,
        }
    }
}

pub fn validate_volume(
    snapshot: &VolumeSnapshot,
    file_size: u64,
    committed_bytes: u64,
) -> Result<(), AppError> {
    if file_size > DEFAULT_MAX_FILE_BYTES {
        return Err(AppError::DestinationPreflight(
            DestinationPreflightFailure::FileTooLarge,
        ));
    }

    let filesystem = normalize_filesystem_name(&snapshot.filesystem);
    if filesystem.is_empty() {
        return Err(AppError::DestinationPreflight(
            DestinationPreflightFailure::UnsupportedFilesystem,
        ));
    }

    if let Some(max_file_bytes) = snapshot.max_file_bytes
        && file_size > max_file_bytes
    {
        return Err(AppError::DestinationPreflight(
            DestinationPreflightFailure::FilesystemLimit,
        ));
    }

    let remaining_bytes = file_size
        .checked_sub(committed_bytes)
        .ok_or_else(|| AppError::InvalidInput("已接收的文件大小不能超过文件总大小".to_string()))?;
    let reserve_bytes = u64::from(TRANSFER_CHUNK_BYTES)
        .checked_mul(DESTINATION_SPACE_RESERVE_CHUNKS)
        .ok_or_else(|| {
            AppError::Storage("无法计算接收目录所需的预留空间，请重新选择目录后重试".to_string())
        })?;
    let final_copy_bytes = if filesystem_supports_hard_links(&filesystem) {
        0
    } else {
        file_size
    };
    let required_bytes = remaining_bytes
        .checked_add(final_copy_bytes)
        .and_then(|bytes| bytes.checked_add(reserve_bytes))
        .ok_or_else(|| {
            AppError::InvalidInput("文件大小过大，无法计算接收目录所需空间".to_string())
        })?;

    if snapshot.available_bytes < required_bytes {
        return Err(AppError::DestinationPreflight(
            DestinationPreflightFailure::InsufficientSpace,
        ));
    }

    Ok(())
}

pub fn inspect_volume(directory: &Path) -> Result<VolumeSnapshot, AppError> {
    platform::inspect_volume(directory)
}

pub fn preflight_destination(
    directory: &Path,
    file_size: u64,
    committed_bytes: u64,
) -> Result<VolumeSnapshot, AppError> {
    // The receiver creates its `.part` file inside this directory, so it remains on this volume.
    probe_writable_directory(directory)?;
    let snapshot = inspect_volume(directory)?;
    validate_volume(&snapshot, file_size, committed_bytes)?;
    Ok(snapshot)
}

fn probe_writable_directory(directory: &Path) -> Result<(), AppError> {
    let metadata = fs::metadata(directory)
        .map_err(|error| destination_io_error(directory, "read directory metadata", error))?;
    if !metadata.is_dir() {
        tracing::warn!(
            directory = %directory.display(),
            "destination path is not a directory"
        );
        return Err(AppError::DestinationPreflight(
            DestinationPreflightFailure::DirectoryUnavailable,
        ));
    }

    for _ in 0..WRITE_PROBE_ATTEMPTS {
        let probe_path = directory.join(format!(
            ".localnet-write-probe-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe_path)
        {
            Ok(mut file) => {
                let write_result = file
                    .write_all(&[0])
                    .map_err(|error| destination_io_error(directory, "write probe", error));
                drop(file);
                let cleanup_result = fs::remove_file(&probe_path)
                    .map_err(|error| destination_io_error(directory, "remove write probe", error));

                return match (write_result, cleanup_result) {
                    (Ok(()), Ok(())) => Ok(()),
                    (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
                    (Err(write_error), Err(cleanup_error)) => {
                        tracing::warn!(
                            directory = %directory.display(),
                            %write_error,
                            %cleanup_error,
                            "destination write and cleanup probes both failed"
                        );
                        Err(write_error)
                    }
                };
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(destination_io_error(directory, "create write probe", error));
            }
        }
    }

    tracing::warn!(
        directory = %directory.display(),
        attempts = WRITE_PROBE_ATTEMPTS,
        "destination write probe names repeatedly collided"
    );
    Err(AppError::DestinationPreflight(
        DestinationPreflightFailure::DirectoryUnavailable,
    ))
}

fn normalize_filesystem_name(filesystem: &str) -> String {
    filesystem
        .trim_matches(char::from(0))
        .trim()
        .to_ascii_uppercase()
}

fn maximum_file_bytes(filesystem: &str) -> Option<u64> {
    match normalize_filesystem_name(filesystem).as_str() {
        "FAT32" | "MSDOS" => Some(FAT32_MAX_FILE_BYTES),
        _ => None,
    }
}

fn filesystem_supports_hard_links(filesystem: &str) -> bool {
    matches!(
        normalize_filesystem_name(filesystem).as_str(),
        "NTFS" | "REFS" | "APFS" | "HFS" | "HFS+" | "HFSX"
    )
}

#[cfg(target_os = "windows")]
mod platform {
    use std::{os::windows::ffi::OsStrExt, path::Path};

    use windows_sys::Win32::Storage::FileSystem::{
        GetDiskFreeSpaceExW, GetVolumeInformationW, GetVolumePathNameW,
    };

    use super::{
        AppError, DestinationPreflightFailure, VolumeSnapshot, destination_io_error,
        maximum_file_bytes, normalize_filesystem_name,
    };

    const VOLUME_PATH_CAPACITY: usize = 32_768;
    const FILESYSTEM_NAME_CAPACITY: usize = 256;

    pub(super) fn inspect_volume(directory: &Path) -> Result<VolumeSnapshot, AppError> {
        let mut directory_wide: Vec<u16> = directory.as_os_str().encode_wide().collect();
        if directory_wide.contains(&0) {
            return Err(AppError::DestinationPreflight(
                DestinationPreflightFailure::DirectoryUnavailable,
            ));
        }
        directory_wide.push(0);

        let mut volume_path = vec![0_u16; VOLUME_PATH_CAPACITY];
        if unsafe {
            GetVolumePathNameW(
                directory_wide.as_ptr(),
                volume_path.as_mut_ptr(),
                VOLUME_PATH_CAPACITY as u32,
            )
        } == 0
        {
            return Err(probe_error(directory, "定位所在卷"));
        }

        let mut filesystem_name = vec![0_u16; FILESYSTEM_NAME_CAPACITY];
        if unsafe {
            GetVolumeInformationW(
                volume_path.as_ptr(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                filesystem_name.as_mut_ptr(),
                FILESYSTEM_NAME_CAPACITY as u32,
            )
        } == 0
        {
            return Err(probe_error(directory, "读取文件系统类型"));
        }
        let filesystem = utf16_buffer_to_string(&filesystem_name).ok_or_else(|| {
            AppError::DestinationPreflight(DestinationPreflightFailure::UnsupportedFilesystem)
        })?;

        let mut available_bytes = 0_u64;
        if unsafe {
            GetDiskFreeSpaceExW(
                volume_path.as_ptr(),
                &mut available_bytes,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(probe_error(directory, "读取可用空间"));
        }

        Ok(VolumeSnapshot {
            max_file_bytes: maximum_file_bytes(&filesystem),
            filesystem,
            available_bytes,
        })
    }

    fn utf16_buffer_to_string(buffer: &[u16]) -> Option<String> {
        let end = buffer
            .iter()
            .position(|unit| *unit == 0)
            .unwrap_or(buffer.len());
        let filesystem = String::from_utf16(&buffer[..end]).ok()?;
        let normalized = normalize_filesystem_name(&filesystem);
        (!normalized.is_empty()).then_some(normalized)
    }

    fn probe_error(directory: &Path, operation: &str) -> AppError {
        destination_io_error(directory, operation, std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::{
        ffi::{CStr, CString},
        mem::MaybeUninit,
        os::unix::ffi::OsStrExt,
        path::Path,
    };

    use super::{
        AppError, DestinationPreflightFailure, VolumeSnapshot, destination_io_error,
        maximum_file_bytes, normalize_filesystem_name,
    };

    pub(super) fn inspect_volume(directory: &Path) -> Result<VolumeSnapshot, AppError> {
        let directory = CString::new(directory.as_os_str().as_bytes()).map_err(|_| {
            AppError::DestinationPreflight(DestinationPreflightFailure::DirectoryUnavailable)
        })?;
        let mut stat = MaybeUninit::<libc::statfs>::zeroed();
        if unsafe { libc::statfs(directory.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(probe_error(
                directory.to_string_lossy().as_ref(),
                "读取卷信息",
            ));
        }
        let stat = unsafe { stat.assume_init() };
        let filesystem = unsafe { CStr::from_ptr(stat.f_fstypename.as_ptr()) }
            .to_str()
            .ok()
            .map(normalize_filesystem_name)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                AppError::DestinationPreflight(DestinationPreflightFailure::UnsupportedFilesystem)
            })?;
        let available_blocks = u64::try_from(stat.f_bavail).map_err(|_| {
            AppError::DestinationPreflight(DestinationPreflightFailure::UnsupportedFilesystem)
        })?;
        let block_size = u64::try_from(stat.f_bsize).map_err(|_| {
            AppError::DestinationPreflight(DestinationPreflightFailure::UnsupportedFilesystem)
        })?;
        let available_bytes = available_blocks.checked_mul(block_size).ok_or_else(|| {
            AppError::DestinationPreflight(DestinationPreflightFailure::UnsupportedFilesystem)
        })?;

        Ok(VolumeSnapshot {
            max_file_bytes: maximum_file_bytes(&filesystem),
            filesystem,
            available_bytes,
        })
    }

    fn probe_error(directory: &str, operation: &str) -> AppError {
        destination_io_error(
            Path::new(directory),
            operation,
            std::io::Error::last_os_error(),
        )
    }
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
mod platform {
    use std::path::Path;

    use super::{AppError, DestinationPreflightFailure, VolumeSnapshot};

    pub(super) fn inspect_volume(_directory: &Path) -> Result<VolumeSnapshot, AppError> {
        Err(AppError::DestinationPreflight(
            DestinationPreflightFailure::UnsupportedFilesystem,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::{
        DESTINATION_PREFLIGHT_PAUSE_MARKER, FAT32_MAX_FILE_BYTES, VolumeSnapshot,
        destination_preflight_failure_from_pause_error, destination_preflight_pause_error,
        maximum_file_bytes, preflight_destination, validate_volume,
    };
    use crate::{
        error::{AppError, DestinationPreflightFailure},
        transfer_policy::TRANSFER_CHUNK_BYTES,
    };

    const GIB: u64 = 1024 * 1024 * 1024;
    const RESERVE_BYTES: u64 = 16 * TRANSFER_CHUNK_BYTES as u64;

    #[test]
    fn destination_pause_markers_round_trip_only_exact_allowlisted_categories() {
        for failure in [
            DestinationPreflightFailure::DirectoryUnavailable,
            DestinationPreflightFailure::PermissionDenied,
            DestinationPreflightFailure::InsufficientSpace,
            DestinationPreflightFailure::FilesystemLimit,
            DestinationPreflightFailure::UnsupportedFilesystem,
            DestinationPreflightFailure::FileTooLarge,
        ] {
            let marker = destination_preflight_pause_error(failure);
            assert_eq!(
                destination_preflight_failure_from_pause_error(&marker),
                Some(failure)
            );
        }
        for invalid in [
            DESTINATION_PREFLIGHT_PAUSE_MARKER.to_string(),
            format!("{DESTINATION_PREFLIGHT_PAUSE_MARKER}directory-unavailable "),
            format!("{DESTINATION_PREFLIGHT_PAUSE_MARKER} DIRECTORY-UNAVAILABLE"),
            format!(
                "{DESTINATION_PREFLIGHT_PAUSE_MARKER}unable to inspect E:\\Private (os error 3)"
            ),
            format!(
                "{DESTINATION_PREFLIGHT_PAUSE_MARKER}{DESTINATION_PREFLIGHT_PAUSE_MARKER}directory-unavailable"
            ),
        ] {
            assert_eq!(
                destination_preflight_failure_from_pause_error(&invalid),
                None
            );
        }
    }

    #[test]
    fn accepts_exact_remaining_plus_64_mib() {
        let file_size = 8 * GIB;
        let committed_bytes = 3 * GIB;
        let snapshot =
            VolumeSnapshot::known("NTFS", file_size - committed_bytes + RESERVE_BYTES, None);

        validate_volume(&snapshot, file_size, committed_bytes).unwrap();
    }

    #[test]
    fn rejects_one_byte_less_than_remaining_plus_64_mib() {
        let file_size = 8 * GIB;
        let committed_bytes = 3 * GIB;
        let snapshot = VolumeSnapshot::known(
            "NTFS",
            file_size - committed_bytes + RESERVE_BYTES - 1,
            None,
        );

        assert!(
            validate_volume(&snapshot, file_size, committed_bytes)
                .unwrap_err()
                .to_string()
                .contains("空间")
        );
    }

    #[test]
    fn fat32_aliases_reject_a_five_gib_file_through_the_production_mapping() {
        for filesystem in ["FAT32", "MSDOS", "msdos"] {
            let snapshot =
                VolumeSnapshot::known(filesystem, 10 * GIB, maximum_file_bytes(filesystem));
            assert_eq!(snapshot.max_file_bytes, Some(FAT32_MAX_FILE_BYTES));
            assert!(matches!(
                validate_volume(&snapshot, 5 * GIB, 0),
                Err(AppError::DestinationPreflight(
                    DestinationPreflightFailure::FilesystemLimit
                ))
            ));
        }
    }

    #[test]
    fn known_hard_link_filesystems_accept_100_gib_at_remaining_plus_margin() {
        for filesystem in ["NTFS", "APFS"] {
            let snapshot = VolumeSnapshot::known(filesystem, 100 * GIB + RESERVE_BYTES, None);
            validate_volume(&snapshot, 100 * GIB, 0).unwrap();
        }
    }

    #[test]
    fn unsupported_or_unknown_hard_link_filesystems_reserve_a_full_copy_candidate() {
        let file_size = 3 * GIB;
        let committed_bytes = GIB;
        let exact = (file_size - committed_bytes) + file_size + RESERVE_BYTES;

        for filesystem in ["EXFAT", "FAT32", "MSDOS", "UNKNOWNFS"] {
            let snapshot = VolumeSnapshot::known(filesystem, exact, maximum_file_bytes(filesystem));
            validate_volume(&snapshot, file_size, committed_bytes)
                .expect("copy-fallback filesystems accept the exact full workspace margin");
            let one_byte_short =
                VolumeSnapshot::known(filesystem, exact - 1, maximum_file_bytes(filesystem));
            assert!(matches!(
                validate_volume(&one_byte_short, file_size, committed_bytes),
                Err(AppError::DestinationPreflight(
                    DestinationPreflightFailure::InsufficientSpace
                ))
            ));
        }
    }

    #[test]
    fn exfat_100_gib_checked_workspace_math_requires_two_full_files_without_allocating() {
        let file_size = 100 * GIB;
        let required = file_size
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(RESERVE_BYTES))
            .expect("100 GiB workspace fits u64");
        validate_volume(
            &VolumeSnapshot::known("EXFAT", required, None),
            file_size,
            0,
        )
        .expect("exact 100 GiB copy workspace is accepted");
        assert!(
            validate_volume(
                &VolumeSnapshot::known("EXFAT", required - 1, None),
                file_size,
                0,
            )
            .is_err()
        );
    }

    #[test]
    fn committed_bytes_reduce_the_required_space() {
        let file_size = 10 * GIB;
        let committed_bytes = 7 * GIB;
        let snapshot =
            VolumeSnapshot::known("NTFS", file_size - committed_bytes + RESERVE_BYTES, None);

        validate_volume(&snapshot, file_size, committed_bytes).unwrap();
    }

    #[test]
    fn preflight_rejects_a_target_that_is_not_a_writable_directory() {
        let file_path = unique_test_path("not-a-directory");
        fs::write(&file_path, b"not a directory").unwrap();
        let _cleanup = TestFileCleanup(file_path.clone());

        assert!(
            preflight_destination(&file_path, 0, 0)
                .unwrap_err()
                .to_string()
                .contains("目录")
        );
    }

    fn unique_test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("localnet-{name}-{}", uuid::Uuid::now_v7()))
    }

    struct TestFileCleanup(PathBuf);

    impl Drop for TestFileCleanup {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }
}
