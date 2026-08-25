use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{error::AppError, volume_preflight};

const RESERVATION_PREFIX: &str = "WELINE-LOCALNET-RESERVATION:";
const PARTIAL_OWNERSHIP_PREFIX: &str = "WELINE-LOCALNET-PARTIAL:";
const FINALIZATION_PREFIX: &str = "WELINE-LOCALNET-FINALIZATION:";

pub struct FinalizedReceive {
    pub path: PathBuf,
    pub reservation_released: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalizationPhase {
    BeforeReplacementReserve,
    AfterReplacementReserve,
    AfterJournalUpdate,
    AfterMetadataSwitch,
    AfterOldReservationRelease,
    AfterFinalNameCreation,
    AfterFinalJournalSync,
    BeforeCopy,
    DuringCopy,
    AfterCopySync,
    BeforeCopyMaterializedJournal,
    #[cfg(target_os = "macos")]
    AfterCopyQuarantineProof,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LegacyStageCleanupPhase {
    AfterOpenIdentityVerified,
    AfterPreparation,
    AfterNamespaceMutation,
    #[cfg(target_os = "macos")]
    AfterQuarantineVerification,
    #[cfg(target_os = "macos")]
    AfterUnlink,
    AfterJournalComplete,
}

static LEGACY_STAGE_CLEANUP_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static OWNED_ARTIFACT_CLEANUP_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExactRemovalOutcome {
    Removed,
    Missing,
    NotOwned,
    ProofLost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExactCleanupPhase {
    AfterInitialProof,
    #[cfg(target_os = "macos")]
    AfterQuarantineProof,
}

pub fn commit_without_overwrite(partial: &Path, destination: &Path) -> std::io::Result<()> {
    match fs::hard_link(partial, destination) {
        Ok(()) => {
            if let Err(error) = fs::remove_file(partial) {
                let _ = fs::remove_file(destination);
                return Err(error);
            }
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Err(error),
        Err(_) => {
            #[cfg(windows)]
            {
                match fs::rename(partial, destination) {
                    Ok(()) => return Ok(()),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        return Err(error);
                    }
                    Err(_) => {}
                }
            }
            copy_without_overwrite(partial, destination)
        }
    }
}

fn copy_without_overwrite(partial: &Path, destination: &Path) -> io::Result<()> {
    let mut source = File::open(partial)?;
    let mut target = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let copied = io::copy(&mut source, &mut target)
        .and_then(|_| target.flush())
        .and_then(|_| target.sync_all());
    drop(target);
    if let Err(error) = copied {
        let _ = fs::remove_file(destination);
        return Err(error);
    }
    if let Err(error) = fs::remove_file(partial) {
        let _ = fs::remove_file(destination);
        return Err(error);
    }
    Ok(())
}

pub fn reserve_receive_path(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<()> {
    if destination.try_exists()? {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "保存位置已经存在同名文件",
        ));
    }
    let reservation_path = reservation_sidecar_path(destination)?;
    let payload = reservation_payload(transfer_id, reservation_token);
    let mut reservation = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&reservation_path)?;
    let result = reservation
        .write_all(&payload)
        .and_then(|_| reservation.flush())
        .and_then(|_| reservation.sync_all());
    drop(reservation);
    if let Err(error) = result {
        let _ = fs::remove_file(&reservation_path);
        return Err(error);
    }
    if destination.try_exists()? {
        let _ = remove_owned_reservation(destination, transfer_id, reservation_token);
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "保存位置已经存在同名文件",
        ));
    }
    Ok(())
}

pub fn reserve_available_receive_path(
    receive_directory: &Path,
    file_name: &str,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<PathBuf> {
    let safe_name = safe_file_name(file_name);
    let mut sequence = 0_u32;
    loop {
        let candidate = receive_directory.join(numbered_file_name(&safe_name, sequence));
        match reserve_receive_path(&candidate, transfer_id, reservation_token) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                sequence = sequence.checked_add(1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::AlreadyExists, "没有可用的文件保存名称")
                })?;
            }
            Err(error) => return Err(error),
        }
    }
}

pub fn reservation_is_owned(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<bool> {
    let reservation_path = reservation_sidecar_path(destination)?;
    let payload = reservation_payload(transfer_id, reservation_token);
    let mut file = match open_regular_file_no_follow(&reservation_path, false) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) if is_no_follow_rejection(&error) => return Ok(false),
        Err(error) => return Err(error),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != payload.len() as u64 {
        return Ok(false);
    }
    let mut actual = vec![0_u8; payload.len()];
    match file.read_exact(&mut actual) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
        Err(error) => return Err(error),
    }
    Ok(actual == payload)
}

pub fn remove_owned_reservation(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<bool> {
    let cleanup_token = uuid::Uuid::new_v4().to_string();
    remove_owned_reservation_internal(
        destination,
        transfer_id,
        reservation_token,
        &cleanup_token,
        |_, _| Ok(()),
    )
}

pub(crate) fn remove_owned_reservation_with_cleanup_token(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    cleanup_token: &str,
) -> io::Result<bool> {
    remove_owned_reservation_internal(
        destination,
        transfer_id,
        reservation_token,
        cleanup_token,
        |_, _| Ok(()),
    )
}

fn remove_owned_reservation_internal<H>(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    cleanup_token: &str,
    mut phase_hook: H,
) -> io::Result<bool>
where
    H: FnMut(ExactCleanupPhase, &Path) -> io::Result<()>,
{
    let reservation_path = reservation_sidecar_path(destination)?;
    let payload = reservation_payload(transfer_id, reservation_token);
    let mut verify = |file: &mut File| opened_file_matches_payload(file, &payload);
    let mut hook = |phase, verified: &Path| phase_hook(phase, verified);
    match remove_exact_verified_file(
        &reservation_path,
        cleanup_token,
        "reservation",
        &mut verify,
        &mut hook,
    )? {
        ExactRemovalOutcome::Removed => Ok(true),
        ExactRemovalOutcome::Missing => {
            if !destination.parent().is_some_and(|parent| parent.is_dir()) {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "接收目录或磁盘当前不可用",
                ));
            }
            Ok(true)
        }
        ExactRemovalOutcome::NotOwned | ExactRemovalOutcome::ProofLost => Ok(false),
    }
}

#[cfg(test)]
fn remove_owned_reservation_with_hook<H>(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    after_proof: H,
) -> io::Result<bool>
where
    H: FnMut() -> io::Result<()>,
{
    let cleanup_token = uuid::Uuid::new_v4().to_string();
    let mut after_proof = after_proof;
    remove_owned_reservation_internal(
        destination,
        transfer_id,
        reservation_token,
        &cleanup_token,
        move |phase, _| {
            if phase == ExactCleanupPhase::AfterInitialProof {
                after_proof()?;
            }
            Ok(())
        },
    )
}

pub fn remove_owned_reservations_in_directory(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<()> {
    let cleanup_token = uuid::Uuid::new_v4().to_string();
    remove_owned_reservations_in_directory_internal(
        destination,
        transfer_id,
        reservation_token,
        &cleanup_token,
        |_, _| Ok(()),
    )?;
    Ok(())
}

pub(crate) fn remove_owned_reservations_in_directory_with_cleanup_token(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    cleanup_token: &str,
) -> io::Result<bool> {
    remove_owned_reservations_in_directory_internal(
        destination,
        transfer_id,
        reservation_token,
        cleanup_token,
        |_, _| Ok(()),
    )
}

fn remove_owned_reservations_in_directory_internal<H>(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    cleanup_token: &str,
    mut phase_hook: H,
) -> io::Result<bool>
where
    H: FnMut(ExactCleanupPhase, &Path) -> io::Result<()>,
{
    let directory = destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let expected = reservation_payload(transfer_id, reservation_token);
    let mut proof_retained = true;
    #[cfg(target_os = "macos")]
    let quarantine_prefix = owned_cleanup_quarantine_prefix(cleanup_token, "reservation-scan");
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let path = entry.path();
        let mut verify = |file: &mut File| opened_file_matches_payload(file, &expected);
        let mut hook = |phase, verified: &Path| phase_hook(phase, verified);

        #[cfg(target_os = "macos")]
        if file_name.starts_with(&quarantine_prefix) {
            let outcome = remove_existing_macos_quarantine(&path, &mut verify, &mut hook)?;
            if outcome == ExactRemovalOutcome::ProofLost {
                proof_retained = false;
            }
            continue;
        }

        if !file_name.starts_with(".weline-localnet-reservation-") {
            continue;
        }
        let outcome = remove_exact_verified_file(
            &path,
            cleanup_token,
            "reservation-scan",
            &mut verify,
            &mut hook,
        )?;
        if outcome == ExactRemovalOutcome::ProofLost {
            proof_retained = false;
        }
    }
    Ok(proof_retained)
}

#[cfg(test)]
fn remove_owned_reservations_in_directory_with_hook<H>(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    after_proof: H,
) -> io::Result<()>
where
    H: FnMut(&Path) -> io::Result<()>,
{
    let cleanup_token = uuid::Uuid::new_v4().to_string();
    let mut after_proof = after_proof;
    remove_owned_reservations_in_directory_internal(
        destination,
        transfer_id,
        reservation_token,
        &cleanup_token,
        move |phase, path| {
            if phase == ExactCleanupPhase::AfterInitialProof {
                after_proof(path)?;
            }
            Ok(())
        },
    )?;
    Ok(())
}

/*
 * Exact cleanup below keeps proof and namespace authority coupled.  Callers must not replace it
 * with a proof followed by `remove_file(path)`; that reintroduces a path-substitution window.
 */
fn remove_exact_verified_file<V, H>(
    path: &Path,
    _cleanup_token: &str,
    _artifact_kind: &str,
    verify: &mut V,
    phase_hook: &mut H,
) -> io::Result<ExactRemovalOutcome>
where
    V: FnMut(&mut File) -> io::Result<bool>,
    H: FnMut(ExactCleanupPhase, &Path) -> io::Result<()>,
{
    let cleanup_lock = OWNED_ARTIFACT_CLEANUP_LOCK.get_or_init(|| Mutex::new(()));
    let _cleanup_guard = cleanup_lock
        .lock()
        .map_err(|_| io::Error::other("接收文件清理锁不可用"))?;

    #[cfg(windows)]
    {
        let mut file = match open_regular_file_for_delete_no_follow(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(ExactRemovalOutcome::Missing);
            }
            Err(error) if is_no_follow_rejection(&error) => {
                return Ok(ExactRemovalOutcome::NotOwned);
            }
            Err(error) if windows_stage_cleanup_should_defer(&error) => {
                return Ok(ExactRemovalOutcome::ProofLost);
            }
            Err(error) => return Err(error),
        };
        if !verify(&mut file)? {
            return Ok(ExactRemovalOutcome::NotOwned);
        }
        let identity = file_identity(&file)?;
        phase_hook(ExactCleanupPhase::AfterInitialProof, path)?;
        if file_identity(&file)? != identity {
            return Ok(ExactRemovalOutcome::ProofLost);
        }
        match mark_windows_file_link_for_delete(&file) {
            Ok(()) => {}
            Err(error) if windows_stage_cleanup_should_defer(&error) => {
                return Ok(ExactRemovalOutcome::ProofLost);
            }
            Err(error) => return Err(error),
        }
        drop(file);
        return Ok(ExactRemovalOutcome::Removed);
    }

    #[cfg(target_os = "macos")]
    {
        let directory = path
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件清理位置无效"))?;
        let quarantine = owned_cleanup_quarantine_path(path, _cleanup_token, _artifact_kind)?;
        match open_regular_file_no_follow(&quarantine, false) {
            Ok(mut quarantined) => {
                if !verify(&mut quarantined)? {
                    return Ok(ExactRemovalOutcome::ProofLost);
                }
                let identity = file_identity(&quarantined)?;
                phase_hook(ExactCleanupPhase::AfterQuarantineProof, &quarantine)?;
                if file_identity(&quarantined)? != identity
                    || !path_has_file_identity(&quarantine, identity)?
                {
                    return Ok(ExactRemovalOutcome::ProofLost);
                }
                fs::remove_file(&quarantine)?;
                sync_directory(directory)?;
                return Ok(ExactRemovalOutcome::Removed);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) if is_no_follow_rejection(&error) => {
                return Ok(ExactRemovalOutcome::NotOwned);
            }
            Err(error) => return Err(error),
        }
        let mut file = match open_regular_file_no_follow(path, false) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(ExactRemovalOutcome::Missing);
            }
            Err(error) if is_no_follow_rejection(&error) => {
                return Ok(ExactRemovalOutcome::ProofLost);
            }
            Err(error) => return Err(error),
        };
        if !verify(&mut file)? {
            return Ok(ExactRemovalOutcome::NotOwned);
        }
        let identity = file_identity(&file)?;
        phase_hook(ExactCleanupPhase::AfterInitialProof, path)?;
        drop(file);
        match macos_rename_no_replace(path, &quarantine) {
            Ok(()) => sync_directory(directory)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) if macos_exclusive_rename_is_unavailable(&error) => {
                return Ok(ExactRemovalOutcome::ProofLost);
            }
            Err(error) => return Err(error),
        }
        let mut quarantined = match open_regular_file_no_follow(&quarantine, false) {
            Ok(file) => file,
            Err(error) if is_no_follow_rejection(&error) => {
                macos_restore_quarantine_without_overwrite(&quarantine, path, directory)?;
                return Ok(ExactRemovalOutcome::ProofLost);
            }
            Err(error) => return Err(error),
        };
        if file_identity(&quarantined)? != identity || !verify(&mut quarantined)? {
            drop(quarantined);
            macos_restore_quarantine_without_overwrite(&quarantine, path, directory)?;
            return Ok(ExactRemovalOutcome::ProofLost);
        }
        phase_hook(ExactCleanupPhase::AfterQuarantineProof, &quarantine)?;
        if file_identity(&quarantined)? != identity
            || !path_has_file_identity(&quarantine, identity)?
        {
            drop(quarantined);
            macos_restore_quarantine_without_overwrite(&quarantine, path, directory)?;
            return Ok(ExactRemovalOutcome::ProofLost);
        }
        fs::remove_file(&quarantine)?;
        sync_directory(directory)?;
        return Ok(ExactRemovalOutcome::Removed);
    }

    #[cfg(not(any(windows, target_os = "macos")))]
    {
        let _ = (path, _cleanup_token, _artifact_kind, verify, phase_hook);
        Ok(ExactRemovalOutcome::NotOwned)
    }
}

#[cfg(target_os = "macos")]
fn remove_existing_macos_quarantine<V, H>(
    quarantine: &Path,
    verify: &mut V,
    phase_hook: &mut H,
) -> io::Result<ExactRemovalOutcome>
where
    V: FnMut(&mut File) -> io::Result<bool>,
    H: FnMut(ExactCleanupPhase, &Path) -> io::Result<()>,
{
    let cleanup_lock = OWNED_ARTIFACT_CLEANUP_LOCK.get_or_init(|| Mutex::new(()));
    let _cleanup_guard = cleanup_lock
        .lock()
        .map_err(|_| io::Error::other("接收文件清理锁不可用"))?;
    let directory = quarantine
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件清理位置无效"))?;
    let mut file = match open_regular_file_no_follow(quarantine, false) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ExactRemovalOutcome::ProofLost);
        }
        Err(error) if is_no_follow_rejection(&error) => {
            return Ok(ExactRemovalOutcome::ProofLost);
        }
        Err(error) => return Err(error),
    };
    if !verify(&mut file)? {
        return Ok(ExactRemovalOutcome::ProofLost);
    }
    let identity = file_identity(&file)?;
    phase_hook(ExactCleanupPhase::AfterQuarantineProof, quarantine)?;
    if file_identity(&file)? != identity || !path_has_file_identity(quarantine, identity)? {
        return Ok(ExactRemovalOutcome::ProofLost);
    }
    fs::remove_file(quarantine)?;
    sync_directory(directory)?;
    Ok(ExactRemovalOutcome::Removed)
}

fn opened_file_matches_payload(file: &mut File, expected: &[u8]) -> io::Result<bool> {
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != expected.len() as u64 {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut actual = vec![0_u8; expected.len()];
    match file.read_exact(&mut actual) {
        Ok(()) => Ok(actual == expected),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error),
    }
}

fn opened_partial_marker_has_prefix(
    file: &mut File,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<bool> {
    if file.metadata()?.len() > 8 * 1024 {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut payload = String::new();
    file.read_to_string(&mut payload)?;
    let expected_prefix = format!("{PARTIAL_OWNERSHIP_PREFIX}{transfer_id}:{reservation_token}:");
    Ok(payload.starts_with(&expected_prefix) && payload.ends_with('\n'))
}

#[cfg(any(target_os = "macos", test))]
fn owned_cleanup_quarantine_prefix(cleanup_token: &str, artifact_kind: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"weline-localnet-owned-cleanup-v2\0");
    hasher.update(cleanup_token.as_bytes());
    hasher.update(b"\0");
    hasher.update(artifact_kind.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!(".weline-localnet-cleanup-{}-", &digest[..32])
}

#[cfg(target_os = "macos")]
pub(crate) fn owned_cleanup_quarantine_path(
    path: &Path,
    cleanup_token: &str,
    artifact_kind: &str,
) -> io::Result<PathBuf> {
    let directory = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件清理位置无效"))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件清理名称无效"))?;
    use std::os::unix::ffi::OsStrExt as _;
    let file_digest = hex::encode(Sha256::digest(file_name.as_bytes()));
    Ok(directory.join(format!(
        "{}{}",
        owned_cleanup_quarantine_prefix(cleanup_token, artifact_kind),
        &file_digest[..16]
    )))
}

pub fn resumable_partial_path(destination: &Path, transfer_id: &str) -> io::Result<PathBuf> {
    resumable_partial_candidate(destination, transfer_id, 0)
}

fn resumable_partial_candidate(
    destination: &Path,
    transfer_id: &str,
    sequence: u32,
) -> io::Result<PathBuf> {
    let directory = destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    if transfer_id.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "文件传输编号无效",
        ));
    }
    let digest = hex::encode(Sha256::digest(transfer_id.as_bytes()));
    let suffix = if sequence == 0 {
        String::new()
    } else {
        format!("-{sequence}")
    };
    Ok(directory.join(format!(
        ".weline-localnet-partial-{}{suffix}.part",
        &digest[..24]
    )))
}

pub fn reserve_resumable_partial(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<PathBuf> {
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "部分文件保存位置无效"))?;
    if !parent.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "接收目录或磁盘当前不可用",
        ));
    }
    if !reservation_is_owned(destination, transfer_id, reservation_token)? {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "部分文件缺少匹配的目标文件占位凭据",
        ));
    }
    let mut sequence = 0_u32;
    loop {
        let partial = resumable_partial_candidate(destination, transfer_id, sequence)?;
        if resumable_partial_is_owned(&partial, destination, transfer_id, reservation_token)? {
            return Ok(partial);
        }
        let owner_path = partial_owner_sidecar_path(&partial)?;
        if partial.try_exists()? || owner_path.try_exists()? {
            sequence = sequence.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::AlreadyExists, "没有可用的部分文件名称")
            })?;
            continue;
        }
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&partial)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                sequence = sequence.checked_add(1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::AlreadyExists, "没有可用的部分文件名称")
                })?;
                continue;
            }
            Err(error) => return Err(error),
        };
        file.sync_all()?;
        let identity = file_identity(&file)?;
        let payload = partial_owner_payload(transfer_id, reservation_token, identity);
        match write_new_marker(&owner_path, &payload) {
            Ok(()) => return Ok(partial),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                sequence = sequence.checked_add(1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::AlreadyExists, "没有可用的部分文件名称")
                })?;
            }
            Err(error) => return Err(error),
        }
    }
}

pub fn resumable_partial_is_owned(
    partial: &Path,
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<bool> {
    if !is_resumable_partial_candidate(partial, destination, transfer_id)? || partial == destination
    {
        return Ok(false);
    }
    Ok(open_identity_verified_partial(partial, transfer_id, reservation_token, false)?.is_some())
}

pub fn open_owned_resumable_partial_file(
    partial: &Path,
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<Option<File>> {
    if !is_resumable_partial_candidate(partial, destination, transfer_id)? || partial == destination
    {
        return Ok(None);
    }
    open_identity_verified_partial(partial, transfer_id, reservation_token, true)
}

pub fn remove_owned_partial(
    partial: &Path,
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<bool> {
    let cleanup_token = uuid::Uuid::new_v4().to_string();
    remove_owned_partial_internal(
        partial,
        destination,
        transfer_id,
        reservation_token,
        &cleanup_token,
        |_, _| Ok(()),
    )
}

pub(crate) fn remove_owned_partial_with_cleanup_token(
    partial: &Path,
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    cleanup_token: &str,
) -> io::Result<bool> {
    remove_owned_partial_internal(
        partial,
        destination,
        transfer_id,
        reservation_token,
        cleanup_token,
        |_, _| Ok(()),
    )
}

fn remove_owned_partial_internal<H>(
    partial: &Path,
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    cleanup_token: &str,
    mut phase_hook: H,
) -> io::Result<bool>
where
    H: FnMut(ExactCleanupPhase, &Path) -> io::Result<()>,
{
    if !is_resumable_partial_candidate(partial, destination, transfer_id)? || partial == destination
    {
        return Ok(false);
    }
    let owner_path = partial_owner_sidecar_path(partial)?;
    let mut verify = |file: &mut File| {
        let identity = file_identity(file)?;
        let payload = partial_owner_payload(transfer_id, reservation_token, identity);
        marker_matches(&owner_path, &payload)
    };
    let mut hook = |phase, verified: &Path| phase_hook(phase, verified);
    match remove_exact_verified_file(partial, cleanup_token, "partial", &mut verify, &mut hook)? {
        ExactRemovalOutcome::Removed => {
            remove_owned_partial_marker_after_file_cleanup_with_cleanup_token(
                partial,
                destination,
                transfer_id,
                reservation_token,
                cleanup_token,
            )
        }
        ExactRemovalOutcome::Missing => {
            if !partial.parent().is_some_and(|parent| parent.is_dir()) {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "接收目录或磁盘当前不可用",
                ));
            }
            remove_owned_partial_marker_after_file_cleanup_with_cleanup_token(
                partial,
                destination,
                transfer_id,
                reservation_token,
                cleanup_token,
            )
        }
        ExactRemovalOutcome::NotOwned | ExactRemovalOutcome::ProofLost => Ok(false),
    }
}

#[cfg(test)]
fn remove_owned_partial_with_hook<H>(
    partial: &Path,
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    after_proof: H,
) -> io::Result<bool>
where
    H: FnMut() -> io::Result<()>,
{
    let cleanup_token = uuid::Uuid::new_v4().to_string();
    let mut after_proof = after_proof;
    remove_owned_partial_internal(
        partial,
        destination,
        transfer_id,
        reservation_token,
        &cleanup_token,
        move |phase, _| {
            if phase == ExactCleanupPhase::AfterInitialProof {
                after_proof()?;
            }
            Ok(())
        },
    )
}

pub(crate) fn remove_owned_partial_marker_after_file_cleanup_with_cleanup_token(
    partial: &Path,
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    cleanup_token: &str,
) -> io::Result<bool> {
    remove_owned_partial_marker_after_file_cleanup_internal(
        partial,
        destination,
        transfer_id,
        reservation_token,
        cleanup_token,
        |_, _| Ok(()),
    )
}

fn remove_owned_partial_marker_after_file_cleanup_internal<H>(
    partial: &Path,
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    cleanup_token: &str,
    mut phase_hook: H,
) -> io::Result<bool>
where
    H: FnMut(ExactCleanupPhase, &Path) -> io::Result<()>,
{
    if !is_resumable_partial_candidate(partial, destination, transfer_id)? {
        return Ok(false);
    }
    match fs::symlink_metadata(partial) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Ok(_) => return Ok(false),
        Err(error) => return Err(error),
    }
    let marker = partial_owner_sidecar_path(partial)?;
    let mut verify =
        |file: &mut File| opened_partial_marker_has_prefix(file, transfer_id, reservation_token);
    let mut hook = |phase, verified: &Path| phase_hook(phase, verified);
    match remove_exact_verified_file(
        &marker,
        cleanup_token,
        "partial-owner",
        &mut verify,
        &mut hook,
    )? {
        ExactRemovalOutcome::Removed | ExactRemovalOutcome::Missing => Ok(true),
        ExactRemovalOutcome::NotOwned | ExactRemovalOutcome::ProofLost => Ok(false),
    }
}

#[cfg(test)]
fn remove_owned_partial_marker_after_file_cleanup_with_hook<H>(
    partial: &Path,
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    after_proof: H,
) -> io::Result<bool>
where
    H: FnMut() -> io::Result<()>,
{
    let cleanup_token = uuid::Uuid::new_v4().to_string();
    let mut after_proof = after_proof;
    remove_owned_partial_marker_after_file_cleanup_internal(
        partial,
        destination,
        transfer_id,
        reservation_token,
        &cleanup_token,
        move |phase, _| {
            if phase == ExactCleanupPhase::AfterInitialProof {
                after_proof()?;
            }
            Ok(())
        },
    )
}

#[cfg(test)]
pub fn finalize_reserved_receive_durable(
    partial: &Path,
    reserved_destination: &Path,
    file_name: &str,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<FinalizedReceive> {
    finalize_reserved_receive_durable_with_hooks(
        partial,
        reserved_destination,
        file_name,
        transfer_id,
        reservation_token,
        |_, _| Ok(()),
        |_| Ok(()),
    )
}

pub fn finalize_reserved_receive_durable_with_hooks<S, H>(
    partial: &Path,
    reserved_destination: &Path,
    file_name: &str,
    transfer_id: &str,
    reservation_token: &str,
    mut persist_switch: S,
    mut phase_hook: H,
) -> io::Result<FinalizedReceive>
where
    S: FnMut(&Path, &Path) -> io::Result<()>,
    H: FnMut(FinalizationPhase) -> io::Result<()>,
{
    let mut hard_link = |source: &Path, destination: &Path| fs::hard_link(source, destination);
    finalize_reserved_receive_durable_internal(
        partial,
        reserved_destination,
        file_name,
        transfer_id,
        reservation_token,
        &mut persist_switch,
        &mut phase_hook,
        &mut hard_link,
    )
}

#[cfg(test)]
pub(crate) fn finalize_reserved_receive_copy_fallback_with_hooks<S, L, H>(
    partial: &Path,
    reserved_destination: &Path,
    file_name: &str,
    transfer_id: &str,
    reservation_token: &str,
    mut persist_switch: S,
    mut hard_link: L,
    mut phase_hook: H,
) -> io::Result<FinalizedReceive>
where
    S: FnMut(&Path, &Path) -> io::Result<()>,
    L: FnMut(&Path, &Path) -> io::Result<()>,
    H: FnMut(FinalizationPhase) -> io::Result<()>,
{
    finalize_reserved_receive_durable_internal(
        partial,
        reserved_destination,
        file_name,
        transfer_id,
        reservation_token,
        &mut persist_switch,
        &mut phase_hook,
        &mut hard_link,
    )
}

fn finalize_reserved_receive_durable_internal<S, H, L>(
    partial: &Path,
    reserved_destination: &Path,
    file_name: &str,
    transfer_id: &str,
    reservation_token: &str,
    persist_switch: &mut S,
    phase_hook: &mut H,
    hard_link: &mut L,
) -> io::Result<FinalizedReceive>
where
    S: FnMut(&Path, &Path) -> io::Result<()>,
    H: FnMut(FinalizationPhase) -> io::Result<()>,
    L: FnMut(&Path, &Path) -> io::Result<()>,
{
    let directory = reserved_destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let partial_file =
        open_identity_verified_partial(partial, transfer_id, reservation_token, false)?
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::PermissionDenied, "部分文件所有权已变化")
            })?;
    let partial_identity = file_identity(&partial_file)?;
    let mut candidate =
        if reservation_is_owned(reserved_destination, transfer_id, reservation_token)? {
            reserved_destination.to_path_buf()
        } else {
            reserve_available_receive_path(directory, file_name, transfer_id, reservation_token)?
        };
    let mut previous = Vec::new();
    if let Some(record) =
        read_finalization_record(reserved_destination, transfer_id, reservation_token)?
    {
        if record.state == FinalizationState::CopyPrepared {
            #[cfg(target_os = "macos")]
            let (record, copy_cleanup_token) =
                persist_copy_cleanup_token_before_namespace_mutation(
                    reserved_destination,
                    transfer_id,
                    reservation_token,
                    record,
                )?;
            candidate = directory.join(record.candidate);
            previous = record
                .previous
                .into_iter()
                .map(|name| directory.join(name))
                .collect();
            let retained_legacy_stage = if let Some(staged) = record.staged.as_deref() {
                let staged = directory.join(staged);
                let expected_identity = record.staged_identity.unwrap_or(record.identity);
                match open_regular_file_no_follow(&staged, false) {
                    Ok(file) => (file_identity(&file)? == expected_identity)
                        .then_some((staged, expected_identity)),
                    Err(error) if is_no_follow_rejection(&error) => None,
                    Err(error) => return Err(error),
                }
            } else {
                None
            };

            #[cfg(target_os = "macos")]
            {
                if !retire_copy_candidate_generation(
                    &candidate,
                    record.identity,
                    &copy_cleanup_token,
                    phase_hook,
                )? {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "复制恢复时无法证明并清理未完成候选文件",
                    ));
                }
                return prepare_copy_candidate(
                    &partial_file,
                    reserved_destination,
                    candidate,
                    previous,
                    retained_legacy_stage,
                    file_name,
                    transfer_id,
                    reservation_token,
                    persist_switch,
                    phase_hook,
                );
            }

            #[cfg(not(target_os = "macos"))]
            {
                if reservation_is_owned(&candidate, transfer_id, reservation_token)? {
                    match open_regular_file_no_follow(&candidate, true) {
                        Ok(candidate_file)
                            if file_identity(&candidate_file)? == record.identity =>
                        {
                            return materialize_copy_candidate(
                                &partial_file,
                                candidate_file,
                                reserved_destination,
                                candidate,
                                previous,
                                retained_legacy_stage,
                                file_name,
                                transfer_id,
                                reservation_token,
                                persist_switch,
                                phase_hook,
                            );
                        }
                        Ok(_) => {}
                        Err(error) if is_no_follow_rejection(&error) => {}
                        Err(error) => return Err(error),
                    }
                }
                return prepare_copy_candidate(
                    &partial_file,
                    reserved_destination,
                    candidate,
                    previous,
                    retained_legacy_stage,
                    file_name,
                    transfer_id,
                    reservation_token,
                    persist_switch,
                    phase_hook,
                );
            }
        }
    }

    loop {
        append_finalization_record(
            reserved_destination,
            transfer_id,
            reservation_token,
            &candidate,
            &previous,
            partial_identity,
            FinalizationState::Prepared,
        )?;
        if !path_has_file_identity(partial, partial_identity)? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "部分文件所有权在完成前已变化",
            ));
        }
        let link_result = hard_link(partial, &candidate);
        match link_result {
            Ok(()) => {
                if !path_has_file_identity(&candidate, partial_identity)? {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "部分文件所有权在完成时已变化",
                    ));
                }
                phase_hook(FinalizationPhase::AfterFinalNameCreation)?;
                append_finalization_record(
                    reserved_destination,
                    transfer_id,
                    reservation_token,
                    &candidate,
                    &previous,
                    partial_identity,
                    FinalizationState::Created,
                )?;
                phase_hook(FinalizationPhase::AfterFinalJournalSync)?;
                return Ok(FinalizedReceive {
                    path: candidate,
                    reservation_released: false,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                phase_hook(FinalizationPhase::BeforeReplacementReserve)?;
                let replacement = reserve_available_receive_path(
                    directory,
                    file_name,
                    transfer_id,
                    reservation_token,
                )?;
                phase_hook(FinalizationPhase::AfterReplacementReserve)?;
                previous.push(candidate.clone());
                append_finalization_record(
                    reserved_destination,
                    transfer_id,
                    reservation_token,
                    &replacement,
                    &previous,
                    partial_identity,
                    FinalizationState::Prepared,
                )?;
                phase_hook(FinalizationPhase::AfterJournalUpdate)?;
                persist_switch(&candidate, &replacement)?;
                phase_hook(FinalizationPhase::AfterMetadataSwitch)?;
                remove_owned_reservation(&candidate, transfer_id, reservation_token)?;
                phase_hook(FinalizationPhase::AfterOldReservationRelease)?;
                candidate = replacement;
            }
            Err(_) => {
                return finalize_copy_fallback(
                    &partial_file,
                    reserved_destination,
                    candidate,
                    previous,
                    file_name,
                    transfer_id,
                    reservation_token,
                    persist_switch,
                    phase_hook,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn finalize_copy_fallback<S, H>(
    partial_file: &File,
    reserved_destination: &Path,
    candidate: PathBuf,
    previous: Vec<PathBuf>,
    file_name: &str,
    transfer_id: &str,
    reservation_token: &str,
    persist_switch: &mut S,
    phase_hook: &mut H,
) -> io::Result<FinalizedReceive>
where
    S: FnMut(&Path, &Path) -> io::Result<()>,
    H: FnMut(FinalizationPhase) -> io::Result<()>,
{
    prepare_copy_candidate(
        partial_file,
        reserved_destination,
        candidate,
        previous,
        None,
        file_name,
        transfer_id,
        reservation_token,
        persist_switch,
        phase_hook,
    )
}

#[cfg(test)]
pub(crate) fn reserve_finalization_stage(
    directory: &Path,
    transfer_id: &str,
) -> io::Result<(PathBuf, File)> {
    let digest = hex::encode(Sha256::digest(transfer_id.as_bytes()));
    let mut sequence = 0_u32;
    loop {
        let suffix = if sequence == 0 {
            String::new()
        } else {
            format!("-{sequence}")
        };
        let staged = directory.join(format!(
            ".weline-localnet-finalize-stage-{}{suffix}",
            &digest[..24]
        ));
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&staged)
        {
            Ok(file) => {
                file.sync_all()?;
                return Ok((staged, file));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                sequence = sequence.checked_add(1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::AlreadyExists, "没有可用的复制暂存文件名")
                })?;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
pub(crate) fn create_legacy_finalized_stage_fixture(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    payload: &[u8],
) -> io::Result<PathBuf> {
    let directory = destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let (staged, mut staged_file) = reserve_finalization_stage(directory, transfer_id)?;
    staged_file.write_all(payload)?;
    staged_file.flush()?;
    staged_file.sync_all()?;
    let staged_identity = file_identity(&staged_file)?;
    fs::hard_link(&staged, destination)?;
    append_finalization_record_with_stage(
        destination,
        transfer_id,
        reservation_token,
        destination,
        &[],
        Some(&staged),
        Some(staged_identity),
        staged_identity,
        FinalizationState::Created,
    )?;
    Ok(staged)
}

#[allow(clippy::too_many_arguments)]
fn prepare_copy_candidate<S, H>(
    partial_file: &File,
    reserved_destination: &Path,
    mut candidate: PathBuf,
    mut previous: Vec<PathBuf>,
    retained_legacy_stage: Option<(PathBuf, FileIdentity)>,
    file_name: &str,
    transfer_id: &str,
    reservation_token: &str,
    persist_switch: &mut S,
    phase_hook: &mut H,
) -> io::Result<FinalizedReceive>
where
    S: FnMut(&Path, &Path) -> io::Result<()>,
    H: FnMut(FinalizationPhase) -> io::Result<()>,
{
    let directory = reserved_destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let mut switch_from = None;

    loop {
        if !reservation_is_owned(&candidate, transfer_id, reservation_token)? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "复制完成前目标文件占位所有权已变化",
            ));
        }
        match create_new_regular_file_no_follow(&candidate) {
            Ok(candidate_file) => {
                candidate_file.sync_all()?;
                if !reservation_is_owned(&candidate, transfer_id, reservation_token)? {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "复制目标创建后占位所有权已变化",
                    ));
                }
                let candidate_identity = file_identity(&candidate_file)?;
                if let Some(previous_candidate) = switch_from.take() {
                    if !previous.contains(&previous_candidate) {
                        previous.push(previous_candidate.clone());
                    }
                    append_finalization_record_with_stage(
                        reserved_destination,
                        transfer_id,
                        reservation_token,
                        &candidate,
                        &previous,
                        retained_legacy_stage
                            .as_ref()
                            .map(|(path, _)| path.as_path()),
                        retained_legacy_stage
                            .as_ref()
                            .map(|(_, identity)| *identity),
                        candidate_identity,
                        FinalizationState::CopyPrepared,
                    )?;
                    phase_hook(FinalizationPhase::AfterJournalUpdate)?;
                    persist_switch(&previous_candidate, &candidate)?;
                    phase_hook(FinalizationPhase::AfterMetadataSwitch)?;
                    remove_owned_reservation(&previous_candidate, transfer_id, reservation_token)?;
                    phase_hook(FinalizationPhase::AfterOldReservationRelease)?;
                } else {
                    append_finalization_record_with_stage(
                        reserved_destination,
                        transfer_id,
                        reservation_token,
                        &candidate,
                        &previous,
                        retained_legacy_stage
                            .as_ref()
                            .map(|(path, _)| path.as_path()),
                        retained_legacy_stage
                            .as_ref()
                            .map(|(_, identity)| *identity),
                        candidate_identity,
                        FinalizationState::CopyPrepared,
                    )?;
                }
                return materialize_copy_candidate(
                    partial_file,
                    candidate_file,
                    reserved_destination,
                    candidate,
                    previous,
                    retained_legacy_stage,
                    file_name,
                    transfer_id,
                    reservation_token,
                    persist_switch,
                    phase_hook,
                );
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                phase_hook(FinalizationPhase::BeforeReplacementReserve)?;
                if switch_from.is_none() {
                    switch_from = Some(candidate.clone());
                } else if !previous.contains(&candidate) {
                    previous.push(candidate.clone());
                }
                let replacement = reserve_available_receive_path(
                    directory,
                    file_name,
                    transfer_id,
                    reservation_token,
                )?;
                phase_hook(FinalizationPhase::AfterReplacementReserve)?;
                candidate = replacement;
            }
            Err(error) => return Err(error),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn materialize_copy_candidate<S, H>(
    partial_file: &File,
    mut candidate_file: File,
    reserved_destination: &Path,
    candidate: PathBuf,
    previous: Vec<PathBuf>,
    retained_legacy_stage: Option<(PathBuf, FileIdentity)>,
    file_name: &str,
    transfer_id: &str,
    reservation_token: &str,
    persist_switch: &mut S,
    phase_hook: &mut H,
) -> io::Result<FinalizedReceive>
where
    S: FnMut(&Path, &Path) -> io::Result<()>,
    H: FnMut(FinalizationPhase) -> io::Result<()>,
{
    if !reservation_is_owned(&candidate, transfer_id, reservation_token)? {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "复制完成前目标文件占位所有权已变化",
        ));
    }
    let candidate_identity = file_identity(&candidate_file)?;
    let source_length = partial_file.metadata()?.len();
    let copy_result = (|| {
        phase_hook(FinalizationPhase::BeforeCopy)?;
        candidate_file.set_len(0)?;
        candidate_file.seek(SeekFrom::Start(0))?;
        let mut source = partial_file;
        source.seek(SeekFrom::Start(0))?;
        let mut buffer = vec![0_u8; 64 * 1024];
        let mut injected_during_copy = false;
        loop {
            let read = source.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            candidate_file.write_all(&buffer[..read])?;
            if !injected_during_copy {
                injected_during_copy = true;
                phase_hook(FinalizationPhase::DuringCopy)?;
            }
        }
        if !injected_during_copy {
            phase_hook(FinalizationPhase::DuringCopy)?;
        }
        candidate_file.flush()?;
        candidate_file.sync_all()?;
        if candidate_file.metadata()?.len() != source_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "复制目标文件长度不完整",
            ));
        }
        Ok(())
    })();
    if let Err(error) = copy_result {
        if is_storage_full_error(&error) {
            let candidate_name = candidate
                .file_name()
                .and_then(|value| value.to_str())
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "复制目标文件名无效"))?;
            let copy_cleanup_token =
                stable_copy_cleanup_token(transfer_id, reservation_token, candidate_name);
            return match retire_incomplete_copy_candidate(
                candidate_file,
                &candidate,
                candidate_identity,
                &copy_cleanup_token,
                phase_hook,
            ) {
                Ok(true) => Err(error),
                Ok(false) => Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "磁盘空间不足后无法证明并清理未完成的目标副本",
                )),
                Err(cleanup_error) => Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("磁盘空间不足，且未完成目标副本无法安全清理：{cleanup_error}"),
                )),
            };
        }
        return Err(error);
    }
    phase_hook(FinalizationPhase::AfterCopySync)?;
    if !reservation_is_owned(&candidate, transfer_id, reservation_token)?
        || !path_has_file_identity(&candidate, candidate_identity)?
    {
        return prepare_copy_candidate(
            partial_file,
            reserved_destination,
            candidate,
            previous,
            retained_legacy_stage,
            file_name,
            transfer_id,
            reservation_token,
            persist_switch,
            phase_hook,
        );
    }
    phase_hook(FinalizationPhase::AfterFinalNameCreation)?;
    phase_hook(FinalizationPhase::BeforeCopyMaterializedJournal)?;
    append_finalization_record_with_stage(
        reserved_destination,
        transfer_id,
        reservation_token,
        &candidate,
        &previous,
        retained_legacy_stage
            .as_ref()
            .map(|(path, _)| path.as_path()),
        retained_legacy_stage
            .as_ref()
            .map(|(_, identity)| *identity),
        candidate_identity,
        FinalizationState::Created,
    )?;
    phase_hook(FinalizationPhase::AfterFinalJournalSync)?;
    Ok(FinalizedReceive {
        path: candidate,
        reservation_released: false,
    })
}

pub(crate) fn is_storage_full_error(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::StorageFull {
        return true;
    }
    #[cfg(windows)]
    if matches!(error.raw_os_error(), Some(39 | 112)) {
        return true;
    }
    #[cfg(unix)]
    if error.raw_os_error() == Some(libc::ENOSPC) {
        return true;
    }
    false
}

fn retire_incomplete_copy_candidate<H>(
    candidate_file: File,
    candidate: &Path,
    expected_identity: FileIdentity,
    cleanup_token: &str,
    phase_hook: &mut H,
) -> io::Result<bool>
where
    H: FnMut(FinalizationPhase) -> io::Result<()>,
{
    if file_identity(&candidate_file)? != expected_identity {
        return Ok(false);
    }

    #[cfg(windows)]
    {
        let _ = (cleanup_token, phase_hook);
        if !path_has_file_identity(candidate, expected_identity)? {
            return Ok(false);
        }
        mark_windows_file_link_for_delete(&candidate_file)?;
        drop(candidate_file);
        return match open_regular_file_no_follow(candidate, false) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
            Ok(file) if file_identity(&file)? != expected_identity => Ok(true),
            Ok(_) => Ok(false),
            Err(error) if is_no_follow_rejection(&error) => Ok(true),
            Err(error) => Err(error),
        };
    }

    #[cfg(target_os = "macos")]
    {
        drop(candidate_file);
        return retire_copy_candidate_generation(
            candidate,
            expected_identity,
            cleanup_token,
            phase_hook,
        );
    }

    #[cfg(not(any(windows, target_os = "macos")))]
    {
        let _ = (
            candidate_file,
            candidate,
            expected_identity,
            cleanup_token,
            phase_hook,
        );
        Ok(false)
    }
}

#[cfg(target_os = "macos")]
fn retire_copy_candidate_generation<H>(
    candidate: &Path,
    expected_identity: FileIdentity,
    cleanup_token: &str,
    phase_hook: &mut H,
) -> io::Result<bool>
where
    H: FnMut(FinalizationPhase) -> io::Result<()>,
{
    let mut verify = |file: &mut File| Ok(file_identity(file)? == expected_identity);
    let mut cleanup_hook = |phase: ExactCleanupPhase, _path: &Path| {
        if phase == ExactCleanupPhase::AfterQuarantineProof {
            phase_hook(FinalizationPhase::AfterCopyQuarantineProof)?;
        }
        Ok(())
    };
    match remove_exact_verified_file(
        candidate,
        cleanup_token,
        "copy-candidate",
        &mut verify,
        &mut cleanup_hook,
    )? {
        ExactRemovalOutcome::Removed | ExactRemovalOutcome::Missing => Ok(true),
        ExactRemovalOutcome::NotOwned | ExactRemovalOutcome::ProofLost => Ok(false),
    }
}

pub fn owned_finalized_receive_path(
    reserved_destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<Option<PathBuf>> {
    let Some(record) =
        read_finalization_record(reserved_destination, transfer_id, reservation_token)?
    else {
        return Ok(None);
    };
    if record.state == FinalizationState::CopyPrepared {
        return Ok(None);
    }
    let directory = reserved_destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let candidate = directory.join(record.candidate);
    if !reservation_is_owned(&candidate, transfer_id, reservation_token)? {
        return Ok(None);
    }
    let candidate_file = match open_regular_file_no_follow(&candidate, false) {
        Ok(file) => file,
        Err(error) if is_no_follow_rejection(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    if file_identity(&candidate_file)? != record.identity {
        return Ok(None);
    }
    Ok(Some(candidate))
}

pub fn owned_finalization_reservations(
    reserved_destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<Vec<PathBuf>> {
    let Some(record) =
        read_finalization_record(reserved_destination, transfer_id, reservation_token)?
    else {
        return Ok(Vec::new());
    };
    let directory = reserved_destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let mut paths = record
        .previous
        .iter()
        .map(|name| directory.join(name))
        .collect::<Vec<_>>();
    paths.push(directory.join(record.candidate));
    Ok(paths)
}

pub fn remove_owned_finalization_marker(
    reserved_destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<bool> {
    if read_finalization_record(reserved_destination, transfer_id, reservation_token)?.is_none() {
        return Ok(false);
    }
    fs::remove_file(finalization_marker_path(reserved_destination, transfer_id)?)?;
    Ok(true)
}

pub fn remove_owned_finalization_stage(
    reserved_destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<bool> {
    remove_owned_finalization_stage_internal(
        reserved_destination,
        transfer_id,
        reservation_token,
        |_| Ok(()),
    )
}

#[cfg(test)]
fn remove_owned_finalization_stage_with_hook<H>(
    reserved_destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    after_verified_open: H,
) -> io::Result<bool>
where
    H: FnOnce() -> io::Result<()>,
{
    let mut after_verified_open = Some(after_verified_open);
    remove_owned_finalization_stage_internal(
        reserved_destination,
        transfer_id,
        reservation_token,
        move |phase| {
            if phase == LegacyStageCleanupPhase::AfterPreparation {
                if let Some(hook) = after_verified_open.take() {
                    hook()?;
                }
            }
            Ok(())
        },
    )
}

#[cfg(test)]
fn remove_owned_finalization_stage_with_phase_hook<H>(
    reserved_destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    phase_hook: H,
) -> io::Result<bool>
where
    H: FnMut(LegacyStageCleanupPhase) -> io::Result<()>,
{
    remove_owned_finalization_stage_internal(
        reserved_destination,
        transfer_id,
        reservation_token,
        phase_hook,
    )
}

fn remove_owned_finalization_stage_internal<H>(
    reserved_destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    mut phase_hook: H,
) -> io::Result<bool>
where
    H: FnMut(LegacyStageCleanupPhase) -> io::Result<()>,
{
    let cleanup_lock = LEGACY_STAGE_CLEANUP_LOCK.get_or_init(|| Mutex::new(()));
    let _cleanup_guard = cleanup_lock
        .lock()
        .map_err(|_| io::Error::other("遗留暂存文件清理锁不可用"))?;
    let Some(record) =
        read_finalization_record(reserved_destination, transfer_id, reservation_token)?
    else {
        return Ok(true);
    };
    let Some(staged) = record.staged.as_deref() else {
        return Ok(true);
    };
    if record
        .stage_cleanup
        .as_ref()
        .is_some_and(|cleanup| cleanup.state == LegacyStageCleanupState::Removed)
    {
        return Ok(true);
    }
    let directory = reserved_destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let staged = directory.join(staged);
    let expected_identity = record.staged_identity.unwrap_or(record.identity);

    #[cfg(windows)]
    {
        return remove_owned_finalization_stage_windows(
            reserved_destination,
            transfer_id,
            &directory,
            &staged,
            expected_identity,
            record,
            &mut phase_hook,
        );
    }

    #[cfg(target_os = "macos")]
    {
        return remove_owned_finalization_stage_macos(
            reserved_destination,
            transfer_id,
            &directory,
            &staged,
            expected_identity,
            record,
            &mut phase_hook,
        );
    }

    #[cfg(not(any(windows, target_os = "macos")))]
    {
        let _ = (record, expected_identity, &mut phase_hook);
        Ok(false)
    }
}

fn complete_legacy_stage_cleanup_record<H>(
    reserved_destination: &Path,
    transfer_id: &str,
    record: &FinalizationRecord,
    expected_identity: FileIdentity,
    quarantine: Option<String>,
    phase_hook: &mut H,
) -> io::Result<bool>
where
    H: FnMut(LegacyStageCleanupPhase) -> io::Result<()>,
{
    append_legacy_stage_cleanup_record(
        reserved_destination,
        transfer_id,
        record,
        LegacyStageCleanupRecord {
            identity: expected_identity,
            quarantine,
            state: LegacyStageCleanupState::Removed,
        },
    )?;
    phase_hook(LegacyStageCleanupPhase::AfterJournalComplete)?;
    Ok(true)
}

#[cfg(windows)]
#[allow(clippy::too_many_arguments)]
fn remove_owned_finalization_stage_windows<H>(
    reserved_destination: &Path,
    transfer_id: &str,
    directory: &Path,
    staged: &Path,
    expected_identity: FileIdentity,
    mut record: FinalizationRecord,
    phase_hook: &mut H,
) -> io::Result<bool>
where
    H: FnMut(LegacyStageCleanupPhase) -> io::Result<()>,
{
    let staged_file = match open_regular_file_for_delete_no_follow(staged) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound && directory.is_dir() => {
            let Some(cleanup) = record.stage_cleanup.as_ref() else {
                return Ok(false);
            };
            if cleanup.state != LegacyStageCleanupState::Prepared {
                return Ok(false);
            }
            return complete_legacy_stage_cleanup_record(
                reserved_destination,
                transfer_id,
                &record,
                expected_identity,
                None,
                phase_hook,
            );
        }
        Err(error) if is_no_follow_rejection(&error) => return Ok(false),
        Err(error) if windows_stage_cleanup_should_defer(&error) => return Ok(false),
        Err(error) => return Err(error),
    };
    if file_identity(&staged_file)? != expected_identity {
        return Ok(false);
    }
    phase_hook(LegacyStageCleanupPhase::AfterOpenIdentityVerified)?;
    match path_has_file_identity(staged, expected_identity) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(error) if windows_stage_cleanup_should_defer(&error) => return Ok(false),
        Err(error) => return Err(error),
    }
    if record.stage_cleanup.is_none() {
        record = append_legacy_stage_cleanup_record(
            reserved_destination,
            transfer_id,
            &record,
            LegacyStageCleanupRecord {
                identity: expected_identity,
                quarantine: None,
                state: LegacyStageCleanupState::Prepared,
            },
        )?;
    }
    phase_hook(LegacyStageCleanupPhase::AfterPreparation)?;
    match path_has_file_identity(staged, expected_identity) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(error) if windows_stage_cleanup_should_defer(&error) => return Ok(false),
        Err(error) => return Err(error),
    }
    match mark_windows_file_link_for_delete(&staged_file) {
        Ok(()) => {}
        Err(error) if windows_stage_cleanup_should_defer(&error) => return Ok(false),
        Err(error) => return Err(error),
    }
    phase_hook(LegacyStageCleanupPhase::AfterNamespaceMutation)?;
    drop(staged_file);

    match open_regular_file_no_follow(staged, false) {
        Err(error) if error.kind() == io::ErrorKind::NotFound && directory.is_dir() => {
            complete_legacy_stage_cleanup_record(
                reserved_destination,
                transfer_id,
                &record,
                expected_identity,
                None,
                phase_hook,
            )
        }
        Ok(file) if file_identity(&file)? == expected_identity => Ok(false),
        Ok(_) => Ok(false),
        Err(error) if is_no_follow_rejection(&error) => Ok(false),
        Err(error) if windows_stage_cleanup_should_defer(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(windows)]
fn open_regular_file_for_delete_no_follow(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_READ, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE,
    };

    let file = OpenOptions::new()
        .read(true)
        .access_mode(DELETE | FILE_GENERIC_READ)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let information = windows_file_information(&file)?;
    if information.dwFileAttributes & (FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_DIRECTORY) != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "遗留暂存路径是重解析点或目录",
        ));
    }
    Ok(file)
}

#[cfg(windows)]
fn mark_windows_file_link_for_delete(file: &File) -> io::Result<()> {
    use std::{mem::size_of, os::windows::io::AsRawHandle as _};
    use windows_sys::Win32::{
        Foundation::{ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER, ERROR_NOT_SUPPORTED},
        Storage::FileSystem::{
            FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_POSIX_SEMANTICS,
            FILE_DISPOSITION_INFO, FILE_DISPOSITION_INFO_EX, FileDispositionInfo,
            FileDispositionInfoEx, SetFileInformationByHandle,
        },
    };

    let disposition = FILE_DISPOSITION_INFO_EX {
        Flags: FILE_DISPOSITION_FLAG_DELETE | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS,
    };
    let succeeded = unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle(),
            FileDispositionInfoEx,
            std::ptr::addr_of!(disposition).cast(),
            u32::try_from(size_of::<FILE_DISPOSITION_INFO_EX>())
                .expect("Windows disposition structure size fits in u32"),
        )
    };
    if succeeded != 0 {
        return Ok(());
    }
    let extended_error = io::Error::last_os_error();
    if !matches!(
        extended_error.raw_os_error().map(|value| value as u32),
        Some(ERROR_INVALID_FUNCTION) | Some(ERROR_INVALID_PARAMETER) | Some(ERROR_NOT_SUPPORTED)
    ) {
        return Err(extended_error);
    }

    let fallback = FILE_DISPOSITION_INFO { DeleteFile: true };
    let succeeded = unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle(),
            FileDispositionInfo,
            std::ptr::addr_of!(fallback).cast(),
            u32::try_from(size_of::<FILE_DISPOSITION_INFO>())
                .expect("Windows disposition structure size fits in u32"),
        )
    };
    if succeeded == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(windows)]
fn windows_stage_cleanup_should_defer(error: &io::Error) -> bool {
    use windows_sys::Win32::Foundation::{
        ERROR_ACCESS_DENIED, ERROR_DELETE_PENDING, ERROR_SHARING_VIOLATION,
    };

    matches!(
        error.raw_os_error().map(|value| value as u32),
        Some(ERROR_ACCESS_DENIED) | Some(ERROR_DELETE_PENDING) | Some(ERROR_SHARING_VIOLATION)
    )
}

#[cfg(target_os = "macos")]
#[allow(clippy::too_many_arguments)]
fn remove_owned_finalization_stage_macos<H>(
    reserved_destination: &Path,
    transfer_id: &str,
    directory: &Path,
    staged: &Path,
    expected_identity: FileIdentity,
    mut record: FinalizationRecord,
    phase_hook: &mut H,
) -> io::Result<bool>
where
    H: FnMut(LegacyStageCleanupPhase) -> io::Result<()>,
{
    let quarantine_name = if let Some(cleanup) = record.stage_cleanup.as_ref() {
        let Some(quarantine) = cleanup.quarantine.as_deref() else {
            return Ok(false);
        };
        quarantine.to_string()
    } else {
        let staged_file = match open_regular_file_no_follow(staged, false) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound && directory.is_dir() => {
                return Ok(false);
            }
            Err(error) if is_no_follow_rejection(&error) => return Ok(false),
            Err(error) => return Err(error),
        };
        if file_identity(&staged_file)? != expected_identity {
            return Ok(false);
        }
        phase_hook(LegacyStageCleanupPhase::AfterOpenIdentityVerified)?;
        if !path_has_file_identity(staged, expected_identity)? {
            return Ok(false);
        }
        let quarantine = format!(
            ".weline-localnet-finalize-quarantine-{}",
            uuid::Uuid::new_v4()
        );
        record = append_legacy_stage_cleanup_record(
            reserved_destination,
            transfer_id,
            &record,
            LegacyStageCleanupRecord {
                identity: expected_identity,
                quarantine: Some(quarantine.clone()),
                state: LegacyStageCleanupState::Prepared,
            },
        )?;
        phase_hook(LegacyStageCleanupPhase::AfterPreparation)?;
        quarantine
    };
    let quarantine = directory.join(&quarantine_name);

    let quarantine_file = match open_regular_file_no_follow(&quarantine, false) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let staged_file = match open_regular_file_no_follow(staged, false) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound && directory.is_dir() => {
                    return complete_legacy_stage_cleanup_record(
                        reserved_destination,
                        transfer_id,
                        &record,
                        expected_identity,
                        Some(quarantine_name),
                        phase_hook,
                    );
                }
                Err(error) if is_no_follow_rejection(&error) => return Ok(false),
                Err(error) => return Err(error),
            };
            if file_identity(&staged_file)? != expected_identity {
                return Ok(false);
            }
            phase_hook(LegacyStageCleanupPhase::AfterOpenIdentityVerified)?;
            if !path_has_file_identity(staged, expected_identity)? {
                return Ok(false);
            }
            match macos_rename_no_replace(staged, &quarantine) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let existing = match open_regular_file_no_follow(&quarantine, false) {
                        Ok(file) => file,
                        Err(error) if is_no_follow_rejection(&error) => {
                            macos_restore_quarantine_without_overwrite(
                                &quarantine,
                                staged,
                                directory,
                            )?;
                            return Ok(false);
                        }
                        Err(error) => return Err(error),
                    };
                    if file_identity(&existing)? != expected_identity {
                        macos_restore_quarantine_without_overwrite(&quarantine, staged, directory)?;
                        return Ok(false);
                    }
                }
                Err(error) if macos_exclusive_rename_is_unavailable(&error) => return Ok(false),
                Err(error) => return Err(error),
            }
            phase_hook(LegacyStageCleanupPhase::AfterNamespaceMutation)?;
            let file = match open_regular_file_no_follow(&quarantine, false) {
                Ok(file) => file,
                Err(error) if is_no_follow_rejection(&error) => {
                    macos_restore_quarantine_without_overwrite(&quarantine, staged, directory)?;
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };
            sync_directory(directory)?;
            file
        }
        Err(error) if is_no_follow_rejection(&error) => {
            macos_restore_quarantine_without_overwrite(&quarantine, staged, directory)?;
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    if file_identity(&quarantine_file)? != expected_identity {
        drop(quarantine_file);
        macos_restore_quarantine_without_overwrite(&quarantine, staged, directory)?;
        return Ok(false);
    }
    phase_hook(LegacyStageCleanupPhase::AfterQuarantineVerification)?;
    if !path_has_file_identity(&quarantine, expected_identity)? {
        drop(quarantine_file);
        macos_restore_quarantine_without_overwrite(&quarantine, staged, directory)?;
        return Ok(false);
    }
    fs::remove_file(&quarantine)?;
    sync_directory(directory)?;
    phase_hook(LegacyStageCleanupPhase::AfterUnlink)?;
    complete_legacy_stage_cleanup_record(
        reserved_destination,
        transfer_id,
        &record,
        expected_identity,
        Some(quarantine_name),
        phase_hook,
    )
}

#[cfg(target_os = "macos")]
fn macos_rename_no_replace(source: &Path, destination: &Path) -> io::Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt as _};

    if source.parent() != destination.parent() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "遗留暂存隔离必须保持在同一目录",
        ));
    }
    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "遗留暂存路径包含空字节"))?;
    let destination = CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "隔离路径包含空字节"))?;
    let succeeded = unsafe {
        libc::renameatx_np(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if succeeded != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn macos_restore_quarantine_without_overwrite(
    quarantine: &Path,
    staged: &Path,
    directory: &Path,
) -> io::Result<()> {
    match macos_rename_no_replace(quarantine, staged) {
        Ok(()) => sync_directory(directory),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::AlreadyExists | io::ErrorKind::NotFound
            ) =>
        {
            Ok(())
        }
        Err(error) if macos_exclusive_rename_is_unavailable(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "macos")]
fn macos_exclusive_rename_is_unavailable(error: &io::Error) -> bool {
    let raw = error.raw_os_error();
    [Some(libc::ENOSYS), Some(libc::ENOTSUP), Some(libc::EINVAL)].contains(&raw)
}

#[cfg(target_os = "macos")]
fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

#[cfg(windows)]
const _: fn(&Path) -> io::Result<File> = open_regular_file_for_delete_no_follow;

#[cfg(target_os = "macos")]
const _: fn(&Path, &Path) -> io::Result<()> = macos_rename_no_replace;

pub fn finalize_reserved_receive(
    partial: &Path,
    reserved_destination: &Path,
    file_name: &str,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<FinalizedReceive> {
    finalize_reserved_receive_with_marker(
        partial,
        reserved_destination,
        file_name,
        transfer_id,
        reservation_token,
    )
}

fn finalize_reserved_receive_with_marker(
    partial: &Path,
    reserved_destination: &Path,
    file_name: &str,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<FinalizedReceive> {
    let directory = reserved_destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let mut candidate =
        if reservation_is_owned(reserved_destination, transfer_id, reservation_token)? {
            reserved_destination.to_path_buf()
        } else {
            reserve_available_receive_path(directory, file_name, transfer_id, reservation_token)?
        };

    loop {
        if !reservation_is_owned(&candidate, transfer_id, reservation_token)? {
            candidate = reserve_available_receive_path(
                directory,
                file_name,
                transfer_id,
                reservation_token,
            )?;
            continue;
        }
        match commit_without_overwrite(partial, &candidate) {
            Ok(()) => {
                let reservation_released =
                    remove_owned_reservation(&candidate, transfer_id, reservation_token)
                        .unwrap_or(false);
                return Ok(FinalizedReceive {
                    path: candidate,
                    reservation_released,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                remove_owned_reservation(&candidate, transfer_id, reservation_token)?;
                candidate = reserve_available_receive_path(
                    directory,
                    file_name,
                    transfer_id,
                    reservation_token,
                )?;
            }
            Err(error) => return Err(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum FinalizationState {
    Prepared,
    CopyPrepared,
    Created,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum LegacyStageCleanupState {
    Prepared,
    Removed,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LegacyStageCleanupRecord {
    identity: FileIdentity,
    #[serde(default)]
    quarantine: Option<String>,
    state: LegacyStageCleanupState,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FinalizationRecord {
    prefix: String,
    transfer_id: String,
    reservation_token: String,
    candidate: String,
    previous: Vec<String>,
    #[serde(default)]
    staged: Option<String>,
    #[serde(default)]
    staged_identity: Option<FileIdentity>,
    #[serde(default)]
    stage_cleanup: Option<LegacyStageCleanupRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    copy_cleanup_token: Option<String>,
    identity: FileIdentity,
    state: FinalizationState,
}

fn stable_copy_cleanup_token(
    transfer_id: &str,
    reservation_token: &str,
    candidate: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"weline-localnet-copy-cleanup-v1\0");
    hasher.update(transfer_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(reservation_token.as_bytes());
    hasher.update(b"\0");
    hasher.update(candidate.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(target_os = "macos")]
fn persist_copy_cleanup_token_before_namespace_mutation(
    reserved_destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    mut record: FinalizationRecord,
) -> io::Result<(FinalizationRecord, String)> {
    let cleanup_token = record.copy_cleanup_token.clone().unwrap_or_else(|| {
        stable_copy_cleanup_token(transfer_id, reservation_token, &record.candidate)
    });
    if record.copy_cleanup_token.is_none() {
        record.copy_cleanup_token = Some(cleanup_token.clone());
        append_finalization_record_value(reserved_destination, transfer_id, &record)?;
    }
    Ok((record, cleanup_token))
}

fn append_finalization_record(
    reserved_destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    candidate: &Path,
    previous: &[PathBuf],
    identity: FileIdentity,
    state: FinalizationState,
) -> io::Result<()> {
    append_finalization_record_with_stage(
        reserved_destination,
        transfer_id,
        reservation_token,
        candidate,
        previous,
        None,
        None,
        identity,
        state,
    )
}

fn append_finalization_record_with_stage(
    reserved_destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    candidate: &Path,
    previous: &[PathBuf],
    staged: Option<&Path>,
    staged_identity: Option<FileIdentity>,
    identity: FileIdentity,
    state: FinalizationState,
) -> io::Result<()> {
    let candidate = candidate
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件名无效"))?
        .to_string();
    let previous = previous
        .iter()
        .map(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .map(str::to_string)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "旧接收文件名无效"))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let staged = staged
        .map(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .map(str::to_string)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "暂存文件名无效"))
        })
        .transpose()?;
    let copy_cleanup_token = (state == FinalizationState::CopyPrepared)
        .then(|| stable_copy_cleanup_token(transfer_id, reservation_token, &candidate));
    let record = FinalizationRecord {
        prefix: FINALIZATION_PREFIX.to_string(),
        transfer_id: transfer_id.to_string(),
        reservation_token: reservation_token.to_string(),
        candidate,
        previous,
        staged,
        staged_identity,
        stage_cleanup: None,
        copy_cleanup_token,
        identity,
        state,
    };
    append_finalization_record_value(reserved_destination, transfer_id, &record)
}

fn append_finalization_record_value(
    reserved_destination: &Path,
    transfer_id: &str,
    record: &FinalizationRecord,
) -> io::Result<()> {
    let encoded = serde_json::to_vec(&record).map_err(io::Error::other)?;
    let checksum = hex::encode(Sha256::digest(&encoded));
    let mut marker = OpenOptions::new()
        .create(true)
        .append(true)
        .open(finalization_marker_path(reserved_destination, transfer_id)?)?;
    writeln!(marker, "{checksum} {}", hex::encode(encoded))?;
    marker.flush()?;
    marker.sync_all()?;
    #[cfg(target_os = "macos")]
    if let Some(directory) = reserved_destination.parent() {
        sync_directory(directory)?;
    }
    Ok(())
}

fn append_legacy_stage_cleanup_record(
    reserved_destination: &Path,
    transfer_id: &str,
    record: &FinalizationRecord,
    cleanup: LegacyStageCleanupRecord,
) -> io::Result<FinalizationRecord> {
    let mut updated = record.clone();
    updated.stage_cleanup = Some(cleanup);
    append_finalization_record_value(reserved_destination, transfer_id, &updated)?;
    Ok(updated)
}

fn read_finalization_record(
    reserved_destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<Option<FinalizationRecord>> {
    let marker_path = finalization_marker_path(reserved_destination, transfer_id)?;
    let mut marker = match File::open(&marker_path) {
        Ok(marker) => marker,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return if marker_path.parent().is_some_and(|parent| parent.is_dir()) {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "接收目录或磁盘当前不可用",
                ))
            };
        }
        Err(error) => return Err(error),
    };
    if marker.metadata()?.len() > 256 * 1024 {
        return Ok(None);
    }
    let mut payload = String::new();
    marker.read_to_string(&mut payload)?;
    let mut latest = None;
    for line in payload.lines() {
        let Some((checksum, encoded)) = line.split_once(' ') else {
            continue;
        };
        let Ok(encoded) = hex::decode(encoded) else {
            continue;
        };
        if hex::encode(Sha256::digest(&encoded)) != checksum {
            continue;
        }
        let Ok(record) = serde_json::from_slice::<FinalizationRecord>(&encoded) else {
            continue;
        };
        if record.prefix != FINALIZATION_PREFIX
            || record.transfer_id != transfer_id
            || record.reservation_token != reservation_token
            || !valid_journal_file_name(&record.candidate)
            || record
                .previous
                .iter()
                .any(|name| !valid_journal_file_name(name))
            || record
                .staged
                .as_deref()
                .is_some_and(|name| !valid_finalization_stage_name(name, transfer_id))
            || (record.staged_identity.is_some() && record.staged.is_none())
            || record
                .copy_cleanup_token
                .as_deref()
                .is_some_and(|cleanup_token| {
                    record.state != FinalizationState::CopyPrepared
                        || cleanup_token
                            != stable_copy_cleanup_token(
                                transfer_id,
                                reservation_token,
                                &record.candidate,
                            )
                })
            || record.stage_cleanup.as_ref().is_some_and(|cleanup| {
                record.staged.is_none()
                    || cleanup.identity != record.staged_identity.unwrap_or(record.identity)
                    || cleanup
                        .quarantine
                        .as_deref()
                        .is_some_and(|name| !valid_finalization_quarantine_name(name))
            })
        {
            continue;
        }
        latest = Some(record);
    }
    Ok(latest)
}

fn valid_finalization_quarantine_name(value: &str) -> bool {
    if !valid_journal_file_name(value) {
        return false;
    }
    value
        .strip_prefix(".weline-localnet-finalize-quarantine-")
        .is_some_and(|suffix| uuid::Uuid::parse_str(suffix).is_ok())
}

fn valid_journal_file_name(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty() && path.file_name() == Some(path.as_os_str())
}

fn valid_finalization_stage_name(value: &str, transfer_id: &str) -> bool {
    if !valid_journal_file_name(value) {
        return false;
    }
    let digest = hex::encode(Sha256::digest(transfer_id.as_bytes()));
    let base = format!(".weline-localnet-finalize-stage-{}", &digest[..24]);
    if value == base {
        return true;
    }
    value
        .strip_prefix(&base)
        .and_then(|suffix| suffix.strip_prefix('-'))
        .is_some_and(|sequence| {
            !sequence.is_empty()
                && sequence.bytes().all(|byte| byte.is_ascii_digit())
                && sequence != "0"
        })
}

fn finalization_marker_path(destination: &Path, transfer_id: &str) -> io::Result<PathBuf> {
    let directory = destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let digest = hex::encode(Sha256::digest(transfer_id.as_bytes()));
    Ok(directory.join(format!(".weline-localnet-finalization-{}", &digest[..24])))
}

fn is_resumable_partial_candidate(
    partial: &Path,
    destination: &Path,
    transfer_id: &str,
) -> io::Result<bool> {
    let base = resumable_partial_path(destination, transfer_id)?;
    if partial == base {
        return Ok(true);
    }
    if partial.parent() != base.parent() {
        return Ok(false);
    }
    let Some(base_name) = base.file_name().and_then(|value| value.to_str()) else {
        return Ok(false);
    };
    let Some(candidate_name) = partial.file_name().and_then(|value| value.to_str()) else {
        return Ok(false);
    };
    let Some(prefix) = base_name.strip_suffix(".part") else {
        return Ok(false);
    };
    let Some(sequence) = candidate_name
        .strip_prefix(prefix)
        .and_then(|value| value.strip_prefix('-'))
        .and_then(|value| value.strip_suffix(".part"))
    else {
        return Ok(false);
    };
    Ok(!sequence.is_empty()
        && sequence.bytes().all(|value| value.is_ascii_digit())
        && sequence != "0")
}

fn open_identity_verified_partial(
    partial: &Path,
    transfer_id: &str,
    reservation_token: &str,
    writable: bool,
) -> io::Result<Option<File>> {
    let file = match open_regular_file_no_follow(partial, writable) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) if is_no_follow_rejection(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let handle_identity = file_identity(&file)?;
    let owner_path = partial_owner_sidecar_path(partial)?;
    let payload = partial_owner_payload(transfer_id, reservation_token, handle_identity);
    if marker_matches(&owner_path, &payload)? {
        Ok(Some(file))
    } else {
        Ok(None)
    }
}

fn path_has_file_identity(path: &Path, expected: FileIdentity) -> io::Result<bool> {
    match open_regular_file_no_follow(path, false) {
        Ok(file) => Ok(file_identity(&file)? == expected),
        Err(error) if is_no_follow_rejection(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

fn is_no_follow_rejection(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::InvalidInput
    ) {
        return true;
    }
    #[cfg(unix)]
    if error.raw_os_error() == Some(libc::ELOOP) {
        return true;
    }
    false
}

#[cfg(unix)]
fn create_new_regular_file_no_follow(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "复制目标不是普通文件",
        ));
    }
    Ok(file)
}

#[cfg(windows)]
fn create_new_regular_file_no_follow(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .access_mode(FILE_GENERIC_READ | FILE_GENERIC_WRITE | DELETE)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .create_new(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let information = windows_file_information(&file)?;
    if information.dwFileAttributes & (FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_DIRECTORY) != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "复制目标是重解析点或目录",
        ));
    }
    Ok(file)
}

#[cfg(unix)]
fn open_regular_file_no_follow(path: &Path, writable: bool) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let file = OpenOptions::new()
        .read(true)
        .write(writable)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "所有权文件不是普通文件",
        ));
    }
    Ok(file)
}

#[cfg(windows)]
fn open_regular_file_no_follow(path: &Path, writable: bool) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_OPEN_REPARSE_POINT,
    };

    let file = OpenOptions::new()
        .read(true)
        .write(writable)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let information = windows_file_information(&file)?;
    if information.dwFileAttributes & (FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_DIRECTORY) != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "所有权文件是重解析点或目录",
        ));
    }
    Ok(file)
}

fn partial_owner_sidecar_path(partial: &Path) -> io::Result<PathBuf> {
    let directory = partial
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "部分文件保存位置无效"))?;
    let file_name = partial
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "部分文件名无效"))?;
    Ok(directory.join(format!("{file_name}.owner")))
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FileIdentity {
    volume: u64,
    file: u64,
}

#[cfg(windows)]
fn file_identity(file: &File) -> io::Result<FileIdentity> {
    let information = windows_file_information(file)?;
    Ok(FileIdentity {
        volume: u64::from(information.dwVolumeSerialNumber),
        file: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow),
    })
}

#[cfg(windows)]
fn windows_file_information(
    file: &File,
) -> io::Result<windows_sys::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION> {
    use std::{mem::MaybeUninit, os::windows::io::AsRawHandle as _};
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let mut information = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    let succeeded =
        unsafe { GetFileInformationByHandle(file.as_raw_handle(), information.as_mut_ptr()) };
    if succeeded == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { information.assume_init() })
}

#[cfg(unix)]
fn file_identity(file: &File) -> io::Result<FileIdentity> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = file.metadata()?;
    Ok(FileIdentity {
        volume: metadata.dev(),
        file: metadata.ino(),
    })
}

fn partial_owner_payload(
    transfer_id: &str,
    reservation_token: &str,
    identity: FileIdentity,
) -> Vec<u8> {
    format!(
        "{PARTIAL_OWNERSHIP_PREFIX}{transfer_id}:{reservation_token}:{}:{}\n",
        identity.volume, identity.file
    )
    .into_bytes()
}

fn write_new_marker(path: &Path, payload: &[u8]) -> io::Result<()> {
    let mut marker = OpenOptions::new().write(true).create_new(true).open(path)?;
    let result = marker
        .write_all(payload)
        .and_then(|_| marker.flush())
        .and_then(|_| marker.sync_all());
    drop(marker);
    if let Err(error) = result {
        let _ = fs::remove_file(path);
        return Err(error);
    }
    Ok(())
}

fn marker_matches(path: &Path, payload: &[u8]) -> io::Result<bool> {
    let mut file = match open_regular_file_no_follow(path, false) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if path.parent().is_some_and(|parent| parent.is_dir()) {
                return Ok(false);
            }
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "接收目录或磁盘当前不可用",
            ));
        }
        Err(error) if is_no_follow_rejection(&error) => return Ok(false),
        Err(error) => return Err(error),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != payload.len() as u64 {
        return Ok(false);
    }
    let mut actual = vec![0_u8; payload.len()];
    match file.read_exact(&mut actual) {
        Ok(()) => Ok(actual == payload),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error),
    }
}

pub fn ensure_writable_directory(path: &Path) -> std::io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "receive directory must be absolute",
        ));
    }
    std::fs::create_dir_all(path)?;
    validate_existing_writable_directory(path)
}

pub fn validate_existing_writable_directory(path: &Path) -> std::io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "receive directory must be absolute",
        ));
    }
    if !path.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "receive directory or selected media is unavailable",
        ));
    }
    let probe = path.join(format!(
        ".weline-localnet-write-check-{}",
        uuid::Uuid::now_v7()
    ));
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)?;
    drop(file);
    std::fs::remove_file(probe)?;
    Ok(path.to_path_buf())
}

pub fn preflight_receive_directory(
    directory: &Path,
    file_size: u64,
    committed_bytes: u64,
) -> Result<(), AppError> {
    volume_preflight::preflight_destination(directory, file_size, committed_bytes).map(|_| ())
}

fn safe_file_name(file_name: &str) -> String {
    let leaf = file_name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .trim();
    let mut safe: String = leaf
        .chars()
        .map(|character| {
            if character.is_alphanumeric() || matches!(character, '.' | '-' | '_' | ' ') {
                character
            } else {
                '_'
            }
        })
        .take(180)
        .collect();
    safe = safe.trim_matches(['.', ' ']).to_string();
    if safe.is_empty() {
        safe = "received-file".to_string();
    }
    let basename = safe.split('.').next().unwrap_or_default();
    if is_windows_reserved_name(basename) {
        safe.insert(0, '_');
    }
    safe
}

fn numbered_file_name(file_name: &str, sequence: u32) -> String {
    if sequence == 0 {
        return file_name.to_string();
    }
    let path = Path::new(file_name);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("received-file");
    match path.extension().and_then(|value| value.to_str()) {
        Some(extension) if !extension.is_empty() => format!("{stem} ({sequence}).{extension}"),
        _ => format!("{stem} ({sequence})"),
    }
}

fn is_windows_reserved_name(stem: &str) -> bool {
    let normalized = stem.trim_end_matches(['.', ' ']).to_ascii_uppercase();
    matches!(normalized.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || normalized
            .strip_prefix("COM")
            .or_else(|| normalized.strip_prefix("LPT"))
            .is_some_and(|suffix| {
                matches!(
                    suffix,
                    "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
                )
            })
}

fn reservation_sidecar_path(destination: &Path) -> io::Result<PathBuf> {
    let directory = destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let file_name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件名无效"))?;
    let digest = hex::encode(Sha256::digest(file_name.to_lowercase().as_bytes()));
    Ok(directory.join(format!(".weline-localnet-reservation-{}", &digest[..24])))
}

fn reservation_payload(transfer_id: &str, reservation_token: &str) -> Vec<u8> {
    format!("{RESERVATION_PREFIX}{transfer_id}:{reservation_token}\n").into_bytes()
}

#[cfg(test)]
mod tests {
    use std::{fs, io, io::Write as _};

    #[cfg(target_os = "macos")]
    use super::{
        ExactCleanupPhase, remove_owned_partial_internal,
        remove_owned_partial_marker_after_file_cleanup_internal, remove_owned_reservation_internal,
        remove_owned_reservations_in_directory_internal,
    };
    use super::{
        FinalizationPhase, FinalizationState, LegacyStageCleanupPhase, LegacyStageCleanupState,
        append_finalization_record_with_stage, commit_without_overwrite, copy_without_overwrite,
        create_legacy_finalized_stage_fixture, file_identity, finalize_reserved_receive,
        finalize_reserved_receive_copy_fallback_with_hooks, finalize_reserved_receive_durable,
        finalize_reserved_receive_durable_with_hooks, owned_finalization_reservations,
        owned_finalized_receive_path, partial_owner_sidecar_path, read_finalization_record,
        remove_owned_finalization_stage, remove_owned_finalization_stage_with_hook,
        remove_owned_finalization_stage_with_phase_hook, remove_owned_partial,
        remove_owned_partial_marker_after_file_cleanup_with_hook, remove_owned_partial_with_hook,
        remove_owned_reservation, remove_owned_reservation_with_hook,
        remove_owned_reservations_in_directory_with_hook, reservation_is_owned,
        reservation_sidecar_path, reserve_available_receive_path, reserve_finalization_stage,
        reserve_receive_path, reserve_resumable_partial, resumable_partial_is_owned,
        resumable_partial_path, safe_file_name,
    };

    fn temporary_directory(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("weline-localnet-{label}-{}", uuid::Uuid::now_v7()))
    }

    #[test]
    fn automatic_receive_stays_inside_directory_and_numbers_collisions() {
        let directory = temporary_directory("receive-path");
        fs::create_dir_all(&directory).expect("create receive directory");
        fs::write(directory.join("quarter_report.pdf"), b"existing").expect("create existing file");
        fs::write(directory.join("quarter_report (1).pdf"), b"reserved")
            .expect("create reserved file");

        let selected = reserve_available_receive_path(
            &directory,
            "../quarter?report.pdf",
            "transfer-path-test",
            "token-path-test",
        )
        .expect("select receive path");

        assert_eq!(selected, directory.join("quarter_report (2).pdf"));
        assert!(
            reservation_is_owned(&selected, "transfer-path-test", "token-path-test")
                .expect("inspect selected reservation")
        );
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn committing_received_file_never_overwrites_existing_destination() {
        let directory = temporary_directory("no-overwrite");
        fs::create_dir_all(&directory).expect("create receive directory");
        let partial = directory.join("incoming.part");
        let destination = directory.join("report.txt");
        fs::write(&partial, b"new").expect("write partial file");
        fs::write(&destination, b"old").expect("write existing destination");

        let error = commit_without_overwrite(&partial, &destination)
            .expect_err("existing destination must be preserved");

        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&destination).expect("read destination"), b"old");
        assert_eq!(fs::read(&partial).expect("read partial"), b"new");
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn windows_reserved_names_are_sanitized_before_the_first_dot() {
        assert_eq!(safe_file_name("NUL.tar.gz"), "_NUL.tar.gz");
        assert_eq!(safe_file_name("CON.foo.txt"), "_CON.foo.txt");
        assert_eq!(safe_file_name("COM¹.txt"), "_COM¹.txt");
        assert_eq!(safe_file_name("LPT³.log"), "_LPT³.log");
        assert_eq!(safe_file_name("normal.txt"), "normal.txt");
    }

    #[test]
    fn owned_cleanup_quarantine_prefix_is_generation_and_artifact_scoped() {
        let first = super::owned_cleanup_quarantine_prefix("cleanup-a", "reservation-scan");
        let restarted = super::owned_cleanup_quarantine_prefix("cleanup-a", "reservation-scan");
        let other_generation =
            super::owned_cleanup_quarantine_prefix("cleanup-b", "reservation-scan");
        let other_artifact = super::owned_cleanup_quarantine_prefix("cleanup-a", "partial");

        assert_eq!(first, restarted);
        assert_ne!(first, other_generation);
        assert_ne!(first, other_artifact);
        assert!(first.starts_with(".weline-localnet-cleanup-"));
    }

    #[test]
    fn automatic_receive_atomically_reserves_numbered_destinations() {
        let directory = temporary_directory("reservation");
        fs::create_dir_all(&directory).expect("create receive directory");

        let first =
            reserve_available_receive_path(&directory, "report.txt", "transfer-one", "token-one")
                .expect("reserve first destination");
        let second =
            reserve_available_receive_path(&directory, "report.txt", "transfer-two", "token-two")
                .expect("reserve numbered destination");

        assert_eq!(first, directory.join("report.txt"));
        assert_eq!(second, directory.join("report (1).txt"));
        assert!(
            reservation_is_owned(&first, "transfer-one", "token-one")
                .expect("inspect first reservation")
        );
        assert!(
            reservation_is_owned(&second, "transfer-two", "token-two")
                .expect("inspect second reservation")
        );
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[cfg(windows)]
    #[test]
    fn windows_partial_cleanup_deletes_the_verified_handle_not_a_path_replacement() {
        let directory = temporary_directory("partial-exact-handle-race");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "partial-race", "race-token")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "partial-race", "race-token")
            .expect("reserve partial");
        fs::write(&partial, b"owned partial").expect("write owned partial");
        let retired = directory.join("retired-owned-partial");

        assert!(
            !remove_owned_partial_with_hook(
                &partial,
                &destination,
                "partial-race",
                "race-token",
                || {
                    fs::rename(&partial, &retired)?;
                    fs::write(&partial, b"replacement")?;
                    Ok(())
                },
            )
            .expect("remove exact opened partial"),
            "the partial was retired, but the replacement prevents marker cleanup"
        );
        assert_eq!(
            fs::read(&partial).expect("replacement remains"),
            b"replacement"
        );
        assert!(
            !retired.exists(),
            "the opened owned file is retired exactly"
        );

        fs::remove_dir_all(directory).expect("remove partial race fixture");
    }

    #[cfg(windows)]
    #[test]
    fn windows_partial_sidecar_cleanup_preserves_a_path_replacement() {
        let directory = temporary_directory("partial-sidecar-exact-handle-race");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "sidecar-race", "race-token")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "sidecar-race", "race-token")
            .expect("reserve partial");
        fs::remove_file(&partial).expect("simulate already-cleaned partial");
        let marker = partial_owner_sidecar_path(&partial).expect("partial marker path");
        let retired = directory.join("retired-owned-sidecar");

        assert!(
            remove_owned_partial_marker_after_file_cleanup_with_hook(
                &partial,
                &destination,
                "sidecar-race",
                "race-token",
                || {
                    fs::rename(&marker, &retired)?;
                    fs::write(&marker, b"replacement")?;
                    Ok(())
                },
            )
            .expect("remove exact opened owner sidecar")
        );
        assert_eq!(
            fs::read(&marker).expect("replacement remains"),
            b"replacement"
        );
        assert!(
            !retired.exists(),
            "the opened owned sidecar is retired exactly"
        );

        fs::remove_dir_all(directory).expect("remove sidecar race fixture");
    }

    #[cfg(windows)]
    #[test]
    fn windows_reservation_cleanup_preserves_a_path_replacement() {
        let directory = temporary_directory("reservation-exact-handle-race");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "reservation-race", "race-token")
            .expect("reserve destination");
        let marker = reservation_sidecar_path(&destination).expect("reservation marker path");
        let retired = directory.join("retired-owned-reservation");

        assert!(
            remove_owned_reservation_with_hook(
                &destination,
                "reservation-race",
                "race-token",
                || {
                    fs::rename(&marker, &retired)?;
                    fs::write(&marker, b"replacement")?;
                    Ok(())
                },
            )
            .expect("remove exact opened reservation")
        );
        assert_eq!(
            fs::read(&marker).expect("replacement remains"),
            b"replacement"
        );
        assert!(!retired.exists(), "opened reservation is retired exactly");

        fs::remove_dir_all(directory).expect("remove reservation race fixture");
    }

    #[cfg(windows)]
    #[test]
    fn windows_directory_scan_cleanup_preserves_a_replaced_marker() {
        let directory = temporary_directory("reservation-scan-exact-handle-race");
        fs::create_dir_all(&directory).expect("create receive directory");
        let first = directory.join("first.bin");
        let second = directory.join("second.bin");
        reserve_receive_path(&first, "scan-race", "race-token").expect("reserve first");
        reserve_receive_path(&second, "scan-race", "race-token").expect("reserve second");
        let first_marker = reservation_sidecar_path(&first).expect("first reservation marker");
        let retired = directory.join("retired-scan-reservation");
        let injected = std::cell::Cell::new(false);

        remove_owned_reservations_in_directory_with_hook(
            &first,
            "scan-race",
            "race-token",
            |path| {
                if path == first_marker && !injected.replace(true) {
                    fs::rename(path, &retired)?;
                    fs::write(path, b"replacement")?;
                }
                Ok(())
            },
        )
        .expect("scan removes exact opened reservations");
        assert_eq!(
            fs::read(&first_marker).expect("scan replacement remains"),
            b"replacement"
        );
        assert!(!retired.exists(), "opened scan marker is retired exactly");
        assert!(
            !reservation_is_owned(&second, "scan-race", "race-token")
                .expect("second owned reservation was cleaned")
        );

        fs::remove_dir_all(directory).expect("remove scan race fixture");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_partial_cleanup_retains_authority_when_quarantine_is_replaced_before_unlink() {
        let directory = temporary_directory("macos-partial-quarantine-race");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "partial-race", "race-token")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "partial-race", "race-token")
            .expect("reserve partial");
        fs::write(&partial, b"owned partial").expect("write owned partial");
        let retired = directory.join("retired-owned-partial");

        let cleaned = remove_owned_partial_internal(
            &partial,
            &destination,
            "partial-race",
            "race-token",
            "durable-cleanup-token",
            |phase, quarantine| {
                if phase == ExactCleanupPhase::AfterQuarantineProof {
                    fs::rename(quarantine, &retired)?;
                    fs::write(quarantine, b"replacement")?;
                }
                Ok(())
            },
        )
        .expect("partial cleanup fails closed");

        assert!(!cleaned);
        assert_eq!(
            fs::read(&partial).expect("replacement remains"),
            b"replacement"
        );
        assert_eq!(
            fs::read(&retired).expect("owned inode remains"),
            b"owned partial"
        );
        assert!(
            partial_owner_sidecar_path(&partial)
                .expect("partial owner marker")
                .exists(),
            "ProofLost must retain the sidecar needed to prove later cleanup"
        );

        fs::remove_dir_all(directory).expect("remove partial race fixture");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_partial_cleanup_restarts_the_same_durable_quarantine_generation() {
        let directory = temporary_directory("macos-partial-quarantine-restart");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        let transfer_id = "partial-quarantine-restart";
        let reservation_token = "partial-quarantine-token";
        let cleanup_token = "database-persisted-cleanup-generation";
        reserve_receive_path(&destination, transfer_id, reservation_token)
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, transfer_id, reservation_token)
            .expect("reserve partial");
        fs::write(&partial, b"owned partial survives until exact cleanup")
            .expect("write owned partial");
        let quarantine = super::owned_cleanup_quarantine_path(&partial, cleanup_token, "partial")
            .expect("derive durable partial quarantine");

        let crashed = remove_owned_partial_internal(
            &partial,
            &destination,
            transfer_id,
            reservation_token,
            cleanup_token,
            |phase, path| {
                if phase == ExactCleanupPhase::AfterQuarantineProof && path == quarantine {
                    return Err(io::Error::other(
                        "simulated crash after partial quarantine proof",
                    ));
                }
                Ok(())
            },
        );
        assert!(crashed.is_err());
        assert!(!partial.exists());
        assert!(quarantine.exists());
        assert!(partial_owner_sidecar_path(&partial).unwrap().exists());

        assert!(
            remove_owned_partial_internal(
                &partial,
                &destination,
                transfer_id,
                reservation_token,
                cleanup_token,
                |_, _| Ok(()),
            )
            .expect("restart exact partial cleanup")
        );
        assert!(!quarantine.exists());
        assert!(!partial_owner_sidecar_path(&partial).unwrap().exists());

        fs::remove_dir_all(directory).expect("remove partial quarantine restart fixture");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_partial_sidecar_cleanup_preserves_quarantine_replacement() {
        let directory = temporary_directory("macos-sidecar-quarantine-race");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "sidecar-race", "race-token")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "sidecar-race", "race-token")
            .expect("reserve partial");
        fs::remove_file(&partial).expect("simulate already-cleaned partial");
        let marker = partial_owner_sidecar_path(&partial).expect("partial marker path");
        let retired = directory.join("retired-owned-sidecar");

        let cleaned = remove_owned_partial_marker_after_file_cleanup_internal(
            &partial,
            &destination,
            "sidecar-race",
            "race-token",
            "durable-cleanup-token",
            |phase, quarantine| {
                if phase == ExactCleanupPhase::AfterQuarantineProof {
                    fs::rename(quarantine, &retired)?;
                    fs::write(quarantine, b"replacement")?;
                }
                Ok(())
            },
        )
        .expect("sidecar cleanup fails closed");

        assert!(!cleaned);
        assert_eq!(
            fs::read(&marker).expect("replacement remains"),
            b"replacement"
        );
        assert!(retired.exists(), "opened owned sidecar remains quarantined");

        fs::remove_dir_all(directory).expect("remove sidecar race fixture");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_reservation_cleanup_preserves_quarantine_replacement() {
        let directory = temporary_directory("macos-reservation-quarantine-race");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "reservation-race", "race-token")
            .expect("reserve destination");
        let marker = reservation_sidecar_path(&destination).expect("reservation marker path");
        let retired = directory.join("retired-owned-reservation");

        let cleaned = remove_owned_reservation_internal(
            &destination,
            "reservation-race",
            "race-token",
            "durable-cleanup-token",
            |phase, quarantine| {
                if phase == ExactCleanupPhase::AfterQuarantineProof {
                    fs::rename(quarantine, &retired)?;
                    fs::write(quarantine, b"replacement")?;
                }
                Ok(())
            },
        )
        .expect("reservation cleanup fails closed");

        assert!(!cleaned);
        assert_eq!(
            fs::read(&marker).expect("replacement remains"),
            b"replacement"
        );
        assert!(retired.exists(), "opened reservation remains quarantined");

        fs::remove_dir_all(directory).expect("remove reservation race fixture");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_directory_scan_retains_cleanup_when_quarantine_proof_is_lost() {
        let directory = temporary_directory("macos-reservation-scan-quarantine-race");
        fs::create_dir_all(&directory).expect("create receive directory");
        let first = directory.join("first.bin");
        let second = directory.join("second.bin");
        reserve_receive_path(&first, "scan-race", "race-token").expect("reserve first");
        reserve_receive_path(&second, "scan-race", "race-token").expect("reserve second");
        let first_marker = reservation_sidecar_path(&first).expect("first reservation marker");
        let first_quarantine = super::owned_cleanup_quarantine_path(
            &first_marker,
            "durable-cleanup-token",
            "reservation-scan",
        )
        .expect("first quarantine path");
        let retired = directory.join("retired-scan-reservation");
        let injected = std::cell::Cell::new(false);

        let cleaned = remove_owned_reservations_in_directory_internal(
            &first,
            "scan-race",
            "race-token",
            "durable-cleanup-token",
            |phase, quarantine| {
                if phase == ExactCleanupPhase::AfterQuarantineProof
                    && quarantine == first_quarantine
                    && !injected.replace(true)
                {
                    fs::rename(quarantine, &retired)?;
                    fs::write(quarantine, b"replacement")?;
                }
                Ok(())
            },
        )
        .expect("directory cleanup fails closed");

        assert!(!cleaned);
        assert_eq!(
            fs::read(&first_marker).expect("replacement remains"),
            b"replacement"
        );
        assert!(retired.exists(), "opened scan marker remains quarantined");
        assert!(
            !reservation_is_owned(&second, "scan-race", "race-token")
                .expect("second owned reservation was cleaned")
        );

        fs::remove_dir_all(directory).expect("remove scan race fixture");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_directory_scan_restarts_a_durable_quarantine_after_crash() {
        let directory = temporary_directory("macos-reservation-scan-quarantine-restart");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("target.bin");
        let extra = directory.join("extra.bin");
        let cleanup_token = "durable-cleanup-token";
        reserve_receive_path(&extra, "scan-restart", "race-token")
            .expect("reserve extra destination");
        let marker = reservation_sidecar_path(&extra).expect("extra reservation marker");
        let quarantine =
            super::owned_cleanup_quarantine_path(&marker, cleanup_token, "reservation-scan")
                .expect("durable quarantine path");

        let crashed = remove_owned_reservations_in_directory_internal(
            &destination,
            "scan-restart",
            "race-token",
            cleanup_token,
            |phase, path| {
                if phase == ExactCleanupPhase::AfterQuarantineProof && path == quarantine {
                    return Err(io::Error::other("simulated crash after quarantine proof"));
                }
                Ok(())
            },
        );
        assert!(crashed.is_err());
        assert!(!marker.exists(), "the owned marker was durably quarantined");
        assert!(quarantine.exists(), "the crash left the quarantine pending");

        assert!(
            remove_owned_reservations_in_directory_internal(
                &destination,
                "scan-restart",
                "race-token",
                cleanup_token,
                |_, _| Ok(()),
            )
            .expect("restart directory cleanup")
        );
        assert!(
            !quarantine.exists(),
            "restart must rediscover and retire the durable quarantine"
        );

        fs::remove_dir_all(directory).expect("remove scan restart fixture");
    }

    #[test]
    fn late_destination_collision_is_preserved_and_receive_is_renumbered() {
        let directory = temporary_directory("late-collision");
        fs::create_dir_all(&directory).expect("create receive directory");
        let reserved =
            reserve_available_receive_path(&directory, "report.txt", "transfer-one", "token-one")
                .expect("reserve destination");
        let partial = directory.join("incoming.part");
        fs::write(&partial, b"new").expect("write partial file");
        fs::write(&reserved, b"external").expect("create late external file");

        let completed = finalize_reserved_receive(
            &partial,
            &reserved,
            "report.txt",
            "transfer-one",
            "token-one",
        )
        .expect("renumber final destination");

        assert_eq!(completed.path, directory.join("report (1).txt"));
        assert!(completed.reservation_released);
        assert_eq!(
            fs::read(&reserved).expect("read external file"),
            b"external"
        );
        assert_eq!(
            fs::read(&completed.path).expect("read received file"),
            b"new"
        );
        assert!(!partial.exists());
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn exclusive_copy_fallback_finishes_without_hard_links() {
        let directory = temporary_directory("copy-fallback");
        fs::create_dir_all(&directory).expect("create receive directory");
        let partial = directory.join("incoming.part");
        let destination = directory.join("received.bin");
        fs::write(&partial, b"portable-copy").expect("write partial file");

        copy_without_overwrite(&partial, &destination).expect("copy into exclusive destination");

        assert_eq!(
            fs::read(&destination).expect("read copied destination"),
            b"portable-copy"
        );
        assert!(!partial.exists());
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn unavailable_reservation_directory_is_not_treated_as_cleaned() {
        let directory = temporary_directory("unavailable-reservation");
        let destination = directory.join("report.txt");

        let error = remove_owned_reservation(&destination, "transfer-one", "token-one")
            .expect_err("unavailable directory must remain pending for later cleanup");

        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert!(!directory.exists());
    }

    #[test]
    fn reservation_cleanup_never_deletes_the_destination_file() {
        let directory = temporary_directory("sidecar-cleanup");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.txt");
        reserve_receive_path(&destination, "transfer-one", "random-token")
            .expect("reserve destination sidecar");
        fs::write(&destination, b"user-or-received-data").expect("write destination file");

        assert!(
            remove_owned_reservation(&destination, "transfer-one", "random-token")
                .expect("remove owned sidecar")
        );
        assert_eq!(
            fs::read(&destination).expect("read preserved destination"),
            b"user-or-received-data"
        );
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn resumable_partial_is_hidden_deterministic_and_on_destination_volume() {
        let directory = temporary_directory("deterministic-partial");
        let destination = directory.join("Report.bin");

        let first = resumable_partial_path(&destination, "A0B1-C2D3")
            .expect("derive deterministic partial path");
        let repeated = resumable_partial_path(&destination, "A0B1-C2D3")
            .expect("derive repeated partial path");
        let differently_cased = resumable_partial_path(&destination, "a0b1-c2d3")
            .expect("derive case-distinct partial path");

        assert_eq!(first, repeated);
        assert_ne!(first, differently_cased);
        assert_eq!(first.parent(), destination.parent());
        let file_name = first
            .file_name()
            .and_then(|value| value.to_str())
            .expect("partial file name");
        assert!(file_name.starts_with('.'));
        assert!(file_name.ends_with(".part"));
    }

    #[test]
    fn whitespace_distinct_transfer_ids_have_distinct_partial_names() {
        let directory = temporary_directory("partial-whitespace-identity");
        let destination = directory.join("report.bin");

        let plain = resumable_partial_path(&destination, "transfer-one")
            .expect("derive plain transfer partial");
        let spaced = resumable_partial_path(&destination, " transfer-one ")
            .expect("derive whitespace-distinct transfer partial");

        assert_ne!(plain, spaced);
    }

    #[test]
    fn preexisting_deterministic_partial_is_preserved_while_a_new_candidate_is_claimed() {
        let directory = temporary_directory("partial-preexisting-collision");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "transfer-one", "token-one")
            .expect("reserve destination");
        let partial = resumable_partial_path(&destination, "transfer-one")
            .expect("derive deterministic partial");
        fs::write(&partial, b"unrelated-user-data").expect("write unrelated deterministic file");

        let reserved = reserve_resumable_partial(&destination, "transfer-one", "token-one")
            .expect("pre-existing partial must select a derived candidate");

        assert_ne!(reserved, partial);
        assert_eq!(
            fs::read(&partial).expect("read preserved unrelated partial"),
            b"unrelated-user-data"
        );
        assert!(
            !resumable_partial_is_owned(&partial, &destination, "transfer-one", "token-one")
                .expect("inspect unrelated partial ownership")
        );
        assert!(
            resumable_partial_is_owned(&reserved, &destination, "transfer-one", "token-one")
                .expect("inspect derived partial ownership")
        );
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn file_only_partial_crash_state_is_preserved_and_a_new_candidate_is_reserved() {
        let directory = temporary_directory("partial-file-only-crash");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "transfer-one", "token-one")
            .expect("reserve destination");
        let orphan = resumable_partial_path(&destination, "transfer-one")
            .expect("derive first partial candidate");
        fs::write(&orphan, b"unowned-orphan").expect("write file-only crash state");

        let reserved = reserve_resumable_partial(&destination, "transfer-one", "token-one")
            .expect("reserve a collision-derived partial");

        assert_ne!(reserved, orphan);
        assert_eq!(
            fs::read(&orphan).expect("read untouched orphan"),
            b"unowned-orphan"
        );
        assert!(
            resumable_partial_is_owned(&reserved, &destination, "transfer-one", "token-one")
                .expect("verify replacement candidate ownership")
        );
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn marker_only_partial_crash_state_is_preserved_and_a_new_candidate_is_reserved() {
        let directory = temporary_directory("partial-marker-only-crash");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "transfer-one", "token-one")
            .expect("reserve destination");
        let orphan = resumable_partial_path(&destination, "transfer-one")
            .expect("derive first partial candidate");
        let owner = partial_owner_sidecar_path(&orphan).expect("derive owner marker");
        fs::write(&owner, b"marker-only-crash-state").expect("write marker-only crash state");

        let reserved = reserve_resumable_partial(&destination, "transfer-one", "token-one")
            .expect("reserve after marker-only crash state");

        assert_ne!(reserved, orphan);
        assert!(!orphan.exists());
        assert!(owner.exists());
        assert!(
            resumable_partial_is_owned(&reserved, &destination, "transfer-one", "token-one")
                .expect("verify replacement candidate ownership")
        );
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn replacing_an_owned_partial_with_a_regular_file_revokes_ownership() {
        let directory = temporary_directory("partial-regular-replacement");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "transfer-one", "token-one")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "transfer-one", "token-one")
            .expect("reserve partial");
        fs::remove_file(&partial).expect("remove originally owned inode");
        fs::write(&partial, b"replacement-user-data").expect("replace partial path");

        assert!(
            !resumable_partial_is_owned(&partial, &destination, "transfer-one", "token-one")
                .expect("replacement must not inherit ownership")
        );
        assert!(
            !remove_owned_partial(&partial, &destination, "transfer-one", "token-one")
                .expect("replacement must not be deleted")
        );
        assert_eq!(
            fs::read(&partial).expect("read preserved replacement"),
            b"replacement-user-data"
        );
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn replacing_an_owned_partial_with_a_symlink_revokes_ownership() {
        let directory = temporary_directory("partial-symlink-replacement");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "transfer-one", "token-one")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "transfer-one", "token-one")
            .expect("reserve partial");
        let user_file = directory.join("user-data.bin");
        fs::write(&user_file, b"symlink-target-user-data").expect("write symlink target");
        fs::remove_file(&partial).expect("remove originally owned inode");
        #[cfg(windows)]
        if let Err(error) = std::os::windows::fs::symlink_file(&user_file, &partial) {
            // Rust may expose Windows ERROR_PRIVILEGE_NOT_HELD as Uncategorized.
            if error.kind() == io::ErrorKind::PermissionDenied || error.raw_os_error() == Some(1314)
            {
                fs::remove_dir_all(directory).expect("remove unsupported symlink fixture");
                return;
            }
            panic!("create file symlink: {error}");
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(&user_file, &partial).expect("create file symlink");

        assert!(
            !resumable_partial_is_owned(&partial, &destination, "transfer-one", "token-one")
                .expect("symlink must never inherit ownership")
        );
        assert!(
            !remove_owned_partial(&partial, &destination, "transfer-one", "token-one")
                .expect("symlink must not be deleted or followed")
        );
        assert_eq!(
            fs::read(&user_file).expect("read preserved symlink target"),
            b"symlink-target-user-data"
        );
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn owned_partial_cleanup_rejects_unrelated_paths_and_preserves_destination() {
        let directory = temporary_directory("owned-partial-cleanup");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        let partial = resumable_partial_path(&destination, "transfer-one")
            .expect("derive deterministic partial path");
        let unrelated = directory.join("user-data.part");
        reserve_receive_path(&destination, "transfer-one", "token-one")
            .expect("reserve destination");
        fs::write(&destination, b"completed-or-user-data").expect("write destination");
        let reserved = reserve_resumable_partial(&destination, "transfer-one", "token-one")
            .expect("reserve exact partial ownership");
        assert_eq!(reserved, partial);
        fs::write(&partial, b"owned-partial").expect("write owned partial");
        fs::write(&unrelated, b"unrelated").expect("write unrelated file");

        assert!(
            !remove_owned_partial(&unrelated, &destination, "transfer-one", "token-one",)
                .expect("reject unrelated cleanup")
        );
        assert!(partial.exists());
        assert!(
            remove_owned_partial(&partial, &destination, "transfer-one", "token-one",)
                .expect("remove exact owned partial")
        );
        assert!(!partial.exists());
        assert_eq!(
            fs::read(&destination).expect("read preserved destination"),
            b"completed-or-user-data"
        );
        assert_eq!(
            fs::read(&unrelated).expect("read unrelated file"),
            b"unrelated"
        );
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn durable_finalize_retains_owned_reservation_until_database_completion() {
        let directory = temporary_directory("durable-finalize-marker");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "transfer-one", "token-one")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "transfer-one", "token-one")
            .expect("reserve partial");
        fs::write(&partial, b"complete-payload").expect("write complete partial");

        let finalized = finalize_reserved_receive_durable(
            &partial,
            &destination,
            "report.bin",
            "transfer-one",
            "token-one",
        )
        .expect("durably finalize receive");

        assert_eq!(finalized.path, destination);
        assert!(!finalized.reservation_released);
        assert!(
            reservation_is_owned(&finalized.path, "transfer-one", "token-one")
                .expect("finalization marker remains owned")
        );
        assert_eq!(
            fs::read(&finalized.path).expect("read finalized payload"),
            b"complete-payload"
        );
        fs::remove_dir_all(directory).expect("remove receive fixture");
    }

    #[test]
    fn durable_finalize_is_recoverable_at_every_replacement_phase() {
        let phases = [
            FinalizationPhase::BeforeReplacementReserve,
            FinalizationPhase::AfterReplacementReserve,
            FinalizationPhase::AfterJournalUpdate,
            FinalizationPhase::AfterMetadataSwitch,
            FinalizationPhase::AfterOldReservationRelease,
            FinalizationPhase::AfterFinalNameCreation,
            FinalizationPhase::AfterFinalJournalSync,
        ];
        for phase in phases {
            let directory = temporary_directory(&format!("finalize-phase-{phase:?}"));
            fs::create_dir_all(&directory).expect("create receive directory");
            let destination = directory.join("report.bin");
            let replacement = directory.join("report (1).bin");
            reserve_receive_path(&destination, "transfer-one", "token-one")
                .expect("reserve original destination");
            let partial = reserve_resumable_partial(&destination, "transfer-one", "token-one")
                .expect("reserve partial");
            fs::write(&partial, b"owned payload").expect("write partial");
            fs::write(&destination, b"late user collision").expect("create late collision");
            let switched = std::cell::RefCell::new(None);

            let error = finalize_reserved_receive_durable_with_hooks(
                &partial,
                &destination,
                "report.bin",
                "transfer-one",
                "token-one",
                |_, next| {
                    *switched.borrow_mut() = Some(next.to_path_buf());
                    Ok(())
                },
                |current| {
                    if current == phase {
                        Err(io::Error::other(format!("injected crash at {phase:?}")))
                    } else {
                        Ok(())
                    }
                },
            )
            .err()
            .expect("phase hook simulates a process crash");
            assert!(error.to_string().contains("injected crash"));
            assert_eq!(fs::read(&destination).unwrap(), b"late user collision");
            assert!(
                partial.exists(),
                "partial remains until database completion"
            );

            if matches!(
                phase,
                FinalizationPhase::AfterReplacementReserve
                    | FinalizationPhase::AfterJournalUpdate
                    | FinalizationPhase::AfterMetadataSwitch
            ) {
                assert!(reservation_is_owned(&destination, "transfer-one", "token-one").unwrap());
                assert!(reservation_is_owned(&replacement, "transfer-one", "token-one").unwrap());
            }
            if matches!(
                phase,
                FinalizationPhase::AfterJournalUpdate
                    | FinalizationPhase::AfterMetadataSwitch
                    | FinalizationPhase::AfterOldReservationRelease
                    | FinalizationPhase::AfterFinalNameCreation
                    | FinalizationPhase::AfterFinalJournalSync
            ) {
                assert_eq!(
                    owned_finalization_reservations(&destination, "transfer-one", "token-one")
                        .unwrap(),
                    vec![destination.clone(), replacement.clone()]
                );
            }
            if matches!(
                phase,
                FinalizationPhase::AfterMetadataSwitch
                    | FinalizationPhase::AfterOldReservationRelease
                    | FinalizationPhase::AfterFinalNameCreation
                    | FinalizationPhase::AfterFinalJournalSync
            ) {
                assert_eq!(*switched.borrow(), Some(replacement.clone()));
            }
            if matches!(
                phase,
                FinalizationPhase::AfterFinalNameCreation
                    | FinalizationPhase::AfterFinalJournalSync
            ) {
                assert_eq!(
                    owned_finalized_receive_path(&destination, "transfer-one", "token-one")
                        .unwrap(),
                    Some(replacement.clone())
                );
                assert_eq!(fs::read(&replacement).unwrap(), b"owned payload");
            }
            fs::remove_dir_all(directory).expect("remove fixture");
        }
    }

    #[test]
    fn copy_fallback_never_accepts_an_incomplete_candidate_and_retries_idempotently() {
        let phases = [
            FinalizationPhase::BeforeCopy,
            FinalizationPhase::DuringCopy,
            FinalizationPhase::AfterCopySync,
            FinalizationPhase::BeforeCopyMaterializedJournal,
            FinalizationPhase::AfterFinalJournalSync,
        ];
        for phase in phases {
            let directory = temporary_directory(&format!("copy-fallback-{phase:?}"));
            fs::create_dir_all(&directory).expect("create receive directory");
            let destination = directory.join("report.bin");
            reserve_receive_path(&destination, "copy-transfer", "copy-token")
                .expect("reserve destination");
            let partial = reserve_resumable_partial(&destination, "copy-transfer", "copy-token")
                .expect("reserve partial");
            let payload = vec![0x5a; 192 * 1024];
            fs::write(&partial, &payload).expect("write complete partial");
            let hard_link_calls = std::cell::Cell::new(0_u32);

            let error = finalize_reserved_receive_copy_fallback_with_hooks(
                &partial,
                &destination,
                "report.bin",
                "copy-transfer",
                "copy-token",
                |_, _| Ok(()),
                |_, _| {
                    hard_link_calls.set(hard_link_calls.get() + 1);
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "representative filesystem does not support hard links",
                    ))
                },
                |current| {
                    if current == phase {
                        Err(io::Error::other(format!(
                            "injected copy crash at {phase:?}"
                        )))
                    } else {
                        Ok(())
                    }
                },
            )
            .err()
            .expect("copy crash is injected");
            assert!(error.to_string().contains("injected copy crash"));
            assert_eq!(fs::read(&partial).unwrap(), payload);
            assert!(
                destination.exists(),
                "the exact token-owned candidate is the copy target"
            );
            if phase == FinalizationPhase::AfterFinalJournalSync {
                assert_eq!(
                    owned_finalized_receive_path(&destination, "copy-transfer", "copy-token")
                        .unwrap(),
                    Some(destination.clone()),
                    "a fully synced materialized record is restart completion authority"
                );
            } else {
                assert_eq!(
                    owned_finalized_receive_path(&destination, "copy-transfer", "copy-token")
                        .unwrap(),
                    None,
                    "an incomplete or unmaterialized candidate is never complete"
                );
            }

            let finalized = if phase == FinalizationPhase::AfterFinalJournalSync {
                super::FinalizedReceive {
                    path: owned_finalized_receive_path(&destination, "copy-transfer", "copy-token")
                        .unwrap()
                        .expect("restart recovers fully materialized candidate"),
                    reservation_released: false,
                }
            } else {
                finalize_reserved_receive_copy_fallback_with_hooks(
                    &partial,
                    &destination,
                    "report.bin",
                    "copy-transfer",
                    "copy-token",
                    |_, _| Ok(()),
                    |_, _| {
                        hard_link_calls.set(hard_link_calls.get() + 1);
                        Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "representative filesystem does not support hard links",
                        ))
                    },
                    |_| Ok(()),
                )
                .expect("retry copy finalization")
            };
            assert_eq!(finalized.path, destination);
            assert_eq!(fs::read(&destination).unwrap(), payload);
            assert_eq!(
                hard_link_calls.get(),
                1,
                "unsupported hard links are probed once and never required by fallback"
            );
            assert_eq!(
                owned_finalized_receive_path(&destination, "copy-transfer", "copy-token").unwrap(),
                Some(destination.clone())
            );
            assert!(
                remove_owned_finalization_stage(&destination, "copy-transfer", "copy-token")
                    .expect("a stage-free fallback has nothing unsafe to remove")
            );
            assert_eq!(fs::read(&destination).unwrap(), payload);
            assert!(
                fs::read_dir(&directory).unwrap().all(|entry| {
                    !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".weline-localnet-finalize-stage-")
                }),
                "new copy fallback never persists a hidden stage pathname"
            );
            fs::remove_dir_all(directory).expect("remove fixture");
        }
    }

    #[test]
    fn copy_fallback_storage_full_retires_visible_candidate_and_retries_from_full_partial() {
        let directory = temporary_directory("copy-fallback-storage-full");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("large.bin");
        reserve_receive_path(&destination, "low-space-copy", "copy-token")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "low-space-copy", "copy-token")
            .expect("reserve partial");
        let payload = vec![0x6c; 192 * 1024];
        fs::write(&partial, &payload).expect("write complete verified partial");
        let injected = std::cell::Cell::new(false);

        let error = finalize_reserved_receive_copy_fallback_with_hooks(
            &partial,
            &destination,
            "large.bin",
            "low-space-copy",
            "copy-token",
            |_, _| Ok(()),
            |_, _| {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "filesystem does not support hard links",
                ))
            },
            |phase| {
                if phase == FinalizationPhase::DuringCopy && !injected.replace(true) {
                    return Err(injected_storage_full_error());
                }
                Ok(())
            },
        )
        .err()
        .expect("injected storage exhaustion pauses finalization");
        assert!(
            super::is_storage_full_error(&error),
            "unexpected error: {error:?}"
        );
        assert_eq!(fs::read(&partial).expect("full partial remains"), payload);
        assert!(
            !destination.exists(),
            "a truncated final-name candidate must not remain visible"
        );
        assert!(
            reservation_is_owned(&destination, "low-space-copy", "copy-token")
                .expect("reservation remains for retry")
        );
        assert_eq!(
            owned_finalized_receive_path(&destination, "low-space-copy", "copy-token")
                .expect("incomplete copy is not finalized"),
            None
        );

        let finalized = finalize_reserved_receive_copy_fallback_with_hooks(
            &partial,
            &destination,
            "large.bin",
            "low-space-copy",
            "copy-token",
            |_, _| Ok(()),
            |_, _| panic!("CopyPrepared restart must not retry the unsupported hard-link probe"),
            |_| Ok(()),
        )
        .expect("retry after space returns materializes from the retained partial");
        assert_eq!(finalized.path, destination);
        assert_eq!(
            fs::read(&destination).expect("read completed retry"),
            payload
        );

        fs::remove_dir_all(directory).expect("remove copy storage-full fixture");
    }

    #[test]
    fn copy_prepared_journal_persists_cleanup_generation_before_copy() {
        let directory = temporary_directory("copy-cleanup-generation-journal");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("large.bin");
        let transfer_id = "copy-cleanup-generation";
        let reservation_token = "copy-cleanup-token";
        reserve_receive_path(&destination, transfer_id, reservation_token)
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, transfer_id, reservation_token)
            .expect("reserve partial");
        fs::write(&partial, vec![0x4a; 128 * 1024]).expect("write complete partial");

        let interrupted = finalize_reserved_receive_copy_fallback_with_hooks(
            &partial,
            &destination,
            "large.bin",
            transfer_id,
            reservation_token,
            |_, _| Ok(()),
            |_, _| {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "filesystem does not support hard links",
                ))
            },
            |phase| {
                if phase == FinalizationPhase::BeforeCopy {
                    Err(io::Error::other("stop after CopyPrepared journal sync"))
                } else {
                    Ok(())
                }
            },
        )
        .err()
        .expect("copy is interrupted after its recovery authority is durable");
        assert!(interrupted.to_string().contains("CopyPrepared"));

        let record = read_finalization_record(&destination, transfer_id, reservation_token)
            .expect("read finalization journal")
            .expect("CopyPrepared record is durable");
        assert_eq!(record.state, FinalizationState::CopyPrepared);
        assert!(
            record
                .copy_cleanup_token
                .as_deref()
                .is_some_and(|value| !value.is_empty()),
            "a crash-safe candidate quarantine generation must be journaled before copying"
        );

        fs::remove_dir_all(directory).expect("remove copy cleanup journal fixture");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_storage_full_quarantine_crash_restarts_from_the_complete_partial() {
        let directory = temporary_directory("macos-copy-quarantine-restart");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("large.bin");
        let transfer_id = "macos-copy-quarantine-restart";
        let reservation_token = "macos-copy-quarantine-token";
        reserve_receive_path(&destination, transfer_id, reservation_token)
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, transfer_id, reservation_token)
            .expect("reserve partial");
        let payload = vec![0x73; 256 * 1024];
        fs::write(&partial, &payload).expect("write complete partial");
        let injected_storage_full = std::cell::Cell::new(false);
        let injected_crash = std::cell::Cell::new(false);

        let crashed = finalize_reserved_receive_copy_fallback_with_hooks(
            &partial,
            &destination,
            "large.bin",
            transfer_id,
            reservation_token,
            |_, _| Ok(()),
            |_, _| {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "filesystem does not support hard links",
                ))
            },
            |phase| {
                if phase == FinalizationPhase::DuringCopy && !injected_storage_full.replace(true) {
                    return Err(injected_storage_full_error());
                }
                if phase == FinalizationPhase::AfterCopyQuarantineProof
                    && !injected_crash.replace(true)
                {
                    return Err(io::Error::other(
                        "simulated crash after durable copy quarantine",
                    ));
                }
                Ok(())
            },
        )
        .err()
        .expect("crash is injected between quarantine rename and unlink");
        assert!(crashed.to_string().contains("无法安全清理"));
        assert_eq!(fs::read(&partial).unwrap(), payload);
        assert!(!destination.exists());

        let record = read_finalization_record(&destination, transfer_id, reservation_token)
            .expect("read CopyPrepared recovery record")
            .expect("copy recovery record remains durable");
        let cleanup_token = record
            .copy_cleanup_token
            .as_deref()
            .expect("copy cleanup generation was journaled before copying");
        let quarantine =
            super::owned_cleanup_quarantine_path(&destination, cleanup_token, "copy-candidate")
                .expect("derive journal-bound copy quarantine");
        assert!(
            quarantine.exists(),
            "crash leaves a discoverable quarantine"
        );

        let finalized = finalize_reserved_receive_copy_fallback_with_hooks(
            &partial,
            &destination,
            "large.bin",
            transfer_id,
            reservation_token,
            |_, _| Ok(()),
            |_, _| panic!("CopyPrepared restart must not reprobe hard-link support"),
            |_| Ok(()),
        )
        .expect("restart retires the quarantine and rematerializes without retransmission");
        assert_eq!(finalized.path, destination);
        assert_eq!(fs::read(&destination).unwrap(), payload);
        assert_eq!(fs::read(&partial).unwrap(), payload);
        assert!(!quarantine.exists());

        fs::remove_dir_all(directory).expect("remove macOS copy quarantine fixture");
    }

    fn injected_storage_full_error() -> io::Error {
        #[cfg(windows)]
        {
            io::Error::from_raw_os_error(112)
        }
        #[cfg(unix)]
        {
            io::Error::from_raw_os_error(libc::ENOSPC)
        }
        #[cfg(not(any(windows, unix)))]
        {
            io::Error::new(io::ErrorKind::StorageFull, "storage full")
        }
    }

    #[test]
    fn unsupported_hard_link_fallback_materializes_without_a_second_link_attempt() {
        let directory = temporary_directory("copy-fallback-no-second-link");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "copy-no-link", "copy-token")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "copy-no-link", "copy-token")
            .expect("reserve partial");
        let payload = vec![0xa5; 160 * 1024];
        fs::write(&partial, &payload).expect("write complete partial");
        let hard_link_calls = std::cell::Cell::new(0_u32);

        let finalized = finalize_reserved_receive_copy_fallback_with_hooks(
            &partial,
            &destination,
            "report.bin",
            "copy-no-link",
            "copy-token",
            |_, _| Ok(()),
            |_, _| {
                hard_link_calls.set(hard_link_calls.get() + 1);
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "representative filesystem does not support hard links",
                ))
            },
            |_| Ok(()),
        )
        .expect("ordinary create, write, flush, and sync materialize the fallback");

        assert_eq!(hard_link_calls.get(), 1);
        assert_eq!(finalized.path, destination);
        assert_eq!(fs::read(&destination).unwrap(), payload);
        assert!(
            fs::read_dir(&directory).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".weline-localnet-finalize-stage-")
            }),
            "fallback must not create a stage that itself needs a hard link"
        );
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn copy_fallback_preserves_a_collision_arriving_after_stage_sync() {
        let directory = temporary_directory("copy-fallback-late-collision");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        let replacement = directory.join("report (1).bin");
        reserve_receive_path(&destination, "copy-collision", "copy-token")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "copy-collision", "copy-token")
            .expect("reserve partial");
        let payload = vec![0x3c; 96 * 1024];
        fs::write(&partial, &payload).expect("write partial");
        let injected = std::cell::Cell::new(false);
        let switched = std::cell::RefCell::new(None);
        let displaced_owned_candidate = directory.join("detached-owned-copy.bin");
        let hard_link_calls = std::cell::Cell::new(0_u32);

        let finalized = finalize_reserved_receive_copy_fallback_with_hooks(
            &partial,
            &destination,
            "report.bin",
            "copy-collision",
            "copy-token",
            |previous, next| {
                *switched.borrow_mut() = Some((previous.to_path_buf(), next.to_path_buf()));
                Ok(())
            },
            |_, _| {
                hard_link_calls.set(hard_link_calls.get() + 1);
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "representative filesystem does not support hard links",
                ))
            },
            |phase| {
                if phase == FinalizationPhase::AfterCopySync && !injected.replace(true) {
                    fs::rename(&destination, &displaced_owned_candidate)?;
                    fs::write(&destination, b"late user collision")?;
                }
                Ok(())
            },
        )
        .expect("renumber copy fallback without overwriting collision");

        assert_eq!(finalized.path, replacement);
        assert_eq!(fs::read(&destination).unwrap(), b"late user collision");
        assert_eq!(fs::read(&replacement).unwrap(), payload);
        assert_eq!(fs::read(&displaced_owned_candidate).unwrap(), payload);
        assert_eq!(hard_link_calls.get(), 1);
        assert_eq!(
            *switched.borrow(),
            Some((destination.clone(), replacement.clone()))
        );
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn hard_link_identity_mismatch_never_deletes_a_concurrent_candidate_replacement() {
        let directory = temporary_directory("hard-link-candidate-replacement");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        let detached_owned_link = directory.join("detached-owned-link.bin");
        reserve_receive_path(&destination, "link-replacement", "link-token")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "link-replacement", "link-token")
            .expect("reserve partial");
        fs::write(&partial, b"owned link payload").expect("write partial");

        let error = finalize_reserved_receive_copy_fallback_with_hooks(
            &partial,
            &destination,
            "report.bin",
            "link-replacement",
            "link-token",
            |_, _| Ok(()),
            |source, candidate| {
                fs::hard_link(source, candidate)?;
                fs::rename(candidate, &detached_owned_link)?;
                fs::write(candidate, b"concurrent user replacement")?;
                Ok(())
            },
            |_| Ok(()),
        )
        .err()
        .expect("identity mismatch aborts this finalization attempt");

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(
            fs::read(&destination).unwrap(),
            b"concurrent user replacement"
        );
        assert_eq!(
            fs::read(&detached_owned_link).unwrap(),
            b"owned link payload"
        );
        assert_eq!(fs::read(&partial).unwrap(), b"owned link payload");
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[cfg(any(windows, target_os = "macos"))]
    #[test]
    fn legacy_stage_cleanup_never_unlinks_a_concurrent_path_replacement() {
        let directory = temporary_directory("legacy-stage-cleanup-race");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "legacy-stage", "legacy-token")
            .expect("reserve destination");
        let (staged, mut staged_file) =
            reserve_finalization_stage(&directory, "legacy-stage").expect("reserve legacy stage");
        staged_file
            .write_all(b"legacy owned stage")
            .expect("write legacy stage");
        staged_file.sync_all().expect("sync legacy stage");
        let staged_identity = file_identity(&staged_file).expect("capture legacy stage identity");
        append_finalization_record_with_stage(
            &destination,
            "legacy-stage",
            "legacy-token",
            &destination,
            &[],
            Some(&staged),
            None,
            staged_identity,
            FinalizationState::CopyPrepared,
        )
        .expect("write backward-compatible CopyPrepared row");
        drop(staged_file);
        let detached = directory.join("detached-legacy-stage");

        let cleaned = remove_owned_finalization_stage_with_hook(
            &destination,
            "legacy-stage",
            "legacy-token",
            || {
                fs::rename(&staged, &detached)?;
                fs::write(&staged, b"concurrent user replacement")?;
                Ok(())
            },
        )
        .expect("legacy cleanup remains fail-closed");

        assert_eq!(fs::read(&staged).unwrap(), b"concurrent user replacement");
        assert!(
            !cleaned,
            "legacy cleanup retains authority instead of unlinking by name"
        );
        let cleanup = read_finalization_record(&destination, "legacy-stage", "legacy-token")
            .unwrap()
            .expect("legacy cleanup journal remains pending")
            .stage_cleanup;
        assert!(matches!(
            cleanup,
            Some(super::LegacyStageCleanupRecord {
                state: LegacyStageCleanupState::Prepared,
                ..
            })
        ));
        assert_eq!(fs::read(&detached).unwrap(), b"legacy owned stage");
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[cfg(windows)]
    #[test]
    fn windows_legacy_stage_cleanup_crashes_are_restart_idempotent() {
        for crash_phase in [
            LegacyStageCleanupPhase::AfterOpenIdentityVerified,
            LegacyStageCleanupPhase::AfterPreparation,
            LegacyStageCleanupPhase::AfterNamespaceMutation,
            LegacyStageCleanupPhase::AfterJournalComplete,
        ] {
            let directory = temporary_directory(&format!("windows-stage-crash-{crash_phase:?}"));
            fs::create_dir_all(&directory).expect("create receive directory");
            let destination = directory.join("report.bin");
            let transfer_id = format!("windows-stage-crash-{crash_phase:?}");
            let token = "legacy-token";
            let payload = b"windows crash-idempotent hard link";
            reserve_receive_path(&destination, &transfer_id, token).expect("reserve destination");
            let staged =
                create_legacy_finalized_stage_fixture(&destination, &transfer_id, token, payload)
                    .expect("create identity-owned legacy stage");

            let error = remove_owned_finalization_stage_with_phase_hook(
                &destination,
                &transfer_id,
                token,
                |phase| {
                    if phase == crash_phase {
                        Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            format!("injected cleanup crash at {phase:?}"),
                        ))
                    } else {
                        Ok(())
                    }
                },
            )
            .expect_err("inject one cleanup crash");
            assert_eq!(error.kind(), io::ErrorKind::Interrupted);

            assert!(
                remove_owned_finalization_stage(&destination, &transfer_id, token)
                    .expect("restart exact legacy cleanup")
            );
            assert!(!staged.exists());
            assert_eq!(fs::read(&destination).unwrap(), payload);
            fs::remove_dir_all(directory).expect("remove fixture");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_legacy_stage_quarantine_crashes_are_restart_idempotent() {
        for crash_phase in [
            LegacyStageCleanupPhase::AfterOpenIdentityVerified,
            LegacyStageCleanupPhase::AfterPreparation,
            LegacyStageCleanupPhase::AfterNamespaceMutation,
            LegacyStageCleanupPhase::AfterQuarantineVerification,
            LegacyStageCleanupPhase::AfterUnlink,
            LegacyStageCleanupPhase::AfterJournalComplete,
        ] {
            let directory = temporary_directory(&format!("macos-stage-crash-{crash_phase:?}"));
            fs::create_dir_all(&directory).expect("create receive directory");
            let destination = directory.join("report.bin");
            let transfer_id = format!("macos-stage-crash-{crash_phase:?}");
            let token = "legacy-token";
            let payload = b"macos crash-idempotent hard link";
            reserve_receive_path(&destination, &transfer_id, token).expect("reserve destination");
            let staged =
                create_legacy_finalized_stage_fixture(&destination, &transfer_id, token, payload)
                    .expect("create identity-owned legacy stage");

            let error = remove_owned_finalization_stage_with_phase_hook(
                &destination,
                &transfer_id,
                token,
                |phase| {
                    if phase == crash_phase {
                        Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            format!("injected cleanup crash at {phase:?}"),
                        ))
                    } else {
                        Ok(())
                    }
                },
            )
            .expect_err("inject one cleanup crash");
            assert_eq!(error.kind(), io::ErrorKind::Interrupted);

            assert!(
                remove_owned_finalization_stage(&destination, &transfer_id, token)
                    .expect("restart quarantined legacy cleanup")
            );
            assert!(!staged.exists());
            assert_eq!(fs::read(&destination).unwrap(), payload);
            fs::remove_dir_all(directory).expect("remove fixture");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_quarantine_identity_mismatch_restores_without_overwrite_and_stays_pending() {
        let directory = temporary_directory("macos-stage-quarantine-mismatch");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        let transfer_id = "macos-stage-quarantine-mismatch";
        let token = "legacy-token";
        let payload = b"macos owned stage payload";
        reserve_receive_path(&destination, transfer_id, token).expect("reserve destination");
        let staged =
            create_legacy_finalized_stage_fixture(&destination, transfer_id, token, payload)
                .expect("create identity-owned legacy stage");
        let detached = directory.join("detached-owned-stage");

        let cleaned = remove_owned_finalization_stage_with_phase_hook(
            &destination,
            transfer_id,
            token,
            |phase| {
                if phase == LegacyStageCleanupPhase::AfterQuarantineVerification {
                    let record = read_finalization_record(&destination, transfer_id, token)?
                        .ok_or_else(|| io::Error::other("cleanup journal disappeared"))?;
                    let quarantine = record
                        .stage_cleanup
                        .and_then(|cleanup| cleanup.quarantine)
                        .ok_or_else(|| io::Error::other("cleanup quarantine was not journaled"))?;
                    let quarantine = directory.join(quarantine);
                    fs::rename(&quarantine, &detached)?;
                    fs::write(&quarantine, b"concurrent quarantine replacement")?;
                }
                Ok(())
            },
        )
        .expect("identity mismatch remains fail-closed");

        assert!(!cleaned);
        assert_eq!(
            fs::read(&staged).unwrap(),
            b"concurrent quarantine replacement"
        );
        assert_eq!(fs::read(&detached).unwrap(), payload);
        assert_eq!(fs::read(&destination).unwrap(), payload);
        assert!(
            !remove_owned_finalization_stage(&destination, transfer_id, token)
                .expect("replacement keeps cleanup pending")
        );
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[cfg(any(windows, target_os = "macos"))]
    #[test]
    fn identity_owned_legacy_stage_is_removed_while_the_final_hard_link_remains() {
        let directory = temporary_directory("legacy-stage-cleanup-owned");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        let payload = b"legacy finalized hard-link payload";
        reserve_receive_path(&destination, "legacy-owned", "legacy-token")
            .expect("reserve destination");
        let staged = create_legacy_finalized_stage_fixture(
            &destination,
            "legacy-owned",
            "legacy-token",
            payload,
        )
        .expect("create identity-owned legacy stage and final hard link");

        assert_eq!(fs::read(&staged).unwrap(), payload);
        assert_eq!(fs::read(&destination).unwrap(), payload);
        assert!(
            remove_owned_finalization_stage(&destination, "legacy-owned", "legacy-token")
                .expect("retire exact legacy stage link")
        );
        assert!(!staged.exists());
        assert_eq!(fs::read(&destination).unwrap(), payload);

        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[cfg(any(windows, target_os = "macos"))]
    #[test]
    fn legacy_copy_prepared_stage_recovers_without_hard_links_and_retains_cleanup_authority() {
        let directory = temporary_directory("legacy-stage-recovery");
        fs::create_dir_all(&directory).expect("create receive directory");
        let destination = directory.join("report.bin");
        reserve_receive_path(&destination, "legacy-recovery", "legacy-token")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "legacy-recovery", "legacy-token")
            .expect("reserve partial");
        let payload = vec![0x77; 80 * 1024];
        fs::write(&partial, &payload).expect("write complete partial");
        let (staged, mut staged_file) = reserve_finalization_stage(&directory, "legacy-recovery")
            .expect("reserve legacy stage");
        staged_file
            .write_all(&payload[..32 * 1024])
            .expect("write interrupted legacy stage");
        staged_file
            .sync_all()
            .expect("sync interrupted legacy stage");
        let staged_identity = file_identity(&staged_file).expect("capture legacy stage identity");
        append_finalization_record_with_stage(
            &destination,
            "legacy-recovery",
            "legacy-token",
            &destination,
            &[],
            Some(&staged),
            None,
            staged_identity,
            FinalizationState::CopyPrepared,
        )
        .expect("write old CopyPrepared journal shape");
        let mut legacy_record =
            read_finalization_record(&destination, "legacy-recovery", "legacy-token")
                .expect("read generated CopyPrepared record")
                .expect("CopyPrepared record exists");
        legacy_record.copy_cleanup_token = None;
        super::append_finalization_record_value(&destination, "legacy-recovery", &legacy_record)
            .expect("append the pre-cleanup-token journal shape");
        drop(staged_file);
        let hard_link_calls = std::cell::Cell::new(0_u32);

        let finalized = finalize_reserved_receive_copy_fallback_with_hooks(
            &partial,
            &destination,
            "report.bin",
            "legacy-recovery",
            "legacy-token",
            |_, _| Ok(()),
            |_, _| {
                hard_link_calls.set(hard_link_calls.get() + 1);
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "legacy filesystem still does not support hard links",
                ))
            },
            |_| Ok(()),
        )
        .expect("recover old stage journal through direct candidate copy");

        assert_eq!(hard_link_calls.get(), 0);
        assert_eq!(finalized.path, destination);
        assert_eq!(fs::read(&destination).unwrap(), payload);
        let cleaned =
            remove_owned_finalization_stage(&destination, "legacy-recovery", "legacy-token")
                .expect("clean legacy stage using retained identity authority");
        assert!(cleaned, "identity-owned legacy stage cleanup must complete");
        assert!(!staged.exists());
        assert_eq!(fs::read(&destination).unwrap(), payload);
        fs::remove_dir_all(directory).expect("remove fixture");
    }
}
