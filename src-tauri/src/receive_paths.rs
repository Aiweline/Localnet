use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
    let mut file = match File::open(&reservation_path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
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
    let reservation_path = reservation_sidecar_path(destination)?;
    if !reservation_is_owned(destination, transfer_id, reservation_token)? {
        if !reservation_path.try_exists()?
            && !destination.parent().is_some_and(|parent| parent.is_dir())
        {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "接收目录或磁盘当前不可用",
            ));
        }
        return Ok(false);
    }
    fs::remove_file(reservation_path)?;
    Ok(true)
}

pub fn remove_owned_reservations_in_directory(
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<()> {
    let directory = destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let expected = reservation_payload(transfer_id, reservation_token);
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if !file_name.starts_with(".weline-localnet-reservation-") {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.file_type().is_file() || metadata.len() != expected.len() as u64 {
            continue;
        }
        let mut marker = File::open(entry.path())?;
        let mut actual = vec![0_u8; expected.len()];
        if marker.read_exact(&mut actual).is_ok() && actual == expected {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
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
    if !resumable_partial_is_owned(partial, destination, transfer_id, reservation_token)? {
        return Ok(false);
    }
    match fs::remove_file(partial) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if !partial.parent().is_some_and(|parent| parent.is_dir()) {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "接收目录或磁盘当前不可用",
                ));
            }
        }
        Err(error) => return Err(error),
    }
    fs::remove_file(partial_owner_sidecar_path(partial)?)?;
    Ok(true)
}

pub fn remove_owned_partial_marker_after_file_cleanup(
    partial: &Path,
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> io::Result<bool> {
    if !is_resumable_partial_candidate(partial, destination, transfer_id)? {
        return Ok(false);
    }
    match fs::symlink_metadata(partial) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Ok(_) => return Ok(false),
        Err(error) => return Err(error),
    }
    let marker = partial_owner_sidecar_path(partial)?;
    let mut file = match File::open(&marker) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() > 8 * 1024 {
        return Ok(false);
    }
    let mut payload = String::new();
    file.read_to_string(&mut payload)?;
    let expected_prefix = format!("{PARTIAL_OWNERSHIP_PREFIX}{transfer_id}:{reservation_token}:");
    if !payload.starts_with(&expected_prefix) || !payload.ends_with('\n') {
        return Ok(false);
    }
    fs::remove_file(marker)?;
    Ok(true)
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
        match fs::hard_link(partial, &candidate) {
            Ok(()) => {
                if !path_has_file_identity(&candidate, partial_identity)? {
                    let _ = fs::remove_file(&candidate);
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
                let mut target = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&candidate)?;
                let target_identity = file_identity(&target)?;
                append_finalization_record(
                    reserved_destination,
                    transfer_id,
                    reservation_token,
                    &candidate,
                    &previous,
                    target_identity,
                    FinalizationState::Prepared,
                )?;
                let mut source = &partial_file;
                io::copy(&mut source, &mut target)?;
                target.flush()?;
                target.sync_all()?;
                phase_hook(FinalizationPhase::AfterFinalNameCreation)?;
                append_finalization_record(
                    reserved_destination,
                    transfer_id,
                    reservation_token,
                    &candidate,
                    &previous,
                    target_identity,
                    FinalizationState::Created,
                )?;
                phase_hook(FinalizationPhase::AfterFinalJournalSync)?;
                return Ok(FinalizedReceive {
                    path: candidate,
                    reservation_released: false,
                });
            }
        }
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
    let directory = reserved_destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "接收文件保存位置无效"))?;
    let candidate = directory.join(record.candidate);
    if !reservation_is_owned(&candidate, transfer_id, reservation_token)? {
        return Ok(None);
    }
    match fs::symlink_metadata(&candidate) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    if file_identity(&File::open(&candidate)?)? != record.identity {
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
    Created,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FinalizationRecord {
    prefix: String,
    transfer_id: String,
    reservation_token: String,
    candidate: String,
    previous: Vec<String>,
    identity: FileIdentity,
    state: FinalizationState,
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
    let record = FinalizationRecord {
        prefix: FINALIZATION_PREFIX.to_string(),
        transfer_id: transfer_id.to_string(),
        reservation_token: reservation_token.to_string(),
        candidate,
        previous,
        identity,
        state,
    };
    let encoded = serde_json::to_vec(&record).map_err(io::Error::other)?;
    let checksum = hex::encode(Sha256::digest(&encoded));
    let mut marker = OpenOptions::new()
        .create(true)
        .append(true)
        .open(finalization_marker_path(reserved_destination, transfer_id)?)?;
    writeln!(marker, "{checksum} {}", hex::encode(encoded))?;
    marker.flush()?;
    marker.sync_all()
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
        {
            continue;
        }
        latest = Some(record);
    }
    Ok(latest)
}

fn valid_journal_file_name(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty() && path.file_name() == Some(path.as_os_str())
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
    match fs::symlink_metadata(partial) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    let file = match OpenOptions::new().read(true).write(writable).open(partial) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
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
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    }
    Ok(file_identity(&File::open(path)?)? == expected)
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
    let information = unsafe { information.assume_init() };
    Ok(FileIdentity {
        volume: u64::from(information.dwVolumeSerialNumber),
        file: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow),
    })
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
    let mut file = match File::open(path) {
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
    use std::{fs, io};

    use super::{
        FinalizationPhase, commit_without_overwrite, copy_without_overwrite,
        finalize_reserved_receive, finalize_reserved_receive_durable,
        finalize_reserved_receive_durable_with_hooks, owned_finalization_reservations,
        owned_finalized_receive_path, partial_owner_sidecar_path, remove_owned_partial,
        remove_owned_reservation, reservation_is_owned, reserve_available_receive_path,
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
            if error.kind() == io::ErrorKind::PermissionDenied {
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
}
