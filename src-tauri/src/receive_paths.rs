use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

const RESERVATION_PREFIX: &str = "WELINE-LOCALNET-RESERVATION:";

pub struct FinalizedReceive {
    pub path: PathBuf,
    pub reservation_released: bool,
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

pub fn finalize_reserved_receive(
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

pub fn ensure_writable_directory(path: &Path) -> std::io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "receive directory must be absolute",
        ));
    }
    std::fs::create_dir_all(path)?;
    if !path.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "receive directory is not a folder",
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
    use std::fs;

    use super::{
        commit_without_overwrite, copy_without_overwrite, finalize_reserved_receive,
        remove_owned_reservation, reservation_is_owned, reserve_available_receive_path,
        reserve_receive_path, safe_file_name,
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
}
