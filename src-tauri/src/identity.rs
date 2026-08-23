use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use keyring::{Entry, Error as KeyringError};
use libp2p::{PeerId, identity::Keypair};

use crate::error::AppError;

const KEYRING_SERVICE: &str = "com.aiweline.localnet";
const KEYRING_USER: &str = "device-ed25519";
const FALLBACK_FILE: &str = "identity.key";

#[derive(Clone)]
pub struct LocalIdentity {
    keypair: Keypair,
    peer_id: PeerId,
}

impl LocalIdentity {
    pub fn load_or_create(app_data_dir: &Path, use_keyring: bool) -> Result<Self, AppError> {
        fs::create_dir_all(app_data_dir)?;
        let fallback_path = app_data_dir.join(FALLBACK_FILE);

        if !use_keyring {
            return if fallback_path.exists() {
                Self::load_file(&fallback_path)
            } else {
                let identity = Self::generate()?;
                Self::write_file(&fallback_path, &identity.encode()?)?;
                Ok(identity)
            };
        }

        match Entry::new(KEYRING_SERVICE, KEYRING_USER) {
            Ok(entry) => match entry.get_secret() {
                Ok(encoded) => Self::decode(&encoded, "系统凭据存储"),
                Err(KeyringError::NoEntry) => {
                    if fallback_path.exists() {
                        return Self::load_file(&fallback_path);
                    }
                    let identity = Self::generate()?;
                    let encoded = identity.encode()?;
                    if entry.set_secret(&encoded).is_err() {
                        Self::write_file(&fallback_path, &encoded)?;
                    }
                    Ok(identity)
                }
                Err(_) if fallback_path.exists() => Self::load_file(&fallback_path),
                Err(_) => {
                    let identity = Self::generate()?;
                    Self::write_file(&fallback_path, &identity.encode()?)?;
                    Ok(identity)
                }
            },
            Err(_) if fallback_path.exists() => Self::load_file(&fallback_path),
            Err(_) => {
                let identity = Self::generate()?;
                Self::write_file(&fallback_path, &identity.encode()?)?;
                Ok(identity)
            }
        }
    }

    pub fn keypair(&self) -> Keypair {
        self.keypair.clone()
    }

    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    pub fn peer_id_string(&self) -> String {
        self.peer_id.to_string()
    }

    fn generate() -> Result<Self, AppError> {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        Ok(Self { keypair, peer_id })
    }

    fn encode(&self) -> Result<Vec<u8>, AppError> {
        self.keypair.to_protobuf_encoding().map_err(|error| {
            AppError::Identity(format!("无法保存设备身份，请重新启动 Localnet：{error}"))
        })
    }

    fn decode(bytes: &[u8], source: &str) -> Result<Self, AppError> {
        let keypair = Keypair::from_protobuf_encoding(bytes).map_err(|error| {
            AppError::Identity(format!(
                "{source}中的设备身份已损坏；为避免好友身份变化，Localnet 不会自动重建：{error}"
            ))
        })?;
        let peer_id = keypair.public().to_peer_id();
        Ok(Self { keypair, peer_id })
    }

    fn load_file(path: &Path) -> Result<Self, AppError> {
        let bytes = fs::read(path).map_err(|error| {
            AppError::Identity(format!("无法读取本机设备身份，请检查应用数据权限：{error}"))
        })?;
        Self::decode(&bytes, "本地身份文件")
    }

    fn write_file(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
        let temporary_path = Self::temporary_path(path);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }

        let mut file = options.open(&temporary_path).map_err(|error| {
            AppError::Identity(format!("无法创建本机设备身份，请检查应用数据权限：{error}"))
        })?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary_path, path).map_err(|error| {
            let _ = fs::remove_file(&temporary_path);
            AppError::Identity(format!("无法完成本机设备身份保存：{error}"))
        })?;
        Ok(())
    }

    fn temporary_path(path: &Path) -> PathBuf {
        let file_name = format!("identity-{}.part", uuid::Uuid::now_v7());
        path.with_file_name(file_name)
    }
}
