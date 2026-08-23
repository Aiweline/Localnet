use std::{path::PathBuf, sync::Mutex};

use tauri::AppHandle;

use crate::{
    domain::{LocalProfile, PROTOCOL_VERSION, Platform},
    error::AppError,
    identity::LocalIdentity,
    network::{NetworkCommand, NetworkHandle, spawn_network},
    storage::Storage,
};

pub struct AppState {
    pub storage: Storage,
    pub identity: LocalIdentity,
    pub app_data_dir: PathBuf,
    network: Mutex<Option<NetworkHandle>>,
}

impl AppState {
    pub fn new(storage: Storage, identity: LocalIdentity, app_data_dir: PathBuf) -> Self {
        Self {
            storage,
            identity,
            app_data_dir,
            network: Mutex::new(None),
        }
    }

    pub fn local_profile(&self) -> Result<Option<LocalProfile>, crate::error::AppError> {
        Ok(self.storage.load_nickname()?.map(|nickname| LocalProfile {
            peer_id: self.identity.peer_id_string(),
            nickname,
            platform: Platform::current(),
            protocol_version: PROTOCOL_VERSION,
        }))
    }

    pub fn start_network_if_ready(&self, app_handle: AppHandle) -> Result<(), AppError> {
        let Some(profile) = self.local_profile()? else {
            return Ok(());
        };
        let mut network = self
            .network
            .lock()
            .map_err(|_| AppError::Network("网络服务状态异常，请重新启动 Localnet".to_string()))?;
        if let Some(handle) = network.as_ref() {
            handle.try_send(NetworkCommand::SetProfile(profile))?;
        } else {
            *network = Some(spawn_network(
                self.identity.clone(),
                profile,
                self.storage.clone(),
                app_handle,
                self.app_data_dir.clone(),
            ));
        }
        Ok(())
    }

    pub fn network(&self) -> Result<NetworkHandle, AppError> {
        self.network
            .lock()
            .map_err(|_| AppError::Network("网络服务状态异常，请重新启动 Localnet".to_string()))?
            .clone()
            .ok_or_else(|| AppError::Network("请先设置昵称，再使用局域网功能".to_string()))
    }
}
