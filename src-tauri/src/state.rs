use std::path::PathBuf;

use crate::{
    domain::{LocalProfile, PROTOCOL_VERSION, Platform},
    identity::LocalIdentity,
    storage::Storage,
};

pub struct AppState {
    pub storage: Storage,
    pub identity: LocalIdentity,
    pub app_data_dir: PathBuf,
}

impl AppState {
    pub fn local_profile(&self) -> Result<Option<LocalProfile>, crate::error::AppError> {
        Ok(self.storage.load_nickname()?.map(|nickname| LocalProfile {
            peer_id: self.identity.peer_id_string(),
            nickname,
            platform: Platform::current(),
            protocol_version: PROTOCOL_VERSION,
        }))
    }
}
