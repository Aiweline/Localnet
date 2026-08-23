use tauri::State;

use crate::{
    domain::{BootstrapSnapshot, LocalProfile, PROTOCOL_VERSION, Platform, validate_nickname},
    error::AppError,
    state::AppState,
};

#[tauri::command]
pub fn bootstrap(state: State<'_, AppState>) -> Result<BootstrapSnapshot, AppError> {
    state.storage.snapshot(state.local_profile()?)
}

#[tauri::command]
pub fn complete_onboarding(
    nickname: String,
    state: State<'_, AppState>,
) -> Result<LocalProfile, AppError> {
    save_nickname(&nickname, &state)
}

#[tauri::command]
pub fn update_nickname(
    nickname: String,
    state: State<'_, AppState>,
) -> Result<LocalProfile, AppError> {
    save_nickname(&nickname, &state)
}

fn save_nickname(nickname: &str, state: &AppState) -> Result<LocalProfile, AppError> {
    let profile = LocalProfile {
        peer_id: state.identity.peer_id_string(),
        nickname: validate_nickname(nickname)?,
        platform: Platform::current(),
        protocol_version: PROTOCOL_VERSION,
    };
    state.storage.save_profile(&profile)?;
    Ok(profile)
}
