mod commands;
mod domain;
mod error;
mod identity;
mod network;
mod protocol;
mod receive_paths;
mod state;
mod storage;
mod transfer_manifest;
pub mod transfer_policy;
mod update;
pub mod volume_preflight;

use std::{path::PathBuf, sync::OnceLock};

use state::AppState;
use tauri::{Manager, WindowEvent};
use tauri_plugin_dialog::{DialogExt, MessageDialogKind};

static LOGGING: OnceLock<()> = OnceLock::new();

pub fn run() {
    let allow_multiple = cfg!(debug_assertions) && std::env::var_os("LOCALNET_DATA_DIR").is_some();
    let mut builder = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_opener::init());

    if !allow_multiple {
        builder = builder.plugin(tauri_plugin_single_instance::init(|app, _, _| {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.unminimize();
                let _ = window.set_focus();
            }
        }));
    }

    builder
        .invoke_handler(tauri::generate_handler![
            commands::bootstrap,
            commands::presence,
            commands::refresh_discovery,
            commands::complete_onboarding,
            commands::update_nickname,
            commands::update_settings,
            commands::send_friend_request,
            commands::resolve_friend_request,
            commands::send_text,
            commands::retry_text,
            commands::send_file,
            commands::resolve_transfer,
            commands::cancel_transfer,
            commands::image_preview,
            commands::update_transfer_preferences,
            update::download_update,
            update::open_downloaded_update,
        ])
        .setup(|app| {
            initialize_logging();
            match initialize_state(app) {
                Ok(state) => {
                    state.start_network_if_ready(app.handle().clone())?;
                    app.manage(state);
                    Ok(())
                }
                Err(error) => {
                    app.dialog()
                        .message(error.to_string())
                        .title("Weline Localnet 无法启动")
                        .kind(MessageDialogKind::Error)
                        .blocking_show();
                    Err(Box::new(error))
                }
            }
        })
        .on_window_event(|window, event| {
            if matches!(event, WindowEvent::CloseRequested { .. }) {
                tracing::info!(window = window.label(), "Weline Localnet window closing");
            }
        })
        .run(tauri::generate_context!())
        .expect("failed to run Weline Localnet");
}

fn initialize_state<R: tauri::Runtime>(
    app: &mut tauri::App<R>,
) -> Result<AppState, error::AppError> {
    let app_data_dir = resolve_app_data_dir(app)?;
    std::fs::create_dir_all(&app_data_dir)?;
    let use_keyring = !(cfg!(debug_assertions) && std::env::var_os("LOCALNET_DATA_DIR").is_some());
    let identity = identity::LocalIdentity::load_or_create(&app_data_dir, use_keyring)?;
    let storage = storage::Storage::open(&app_data_dir.join("localnet.sqlite3"))?;
    let default_receive_directory = app
        .path()
        .download_dir()
        .unwrap_or_else(|_| app_data_dir.join("received"))
        .join("Weline Localnet");
    storage.load_transfer_preferences(&default_receive_directory)?;
    Ok(AppState::new(storage, identity, default_receive_directory))
}

fn resolve_app_data_dir<R: tauri::Runtime>(
    app: &tauri::App<R>,
) -> Result<PathBuf, error::AppError> {
    #[cfg(debug_assertions)]
    if let Some(path) = std::env::var_os("LOCALNET_DATA_DIR") {
        return Ok(PathBuf::from(path));
    }

    app.path().app_data_dir().map_err(|error| {
        error::AppError::Storage(format!("无法定位 Weline Localnet 应用数据目录：{error}"))
    })
}

fn initialize_logging() {
    LOGGING.get_or_init(|| {
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("localnet=info"));
        let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    });
}
