use std::{
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
};

use base64::Engine as _;
use libp2p::PeerId;
use sha2::{Digest, Sha256};
use tauri::{AppHandle, State};

use crate::{
    domain::{
        BootstrapSnapshot, ChatMessage, Direction, Friend, FriendRequest, FriendRequestStatus,
        LocalProfile, MessageKind, MessageStatus, PROTOCOL_VERSION, PeerSummary, Platform,
        PresenceSnapshot, TransferKind, TransferPreferences, TransferRecord, TransferStatus,
        now_rfc3339, validate_language_preference, validate_nickname, validate_text,
    },
    error::AppError,
    network::NetworkCommand,
    receive_paths::{
        ensure_writable_directory, preflight_receive_directory, remove_owned_reservation,
        reserve_receive_path,
    },
    state::AppState,
    storage::Storage,
    transfer_manifest::{TransferChunk, build_manifest_from_snapshot, capture_source_snapshot},
    transfer_policy::{TRANSFER_CHUNK_BYTES, TransferProtocol, select_transfer_protocol},
    volume_preflight::sanitize_destination_preflight_error,
};

#[tauri::command]
pub fn bootstrap(state: State<'_, AppState>) -> Result<BootstrapSnapshot, AppError> {
    state.storage.snapshot(
        state.local_profile()?,
        state.default_receive_directory.as_path(),
    )
}

#[tauri::command]
pub fn presence(state: State<'_, AppState>) -> Result<PresenceSnapshot, AppError> {
    state.storage.presence_snapshot()
}

#[tauri::command]
pub fn refresh_discovery(state: State<'_, AppState>) -> Result<(), AppError> {
    state.network()?.try_send(NetworkCommand::RefreshDiscovery)
}

#[tauri::command]
pub fn update_language_preference(
    language_preference: String,
    state: State<'_, AppState>,
) -> Result<String, AppError> {
    let language_preference = validate_language_preference(&language_preference)?;
    state
        .storage
        .save_language_preference(&language_preference)?;
    Ok(language_preference)
}

#[tauri::command]
pub fn complete_onboarding(
    nickname: String,
    app_handle: AppHandle,
    state: State<'_, AppState>,
) -> Result<LocalProfile, AppError> {
    save_nickname(&nickname, &app_handle, &state)
}

#[tauri::command]
pub fn update_nickname(
    nickname: String,
    app_handle: AppHandle,
    state: State<'_, AppState>,
) -> Result<LocalProfile, AppError> {
    save_nickname(&nickname, &app_handle, &state)
}

#[tauri::command]
pub fn update_settings(
    nickname: String,
    auto_receive_files: bool,
    receive_directory: String,
    app_handle: AppHandle,
    state: State<'_, AppState>,
) -> Result<LocalProfile, AppError> {
    let previous_nickname = state
        .storage
        .load_nickname()?
        .ok_or_else(|| AppError::InvalidInput("请先完成昵称设置".to_string()))?;
    let previous_preferences = state
        .storage
        .load_transfer_preferences(state.default_receive_directory.as_path())?;
    let profile = LocalProfile {
        peer_id: state.identity.peer_id_string(),
        nickname: validate_nickname(&nickname)?,
        platform: Platform::current(),
        protocol_version: PROTOCOL_VERSION,
    };
    let preferences = prepare_transfer_preferences(
        auto_receive_files,
        &receive_directory,
        &previous_preferences,
        state.default_receive_directory.as_path(),
    )?;
    state
        .storage
        .save_profile_and_transfer_preferences(&profile, &preferences)?;
    if let Err(error) = state.start_network_if_ready(app_handle) {
        let previous_profile = LocalProfile {
            peer_id: state.identity.peer_id_string(),
            nickname: previous_nickname,
            platform: Platform::current(),
            protocol_version: PROTOCOL_VERSION,
        };
        if let Err(rollback_error) = state
            .storage
            .save_profile_and_transfer_preferences(&previous_profile, &previous_preferences)
        {
            tracing::error!(%rollback_error, "failed to roll back application settings");
        }
        return Err(error);
    }
    Ok(profile)
}

#[tauri::command]
pub fn update_transfer_preferences(
    auto_receive_files: bool,
    receive_directory: String,
    state: State<'_, AppState>,
) -> Result<TransferPreferences, AppError> {
    let directory = prepare_receive_directory(
        &receive_directory,
        auto_receive_files,
        state.default_receive_directory.as_path(),
    )?;
    let preferences = TransferPreferences {
        auto_receive_files,
        receive_directory: directory.to_string_lossy().into_owned(),
    };
    state.storage.save_transfer_preferences(&preferences)?;
    Ok(preferences)
}

#[tauri::command]
pub fn send_friend_request(
    peer_id: String,
    state: State<'_, AppState>,
) -> Result<Option<FriendRequest>, AppError> {
    validate_peer_id(&peer_id, state.identity.peer_id_string().as_str())?;
    send_friend_request_if_needed(
        &state.storage,
        peer_id.clone(),
        || require_online_peer(&peer_id, &state),
        |request| {
            state
                .network()?
                .try_send(NetworkCommand::SendFriendRequest(request))
        },
    )
}

fn send_friend_request_if_needed(
    storage: &Storage,
    peer_id: String,
    resolve_online_peer: impl FnOnce() -> Result<PeerSummary, AppError>,
    dispatch: impl FnMut(FriendRequest) -> Result<(), AppError>,
) -> Result<Option<FriendRequest>, AppError> {
    if storage.is_friend(&peer_id)? {
        return Ok(None);
    }
    send_friend_request_with_dispatch(storage, peer_id, resolve_online_peer()?, dispatch)
}

fn send_friend_request_with_dispatch(
    storage: &Storage,
    peer_id: String,
    peer: PeerSummary,
    mut dispatch: impl FnMut(FriendRequest) -> Result<(), AppError>,
) -> Result<Option<FriendRequest>, AppError> {
    if storage.is_friend(&peer_id)? {
        return Ok(None);
    }
    if storage
        .find_pending_friend_request(&peer_id, Direction::Outgoing)?
        .is_some()
    {
        return Err(AppError::InvalidInput(
            "好友申请已经发送，请等待对方处理".to_string(),
        ));
    }
    let now = now_rfc3339();
    let request = FriendRequest {
        request_id: uuid::Uuid::new_v4().to_string(),
        peer_id,
        nickname: peer.nickname,
        direction: Direction::Outgoing,
        status: FriendRequestStatus::Pending,
        created_at: now.clone(),
        updated_at: now,
    };
    storage.put_friend_request(&request)?;
    if let Err(error) = dispatch(request.clone()) {
        storage.remove_pending_outgoing_friend_request(&request.request_id)?;
        return Err(error);
    }
    Ok(Some(request))
}

#[tauri::command]
pub fn resolve_friend_request(
    request_id: String,
    accepted: bool,
    state: State<'_, AppState>,
) -> Result<FriendRequest, AppError> {
    uuid::Uuid::parse_str(&request_id)
        .map_err(|_| AppError::InvalidInput("好友申请编号无效".to_string()))?;
    let request = state
        .storage
        .get_friend_request(&request_id)?
        .ok_or_else(|| AppError::InvalidInput("找不到这条好友申请".to_string()))?;
    if request.direction != Direction::Incoming || request.status != FriendRequestStatus::Pending {
        return Err(AppError::InvalidInput(
            "这条好友申请已经处理，请刷新后查看".to_string(),
        ));
    }
    let peer = state.storage.get_peer(&request.peer_id)?;
    let status = if accepted {
        FriendRequestStatus::Accepted
    } else {
        FriendRequestStatus::Rejected
    };
    let now = now_rfc3339();
    let friend = accepted.then(|| Friend {
        peer_id: request.peer_id.clone(),
        nickname: request.nickname.clone(),
        platform: peer
            .as_ref()
            .map_or(Platform::Unknown, |peer| peer.platform),
        online: true,
        added_at: now.clone(),
        last_seen: now.clone(),
    });
    state
        .storage
        .resolve_friend_request(&request_id, status, friend.as_ref(), &now)?;
    let network_command = NetworkCommand::ResolveFriendRequest {
        peer_id: request.peer_id.clone(),
    };
    match state.network() {
        Ok(network) => {
            if let Err(error) = network.try_send(network_command) {
                tracing::warn!(%error, "durable friend decision will retry after reconnect");
            }
        }
        Err(error) => {
            tracing::warn!(%error, "network runtime unavailable; durable friend decision retained");
        }
    }
    Ok(FriendRequest {
        status,
        updated_at: now,
        ..request
    })
}

#[tauri::command]
pub fn send_text(
    peer_id: String,
    body: String,
    state: State<'_, AppState>,
) -> Result<ChatMessage, AppError> {
    validate_peer_id(&peer_id, state.identity.peer_id_string().as_str())?;
    require_online_peer(&peer_id, &state)?;
    if !state.storage.is_friend(&peer_id)? {
        return Err(AppError::NotFriend);
    }
    let message = ChatMessage {
        message_id: uuid::Uuid::new_v4().to_string(),
        peer_id,
        direction: Direction::Outgoing,
        kind: MessageKind::Text,
        body: Some(validate_text(&body)?),
        local_path: None,
        file_name: None,
        file_size: None,
        status: MessageStatus::Sending,
        error: None,
        created_at: now_rfc3339(),
    };
    state.storage.insert_message(&message)?;
    if let Err(error) = state
        .network()?
        .try_send(NetworkCommand::SendText(message.clone()))
    {
        state.storage.update_message_status(
            &message.message_id,
            MessageStatus::Failed,
            Some(&error.to_string()),
        )?;
        return Err(error);
    }
    Ok(message)
}

#[tauri::command]
pub fn retry_text(message_id: String, state: State<'_, AppState>) -> Result<ChatMessage, AppError> {
    let mut message = state
        .storage
        .get_message(&message_id)?
        .ok_or_else(|| AppError::InvalidInput("找不到这条消息".to_string()))?;
    if message.direction != Direction::Outgoing || message.kind != MessageKind::Text {
        return Err(AppError::InvalidInput("这条消息不能重新发送".to_string()));
    }
    require_online_peer(&message.peer_id, &state)?;
    if !state.storage.is_friend(&message.peer_id)? {
        return Err(AppError::NotFriend);
    }
    message.status = MessageStatus::Sending;
    message.error = None;
    state
        .storage
        .update_message_status(&message.message_id, MessageStatus::Sending, None)?;
    state
        .network()?
        .try_send(NetworkCommand::SendText(message.clone()))?;
    Ok(message)
}

#[tauri::command]
pub async fn send_file(
    peer_id: String,
    path: String,
    kind: String,
    state: State<'_, AppState>,
) -> Result<TransferRecord, AppError> {
    validate_peer_id(&peer_id, state.identity.peer_id_string().as_str())?;
    let peer = require_online_peer(&peer_id, &state)?;
    if !state.storage.is_friend(&peer_id)? {
        return Err(AppError::NotFriend);
    }
    let kind = TransferKind::from_str(&kind)?;
    let capabilities = peer.capabilities;
    let source = tauri::async_runtime::spawn_blocking(move || {
        prepare_source(&path, kind, capabilities.as_slice())
    })
    .await
    .map_err(|error| AppError::Io(std::io::Error::other(error.to_string())))??;
    let now = now_rfc3339();
    let transfer_id = uuid::Uuid::now_v7().to_string();
    let transfer = TransferRecord {
        transfer_id: transfer_id.clone(),
        peer_id: peer_id.clone(),
        direction: Direction::Outgoing,
        kind,
        file_name: source.file_name.clone(),
        file_size: source.file_size,
        mime_type: source.mime_type,
        sha256: source.sha256,
        local_path: Some(source.path.to_string_lossy().into_owned()),
        destination_reserved: false,
        reservation_token: None,
        transfer_protocol: source.transfer_protocol,
        chunk_size: source.chunk_size,
        chunk_count: source.chunk_count,
        manifest_sha256: source.manifest_sha256,
        partial_path: None,
        source_modified_ns: source.source_modified_ns,
        send_claimed: false,
        transferred_bytes: 0,
        status: TransferStatus::AwaitingAcceptance,
        error: None,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let message = ChatMessage {
        message_id: transfer_id,
        peer_id,
        direction: Direction::Outgoing,
        kind: match kind {
            TransferKind::Image => MessageKind::Image,
            TransferKind::File => MessageKind::File,
        },
        body: None,
        local_path: transfer.local_path.clone(),
        file_name: Some(source.file_name),
        file_size: Some(source.file_size),
        status: MessageStatus::Sending,
        error: None,
        created_at: now,
    };
    if transfer.transfer_protocol == TransferProtocol::ResumableV2 as u8 {
        state
            .storage
            .create_outgoing_transfer_with_manifest(&transfer, &source.chunks)?;
    } else {
        state.storage.upsert_transfer(&transfer)?;
    }
    state.storage.insert_message(&message)?;
    if let Err(error) = state
        .network()?
        .try_send(NetworkCommand::OfferTransfer(transfer.clone()))
    {
        let mut failed = transfer.clone();
        failed.status = TransferStatus::Failed;
        failed.error = Some(error.to_string());
        failed.updated_at = now_rfc3339();
        state.storage.upsert_transfer(&failed)?;
        state.storage.update_message_status(
            &failed.transfer_id,
            MessageStatus::Failed,
            failed.error.as_deref(),
        )?;
        return Err(error);
    }
    Ok(transfer)
}

#[tauri::command]
pub async fn resolve_transfer(
    transfer_id: String,
    accepted: bool,
    save_path: Option<String>,
    state: State<'_, AppState>,
) -> Result<TransferRecord, AppError> {
    uuid::Uuid::parse_str(&transfer_id)
        .map_err(|_| AppError::InvalidInput("文件传输编号无效".to_string()))?;
    let mut transfer = state
        .storage
        .get_transfer(&transfer_id)?
        .ok_or_else(|| AppError::InvalidInput("找不到这次文件传输".to_string()))?;
    if transfer.direction != Direction::Incoming
        || transfer.status != TransferStatus::AwaitingAcceptance
    {
        return Err(AppError::InvalidInput(
            "这次文件传输已经处理，请刷新后查看".to_string(),
        ));
    }
    require_online_peer(&transfer.peer_id, &state)?;
    if accepted {
        let (completion_sender, completion_receiver) = tokio::sync::oneshot::channel();
        let completion = Arc::new(Mutex::new(Some(completion_sender)));
        let path = save_path
            .filter(|path| !path.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| AppError::InvalidInput("请选择文件保存位置".to_string()))?;
        if !path.is_absolute() {
            return Err(AppError::InvalidInput(
                "文件保存位置必须是绝对路径".to_string(),
            ));
        }
        transfer = accept_incoming_transfer_with_preflight(
            &state.storage,
            &transfer,
            &path,
            &preflight_receive_directory,
            &mut |accepted| {
                state.network()?.try_send(NetworkCommand::ResolveTransfer {
                    peer_id: accepted.peer_id.clone(),
                    transfer_id: accepted.transfer_id.clone(),
                    accepted: true,
                    completion: Some(completion.clone()),
                })
            },
        )?;
        match completion_receiver.await {
            Ok(Ok(())) => {
                return state
                    .storage
                    .get_transfer(&transfer.transfer_id)?
                    .ok_or_else(|| AppError::Storage("接收确认提交后传输记录不存在".to_string()));
            }
            Ok(Err(message)) => return Err(AppError::Network(message)),
            Err(error) => {
                let message = format!("接收确认未提交，请重新确认：{error}");
                if state
                    .storage
                    .pending_incoming_decision_token(&transfer.transfer_id)?
                    .is_none()
                {
                    crate::network::return_pending_incoming_decision_to_manual(
                        &transfer.transfer_id,
                        &state.storage,
                        message.clone(),
                    )?;
                }
                return Err(AppError::Network(message));
            }
        }
    } else {
        if !state.storage.try_cancel_unclaimed_incoming_transfer(
            &transfer.transfer_id,
            &transfer.peer_id,
            transfer.transfer_protocol,
            "你拒绝了这次传输",
        )? {
            return Err(AppError::InvalidInput(
                "这次文件传输已经处理，请刷新后查看".to_string(),
            ));
        }
        transfer = state
            .storage
            .get_transfer(&transfer.transfer_id)?
            .ok_or_else(|| AppError::InvalidInput("找不到这次文件传输".to_string()))?;
    }
    let network = match state.network() {
        Ok(network) => network,
        Err(error) => {
            if transfer.destination_reserved {
                if let (Some(path), Some(token)) = (
                    transfer.local_path.as_deref(),
                    transfer.reservation_token.as_deref(),
                ) {
                    match remove_owned_reservation(Path::new(path), &transfer.transfer_id, token) {
                        Ok(_) => {
                            transfer.destination_reserved = false;
                            transfer.reservation_token = None;
                        }
                        Err(cleanup_error) => {
                            tracing::warn!(
                                transfer_id = %transfer.transfer_id,
                                %cleanup_error,
                                "failed to clean receive reservation after network failure"
                            );
                        }
                    }
                }
            }
            transfer.status = TransferStatus::Failed;
            transfer.error = Some(error.to_string());
            transfer.updated_at = now_rfc3339();
            state.storage.upsert_transfer(&transfer)?;
            return Err(error);
        }
    };
    if let Err(error) = network.try_send(NetworkCommand::ResolveTransfer {
        peer_id: transfer.peer_id.clone(),
        transfer_id: transfer_id.clone(),
        accepted,
        completion: None,
    }) {
        if transfer.destination_reserved {
            if let (Some(path), Some(token)) = (
                transfer.local_path.as_deref(),
                transfer.reservation_token.as_deref(),
            ) {
                match remove_owned_reservation(Path::new(path), &transfer.transfer_id, token) {
                    Ok(_) => {
                        transfer.destination_reserved = false;
                        transfer.reservation_token = None;
                    }
                    Err(cleanup_error) => {
                        tracing::warn!(
                            transfer_id = %transfer.transfer_id,
                            %cleanup_error,
                            "failed to clean receive reservation after decision failure"
                        );
                    }
                }
            }
        }
        transfer.status = TransferStatus::Failed;
        transfer.error = Some(error.to_string());
        transfer.updated_at = now_rfc3339();
        state.storage.upsert_transfer(&transfer)?;
        return Err(error);
    }
    Ok(transfer)
}

#[tauri::command]
pub fn cancel_transfer(
    transfer_id: String,
    state: State<'_, AppState>,
) -> Result<TransferRecord, AppError> {
    let transfer = cancel_transfer_locally(&state.storage, &transfer_id)?;
    if transfer.transfer_protocol == TransferProtocol::ResumableV2 as u8 {
        if let Ok(network) = state.network() {
            let _ = network.try_send(NetworkCommand::FlushTerminalNotifications {
                peer_id: transfer.peer_id.clone(),
            });
        }
    } else {
        state.network()?.try_send(NetworkCommand::CancelTransfer {
            peer_id: transfer.peer_id.clone(),
            transfer_id,
        })?;
    }
    Ok(transfer)
}

fn cancel_transfer_locally(
    storage: &Storage,
    transfer_id: &str,
) -> Result<TransferRecord, AppError> {
    let transfer = storage
        .get_transfer(transfer_id)?
        .ok_or_else(|| AppError::InvalidInput("找不到这次文件传输".to_string()))?;
    let cancelled = if transfer.transfer_protocol == TransferProtocol::ResumableV2 as u8 {
        match transfer.direction {
            Direction::Outgoing => storage.try_cancel_unclaimed_outgoing_transfer(
                &transfer.transfer_id,
                &transfer.peer_id,
                "你取消了传输",
            )?,
            Direction::Incoming => storage.try_cancel_unclaimed_incoming_transfer(
                &transfer.transfer_id,
                &transfer.peer_id,
                transfer.transfer_protocol,
                "你取消了传输",
            )?,
        }
    } else if transfer.direction == Direction::Outgoing
        && transfer.status == TransferStatus::AwaitingAcceptance
    {
        storage.try_transition_outgoing_awaiting(
            &transfer.transfer_id,
            &transfer.peer_id,
            TransferStatus::Cancelled,
            Some("你取消了传输"),
        )?
    } else {
        false
    };
    if !cancelled {
        return Err(AppError::InvalidInput("当前传输不能取消".to_string()));
    }

    let transfer = storage
        .get_transfer(&transfer.transfer_id)?
        .ok_or_else(|| AppError::InvalidInput("找不到这次文件传输".to_string()))?;
    if transfer.direction == Direction::Outgoing {
        storage.update_message_status(
            &transfer.transfer_id,
            MessageStatus::Failed,
            transfer.error.as_deref(),
        )?;
    }
    Ok(transfer)
}

#[tauri::command]
pub async fn image_preview(
    message_id: String,
    state: State<'_, AppState>,
) -> Result<Option<String>, AppError> {
    let message = state
        .storage
        .get_message(&message_id)?
        .ok_or_else(|| AppError::InvalidInput("找不到这张图片".to_string()))?;
    if message.kind != MessageKind::Image || message.status != MessageStatus::Delivered {
        return Ok(None);
    }
    let Some(path) = message.local_path else {
        return Ok(None);
    };
    tauri::async_runtime::spawn_blocking(move || {
        let path = PathBuf::from(path);
        let metadata = std::fs::metadata(&path)?;
        if !metadata.is_file() || metadata.len() > 12 * 1024 * 1024 {
            return Ok(None);
        }
        let mime_type = detect_mime_type(&path);
        if !mime_type.starts_with("image/") || mime_type == "image/svg+xml" {
            return Ok(None);
        }
        let bytes = std::fs::read(path)?;
        Ok(Some(format!(
            "data:{mime_type};base64,{}",
            base64::engine::general_purpose::STANDARD.encode(bytes)
        )))
    })
    .await
    .map_err(|error| AppError::Io(std::io::Error::other(error.to_string())))?
}

#[tauri::command]
pub async fn save_message_file_as(
    message_id: String,
    destination_path: String,
    state: State<'_, AppState>,
) -> Result<u64, AppError> {
    let storage = state.storage.clone();
    tauri::async_runtime::spawn_blocking(move || {
        save_message_file_as_with_storage(&storage, &message_id, Path::new(&destination_path))
    })
    .await
    .map_err(|error| AppError::Io(std::io::Error::other(error.to_string())))?
}

fn save_message_file_as_with_storage(
    storage: &Storage,
    message_id: &str,
    destination: &Path,
) -> Result<u64, AppError> {
    save_message_file_as_with_storage_and_hook(storage, message_id, destination, |_| {})
}

fn save_message_file_as_with_storage_and_hook<F>(
    storage: &Storage,
    message_id: &str,
    destination: &Path,
    before_finalize: F,
) -> Result<u64, AppError>
where
    F: FnOnce(&Path),
{
    let message = storage
        .get_message(message_id)?
        .ok_or_else(|| AppError::InvalidInput("找不到这条文件消息".to_string()))?;
    if !matches!(message.kind, MessageKind::Image | MessageKind::File)
        || message.status != MessageStatus::Delivered
    {
        return Err(AppError::InvalidInput(
            "只有已完成传输的图片或文件可以另存".to_string(),
        ));
    }
    let source = message
        .local_path
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .ok_or_else(|| AppError::InvalidInput("这条文件消息没有可用的本地文件".to_string()))?;
    let source = std::fs::canonicalize(source)?;
    let mut source_file = File::open(&source)?;
    let source_metadata = source_file.metadata()?;
    if !source_metadata.is_file() {
        return Err(AppError::InvalidInput(
            "消息对应的本地文件不可用".to_string(),
        ));
    }
    if !destination.is_absolute() {
        return Err(AppError::InvalidInput("另存位置必须是绝对路径".to_string()));
    }
    let parent = destination
        .parent()
        .filter(|parent| parent.is_dir())
        .ok_or_else(|| AppError::InvalidInput("另存目录不存在".to_string()))?;
    let destination_name = destination
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| AppError::InvalidInput("另存文件名无效".to_string()))?;

    if destination.exists() {
        if destination.is_dir() {
            return Err(AppError::InvalidInput("另存位置不能是目录".to_string()));
        }
        if std::fs::canonicalize(destination)? == source {
            return Ok(source_metadata.len());
        }
    }

    let (temporary, staging_directory, mut temporary_file) =
        create_save_temporary(parent, destination_name)?;
    let temporary_identity = save_file_identity(&temporary_file)?;
    let copy_result = (|| -> Result<u64, AppError> {
        let copied = std::io::copy(&mut source_file, &mut temporary_file)?;
        if copied != source_metadata.len() {
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "source file changed while saving a copy",
            )));
        }
        temporary_file.sync_all()?;
        before_finalize(&temporary);
        if !save_path_has_identity(&temporary, temporary_identity)? {
            return Err(AppError::Io(std::io::Error::other(
                "save-as staging file identity changed before finalization",
            )));
        }
        replace_saved_copy(&temporary_file, &temporary, destination, temporary_identity)?;
        Ok(copied)
    })();
    if copy_result.is_err() {
        remove_save_temporary_if_owned(&temporary, temporary_identity);
    }
    drop(temporary_file);
    if let Some(staging_directory) = staging_directory {
        let _ = std::fs::remove_dir(staging_directory);
    }
    copy_result
}

#[cfg(windows)]
fn create_save_temporary(
    parent: &Path,
    destination_name: &std::ffi::OsStr,
) -> Result<(PathBuf, Option<PathBuf>, File), AppError> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_READ, FILE_GENERIC_WRITE,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let temporary = parent.join(format!(
        ".{}.weline-save-{}.part",
        destination_name.to_string_lossy(),
        uuid::Uuid::now_v7()
    ));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .access_mode(FILE_GENERIC_READ | FILE_GENERIC_WRITE | DELETE)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .create_new(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(&temporary)?;
    Ok((temporary, None, file))
}

#[cfg(unix)]
fn create_save_temporary(
    parent: &Path,
    _destination_name: &std::ffi::OsStr,
) -> Result<(PathBuf, Option<PathBuf>, File), AppError> {
    use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};

    let staging_directory = parent.join(format!(".weline-save-{}.staging", uuid::Uuid::now_v7()));
    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700).create(&staging_directory)?;
    let temporary = staging_directory.join("payload");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)?;
    Ok((temporary, Some(staging_directory), file))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SaveFileIdentity {
    volume: u64,
    file: u64,
}

#[cfg(windows)]
fn save_file_identity(file: &File) -> Result<SaveFileIdentity, AppError> {
    use std::{mem::MaybeUninit, os::windows::io::AsRawHandle as _};
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let mut information = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    let succeeded =
        unsafe { GetFileInformationByHandle(file.as_raw_handle(), information.as_mut_ptr()) };
    if succeeded == 0 {
        return Err(AppError::Io(std::io::Error::last_os_error()));
    }
    let information = unsafe { information.assume_init() };
    Ok(SaveFileIdentity {
        volume: u64::from(information.dwVolumeSerialNumber),
        file: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow),
    })
}

#[cfg(unix)]
fn save_file_identity(file: &File) -> Result<SaveFileIdentity, AppError> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = file.metadata()?;
    Ok(SaveFileIdentity {
        volume: metadata.dev(),
        file: metadata.ino(),
    })
}

#[cfg(windows)]
fn open_save_path_no_follow(path: &Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
}

#[cfg(unix)]
fn open_save_path_no_follow(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

fn save_path_has_identity(path: &Path, expected: SaveFileIdentity) -> Result<bool, AppError> {
    match open_save_path_no_follow(path) {
        Ok(file) => Ok(save_file_identity(&file)? == expected),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(AppError::Io(error)),
    }
}

fn remove_save_temporary_if_owned(path: &Path, expected: SaveFileIdentity) {
    if matches!(save_path_has_identity(path, expected), Ok(true)) {
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(windows)]
fn replace_saved_copy(
    source_file: &File,
    _source: &Path,
    destination: &Path,
    expected: SaveFileIdentity,
) -> Result<(), AppError> {
    use std::{
        mem::size_of,
        os::windows::{ffi::OsStrExt as _, io::AsRawHandle as _},
    };
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_RENAME_INFO, FILE_RENAME_INFO_0, FileRenameInfo, FileRenameInfoEx,
        SetFileInformationByHandle,
    };

    let destination_wide: Vec<u16> = destination.as_os_str().encode_wide().collect();
    let header_size = size_of::<FILE_RENAME_INFO>() - size_of::<u16>();
    let byte_length = header_size
        .checked_add(destination_wide.len().saturating_mul(size_of::<u16>()))
        .ok_or_else(|| AppError::InvalidInput("另存位置过长".to_string()))?;
    let mut buffer = vec![0usize; byte_length.div_ceil(size_of::<usize>())];
    let information = buffer.as_mut_ptr().cast::<FILE_RENAME_INFO>();
    unsafe {
        (*information).Anonymous = FILE_RENAME_INFO_0 { Flags: 1 };
        (*information).RootDirectory = std::ptr::null_mut();
        (*information).FileNameLength = u32::try_from(destination_wide.len() * size_of::<u16>())
            .map_err(|_| AppError::InvalidInput("另存位置过长".to_string()))?;
        std::ptr::copy_nonoverlapping(
            destination_wide.as_ptr(),
            std::ptr::addr_of_mut!((*information).FileName).cast::<u16>(),
            destination_wide.len(),
        );
    }

    let mut renamed = unsafe {
        SetFileInformationByHandle(
            source_file.as_raw_handle(),
            FileRenameInfoEx,
            information.cast(),
            u32::try_from(byte_length).expect("rename buffer length fits in u32"),
        )
    };
    if renamed == 0 {
        unsafe {
            (*information).Anonymous = FILE_RENAME_INFO_0 {
                ReplaceIfExists: true,
            }
        };
        renamed = unsafe {
            SetFileInformationByHandle(
                source_file.as_raw_handle(),
                FileRenameInfo,
                information.cast(),
                u32::try_from(byte_length).expect("rename buffer length fits in u32"),
            )
        };
    }
    if renamed == 0 {
        return Err(AppError::Io(std::io::Error::last_os_error()));
    }
    if !save_path_has_identity(destination, expected)? {
        return Err(AppError::Io(std::io::Error::other(
            "save-as destination identity changed during finalization",
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn replace_saved_copy(
    _source_file: &File,
    source: &Path,
    destination: &Path,
    expected: SaveFileIdentity,
) -> Result<(), AppError> {
    std::fs::rename(source, destination)?;
    if !save_path_has_identity(destination, expected)? {
        return Err(AppError::Io(std::io::Error::other(
            "save-as destination identity changed during finalization",
        )));
    }
    Ok(())
}

fn save_nickname(
    nickname: &str,
    app_handle: &AppHandle,
    state: &AppState,
) -> Result<LocalProfile, AppError> {
    let profile = LocalProfile {
        peer_id: state.identity.peer_id_string(),
        nickname: validate_nickname(nickname)?,
        platform: Platform::current(),
        protocol_version: PROTOCOL_VERSION,
    };
    state.storage.save_profile(&profile)?;
    state.start_network_if_ready(app_handle.clone())?;
    Ok(profile)
}

fn prepare_receive_directory(
    value: &str,
    auto_receive_files: bool,
    default_directory: &Path,
) -> Result<PathBuf, AppError> {
    let value = value.trim();
    let path = if value.is_empty() {
        default_directory.to_path_buf()
    } else {
        PathBuf::from(value)
    };
    if !auto_receive_files {
        return Ok(path);
    }
    if !path.is_absolute() {
        return Err(AppError::InvalidInput(
            "文件接收目录必须是绝对路径".to_string(),
        ));
    }
    ensure_writable_directory(&path)
        .map_err(|error| AppError::Permission(format!("文件接收目录不可写，请重新选择：{error}")))
}

fn reserve_manual_receive_destination(
    path: &Path,
    transfer_id: &str,
    reservation_token: &str,
) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "接收文件保存位置无效")
    })?;
    if !parent.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "接收目录或磁盘当前不可用",
        ));
    }
    reserve_receive_path(path, transfer_id, reservation_token)
}

fn accept_incoming_transfer_with_preflight<P, D>(
    storage: &Storage,
    transfer: &TransferRecord,
    path: &Path,
    preflight: &P,
    dispatch: &mut D,
) -> Result<TransferRecord, AppError>
where
    P: Fn(&Path, u64, u64) -> Result<(), AppError>,
    D: FnMut(&TransferRecord) -> Result<(), AppError>,
{
    if !path.is_absolute() {
        return Err(AppError::InvalidInput(
            "文件保存位置必须是绝对路径".to_string(),
        ));
    }
    storage.drain_incoming_cleanup_before_acceptance(&transfer.transfer_id)?;
    let parent = path
        .parent()
        .ok_or_else(|| AppError::InvalidInput("接收文件保存位置无效".to_string()))?;
    if let Err(error) = preflight(parent, transfer.file_size, 0) {
        return Err(sanitize_destination_preflight_error(parent, error));
    }

    let reservation_token = uuid::Uuid::now_v7().to_string();
    reserve_manual_receive_destination(path, &transfer.transfer_id, &reservation_token)
        .map_err(map_receive_reservation_error)?;

    let mut accepted = transfer.clone();
    accepted.local_path = Some(path.to_string_lossy().into_owned());
    accepted.destination_reserved = true;
    accepted.reservation_token = Some(reservation_token);
    accepted.status = TransferStatus::Transferring;
    accepted.error = None;
    accepted.updated_at = now_rfc3339();
    match storage.try_accept_incoming_transfer(&accepted) {
        Ok(true) => {}
        Ok(false) => {
            cleanup_manual_reservation(&accepted);
            return Err(AppError::InvalidInput(
                "这次文件传输已经处理，请刷新后查看".to_string(),
            ));
        }
        Err(error) => {
            cleanup_manual_reservation(&accepted);
            return Err(error);
        }
    }

    if let Err(error) = dispatch(&accepted) {
        if let Err(rollback_error) = crate::network::return_pending_incoming_decision_to_manual(
            &accepted.transfer_id,
            storage,
            error.to_string(),
        ) {
            tracing::warn!(
                transfer_id = %accepted.transfer_id,
                %rollback_error,
                "failed to return incoming acceptance to manual review"
            );
        }
        return Err(error);
    }
    Ok(accepted)
}

fn cleanup_manual_reservation(transfer: &TransferRecord) {
    if let (Some(path), Some(token)) = (
        transfer.local_path.as_deref(),
        transfer.reservation_token.as_deref(),
    ) {
        let _ = remove_owned_reservation(Path::new(path), &transfer.transfer_id, token);
    }
}

fn map_receive_reservation_error(error: std::io::Error) -> AppError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        AppError::InvalidInput("保存位置已经存在同名文件，请重新选择".to_string())
    } else {
        AppError::Permission(format!("无法占用文件保存位置，请重新选择：{error}"))
    }
}

fn prepare_transfer_preferences(
    auto_receive_files: bool,
    receive_directory: &str,
    previous: &TransferPreferences,
    default_directory: &Path,
) -> Result<TransferPreferences, AppError> {
    let receive_directory = receive_directory.trim();
    if auto_receive_files == previous.auto_receive_files
        && receive_directory == previous.receive_directory
    {
        return Ok(previous.clone());
    }
    let directory =
        prepare_receive_directory(receive_directory, auto_receive_files, default_directory)?;
    Ok(TransferPreferences {
        auto_receive_files,
        receive_directory: directory.to_string_lossy().into_owned(),
    })
}

fn validate_peer_id(peer_id: &str, local_peer_id: &str) -> Result<(), AppError> {
    peer_id
        .parse::<PeerId>()
        .map_err(|_| AppError::InvalidInput("用户设备编号无效".to_string()))?;
    if peer_id == local_peer_id {
        return Err(AppError::InvalidInput("不能添加自己为好友".to_string()));
    }
    Ok(())
}

fn require_online_peer(
    peer_id: &str,
    state: &AppState,
) -> Result<crate::domain::PeerSummary, AppError> {
    let peer = state
        .storage
        .get_peer(peer_id)?
        .ok_or(AppError::OfflinePeer)?;
    if !peer.online {
        return Err(AppError::OfflinePeer);
    }
    if peer.protocol_version != PROTOCOL_VERSION {
        return Err(AppError::IncompatibleProtocol);
    }
    Ok(peer)
}

struct PreparedSource {
    path: PathBuf,
    file_name: String,
    file_size: u64,
    mime_type: String,
    sha256: String,
    transfer_protocol: u8,
    chunk_size: u32,
    chunk_count: u32,
    manifest_sha256: Option<String>,
    source_modified_ns: Option<u64>,
    chunks: Vec<TransferChunk>,
}

fn prepare_source(
    path: &str,
    kind: TransferKind,
    capabilities: &[String],
) -> Result<PreparedSource, AppError> {
    let path = std::fs::canonicalize(path)?;
    let source_snapshot = capture_source_snapshot(&path)?;
    let transfer_protocol = select_transfer_protocol(capabilities, source_snapshot.file_size)?;
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| !name.trim().is_empty())
        .ok_or_else(|| AppError::InvalidInput("文件名无效".to_string()))?;
    let mime_type = detect_mime_type(&path);
    if kind == TransferKind::Image && !mime_type.starts_with("image/") {
        return Err(AppError::InvalidInput(
            "所选文件不是支持的图片格式".to_string(),
        ));
    }
    let (file_size, sha256, chunk_size, chunk_count, manifest_sha256, source_modified_ns, chunks) =
        match transfer_protocol {
            TransferProtocol::LegacyV1 => (
                source_snapshot.file_size,
                hash_file(&path)?,
                0,
                0,
                None,
                None,
                Vec::new(),
            ),
            TransferProtocol::ResumableV2 => {
                let manifest =
                    build_manifest_from_snapshot(&path, TRANSFER_CHUNK_BYTES, source_snapshot)?;
                let chunk_count = u32::try_from(manifest.chunks.len())
                    .map_err(|_| AppError::InvalidInput("分块数量超出协议限制".to_string()))?;
                (
                    manifest.file_size,
                    hex::encode(manifest.file_sha256),
                    TRANSFER_CHUNK_BYTES,
                    chunk_count,
                    Some(hex::encode(manifest.manifest_sha256)),
                    Some(manifest.source_modified_ns),
                    manifest.chunks,
                )
            }
        };
    Ok(PreparedSource {
        path,
        file_name,
        file_size,
        mime_type,
        sha256,
        transfer_protocol: transfer_protocol as u8,
        chunk_size,
        chunk_count,
        manifest_sha256,
        source_modified_ns,
        chunks,
    })
}

fn hash_file(path: &Path) -> Result<String, AppError> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn detect_mime_type(path: &Path) -> String {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match extension.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "heic" | "heif" => "image/heic",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        "txt" | "md" | "csv" | "log" => "text/plain",
        "zip" => "application/zip",
        "json" => "application/json",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        _ => "application/octet-stream",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use super::{
        accept_incoming_transfer_with_preflight, cancel_transfer_locally,
        prepare_receive_directory, prepare_source, prepare_transfer_preferences,
        reserve_manual_receive_destination, save_message_file_as_with_storage,
        save_message_file_as_with_storage_and_hook, send_friend_request_if_needed,
    };
    use crate::{
        domain::{
            ChatMessage, Direction, Friend, FriendRequest, FriendRequestStatus, MessageKind,
            MessageStatus, PeerSummary, Platform, TransferKind, TransferPreferences,
            TransferRecord, TransferStatus, now_rfc3339,
        },
        error::AppError,
        receive_paths::{preflight_receive_directory, reserve_receive_path},
        storage::Storage,
        transfer_policy::{FILE_RESUME_V2_CAPABILITY, TRANSFER_CHUNK_BYTES, TransferProtocol},
        volume_preflight::{VolumeSnapshot, validate_volume},
    };

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    const DESTINATION_RESERVE_BYTES: u64 = 64 * MIB;

    #[test]
    fn save_as_copies_only_the_authoritative_delivered_attachment_source() {
        let directory =
            std::env::temp_dir().join(format!("weline-localnet-save-as-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(&directory).expect("create save-as fixture");
        let storage =
            Storage::open(&directory.join("localnet.sqlite3")).expect("open save-as storage");
        let source = directory.join("authoritative-source.bin");
        fs::write(&source, b"trusted attachment bytes").expect("write attachment source");
        let message = ChatMessage {
            message_id: "save-as-delivered-file".to_string(),
            peer_id: "save-as-peer".to_string(),
            direction: Direction::Incoming,
            kind: MessageKind::File,
            body: None,
            local_path: Some(source.to_string_lossy().into_owned()),
            file_name: Some("report.bin".to_string()),
            file_size: Some(24),
            status: MessageStatus::Delivered,
            error: None,
            created_at: now_rfc3339(),
        };
        storage
            .insert_message(&message)
            .expect("persist attachment message");
        let destination = directory.join("saved-copy.bin");

        let copied = save_message_file_as_with_storage(&storage, &message.message_id, &destination)
            .expect("save delivered attachment");

        assert_eq!(copied, 24);
        assert_eq!(
            fs::read(&destination).expect("read saved attachment"),
            b"trusted attachment bytes"
        );
        fs::write(&destination, b"old destination bytes").expect("replace destination fixture");
        let replaced =
            save_message_file_as_with_storage(&storage, &message.message_id, &destination)
                .expect("replace the user-confirmed destination");
        assert_eq!(replaced, 24);
        assert_eq!(
            fs::read(&destination).expect("read replaced attachment"),
            b"trusted attachment bytes"
        );
        assert_eq!(
            save_message_file_as_with_storage(&storage, &message.message_id, &source)
                .expect("saving to the source path is a safe no-op"),
            24
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove save-as fixture");
    }

    #[test]
    fn save_as_fails_closed_if_the_staging_path_is_replaced_before_finalize() {
        let directory = std::env::temp_dir().join(format!(
            "weline-localnet-save-as-swap-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&directory).expect("create save-as swap fixture");
        let storage =
            Storage::open(&directory.join("localnet.sqlite3")).expect("open save-as swap storage");
        let source = directory.join("trusted-source.bin");
        fs::write(&source, b"trusted attachment bytes").expect("write trusted attachment");
        let message = ChatMessage {
            message_id: "save-as-staging-swap".to_string(),
            peer_id: "save-as-peer".to_string(),
            direction: Direction::Incoming,
            kind: MessageKind::File,
            body: None,
            local_path: Some(source.to_string_lossy().into_owned()),
            file_name: Some("report.bin".to_string()),
            file_size: Some(24),
            status: MessageStatus::Delivered,
            error: None,
            created_at: now_rfc3339(),
        };
        storage
            .insert_message(&message)
            .expect("persist swap attachment message");
        let destination = directory.join("existing-destination.bin");
        fs::write(&destination, b"existing destination bytes").expect("write existing destination");
        let displaced = directory.join("displaced-trusted-copy.bin");

        let error = save_message_file_as_with_storage_and_hook(
            &storage,
            &message.message_id,
            &destination,
            |temporary| {
                fs::rename(temporary, &displaced).expect("move the verified staging file");
                fs::write(temporary, b"attacker replacement bytes")
                    .expect("replace the staging path");
            },
        )
        .expect_err("a replaced staging path must fail closed");

        assert!(matches!(error, AppError::Io(_)));
        assert_eq!(
            fs::read(&destination).expect("read protected destination"),
            b"existing destination bytes"
        );
        assert_ne!(
            fs::read(&destination).expect("read protected destination again"),
            b"attacker replacement bytes"
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove save-as swap fixture");
    }

    #[test]
    fn save_as_rejects_text_and_unfinished_attachment_messages() {
        let directory = std::env::temp_dir().join(format!(
            "weline-localnet-save-as-reject-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&directory).expect("create save-as reject fixture");
        let storage = Storage::open(&directory.join("localnet.sqlite3"))
            .expect("open save-as reject storage");
        let source = directory.join("private.bin");
        fs::write(&source, b"must not be copied").expect("write private fixture");

        for (message_id, kind, status) in [
            ("text-message", MessageKind::Text, MessageStatus::Delivered),
            ("pending-file", MessageKind::File, MessageStatus::Sending),
        ] {
            storage
                .insert_message(&ChatMessage {
                    message_id: message_id.to_string(),
                    peer_id: "save-as-peer".to_string(),
                    direction: Direction::Incoming,
                    kind,
                    body: None,
                    local_path: Some(source.to_string_lossy().into_owned()),
                    file_name: Some("private.bin".to_string()),
                    file_size: Some(18),
                    status,
                    error: None,
                    created_at: now_rfc3339(),
                })
                .expect("persist rejected message fixture");

            let destination = directory.join(format!("{message_id}-copy.bin"));
            let error = save_message_file_as_with_storage(&storage, message_id, &destination)
                .expect_err("ineligible message must be rejected");
            assert!(matches!(error, AppError::InvalidInput(_)));
            assert!(!destination.exists());
        }

        let missing_destination = directory.join("unknown-message-copy.bin");
        assert!(matches!(
            save_message_file_as_with_storage(&storage, "unknown-message", &missing_destination),
            Err(AppError::InvalidInput(_))
        ));
        assert!(!missing_destination.exists());

        for (message_id, local_path) in [
            ("pathless-file", None),
            (
                "missing-source-file",
                Some(
                    directory
                        .join("missing-source.bin")
                        .to_string_lossy()
                        .into_owned(),
                ),
            ),
            (
                "directory-source-file",
                Some(directory.to_string_lossy().into_owned()),
            ),
        ] {
            storage
                .insert_message(&ChatMessage {
                    message_id: message_id.to_string(),
                    peer_id: "save-as-peer".to_string(),
                    direction: Direction::Incoming,
                    kind: MessageKind::File,
                    body: None,
                    local_path,
                    file_name: Some("fixture.bin".to_string()),
                    file_size: Some(1),
                    status: MessageStatus::Delivered,
                    error: None,
                    created_at: now_rfc3339(),
                })
                .expect("persist invalid source fixture");
            let destination = directory.join(format!("{message_id}-copy.bin"));
            assert!(
                save_message_file_as_with_storage(&storage, message_id, &destination).is_err(),
                "invalid source must be rejected: {message_id}"
            );
            assert!(!destination.exists());
        }

        storage
            .insert_message(&ChatMessage {
                message_id: "directory-destination".to_string(),
                peer_id: "save-as-peer".to_string(),
                direction: Direction::Incoming,
                kind: MessageKind::File,
                body: None,
                local_path: Some(source.to_string_lossy().into_owned()),
                file_name: Some("private.bin".to_string()),
                file_size: Some(18),
                status: MessageStatus::Delivered,
                error: None,
                created_at: now_rfc3339(),
            })
            .expect("persist destination validation fixture");
        assert!(matches!(
            save_message_file_as_with_storage(&storage, "directory-destination", &directory),
            Err(AppError::InvalidInput(_))
        ));

        drop(storage);
        fs::remove_dir_all(directory).expect("remove save-as reject fixture");
    }

    fn acceptance_fixture(name: &str, file_size: u64) -> (PathBuf, Storage, TransferRecord) {
        let directory = std::env::temp_dir().join(format!(
            "weline-localnet-manual-preflight-{name}-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&directory).expect("create manual preflight fixture");
        let storage = Storage::open(&directory.join("localnet.sqlite3"))
            .expect("open manual preflight storage");
        let now = now_rfc3339();
        let transfer = TransferRecord {
            transfer_id: uuid::Uuid::now_v7().to_string(),
            peer_id: "manual-preflight-peer".to_string(),
            direction: Direction::Incoming,
            kind: TransferKind::File,
            file_name: "archive.bin".to_string(),
            file_size,
            mime_type: "application/octet-stream".to_string(),
            sha256: "0".repeat(64),
            local_path: None,
            destination_reserved: false,
            reservation_token: None,
            transfer_protocol: TransferProtocol::ResumableV2 as u8,
            chunk_size: TRANSFER_CHUNK_BYTES,
            chunk_count: u32::try_from(file_size.div_ceil(u64::from(TRANSFER_CHUNK_BYTES)))
                .expect("fixture chunk count"),
            manifest_sha256: Some("1".repeat(64)),
            partial_path: None,
            source_modified_ns: None,
            send_claimed: false,
            transferred_bytes: 0,
            status: TransferStatus::AwaitingAcceptance,
            error: None,
            created_at: now.clone(),
            updated_at: now,
        };
        storage
            .upsert_transfer(&transfer)
            .expect("persist incoming acceptance fixture");
        (directory, storage, transfer)
    }

    #[test]
    fn existing_friend_request_is_idempotent_and_never_dispatches_a_duplicate() {
        let directory = std::env::temp_dir().join(format!(
            "weline-localnet-idempotent-friend-request-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&directory).expect("create friend request fixture");
        let storage =
            Storage::open(&directory.join("localnet.sqlite3")).expect("open friend storage");
        let peer_id = "existing-friend-peer";
        let now = now_rfc3339();
        let peer = PeerSummary {
            peer_id: peer_id.to_string(),
            nickname: "Mac".to_string(),
            platform: Platform::Macos,
            online: true,
            protocol_version: 1,
            capabilities: Vec::new(),
            last_seen: now.clone(),
        };
        storage.upsert_peer(&peer).expect("persist online peer");
        let request = FriendRequest {
            request_id: uuid::Uuid::now_v7().to_string(),
            peer_id: peer_id.to_string(),
            nickname: peer.nickname.clone(),
            direction: Direction::Incoming,
            status: FriendRequestStatus::Pending,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        storage
            .put_friend_request(&request)
            .expect("persist accepted request fixture");
        storage
            .resolve_friend_request(
                &request.request_id,
                FriendRequestStatus::Accepted,
                Some(&Friend {
                    peer_id: peer_id.to_string(),
                    nickname: peer.nickname.clone(),
                    platform: peer.platform,
                    online: true,
                    added_at: now.clone(),
                    last_seen: now,
                }),
                &request.updated_at,
            )
            .expect("persist existing friendship");

        let resolved_online_peer = std::cell::Cell::new(false);
        let dispatched = std::cell::Cell::new(false);
        let result = send_friend_request_if_needed(
            &storage,
            peer_id.to_string(),
            || {
                resolved_online_peer.set(true);
                Err(AppError::InvalidInput("peer is offline".to_string()))
            },
            |_| {
                dispatched.set(true);
                Ok(())
            },
        )
        .expect("an existing friendship is an idempotent success even while offline");

        assert!(result.is_none());
        assert!(!resolved_online_peer.get());
        assert!(!dispatched.get());
        assert!(storage.is_friend(peer_id).expect("friendship remains"));

        drop(storage);
        fs::remove_dir_all(directory).expect("remove friend request fixture");
    }

    fn assert_manual_acceptance_unchanged(
        storage: &Storage,
        transfer_id: &str,
        destination: &Path,
    ) {
        let stored = storage
            .get_transfer(transfer_id)
            .expect("reload incoming transfer")
            .expect("incoming transfer remains");
        assert_eq!(stored.status, TransferStatus::AwaitingAcceptance);
        assert!(!stored.destination_reserved);
        assert!(stored.reservation_token.is_none());
        assert!(stored.local_path.is_none());
        assert!(stored.partial_path.is_none());
        assert!(!destination.exists());
    }

    fn legacy_acceptance_fixture(name: &str, file_size: u64) -> (PathBuf, Storage, TransferRecord) {
        let (directory, storage, mut transfer) = acceptance_fixture(name, file_size);
        transfer.transfer_protocol = TransferProtocol::LegacyV1 as u8;
        transfer.chunk_size = 0;
        transfer.chunk_count = 0;
        transfer.manifest_sha256 = None;
        storage
            .upsert_transfer(&transfer)
            .expect("persist legacy acceptance fixture");
        (directory, storage, transfer)
    }

    #[test]
    fn paused_v2_transfer_can_be_cancelled_locally_without_a_network_handle() {
        let directory = std::env::temp_dir().join(format!(
            "weline-localnet-local-cancel-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&directory).expect("create local cancellation fixture");
        let storage =
            Storage::open(&directory.join("localnet.sqlite3")).expect("open cancellation storage");
        let now = now_rfc3339();

        for (transfer_id, direction) in [
            ("paused-outgoing", Direction::Outgoing),
            ("paused-incoming", Direction::Incoming),
        ] {
            storage
                .upsert_transfer(&TransferRecord {
                    transfer_id: transfer_id.to_string(),
                    peer_id: "cancel-peer".to_string(),
                    direction,
                    kind: TransferKind::File,
                    file_name: "archive.bin".to_string(),
                    file_size: u64::from(TRANSFER_CHUNK_BYTES),
                    mime_type: "application/octet-stream".to_string(),
                    sha256: "0".repeat(64),
                    local_path: None,
                    destination_reserved: false,
                    reservation_token: None,
                    transfer_protocol: TransferProtocol::ResumableV2 as u8,
                    chunk_size: TRANSFER_CHUNK_BYTES,
                    chunk_count: 1,
                    manifest_sha256: Some("1".repeat(64)),
                    partial_path: None,
                    source_modified_ns: None,
                    send_claimed: false,
                    transferred_bytes: 0,
                    status: TransferStatus::Paused,
                    error: Some("network disconnected".to_string()),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                })
                .expect("persist paused cancellable transfer");

            let cancelled = cancel_transfer_locally(&storage, transfer_id)
                .expect("paused transfer cancellation is local-first");
            assert_eq!(cancelled.status, TransferStatus::Cancelled);
        }

        let pending = storage
            .list_pending_terminal_notifications("cancel-peer")
            .expect("list durable cancellation notifications");
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|notification| {
            notification.state == TransferStatus::Cancelled
                && matches!(
                    notification.transfer_id.as_str(),
                    "paused-outgoing" | "paused-incoming"
                )
        }));

        drop(storage);
        fs::remove_dir_all(directory).expect("remove local cancellation fixture");
    }

    #[test]
    fn paused_incoming_cancel_reports_local_success_when_cleanup_media_is_temporarily_offline() {
        let (directory, storage, mut transfer) =
            acceptance_fixture("cancel-offline-cleanup", u64::from(TRANSFER_CHUNK_BYTES));
        let media = directory.join("selected-media");
        let detached_media = directory.join("detached-media");
        fs::create_dir_all(&media).expect("create selected media");
        let destination = media.join("archive.bin");
        let token = "cancel-offline-cleanup-token";
        reserve_receive_path(&destination, &transfer.transfer_id, token)
            .expect("reserve incoming destination");
        transfer.local_path = Some(destination.to_string_lossy().into_owned());
        transfer.destination_reserved = true;
        transfer.reservation_token = Some(token.to_string());
        transfer.status = TransferStatus::Transferring;
        assert!(
            storage
                .try_accept_incoming_transfer(&transfer)
                .expect("accept incoming transfer")
        );
        assert!(
            storage
                .try_claim_incoming_transfer(&transfer.transfer_id, &transfer.peer_id)
                .expect("claim incoming transfer")
        );
        assert!(
            storage
                .try_pause_claimed_incoming_transfer(
                    &transfer.transfer_id,
                    &transfer.peer_id,
                    "network disconnected",
                )
                .expect("pause incoming transfer")
        );
        fs::rename(&media, &detached_media).expect("detach selected media");

        let cancelled = cancel_transfer_locally(&storage, &transfer.transfer_id)
            .expect("durably committed local cancel is successful while cleanup is deferred");
        assert_eq!(cancelled.status, TransferStatus::Cancelled);
        assert_eq!(
            storage
                .list_pending_terminal_notifications(&transfer.peer_id)
                .expect("load terminal outbox")
                .len(),
            1
        );

        drop(storage);
        fs::rename(&detached_media, &media).expect("restore selected media");
        let storage = Storage::open(&directory.join("localnet.sqlite3"))
            .expect("restart drains deferred owned cleanup");
        assert_eq!(
            storage
                .get_transfer(&transfer.transfer_id)
                .expect("reload cancelled transfer")
                .expect("cancelled transfer exists")
                .status,
            TransferStatus::Cancelled
        );

        drop(storage);
        fs::remove_dir_all(directory).expect("remove offline cleanup fixture");
    }

    #[test]
    fn upgraded_peer_prepares_a_v2_manifest_before_offering_a_source() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-v2-source-{}",
            uuid::Uuid::now_v7()
        ));
        fs::write(&fixture, b"abc").expect("write source fixture");
        let capabilities = vec![FILE_RESUME_V2_CAPABILITY.to_string()];

        let source = prepare_source(
            &fixture.to_string_lossy(),
            TransferKind::File,
            &capabilities,
        )
        .expect("prepare v2 source");

        assert_eq!(
            source.transfer_protocol,
            TransferProtocol::ResumableV2 as u8
        );
        assert_eq!(source.chunk_size, TRANSFER_CHUNK_BYTES);
        assert_eq!(source.chunk_count, 1);
        assert_eq!(source.manifest_sha256.as_deref().map(str::len), Some(64));
        assert!(source.source_modified_ns.is_some());
        assert_eq!(source.chunks.len(), 1);

        fs::remove_file(fixture).expect("remove source fixture");
    }

    #[test]
    fn manual_acceptance_never_returns_private_preflight_diagnostics() {
        let (directory, storage, transfer) = acceptance_fixture("private-diagnostics", GIB);
        let destination = directory.join("archive.bin");
        let private_diagnostic =
            r"unable to inspect E:\Private\客户\Archive: path unavailable (os error 3)";
        let mut decisions = 0_u8;

        let error = accept_incoming_transfer_with_preflight(
            &storage,
            &transfer,
            &destination,
            &|_, _, _| Err(AppError::Storage(private_diagnostic.to_string())),
            &mut |_| {
                decisions += 1;
                Ok(())
            },
        )
        .expect_err("private preflight diagnostics must block acceptance safely");

        assert_eq!(error.code(), "destination_preflight_error");
        assert!(error.to_string().contains("不可用"));
        assert!(!error.to_string().contains("E:\\Private"));
        assert!(!error.to_string().contains("os error"));
        assert_eq!(decisions, 0);
        assert_manual_acceptance_unchanged(&storage, &transfer.transfer_id, &destination);
        drop(storage);
        fs::remove_dir_all(directory).expect("remove private diagnostics fixture");
    }

    #[test]
    fn manual_acceptance_insufficient_space_has_no_reservation_or_transfer_decision() {
        let file_size = 5 * GIB;
        let (directory, storage, transfer) = acceptance_fixture("insufficient", file_size);
        let destination = directory.join("archive.bin");
        let decision_sent = Arc::new(AtomicBool::new(false));
        let decision_sent_for_dispatch = decision_sent.clone();
        let snapshot =
            VolumeSnapshot::known("NTFS", file_size + DESTINATION_RESERVE_BYTES - 1, None);

        let error = accept_incoming_transfer_with_preflight(
            &storage,
            &transfer,
            &destination,
            &|_, size, committed| validate_volume(&snapshot, size, committed),
            &mut |_| {
                decision_sent_for_dispatch.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .expect_err("one byte below the required margin must block acceptance");

        assert!(error.to_string().contains("可用空间不足"));
        assert!(!decision_sent.load(Ordering::SeqCst));
        assert_manual_acceptance_unchanged(&storage, &transfer.transfer_id, &destination);
        drop(storage);
        fs::remove_dir_all(directory).expect("remove insufficient-space fixture");
    }

    #[test]
    fn manual_acceptance_rejects_fat32_before_reservation_or_transfer_decision() {
        let file_size = 5 * GIB;
        let (directory, storage, transfer) = acceptance_fixture("fat32", file_size);
        let destination = directory.join("archive.bin");
        let decision_sent = Arc::new(AtomicBool::new(false));
        let decision_sent_for_dispatch = decision_sent.clone();
        let snapshot = VolumeSnapshot::known("FAT32", 10 * GIB, Some(4 * GIB - 1));

        let error = accept_incoming_transfer_with_preflight(
            &storage,
            &transfer,
            &destination,
            &|_, size, committed| validate_volume(&snapshot, size, committed),
            &mut |_| {
                decision_sent_for_dispatch.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .expect_err("FAT32 must reject a five GiB destination");

        assert!(error.to_string().contains("磁盘格式"));
        assert!(!decision_sent.load(Ordering::SeqCst));
        assert_manual_acceptance_unchanged(&storage, &transfer.transfer_id, &destination);
        drop(storage);
        fs::remove_dir_all(directory).expect("remove FAT32 fixture");
    }

    #[test]
    fn manual_acceptance_allows_exact_remaining_plus_64_mib() {
        let file_size = 5 * GIB;
        let (directory, storage, transfer) = acceptance_fixture("exact-margin", file_size);
        let destination = directory.join("archive.bin");
        let snapshot = VolumeSnapshot::known("NTFS", file_size + DESTINATION_RESERVE_BYTES, None);
        let mut decisions = 0_u8;

        let accepted = accept_incoming_transfer_with_preflight(
            &storage,
            &transfer,
            &destination,
            &|_, size, committed| validate_volume(&snapshot, size, committed),
            &mut |_| {
                decisions += 1;
                Ok(())
            },
        )
        .expect("exact capacity margin must permit acceptance");

        assert_eq!(decisions, 1);
        assert_eq!(accepted.status, TransferStatus::Transferring);
        assert!(accepted.destination_reserved);
        assert!(
            storage
                .get_transfer(&accepted.transfer_id)
                .expect("reload accepted transfer")
                .expect("accepted transfer remains")
                .partial_path
                .is_some()
        );
        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    &accepted.transfer_id,
                    &accepted.peer_id,
                    accepted.transfer_protocol,
                    "test cleanup",
                )
                .expect("clean accepted fixture")
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove exact-margin fixture");
    }

    #[test]
    fn manual_acceptance_missing_or_unwritable_destination_stays_awaiting() {
        for unavailable in ["missing", "unwritable"] {
            let file_size = GIB;
            let (directory, storage, transfer) = acceptance_fixture(unavailable, file_size);
            let destination = if unavailable == "missing" {
                directory.join("missing-volume").join("archive.bin")
            } else {
                directory.join("archive.bin")
            };
            let mut decisions = 0_u8;
            let error = accept_incoming_transfer_with_preflight(
                &storage,
                &transfer,
                &destination,
                &|target, size, committed| {
                    if unavailable == "missing" {
                        preflight_receive_directory(target, size, committed)
                    } else {
                        Err(AppError::Storage(format!(
                            "无法写入接收目录 {}；请选择可写入的目录后重试",
                            target.display()
                        )))
                    }
                },
                &mut |_| {
                    decisions += 1;
                    Ok(())
                },
            )
            .expect_err("an unavailable destination must block acceptance");

            assert!(error.to_string().contains("不可用"));
            assert_eq!(decisions, 0);
            assert_manual_acceptance_unchanged(&storage, &transfer.transfer_id, &destination);
            drop(storage);
            fs::remove_dir_all(directory).expect("remove unavailable fixture");
        }
    }

    #[test]
    fn manual_v1_acceptance_preflights_space_and_missing_directory_before_side_effects() {
        let file_size = GIB;
        for unavailable in ["insufficient", "missing"] {
            let (directory, storage, transfer) = legacy_acceptance_fixture(unavailable, file_size);
            let destination = if unavailable == "missing" {
                directory.join("missing-volume").join("archive.bin")
            } else {
                directory.join("archive.bin")
            };
            let mut decisions = 0_u8;
            let snapshot =
                VolumeSnapshot::known("NTFS", file_size + DESTINATION_RESERVE_BYTES - 1, None);
            let error = accept_incoming_transfer_with_preflight(
                &storage,
                &transfer,
                &destination,
                &|target, size, committed| {
                    if unavailable == "missing" {
                        preflight_receive_directory(target, size, committed)
                    } else {
                        validate_volume(&snapshot, size, committed)
                    }
                },
                &mut |_| {
                    decisions += 1;
                    Ok(())
                },
            )
            .expect_err("legacy acceptance must fail preflight before reservation");

            let expected = if unavailable == "missing" {
                "不可用"
            } else {
                "可用空间不足"
            };
            assert!(error.to_string().contains(expected));
            assert_eq!(decisions, 0);
            assert_manual_acceptance_unchanged(&storage, &transfer.transfer_id, &destination);
            assert!(!directory.join("missing-volume").exists());
            drop(storage);
            fs::remove_dir_all(directory).expect("remove legacy preflight fixture");
        }
    }

    #[test]
    fn manual_v1_acceptance_allows_exact_remaining_plus_64_mib() {
        let file_size = GIB;
        let (directory, storage, transfer) =
            legacy_acceptance_fixture("v1-exact-margin", file_size);
        let destination = directory.join("archive.bin");
        let snapshot = VolumeSnapshot::known("NTFS", file_size + DESTINATION_RESERVE_BYTES, None);
        let probes = std::cell::Cell::new(0_u8);

        let accepted = accept_incoming_transfer_with_preflight(
            &storage,
            &transfer,
            &destination,
            &|_, size, committed| {
                probes.set(probes.get() + 1);
                validate_volume(&snapshot, size, committed)
            },
            &mut |_| Ok(()),
        )
        .expect("legacy acceptance must permit the exact safety margin");

        assert_eq!(probes.get(), 1);
        assert_eq!(accepted.status, TransferStatus::Transferring);
        assert!(accepted.destination_reserved);
        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    &accepted.transfer_id,
                    &accepted.peer_id,
                    accepted.transfer_protocol,
                    "test cleanup",
                )
                .expect("clean accepted legacy fixture")
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove exact-margin legacy fixture");
    }

    #[test]
    fn manual_decision_enqueue_failure_reverts_v1_and_v2_to_awaiting_without_owned_artifacts() {
        for protocol in [
            TransferProtocol::LegacyV1 as u8,
            TransferProtocol::ResumableV2 as u8,
        ] {
            let (directory, storage, transfer) = if protocol == TransferProtocol::LegacyV1 as u8 {
                legacy_acceptance_fixture("v1-dispatch-failure", MIB)
            } else {
                acceptance_fixture("v2-dispatch-failure", MIB)
            };
            let destination = directory.join("archive.bin");

            let error = accept_incoming_transfer_with_preflight(
                &storage,
                &transfer,
                &destination,
                &|_, _, _| Ok(()),
                &mut |_| {
                    Err(AppError::Network(
                        "injected offline command queue failure".to_string(),
                    ))
                },
            )
            .expect_err("failed network enqueue must compensate acceptance");

            assert!(error.to_string().contains("offline command queue"));
            let stored = storage
                .get_transfer(&transfer.transfer_id)
                .expect("reload compensated manual transfer")
                .expect("compensated manual transfer exists");
            assert_eq!(stored.status, TransferStatus::AwaitingAcceptance);
            assert!(
                stored
                    .error
                    .as_deref()
                    .is_some_and(|value| value.contains("offline command queue"))
            );
            assert!(!stored.destination_reserved);
            assert!(stored.local_path.is_none());
            assert!(stored.reservation_token.is_none());
            assert!(stored.partial_path.is_none());
            assert!(
                fs::read_dir(&directory)
                    .expect("list manual compensation directory")
                    .filter_map(Result::ok)
                    .all(|entry| !entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".weline-localnet"))
            );
            drop(storage);
            fs::remove_dir_all(directory).expect("remove manual dispatch failure fixture");
        }
    }

    #[test]
    fn manual_acceptance_uses_a_time_ordered_reservation_token() {
        let (directory, storage, transfer) = acceptance_fixture("v7-reservation-token", MIB);
        let destination = directory.join("archive.bin");

        let accepted = accept_incoming_transfer_with_preflight(
            &storage,
            &transfer,
            &destination,
            &|_, _, _| Ok(()),
            &mut |_| Ok(()),
        )
        .expect("manual acceptance succeeds");

        let token = uuid::Uuid::parse_str(
            accepted
                .reservation_token
                .as_deref()
                .expect("accepted transfer has reservation token"),
        )
        .expect("reservation token is a UUID");
        assert_eq!(token.get_version_num(), 7);

        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    &accepted.transfer_id,
                    &accepted.peer_id,
                    accepted.transfer_protocol,
                    "test cleanup",
                )
                .expect("clean accepted transfer")
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove v7 reservation token fixture");
    }

    #[test]
    fn manual_dispatch_error_remains_primary_when_rollback_also_fails() {
        let (directory, storage, transfer) =
            acceptance_fixture("dispatch-and-rollback-failure", MIB);
        let database = directory.join("localnet.sqlite3");
        let destination = directory.join("archive.bin");
        let mut write_blocker = None;

        let error = accept_incoming_transfer_with_preflight(
            &storage,
            &transfer,
            &destination,
            &|_, _, _| Ok(()),
            &mut |_| {
                let connection = rusqlite::Connection::open(&database)
                    .expect("open independent rollback blocker");
                connection
                    .execute_batch("BEGIN IMMEDIATE")
                    .expect("hold database writer lock across rollback");
                write_blocker = Some(connection);
                Err(AppError::Network(
                    "injected command dispatch failure".to_string(),
                ))
            },
        )
        .expect_err("dispatch failure is returned even when rollback is blocked");

        assert_eq!(error.code(), "network_error");
        assert_eq!(error.to_string(), "injected command dispatch failure");

        drop(write_blocker);
        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    &transfer.transfer_id,
                    &transfer.peer_id,
                    transfer.transfer_protocol,
                    "test cleanup",
                )
                .expect("clean transfer after releasing rollback blocker")
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove double failure fixture");
    }

    #[test]
    fn disabling_auto_receive_accepts_an_unavailable_absolute_directory() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-disabled-directory-{}",
            uuid::Uuid::now_v7()
        ));
        let default_directory = fixture.join("default");
        let unavailable = fixture.join("unplugged-drive");

        let prepared = prepare_receive_directory(
            unavailable.to_string_lossy().as_ref(),
            false,
            &default_directory,
        )
        .expect("disabling must not probe the unavailable directory");

        assert_eq!(prepared, unavailable);
        assert!(!prepared.exists());
        let _ = fs::remove_dir_all(fixture);
    }

    #[test]
    fn manual_v2_acceptance_never_recreates_a_missing_selected_parent() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-manual-v2-missing-parent-{}",
            uuid::Uuid::now_v7()
        ));
        let destination = fixture.join("unplugged-volume").join("report.bin");

        let error = reserve_manual_receive_destination(&destination, "transfer-one", "token-one")
            .expect_err("missing selected parent must reject v2 acceptance");

        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert!(!fixture.exists());
    }

    #[test]
    fn unchanged_enabled_preferences_do_not_probe_an_unavailable_directory() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-unchanged-directory-{}",
            uuid::Uuid::now_v7()
        ));
        let default_directory = fixture.join("default");
        let unavailable = fixture.join("unplugged-drive");
        let previous = TransferPreferences {
            auto_receive_files: true,
            receive_directory: unavailable.to_string_lossy().into_owned(),
        };

        let prepared = prepare_transfer_preferences(
            true,
            unavailable.to_string_lossy().as_ref(),
            &previous,
            &default_directory,
        )
        .expect("unchanged preferences must not block nickname updates");

        assert_eq!(prepared.receive_directory, previous.receive_directory);
        assert!(prepared.auto_receive_files);
        assert!(!unavailable.exists());
        let _ = fs::remove_dir_all(fixture);
    }
}
