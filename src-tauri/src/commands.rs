use std::{
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    str::FromStr,
};

use base64::Engine as _;
use libp2p::PeerId;
use sha2::{Digest, Sha256};
use tauri::{AppHandle, State};

use crate::{
    domain::{
        BootstrapSnapshot, ChatMessage, Direction, Friend, FriendRequest, FriendRequestStatus,
        LocalProfile, MessageKind, MessageStatus, PROTOCOL_VERSION, Platform, PresenceSnapshot,
        TransferKind, TransferPreferences, TransferRecord, TransferStatus, now_rfc3339,
        validate_nickname, validate_text,
    },
    error::AppError,
    network::NetworkCommand,
    receive_paths::{ensure_writable_directory, remove_owned_reservation, reserve_receive_path},
    state::AppState,
    transfer_manifest::{TransferChunk, build_manifest_from_snapshot, capture_source_snapshot},
    transfer_policy::{TRANSFER_CHUNK_BYTES, TransferProtocol, select_transfer_protocol},
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
) -> Result<FriendRequest, AppError> {
    validate_peer_id(&peer_id, state.identity.peer_id_string().as_str())?;
    let peer = require_online_peer(&peer_id, &state)?;
    if state.storage.is_friend(&peer_id)? {
        return Err(AppError::InvalidInput("双方已经是好友".to_string()));
    }
    if state
        .storage
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
    state.storage.put_friend_request(&request)?;
    if let Err(error) = state
        .network()?
        .try_send(NetworkCommand::SendFriendRequest(request.clone()))
    {
        state
            .storage
            .remove_pending_outgoing_friend_request(&request.request_id)?;
        return Err(error);
    }
    Ok(request)
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
    let peer = require_online_peer(&request.peer_id, &state)?;
    let status = if accepted {
        FriendRequestStatus::Accepted
    } else {
        FriendRequestStatus::Rejected
    };
    let now = now_rfc3339();
    let friend = accepted.then(|| Friend {
        peer_id: request.peer_id.clone(),
        nickname: request.nickname.clone(),
        platform: peer.platform,
        online: true,
        added_at: now.clone(),
        last_seen: now.clone(),
    });
    state
        .storage
        .resolve_friend_request(&request_id, status, friend.as_ref(), &now)?;
    state
        .network()?
        .try_send(NetworkCommand::ResolveFriendRequest {
            peer_id: request.peer_id.clone(),
            request_id,
            accepted,
        })?;
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
pub fn resolve_transfer(
    transfer_id: String,
    accepted: bool,
    save_path: Option<String>,
    app_handle: AppHandle,
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
        let path = save_path
            .filter(|path| !path.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| AppError::InvalidInput("请选择文件保存位置".to_string()))?;
        if !path.is_absolute() {
            return Err(AppError::InvalidInput(
                "文件保存位置必须是绝对路径".to_string(),
            ));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let reservation_token = uuid::Uuid::new_v4().to_string();
        reserve_receive_path(&path, &transfer.transfer_id, &reservation_token).map_err(
            |error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    AppError::InvalidInput("保存位置已经存在同名文件，请重新选择".to_string())
                } else {
                    AppError::Permission(format!("无法占用文件保存位置，请重新选择：{error}"))
                }
            },
        )?;
        transfer.local_path = Some(path.to_string_lossy().into_owned());
        transfer.destination_reserved = true;
        transfer.reservation_token = Some(reservation_token);
        transfer.status = TransferStatus::Transferring;
        transfer.error = None;
        transfer.updated_at = now_rfc3339();
        match state.storage.try_accept_incoming_transfer(&transfer) {
            Ok(true) => {}
            Ok(false) => {
                if let (Some(path), Some(token)) = (
                    transfer.local_path.as_deref(),
                    transfer.reservation_token.as_deref(),
                ) {
                    let _ = remove_owned_reservation(Path::new(path), &transfer.transfer_id, token);
                }
                return Err(AppError::InvalidInput(
                    "这次文件传输已经处理，请刷新后查看".to_string(),
                ));
            }
            Err(error) => {
                if let (Some(path), Some(token)) = (
                    transfer.local_path.as_deref(),
                    transfer.reservation_token.as_deref(),
                ) {
                    let _ = remove_owned_reservation(Path::new(path), &transfer.transfer_id, token);
                }
                return Err(error);
            }
        }
    } else {
        if !state.storage.try_cancel_unclaimed_incoming_transfer(
            &transfer.transfer_id,
            &transfer.peer_id,
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
    if accepted {
        crate::network::spawn_incoming_start_timeout(
            transfer.transfer_id.clone(),
            state.storage.clone(),
            app_handle,
        );
    }
    Ok(transfer)
}

#[tauri::command]
pub fn cancel_transfer(
    transfer_id: String,
    state: State<'_, AppState>,
) -> Result<TransferRecord, AppError> {
    let mut transfer = state
        .storage
        .get_transfer(&transfer_id)?
        .ok_or_else(|| AppError::InvalidInput("找不到这次文件传输".to_string()))?;
    if transfer.direction != Direction::Outgoing
        || transfer.status != TransferStatus::AwaitingAcceptance
    {
        return Err(AppError::InvalidInput("当前传输不能取消".to_string()));
    }
    if !state.storage.try_transition_outgoing_awaiting(
        &transfer.transfer_id,
        &transfer.peer_id,
        TransferStatus::Cancelled,
        Some("你取消了传输"),
    )? {
        return Err(AppError::InvalidInput("当前传输不能取消".to_string()));
    }
    transfer = state
        .storage
        .get_transfer(&transfer.transfer_id)?
        .ok_or_else(|| AppError::InvalidInput("找不到这次文件传输".to_string()))?;
    state.storage.update_message_status(
        &transfer.transfer_id,
        MessageStatus::Failed,
        transfer.error.as_deref(),
    )?;
    state.network()?.try_send(NetworkCommand::CancelTransfer {
        peer_id: transfer.peer_id.clone(),
        transfer_id,
    })?;
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
    use std::fs;

    use super::{prepare_receive_directory, prepare_source, prepare_transfer_preferences};
    use crate::{
        domain::{TransferKind, TransferPreferences},
        transfer_policy::{FILE_RESUME_V2_CAPABILITY, TRANSFER_CHUNK_BYTES, TransferProtocol},
    };

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
