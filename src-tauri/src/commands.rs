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
        LocalProfile, MAX_FILE_BYTES, MessageKind, MessageStatus, PROTOCOL_VERSION, Platform,
        TransferKind, TransferRecord, TransferStatus, now_rfc3339, validate_nickname,
        validate_text,
    },
    error::AppError,
    network::NetworkCommand,
    state::AppState,
};

#[tauri::command]
pub fn bootstrap(state: State<'_, AppState>) -> Result<BootstrapSnapshot, AppError> {
    state.storage.snapshot(state.local_profile()?)
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
    state
        .network()?
        .try_send(NetworkCommand::SendFriendRequest(request.clone()))?;
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
    require_online_peer(&peer_id, &state)?;
    if !state.storage.is_friend(&peer_id)? {
        return Err(AppError::NotFriend);
    }
    let kind = TransferKind::from_str(&kind)?;
    let source = tauri::async_runtime::spawn_blocking(move || prepare_source(&path, kind))
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
    state.storage.upsert_transfer(&transfer)?;
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
        transfer.local_path = Some(path.to_string_lossy().into_owned());
        transfer.status = TransferStatus::Transferring;
        transfer.error = None;
    } else {
        transfer.status = TransferStatus::Cancelled;
        transfer.error = Some("你拒绝了这次传输".to_string());
    }
    transfer.updated_at = now_rfc3339();
    state.storage.upsert_transfer(&transfer)?;
    state.network()?.try_send(NetworkCommand::ResolveTransfer {
        peer_id: transfer.peer_id.clone(),
        transfer_id,
        accepted,
    })?;
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
    transfer.status = TransferStatus::Cancelled;
    transfer.error = Some("你取消了传输".to_string());
    transfer.updated_at = now_rfc3339();
    state.storage.upsert_transfer(&transfer)?;
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
}

fn prepare_source(path: &str, kind: TransferKind) -> Result<PreparedSource, AppError> {
    let path = std::fs::canonicalize(path)?;
    let metadata = std::fs::metadata(&path)?;
    if !metadata.is_file() {
        return Err(AppError::InvalidInput("请选择一个普通文件".to_string()));
    }
    if metadata.len() > MAX_FILE_BYTES {
        return Err(AppError::InvalidInput("单个文件不能超过 2 GiB".to_string()));
    }
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
    let mut reader = BufReader::new(File::open(&path)?);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(PreparedSource {
        path,
        file_name,
        file_size: metadata.len(),
        mime_type,
        sha256: hex::encode(hasher.finalize()),
    })
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
