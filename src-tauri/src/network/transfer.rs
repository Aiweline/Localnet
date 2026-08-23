use std::{cmp, path::PathBuf, time::Instant};

use futures::{AsyncReadExt as _, AsyncWriteExt as _, StreamExt as _};
use libp2p::{PeerId, StreamProtocol};
use sha2::{Digest, Sha256};
use tauri::AppHandle;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::runtime::{NetworkEvent, emit_event};
use crate::{
    domain::{
        ChatMessage, Direction, MessageKind, MessageStatus, TransferRecord, TransferStatus,
        now_rfc3339,
    },
    error::AppError,
    protocol::{FILE_PROTOCOL, TransferStreamHeader},
    storage::Storage,
};

const BUFFER_SIZE: usize = 64 * 1024;
const PROGRESS_BYTES: u64 = 1024 * 1024;
const MAX_HEADER_BYTES: usize = 8 * 1024;

pub fn spawn_incoming_transfers(
    mut incoming: libp2p_stream::IncomingStreams,
    storage: Storage,
    app_handle: AppHandle,
) {
    tauri::async_runtime::spawn(async move {
        while let Some((peer_id, stream)) = incoming.next().await {
            let storage = storage.clone();
            let app_handle = app_handle.clone();
            tauri::async_runtime::spawn(async move {
                if let Err(error) = receive_transfer(peer_id, stream, storage, app_handle).await {
                    tracing::warn!(peer_id = %peer_id, %error, "incoming transfer failed");
                }
            });
        }
    });
}

pub fn spawn_outgoing_transfer(
    mut control: libp2p_stream::Control,
    peer_id: PeerId,
    transfer: TransferRecord,
    storage: Storage,
    app_handle: AppHandle,
) {
    tauri::async_runtime::spawn(async move {
        let result = send_transfer(&mut control, peer_id, &transfer, &storage, &app_handle).await;
        if let Err(error) = result {
            fail_transfer(&storage, &app_handle, transfer, error.to_string());
        }
    });
}

async fn send_transfer(
    control: &mut libp2p_stream::Control,
    peer_id: PeerId,
    transfer: &TransferRecord,
    storage: &Storage,
    app_handle: &AppHandle,
) -> Result<(), AppError> {
    let source_path = transfer
        .local_path
        .as_deref()
        .map(PathBuf::from)
        .ok_or_else(|| AppError::Io(std::io::Error::other("发送文件路径缺失")))?;
    let mut file = tokio::fs::File::open(&source_path).await?;
    let mut stream = control
        .open_stream(peer_id, StreamProtocol::new(FILE_PROTOCOL))
        .await
        .map_err(|error| AppError::Network(format!("无法建立文件传输流：{error}")))?;
    let header = serde_json::to_vec(&TransferStreamHeader {
        transfer_id: transfer.transfer_id.clone(),
    })
    .map_err(|error| AppError::Network(format!("无法编码文件传输头：{error}")))?;
    let header_size =
        u32::try_from(header.len()).map_err(|_| AppError::Network("文件传输头过大".to_string()))?;
    stream.write_all(&header_size.to_be_bytes()).await?;
    stream.write_all(&header).await?;

    let mut buffer = vec![0_u8; BUFFER_SIZE];
    let mut transferred = 0_u64;
    let mut last_emitted = 0_u64;
    let mut last_emit_time = Instant::now();
    while transferred < transfer.file_size {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "发送文件在传输期间发生变化",
            )));
        }
        stream.write_all(&buffer[..read]).await?;
        transferred += read as u64;
        if transferred > transfer.file_size {
            return Err(AppError::InvalidInput(
                "发送文件大小与开始传输时不一致".to_string(),
            ));
        }
        if transferred.saturating_sub(last_emitted) >= PROGRESS_BYTES
            || last_emit_time.elapsed().as_millis() >= 400
        {
            update_progress(storage, app_handle, transfer, transferred)?;
            last_emitted = transferred;
            last_emit_time = Instant::now();
        }
    }
    stream.flush().await?;
    let mut acknowledgement = [0_u8; 1];
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        stream.read_exact(&mut acknowledgement),
    )
    .await
    .map_err(|_| AppError::Network("等待接收方校验文件超时".to_string()))??;
    if acknowledgement[0] != 1 {
        return Err(AppError::IntegrityFailure);
    }
    stream.close().await?;
    complete_outgoing(storage, app_handle, transfer.clone())
}

async fn receive_transfer(
    peer_id: PeerId,
    mut stream: libp2p::swarm::Stream,
    storage: Storage,
    app_handle: AppHandle,
) -> Result<(), AppError> {
    let mut header_size = [0_u8; 4];
    stream.read_exact(&mut header_size).await?;
    let header_size = u32::from_be_bytes(header_size) as usize;
    if header_size == 0 || header_size > MAX_HEADER_BYTES {
        return Err(AppError::InvalidInput("文件传输头无效".to_string()));
    }
    let mut header = vec![0_u8; header_size];
    stream.read_exact(&mut header).await?;
    let header: TransferStreamHeader = serde_json::from_slice(&header)
        .map_err(|error| AppError::Network(format!("无法读取文件传输头：{error}")))?;
    let transfer = storage
        .get_transfer(&header.transfer_id)?
        .ok_or_else(|| AppError::Permission("未找到已接受的文件传输".to_string()))?;
    if transfer.peer_id != peer_id.to_string()
        || transfer.direction != Direction::Incoming
        || transfer.status != TransferStatus::Transferring
        || !storage.is_friend(&transfer.peer_id)?
    {
        return Err(AppError::Permission(
            "该文件传输未获授权，连接已拒绝".to_string(),
        ));
    }

    let result = receive_body(&mut stream, &storage, &app_handle, &transfer).await;
    if let Err(error) = result {
        let _ = stream.write_all(&[0]).await;
        let _ = stream.close().await;
        fail_transfer(&storage, &app_handle, transfer, error.to_string());
        return Err(error);
    }
    stream.write_all(&[1]).await?;
    stream.close().await?;
    Ok(())
}

async fn receive_body(
    stream: &mut libp2p::swarm::Stream,
    storage: &Storage,
    app_handle: &AppHandle,
    transfer: &TransferRecord,
) -> Result<(), AppError> {
    let destination = transfer
        .local_path
        .as_deref()
        .map(PathBuf::from)
        .ok_or_else(|| AppError::Permission("接收文件尚未选择保存位置".to_string()))?;
    let parent = destination
        .parent()
        .ok_or_else(|| AppError::InvalidInput("接收文件保存位置无效".to_string()))?;
    tokio::fs::create_dir_all(parent).await?;
    if tokio::fs::try_exists(&destination).await? {
        return Err(AppError::InvalidInput(
            "保存位置已经存在同名文件，请重新选择".to_string(),
        ));
    }
    let partial = parent.join(format!(
        ".localnet-{}.part-{}",
        transfer.transfer_id,
        uuid::Uuid::now_v7()
    ));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial)
        .await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; BUFFER_SIZE];
    let mut transferred = 0_u64;
    let mut last_emitted = 0_u64;
    let mut last_emit_time = Instant::now();

    let receive_result = async {
        while transferred < transfer.file_size {
            let remaining = transfer.file_size - transferred;
            let desired = cmp::min(remaining, BUFFER_SIZE as u64) as usize;
            let read = stream.read(&mut buffer[..desired]).await?;
            if read == 0 {
                return Err(AppError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "文件传输提前中断",
                )));
            }
            file.write_all(&buffer[..read]).await?;
            hasher.update(&buffer[..read]);
            transferred += read as u64;
            if transferred.saturating_sub(last_emitted) >= PROGRESS_BYTES
                || last_emit_time.elapsed().as_millis() >= 400
            {
                update_progress(storage, app_handle, transfer, transferred)?;
                last_emitted = transferred;
                last_emit_time = Instant::now();
            }
        }
        file.flush().await?;
        drop(file);
        let digest = hex::encode(hasher.finalize());
        if !digest.eq_ignore_ascii_case(&transfer.sha256) {
            return Err(AppError::IntegrityFailure);
        }
        tokio::fs::rename(&partial, &destination).await?;
        complete_incoming(storage, app_handle, transfer.clone())
    }
    .await;

    if receive_result.is_err() {
        let _ = tokio::fs::remove_file(&partial).await;
    }
    receive_result
}

fn update_progress(
    storage: &Storage,
    app_handle: &AppHandle,
    original: &TransferRecord,
    transferred_bytes: u64,
) -> Result<(), AppError> {
    let mut transfer = original.clone();
    transfer.transferred_bytes = transferred_bytes.min(transfer.file_size);
    transfer.updated_at = now_rfc3339();
    storage.upsert_transfer(&transfer)?;
    emit_event(app_handle, &NetworkEvent::TransferUpdated { transfer });
    Ok(())
}

fn complete_outgoing(
    storage: &Storage,
    app_handle: &AppHandle,
    mut transfer: TransferRecord,
) -> Result<(), AppError> {
    transfer.transferred_bytes = transfer.file_size;
    transfer.status = TransferStatus::Completed;
    transfer.error = None;
    transfer.updated_at = now_rfc3339();
    storage.upsert_transfer(&transfer)?;
    storage.update_message_status(&transfer.transfer_id, MessageStatus::Delivered, None)?;
    emit_event(
        app_handle,
        &NetworkEvent::TransferUpdated {
            transfer: transfer.clone(),
        },
    );
    emit_event(
        app_handle,
        &NetworkEvent::MessageStatusChanged {
            message_id: transfer.transfer_id,
            status: MessageStatus::Delivered,
            error: None,
        },
    );
    Ok(())
}

fn complete_incoming(
    storage: &Storage,
    app_handle: &AppHandle,
    mut transfer: TransferRecord,
) -> Result<(), AppError> {
    transfer.transferred_bytes = transfer.file_size;
    transfer.status = TransferStatus::Completed;
    transfer.error = None;
    transfer.updated_at = now_rfc3339();
    storage.upsert_transfer(&transfer)?;
    let message = ChatMessage {
        message_id: transfer.transfer_id.clone(),
        peer_id: transfer.peer_id.clone(),
        direction: Direction::Incoming,
        kind: match transfer.kind {
            crate::domain::TransferKind::Image => MessageKind::Image,
            crate::domain::TransferKind::File => MessageKind::File,
        },
        body: None,
        local_path: transfer.local_path.clone(),
        file_name: Some(transfer.file_name.clone()),
        file_size: Some(transfer.file_size),
        status: MessageStatus::Delivered,
        error: None,
        created_at: transfer.created_at.clone(),
    };
    storage.insert_message(&message)?;
    emit_event(app_handle, &NetworkEvent::TransferUpdated { transfer });
    emit_event(app_handle, &NetworkEvent::MessageReceived { message });
    Ok(())
}

fn fail_transfer(
    storage: &Storage,
    app_handle: &AppHandle,
    mut transfer: TransferRecord,
    message: String,
) {
    transfer.status = TransferStatus::Failed;
    transfer.error = Some(message.clone());
    transfer.updated_at = now_rfc3339();
    if let Err(error) = storage.upsert_transfer(&transfer) {
        tracing::warn!(%error, "failed to persist transfer failure");
    }
    let _ =
        storage.update_message_status(&transfer.transfer_id, MessageStatus::Failed, Some(&message));
    emit_event(app_handle, &NetworkEvent::TransferUpdated { transfer });
}
