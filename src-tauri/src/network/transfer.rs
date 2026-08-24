use std::{
    cmp,
    path::PathBuf,
    time::{Duration, Instant},
};

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
    receive_paths::{
        commit_without_overwrite, finalize_reserved_receive, remove_owned_reservation,
    },
    storage::Storage,
};

const BUFFER_SIZE: usize = 64 * 1024;
const PROGRESS_BYTES: u64 = 1024 * 1024;
const MAX_HEADER_BYTES: usize = 8 * 1024;
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const INCOMING_START_TIMEOUT: Duration = Duration::from_secs(35);

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
            if let Err(persist_error) =
                fail_transfer(&storage, &app_handle, transfer, error.to_string())
            {
                tracing::warn!(%persist_error, "failed to persist outgoing transfer failure");
            }
        }
    });
}

pub fn spawn_incoming_start_timeout(transfer_id: String, storage: Storage, app_handle: AppHandle) {
    tauri::async_runtime::spawn(async move {
        tokio::time::sleep(INCOMING_START_TIMEOUT).await;
        let result = fail_pending_incoming_decision(
            &transfer_id,
            &storage,
            &app_handle,
            "对方未在限定时间内开始传输，请重新确认接收".to_string(),
        );
        if let Err(error) = result {
            tracing::warn!(%transfer_id, %error, "failed to expire unstarted incoming transfer");
        }
    });
}

pub fn fail_pending_incoming_decision(
    transfer_id: &str,
    storage: &Storage,
    app_handle: &AppHandle,
    message: String,
) -> Result<(), AppError> {
    let Some(candidate) = storage.get_transfer(transfer_id)? else {
        return Ok(());
    };
    if candidate.direction != Direction::Incoming
        || candidate.status != TransferStatus::Transferring
        || !storage.try_claim_incoming_transfer(transfer_id, &candidate.peer_id)?
    {
        return Ok(());
    }
    let result = storage
        .get_transfer(transfer_id)?
        .ok_or_else(|| AppError::Storage("接收文件记录在清理期间消失".to_string()))
        .and_then(|transfer| return_incoming_to_manual(storage, app_handle, transfer, message));
    if result.is_ok() {
        storage.release_incoming_transfer_claim(transfer_id)?;
    }
    result
}

fn return_incoming_to_manual(
    storage: &Storage,
    app_handle: &AppHandle,
    mut transfer: TransferRecord,
    message: String,
) -> Result<(), AppError> {
    let reservation_released = cleanup_reservation(&mut transfer);
    transfer.status = if reservation_released {
        transfer.local_path = None;
        TransferStatus::AwaitingAcceptance
    } else {
        TransferStatus::Failed
    };
    transfer.transferred_bytes = 0;
    transfer.error = Some(message.clone());
    transfer.updated_at = now_rfc3339();
    storage.upsert_transfer(&transfer)?;
    emit_event(
        app_handle,
        &NetworkEvent::TransferUpdated {
            transfer: transfer.clone(),
        },
    );
    emit_event(
        app_handle,
        &NetworkEvent::NetworkError {
            code: "transfer.receive_not_started".to_string(),
            message,
        },
    );
    Ok(())
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
    tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.read_exact(&mut header_size))
        .await
        .map_err(|_| AppError::Network("文件传输连接等待超时".to_string()))??;
    let header_size = u32::from_be_bytes(header_size) as usize;
    if header_size == 0 || header_size > MAX_HEADER_BYTES {
        return Err(AppError::InvalidInput("文件传输头无效".to_string()));
    }
    let mut header = vec![0_u8; header_size];
    tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.read_exact(&mut header))
        .await
        .map_err(|_| AppError::Network("文件传输头等待超时".to_string()))??;
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
    if !storage.try_claim_incoming_transfer(&transfer.transfer_id, &transfer.peer_id)? {
        return Err(AppError::Permission(
            "该文件传输已有接收连接，重复连接已拒绝".to_string(),
        ));
    }

    let result = receive_body(&mut stream, &storage, &app_handle, &transfer).await;
    if let Err(error) = result {
        let _ = stream.write_all(&[0]).await;
        let _ = stream.close().await;
        let failure_persisted =
            fail_transfer(&storage, &app_handle, transfer.clone(), error.to_string()).is_ok();
        if failure_persisted {
            if let Err(release_error) =
                storage.release_incoming_transfer_claim(&transfer.transfer_id)
            {
                tracing::warn!(
                    transfer_id = %transfer.transfer_id,
                    %release_error,
                    "failed to release failed incoming transfer claim"
                );
            }
        }
        return Err(error);
    }
    if let Err(error) = storage.release_incoming_transfer_claim(&transfer.transfer_id) {
        tracing::warn!(
            transfer_id = %transfer.transfer_id,
            %error,
            "failed to release completed incoming transfer claim"
        );
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
    if !transfer.destination_reserved && tokio::fs::try_exists(&destination).await? {
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
            let read =
                tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.read(&mut buffer[..desired]))
                    .await
                    .map_err(|_| AppError::Network("文件传输等待数据超时".to_string()))??;
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
        let partial_for_commit = partial.clone();
        let destination_for_commit = destination.clone();
        let file_name = destination
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or(&transfer.file_name)
            .to_string();
        let transfer_id = transfer.transfer_id.clone();
        let destination_reserved = transfer.destination_reserved;
        let reservation_token = transfer.reservation_token.clone();
        let completed_path = tokio::task::spawn_blocking(move || {
            if destination_reserved {
                let token = reservation_token.as_deref().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "接收文件占位凭据缺失")
                })?;
                finalize_reserved_receive(
                    &partial_for_commit,
                    &destination_for_commit,
                    &file_name,
                    &transfer_id,
                    token,
                )
            } else {
                commit_without_overwrite(&partial_for_commit, &destination_for_commit)?;
                Ok(crate::receive_paths::FinalizedReceive {
                    path: destination_for_commit,
                    reservation_released: true,
                })
            }
        })
        .await
        .map_err(|error| AppError::Io(std::io::Error::other(error.to_string())))??;
        let mut completed = transfer.clone();
        completed.local_path = Some(completed_path.path.to_string_lossy().into_owned());
        completed.destination_reserved = !completed_path.reservation_released;
        if completed_path.reservation_released {
            completed.reservation_token = None;
        }
        complete_incoming(storage, app_handle, completed)
    }
    .await;

    if receive_result.is_err() {
        let _ = tokio::fs::remove_file(&partial).await;
        if transfer.destination_reserved {
            if let Some(token) = transfer.reservation_token.as_deref() {
                let _ = remove_owned_reservation(&destination, &transfer.transfer_id, token);
            }
        }
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
) -> Result<(), AppError> {
    cleanup_reservation(&mut transfer);
    transfer.status = TransferStatus::Failed;
    transfer.error = Some(message.clone());
    transfer.updated_at = now_rfc3339();
    storage.upsert_transfer(&transfer)?;
    let _ =
        storage.update_message_status(&transfer.transfer_id, MessageStatus::Failed, Some(&message));
    emit_event(app_handle, &NetworkEvent::TransferUpdated { transfer });
    Ok(())
}

fn cleanup_reservation(transfer: &mut TransferRecord) -> bool {
    if !transfer.destination_reserved {
        return true;
    }
    let (Some(path), Some(token)) = (
        transfer.local_path.as_deref(),
        transfer.reservation_token.as_deref(),
    ) else {
        tracing::warn!(
            transfer_id = %transfer.transfer_id,
            "receive reservation metadata is incomplete"
        );
        return false;
    };
    match remove_owned_reservation(std::path::Path::new(path), &transfer.transfer_id, token) {
        Ok(_) => {
            transfer.destination_reserved = false;
            transfer.reservation_token = None;
            true
        }
        Err(error) => {
            tracing::warn!(
                transfer_id = %transfer.transfer_id,
                %error,
                "failed to clean receive reservation"
            );
            false
        }
    }
}
