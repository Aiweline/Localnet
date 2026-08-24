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

use super::{
    resumable_transfer::{
        is_recoverable_receive_error, is_recoverable_send_error, open_owned_resumable_partial,
        receive_acknowledged_chunks, send_acknowledged_chunks, verify_committed_manifest,
    },
    runtime::{NetworkEvent, emit_event},
};
use crate::{
    domain::{
        ChatMessage, Direction, MessageKind, MessageStatus, TransferRecord, TransferStatus,
        now_rfc3339,
    },
    error::AppError,
    protocol::{FILE_PROTOCOL, FILE_PROTOCOL_V2, TransferStreamHeader},
    receive_paths::{
        commit_without_overwrite, finalize_reserved_receive, finalize_reserved_receive_durable,
        remove_owned_reservation,
    },
    storage::Storage,
    transfer_policy::{TRANSFER_CHUNK_BYTES, TransferProtocol},
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

// Task 7 registers this entry point on the separately accepted v2 protocol stream.
#[allow(dead_code)]
pub fn spawn_incoming_resumable_transfers(
    mut incoming: libp2p_stream::IncomingStreams,
    storage: Storage,
    app_handle: AppHandle,
) {
    tauri::async_runtime::spawn(async move {
        while let Some((peer_id, stream)) = incoming.next().await {
            let storage = storage.clone();
            let app_handle = app_handle.clone();
            tauri::async_runtime::spawn(async move {
                if let Err(error) =
                    receive_resumable_transfer(peer_id, stream, storage, app_handle).await
                {
                    tracing::warn!(peer_id = %peer_id, %error, "incoming resumable transfer failed");
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
            if transfer.transfer_protocol == TransferProtocol::LegacyV1 as u8 {
                if let Err(persist_error) =
                    fail_transfer(&storage, &app_handle, transfer, error.to_string())
                {
                    tracing::warn!(%persist_error, "failed to persist outgoing transfer failure");
                }
            } else {
                tracing::warn!(transfer_id = %transfer.transfer_id, %error, "resumable outgoing transfer stopped");
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
    if candidate.transfer_protocol == TransferProtocol::ResumableV2 as u8 {
        if storage.try_pause_claimed_incoming_transfer(transfer_id, &candidate.peer_id, &message)? {
            if let Some(updated) = storage.get_transfer(transfer_id)? {
                emit_event(
                    app_handle,
                    &NetworkEvent::TransferUpdated { transfer: updated },
                );
            }
            emit_event(
                app_handle,
                &NetworkEvent::NetworkError {
                    code: "transfer.receive_not_started".to_string(),
                    message,
                },
            );
        }
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
    match transfer.transfer_protocol {
        value if value == TransferProtocol::LegacyV1 as u8 => {
            send_legacy_transfer(control, peer_id, transfer, storage, app_handle).await
        }
        value if value == TransferProtocol::ResumableV2 as u8 => {
            send_resumable_transfer(control, peer_id, transfer, storage, app_handle).await
        }
        _ => Err(AppError::InvalidInput("文件传输协议版本无效".to_string())),
    }
}

async fn send_legacy_transfer(
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
        version: 1,
        start_offset: 0,
        chunk_size: 0,
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

async fn send_resumable_transfer(
    control: &mut libp2p_stream::Control,
    peer_id: PeerId,
    transfer: &TransferRecord,
    storage: &Storage,
    app_handle: &AppHandle,
) -> Result<(), AppError> {
    let Some(transfer) =
        claim_resumable_outgoing(storage, &transfer.transfer_id, &transfer.peer_id)?
    else {
        return Ok(());
    };
    let result =
        send_claimed_resumable_transfer(control, peer_id, &transfer, storage, app_handle).await;
    if let Err(error) = &result {
        if persist_claimed_outgoing_error(storage, &transfer, error)? {
            if let Some(updated) = storage.get_transfer(&transfer.transfer_id)? {
                let terminal = updated.status == TransferStatus::Failed;
                emit_event(
                    app_handle,
                    &NetworkEvent::TransferUpdated {
                        transfer: updated.clone(),
                    },
                );
                if terminal {
                    let _ = storage.update_message_status(
                        &updated.transfer_id,
                        MessageStatus::Failed,
                        updated.error.as_deref(),
                    );
                    emit_event(
                        app_handle,
                        &NetworkEvent::MessageStatusChanged {
                            message_id: updated.transfer_id,
                            status: MessageStatus::Failed,
                            error: updated.error,
                        },
                    );
                }
            }
        }
    }
    result
}

async fn send_claimed_resumable_transfer(
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
    let chunks = storage.list_transfer_chunks(&transfer.transfer_id)?;
    let mut stream = control
        .open_stream(peer_id, StreamProtocol::new(FILE_PROTOCOL_V2))
        .await
        .map_err(|error| AppError::Network(format!("无法建立可恢复文件传输流：{error}")))?;
    let header = serde_json::to_vec(&TransferStreamHeader {
        transfer_id: transfer.transfer_id.clone(),
        version: TransferProtocol::ResumableV2 as u16,
        start_offset: transfer.transferred_bytes,
        chunk_size: transfer.chunk_size,
    })
    .map_err(|error| AppError::Network(format!("无法编码可恢复文件传输头：{error}")))?;
    let header_size =
        u32::try_from(header.len()).map_err(|_| AppError::Network("文件传输头过大".to_string()))?;
    stream.write_all(&header_size.to_be_bytes()).await?;
    stream.write_all(&header).await?;

    let mut previous_offset = transfer.transferred_bytes;
    send_acknowledged_chunks(
        &mut stream,
        &source_path,
        transfer,
        &chunks,
        transfer.transferred_bytes,
        |acknowledged_offset| {
            if !storage.commit_claimed_outgoing_progress(
                &transfer.transfer_id,
                &transfer.peer_id,
                previous_offset,
                acknowledged_offset,
            )? {
                return Err(AppError::Storage(
                    "可恢复发送进度状态已变化，已停止旧传输回调".to_string(),
                ));
            }
            previous_offset = acknowledged_offset;
            if let Some(updated) = storage.get_transfer(&transfer.transfer_id)? {
                emit_event(
                    app_handle,
                    &NetworkEvent::TransferUpdated { transfer: updated },
                );
            }
            Ok(())
        },
    )
    .await?;
    if !storage.try_complete_claimed_outgoing_transfer(&transfer.transfer_id, &transfer.peer_id)? {
        return Err(AppError::Storage(
            "可恢复发送完成状态已变化，未覆盖当前记录".to_string(),
        ));
    }
    let completed = storage
        .get_transfer(&transfer.transfer_id)?
        .ok_or_else(|| AppError::Storage("完成的可恢复发送记录不存在".to_string()))?;
    storage.update_message_status(&completed.transfer_id, MessageStatus::Delivered, None)?;
    emit_event(
        app_handle,
        &NetworkEvent::TransferUpdated {
            transfer: completed.clone(),
        },
    );
    emit_event(
        app_handle,
        &NetworkEvent::MessageStatusChanged {
            message_id: completed.transfer_id,
            status: MessageStatus::Delivered,
            error: None,
        },
    );
    if let Err(error) = stream.close().await {
        tracing::debug!(transfer_id = %transfer.transfer_id, %error, "ignored stream close after resumable send completion");
    }
    Ok(())
}

fn claim_resumable_outgoing(
    storage: &Storage,
    transfer_id: &str,
    peer_id: &str,
) -> Result<Option<TransferRecord>, AppError> {
    if !storage.try_claim_outgoing_transfer(transfer_id, peer_id)? {
        return Ok(None);
    }
    let transfer = storage
        .get_transfer(transfer_id)?
        .ok_or_else(|| AppError::Storage("已占用的可恢复发送记录不存在".to_string()))?;
    if transfer.direction != Direction::Outgoing
        || transfer.peer_id != peer_id
        || transfer.transfer_protocol != TransferProtocol::ResumableV2 as u8
        || transfer.status != TransferStatus::Transferring
        || !transfer.send_claimed
    {
        return Err(AppError::Storage(
            "可恢复发送占用状态与持久化记录不一致".to_string(),
        ));
    }
    Ok(Some(transfer))
}

fn persist_claimed_outgoing_error(
    storage: &Storage,
    transfer: &TransferRecord,
    error: &AppError,
) -> Result<bool, AppError> {
    if is_recoverable_send_error(error) {
        storage.try_pause_claimed_outgoing_transfer(
            &transfer.transfer_id,
            &transfer.peer_id,
            &error.to_string(),
        )
    } else {
        storage.try_fail_claimed_outgoing_transfer(
            &transfer.transfer_id,
            &transfer.peer_id,
            &error.to_string(),
        )
    }
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

async fn receive_resumable_transfer(
    peer_id: PeerId,
    mut stream: libp2p::swarm::Stream,
    storage: Storage,
    app_handle: AppHandle,
) -> Result<(), AppError> {
    let mut header_size = [0_u8; 4];
    tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.read_exact(&mut header_size))
        .await
        .map_err(|_| AppError::Network("可恢复文件传输连接等待超时".to_string()))??;
    let header_size = usize::try_from(u32::from_be_bytes(header_size))
        .map_err(|_| AppError::InvalidInput("文件传输头长度超出当前平台限制".to_string()))?;
    if header_size == 0 || header_size > MAX_HEADER_BYTES {
        return Err(AppError::InvalidInput("文件传输头无效".to_string()));
    }
    let mut header = vec![0_u8; header_size];
    tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.read_exact(&mut header))
        .await
        .map_err(|_| AppError::Network("可恢复文件传输头等待超时".to_string()))??;
    let header: TransferStreamHeader = serde_json::from_slice(&header)
        .map_err(|error| AppError::InvalidInput(format!("无法读取可恢复文件传输头：{error}")))?;
    if header.version != TransferProtocol::ResumableV2 as u16 {
        return Err(AppError::InvalidInput(
            "可恢复文件传输头版本无效".to_string(),
        ));
    }
    let transfer = storage
        .get_transfer(&header.transfer_id)?
        .ok_or_else(|| AppError::Permission("未找到已接受的可恢复文件传输".to_string()))?;
    if transfer.peer_id != peer_id.to_string()
        || transfer.direction != Direction::Incoming
        || transfer.status != TransferStatus::Transferring
        || transfer.transfer_protocol != TransferProtocol::ResumableV2 as u8
        || header.chunk_size != TRANSFER_CHUNK_BYTES
        || header.chunk_size != transfer.chunk_size
        || header.start_offset != transfer.transferred_bytes
        || !storage.is_friend(&transfer.peer_id)?
    {
        return Err(AppError::Permission(
            "该可恢复文件传输未获授权或恢复几何信息无效".to_string(),
        ));
    }
    if !storage.try_claim_incoming_transfer(&transfer.transfer_id, &transfer.peer_id)? {
        return Err(AppError::Permission(
            "该文件传输已有接收连接，重复连接已拒绝".to_string(),
        ));
    }

    let result = receive_resumable_body(
        &mut stream,
        &storage,
        &app_handle,
        &transfer,
        header.start_offset,
    )
    .await;
    if let Err(error) = &result {
        let changed = if is_recoverable_receive_error(error) {
            storage.try_pause_claimed_incoming_transfer(
                &transfer.transfer_id,
                &transfer.peer_id,
                &error.to_string(),
            )?
        } else {
            storage.try_fail_claimed_incoming_transfer(
                &transfer.transfer_id,
                &transfer.peer_id,
                &error.to_string(),
            )?
        };
        if changed {
            if let Some(updated) = storage.get_transfer(&transfer.transfer_id)? {
                emit_event(
                    &app_handle,
                    &NetworkEvent::TransferUpdated { transfer: updated },
                );
            }
        }
        let _ = stream.close().await;
        return result;
    }
    stream.close().await?;
    Ok(())
}

async fn receive_resumable_body(
    stream: &mut libp2p::swarm::Stream,
    storage: &Storage,
    app_handle: &AppHandle,
    transfer: &TransferRecord,
    start_offset: u64,
) -> Result<(), AppError> {
    let destination = transfer
        .local_path
        .as_deref()
        .map(PathBuf::from)
        .ok_or_else(|| AppError::Permission("接收文件尚未选择保存位置".to_string()))?;
    let partial = transfer
        .partial_path
        .as_deref()
        .map(PathBuf::from)
        .ok_or_else(|| AppError::Permission("可恢复文件缺少部分文件路径".to_string()))?;
    let reservation_token = transfer
        .reservation_token
        .as_deref()
        .ok_or_else(|| AppError::Permission("可恢复接收缺少所有权凭据".to_string()))?;
    if !transfer.destination_reserved {
        return Err(AppError::Permission(
            "可恢复接收缺少目标文件占位".to_string(),
        ));
    }
    let file = open_owned_resumable_partial(
        &partial,
        &destination,
        &transfer.transfer_id,
        reservation_token,
        start_offset,
    )
    .await?;

    let transfer_id = transfer.transfer_id.clone();
    let storage_for_finalize = storage.clone();
    let app_for_finalize = app_handle.clone();
    let transfer_for_finalize = transfer.clone();
    let partial_for_finalize = partial.clone();
    let destination_for_finalize = destination.clone();
    receive_acknowledged_chunks(
        stream,
        file,
        transfer,
        start_offset,
        |chunk, committed_bytes| {
            storage.commit_received_chunk(&transfer_id, &transfer.peer_id, chunk, committed_bytes)
        },
        || verify_committed_manifest(storage, transfer),
        move || async move {
            let file_name = destination_for_finalize
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or(&transfer_for_finalize.file_name)
                .to_string();
            let transfer_id = transfer_for_finalize.transfer_id.clone();
            let destination_reserved = transfer_for_finalize.destination_reserved;
            let reservation_token = transfer_for_finalize.reservation_token.clone();
            let partial_for_commit = partial_for_finalize.clone();
            let destination_for_commit = destination_for_finalize.clone();
            let completed_path = tokio::task::spawn_blocking(move || {
                if !destination_reserved {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "可恢复接收缺少目标文件占位",
                    ));
                }
                let token = reservation_token.as_deref().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "接收文件占位凭据缺失")
                })?;
                finalize_reserved_receive_durable(
                    &partial_for_commit,
                    &destination_for_commit,
                    &file_name,
                    &transfer_id,
                    token,
                )
            })
            .await
            .map_err(|error| AppError::Io(std::io::Error::other(error.to_string())))??;
            let mut completed = transfer_for_finalize;
            completed.local_path = Some(completed_path.path.to_string_lossy().into_owned());
            completed.partial_path = None;
            completed.destination_reserved = !completed_path.reservation_released;
            if completed_path.reservation_released {
                completed.reservation_token = None;
            }
            complete_incoming(&storage_for_finalize, &app_for_finalize, completed)
        },
    )
    .await?;
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
    if transfer.transfer_protocol == TransferProtocol::ResumableV2 as u8 {
        if !storage.try_complete_claimed_incoming_transfer(&transfer)? {
            return Err(AppError::Storage(
                "可恢复接收完成状态已变化，未覆盖当前记录".to_string(),
            ));
        }
        storage.cleanup_completed_incoming_artifacts(&transfer.transfer_id)?;
        transfer = storage
            .get_transfer(&transfer.transfer_id)?
            .ok_or_else(|| AppError::Storage("完成的可恢复接收记录不存在".to_string()))?;
    } else {
        storage.upsert_transfer(&transfer)?;
    }
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

#[cfg(test)]
mod tests {
    use std::{
        fs, io,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::io::{AsyncRead, AsyncWrite};

    use super::{
        claim_resumable_outgoing, persist_claimed_outgoing_error, send_acknowledged_chunks,
    };
    use crate::{
        domain::{Direction, TransferKind, TransferRecord, TransferStatus},
        error::AppError,
        storage::Storage,
        transfer_manifest::build_manifest,
        transfer_policy::{TRANSFER_CHUNK_BYTES, TransferProtocol},
    };

    struct DisconnectingStream;

    impl AsyncRead for DisconnectingStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(0))
        }
    }

    impl AsyncWrite for DisconnectingStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "real scripted transport disconnect",
            )))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn outgoing_transfer(transfer_id: &str) -> TransferRecord {
        TransferRecord {
            transfer_id: transfer_id.to_string(),
            peer_id: "peer-one".to_string(),
            direction: Direction::Outgoing,
            kind: TransferKind::File,
            file_name: "report.bin".to_string(),
            file_size: u64::from(TRANSFER_CHUNK_BYTES),
            mime_type: "application/octet-stream".to_string(),
            sha256: "0".repeat(64),
            local_path: Some("C:\\fixtures\\report.bin".to_string()),
            destination_reserved: false,
            reservation_token: None,
            transfer_protocol: TransferProtocol::ResumableV2 as u8,
            chunk_size: TRANSFER_CHUNK_BYTES,
            chunk_count: 1,
            manifest_sha256: Some("0".repeat(64)),
            partial_path: None,
            source_modified_ns: Some(1),
            send_claimed: false,
            transferred_bytes: 0,
            status: TransferStatus::Transferring,
            error: None,
            created_at: "2026-08-24T00:00:00.000Z".to_string(),
            updated_at: "2026-08-24T00:00:00.000Z".to_string(),
        }
    }

    #[test]
    fn live_v2_send_claims_before_work_and_recoverable_disconnect_pauses() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-live-v2-send-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create fixture");
        let storage = Storage::open(&fixture.join("localnet.sqlite3")).expect("open storage");
        storage
            .upsert_transfer(&outgoing_transfer("recoverable-send"))
            .expect("persist outgoing transfer");

        let claimed = claim_resumable_outgoing(&storage, "recoverable-send", "peer-one")
            .expect("claim live v2 send")
            .expect("claim winner loads transfer");
        assert!(claimed.send_claimed);
        assert_eq!(claimed.status, TransferStatus::Transferring);

        assert!(
            persist_claimed_outgoing_error(
                &storage,
                &claimed,
                &AppError::Network("connection reset".to_string()),
            )
            .expect("persist recoverable disconnect")
        );
        let paused = storage
            .get_transfer("recoverable-send")
            .expect("load paused send")
            .expect("paused send exists");
        assert_eq!(paused.status, TransferStatus::Paused);
        assert!(!paused.send_claimed);
        assert_eq!(paused.error.as_deref(), Some("connection reset"));

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[test]
    fn live_v2_send_terminal_source_error_fails_and_cancelled_row_never_claims() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-live-v2-terminal-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create fixture");
        let storage = Storage::open(&fixture.join("localnet.sqlite3")).expect("open storage");
        let mut cancelled = outgoing_transfer("cancelled-send");
        cancelled.status = TransferStatus::Cancelled;
        storage
            .upsert_transfer(&cancelled)
            .expect("persist cancelled transfer");
        assert!(
            claim_resumable_outgoing(&storage, "cancelled-send", "peer-one")
                .expect("attempt cancelled claim")
                .is_none()
        );

        storage
            .upsert_transfer(&outgoing_transfer("terminal-send"))
            .expect("persist outgoing transfer");
        let claimed = claim_resumable_outgoing(&storage, "terminal-send", "peer-one")
            .expect("claim terminal test send")
            .expect("claim winner loads transfer");
        assert!(
            persist_claimed_outgoing_error(
                &storage,
                &claimed,
                &AppError::InvalidInput("source was truncated".to_string()),
            )
            .expect("persist terminal source failure")
        );
        let failed = storage
            .get_transfer("terminal-send")
            .expect("load failed send")
            .expect("failed send exists");
        assert_eq!(failed.status, TransferStatus::Failed);
        assert!(!failed.send_claimed);

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[tokio::test]
    async fn acknowledged_send_disconnect_pauses_the_claimed_database_row() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-acknowledged-send-disconnect-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create fixture");
        let source = fixture.join("source.bin");
        fs::write(&source, b"payload").expect("write source");
        let manifest = build_manifest(&source, TRANSFER_CHUNK_BYTES).expect("build manifest");
        let mut transfer = outgoing_transfer("disconnecting-send");
        transfer.file_size = manifest.file_size;
        transfer.sha256 = hex::encode(manifest.file_sha256);
        transfer.local_path = Some(source.to_string_lossy().into_owned());
        transfer.chunk_count = u32::try_from(manifest.chunks.len()).expect("small manifest");
        transfer.manifest_sha256 = Some(hex::encode(manifest.manifest_sha256));
        transfer.source_modified_ns = Some(manifest.source_modified_ns);
        let storage = Storage::open(&fixture.join("localnet.sqlite3")).expect("open storage");
        storage
            .create_outgoing_transfer_with_manifest(&transfer, &manifest.chunks)
            .expect("persist outgoing manifest");
        let claimed = claim_resumable_outgoing(&storage, &transfer.transfer_id, &transfer.peer_id)
            .expect("claim outgoing transfer")
            .expect("claim wins");
        let mut stream = DisconnectingStream;

        let error =
            send_acknowledged_chunks(&mut stream, &source, &claimed, &manifest.chunks, 0, |_| {
                Ok(())
            })
            .await
            .expect_err("transport disconnect must stop acknowledged send");
        assert!(
            matches!(error, AppError::Io(ref error) if error.kind() == io::ErrorKind::BrokenPipe)
        );
        assert!(
            persist_claimed_outgoing_error(&storage, &claimed, &error)
                .expect("persist disconnect outcome")
        );
        let paused = storage
            .get_transfer(&transfer.transfer_id)
            .expect("load paused transfer")
            .expect("paused transfer exists");
        assert_eq!(paused.status, TransferStatus::Paused);
        assert!(!paused.send_claimed);
        assert_eq!(paused.transferred_bytes, 0);

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }
}
