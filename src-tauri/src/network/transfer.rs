use std::{
    cmp,
    future::Future,
    path::PathBuf,
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::{Arc, Mutex};

use futures::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, StreamExt as _};
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
        commit_without_overwrite, finalize_reserved_receive,
        finalize_reserved_receive_durable_with_hooks, preflight_receive_directory,
        remove_owned_reservation,
    },
    storage::Storage,
    transfer_policy::{TRANSFER_CHUNK_BYTES, TransferProtocol},
};

const BUFFER_SIZE: usize = 64 * 1024;
const PROGRESS_BYTES: u64 = 1024 * 1024;
const MAX_HEADER_BYTES: usize = 8 * 1024;
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) trait ResumableIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> ResumableIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

enum ResumableStreamOpener<'a> {
    Network(&'a mut libp2p_stream::Control, PeerId),
    #[cfg(test)]
    Scripted(Box<dyn ResumableIo>),
    #[cfg(test)]
    ObservedScripted {
        stream: Box<dyn ResumableIo>,
        opened_protocol: Arc<Mutex<Option<String>>>,
    },
}

impl ResumableStreamOpener<'_> {
    async fn open(self) -> Result<Box<dyn ResumableIo>, AppError> {
        let protocol = StreamProtocol::new(FILE_PROTOCOL_V2);
        match self {
            Self::Network(control, peer_id) => control
                .open_stream(peer_id, protocol)
                .await
                .map(|stream| Box::new(stream) as Box<dyn ResumableIo>)
                .map_err(|error| AppError::Network(format!("无法建立可恢复文件传输流：{error}"))),
            #[cfg(test)]
            Self::Scripted(stream) => Ok(stream),
            #[cfg(test)]
            Self::ObservedScripted {
                stream,
                opened_protocol,
            } => {
                *opened_protocol
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(protocol.as_ref().to_string());
                Ok(stream)
            }
        }
    }
}

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

pub fn spawn_claimed_outgoing_resumable_transfer(
    mut control: libp2p_stream::Control,
    peer_id: PeerId,
    transfer: TransferRecord,
    query_token: String,
    storage: Storage,
    app_handle: AppHandle,
) {
    tauri::async_runtime::spawn(async move {
        let opener = ResumableStreamOpener::Network(&mut control, peer_id);
        let mut publish = |event| emit_event(&app_handle, &event);
        let result = run_claimed_outgoing_resumable_transfer(
            opener,
            transfer,
            query_token,
            &storage,
            &mut publish,
        )
        .await;
        if let Err(error) = result {
            tracing::warn!(
                %error,
                "claimed resumable outgoing transfer stopped"
            );
        }
    });
}

async fn run_claimed_outgoing_resumable_transfer<P>(
    opener: ResumableStreamOpener<'_>,
    transfer: TransferRecord,
    query_token: String,
    storage: &Storage,
    publish: &mut P,
) -> Result<(), AppError>
where
    P: FnMut(NetworkEvent),
{
    let result =
        send_claimed_resumable_transfer(opener, &transfer, Some(&query_token), storage, publish)
            .await;
    if let Err(error) = &result {
        match persist_claimed_outgoing_resume_error(storage, &transfer, &query_token, error) {
            Ok(true) => {
                if let Ok(Some(updated)) = storage.get_transfer(&transfer.transfer_id) {
                    let terminal = updated.status == TransferStatus::Failed;
                    publish(NetworkEvent::TransferUpdated {
                        transfer: updated.clone(),
                    });
                    if terminal {
                        let _ = storage.update_message_status(
                            &updated.transfer_id,
                            MessageStatus::Failed,
                            updated.error.as_deref(),
                        );
                        publish(NetworkEvent::MessageStatusChanged {
                            message_id: updated.transfer_id,
                            status: MessageStatus::Failed,
                            error: updated.error,
                        });
                    }
                }
            }
            Ok(false) => {}
            Err(persist_error) => {
                tracing::warn!(
                    transfer_id = %transfer.transfer_id,
                    %persist_error,
                    "failed to persist claimed resumable send failure"
                );
            }
        }
    }
    result
}

#[cfg(test)]
pub(super) async fn run_claimed_outgoing_resumable_transfer_with_stream<P>(
    stream: Box<dyn ResumableIo>,
    opened_protocol: Arc<Mutex<Option<String>>>,
    transfer: TransferRecord,
    query_token: String,
    storage: &Storage,
    publish: &mut P,
) -> Result<(), AppError>
where
    P: FnMut(NetworkEvent),
{
    run_claimed_outgoing_resumable_transfer(
        ResumableStreamOpener::ObservedScripted {
            stream,
            opened_protocol,
        },
        transfer,
        query_token,
        storage,
        publish,
    )
    .await
}

pub(crate) fn return_pending_incoming_decision_to_manual(
    transfer_id: &str,
    storage: &Storage,
    message: String,
) -> Result<Option<TransferRecord>, AppError> {
    let Some(candidate) = storage.get_transfer(transfer_id)? else {
        return Ok(None);
    };
    if candidate.direction != Direction::Incoming
        || candidate.status != TransferStatus::Transferring
        || candidate.transferred_bytes != 0
        || !storage.try_claim_incoming_transfer(transfer_id, &candidate.peer_id)?
    {
        return Ok(None);
    }
    let claimed = storage
        .get_transfer(transfer_id)?
        .ok_or_else(|| AppError::Storage("接收文件记录在清理期间消失".to_string()))?;
    if !storage.try_return_claimed_incoming_to_awaiting(&claimed, &message)? {
        return Err(AppError::Storage(
            "接收确认清理期间状态已变化，请刷新后重试".to_string(),
        ));
    }
    storage
        .get_transfer(transfer_id)?
        .map(Some)
        .ok_or_else(|| AppError::Storage("接收文件记录在回退后消失".to_string()))
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
            let opener = ResumableStreamOpener::Network(control, peer_id);
            send_resumable_transfer(opener, transfer, storage, &mut |event| {
                emit_event(app_handle, &event);
            })
            .await
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

async fn send_resumable_transfer<P>(
    opener: ResumableStreamOpener<'_>,
    transfer: &TransferRecord,
    storage: &Storage,
    publish: &mut P,
) -> Result<(), AppError>
where
    P: FnMut(NetworkEvent),
{
    let Some(transfer) =
        claim_resumable_outgoing(storage, &transfer.transfer_id, &transfer.peer_id)?
    else {
        return Ok(());
    };
    let result = send_claimed_resumable_transfer(opener, &transfer, None, storage, publish).await;
    if let Err(error) = &result {
        if persist_claimed_outgoing_error(storage, &transfer, error)? {
            if let Some(updated) = storage.get_transfer(&transfer.transfer_id)? {
                let terminal = updated.status == TransferStatus::Failed;
                publish(NetworkEvent::TransferUpdated {
                    transfer: updated.clone(),
                });
                if terminal {
                    let _ = storage.update_message_status(
                        &updated.transfer_id,
                        MessageStatus::Failed,
                        updated.error.as_deref(),
                    );
                    publish(NetworkEvent::MessageStatusChanged {
                        message_id: updated.transfer_id,
                        status: MessageStatus::Failed,
                        error: updated.error,
                    });
                }
            }
        }
    }
    result
}

async fn send_claimed_resumable_transfer<P>(
    opener: ResumableStreamOpener<'_>,
    transfer: &TransferRecord,
    query_token: Option<&str>,
    storage: &Storage,
    publish: &mut P,
) -> Result<(), AppError>
where
    P: FnMut(NetworkEvent),
{
    let source_path = transfer
        .local_path
        .as_deref()
        .map(PathBuf::from)
        .ok_or_else(|| AppError::Io(std::io::Error::other("发送文件路径缺失")))?;
    let chunks = storage.list_transfer_chunks(&transfer.transfer_id)?;
    let mut stream = opener.open().await?;
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
            let committed = match query_token {
                Some(query_token) => storage.commit_claimed_outgoing_resume_progress(
                    &transfer.transfer_id,
                    &transfer.peer_id,
                    query_token,
                    previous_offset,
                    acknowledged_offset,
                )?,
                None => storage.commit_claimed_outgoing_progress(
                    &transfer.transfer_id,
                    &transfer.peer_id,
                    previous_offset,
                    acknowledged_offset,
                )?,
            };
            if !committed {
                return Err(AppError::Storage(
                    "可恢复发送进度状态已变化，已停止旧传输回调".to_string(),
                ));
            }
            previous_offset = acknowledged_offset;
            if let Some(updated) = storage.get_transfer(&transfer.transfer_id)? {
                publish(NetworkEvent::TransferUpdated { transfer: updated });
            }
            Ok(())
        },
    )
    .await?;
    let message_completed_atomically = match query_token {
        Some(query_token) => match storage.try_complete_claimed_outgoing_resume_and_message(
            &transfer.transfer_id,
            &transfer.peer_id,
            query_token,
        ) {
            Ok(true) => true,
            Ok(false) => {
                return Err(AppError::Storage(
                    "可恢复发送完成代次已变化，未覆盖当前记录".to_string(),
                ));
            }
            Err(error) => {
                if storage.try_pause_claimed_outgoing_resume_transfer(
                    &transfer.transfer_id,
                    &transfer.peer_id,
                    query_token,
                    &error.to_string(),
                )? && let Some(updated) = storage.get_transfer(&transfer.transfer_id)?
                {
                    publish(NetworkEvent::TransferUpdated { transfer: updated });
                }
                return Err(error);
            }
        },
        None => {
            if !storage
                .try_complete_claimed_outgoing_transfer(&transfer.transfer_id, &transfer.peer_id)?
            {
                return Err(AppError::Storage(
                    "可恢复发送完成状态已变化，未覆盖当前记录".to_string(),
                ));
            }
            false
        }
    };
    let completed = storage
        .get_transfer(&transfer.transfer_id)?
        .ok_or_else(|| AppError::Storage("完成的可恢复发送记录不存在".to_string()))?;
    if !message_completed_atomically {
        storage.update_message_status(&completed.transfer_id, MessageStatus::Delivered, None)?;
    }
    publish(NetworkEvent::TransferUpdated {
        transfer: completed.clone(),
    });
    publish(NetworkEvent::MessageStatusChanged {
        message_id: completed.transfer_id,
        status: MessageStatus::Delivered,
        error: None,
    });
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

pub(super) fn persist_claimed_outgoing_error(
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

pub(super) fn persist_claimed_outgoing_resume_error(
    storage: &Storage,
    transfer: &TransferRecord,
    query_token: &str,
    error: &AppError,
) -> Result<bool, AppError> {
    if is_recoverable_send_error(error) {
        storage.try_pause_claimed_outgoing_resume_transfer(
            &transfer.transfer_id,
            &transfer.peer_id,
            query_token,
            &error.to_string(),
        )
    } else {
        storage.try_fail_claimed_outgoing_resume_transfer(
            &transfer.transfer_id,
            &transfer.peer_id,
            query_token,
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
        || !matches!(
            transfer.status,
            TransferStatus::Transferring | TransferStatus::Paused
        )
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
    let transfer_id = transfer.transfer_id.clone();
    let storage_for_body = storage.clone();
    let app_for_body = app_handle.clone();
    let result = run_resumable_receive_body_with(
        &storage,
        &transfer,
        header.start_offset,
        &preflight_receive_directory,
        move |claimed, authoritative_offset| async move {
            let result = receive_resumable_body(
                &mut stream,
                &storage_for_body,
                &app_for_body,
                &claimed,
                authoritative_offset,
            )
            .await;
            let close_result = stream.close().await.map_err(AppError::from);
            result.and(close_result)
        },
    )
    .await;
    if let Err(error) = &result {
        if let Some(updated) = storage.get_transfer(&transfer_id)? {
            emit_event(
                &app_handle,
                &NetworkEvent::TransferUpdated { transfer: updated },
            );
        }
        emit_event(
            &app_handle,
            &NetworkEvent::NetworkError {
                code: "transfer.resume_destination_unavailable".to_string(),
                message: error.to_string(),
            },
        );
        return result;
    }
    Ok(())
}

async fn run_resumable_receive_body_with<P, B, F>(
    storage: &Storage,
    candidate: &TransferRecord,
    incoming_start_offset: u64,
    preflight: &P,
    body: B,
) -> Result<(), AppError>
where
    P: Fn(&std::path::Path, u64, u64) -> Result<(), AppError>,
    B: FnOnce(TransferRecord, u64) -> F,
    F: Future<Output = Result<(), AppError>>,
{
    let claimed = match super::resumable_transfer::claim_incoming_at_offset_with_preflight(
        storage,
        candidate,
        incoming_start_offset,
        preflight,
    )? {
        Some(claimed) => claimed,
        None => {
            return Err(AppError::Permission(
                "该文件传输已有接收连接，重复连接已拒绝".to_string(),
            ));
        }
    };

    let authoritative_offset = claimed.transferred_bytes;
    let result = body(claimed.clone(), authoritative_offset).await;
    if let Err(error) = &result {
        if is_recoverable_receive_error(error) {
            storage.try_pause_claimed_incoming_transfer(
                &claimed.transfer_id,
                &claimed.peer_id,
                &error.to_string(),
            )?;
        } else {
            storage.try_fail_claimed_incoming_transfer(
                &claimed.transfer_id,
                &claimed.peer_id,
                &error.to_string(),
            )?;
        }
    }
    result
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
            let storage_for_switch = storage_for_finalize.clone();
            let peer_for_switch = transfer_for_finalize.peer_id.clone();
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
                finalize_reserved_receive_durable_with_hooks(
                    &partial_for_commit,
                    &destination_for_commit,
                    &file_name,
                    &transfer_id,
                    token,
                    |previous, next| {
                        if storage_for_switch
                            .try_switch_claimed_incoming_destination(
                                &transfer_id,
                                &peer_for_switch,
                                previous,
                                next,
                                token,
                            )
                            .map_err(|error| std::io::Error::other(error.to_string()))?
                        {
                            Ok(())
                        } else {
                            Err(std::io::Error::other("可恢复接收目标切换状态已变化"))
                        }
                    },
                    |_| Ok(()),
                )
            })
            .await
            .map_err(|error| AppError::Io(std::io::Error::other(error.to_string())))??;
            let mut completed = transfer_for_finalize;
            completed.local_path = Some(completed_path.path.to_string_lossy().into_owned());
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
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
    };

    use futures::io::{AsyncRead, AsyncWrite};

    use super::{
        ResumableStreamOpener, claim_resumable_outgoing, persist_claimed_outgoing_error,
        run_resumable_receive_body_with, send_resumable_transfer,
    };
    use crate::{
        domain::{Direction, TransferKind, TransferRecord, TransferStatus},
        error::AppError,
        receive_paths::reserve_receive_path,
        storage::Storage,
        transfer_manifest::{TransferChunk, build_manifest, manifest_root},
        transfer_policy::{TRANSFER_CHUNK_BYTES, TransferProtocol},
        volume_preflight::{VolumeSnapshot, validate_volume},
    };

    const MIB: u64 = 1024 * 1024;
    const DESTINATION_RESERVE_BYTES: u64 = 64 * MIB;

    struct DisconnectingStream;

    struct CancellingAckStream {
        incoming: Vec<u8>,
        cursor: usize,
        storage: Storage,
        transfer_id: String,
        peer_id: String,
        cancelled: bool,
    }

    impl AsyncRead for CancellingAckStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            if !self.cancelled {
                self.storage
                    .try_pause_claimed_outgoing_transfer(
                        &self.transfer_id,
                        &self.peer_id,
                        "cancel raced with acknowledgement",
                    )
                    .expect("pause active send at ACK boundary");
                self.storage
                    .try_cancel_unclaimed_outgoing_transfer(
                        &self.transfer_id,
                        &self.peer_id,
                        "cancelled while ACK callback was pending",
                    )
                    .expect("cancel paused send at ACK boundary");
                self.cancelled = true;
            }
            let remaining = &self.incoming[self.cursor..];
            let read = remaining.len().min(buffer.len());
            buffer[..read].copy_from_slice(&remaining[..read]);
            self.cursor += read;
            Poll::Ready(Ok(read))
        }
    }

    impl AsyncWrite for CancellingAckStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

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

    fn paused_receive_with_real_partial(
        name: &str,
    ) -> (std::path::PathBuf, Storage, TransferRecord) {
        let directory = std::env::temp_dir().join(format!(
            "weline-localnet-production-receive-{name}-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&directory).expect("create production receive fixture");
        let storage = Storage::open(&directory.join("localnet.sqlite3")).expect("open storage");
        let chunks = [
            TransferChunk {
                index: 0,
                length: TRANSFER_CHUNK_BYTES,
                sha256: [1; 32],
            },
            TransferChunk {
                index: 1,
                length: TRANSFER_CHUNK_BYTES,
                sha256: [2; 32],
            },
            TransferChunk {
                index: 2,
                length: TRANSFER_CHUNK_BYTES,
                sha256: [3; 32],
            },
        ];
        let destination = directory.join("payload.bin");
        let token = uuid::Uuid::now_v7().to_string();
        let now = "2026-08-25T00:00:00.000Z".to_string();
        let mut transfer = TransferRecord {
            transfer_id: uuid::Uuid::now_v7().to_string(),
            peer_id: "peer-one".to_string(),
            direction: Direction::Incoming,
            kind: TransferKind::File,
            file_name: "payload.bin".to_string(),
            file_size: 3 * u64::from(TRANSFER_CHUNK_BYTES),
            mime_type: "application/octet-stream".to_string(),
            sha256: "0".repeat(64),
            local_path: None,
            destination_reserved: false,
            reservation_token: None,
            transfer_protocol: TransferProtocol::ResumableV2 as u8,
            chunk_size: TRANSFER_CHUNK_BYTES,
            chunk_count: 3,
            manifest_sha256: Some(hex::encode(manifest_root(&chunks))),
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
            .expect("persist awaiting receive");
        reserve_receive_path(&destination, &transfer.transfer_id, &token)
            .expect("reserve receive destination");
        transfer.local_path = Some(destination.to_string_lossy().into_owned());
        transfer.destination_reserved = true;
        transfer.reservation_token = Some(token);
        transfer.status = TransferStatus::Transferring;
        assert!(
            storage
                .try_accept_incoming_transfer(&transfer)
                .expect("accept incoming fixture")
        );
        assert!(
            storage
                .try_claim_incoming_transfer(&transfer.transfer_id, &transfer.peer_id)
                .expect("claim incoming fixture")
        );
        let accepted = storage
            .get_transfer(&transfer.transfer_id)
            .expect("reload accepted fixture")
            .expect("accepted fixture exists");
        let partial = accepted.partial_path.as_deref().expect("partial path");
        fs::OpenOptions::new()
            .write(true)
            .open(partial)
            .expect("open owned partial")
            .set_len(u64::from(TRANSFER_CHUNK_BYTES))
            .expect("materialize first committed chunk");
        assert!(
            storage
                .commit_received_chunk(
                    &transfer.transfer_id,
                    &transfer.peer_id,
                    &chunks[0],
                    u64::from(TRANSFER_CHUNK_BYTES),
                )
                .expect("commit first receive chunk")
        );
        assert!(
            storage
                .try_pause_claimed_incoming_transfer(
                    &transfer.transfer_id,
                    &transfer.peer_id,
                    "network interrupted",
                )
                .expect("pause receive fixture")
        );
        let paused = storage
            .get_transfer(&transfer.transfer_id)
            .expect("reload paused receive")
            .expect("paused receive exists");
        (directory, storage, paused)
    }

    fn advance_paused_receive(storage: &Storage, paused: &TransferRecord) -> TransferRecord {
        assert!(
            storage
                .try_claim_incoming_transfer(&paused.transfer_id, &paused.peer_id)
                .expect("claim competing receive")
        );
        let partial = paused.partial_path.as_deref().expect("partial path");
        fs::OpenOptions::new()
            .write(true)
            .open(partial)
            .expect("open competing partial")
            .set_len(2 * u64::from(TRANSFER_CHUNK_BYTES))
            .expect("materialize second committed chunk");
        let second = TransferChunk {
            index: 1,
            length: TRANSFER_CHUNK_BYTES,
            sha256: [2; 32],
        };
        assert!(
            storage
                .commit_received_chunk(
                    &paused.transfer_id,
                    &paused.peer_id,
                    &second,
                    2 * u64::from(TRANSFER_CHUNK_BYTES),
                )
                .expect("commit competing receive progress")
        );
        assert!(
            storage
                .try_pause_claimed_incoming_transfer(
                    &paused.transfer_id,
                    &paused.peer_id,
                    "newer stream paused",
                )
                .expect("pause competing receive")
        );
        storage
            .get_transfer(&paused.transfer_id)
            .expect("reload advanced receive")
            .expect("advanced receive exists")
    }

    #[tokio::test]
    async fn production_receive_boundary_rejects_stale_header_without_truncating_and_fresh_offset_succeeds()
     {
        let (directory, storage, stale_snapshot) = paused_receive_with_real_partial("stale-offset");
        let advanced = advance_paused_receive(&storage, &stale_snapshot);
        let partial = std::path::PathBuf::from(
            advanced
                .partial_path
                .as_deref()
                .expect("advanced partial path"),
        );
        let stale_body_started = Arc::new(AtomicBool::new(false));
        let stale_body_flag = stale_body_started.clone();

        let error = run_resumable_receive_body_with(
            &storage,
            &stale_snapshot,
            stale_snapshot.transferred_bytes,
            &|_, _, _| Ok(()),
            move |claimed, offset| {
                let started = stale_body_flag.clone();
                let partial = std::path::PathBuf::from(
                    claimed.partial_path.as_deref().expect("claimed partial"),
                );
                let destination = std::path::PathBuf::from(
                    claimed.local_path.as_deref().expect("claimed destination"),
                );
                let transfer_id = claimed.transfer_id.clone();
                let token = claimed
                    .reservation_token
                    .as_deref()
                    .expect("claimed token")
                    .to_string();
                Box::pin(async move {
                    started.store(true, Ordering::SeqCst);
                    let file = super::open_owned_resumable_partial(
                        &partial,
                        &destination,
                        &transfer_id,
                        &token,
                        offset,
                    )
                    .await?;
                    drop(file);
                    Ok(())
                })
            },
        )
        .await
        .expect_err("stale stream must lose after the authoritative claim reload");

        assert!(error.to_string().contains("恢复偏移"));
        assert!(!stale_body_started.load(Ordering::SeqCst));
        assert_eq!(
            fs::metadata(&partial)
                .expect("inspect preserved partial")
                .len(),
            advanced.transferred_bytes
        );
        let paused = storage
            .get_transfer(&advanced.transfer_id)
            .expect("reload stale loser")
            .expect("stale loser remains");
        assert_eq!(paused.status, TransferStatus::Paused);
        assert_eq!(paused.transferred_bytes, advanced.transferred_bytes);
        assert_eq!(paused.error.as_deref(), Some(error.to_string().as_str()));
        assert!(
            !paused
                .error
                .as_deref()
                .is_some_and(|value| value.contains("weline-localnet:destination-preflight"))
        );

        let fresh_body_started = Arc::new(AtomicBool::new(false));
        let fresh_flag = fresh_body_started.clone();
        run_resumable_receive_body_with(
            &storage,
            &paused,
            paused.transferred_bytes,
            &|_, _, _| Ok(()),
            move |_, offset| {
                let started = fresh_flag.clone();
                let expected = paused.transferred_bytes;
                Box::pin(async move {
                    assert_eq!(offset, expected);
                    started.store(true, Ordering::SeqCst);
                    Ok(())
                })
            },
        )
        .await
        .expect("fresh authoritative offset must reach the body boundary");
        assert!(fresh_body_started.load(Ordering::SeqCst));
        assert!(
            storage
                .try_pause_claimed_incoming_transfer(
                    &advanced.transfer_id,
                    &advanced.peer_id,
                    "test cleanup",
                )
                .expect("release fresh receive claim")
        );
        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    &advanced.transfer_id,
                    &advanced.peer_id,
                    advanced.transfer_protocol,
                    "test cleanup",
                )
                .expect("clean stale receive fixture")
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove stale receive fixture");
    }

    #[tokio::test]
    async fn production_receive_boundary_retries_preflight_with_committed_bytes_before_body() {
        let (directory, storage, paused) = paused_receive_with_real_partial("space-retry");
        let partial = std::path::PathBuf::from(paused.partial_path.as_deref().expect("partial"));
        let remaining = paused.file_size - paused.transferred_bytes;
        let insufficient =
            VolumeSnapshot::known("NTFS", remaining + DESTINATION_RESERVE_BYTES - 1, None);
        let blocked_body = Arc::new(AtomicBool::new(false));
        let blocked_flag = blocked_body.clone();

        let error = run_resumable_receive_body_with(
            &storage,
            &paused,
            paused.transferred_bytes,
            &|_, size, committed| validate_volume(&insufficient, size, committed),
            move |_, _| {
                let started = blocked_flag.clone();
                Box::pin(async move {
                    started.store(true, Ordering::SeqCst);
                    Ok(())
                })
            },
        )
        .await
        .expect_err("insufficient resume space must block production body entry");
        assert!(error.to_string().contains("可用空间不足"));
        assert!(!blocked_body.load(Ordering::SeqCst));
        assert_eq!(
            fs::metadata(&partial)
                .expect("inspect blocked partial")
                .len(),
            paused.transferred_bytes
        );

        let retried = storage
            .get_transfer(&paused.transfer_id)
            .expect("reload retryable receive")
            .expect("retryable receive exists");
        let exact = VolumeSnapshot::known("NTFS", remaining + DESTINATION_RESERVE_BYTES, None);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let probe_values = observed.clone();
        let body_started = Arc::new(AtomicBool::new(false));
        let body_flag = body_started.clone();
        run_resumable_receive_body_with(
            &storage,
            &retried,
            retried.transferred_bytes,
            &move |_, size, committed| {
                probe_values
                    .lock()
                    .expect("lock probe values")
                    .push((size, committed));
                validate_volume(&exact, size, committed)
            },
            move |_, offset| {
                let started = body_flag.clone();
                let expected = retried.transferred_bytes;
                Box::pin(async move {
                    assert_eq!(offset, expected);
                    started.store(true, Ordering::SeqCst);
                    Ok(())
                })
            },
        )
        .await
        .expect("restored exact space must permit production body entry");
        assert_eq!(
            observed.lock().expect("lock probe values").as_slice(),
            &[(paused.file_size, paused.transferred_bytes)]
        );
        assert!(body_started.load(Ordering::SeqCst));
        assert!(
            storage
                .try_pause_claimed_incoming_transfer(
                    &paused.transfer_id,
                    &paused.peer_id,
                    "test cleanup",
                )
                .expect("release restored receive claim")
        );
        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    &paused.transfer_id,
                    &paused.peer_id,
                    paused.transfer_protocol,
                    "test cleanup",
                )
                .expect("clean preflight receive fixture")
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove preflight receive fixture");
    }

    #[tokio::test]
    async fn recoverable_receive_stream_pause_remains_untagged_after_restart() {
        let (directory, storage, paused) = paused_receive_with_real_partial("stream-pause-tag");
        let database = directory.join("localnet.sqlite3");
        let expected = "connection reset while receiving body";

        let error = run_resumable_receive_body_with(
            &storage,
            &paused,
            paused.transferred_bytes,
            &|_, _, _| Ok(()),
            move |_, _| Box::pin(async move { Err(AppError::Network(expected.to_string())) }),
        )
        .await
        .expect_err("recoverable stream error must pause production receive path");
        assert_eq!(error.to_string(), expected);

        drop(storage);
        let restarted = Storage::open(&database).expect("reopen receiver storage");
        let blocked = restarted
            .get_transfer(&paused.transfer_id)
            .expect("reload stream pause after restart")
            .expect("stream pause remains after restart");
        assert_eq!(blocked.status, TransferStatus::Paused);
        assert_eq!(blocked.error.as_deref(), Some(expected));
        assert!(
            restarted
                .try_cancel_unclaimed_incoming_transfer(
                    &blocked.transfer_id,
                    &blocked.peer_id,
                    blocked.transfer_protocol,
                    "test cleanup",
                )
                .expect("stream pause must release receive claim")
        );

        drop(restarted);
        fs::remove_dir_all(directory).expect("remove stream pause fixture");
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
    async fn production_resumable_send_disconnect_pauses_the_claimed_database_row() {
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
        let error = send_resumable_transfer(
            ResumableStreamOpener::Scripted(Box::new(DisconnectingStream)),
            &transfer,
            &storage,
            &mut |_| {},
        )
        .await
        .expect_err("transport disconnect must stop production send");
        assert!(
            matches!(error, AppError::Io(ref error) if error.kind() == io::ErrorKind::BrokenPipe)
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

    #[tokio::test]
    async fn production_resumable_send_rejects_stale_ack_progress_after_cancel() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-production-send-cancel-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create fixture");
        let source = fixture.join("source.bin");
        fs::write(&source, b"payload").expect("write source");
        let manifest = build_manifest(&source, TRANSFER_CHUNK_BYTES).expect("build manifest");
        let mut transfer = outgoing_transfer("cancel-during-ack");
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
        let stream = CancellingAckStream {
            incoming: manifest.file_size.to_be_bytes().to_vec(),
            cursor: 0,
            storage: storage.clone(),
            transfer_id: transfer.transfer_id.clone(),
            peer_id: transfer.peer_id.clone(),
            cancelled: false,
        };

        let error = send_resumable_transfer(
            ResumableStreamOpener::Scripted(Box::new(stream)),
            &transfer,
            &storage,
            &mut |_| {},
        )
        .await
        .expect_err("stale ACK progress must lose its claim-scoped CAS");
        assert!(matches!(error, AppError::Storage(_)));
        let cancelled = storage
            .get_transfer(&transfer.transfer_id)
            .unwrap()
            .expect("cancelled transfer exists");
        assert_eq!(cancelled.status, TransferStatus::Cancelled);
        assert!(!cancelled.send_claimed);
        assert_eq!(cancelled.transferred_bytes, 0);

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }
}
