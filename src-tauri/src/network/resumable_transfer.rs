use std::{future::Future, path::Path};

use futures::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

use crate::{
    domain::{Direction, TransferRecord},
    error::AppError,
    receive_paths::resumable_partial_is_owned,
    storage::Storage,
    transfer_manifest::{
        TransferChunk, capture_source_snapshot, decode_sha256, expected_chunk_count,
        expected_chunk_length, manifest_root,
    },
    transfer_policy::{TRANSFER_CHUNK_BYTES, TransferProtocol},
};

const CHUNK_FRAME_HEADER_BYTES: usize = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkFrameHeader {
    pub index: u32,
    pub length: u32,
    pub sha256: [u8; 32],
}

impl ChunkFrameHeader {
    pub fn encode(self) -> [u8; CHUNK_FRAME_HEADER_BYTES] {
        let mut encoded = [0_u8; CHUNK_FRAME_HEADER_BYTES];
        encoded[..4].copy_from_slice(&self.index.to_be_bytes());
        encoded[4..8].copy_from_slice(&self.length.to_be_bytes());
        encoded[8..].copy_from_slice(&self.sha256);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, AppError> {
        let encoded: &[u8; CHUNK_FRAME_HEADER_BYTES] = encoded
            .try_into()
            .map_err(|_| AppError::InvalidInput("文件分块头长度无效".to_string()))?;
        Ok(Self {
            index: u32::from_be_bytes(encoded[..4].try_into().expect("four-byte index")),
            length: u32::from_be_bytes(encoded[4..8].try_into().expect("four-byte length")),
            sha256: encoded[8..]
                .try_into()
                .expect("32-byte SHA-256 in canonical header"),
        })
    }
}

pub(crate) fn validate_resume_offset(
    file_size: u64,
    chunk_size: u32,
    start_offset: u64,
) -> Result<u32, AppError> {
    if chunk_size != TRANSFER_CHUNK_BYTES {
        return Err(AppError::InvalidInput("恢复分块大小无效".to_string()));
    }
    if start_offset > file_size {
        return Err(AppError::InvalidInput("恢复偏移量超出文件范围".to_string()));
    }
    if start_offset != file_size && start_offset % u64::from(chunk_size) != 0 {
        return Err(AppError::InvalidInput(
            "恢复偏移量未对齐分块边界".to_string(),
        ));
    }
    let chunk_count = expected_chunk_count(file_size, chunk_size)?;
    let index = if start_offset == file_size {
        u64::from(chunk_count)
    } else {
        start_offset
            .checked_div(u64::from(chunk_size))
            .ok_or_else(|| AppError::InvalidInput("恢复分块大小无效".to_string()))?
    };
    u32::try_from(index).map_err(|_| AppError::InvalidInput("恢复分块索引溢出".to_string()))
}

pub(crate) async fn read_chunk_frame<S>(
    stream: &mut S,
    expected_index: u32,
    file_size: u64,
    chunk_size: u32,
) -> Result<(ChunkFrameHeader, Vec<u8>), AppError>
where
    S: AsyncRead + Unpin,
{
    let mut encoded_header = [0_u8; CHUNK_FRAME_HEADER_BYTES];
    stream.read_exact(&mut encoded_header).await?;
    let header = ChunkFrameHeader::decode(&encoded_header)?;
    validate_frame_header(&header, expected_index, file_size, chunk_size)?;

    let payload_len = usize::try_from(header.length)
        .map_err(|_| AppError::InvalidInput("文件分块长度超出当前平台限制".to_string()))?;
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(payload_len)
        .map_err(|_| AppError::InvalidInput("文件分块缓冲区无法分配".to_string()))?;
    payload.resize(payload_len, 0);
    stream.read_exact(&mut payload).await?;
    let actual_sha256: [u8; 32] = Sha256::digest(&payload).into();
    if actual_sha256 != header.sha256 {
        return Err(AppError::IntegrityFailure);
    }
    Ok((header, payload))
}

pub(crate) async fn write_chunk_frame<S>(
    stream: &mut S,
    header: &ChunkFrameHeader,
    payload: &[u8],
    file_size: u64,
    chunk_size: u32,
) -> Result<(), AppError>
where
    S: AsyncWrite + Unpin,
{
    validate_frame_header(header, header.index, file_size, chunk_size)?;
    if usize::try_from(header.length).ok() != Some(payload.len()) {
        return Err(AppError::InvalidInput(
            "文件分块头与负载长度不一致".to_string(),
        ));
    }
    let actual_sha256: [u8; 32] = Sha256::digest(payload).into();
    if actual_sha256 != header.sha256 {
        return Err(AppError::InvalidInput(
            "源文件分块与已保存哈希不一致，源文件可能已变化".to_string(),
        ));
    }
    stream.write_all(&header.encode()).await?;
    stream.write_all(payload).await?;
    Ok(())
}

fn validate_frame_header(
    header: &ChunkFrameHeader,
    expected_index: u32,
    file_size: u64,
    chunk_size: u32,
) -> Result<(), AppError> {
    if chunk_size != TRANSFER_CHUNK_BYTES {
        return Err(AppError::InvalidInput("文件分块大小无效".to_string()));
    }
    if header.index != expected_index {
        return Err(AppError::InvalidInput(
            "文件分块索引与已提交进度不一致".to_string(),
        ));
    }
    if header.length == 0 || header.length > chunk_size || header.length > TRANSFER_CHUNK_BYTES {
        return Err(AppError::InvalidInput("文件分块长度无效".to_string()));
    }
    let expected_length = expected_chunk_length(file_size, chunk_size, expected_index)?;
    if header.length != expected_length {
        return Err(AppError::InvalidInput(
            "文件分块长度与文件几何信息不一致".to_string(),
        ));
    }
    let offset = u64::from(expected_index)
        .checked_mul(u64::from(chunk_size))
        .ok_or_else(|| AppError::InvalidInput("文件分块偏移量溢出".to_string()))?;
    let end = offset
        .checked_add(u64::from(header.length))
        .ok_or_else(|| AppError::InvalidInput("文件分块结束偏移量溢出".to_string()))?;
    if end > file_size {
        return Err(AppError::InvalidInput("文件分块超出文件范围".to_string()));
    }
    Ok(())
}

#[allow(async_fn_in_trait)]
pub(crate) trait DurableChunkWriter {
    async fn seek_to(&mut self, offset: u64) -> std::io::Result<()>;
    async fn write_chunk(&mut self, bytes: &[u8]) -> std::io::Result<()>;
    async fn sync_data(&mut self) -> std::io::Result<()>;
}

impl DurableChunkWriter for tokio::fs::File {
    async fn seek_to(&mut self, offset: u64) -> std::io::Result<()> {
        self.seek(std::io::SeekFrom::Start(offset)).await?;
        Ok(())
    }

    async fn write_chunk(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, bytes).await
    }

    async fn sync_data(&mut self) -> std::io::Result<()> {
        tokio::fs::File::sync_data(self).await
    }
}

pub(crate) async fn open_resumable_partial(
    partial_path: &Path,
    committed_offset: u64,
) -> Result<tokio::fs::File, AppError> {
    let file = tokio::fs::OpenOptions::new()
        .create(false)
        .read(true)
        .write(true)
        .truncate(false)
        .open(partial_path)
        .await?;
    let length = file.metadata().await?.len();
    if length < committed_offset {
        return Err(AppError::InvalidInput(
            "部分文件短于已提交恢复偏移量，需要先执行恢复对账".to_string(),
        ));
    }
    if length > committed_offset {
        file.set_len(committed_offset).await?;
        file.sync_data().await?;
    }
    Ok(file)
}

pub(crate) async fn open_owned_resumable_partial(
    partial_path: &Path,
    destination: &Path,
    transfer_id: &str,
    reservation_token: &str,
    committed_offset: u64,
) -> Result<tokio::fs::File, AppError> {
    if !partial_path.parent().is_some_and(|parent| parent.is_dir()) {
        return Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "接收目录或磁盘当前不可用",
        )));
    }
    if !resumable_partial_is_owned(partial_path, destination, transfer_id, reservation_token)? {
        return Err(AppError::Permission(
            "可恢复部分文件缺少匹配的所有权凭据".to_string(),
        ));
    }
    open_resumable_partial(partial_path, committed_offset).await
}

pub(crate) async fn send_acknowledged_chunks<S, P>(
    stream: &mut S,
    source_path: &Path,
    transfer: &TransferRecord,
    chunks: &[TransferChunk],
    start_offset: u64,
    mut acknowledge_progress: P,
) -> Result<u64, AppError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    P: FnMut(u64) -> Result<(), AppError>,
{
    validate_outgoing_manifest(transfer, chunks)?;
    let next_index = validate_resume_offset(transfer.file_size, transfer.chunk_size, start_offset)?;
    verify_source_snapshot(source_path, transfer)?;

    let mut source = tokio::fs::File::open(source_path).await?;
    source.seek(std::io::SeekFrom::Start(start_offset)).await?;
    let mut committed_offset = start_offset;
    let start = usize::try_from(next_index)
        .map_err(|_| AppError::InvalidInput("恢复分块索引超出当前平台限制".to_string()))?;
    let has_remaining_chunks = start < chunks.len();
    for chunk in chunks
        .get(start..)
        .ok_or_else(|| AppError::InvalidInput("恢复分块索引超出已保存清单范围".to_string()))?
    {
        let payload = read_source_chunk(&mut source, chunk.length).await?;
        let actual_sha256: [u8; 32] = Sha256::digest(&payload).into();
        if actual_sha256 != chunk.sha256 {
            return Err(AppError::InvalidInput(
                "源文件分块与已保存哈希不一致，源文件可能已变化".to_string(),
            ));
        }
        let header = ChunkFrameHeader {
            index: chunk.index,
            length: chunk.length,
            sha256: chunk.sha256,
        };
        write_chunk_frame(
            stream,
            &header,
            &payload,
            transfer.file_size,
            transfer.chunk_size,
        )
        .await?;
        stream.flush().await?;

        let expected_offset = committed_offset
            .checked_add(u64::from(chunk.length))
            .ok_or_else(|| AppError::InvalidInput("接收确认偏移量溢出".to_string()))?;
        let mut encoded_acknowledgement = [0_u8; 8];
        stream.read_exact(&mut encoded_acknowledgement).await?;
        let acknowledged_offset = u64::from_be_bytes(encoded_acknowledgement);
        validate_acknowledgement(
            committed_offset,
            expected_offset,
            acknowledged_offset,
            transfer.file_size,
            transfer.chunk_size,
        )?;
        acknowledge_progress(acknowledged_offset)?;
        committed_offset = acknowledged_offset;
    }

    if committed_offset == transfer.file_size && !has_remaining_chunks {
        stream.flush().await?;
        let mut encoded_acknowledgement = [0_u8; 8];
        stream.read_exact(&mut encoded_acknowledgement).await?;
        let acknowledged_offset = u64::from_be_bytes(encoded_acknowledgement);
        validate_acknowledgement(
            committed_offset,
            committed_offset,
            acknowledged_offset,
            transfer.file_size,
            transfer.chunk_size,
        )?;
    }
    Ok(committed_offset)
}

pub(crate) async fn receive_acknowledged_chunks<S, W, C, V, F, Fut>(
    stream: &mut S,
    mut destination: W,
    transfer: &TransferRecord,
    start_offset: u64,
    mut commit_chunk: C,
    verify_manifest: V,
    finalize: F,
) -> Result<u64, AppError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    W: DurableChunkWriter,
    C: FnMut(&TransferChunk, u64) -> Result<bool, AppError>,
    V: FnOnce() -> Result<(), AppError>,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), AppError>>,
{
    if transfer.direction != Direction::Incoming
        || transfer.transfer_protocol != TransferProtocol::ResumableV2 as u8
    {
        return Err(AppError::InvalidInput(
            "接收循环只能处理传入的文件协议 v2 记录".to_string(),
        ));
    }
    validate_incoming_metadata(transfer)?;
    if transfer.transferred_bytes != start_offset {
        return Err(AppError::InvalidInput(
            "恢复偏移量与接收方已提交进度不一致".to_string(),
        ));
    }
    let mut expected_index =
        validate_resume_offset(transfer.file_size, transfer.chunk_size, start_offset)?;
    destination.seek_to(start_offset).await?;
    let mut committed_offset = start_offset;

    while committed_offset < transfer.file_size {
        let (header, payload) = read_chunk_frame(
            stream,
            expected_index,
            transfer.file_size,
            transfer.chunk_size,
        )
        .await?;
        destination.write_chunk(&payload).await?;
        destination.sync_data().await?;
        let next_offset = committed_offset
            .checked_add(u64::from(header.length))
            .ok_or_else(|| AppError::InvalidInput("已接收字节数溢出".to_string()))?;
        let chunk = TransferChunk {
            index: header.index,
            length: header.length,
            sha256: header.sha256,
        };
        if !commit_chunk(&chunk, next_offset)? {
            return Err(AppError::Storage(
                "分块已写入磁盘，但接收进度未能原子提交".to_string(),
            ));
        }
        committed_offset = next_offset;
        if committed_offset < transfer.file_size {
            stream.write_all(&committed_offset.to_be_bytes()).await?;
            stream.flush().await?;
            expected_index = expected_index
                .checked_add(1)
                .ok_or_else(|| AppError::InvalidInput("文件分块索引溢出".to_string()))?;
        }
    }

    drop(destination);
    verify_manifest()?;
    finalize().await?;
    stream.write_all(&committed_offset.to_be_bytes()).await?;
    stream.flush().await?;
    Ok(committed_offset)
}

fn validate_outgoing_manifest(
    transfer: &TransferRecord,
    chunks: &[TransferChunk],
) -> Result<(), AppError> {
    if transfer.direction != Direction::Outgoing
        || transfer.transfer_protocol != TransferProtocol::ResumableV2 as u8
        || usize::try_from(transfer.chunk_count).ok() != Some(chunks.len())
    {
        return Err(AppError::InvalidInput(
            "发送循环只能处理具有完整清单的文件协议 v2 记录".to_string(),
        ));
    }
    validate_resume_offset(transfer.file_size, transfer.chunk_size, transfer.file_size)?;
    for (expected_index, chunk) in chunks.iter().enumerate() {
        let expected_index = u32::try_from(expected_index)
            .map_err(|_| AppError::InvalidInput("发送分块索引溢出".to_string()))?;
        if chunk.index != expected_index
            || chunk.length
                != expected_chunk_length(transfer.file_size, transfer.chunk_size, expected_index)?
        {
            return Err(AppError::InvalidInput(
                "已保存发送清单的分块几何信息无效".to_string(),
            ));
        }
    }
    let expected_manifest = decode_sha256(
        transfer
            .manifest_sha256
            .as_deref()
            .ok_or_else(|| AppError::InvalidInput("缺少发送分块清单哈希".to_string()))?,
    )?;
    if manifest_root(chunks) != expected_manifest {
        return Err(AppError::IntegrityFailure);
    }
    Ok(())
}

fn validate_incoming_metadata(transfer: &TransferRecord) -> Result<(), AppError> {
    let expected_count = expected_chunk_count(transfer.file_size, transfer.chunk_size)?;
    if transfer.chunk_count != expected_count {
        return Err(AppError::InvalidInput(
            "接收文件的分块数量与文件大小不一致".to_string(),
        ));
    }
    let expected_manifest = decode_sha256(
        transfer
            .manifest_sha256
            .as_deref()
            .ok_or_else(|| AppError::InvalidInput("缺少接收分块清单哈希".to_string()))?,
    )?;
    if expected_count == 0 && expected_manifest != manifest_root(&[]) {
        return Err(AppError::IntegrityFailure);
    }
    Ok(())
}

pub(crate) fn verify_committed_manifest(
    storage: &Storage,
    accepted: &TransferRecord,
) -> Result<(), AppError> {
    let current = storage
        .get_transfer(&accepted.transfer_id)?
        .ok_or_else(|| AppError::Storage("最终清单校验时找不到接收传输记录".to_string()))?;
    if current.direction != Direction::Incoming
        || current.transfer_protocol != TransferProtocol::ResumableV2 as u8
        || current.file_size != accepted.file_size
        || current.chunk_size != accepted.chunk_size
        || current.chunk_count != accepted.chunk_count
        || current.transferred_bytes != accepted.file_size
    {
        return Err(AppError::InvalidInput(
            "最终清单校验发现接收传输几何信息不一致".to_string(),
        ));
    }
    let chunks = storage.list_transfer_chunks(&accepted.transfer_id)?;
    if usize::try_from(accepted.chunk_count).ok() != Some(chunks.len()) {
        return Err(AppError::IntegrityFailure);
    }
    let mut total_bytes = 0_u64;
    for (expected_index, chunk) in chunks.iter().enumerate() {
        let expected_index = u32::try_from(expected_index)
            .map_err(|_| AppError::InvalidInput("最终清单分块索引溢出".to_string()))?;
        if chunk.index != expected_index
            || chunk.length
                != expected_chunk_length(accepted.file_size, accepted.chunk_size, expected_index)?
        {
            return Err(AppError::IntegrityFailure);
        }
        total_bytes = total_bytes
            .checked_add(u64::from(chunk.length))
            .ok_or_else(|| AppError::InvalidInput("最终清单字节数溢出".to_string()))?;
    }
    let expected_root = decode_sha256(
        accepted
            .manifest_sha256
            .as_deref()
            .ok_or_else(|| AppError::InvalidInput("缺少接收分块清单哈希".to_string()))?,
    )?;
    if total_bytes != accepted.file_size || manifest_root(&chunks) != expected_root {
        return Err(AppError::IntegrityFailure);
    }
    Ok(())
}

fn verify_source_snapshot(source_path: &Path, transfer: &TransferRecord) -> Result<(), AppError> {
    let expected_modified_ns = transfer
        .source_modified_ns
        .ok_or_else(|| AppError::InvalidInput("缺少源文件修改时间快照".to_string()))?;
    let current = capture_source_snapshot(source_path)?;
    if current.file_size != transfer.file_size || current.source_modified_ns != expected_modified_ns
    {
        return Err(AppError::InvalidInput(
            "源文件在传输前发生了变化，请重新发送".to_string(),
        ));
    }
    Ok(())
}

fn validate_acknowledgement(
    previous_offset: u64,
    expected_offset: u64,
    acknowledged_offset: u64,
    file_size: u64,
    chunk_size: u32,
) -> Result<(), AppError> {
    if acknowledged_offset != expected_offset
        || acknowledged_offset < previous_offset
        || acknowledged_offset > file_size
    {
        return Err(AppError::InvalidInput(
            "接收方返回了无效的已提交偏移量".to_string(),
        ));
    }
    validate_resume_offset(file_size, chunk_size, acknowledged_offset)?;
    Ok(())
}

pub(crate) fn is_recoverable_receive_error(error: &AppError) -> bool {
    matches!(error, AppError::Io(error) if error.kind() == std::io::ErrorKind::NotFound)
        || is_recoverable_stream_error(error)
}

pub(crate) fn is_recoverable_send_error(error: &AppError) -> bool {
    is_recoverable_stream_error(error)
}

fn is_recoverable_stream_error(error: &AppError) -> bool {
    match error {
        AppError::Network(_) | AppError::OfflinePeer => true,
        AppError::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::TimedOut
        ),
        AppError::InvalidInput(_)
        | AppError::Storage(_)
        | AppError::Identity(_)
        | AppError::Permission(_)
        | AppError::NotFriend
        | AppError::IncompatibleProtocol
        | AppError::IntegrityFailure => false,
    }
}

async fn read_source_chunk(
    source: &mut tokio::fs::File,
    chunk_length: u32,
) -> Result<Vec<u8>, AppError> {
    let payload_len = usize::try_from(chunk_length)
        .map_err(|_| AppError::InvalidInput("源文件分块长度超出当前平台限制".to_string()))?;
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(payload_len)
        .map_err(|_| AppError::InvalidInput("源文件分块缓冲区无法分配".to_string()))?;
    payload.resize(payload_len, 0);
    match source.read_exact(&mut payload).await {
        Ok(_) => Ok(payload),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Err(
            AppError::InvalidInput("源文件在传输期间被截断或替换".to_string()),
        ),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        path::{Path, PathBuf},
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    };

    use futures::io::{AsyncRead, AsyncWrite};
    use sha2::{Digest, Sha256};
    use tokio::io::{
        AsyncRead as TokioAsyncRead, AsyncSeekExt as _, AsyncWrite as TokioAsyncWrite,
        AsyncWriteExt as _, ReadBuf,
    };

    use super::{
        ChunkFrameHeader, DurableChunkWriter, is_recoverable_receive_error,
        is_recoverable_send_error, open_owned_resumable_partial, open_resumable_partial,
        read_chunk_frame, read_source_chunk, receive_acknowledged_chunks, send_acknowledged_chunks,
        validate_resume_offset, verify_committed_manifest, write_chunk_frame,
    };
    use crate::{
        domain::{Direction, TransferKind, TransferRecord, TransferStatus},
        error::AppError,
        receive_paths::{reserve_receive_path, reserve_resumable_partial},
        storage::Storage,
        transfer_manifest::{TransferManifest, build_manifest, manifest_root},
        transfer_policy::{TRANSFER_CHUNK_BYTES, TransferProtocol},
    };

    #[derive(Default)]
    struct ScriptedStream {
        incoming: std::io::Cursor<Vec<u8>>,
        written: Vec<u8>,
        write_limit: Option<usize>,
    }

    impl ScriptedStream {
        fn with_incoming(incoming: Vec<u8>) -> Self {
            Self {
                incoming: std::io::Cursor::new(incoming),
                written: Vec::new(),
                write_limit: None,
            }
        }

        fn with_incoming_and_write_limit(incoming: Vec<u8>, write_limit: usize) -> Self {
            Self {
                incoming: std::io::Cursor::new(incoming),
                written: Vec::new(),
                write_limit: Some(write_limit),
            }
        }
    }

    impl AsyncRead for ScriptedStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(std::io::Read::read(&mut self.incoming, buffer))
        }
    }

    impl AsyncWrite for ScriptedStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.write_limit.is_some_and(|limit| {
                self.written
                    .len()
                    .checked_add(buffer.len())
                    .is_none_or(|next| next > limit)
            }) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "scripted disconnect",
                )));
            }
            self.written.extend_from_slice(buffer);
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct FuturesDuplex {
        inner: tokio::io::DuplexStream,
        write_limit: Option<usize>,
        written: usize,
    }

    impl FuturesDuplex {
        fn new(inner: tokio::io::DuplexStream) -> Self {
            Self {
                inner,
                write_limit: None,
                written: 0,
            }
        }

        fn with_write_limit(inner: tokio::io::DuplexStream, write_limit: usize) -> Self {
            Self {
                inner,
                write_limit: Some(write_limit),
                written: 0,
            }
        }
    }

    impl AsyncRead for FuturesDuplex {
        fn poll_read(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let mut read_buffer = ReadBuf::new(buffer);
            match TokioAsyncRead::poll_read(Pin::new(&mut self.inner), context, &mut read_buffer) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buffer.filled().len())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl AsyncWrite for FuturesDuplex {
        fn poll_write(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            let allowed = match self.write_limit {
                Some(limit) => limit.saturating_sub(self.written),
                None => buffer.len(),
            };
            if allowed == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "test duplex disconnected",
                )));
            }
            let allowed_buffer = &buffer[..buffer.len().min(allowed)];
            match TokioAsyncWrite::poll_write(Pin::new(&mut self.inner), context, allowed_buffer) {
                Poll::Ready(Ok(written)) => {
                    self.written = self
                        .written
                        .checked_add(written)
                        .expect("test duplex byte count fits");
                    Poll::Ready(Ok(written))
                }
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        }

        fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            TokioAsyncWrite::poll_flush(Pin::new(&mut self.inner), context)
        }

        fn poll_close(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            TokioAsyncWrite::poll_shutdown(Pin::new(&mut self.inner), context)
        }
    }

    fn sha256(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }

    fn encoded_frame(index: u32, payload: &[u8], advertised_hash: [u8; 32]) -> Vec<u8> {
        let mut encoded = ChunkFrameHeader {
            index,
            length: u32::try_from(payload.len()).expect("fixture length fits u32"),
            sha256: advertised_hash,
        }
        .encode()
        .to_vec();
        encoded.extend_from_slice(payload);
        encoded
    }

    fn fixture_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "weline-localnet-resumable-{name}-{}",
            uuid::Uuid::now_v7()
        ))
    }

    fn transfer_record(
        direction: Direction,
        source_path: Option<&Path>,
        destination_path: Option<&Path>,
        partial_path: Option<&Path>,
        manifest: &TransferManifest,
    ) -> TransferRecord {
        TransferRecord {
            transfer_id: "01993c20-b0f0-7fb0-9d4d-0ab6ec9bb331".to_string(),
            peer_id: "peer-one".to_string(),
            direction,
            kind: TransferKind::File,
            file_name: "payload.bin".to_string(),
            file_size: manifest.file_size,
            mime_type: "application/octet-stream".to_string(),
            sha256: hex::encode(manifest.file_sha256),
            local_path: source_path
                .or(destination_path)
                .map(|path| path.to_string_lossy().into_owned()),
            destination_reserved: false,
            reservation_token: None,
            transfer_protocol: TransferProtocol::ResumableV2 as u8,
            chunk_size: TRANSFER_CHUNK_BYTES,
            chunk_count: u32::try_from(manifest.chunks.len()).expect("fixture chunk count fits"),
            manifest_sha256: Some(hex::encode(manifest.manifest_sha256)),
            partial_path: partial_path.map(|path| path.to_string_lossy().into_owned()),
            source_modified_ns: (direction == Direction::Outgoing)
                .then_some(manifest.source_modified_ns),
            send_claimed: false,
            transferred_bytes: 0,
            status: TransferStatus::Transferring,
            error: None,
            created_at: "2026-08-24T00:00:00.000Z".to_string(),
            updated_at: "2026-08-24T00:00:00.000Z".to_string(),
        }
    }

    fn small_incoming_transfer(payload: &[u8], advertised_hash: [u8; 32]) -> TransferRecord {
        let chunk = crate::transfer_manifest::TransferChunk {
            index: 0,
            length: u32::try_from(payload.len()).expect("fixture length fits"),
            sha256: advertised_hash,
        };
        TransferRecord {
            transfer_id: "small-transfer".to_string(),
            peer_id: "peer-one".to_string(),
            direction: Direction::Incoming,
            kind: TransferKind::File,
            file_name: "small.bin".to_string(),
            file_size: payload.len() as u64,
            mime_type: "application/octet-stream".to_string(),
            sha256: hex::encode(sha256(payload)),
            local_path: None,
            destination_reserved: false,
            reservation_token: None,
            transfer_protocol: TransferProtocol::ResumableV2 as u8,
            chunk_size: TRANSFER_CHUNK_BYTES,
            chunk_count: 1,
            manifest_sha256: Some(hex::encode(manifest_root(&[chunk]))),
            partial_path: None,
            source_modified_ns: None,
            send_claimed: false,
            transferred_bytes: 0,
            status: TransferStatus::Transferring,
            error: None,
            created_at: "2026-08-24T00:00:00.000Z".to_string(),
            updated_at: "2026-08-24T00:00:00.000Z".to_string(),
        }
    }

    struct TrackingFile {
        file: tokio::fs::File,
        current_offset: u64,
        write_offsets: Arc<Mutex<Vec<u64>>>,
        fail_sync: bool,
    }

    impl TrackingFile {
        async fn open(path: &Path, write_offsets: Arc<Mutex<Vec<u64>>>, fail_sync: bool) -> Self {
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(path)
                .await
                .expect("open tracking file");
            Self {
                file,
                current_offset: 0,
                write_offsets,
                fail_sync,
            }
        }
    }

    impl DurableChunkWriter for TrackingFile {
        async fn seek_to(&mut self, offset: u64) -> io::Result<()> {
            self.file.seek(std::io::SeekFrom::Start(offset)).await?;
            self.current_offset = offset;
            Ok(())
        }

        async fn write_chunk(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.write_offsets
                .lock()
                .expect("lock write offsets")
                .push(self.current_offset);
            self.file.write_all(bytes).await?;
            self.current_offset = self
                .current_offset
                .checked_add(bytes.len() as u64)
                .expect("fixture offset fits");
            Ok(())
        }

        async fn sync_data(&mut self) -> io::Result<()> {
            if self.fail_sync {
                return Err(io::Error::other("scripted sync failure"));
            }
            self.file.sync_data().await
        }
    }

    fn acknowledgement_bytes(offsets: &[u64]) -> Vec<u8> {
        offsets
            .iter()
            .flat_map(|offset| offset.to_be_bytes())
            .collect()
    }

    #[test]
    fn transport_disconnect_errors_are_recoverable_for_resumable_transfers() {
        assert!(is_recoverable_receive_error(&AppError::Network(
            "connection closed".to_string()
        )));
        assert!(is_recoverable_receive_error(&AppError::OfflinePeer));
        for kind in [
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::NotConnected,
            io::ErrorKind::TimedOut,
        ] {
            assert!(is_recoverable_receive_error(&AppError::Io(io::Error::new(
                kind,
                "transport stopped"
            ))));
        }
    }

    #[test]
    fn integrity_source_identity_and_local_io_errors_are_terminal() {
        assert!(!is_recoverable_receive_error(&AppError::IntegrityFailure));
        assert!(!is_recoverable_receive_error(&AppError::InvalidInput(
            "源文件在传输前发生了变化".to_string()
        )));
        assert!(!is_recoverable_receive_error(&AppError::Permission(
            "peer identity mismatch".to_string()
        )));
        assert!(!is_recoverable_send_error(&AppError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            "source missing"
        ))));
        assert!(!is_recoverable_receive_error(&AppError::Io(
            io::Error::new(io::ErrorKind::InvalidData, "invalid local data")
        )));
    }

    #[tokio::test]
    async fn missing_receive_media_is_recoverable_and_never_recreated() {
        let fixture = fixture_directory("missing-receive-media");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let destination = fixture.join("report.bin");
        reserve_receive_path(&destination, "transfer-one", "token-one")
            .expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, "transfer-one", "token-one")
            .expect("reserve partial");
        std::fs::remove_dir_all(&fixture).expect("simulate removed destination media");

        let error =
            open_owned_resumable_partial(&partial, &destination, "transfer-one", "token-one", 0)
                .await
                .expect_err("missing media must pause rather than recreate");

        assert!(is_recoverable_receive_error(&error));
        assert!(!fixture.exists());
    }

    #[tokio::test]
    async fn production_source_eof_is_terminal_while_network_eof_is_recoverable() {
        let fixture = fixture_directory("source-eof-classification");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let source = fixture.join("source.bin");
        std::fs::write(&source, b"short").expect("write truncated source");
        let mut source_file = tokio::fs::File::open(&source)
            .await
            .expect("open truncated source");

        let source_error = read_source_chunk(&mut source_file, 8)
            .await
            .expect_err("short source must fail exact production read");

        assert_eq!(source_error.code(), "invalid_input");
        assert!(!is_recoverable_send_error(&source_error));
        assert!(is_recoverable_send_error(&AppError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "remote acknowledgement stream ended",
        ))));
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[test]
    fn chunk_header_uses_canonical_40_byte_big_endian_encoding() {
        let header = ChunkFrameHeader {
            index: 0x0102_0304,
            length: 0x0506_0708,
            sha256: [0xab; 32],
        };

        let encoded = header.encode();

        assert_eq!(encoded.len(), 40);
        assert_eq!(&encoded[..4], &[1, 2, 3, 4]);
        assert_eq!(&encoded[4..8], &[5, 6, 7, 8]);
        assert_eq!(&encoded[8..], &[0xab; 32]);
        assert_eq!(ChunkFrameHeader::decode(&encoded).unwrap(), header);
    }

    #[test]
    fn chunk_header_rejects_truncated_or_extended_encodings() {
        assert!(ChunkFrameHeader::decode(&[0_u8; 39]).is_err());
        assert!(ChunkFrameHeader::decode(&[0_u8; 41]).is_err());
    }

    #[tokio::test]
    async fn bounded_reader_accepts_a_final_short_chunk() {
        let payload = b"e";
        let mut stream = ScriptedStream::with_incoming(encoded_frame(1, payload, sha256(payload)));

        let (header, bytes) = read_chunk_frame(
            &mut stream,
            1,
            u64::from(TRANSFER_CHUNK_BYTES) + 1,
            TRANSFER_CHUNK_BYTES,
        )
        .await
        .expect("read final short frame");

        assert_eq!(header.index, 1);
        assert_eq!(header.length, 1);
        assert_eq!(bytes, payload);
    }

    #[tokio::test]
    async fn protocol_v2_reader_rejects_a_smaller_consistent_chunk_size() {
        let payload = b"tiny";
        let mut stream = ScriptedStream::with_incoming(encoded_frame(0, payload, sha256(payload)));

        let result =
            read_chunk_frame(&mut stream, 0, payload.len() as u64, payload.len() as u32).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn bounded_reader_rejects_a_payload_hash_mismatch() {
        let mut stream = ScriptedStream::with_incoming(encoded_frame(0, b"abcd", sha256(b"wxyz")));

        let error = read_chunk_frame(&mut stream, 0, 4, TRANSFER_CHUNK_BYTES)
            .await
            .expect_err("corrupted payload must fail");

        assert_eq!(error.code(), "integrity_failure");
    }

    #[tokio::test]
    async fn bounded_reader_rejects_wrong_or_reordered_indexes() {
        let frame = encoded_frame(1, b"abcd", sha256(b"abcd"));

        assert!(
            read_chunk_frame(
                &mut ScriptedStream::with_incoming(frame.clone()),
                0,
                u64::from(TRANSFER_CHUNK_BYTES) * 2,
                TRANSFER_CHUNK_BYTES,
            )
            .await
            .is_err()
        );
        assert!(
            read_chunk_frame(
                &mut ScriptedStream::with_incoming(frame),
                2,
                u64::from(TRANSFER_CHUNK_BYTES) * 3,
                TRANSFER_CHUNK_BYTES,
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn bounded_reader_rejects_truncated_headers_and_payloads() {
        let complete = encoded_frame(0, b"abcd", sha256(b"abcd"));

        let header_error = read_chunk_frame(
            &mut ScriptedStream::with_incoming(complete[..39].to_vec()),
            0,
            4,
            TRANSFER_CHUNK_BYTES,
        )
        .await
        .expect_err("truncated header must fail");
        let payload_error = read_chunk_frame(
            &mut ScriptedStream::with_incoming(complete[..42].to_vec()),
            0,
            4,
            TRANSFER_CHUNK_BYTES,
        )
        .await
        .expect_err("truncated payload must fail");

        assert!(matches!(header_error, crate::error::AppError::Io(_)));
        assert!(matches!(payload_error, crate::error::AppError::Io(_)));
    }

    #[tokio::test]
    async fn bounded_reader_rejects_zero_length_non_final_and_oversized_frames_before_payload_read()
    {
        let zero = ChunkFrameHeader {
            index: 0,
            length: 0,
            sha256: sha256(&[]),
        }
        .encode()
        .to_vec();
        let oversized = ChunkFrameHeader {
            index: 0,
            length: TRANSFER_CHUNK_BYTES + 1,
            sha256: sha256(b"abcde"),
        }
        .encode()
        .to_vec();

        assert!(
            read_chunk_frame(
                &mut ScriptedStream::with_incoming(zero),
                0,
                u64::from(TRANSFER_CHUNK_BYTES) * 2,
                TRANSFER_CHUNK_BYTES,
            )
            .await
            .is_err()
        );
        assert!(
            read_chunk_frame(
                &mut ScriptedStream::with_incoming(oversized),
                0,
                u64::from(TRANSFER_CHUNK_BYTES),
                TRANSFER_CHUNK_BYTES,
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn bounded_writer_rejects_header_payload_length_mismatch() {
        let mut stream = ScriptedStream::default();
        let header = ChunkFrameHeader {
            index: 0,
            length: 3,
            sha256: sha256(b"abc"),
        };

        assert!(
            write_chunk_frame(&mut stream, &header, b"ab", 3, TRANSFER_CHUNK_BYTES)
                .await
                .is_err()
        );
        assert!(stream.written.is_empty());
    }

    #[test]
    fn resume_offsets_accept_boundaries_and_complete_size() {
        let file_size = u64::from(TRANSFER_CHUNK_BYTES) * 2 + 2;
        assert_eq!(
            validate_resume_offset(file_size, TRANSFER_CHUNK_BYTES, 0).unwrap(),
            0
        );
        assert_eq!(
            validate_resume_offset(
                file_size,
                TRANSFER_CHUNK_BYTES,
                u64::from(TRANSFER_CHUNK_BYTES) * 2,
            )
            .unwrap(),
            2
        );
        assert_eq!(
            validate_resume_offset(file_size, TRANSFER_CHUNK_BYTES, file_size).unwrap(),
            3
        );
    }

    #[test]
    fn resume_offsets_reject_unaligned_out_of_range_and_index_overflow() {
        let file_size = u64::from(TRANSFER_CHUNK_BYTES) * 2 + 2;
        assert!(
            validate_resume_offset(
                file_size,
                TRANSFER_CHUNK_BYTES,
                u64::from(TRANSFER_CHUNK_BYTES) + 1,
            )
            .is_err()
        );
        assert!(validate_resume_offset(file_size, TRANSFER_CHUNK_BYTES, file_size + 1).is_err());
        assert!(validate_resume_offset(u64::MAX, TRANSFER_CHUNK_BYTES, u64::MAX - 1).is_err());
        assert!(validate_resume_offset(u64::MAX, TRANSFER_CHUNK_BYTES, 0).is_err());
        assert!(
            validate_resume_offset(u64::from(TRANSFER_CHUNK_BYTES), TRANSFER_CHUNK_BYTES / 2, 0,)
                .is_err()
        );
        assert!(validate_resume_offset(10, 0, 0).is_err());
    }

    #[tokio::test]
    async fn empty_receiver_rejects_a_manifest_that_is_not_the_empty_root() {
        let fixture = fixture_directory("empty-root");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let partial = fixture.join("payload.part");
        let writer = TrackingFile::open(&partial, Arc::new(Mutex::new(Vec::new())), false).await;
        let mut transfer = small_incoming_transfer(b"x", sha256(b"x"));
        transfer.file_size = 0;
        transfer.chunk_size = TRANSFER_CHUNK_BYTES;
        transfer.chunk_count = 0;
        transfer.manifest_sha256 = Some("0".repeat(64));
        let mut stream = ScriptedStream::default();
        let finalized = Arc::new(Mutex::new(false));
        let finalized_for_callback = finalized.clone();

        let result = receive_acknowledged_chunks(
            &mut stream,
            writer,
            &transfer,
            0,
            |_, _| Ok(true),
            || Ok(()),
            move || async move {
                *finalized_for_callback.lock().expect("lock finalized flag") = true;
                Ok(())
            },
        )
        .await;

        assert!(result.is_err());
        assert!(!*finalized.lock().expect("lock finalized flag"));
        assert!(stream.written.is_empty());
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[tokio::test]
    async fn sender_at_complete_offset_still_requires_the_final_acknowledgement() {
        let fixture = fixture_directory("complete-offset-ack");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let source = fixture.join("source.bin");
        std::fs::write(&source, b"done").expect("write source");
        let manifest = build_manifest(&source, 4).expect("build manifest");
        let transfer = transfer_record(Direction::Outgoing, Some(&source), None, None, &manifest);
        let mut stream = ScriptedStream::default();

        let result = send_acknowledged_chunks(
            &mut stream,
            &source,
            &transfer,
            &manifest.chunks,
            transfer.file_size,
            |_| Ok(()),
        )
        .await;

        assert!(matches!(result, Err(AppError::Io(_))));
        assert!(stream.written.is_empty());
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[tokio::test]
    async fn receiver_never_writes_or_acknowledges_a_corrupt_chunk() {
        let fixture = fixture_directory("corrupt");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let partial = fixture.join("payload.part");
        let write_offsets = Arc::new(Mutex::new(Vec::new()));
        let writer = TrackingFile::open(&partial, write_offsets.clone(), false).await;
        let transfer = small_incoming_transfer(b"good", sha256(b"good"));
        let mut stream = ScriptedStream::with_incoming(encoded_frame(0, b"evil", sha256(b"good")));
        let mut commit_called = false;

        let error = receive_acknowledged_chunks(
            &mut stream,
            writer,
            &transfer,
            0,
            |_, _| {
                commit_called = true;
                Ok(true)
            },
            || Ok(()),
            || async { Ok(()) },
        )
        .await
        .expect_err("corrupt chunk must fail");

        assert_eq!(error.code(), "integrity_failure");
        assert!(!commit_called);
        assert!(write_offsets.lock().expect("lock offsets").is_empty());
        assert!(stream.written.is_empty());
        assert_eq!(
            std::fs::metadata(&partial).expect("partial metadata").len(),
            0
        );
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[tokio::test]
    async fn receiver_does_not_acknowledge_a_sync_or_commit_failure() {
        for fail_sync in [true, false] {
            let fixture = fixture_directory(if fail_sync {
                "sync-failure"
            } else {
                "commit-failure"
            });
            std::fs::create_dir_all(&fixture).expect("create fixture");
            let partial = fixture.join("payload.part");
            let write_offsets = Arc::new(Mutex::new(Vec::new()));
            let writer = TrackingFile::open(&partial, write_offsets, fail_sync).await;
            let payload = b"good";
            let transfer = small_incoming_transfer(payload, sha256(payload));
            let mut stream =
                ScriptedStream::with_incoming(encoded_frame(0, payload, sha256(payload)));
            let mut commit_called = false;

            let result = receive_acknowledged_chunks(
                &mut stream,
                writer,
                &transfer,
                0,
                |_, _| {
                    commit_called = true;
                    Ok(false)
                },
                || Ok(()),
                || async { Ok(()) },
            )
            .await;

            assert!(result.is_err());
            assert_eq!(commit_called, !fail_sync);
            assert!(stream.written.is_empty());
            std::fs::remove_dir_all(fixture).expect("remove fixture");
        }
    }

    #[tokio::test]
    async fn receiver_sends_final_ack_only_after_finalization() {
        let fixture = fixture_directory("finalize-failure");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let partial = fixture.join("payload.part");
        let writer = TrackingFile::open(&partial, Arc::new(Mutex::new(Vec::new())), false).await;
        let payload = b"good";
        let transfer = small_incoming_transfer(payload, sha256(payload));
        let mut stream = ScriptedStream::with_incoming(encoded_frame(0, payload, sha256(payload)));

        let error = receive_acknowledged_chunks(
            &mut stream,
            writer,
            &transfer,
            0,
            |_, _| Ok(true),
            || Ok(()),
            || async { Err(AppError::Storage("scripted finalize failure".to_string())) },
        )
        .await
        .expect_err("finalization must gate final ack");

        assert_eq!(error.code(), "storage_error");
        assert!(stream.written.is_empty());
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[tokio::test]
    async fn receiver_rejects_a_self_consistent_substituted_non_empty_manifest_before_finalize() {
        let fixture = fixture_directory("substituted-manifest");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let partial = fixture.join("payload.part");
        let writer = TrackingFile::open(&partial, Arc::new(Mutex::new(Vec::new())), false).await;
        let accepted_payload = b"good";
        let substituted_payload = b"evil";
        let accepted_hash = sha256(accepted_payload);
        let substituted_hash = sha256(substituted_payload);
        let transfer = small_incoming_transfer(accepted_payload, accepted_hash);
        let mut stream =
            ScriptedStream::with_incoming(encoded_frame(0, substituted_payload, substituted_hash));
        let committed = Arc::new(Mutex::new(Vec::new()));
        let committed_for_commit = committed.clone();
        let committed_for_verify = committed.clone();
        let expected_root = crate::transfer_manifest::decode_sha256(
            transfer
                .manifest_sha256
                .as_deref()
                .expect("accepted manifest root"),
        )
        .expect("decode accepted root");
        let finalized = Arc::new(Mutex::new(false));
        let finalized_for_callback = finalized.clone();

        let error = receive_acknowledged_chunks(
            &mut stream,
            writer,
            &transfer,
            0,
            move |chunk, _| {
                committed_for_commit
                    .lock()
                    .expect("lock committed chunks")
                    .push(chunk.clone());
                Ok(true)
            },
            move || {
                let actual_root =
                    manifest_root(&committed_for_verify.lock().expect("lock committed chunks"));
                if actual_root != expected_root {
                    return Err(AppError::IntegrityFailure);
                }
                Ok(())
            },
            move || async move {
                *finalized_for_callback.lock().expect("lock finalized flag") = true;
                Ok(())
            },
        )
        .await
        .expect_err("substituted manifest must fail before finalization");

        assert_eq!(error.code(), "integrity_failure");
        assert!(!*finalized.lock().expect("lock finalized flag"));
        assert!(stream.written.is_empty());
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[test]
    fn production_manifest_verifier_loads_ordered_records_and_rejects_a_substitution() {
        let fixture = fixture_directory("persisted-manifest-verify");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let accepted_payload = b"good";
        let substituted_payload = b"evil";
        let transfer = small_incoming_transfer(accepted_payload, sha256(accepted_payload));
        let database = fixture.join("receiver.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        storage
            .upsert_transfer(&transfer)
            .expect("persist accepted transfer");
        let connection = rusqlite::Connection::open(&database).expect("open storage connection");
        connection
            .execute(
                "INSERT INTO transfer_chunks (transfer_id, chunk_index, chunk_length, sha256)
                 VALUES (?1, 0, 4, ?2)",
                rusqlite::params![transfer.transfer_id, sha256(substituted_payload).as_slice()],
            )
            .expect("insert substituted committed record");
        connection
            .execute(
                "UPDATE transfers SET transferred_bytes = 4 WHERE transfer_id = ?1",
                [&transfer.transfer_id],
            )
            .expect("advance simulated committed progress");
        drop(connection);

        assert_eq!(
            verify_committed_manifest(&storage, &transfer)
                .expect_err("substituted persisted manifest must fail")
                .code(),
            "integrity_failure"
        );

        let connection = rusqlite::Connection::open(&database).expect("open storage connection");
        connection
            .execute(
                "UPDATE transfer_chunks SET sha256 = ?2 WHERE transfer_id = ?1",
                rusqlite::params![transfer.transfer_id, sha256(accepted_payload).as_slice()],
            )
            .expect("restore accepted committed record");
        drop(connection);
        verify_committed_manifest(&storage, &transfer).expect("accepted ordered manifest succeeds");

        drop(storage);
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[tokio::test]
    async fn reopening_truncates_uncommitted_tail_and_resumes_without_touching_prefix() {
        let fixture = fixture_directory("truncate-tail-resume");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let source = fixture.join("source.bin");
        let partial = fixture.join("payload.part");
        let chunk_len = usize::try_from(TRANSFER_CHUNK_BYTES).expect("chunk size fits usize");
        let prefix = vec![0x31; chunk_len];
        let suffix = b"verified-suffix";
        let mut expected = prefix.clone();
        expected.extend_from_slice(suffix);
        std::fs::write(&source, &expected).expect("write source");
        let manifest = build_manifest(&source, TRANSFER_CHUNK_BYTES).expect("build manifest");
        let transfer = transfer_record(
            Direction::Incoming,
            None,
            Some(&fixture.join("destination.bin")),
            Some(&partial),
            &manifest,
        );
        let resume_offset = u64::from(TRANSFER_CHUNK_BYTES);
        let storage = Storage::open(&fixture.join("receiver.sqlite3")).expect("open storage");
        storage
            .upsert_transfer(&transfer)
            .expect("persist incoming transfer");
        assert!(
            storage
                .try_claim_incoming_transfer(&transfer.transfer_id, &transfer.peer_id)
                .expect("claim incoming transfer")
        );
        assert!(
            storage
                .commit_received_chunk(
                    &transfer.transfer_id,
                    &transfer.peer_id,
                    &manifest.chunks[0],
                    resume_offset,
                )
                .expect("commit prefix authority")
        );
        let resumed_transfer = storage
            .get_transfer(&transfer.transfer_id)
            .expect("load receiver authority")
            .expect("incoming transfer exists");
        let mut partial_with_tail = prefix.clone();
        partial_with_tail.extend_from_slice(b"uncommitted-tail-that-must-be-removed");
        std::fs::write(&partial, partial_with_tail).expect("write partial with durable tail");

        let file = open_resumable_partial(&partial, resume_offset)
            .await
            .expect("truncate uncommitted tail");
        assert_eq!(
            file.metadata().await.expect("partial metadata").len(),
            resume_offset
        );
        let mut stream =
            ScriptedStream::with_incoming(encoded_frame(1, suffix, manifest.chunks[1].sha256));
        receive_acknowledged_chunks(
            &mut stream,
            file,
            &resumed_transfer,
            resume_offset,
            |chunk, committed| {
                storage.commit_received_chunk(
                    &transfer.transfer_id,
                    &transfer.peer_id,
                    chunk,
                    committed,
                )
            },
            || verify_committed_manifest(&storage, &resumed_transfer),
            || async { Ok(()) },
        )
        .await
        .expect("resume suffix after truncation");

        assert_eq!(
            std::fs::read(&partial).expect("read resumed partial"),
            expected
        );
        assert_eq!(stream.written, acknowledgement_bytes(&[manifest.file_size]));

        std::fs::write(&partial, &prefix[..chunk_len - 1]).expect("write short partial");
        assert!(
            open_resumable_partial(&partial, resume_offset)
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::metadata(&partial)
                .expect("short partial metadata")
                .len(),
            resume_offset - 1
        );
        drop(storage);
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[tokio::test]
    async fn sender_rejects_wrong_or_truncated_acknowledgements_without_advancing_progress() {
        let fixture = fixture_directory("bad-ack");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let source = fixture.join("source.bin");
        std::fs::write(
            &source,
            vec![7_u8; usize::try_from(TRANSFER_CHUNK_BYTES).unwrap()],
        )
        .expect("write source");
        let manifest = build_manifest(&source, TRANSFER_CHUNK_BYTES).expect("build manifest");
        let transfer = transfer_record(Direction::Outgoing, Some(&source), None, None, &manifest);

        for incoming in [acknowledgement_bytes(&[1]), vec![0_u8; 7]] {
            let mut stream = ScriptedStream::with_incoming(incoming);
            let mut acknowledged = Vec::new();
            let result = send_acknowledged_chunks(
                &mut stream,
                &source,
                &transfer,
                &manifest.chunks,
                0,
                |offset| {
                    acknowledged.push(offset);
                    Ok(())
                },
            )
            .await;

            assert!(result.is_err());
            assert!(acknowledged.is_empty());
        }
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[tokio::test]
    async fn sender_verifies_source_bytes_against_persisted_chunk_hash_before_writing() {
        let fixture = fixture_directory("source-hash");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let source = fixture.join("source.bin");
        std::fs::write(
            &source,
            vec![1_u8; usize::try_from(TRANSFER_CHUNK_BYTES).unwrap()],
        )
        .expect("write original source");
        let manifest = build_manifest(&source, TRANSFER_CHUNK_BYTES).expect("build manifest");
        std::fs::write(
            &source,
            vec![2_u8; usize::try_from(TRANSFER_CHUNK_BYTES).unwrap()],
        )
        .expect("replace source bytes");
        let changed = crate::transfer_manifest::capture_source_snapshot(&source)
            .expect("capture changed snapshot");
        let mut transfer =
            transfer_record(Direction::Outgoing, Some(&source), None, None, &manifest);
        transfer.source_modified_ns = Some(changed.source_modified_ns);
        let mut stream = ScriptedStream::with_incoming(acknowledgement_bytes(&[u64::from(
            TRANSFER_CHUNK_BYTES,
        )]));

        let error =
            send_acknowledged_chunks(&mut stream, &source, &transfer, &manifest.chunks, 0, |_| {
                Ok(())
            })
            .await
            .expect_err("changed chunk bytes must fail before send");

        assert_eq!(error.code(), "invalid_input");
        assert!(stream.written.is_empty());
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[tokio::test]
    async fn interrupted_stream_resumes_from_acknowledged_offset_without_rewriting_prefix() {
        let fixture = fixture_directory("interrupted-stream");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let source = fixture.join("source.bin");
        let partial = fixture.join("payload.part");
        let destination = fixture.join("payload.bin");
        let sender_database = fixture.join("sender.sqlite3");
        let receiver_database = fixture.join("receiver.sqlite3");
        let chunk_len = usize::try_from(TRANSFER_CHUNK_BYTES).expect("chunk size fits usize");
        let mut source_bytes = vec![0x11; chunk_len];
        source_bytes.extend(vec![0x22; chunk_len]);
        source_bytes.extend(b"final-short-chunk");
        std::fs::write(&source, &source_bytes).expect("write source fixture");
        let manifest = build_manifest(&source, TRANSFER_CHUNK_BYTES).expect("build manifest");
        let outgoing = transfer_record(Direction::Outgoing, Some(&source), None, None, &manifest);
        let incoming = transfer_record(
            Direction::Incoming,
            None,
            Some(&destination),
            Some(&partial),
            &manifest,
        );
        let sender_storage = Storage::open(&sender_database).expect("open sender storage");
        sender_storage
            .create_outgoing_transfer_with_manifest(&outgoing, &manifest.chunks)
            .expect("persist sender manifest");
        let persisted_chunks = sender_storage
            .list_transfer_chunks(&outgoing.transfer_id)
            .expect("load persisted sender chunks");
        let receiver_storage = Storage::open(&receiver_database).expect("open receiver storage");
        receiver_storage
            .upsert_transfer(&incoming)
            .expect("persist incoming transfer");
        assert!(
            receiver_storage
                .try_claim_incoming_transfer(&incoming.transfer_id, &incoming.peer_id)
                .expect("claim initial incoming transfer")
        );

        let first_offset = u64::from(TRANSFER_CHUNK_BYTES);
        let resume_offset = first_offset * 2;
        let two_frame_bytes = (40 + chunk_len) * 2;
        let mut first_sender = ScriptedStream::with_incoming_and_write_limit(
            acknowledgement_bytes(&[first_offset, resume_offset]),
            two_frame_bytes,
        );
        let mut sender_progress = Vec::new();
        let first_send = send_acknowledged_chunks(
            &mut first_sender,
            &source,
            &outgoing,
            &persisted_chunks,
            0,
            |offset| {
                sender_progress.push(offset);
                Ok(())
            },
        )
        .await;
        assert!(matches!(first_send, Err(AppError::Io(_))));
        assert_eq!(sender_progress, vec![first_offset, resume_offset]);
        assert_eq!(first_sender.written.len(), two_frame_bytes);

        let write_offsets = Arc::new(Mutex::new(Vec::new()));
        let first_writer = TrackingFile::open(&partial, write_offsets.clone(), false).await;
        let mut first_receiver = ScriptedStream::with_incoming(first_sender.written.clone());
        let first_receive = receive_acknowledged_chunks(
            &mut first_receiver,
            first_writer,
            &incoming,
            0,
            |chunk, committed| {
                receiver_storage.commit_received_chunk(
                    &incoming.transfer_id,
                    &incoming.peer_id,
                    chunk,
                    committed,
                )
            },
            || Ok(()),
            || async { Ok(()) },
        )
        .await;
        assert!(matches!(first_receive, Err(AppError::Io(_))));
        assert_eq!(
            first_receiver.written,
            acknowledgement_bytes(&[first_offset, resume_offset])
        );
        assert_eq!(
            receiver_storage
                .get_transfer(&incoming.transfer_id)
                .expect("load receiver progress")
                .expect("incoming transfer exists")
                .transferred_bytes,
            resume_offset
        );
        assert!(
            receiver_storage
                .try_pause_claimed_incoming_transfer(
                    &incoming.transfer_id,
                    &incoming.peer_id,
                    "scripted disconnect",
                )
                .expect("pause interrupted incoming transfer")
        );
        assert!(
            receiver_storage
                .try_claim_incoming_transfer(&incoming.transfer_id, &incoming.peer_id)
                .expect("reclaim resumed incoming transfer")
        );

        let final_offset = manifest.file_size;
        let mut resumed_sender =
            ScriptedStream::with_incoming(acknowledgement_bytes(&[final_offset]));
        let mut resumed_progress = Vec::new();
        send_acknowledged_chunks(
            &mut resumed_sender,
            &source,
            &outgoing,
            &persisted_chunks,
            resume_offset,
            |offset| {
                resumed_progress.push(offset);
                Ok(())
            },
        )
        .await
        .expect("send remaining suffix");
        assert_eq!(resumed_progress, vec![final_offset]);

        let resumed_writer = TrackingFile::open(&partial, write_offsets.clone(), false).await;
        let mut resumed_receiver = ScriptedStream::with_incoming(resumed_sender.written);
        let resumed_incoming = receiver_storage
            .get_transfer(&incoming.transfer_id)
            .expect("load resumed incoming transfer")
            .expect("resumed incoming transfer exists");
        let finalized = Arc::new(Mutex::new(false));
        let finalized_for_callback = finalized.clone();
        receive_acknowledged_chunks(
            &mut resumed_receiver,
            resumed_writer,
            &resumed_incoming,
            resume_offset,
            |chunk, committed| {
                receiver_storage.commit_received_chunk(
                    &incoming.transfer_id,
                    &incoming.peer_id,
                    chunk,
                    committed,
                )
            },
            || verify_committed_manifest(&receiver_storage, &resumed_incoming),
            move || async move {
                *finalized_for_callback.lock().expect("lock finalized flag") = true;
                Ok(())
            },
        )
        .await
        .expect("receive remaining suffix");

        assert_eq!(
            resumed_receiver.written,
            acknowledgement_bytes(&[final_offset])
        );
        assert!(*finalized.lock().expect("lock finalized flag"));
        assert_eq!(
            *write_offsets.lock().expect("lock write offsets"),
            vec![0, first_offset, resume_offset]
        );
        assert_eq!(
            std::fs::read(&partial).expect("read resumed partial"),
            source_bytes
        );
        assert_eq!(
            receiver_storage
                .get_transfer(&incoming.transfer_id)
                .expect("load final receiver progress")
                .expect("incoming transfer exists")
                .transferred_bytes,
            final_offset
        );

        drop(receiver_storage);
        drop(sender_storage);
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[tokio::test]
    async fn concurrent_duplex_exchange_resumes_from_receiver_acks_and_finalizes_before_final_ack()
    {
        let fixture = fixture_directory("concurrent-duplex-resume");
        std::fs::create_dir_all(&fixture).expect("create fixture");
        let source = fixture.join("source.bin");
        let partial = fixture.join("payload.part");
        let destination = fixture.join("payload.bin");
        let chunk_len = usize::try_from(TRANSFER_CHUNK_BYTES).expect("chunk size fits usize");
        let mut source_bytes = vec![0x41; chunk_len];
        source_bytes.extend(vec![0x52; chunk_len]);
        source_bytes.extend(b"duplex-final-suffix");
        std::fs::write(&source, &source_bytes).expect("write source fixture");
        let manifest = build_manifest(&source, TRANSFER_CHUNK_BYTES).expect("build manifest");
        let outgoing = transfer_record(Direction::Outgoing, Some(&source), None, None, &manifest);
        let incoming = transfer_record(
            Direction::Incoming,
            None,
            Some(&destination),
            Some(&partial),
            &manifest,
        );
        let sender_storage =
            Storage::open(&fixture.join("duplex-sender.sqlite3")).expect("open sender storage");
        sender_storage
            .create_outgoing_transfer_with_manifest(&outgoing, &manifest.chunks)
            .expect("persist sender manifest");
        let persisted_chunks = sender_storage
            .list_transfer_chunks(&outgoing.transfer_id)
            .expect("load sender chunks");
        let receiver_storage =
            Storage::open(&fixture.join("duplex-receiver.sqlite3")).expect("open receiver storage");
        receiver_storage
            .upsert_transfer(&incoming)
            .expect("persist incoming transfer");
        assert!(
            receiver_storage
                .try_claim_incoming_transfer(&incoming.transfer_id, &incoming.peer_id)
                .expect("claim initial incoming transfer")
        );
        let first_offset = u64::from(TRANSFER_CHUNK_BYTES);
        let resume_offset = first_offset * 2;
        let two_frame_bytes = (40 + chunk_len) * 2;
        let sender_progress = Arc::new(Mutex::new(Vec::new()));
        let sender_progress_for_task = sender_progress.clone();
        let finalized_first_exchange = Arc::new(AtomicBool::new(false));
        let finalized_first_for_task = finalized_first_exchange.clone();
        let write_offsets = Arc::new(Mutex::new(Vec::new()));
        let first_writer = TrackingFile::open(&partial, write_offsets.clone(), false).await;
        let (sender_half, receiver_half) = tokio::io::duplex(64 * 1024);
        let source_for_sender = source.clone();
        let outgoing_for_sender = outgoing.clone();
        let chunks_for_sender = persisted_chunks.clone();
        let incoming_for_receiver = incoming.clone();
        let receiver_storage_for_task = receiver_storage.clone();
        let first_exchange = async move {
            let sender = async move {
                let mut stream = FuturesDuplex::with_write_limit(sender_half, two_frame_bytes);
                send_acknowledged_chunks(
                    &mut stream,
                    &source_for_sender,
                    &outgoing_for_sender,
                    &chunks_for_sender,
                    0,
                    |offset| {
                        sender_progress_for_task
                            .lock()
                            .expect("lock sender progress")
                            .push(offset);
                        Ok(())
                    },
                )
                .await
            };
            let receiver = async move {
                let mut stream = FuturesDuplex::new(receiver_half);
                receive_acknowledged_chunks(
                    &mut stream,
                    first_writer,
                    &incoming_for_receiver,
                    0,
                    |chunk, committed| {
                        receiver_storage_for_task.commit_received_chunk(
                            &incoming_for_receiver.transfer_id,
                            &incoming_for_receiver.peer_id,
                            chunk,
                            committed,
                        )
                    },
                    || {
                        verify_committed_manifest(
                            &receiver_storage_for_task,
                            &incoming_for_receiver,
                        )
                    },
                    move || async move {
                        finalized_first_for_task.store(true, Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await
            };
            tokio::join!(sender, receiver)
        };
        let (first_send, first_receive) =
            tokio::time::timeout(Duration::from_secs(30), first_exchange)
                .await
                .expect("first duplex exchange must not deadlock");

        assert!(matches!(first_send, Err(AppError::Io(_))));
        assert!(matches!(first_receive, Err(AppError::Io(_))));
        assert_eq!(
            *sender_progress.lock().expect("lock sender progress"),
            vec![first_offset, resume_offset]
        );
        assert!(!finalized_first_exchange.load(Ordering::SeqCst));
        assert!(
            receiver_storage
                .try_pause_claimed_incoming_transfer(
                    &incoming.transfer_id,
                    &incoming.peer_id,
                    "scripted disconnect",
                )
                .expect("pause interrupted incoming transfer")
        );
        assert!(
            receiver_storage
                .try_claim_incoming_transfer(&incoming.transfer_id, &incoming.peer_id)
                .expect("reclaim resumed incoming transfer")
        );
        let resumed_incoming = receiver_storage
            .get_transfer(&incoming.transfer_id)
            .expect("load receiver authority")
            .expect("incoming transfer exists");
        assert_eq!(resumed_incoming.transferred_bytes, resume_offset);

        let resumed_file = open_resumable_partial(&partial, resume_offset)
            .await
            .expect("open committed partial");
        let finalized = Arc::new(AtomicBool::new(false));
        let finalized_for_receiver = finalized.clone();
        let ack_observed_after_finalize = Arc::new(AtomicBool::new(false));
        let ack_order_for_sender = ack_observed_after_finalize.clone();
        let finalized_for_sender = finalized.clone();
        let resumed_progress = Arc::new(Mutex::new(Vec::new()));
        let resumed_progress_for_sender = resumed_progress.clone();
        let (sender_half, receiver_half) = tokio::io::duplex(64 * 1024);
        let source_for_sender = source.clone();
        let outgoing_for_sender = outgoing.clone();
        let chunks_for_sender = persisted_chunks.clone();
        let receiver_storage_for_task = receiver_storage.clone();
        let resumed_transfer_for_receiver = resumed_incoming.clone();
        let final_exchange = async move {
            let sender = async move {
                let mut stream = FuturesDuplex::new(sender_half);
                send_acknowledged_chunks(
                    &mut stream,
                    &source_for_sender,
                    &outgoing_for_sender,
                    &chunks_for_sender,
                    resume_offset,
                    |offset| {
                        ack_order_for_sender.store(
                            finalized_for_sender.load(Ordering::SeqCst),
                            Ordering::SeqCst,
                        );
                        resumed_progress_for_sender
                            .lock()
                            .expect("lock resumed progress")
                            .push(offset);
                        Ok(())
                    },
                )
                .await
            };
            let receiver = async move {
                let mut stream = FuturesDuplex::new(receiver_half);
                receive_acknowledged_chunks(
                    &mut stream,
                    resumed_file,
                    &resumed_transfer_for_receiver,
                    resume_offset,
                    |chunk, committed| {
                        receiver_storage_for_task.commit_received_chunk(
                            &resumed_transfer_for_receiver.transfer_id,
                            &resumed_transfer_for_receiver.peer_id,
                            chunk,
                            committed,
                        )
                    },
                    || {
                        verify_committed_manifest(
                            &receiver_storage_for_task,
                            &resumed_transfer_for_receiver,
                        )
                    },
                    move || async move {
                        finalized_for_receiver.store(true, Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await
            };
            tokio::join!(sender, receiver)
        };
        let (final_send, final_receive) =
            tokio::time::timeout(Duration::from_secs(30), final_exchange)
                .await
                .expect("resumed duplex exchange must not deadlock");

        assert_eq!(
            final_send.expect("sender receives final ACK"),
            manifest.file_size
        );
        assert_eq!(
            final_receive.expect("receiver completes suffix"),
            manifest.file_size
        );
        assert_eq!(
            *resumed_progress.lock().expect("lock resumed progress"),
            vec![manifest.file_size]
        );
        assert!(finalized.load(Ordering::SeqCst));
        assert!(ack_observed_after_finalize.load(Ordering::SeqCst));
        assert_eq!(
            *write_offsets.lock().expect("lock write offsets"),
            vec![0, first_offset]
        );
        assert_eq!(
            std::fs::read(&partial).expect("read final partial"),
            source_bytes
        );

        drop(receiver_storage);
        drop(sender_storage);
        std::fs::remove_dir_all(fixture).expect("remove fixture");
    }
}
