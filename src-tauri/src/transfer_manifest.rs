use std::{
    fs::{self, File, Metadata},
    io::{BufReader, Read},
    path::Path,
    time::UNIX_EPOCH,
};

use sha2::{Digest, Sha256};

use crate::{
    error::AppError,
    transfer_policy::{
        FILE_RESUME_V2_CAPABILITY, TRANSFER_CHUNK_BYTES, TransferProtocol, select_transfer_protocol,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferChunk {
    pub index: u32,
    pub length: u32,
    pub sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferManifest {
    pub file_size: u64,
    pub file_sha256: [u8; 32],
    pub manifest_sha256: [u8; 32],
    pub chunks: Vec<TransferChunk>,
    pub source_modified_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSnapshot {
    pub file_size: u64,
    pub source_modified_ns: u64,
}

#[allow(dead_code)] // Public crate API for callers that do not preselect a source snapshot.
pub fn build_manifest(path: &Path, chunk_size: u32) -> Result<TransferManifest, AppError> {
    let snapshot = capture_source_snapshot(path)?;
    build_manifest_from_snapshot(path, chunk_size, snapshot)
}

pub fn capture_source_snapshot(path: &Path) -> Result<SourceSnapshot, AppError> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(AppError::InvalidInput("请选择一个普通文件".to_string()));
    }
    Ok(SourceSnapshot {
        file_size: metadata.len(),
        source_modified_ns: source_modified_ns(&metadata)?,
    })
}

pub fn build_manifest_from_snapshot(
    path: &Path,
    chunk_size: u32,
    expected: SourceSnapshot,
) -> Result<TransferManifest, AppError> {
    if chunk_size == 0 {
        return Err(AppError::InvalidInput("分块大小必须大于零".to_string()));
    }
    let initial_metadata = fs::metadata(path)?;
    if !initial_metadata.is_file()
        || initial_metadata.len() != expected.file_size
        || source_modified_ns(&initial_metadata)? != expected.source_modified_ns
    {
        return Err(source_changed_error());
    }
    let file_size = expected.file_size;
    let initial_modified_ns = expected.source_modified_ns;
    let chunk_count = chunk_count(file_size, chunk_size)?;
    let buffer_len = usize::try_from(chunk_size)
        .map_err(|_| AppError::InvalidInput("分块大小超出当前平台限制".to_string()))?;
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(buffer_len)
        .map_err(|_| AppError::InvalidInput("分块大小无法分配".to_string()))?;
    buffer.resize(buffer_len, 0);

    let mut reader = BufReader::new(File::open(path)?);
    let mut file_hasher = Sha256::new();
    let mut chunks = Vec::new();
    let mut remaining = file_size;
    for index in 0..chunk_count {
        let length = remaining.min(u64::from(chunk_size));
        let length_usize = usize::try_from(length)
            .map_err(|_| AppError::InvalidInput("分块长度超出当前平台限制".to_string()))?;
        reader.read_exact(&mut buffer[..length_usize])?;
        let bytes = &buffer[..length_usize];
        file_hasher.update(bytes);
        let sha256: [u8; 32] = Sha256::digest(bytes).into();
        chunks.push(TransferChunk {
            index,
            length: u32::try_from(length)
                .map_err(|_| AppError::InvalidInput("分块长度超出协议限制".to_string()))?,
            sha256,
        });
        remaining = remaining
            .checked_sub(length)
            .ok_or_else(|| AppError::InvalidInput("源文件大小与分块长度不一致".to_string()))?;
    }
    if remaining != 0 {
        return Err(AppError::InvalidInput(
            "源文件大小与分块长度不一致".to_string(),
        ));
    }

    let final_metadata = fs::metadata(path)?;
    if !final_metadata.is_file()
        || final_metadata.len() != file_size
        || source_modified_ns(&final_metadata)? != initial_modified_ns
    {
        return Err(source_changed_error());
    }
    Ok(TransferManifest {
        file_size,
        file_sha256: file_hasher.finalize().into(),
        manifest_sha256: manifest_root(&chunks),
        chunks,
        source_modified_ns: initial_modified_ns,
    })
}

pub fn manifest_root(chunks: &[TransferChunk]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for chunk in chunks {
        hasher.update(chunk.index.to_be_bytes());
        hasher.update(chunk.length.to_be_bytes());
        hasher.update(chunk.sha256);
    }
    hasher.finalize().into()
}

pub fn expected_chunk_count(file_size: u64, chunk_size: u32) -> Result<u32, AppError> {
    if chunk_size == 0 {
        return Err(AppError::InvalidInput("分块大小必须大于零".to_string()));
    }
    let chunk_size = u64::from(chunk_size);
    let count = if file_size == 0 {
        0
    } else {
        file_size
            .checked_sub(1)
            .and_then(|size| size.checked_div(chunk_size))
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| AppError::InvalidInput("分块数量溢出".to_string()))?
    };
    u32::try_from(count).map_err(|_| AppError::InvalidInput("分块数量超出协议限制".to_string()))
}

pub fn expected_chunk_length(file_size: u64, chunk_size: u32, index: u32) -> Result<u32, AppError> {
    let count = expected_chunk_count(file_size, chunk_size)?;
    if index >= count {
        return Err(AppError::InvalidInput("分块索引超出文件范围".to_string()));
    }
    if index + 1 < count {
        return Ok(chunk_size);
    }
    let offset = u64::from(index)
        .checked_mul(u64::from(chunk_size))
        .ok_or_else(|| AppError::InvalidInput("分块偏移量溢出".to_string()))?;
    u32::try_from(
        file_size
            .checked_sub(offset)
            .ok_or_else(|| AppError::InvalidInput("分块偏移量无效".to_string()))?,
    )
    .map_err(|_| AppError::InvalidInput("分块长度超出协议限制".to_string()))
}

pub fn validate_transfer_metadata(
    transfer_protocol: u8,
    file_size: u64,
    chunk_size: u32,
    chunk_count: u32,
    manifest_sha256: Option<&str>,
) -> Result<(), AppError> {
    let protocol = match transfer_protocol {
        value if value == TransferProtocol::LegacyV1 as u8 => TransferProtocol::LegacyV1,
        value if value == TransferProtocol::ResumableV2 as u8 => TransferProtocol::ResumableV2,
        _ => return Err(AppError::InvalidInput("传输协议版本无效".to_string())),
    };
    let capabilities = match protocol {
        TransferProtocol::LegacyV1 => Vec::new(),
        TransferProtocol::ResumableV2 => vec![FILE_RESUME_V2_CAPABILITY.to_string()],
    };
    if select_transfer_protocol(&capabilities, file_size)? != protocol {
        return Err(AppError::InvalidInput(
            "传输协议与文件大小不匹配".to_string(),
        ));
    }
    match protocol {
        TransferProtocol::LegacyV1 => {
            if chunk_size != 0 || chunk_count != 0 || manifest_sha256.is_some() {
                return Err(AppError::InvalidInput(
                    "旧版传输不能携带分块清单元数据".to_string(),
                ));
            }
        }
        TransferProtocol::ResumableV2 => {
            if chunk_size != TRANSFER_CHUNK_BYTES {
                return Err(AppError::InvalidInput("分块大小与协议不匹配".to_string()));
            }
            if chunk_count != expected_chunk_count(file_size, chunk_size)? {
                return Err(AppError::InvalidInput(
                    "分块数量与文件大小不匹配".to_string(),
                ));
            }
            decode_sha256(
                manifest_sha256
                    .ok_or_else(|| AppError::InvalidInput("缺少传输清单哈希".to_string()))?,
            )?;
        }
    }
    Ok(())
}

pub fn decode_sha256(value: &str) -> Result<[u8; 32], AppError> {
    let bytes = hex::decode(value)
        .map_err(|_| AppError::InvalidInput("哈希不是有效的十六进制值".to_string()))?;
    bytes
        .try_into()
        .map_err(|_| AppError::InvalidInput("哈希长度无效，必须为 32 字节".to_string()))
}

fn chunk_count(file_size: u64, chunk_size: u32) -> Result<u32, AppError> {
    expected_chunk_count(file_size, chunk_size)
}

fn source_changed_error() -> AppError {
    AppError::InvalidInput("源文件在准备传输时发生了变化，请重试".to_string())
}

fn source_modified_ns(metadata: &Metadata) -> Result<u64, AppError> {
    let modified = metadata.modified()?;
    let duration = modified
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AppError::InvalidInput("源文件修改时间早于 Unix 时间起点".to_string()))?;
    u64::try_from(duration.as_nanos())
        .map_err(|_| AppError::InvalidInput("源文件修改时间超出存储范围".to_string()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        build_manifest, build_manifest_from_snapshot, capture_source_snapshot, chunk_count,
        manifest_root,
    };

    fn fixture_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "weline-localnet-transfer-manifest-{name}-{}",
            uuid::Uuid::now_v7()
        ))
    }

    #[test]
    fn builds_exact_two_chunk_manifest_in_stream_order() {
        let path = fixture_path("two-chunks");
        fs::write(&path, b"abcde").expect("write fixture");

        let manifest = build_manifest(&path, 3).expect("build manifest");

        assert_eq!(manifest.file_size, 5);
        assert_eq!(
            hex::encode(manifest.file_sha256),
            "36bbe50ed96841d10443bcb670d6554f0a34b761be67ec9c4a8ad2c0c44ca42c"
        );
        assert_eq!(manifest.chunks.len(), 2);
        assert_eq!(manifest.chunks[0].index, 0);
        assert_eq!(manifest.chunks[0].length, 3);
        assert_eq!(
            hex::encode(manifest.chunks[0].sha256),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(manifest.chunks[1].index, 1);
        assert_eq!(manifest.chunks[1].length, 2);
        assert_eq!(
            hex::encode(manifest.chunks[1].sha256),
            "959a45d44e6fcf58361ed004681556fe50129f2109e817dec098c00c9e5d2578"
        );
        assert_eq!(
            hex::encode(manifest.manifest_sha256),
            "bc35dc9950114089fd447251a49b379b441b327166d2500fdbbe8e4be8aeba33"
        );
        assert_eq!(manifest.manifest_sha256, manifest_root(&manifest.chunks));

        fs::remove_file(path).expect("remove fixture");
    }

    #[test]
    fn hashes_empty_files_without_chunks() {
        let path = fixture_path("empty");
        fs::write(&path, []).expect("write empty fixture");

        let manifest = build_manifest(&path, 3).expect("build empty manifest");

        assert_eq!(manifest.file_size, 0);
        assert!(manifest.chunks.is_empty());
        assert_eq!(
            hex::encode(manifest.file_sha256),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(manifest.manifest_sha256, manifest_root(&[]));

        fs::remove_file(path).expect("remove fixture");
    }

    #[test]
    fn rejects_chunk_counts_that_do_not_fit_the_wire_index() {
        let error = chunk_count(u64::from(u32::MAX) + 1, 1).expect_err("must reject overflow");

        assert!(error.to_string().contains("分块数量"));
    }

    #[test]
    fn rejects_a_source_changed_after_the_selection_snapshot() {
        let path = fixture_path("changed-after-selection");
        fs::write(&path, b"before").expect("write initial fixture");
        let snapshot = capture_source_snapshot(&path).expect("capture selection snapshot");
        fs::write(&path, b"after-change").expect("mutate fixture before hashing");

        let error = build_manifest_from_snapshot(&path, 3, snapshot)
            .expect_err("changed source must not be hashed against a stale decision");

        assert!(error.to_string().contains("发生了变化"));
        fs::remove_file(path).expect("remove fixture");
    }
}
