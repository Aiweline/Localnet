use std::{
    fs::{self, File, Metadata},
    io::{BufReader, Read},
    path::Path,
    time::UNIX_EPOCH,
};

use sha2::{Digest, Sha256};

use crate::error::AppError;

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

pub fn build_manifest(path: &Path, chunk_size: u32) -> Result<TransferManifest, AppError> {
    if chunk_size == 0 {
        return Err(AppError::InvalidInput("分块大小必须大于零".to_string()));
    }
    let initial_metadata = fs::metadata(path)?;
    if !initial_metadata.is_file() {
        return Err(AppError::InvalidInput("请选择一个普通文件".to_string()));
    }
    let file_size = initial_metadata.len();
    let initial_modified_ns = source_modified_ns(&initial_metadata)?;
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
    if final_metadata.len() != file_size
        || source_modified_ns(&final_metadata)? != initial_modified_ns
    {
        return Err(AppError::InvalidInput(
            "源文件在准备传输时发生了变化，请重试".to_string(),
        ));
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

fn chunk_count(file_size: u64, chunk_size: u32) -> Result<u32, AppError> {
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

    use super::{build_manifest, chunk_count, manifest_root};

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
}
