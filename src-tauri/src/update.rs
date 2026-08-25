use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::{AppHandle, Emitter, Manager};
use tauri_plugin_opener::OpenerExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::AppError;

const UPDATE_PROGRESS_EVENT: &str = "localnet://update-progress";
const MAX_UPDATE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateDownloadRequest {
    pub version: String,
    pub asset_name: String,
    pub download_url: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadedUpdate {
    pub path: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateDownloadProgress {
    pub version: String,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdatePlatform {
    #[cfg_attr(not(any(target_os = "windows", test)), allow(dead_code))]
    Windows,
    #[cfg_attr(not(any(target_os = "macos", test)), allow(dead_code))]
    Macos,
}

impl UpdatePlatform {
    fn current() -> Result<Self, AppError> {
        #[cfg(target_os = "windows")]
        {
            return Ok(Self::Windows);
        }
        #[cfg(target_os = "macos")]
        {
            return Ok(Self::Macos);
        }
        #[allow(unreachable_code)]
        Err(AppError::Update("当前系统暂不支持应用内更新".to_string()))
    }

    fn asset_name(self, version: &str) -> String {
        match self {
            Self::Windows => format!("Weline_Localnet_{version}_x64-setup.exe"),
            Self::Macos => format!("Weline_Localnet_{version}_universal.dmg"),
        }
    }
}

struct ValidatedUpdateRequest {
    digest: [u8; 32],
}

#[tauri::command]
pub async fn download_update(
    app: AppHandle,
    request: UpdateDownloadRequest,
) -> Result<DownloadedUpdate, AppError> {
    let platform = UpdatePlatform::current()?;
    validate_update_request_for_platform(&request, platform)?;
    let cache_directory = update_cache_directory(&app)?;
    if let Some(downloaded) = verified_cached_update(&cache_directory, &request, platform).await? {
        return Ok(downloaded);
    }

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30 * 60))
        .build()
        .map_err(|error| {
            tracing::error!(%error, "unable to create update HTTP client");
            AppError::Update("无法启动更新下载，请稍后重试".to_string())
        })?;
    let response = client
        .get(&request.download_url)
        .header(
            reqwest::header::USER_AGENT,
            format!("Weline-Localnet/{}", app.package_info().version),
        )
        .send()
        .await
        .map_err(|error| {
            tracing::warn!(%error, "update package request failed");
            AppError::Update("更新下载失败，请检查网络后重试".to_string())
        })?;
    if !response.status().is_success() {
        tracing::warn!(status = %response.status(), "update package server returned an error");
        return Err(AppError::Update("更新下载失败，请稍后重试".to_string()));
    }
    if response
        .content_length()
        .is_some_and(|content_length| content_length != request.size)
    {
        return Err(AppError::Update(
            "更新包大小与发布信息不一致，已停止下载".to_string(),
        ));
    }

    let app_handle = app.clone();
    let progress_version = request.version.clone();
    let total_bytes = request.size;
    let chunks = response.bytes_stream().map(|chunk| {
        chunk.map(|bytes| bytes.to_vec()).map_err(|error| {
            tracing::warn!(%error, "update package stream interrupted");
            AppError::Update("更新下载中断，请检查网络后重试".to_string())
        })
    });
    persist_verified_update_chunks(
        &cache_directory,
        &request,
        platform,
        chunks,
        move |downloaded_bytes| {
            let _ = app_handle.emit(
                UPDATE_PROGRESS_EVENT,
                UpdateDownloadProgress {
                    version: progress_version.clone(),
                    downloaded_bytes,
                    total_bytes,
                },
            );
        },
    )
    .await
}

#[tauri::command]
pub async fn open_downloaded_update(
    app: AppHandle,
    request: UpdateDownloadRequest,
) -> Result<(), AppError> {
    let platform = UpdatePlatform::current()?;
    validate_update_request_for_platform(&request, platform)?;
    let cache_directory = update_cache_directory(&app)?;
    let Some(downloaded) = verified_cached_update(&cache_directory, &request, platform).await?
    else {
        return Err(AppError::Update(
            "已下载的更新包校验失败，请重新下载".to_string(),
        ));
    };
    app.opener()
        .open_path(downloaded.path, None::<&str>)
        .map_err(|error| {
            tracing::error!(%error, "unable to open verified update package");
            AppError::Update("无法打开更新安装包，请稍后重试".to_string())
        })
}

async fn persist_verified_update_chunks<S, F>(
    directory: &Path,
    request: &UpdateDownloadRequest,
    platform: UpdatePlatform,
    mut chunks: S,
    mut publish_progress: F,
) -> Result<DownloadedUpdate, AppError>
where
    S: Stream<Item = Result<Vec<u8>, AppError>> + Unpin,
    F: FnMut(u64),
{
    let validated = validate_update_request_for_platform(request, platform)?;
    tokio::fs::create_dir_all(directory)
        .await
        .map_err(update_cache_io_error)?;
    if let Some(downloaded) = verified_cached_update(directory, request, platform).await? {
        publish_progress(downloaded.bytes);
        return Ok(downloaded);
    }

    let final_path = directory.join(&request.asset_name);
    let partial_path = directory.join(format!(
        ".{}.{}.part",
        request.asset_name,
        uuid::Uuid::now_v7()
    ));
    let mut partial = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&partial_path)
        .await
        .map_err(update_cache_io_error)?;
    let mut downloaded_bytes = 0_u64;
    let mut hasher = Sha256::new();

    let write_result: Result<(), AppError> = async {
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            downloaded_bytes = downloaded_bytes
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| AppError::Update("更新包大小无效，已停止下载".to_string()))?;
            if downloaded_bytes > request.size || downloaded_bytes > MAX_UPDATE_BYTES {
                return Err(AppError::Update(
                    "更新包超过发布信息中的大小，已停止下载".to_string(),
                ));
            }
            partial
                .write_all(&chunk)
                .await
                .map_err(update_cache_io_error)?;
            hasher.update(&chunk);
            publish_progress(downloaded_bytes);
        }
        partial.flush().await.map_err(update_cache_io_error)?;
        partial.sync_all().await.map_err(update_cache_io_error)?;
        if downloaded_bytes != request.size {
            return Err(AppError::Update("更新下载不完整，请重新下载".to_string()));
        }
        let digest: [u8; 32] = hasher.finalize().into();
        if digest != validated.digest {
            return Err(AppError::Update(
                "更新包完整性校验失败，已拒绝打开".to_string(),
            ));
        }
        Ok(())
    }
    .await;
    drop(partial);

    if let Err(error) = write_result {
        remove_partial(&partial_path).await;
        return Err(error);
    }

    if let Err(error) = tokio::fs::hard_link(&partial_path, &final_path).await {
        if let Some(downloaded) = verified_cached_update(directory, request, platform).await? {
            remove_partial(&partial_path).await;
            return Ok(downloaded);
        }
        tracing::error!(%error, "unable to publish verified update package");
        remove_partial(&partial_path).await;
        return Err(AppError::Update(
            "无法保存已校验的更新包，请稍后重试".to_string(),
        ));
    }
    remove_partial(&partial_path).await;
    Ok(DownloadedUpdate {
        path: final_path.to_string_lossy().into_owned(),
        bytes: downloaded_bytes,
    })
}

fn validate_update_request_for_platform(
    request: &UpdateDownloadRequest,
    platform: UpdatePlatform,
) -> Result<ValidatedUpdateRequest, AppError> {
    if !is_stable_version(&request.version) {
        return Err(AppError::Update("更新版本号无效".to_string()));
    }
    let expected_asset_name = platform.asset_name(&request.version);
    if request.asset_name != expected_asset_name {
        return Err(AppError::Update("更新包名称与当前系统不匹配".to_string()));
    }
    if request.size == 0 || request.size > MAX_UPDATE_BYTES {
        return Err(AppError::Update("更新包大小无效".to_string()));
    }
    if request.sha256.len() != 64
        || !request
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(AppError::Update("更新包摘要无效".to_string()));
    }
    let decoded =
        hex::decode(&request.sha256).map_err(|_| AppError::Update("更新包摘要无效".to_string()))?;
    let digest: [u8; 32] = decoded
        .try_into()
        .map_err(|_| AppError::Update("更新包摘要无效".to_string()))?;
    let url = reqwest::Url::parse(&request.download_url)
        .map_err(|_| AppError::Update("更新下载地址无效".to_string()))?;
    let expected_path = format!(
        "/Aiweline/Localnet/releases/download/v{}/{}",
        request.version, expected_asset_name
    );
    if url.scheme() != "https"
        || url.host_str() != Some("github.com")
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != expected_path
    {
        return Err(AppError::Update("更新下载地址不可信".to_string()));
    }
    Ok(ValidatedUpdateRequest { digest })
}

async fn verified_cached_update(
    directory: &Path,
    request: &UpdateDownloadRequest,
    platform: UpdatePlatform,
) -> Result<Option<DownloadedUpdate>, AppError> {
    let validated = validate_update_request_for_platform(request, platform)?;
    let path = directory.join(&request.asset_name);
    let metadata = match tokio::fs::symlink_metadata(&path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(update_cache_io_error(error)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(AppError::Update("更新缓存位置包含不安全的文件".to_string()));
    }
    if metadata.len() != request.size {
        tokio::fs::remove_file(&path)
            .await
            .map_err(update_cache_io_error)?;
        return Ok(None);
    }
    let mut file = tokio::fs::File::open(&path)
        .await
        .map_err(update_cache_io_error)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 128 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(update_cache_io_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest: [u8; 32] = hasher.finalize().into();
    if digest != validated.digest {
        drop(file);
        tokio::fs::remove_file(&path)
            .await
            .map_err(update_cache_io_error)?;
        return Ok(None);
    }
    Ok(Some(DownloadedUpdate {
        path: path.to_string_lossy().into_owned(),
        bytes: metadata.len(),
    }))
}

fn update_cache_directory(app: &AppHandle) -> Result<PathBuf, AppError> {
    app.path()
        .app_cache_dir()
        .map(|directory| directory.join("updates"))
        .map_err(|error| {
            tracing::error!(%error, "unable to resolve update cache directory");
            AppError::Update("无法定位更新缓存目录".to_string())
        })
}

fn is_stable_version(version: &str) -> bool {
    let parts = version.split('.').collect::<Vec<_>>();
    parts.len() == 3
        && parts.iter().all(|part| {
            !part.is_empty()
                && part.bytes().all(|byte| byte.is_ascii_digit())
                && (part == &"0" || !part.starts_with('0'))
                && part.parse::<u64>().is_ok()
        })
}

fn update_cache_io_error(error: std::io::Error) -> AppError {
    tracing::error!(%error, "update cache operation failed");
    AppError::Update("无法写入更新缓存，请检查磁盘空间和权限".to_string())
}

async fn remove_partial(path: &Path) {
    if let Err(error) = tokio::fs::remove_file(path).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(%error, "unable to remove incomplete update package");
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use futures::stream;
    use sha2::{Digest, Sha256};

    use super::{
        UpdateDownloadRequest, UpdatePlatform, persist_verified_update_chunks,
        validate_update_request_for_platform,
    };

    fn temporary_directory(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "weline-localnet-update-{label}-{}",
            uuid::Uuid::now_v7()
        ))
    }

    fn request(platform: UpdatePlatform, body: &[u8]) -> UpdateDownloadRequest {
        let version = "0.2.3";
        let asset_name = match platform {
            UpdatePlatform::Windows => format!("Weline_Localnet_{version}_x64-setup.exe"),
            UpdatePlatform::Macos => format!("Weline_Localnet_{version}_universal.dmg"),
        };
        UpdateDownloadRequest {
            version: version.to_string(),
            download_url: format!(
                "https://github.com/Aiweline/Localnet/releases/download/v{version}/{asset_name}"
            ),
            asset_name,
            sha256: hex::encode(Sha256::digest(body)),
            size: body.len() as u64,
        }
    }

    #[test]
    fn update_request_accepts_only_exact_platform_release_assets() {
        let body = b"verified update package";
        validate_update_request_for_platform(
            &request(UpdatePlatform::Windows, body),
            UpdatePlatform::Windows,
        )
        .expect("exact Windows installer is accepted");
        validate_update_request_for_platform(
            &request(UpdatePlatform::Macos, body),
            UpdatePlatform::Macos,
        )
        .expect("exact macOS installer is accepted");

        let mut cases = Vec::new();
        let valid = request(UpdatePlatform::Windows, body);
        cases.push(UpdateDownloadRequest {
            version: "0.2.3-beta.1".to_string(),
            ..valid.clone()
        });
        cases.push(UpdateDownloadRequest {
            asset_name: "../Localnet.exe".to_string(),
            ..valid.clone()
        });
        cases.push(UpdateDownloadRequest {
            download_url: valid.download_url.replace("https://", "http://"),
            ..valid.clone()
        });
        cases.push(UpdateDownloadRequest {
            download_url: valid.download_url.replace("github.com", "evil.example"),
            ..valid.clone()
        });
        cases.push(UpdateDownloadRequest {
            download_url: valid
                .download_url
                .replace("Aiweline/Localnet", "Aiweline/Other"),
            ..valid.clone()
        });
        cases.push(UpdateDownloadRequest {
            sha256: "A".repeat(64),
            ..valid.clone()
        });
        cases.push(UpdateDownloadRequest {
            sha256: "0".repeat(63),
            ..valid.clone()
        });
        cases.push(UpdateDownloadRequest {
            size: 0,
            ..valid.clone()
        });
        cases.push(UpdateDownloadRequest {
            size: 536_870_913,
            ..valid
        });

        for invalid in cases {
            assert!(
                validate_update_request_for_platform(&invalid, UpdatePlatform::Windows).is_err(),
                "invalid update request must be rejected: {invalid:?}"
            );
        }
    }

    #[tokio::test]
    async fn verified_update_stream_becomes_the_exact_final_package() {
        let directory = temporary_directory("success");
        fs::create_dir_all(&directory).expect("create update fixture");
        let body = b"verified update package";
        let request = request(UpdatePlatform::Windows, body);
        let chunks = stream::iter(vec![
            Ok(body[..8].to_vec()),
            Ok(body[8..17].to_vec()),
            Ok(body[17..].to_vec()),
        ]);
        let mut progress = Vec::new();

        let downloaded = persist_verified_update_chunks(
            &directory,
            &request,
            UpdatePlatform::Windows,
            chunks,
            |bytes| progress.push(bytes),
        )
        .await
        .expect("verified stream completes");

        assert_eq!(downloaded.bytes, body.len() as u64);
        assert_eq!(
            PathBuf::from(&downloaded.path),
            directory.join(&request.asset_name)
        );
        assert_eq!(
            fs::read(&downloaded.path).expect("read final package"),
            body
        );
        assert_eq!(progress.last().copied(), Some(body.len() as u64));
        assert_eq!(
            fs::read_dir(&directory).expect("list cache").count(),
            1,
            "only the verified final package remains"
        );

        fs::remove_dir_all(directory).expect("remove update fixture");
    }

    #[tokio::test]
    async fn invalid_or_interrupted_stream_never_leaves_an_installable_package() {
        let body = b"verified update package";
        for (label, update_request, chunks) in [
            (
                "truncated",
                request(UpdatePlatform::Windows, body),
                vec![Ok(body[..8].to_vec())],
            ),
            (
                "oversized",
                request(UpdatePlatform::Windows, body),
                vec![Ok([body.as_slice(), b"extra"].concat())],
            ),
            (
                "digest",
                UpdateDownloadRequest {
                    sha256: "0".repeat(64),
                    ..request(UpdatePlatform::Windows, body)
                },
                vec![Ok(body.to_vec())],
            ),
        ] {
            let directory = temporary_directory(label);
            fs::create_dir_all(&directory).expect("create invalid fixture");
            let result = persist_verified_update_chunks(
                &directory,
                &update_request,
                UpdatePlatform::Windows,
                stream::iter(chunks),
                |_| {},
            )
            .await;

            assert!(result.is_err(), "{label} stream must fail");
            assert!(
                !directory.join(&update_request.asset_name).exists(),
                "{label} stream must not create a final package"
            );
            assert_eq!(
                fs::read_dir(&directory)
                    .expect("list invalid fixture")
                    .count(),
                0,
                "{label} partial must be cleaned"
            );
            fs::remove_dir_all(directory).expect("remove invalid fixture");
        }
    }
}
