use serde::ser::{Serialize, Serializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationPreflightFailure {
    DirectoryUnavailable,
    PermissionDenied,
    InsufficientSpace,
    FilesystemLimit,
    UnsupportedFilesystem,
    FileTooLarge,
}

impl std::fmt::Display for DestinationPreflightFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.public_message())
    }
}

impl std::error::Error for DestinationPreflightFailure {}

impl DestinationPreflightFailure {
    pub(crate) const fn public_message(self) -> &'static str {
        match self {
            Self::DirectoryUnavailable => "接收目录当前不可用，请恢复磁盘或重新选择目录后重试",
            Self::PermissionDenied => "没有权限写入接收目录，请重新选择可写入的目录",
            Self::InsufficientSpace => "接收目录可用空间不足，请释放空间后等待自动恢复",
            Self::FilesystemLimit => "接收目录的磁盘格式不支持这么大的文件，请选择支持大文件的目录",
            Self::UnsupportedFilesystem => "无法安全检查接收目录所在磁盘，请选择本地磁盘目录后重试",
            Self::FileTooLarge => "单个文件不能超过 100 GiB",
        }
    }

    pub(crate) const fn marker_token(self) -> &'static str {
        match self {
            Self::DirectoryUnavailable => "directory-unavailable",
            Self::PermissionDenied => "permission-denied",
            Self::InsufficientSpace => "insufficient-space",
            Self::FilesystemLimit => "filesystem-limit",
            Self::UnsupportedFilesystem => "unsupported-filesystem",
            Self::FileTooLarge => "file-too-large",
        }
    }

    pub(crate) fn from_marker_token(token: &str) -> Option<Self> {
        match token {
            "directory-unavailable" => Some(Self::DirectoryUnavailable),
            "permission-denied" => Some(Self::PermissionDenied),
            "insufficient-space" => Some(Self::InsufficientSpace),
            "filesystem-limit" => Some(Self::FilesystemLimit),
            "unsupported-filesystem" => Some(Self::UnsupportedFilesystem),
            "file-too-large" => Some(Self::FileTooLarge),
            _ => None,
        }
    }

    pub(crate) fn from_io_error(error: &std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::PermissionDenied => Self::PermissionDenied,
            std::io::ErrorKind::StorageFull => Self::InsufficientSpace,
            _ => Self::DirectoryUnavailable,
        }
    }

    pub(crate) fn from_app_error(error: &AppError) -> Self {
        match error {
            AppError::DestinationPreflight(failure) => *failure,
            AppError::Io(error) => Self::from_io_error(error),
            AppError::Permission(_) => Self::PermissionDenied,
            _ => Self::DirectoryUnavailable,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    InvalidInput(String),
    #[error("{0}")]
    Storage(String),
    #[error("{0}")]
    Identity(String),
    #[error("{0}")]
    Network(String),
    #[error("{0}")]
    Permission(String),
    #[error("好友当前不在线，请对方启动 Weline Localnet 后重试")]
    OfflinePeer,
    #[error("对方尚未成为好友，请先发送好友申请并等待接受")]
    NotFriend,
    #[error("双方软件版本不兼容，请升级 Weline Localnet 后重试")]
    IncompatibleProtocol,
    #[error("文件完整性校验失败，请重新发送")]
    IntegrityFailure,
    #[error("{0}")]
    DestinationPreflight(DestinationPreflightFailure),
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorPayload {
    pub code: &'static str,
    pub message: String,
}

impl AppError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::Storage(_) => "storage_error",
            Self::Identity(_) => "identity_error",
            Self::Network(_) => "network_error",
            Self::Permission(_) => "permission_error",
            Self::OfflinePeer => "peer_offline",
            Self::NotFriend => "not_friend",
            Self::IncompatibleProtocol => "incompatible_protocol",
            Self::IntegrityFailure => "integrity_failure",
            Self::DestinationPreflight(_) => "destination_preflight_error",
            Self::Io(_) => "io_error",
        }
    }

    pub fn payload(&self) -> ErrorPayload {
        ErrorPayload {
            code: self.code(),
            message: self.to_string(),
        }
    }
}

impl Serialize for AppError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.payload().serialize(serializer)
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(format!(
            "本地数据处理失败，请重新启动 Weline Localnet：{error}"
        ))
    }
}
