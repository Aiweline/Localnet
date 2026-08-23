use serde::ser::{Serialize, Serializer};

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
    #[error("好友当前不在线，请对方启动 Weline Chat 后重试")]
    OfflinePeer,
    #[error("对方尚未成为好友，请先发送好友申请并等待接受")]
    NotFriend,
    #[error("双方软件版本不兼容，请升级 Weline Chat 后重试")]
    IncompatibleProtocol,
    #[error("文件完整性校验失败，请重新发送")]
    IntegrityFailure,
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
        Self::Storage(format!("本地数据处理失败，请重新启动 Weline Chat：{error}"))
    }
}
