use std::{fmt, str::FromStr};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::error::AppError;

pub const MAX_NICKNAME_CHARS: usize = 32;
pub const MAX_TEXT_BYTES: usize = 16 * 1024;
pub const PROTOCOL_VERSION: u16 = 1;

pub const fn default_transfer_protocol() -> u8 {
    1
}

macro_rules! string_enum {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = AppError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(AppError::Storage(format!("本地数据包含未知状态：{value}"))),
                }
            }
        }
    };
}

string_enum!(Platform {
    Windows => "windows",
    Macos => "macos",
    Unknown => "unknown",
});

impl Platform {
    pub const fn current() -> Self {
        if cfg!(target_os = "windows") {
            Self::Windows
        } else if cfg!(target_os = "macos") {
            Self::Macos
        } else {
            Self::Unknown
        }
    }
}

string_enum!(Direction {
    Incoming => "incoming",
    Outgoing => "outgoing",
});

string_enum!(FriendRequestStatus {
    Pending => "pending",
    Accepted => "accepted",
    Rejected => "rejected",
});

string_enum!(MessageKind {
    Text => "text",
    Image => "image",
    File => "file",
});

string_enum!(MessageStatus {
    Sending => "sending",
    Delivered => "delivered",
    Failed => "failed",
});

string_enum!(TransferKind {
    Image => "image",
    File => "file",
});

string_enum!(TransferStatus {
    AwaitingAcceptance => "awaitingAcceptance",
    Transferring => "transferring",
    Paused => "paused",
    Completed => "completed",
    Cancelled => "cancelled",
    Failed => "failed",
});

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalProfile {
    pub peer_id: String,
    pub nickname: String,
    pub platform: Platform,
    pub protocol_version: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerSummary {
    pub peer_id: String,
    pub nickname: String,
    pub platform: Platform,
    pub online: bool,
    pub protocol_version: u16,
    #[serde(default)]
    pub capabilities: Vec<String>,
    pub last_seen: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FriendRequest {
    pub request_id: String,
    pub peer_id: String,
    pub nickname: String,
    pub direction: Direction,
    pub status: FriendRequestStatus,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Friend {
    pub peer_id: String,
    pub nickname: String,
    pub platform: Platform,
    pub online: bool,
    pub added_at: String,
    pub last_seen: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatMessage {
    pub message_id: String,
    pub peer_id: String,
    pub direction: Direction,
    pub kind: MessageKind,
    pub body: Option<String>,
    pub local_path: Option<String>,
    pub file_name: Option<String>,
    pub file_size: Option<u64>,
    pub status: MessageStatus,
    pub error: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferRecord {
    pub transfer_id: String,
    pub peer_id: String,
    pub direction: Direction,
    pub kind: TransferKind,
    pub file_name: String,
    pub file_size: u64,
    pub mime_type: String,
    pub sha256: String,
    pub local_path: Option<String>,
    #[serde(default, skip_serializing)]
    pub destination_reserved: bool,
    #[serde(default, skip_serializing)]
    pub reservation_token: Option<String>,
    #[serde(default = "default_transfer_protocol")]
    pub transfer_protocol: u8,
    #[serde(default)]
    pub chunk_size: u32,
    #[serde(default)]
    pub chunk_count: u32,
    #[serde(default)]
    pub manifest_sha256: Option<String>,
    #[serde(default, skip_serializing)]
    pub partial_path: Option<String>,
    #[serde(default)]
    pub source_modified_ns: Option<u64>,
    #[serde(default, skip_serializing)]
    pub send_claimed: bool,
    pub transferred_bytes: u64,
    pub status: TransferStatus,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferPreferences {
    pub auto_receive_files: bool,
    pub receive_directory: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PresenceSnapshot {
    pub peers: Vec<PeerSummary>,
    pub friends: Vec<Friend>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapSnapshot {
    pub local_profile: Option<LocalProfile>,
    pub transfer_preferences: TransferPreferences,
    pub peers: Vec<PeerSummary>,
    pub friend_requests: Vec<FriendRequest>,
    pub friends: Vec<Friend>,
    pub messages: Vec<ChatMessage>,
    pub transfers: Vec<TransferRecord>,
}

pub fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn validate_nickname(value: &str) -> Result<String, AppError> {
    let nickname = value.trim();
    let character_count = nickname.chars().count();
    if !(1..=MAX_NICKNAME_CHARS).contains(&character_count) {
        return Err(AppError::InvalidInput(format!(
            "昵称需要包含 1–{MAX_NICKNAME_CHARS} 个字符"
        )));
    }
    if nickname.chars().any(char::is_control) {
        return Err(AppError::InvalidInput(
            "昵称不能包含控制字符，请重新输入".to_string(),
        ));
    }
    Ok(nickname.to_string())
}

pub fn validate_text(value: &str) -> Result<String, AppError> {
    let normalized = value.replace("\r\n", "\n").replace('\r', "\n");
    if normalized.trim().is_empty() {
        return Err(AppError::InvalidInput("消息不能为空".to_string()));
    }
    if normalized.len() > MAX_TEXT_BYTES {
        return Err(AppError::InvalidInput(format!(
            "单条消息不能超过 {} KiB",
            MAX_TEXT_BYTES / 1024
        )));
    }
    Ok(normalized)
}
