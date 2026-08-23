use serde::{Deserialize, Serialize};

use crate::domain::{Platform, TransferKind};

pub const CONTROL_PROTOCOL: &str = "/localnet/control/1";
pub const FILE_PROTOCOL: &str = "/localnet/file/1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelloPayload {
    pub version: u16,
    pub nickname: String,
    pub platform: Platform,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferOffer {
    pub transfer_id: String,
    pub kind: TransferKind,
    pub file_name: String,
    pub file_size: u64,
    pub mime_type: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferStreamHeader {
    pub transfer_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ControlRequest {
    Hello {
        version: u16,
        nickname: String,
        platform: Platform,
    },
    FriendRequest {
        request_id: String,
        nickname: String,
    },
    FriendDecision {
        request_id: String,
        accepted: bool,
        nickname: String,
    },
    TextMessage {
        message_id: String,
        sent_at: String,
        body: String,
    },
    TransferOffer {
        offer: TransferOffer,
    },
    TransferDecision {
        transfer_id: String,
        accepted: bool,
    },
    TransferCancel {
        transfer_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ControlResponse {
    Accepted,
    Rejected { code: String, message: String },
    Hello { payload: HelloPayload },
}
