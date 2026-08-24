use serde::{Deserialize, Serialize};

use crate::domain::{Platform, TransferKind, default_transfer_protocol};

pub const CONTROL_PROTOCOL: &str = "/localnet/control/1";
pub const FILE_PROTOCOL: &str = "/localnet/file/1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelloPayload {
    pub version: u16,
    pub nickname: String,
    pub platform: Platform,
    #[serde(default)]
    pub capabilities: Vec<String>,
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
    #[serde(default = "default_transfer_protocol")]
    pub transfer_protocol: u8,
    #[serde(default)]
    pub chunk_size: u32,
    #[serde(default)]
    pub chunk_count: u32,
    #[serde(default)]
    pub manifest_sha256: Option<String>,
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
        #[serde(default)]
        capabilities: Vec<String>,
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

#[cfg(test)]
mod tests {
    use super::{ControlRequest, ControlResponse};

    #[test]
    fn legacy_hello_messages_default_capabilities_to_empty() {
        let request: ControlRequest = serde_json::from_str(
            r#"{"type":"hello","version":1,"nickname":"Legacy","platform":"windows"}"#,
        )
        .expect("deserialize legacy hello request");
        let response: ControlResponse = serde_json::from_str(
            r#"{"type":"hello","payload":{"version":1,"nickname":"Legacy","platform":"windows"}}"#,
        )
        .expect("deserialize legacy hello response");

        let ControlRequest::Hello { capabilities, .. } = request else {
            panic!("expected hello request");
        };
        let ControlResponse::Hello { payload } = response else {
            panic!("expected hello response");
        };

        assert!(capabilities.is_empty());
        assert!(payload.capabilities.is_empty());
    }
}
