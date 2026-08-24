use serde::{Deserialize, Serialize};

use crate::domain::{Platform, TransferKind, default_transfer_protocol};

pub const CONTROL_PROTOCOL: &str = "/localnet/control/1";
pub const FILE_PROTOCOL: &str = "/localnet/file/1";
pub const FILE_PROTOCOL_V2: &str = "/localnet/file/2";

const fn default_file_stream_version() -> u16 {
    1
}

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
    #[serde(default = "default_file_stream_version")]
    pub version: u16,
    #[serde(default)]
    pub start_offset: u64,
    #[serde(default)]
    pub chunk_size: u32,
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
    use super::{
        ControlRequest, ControlResponse, FILE_PROTOCOL, FILE_PROTOCOL_V2, TransferStreamHeader,
    };

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

    #[test]
    fn file_protocol_v2_is_additive_and_legacy_stream_headers_still_decode() {
        assert_eq!(FILE_PROTOCOL, "/localnet/file/1");
        assert_eq!(FILE_PROTOCOL_V2, "/localnet/file/2");

        let legacy: TransferStreamHeader =
            serde_json::from_str(r#"{"transferId":"legacy-transfer"}"#)
                .expect("decode legacy stream header");

        assert_eq!(legacy.transfer_id, "legacy-transfer");
        assert_eq!(legacy.version, 1);
        assert_eq!(legacy.start_offset, 0);
        assert_eq!(legacy.chunk_size, 0);
    }

    #[test]
    fn file_protocol_v2_stream_header_round_trips_resume_geometry() {
        let header = TransferStreamHeader {
            transfer_id: "transfer-v2".to_string(),
            version: 2,
            start_offset: 8_388_608,
            chunk_size: 4_194_304,
        };

        let decoded: TransferStreamHeader =
            serde_json::from_slice(&serde_json::to_vec(&header).expect("encode v2 stream header"))
                .expect("decode v2 stream header");

        assert_eq!(decoded.transfer_id, header.transfer_id);
        assert_eq!(decoded.version, 2);
        assert_eq!(decoded.start_offset, 8_388_608);
        assert_eq!(decoded.chunk_size, 4_194_304);
    }
}
