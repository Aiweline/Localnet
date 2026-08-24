use serde::{Deserialize, Serialize};

use crate::error::AppError;

pub const LEGACY_MAX_FILE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_MAX_FILE_BYTES: u64 = 100 * 1024 * 1024 * 1024;
pub const TRANSFER_CHUNK_BYTES: u32 = 4 * 1024 * 1024;
pub const FILE_RESUME_V2_CAPABILITY: &str = "file-resume-v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferProtocol {
    LegacyV1 = 1,
    ResumableV2 = 2,
}

pub fn select_transfer_protocol(
    capabilities: &[String],
    file_size: u64,
) -> Result<TransferProtocol, AppError> {
    if file_size > DEFAULT_MAX_FILE_BYTES {
        return Err(AppError::InvalidInput(
            "单个文件不能超过 100 GiB".to_string(),
        ));
    }
    if capabilities
        .iter()
        .any(|capability| capability == FILE_RESUME_V2_CAPABILITY)
    {
        return Ok(TransferProtocol::ResumableV2);
    }
    if file_size <= LEGACY_MAX_FILE_BYTES {
        return Ok(TransferProtocol::LegacyV1);
    }
    Err(AppError::InvalidInput(
        "对方设备需要升级 Weline Localnet 才能传输超过 2 GiB 的文件".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        FILE_RESUME_V2_CAPABILITY, LEGACY_MAX_FILE_BYTES, TransferProtocol,
        select_transfer_protocol,
    };

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn upgraded_peer_accepts_exactly_100_gib() {
        let caps = vec![FILE_RESUME_V2_CAPABILITY.to_string()];
        assert_eq!(
            select_transfer_protocol(&caps, 100 * GIB).unwrap(),
            TransferProtocol::ResumableV2
        );
    }

    #[test]
    fn local_policy_rejects_100_gib_plus_one() {
        assert!(
            select_transfer_protocol(&[FILE_RESUME_V2_CAPABILITY.into()], 100 * GIB + 1)
                .unwrap_err()
                .to_string()
                .contains("100 GiB")
        );
    }

    #[test]
    fn legacy_peer_above_2_gib_gets_upgrade_error() {
        assert!(
            select_transfer_protocol(&[], LEGACY_MAX_FILE_BYTES + 1)
                .unwrap_err()
                .to_string()
                .contains("升级")
        );
    }
}
