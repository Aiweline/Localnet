use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard},
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::Digest;

use crate::{
    domain::{
        BootstrapSnapshot, ChatMessage, Direction, Friend, FriendRequest, FriendRequestStatus,
        LocalProfile, MessageKind, MessageStatus, PeerSummary, Platform, PresenceSnapshot,
        TransferKind, TransferPreferences, TransferRecord, TransferStatus,
    },
    error::AppError,
    receive_paths::{
        open_owned_resumable_partial_file, owned_finalization_reservations,
        owned_finalized_receive_path, remove_owned_finalization_marker,
        remove_owned_finalization_stage, remove_owned_partial,
        remove_owned_partial_marker_after_file_cleanup, remove_owned_reservation,
        remove_owned_reservations_in_directory, reserve_resumable_partial,
        resumable_partial_is_owned, resumable_partial_path,
    },
    transfer_manifest::{
        TransferChunk, decode_sha256, expected_chunk_count, expected_chunk_length, manifest_root,
        validate_transfer_metadata,
    },
};

#[derive(Clone)]
pub struct Storage {
    connection: Arc<Mutex<Connection>>,
}

impl Storage {
    pub fn open(path: &Path) -> Result<Self, AppError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;

        let storage = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        storage.migrate()?;
        storage.reset_ephemeral_state()?;
        storage.reconcile_resumable_partials()?;
        storage.cleanup_stale_owned_artifacts()?;
        Ok(storage)
    }

    pub fn load_nickname(&self) -> Result<Option<String>, AppError> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT value FROM settings WHERE key = 'nickname'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn save_profile(&self, profile: &LocalProfile) -> Result<(), AppError> {
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO settings (key, value) VALUES ('nickname', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [&profile.nickname],
        )?;
        Ok(())
    }

    pub fn load_transfer_preferences(
        &self,
        default_directory: &Path,
    ) -> Result<TransferPreferences, AppError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('auto_receive_files', '0')",
            [],
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('receive_directory', ?1)",
            [default_directory.to_string_lossy().as_ref()],
        )?;
        let auto_receive_files: String = transaction.query_row(
            "SELECT value FROM settings WHERE key = 'auto_receive_files'",
            [],
            |row| row.get(0),
        )?;
        let receive_directory: String = transaction.query_row(
            "SELECT value FROM settings WHERE key = 'receive_directory'",
            [],
            |row| row.get(0),
        )?;
        transaction.commit()?;
        let auto_receive_files = match auto_receive_files.as_str() {
            "0" => false,
            "1" => true,
            value => {
                return Err(AppError::Storage(format!("自动接收设置值无效：{value}")));
            }
        };
        Ok(TransferPreferences {
            auto_receive_files,
            receive_directory,
        })
    }

    pub fn save_transfer_preferences(
        &self,
        preferences: &TransferPreferences,
    ) -> Result<(), AppError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO settings (key, value) VALUES ('auto_receive_files', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [if preferences.auto_receive_files {
                "1"
            } else {
                "0"
            }],
        )?;
        transaction.execute(
            "INSERT INTO settings (key, value) VALUES ('receive_directory', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [&preferences.receive_directory],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn save_profile_and_transfer_preferences(
        &self,
        profile: &LocalProfile,
        preferences: &TransferPreferences,
    ) -> Result<(), AppError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO settings (key, value) VALUES ('nickname', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [&profile.nickname],
        )?;
        transaction.execute(
            "INSERT INTO settings (key, value) VALUES ('auto_receive_files', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [if preferences.auto_receive_files {
                "1"
            } else {
                "0"
            }],
        )?;
        transaction.execute(
            "INSERT INTO settings (key, value) VALUES ('receive_directory', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [&preferences.receive_directory],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn upsert_peer(&self, peer: &PeerSummary) -> Result<(), AppError> {
        let connection = self.connection()?;
        let capabilities_json = serde_json::to_string(&peer.capabilities)
            .map_err(|error| AppError::Storage(format!("无法序列化设备能力信息：{error}")))?;
        connection.execute(
            "INSERT INTO peers
               (peer_id, nickname, platform, online, protocol_version, capabilities_json, last_seen)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(peer_id) DO UPDATE SET
               nickname = excluded.nickname,
               platform = excluded.platform,
               online = excluded.online,
               protocol_version = excluded.protocol_version,
               capabilities_json = excluded.capabilities_json,
               last_seen = excluded.last_seen",
            params![
                peer.peer_id,
                peer.nickname,
                peer.platform.as_str(),
                peer.online,
                peer.protocol_version,
                capabilities_json,
                peer.last_seen,
            ],
        )?;
        Ok(())
    }

    pub fn get_peer(&self, peer_id: &str) -> Result<Option<PeerSummary>, AppError> {
        Ok(self
            .list_peers()?
            .into_iter()
            .find(|peer| peer.peer_id == peer_id))
    }

    pub fn set_peer_offline(&self, peer_id: &str, last_seen: &str) -> Result<(), AppError> {
        let connection = self.connection()?;
        connection.execute(
            "UPDATE peers SET online = 0, last_seen = ?2 WHERE peer_id = ?1",
            params![peer_id, last_seen],
        )?;
        Ok(())
    }

    pub fn put_friend_request(&self, request: &FriendRequest) -> Result<(), AppError> {
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO friend_requests
               (request_id, peer_id, nickname, direction, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(request_id) DO UPDATE SET
               nickname = excluded.nickname,
               status = excluded.status,
               updated_at = excluded.updated_at",
            params![
                request.request_id,
                request.peer_id,
                request.nickname,
                request.direction.as_str(),
                request.status.as_str(),
                request.created_at,
                request.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn get_friend_request(&self, request_id: &str) -> Result<Option<FriendRequest>, AppError> {
        Ok(self
            .list_friend_requests()?
            .into_iter()
            .find(|request| request.request_id == request_id))
    }

    pub fn find_pending_friend_request(
        &self,
        peer_id: &str,
        direction: Direction,
    ) -> Result<Option<FriendRequest>, AppError> {
        Ok(self.list_friend_requests()?.into_iter().find(|request| {
            request.peer_id == peer_id
                && request.direction == direction
                && request.status == FriendRequestStatus::Pending
        }))
    }

    pub fn remove_pending_outgoing_friend_request(
        &self,
        request_id: &str,
    ) -> Result<bool, AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "DELETE FROM friend_requests
             WHERE request_id = ?1 AND direction = 'outgoing' AND status = 'pending'",
            [request_id],
        )?;
        Ok(changed == 1)
    }

    pub fn resolve_friend_request(
        &self,
        request_id: &str,
        status: FriendRequestStatus,
        friend: Option<&Friend>,
        updated_at: &str,
    ) -> Result<(), AppError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let changed = transaction.execute(
            "UPDATE friend_requests SET status = ?2, updated_at = ?3
             WHERE request_id = ?1 AND status = 'pending'",
            params![request_id, status.as_str(), updated_at],
        )?;
        if changed == 0 {
            let existing: Option<String> = transaction
                .query_row(
                    "SELECT status FROM friend_requests WHERE request_id = ?1",
                    [request_id],
                    |row| row.get(0),
                )
                .optional()?;
            if existing.as_deref() != Some(status.as_str()) {
                return Err(AppError::InvalidInput(
                    "好友申请不存在或已处理，请刷新后重试".to_string(),
                ));
            }
        }
        if let Some(friend) = friend {
            transaction.execute(
                "INSERT INTO friends (peer_id, nickname, platform, added_at, last_seen)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(peer_id) DO UPDATE SET
                   nickname = excluded.nickname,
                   platform = excluded.platform,
                   last_seen = excluded.last_seen",
                params![
                    friend.peer_id,
                    friend.nickname,
                    friend.platform.as_str(),
                    friend.added_at,
                    friend.last_seen,
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn is_friend(&self, peer_id: &str) -> Result<bool, AppError> {
        let connection = self.connection()?;
        let count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM friends WHERE peer_id = ?1",
            [peer_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn insert_message(&self, message: &ChatMessage) -> Result<bool, AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "INSERT OR IGNORE INTO messages
               (message_id, peer_id, direction, kind, body, local_path, file_name, file_size,
                status, error, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                message.message_id,
                message.peer_id,
                message.direction.as_str(),
                message.kind.as_str(),
                message.body,
                message.local_path,
                message.file_name,
                message.file_size,
                message.status.as_str(),
                message.error,
                message.created_at,
            ],
        )?;
        Ok(changed > 0)
    }

    pub fn update_message_status(
        &self,
        message_id: &str,
        status: MessageStatus,
        error: Option<&str>,
    ) -> Result<(), AppError> {
        let connection = self.connection()?;
        connection.execute(
            "UPDATE messages SET status = ?2, error = ?3 WHERE message_id = ?1",
            params![message_id, status.as_str(), error],
        )?;
        Ok(())
    }

    pub fn get_message(&self, message_id: &str) -> Result<Option<ChatMessage>, AppError> {
        Ok(self
            .list_messages()?
            .into_iter()
            .find(|message| message.message_id == message_id))
    }

    pub fn upsert_transfer(&self, transfer: &TransferRecord) -> Result<(), AppError> {
        let mut transfer = transfer.clone();
        let source_modified_ns = validate_transfer_for_storage(&transfer)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let protected_v2: bool = transaction.query_row(
            "SELECT EXISTS(
               SELECT 1 FROM transfers
               WHERE transfer_id = ?1 AND transfer_protocol = 2
                 AND (send_claimed = 1 OR receive_claimed = 1
                      OR status IN ('paused', 'cancelled', 'failed', 'completed'))
             )",
            [&transfer.transfer_id],
            |row| row.get(0),
        )?;
        if protected_v2 {
            return Ok(());
        }
        Self::reserve_resumable_partial_for_transfer(&mut transfer)?;
        let changed =
            Self::upsert_transfer_in_transaction(&transaction, &transfer, source_modified_ns)?;
        transaction.commit()?;
        debug_assert_eq!(changed, 1);
        Ok(())
    }

    pub fn create_outgoing_transfer_with_manifest(
        &self,
        transfer: &TransferRecord,
        chunks: &[TransferChunk],
    ) -> Result<(), AppError> {
        let source_modified_ns = validate_transfer_for_storage(transfer)?;
        validate_outgoing_manifest(transfer, chunks)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if Self::upsert_transfer_in_transaction(&transaction, transfer, source_modified_ns)? != 1 {
            return Err(AppError::Storage(
                "可恢复发送状态已变化，未替换已保存清单".to_string(),
            ));
        }
        replace_chunks_in_transaction(&transaction, &transfer.transfer_id, chunks)?;
        transaction.commit()?;
        Ok(())
    }

    fn upsert_transfer_in_transaction(
        transaction: &rusqlite::Transaction<'_>,
        transfer: &TransferRecord,
        source_modified_ns: Option<i64>,
    ) -> Result<usize, AppError> {
        let changed = transaction.execute(
            "INSERT INTO transfers
               (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type, sha256,
                local_path, destination_reserved, reservation_token, transfer_protocol, chunk_size,
                chunk_count, manifest_sha256, partial_path, source_modified_ns, send_claimed,
                transferred_bytes, status, error, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                     ?17, ?18, ?19, ?20, ?21, ?22, ?23)
             ON CONFLICT(transfer_id) DO UPDATE SET
               local_path = excluded.local_path,
               destination_reserved = excluded.destination_reserved,
               reservation_token = excluded.reservation_token,
               transfer_protocol = excluded.transfer_protocol,
               chunk_size = excluded.chunk_size,
               chunk_count = excluded.chunk_count,
               manifest_sha256 = excluded.manifest_sha256,
               partial_path = excluded.partial_path,
               source_modified_ns = excluded.source_modified_ns,
               send_claimed = excluded.send_claimed,
               transferred_bytes = excluded.transferred_bytes,
               status = excluded.status,
               error = excluded.error,
               updated_at = excluded.updated_at
             WHERE NOT (
               transfers.transfer_protocol = 2
               AND (transfers.send_claimed = 1 OR transfers.receive_claimed = 1
                    OR transfers.status IN ('cancelled', 'failed', 'completed'))
             )",
            params![
                transfer.transfer_id,
                transfer.peer_id,
                transfer.direction.as_str(),
                transfer.kind.as_str(),
                transfer.file_name,
                transfer.file_size,
                transfer.mime_type,
                transfer.sha256,
                transfer.local_path,
                transfer.destination_reserved,
                transfer.reservation_token,
                transfer.transfer_protocol,
                transfer.chunk_size,
                transfer.chunk_count,
                transfer.manifest_sha256,
                transfer.partial_path,
                source_modified_ns,
                transfer.send_claimed,
                transfer.transferred_bytes,
                transfer.status.as_str(),
                transfer.error,
                transfer.created_at,
                transfer.updated_at,
            ],
        )?;
        Ok(changed)
    }

    #[allow(dead_code)] // Public crate API for manifest replacement after later source updates.
    pub fn replace_outgoing_chunks(
        &self,
        transfer_id: &str,
        chunks: &[TransferChunk],
    ) -> Result<(), AppError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let metadata = load_manifest_metadata(&transaction, transfer_id)?
            .ok_or_else(|| AppError::Storage("找不到需要保存分块的传输记录".to_string()))?;
        validate_stored_manifest_metadata(&metadata, Direction::Outgoing)?;
        validate_chunk_geometry(
            metadata.file_size,
            metadata.chunk_size,
            metadata.chunk_count,
            metadata.manifest_sha256.as_deref(),
            chunks,
        )?;
        replace_chunks_in_transaction(&transaction, transfer_id, chunks)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn list_transfer_chunks(&self, transfer_id: &str) -> Result<Vec<TransferChunk>, AppError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT chunk_index, chunk_length, sha256 FROM transfer_chunks
             WHERE transfer_id = ?1 ORDER BY chunk_index ASC",
        )?;
        let rows = statement.query_map([transfer_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (index, length, sha256) = row?;
            let sha256: [u8; 32] = sha256.try_into().map_err(|_| {
                AppError::Storage("传输分块哈希长度无效，必须为 32 字节".to_string())
            })?;
            let length = u32::try_from(length)
                .map_err(|_| AppError::Storage("传输分块长度无效".to_string()))?;
            if length == 0 {
                return Err(AppError::Storage("传输分块长度无效".to_string()));
            }
            Ok(TransferChunk {
                index: u32::try_from(index)
                    .map_err(|_| AppError::Storage("传输分块索引无效".to_string()))?,
                length,
                sha256,
            })
        })
        .collect()
    }

    pub fn commit_received_chunk(
        &self,
        transfer_id: &str,
        peer_id: &str,
        chunk: &TransferChunk,
        committed_bytes: u64,
    ) -> Result<bool, AppError> {
        let committed_bytes = i64::try_from(committed_bytes)
            .map_err(|_| AppError::Storage("已接收字节数超出存储范围".to_string()))?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let active: bool = transaction.query_row(
            "SELECT EXISTS(
               SELECT 1 FROM transfers
               WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
                 AND transfer_protocol = 2 AND status = 'transferring'
                 AND receive_claimed = 1
             )",
            params![transfer_id, peer_id],
            |row| row.get(0),
        )?;
        if !active {
            return Ok(false);
        }
        let Some(metadata) = load_manifest_metadata(&transaction, transfer_id)? else {
            return Ok(false);
        };
        if metadata.transfer_protocol != 2
            || validate_stored_manifest_metadata(&metadata, Direction::Incoming).is_err()
        {
            return Ok(false);
        }
        let file_size = metadata.file_size;
        let transferred_bytes = metadata.transferred_bytes;
        let (chunk_rows, last_index): (i64, Option<i64>) = transaction.query_row(
            "SELECT COUNT(*), MAX(chunk_index) FROM transfer_chunks WHERE transfer_id = ?1",
            [transfer_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let expected_index = match last_index {
            Some(last_index) => last_index
                .checked_add(1)
                .ok_or_else(|| AppError::Storage("传输分块索引溢出".to_string()))?,
            None => 0,
        };
        let expected_index_u32 = u32::try_from(expected_index).ok();
        if expected_index != chunk_rows
            || expected_index_u32.is_none()
            || i64::from(chunk.index) != expected_index
            || file_size < 0
            || transferred_bytes < 0
        {
            return Ok(false);
        }
        let expected_index_u32 = expected_index_u32.expect("checked above");
        let file_size_u64 = u64::try_from(file_size)
            .map_err(|_| AppError::Storage("传输文件大小无效".to_string()))?;
        let expected_length =
            expected_chunk_length(file_size_u64, metadata.chunk_size, expected_index_u32)
                .map_err(storage_metadata_error)?;
        let expected_bytes = u64::from(expected_index_u32)
            .checked_mul(u64::from(metadata.chunk_size))
            .and_then(|offset| offset.checked_add(u64::from(expected_length)))
            .ok_or_else(|| AppError::Storage("已接收字节数溢出".to_string()))?;
        if chunk.length != expected_length
            || i64::try_from(expected_bytes).ok() != Some(committed_bytes)
            || u64::try_from(transferred_bytes).ok()
                != expected_bytes.checked_sub(u64::from(expected_length))
            || committed_bytes > file_size
        {
            return Ok(false);
        }
        transaction.execute(
            "INSERT INTO transfer_chunks (transfer_id, chunk_index, chunk_length, sha256)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                transfer_id,
                i64::from(chunk.index),
                i64::from(chunk.length),
                chunk.sha256.as_slice(),
            ],
        )?;
        if transaction.execute(
            "UPDATE transfers SET transferred_bytes = ?3
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND transfer_protocol = 2 AND status = 'transferring'
               AND receive_claimed = 1 AND transferred_bytes = ?4",
            params![transfer_id, peer_id, committed_bytes, transferred_bytes],
        )? != 1
        {
            return Err(AppError::Storage("无法更新传输进度".to_string()));
        }
        if expected_index_u32 + 1 == metadata.chunk_count {
            let chunks = list_transfer_chunks_in_transaction(&transaction, transfer_id)?;
            let expected_root = decode_sha256(
                metadata
                    .manifest_sha256
                    .as_deref()
                    .ok_or_else(|| AppError::Storage("缺少传输清单哈希".to_string()))?,
            )
            .map_err(storage_metadata_error)?;
            if manifest_root(&chunks) != expected_root {
                return Err(AppError::IntegrityFailure);
            }
        }
        transaction.commit()?;
        Ok(true)
    }

    pub fn get_transfer(&self, transfer_id: &str) -> Result<Option<TransferRecord>, AppError> {
        Ok(self
            .list_transfers()?
            .into_iter()
            .find(|transfer| transfer.transfer_id == transfer_id))
    }

    pub fn try_claim_incoming_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
    ) -> Result<bool, AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET receive_claimed = 1,
                 status = CASE WHEN transfer_protocol = 2 THEN 'transferring' ELSE status END,
                 error = CASE WHEN transfer_protocol = 2 THEN NULL ELSE error END,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND receive_claimed = 0
               AND ((transfer_protocol = 2 AND status IN ('paused', 'transferring'))
                    OR (transfer_protocol = 1 AND status = 'transferring'))",
            params![transfer_id, peer_id],
        )?;
        Ok(changed == 1)
    }

    pub fn try_claim_outgoing_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
    ) -> Result<bool, AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET send_claimed = 1, status = 'transferring', error = NULL,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'outgoing'
               AND transfer_protocol = 2 AND status IN ('paused', 'transferring')
               AND send_claimed = 0",
            params![transfer_id, peer_id],
        )?;
        Ok(changed == 1)
    }

    pub fn commit_claimed_outgoing_progress(
        &self,
        transfer_id: &str,
        peer_id: &str,
        previous_bytes: u64,
        acknowledged_bytes: u64,
    ) -> Result<bool, AppError> {
        let previous_bytes = i64::try_from(previous_bytes)
            .map_err(|_| AppError::Storage("发送进度超出存储范围".to_string()))?;
        let acknowledged_bytes = i64::try_from(acknowledged_bytes)
            .map_err(|_| AppError::Storage("发送进度超出存储范围".to_string()))?;
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET transferred_bytes = ?4,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'outgoing'
               AND transfer_protocol = 2 AND status = 'transferring' AND send_claimed = 1
               AND transferred_bytes = ?3 AND ?4 > ?3 AND ?4 <= file_size
               AND (?4 = file_size OR ?4 % chunk_size = 0)",
            params![transfer_id, peer_id, previous_bytes, acknowledged_bytes],
        )?;
        Ok(changed == 1)
    }

    pub fn try_pause_claimed_incoming_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
        error: &str,
    ) -> Result<bool, AppError> {
        self.try_transition_claimed_incoming_transfer(
            transfer_id,
            peer_id,
            TransferStatus::Paused,
            Some(error),
        )
    }

    pub fn try_fail_claimed_incoming_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
        error: &str,
    ) -> Result<bool, AppError> {
        let changed = self.try_transition_claimed_incoming_transfer(
            transfer_id,
            peer_id,
            TransferStatus::Failed,
            Some(error),
        )?;
        if changed {
            self.cleanup_owned_transfer_artifacts(transfer_id)?;
        }
        Ok(changed)
    }

    pub fn try_complete_claimed_incoming_transfer(
        &self,
        transfer: &TransferRecord,
    ) -> Result<bool, AppError> {
        if transfer.direction != Direction::Incoming
            || transfer.transfer_protocol != 2
            || transfer.status != TransferStatus::Completed
            || transfer.local_path.is_none()
            || transfer.transferred_bytes != transfer.file_size
        {
            return Err(AppError::InvalidInput("可恢复接收完成状态无效".to_string()));
        }
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET local_path = ?3, destination_reserved = ?4, reservation_token = ?5,
                 partial_path = ?6, transferred_bytes = file_size, status = 'completed',
                 error = NULL, receive_claimed = 0, updated_at = ?7
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND transfer_protocol = 2 AND status = 'transferring' AND receive_claimed = 1",
            params![
                transfer.transfer_id,
                transfer.peer_id,
                transfer.local_path,
                transfer.destination_reserved,
                transfer.reservation_token,
                transfer.partial_path,
                transfer.updated_at,
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn try_switch_claimed_incoming_destination(
        &self,
        transfer_id: &str,
        peer_id: &str,
        previous_destination: &Path,
        next_destination: &Path,
        reservation_token: &str,
    ) -> Result<bool, AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers SET local_path = ?4,
                                  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND transfer_protocol = 2 AND status = 'transferring' AND receive_claimed = 1
               AND destination_reserved = 1 AND reservation_token = ?3
               AND local_path = ?5",
            params![
                transfer_id,
                peer_id,
                reservation_token,
                next_destination.to_string_lossy(),
                previous_destination.to_string_lossy(),
            ],
        )?;
        Ok(changed == 1)
    }

    fn try_transition_claimed_incoming_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
        status: TransferStatus,
        error: Option<&str>,
    ) -> Result<bool, AppError> {
        if !matches!(status, TransferStatus::Paused | TransferStatus::Failed) {
            return Err(AppError::InvalidInput(
                "接收文件的已占用状态转换无效".to_string(),
            ));
        }
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET status = ?3, error = ?4, receive_claimed = 0,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND transfer_protocol = 2 AND status = 'transferring' AND receive_claimed = 1",
            params![transfer_id, peer_id, status.as_str(), error],
        )?;
        Ok(changed == 1)
    }

    pub fn try_pause_claimed_outgoing_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
        error: &str,
    ) -> Result<bool, AppError> {
        self.try_transition_claimed_outgoing_transfer(
            transfer_id,
            peer_id,
            TransferStatus::Paused,
            Some(error),
        )
    }

    pub fn try_fail_claimed_outgoing_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
        error: &str,
    ) -> Result<bool, AppError> {
        let changed = self.try_transition_claimed_outgoing_transfer(
            transfer_id,
            peer_id,
            TransferStatus::Failed,
            Some(error),
        )?;
        if changed {
            self.delete_terminal_transfer_chunks(transfer_id)?;
        }
        Ok(changed)
    }

    pub fn try_complete_claimed_outgoing_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
    ) -> Result<bool, AppError> {
        self.try_transition_claimed_outgoing_transfer(
            transfer_id,
            peer_id,
            TransferStatus::Completed,
            None,
        )
    }

    fn try_transition_claimed_outgoing_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
        status: TransferStatus,
        error: Option<&str>,
    ) -> Result<bool, AppError> {
        if !matches!(
            status,
            TransferStatus::Paused | TransferStatus::Failed | TransferStatus::Completed
        ) {
            return Err(AppError::InvalidInput(
                "发送文件的已占用状态转换无效".to_string(),
            ));
        }
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET status = ?3, error = ?4, send_claimed = 0,
                 transferred_bytes = CASE WHEN ?3 = 'completed' THEN file_size
                                           ELSE transferred_bytes END,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'outgoing'
               AND transfer_protocol = 2 AND status = 'transferring' AND send_claimed = 1",
            params![transfer_id, peer_id, status.as_str(), error],
        )?;
        Ok(changed == 1)
    }

    #[allow(dead_code)] // Task 7 routes resumable cancellation through this CAS.
    pub fn try_cancel_unclaimed_outgoing_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
        error: &str,
    ) -> Result<bool, AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET status = 'cancelled', error = ?3, send_claimed = 0,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'outgoing'
               AND transfer_protocol = 2
               AND status IN ('awaitingAcceptance', 'transferring', 'paused')
               AND send_claimed = 0",
            params![transfer_id, peer_id, error],
        )?;
        drop(connection);
        if changed == 1 {
            self.delete_terminal_transfer_chunks(transfer_id)?;
        }
        Ok(changed == 1)
    }

    #[allow(dead_code)] // Task 7 queries resumable sends after reconnect.
    pub fn list_resumable_outgoing(&self, peer_id: &str) -> Result<Vec<TransferRecord>, AppError> {
        Ok(self
            .list_transfers()?
            .into_iter()
            .filter(|transfer| {
                transfer.peer_id == peer_id
                    && transfer.direction == Direction::Outgoing
                    && transfer.transfer_protocol == 2
                    && transfer.status == TransferStatus::Paused
            })
            .collect())
    }

    pub fn release_incoming_transfer_claim(&self, transfer_id: &str) -> Result<(), AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers SET receive_claimed = 0
             WHERE transfer_id = ?1 AND transfer_protocol = 1 AND receive_claimed = 1",
            [transfer_id],
        )?;
        if changed != 1 {
            return Err(AppError::InvalidInput(
                "可恢复传输必须在状态转换时原子释放接收占用".to_string(),
            ));
        }
        Ok(())
    }

    pub fn try_accept_incoming_transfer(
        &self,
        transfer: &TransferRecord,
    ) -> Result<bool, AppError> {
        if transfer.direction != Direction::Incoming
            || transfer.status != TransferStatus::Transferring
            || !transfer.destination_reserved
            || transfer.local_path.is_none()
            || transfer.reservation_token.is_none()
        {
            return Err(AppError::InvalidInput("接收文件状态不允许确认".to_string()));
        }
        if !matches!(transfer.transfer_protocol, 1 | 2) {
            return Err(AppError::InvalidInput("接收文件协议版本无效".to_string()));
        }
        let mut accepted = transfer.clone();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::reserve_resumable_partial_for_transfer(&mut accepted)?;
        let changed = transaction.execute(
            "UPDATE transfers
             SET local_path = ?3, destination_reserved = 1, reservation_token = ?4,
                 partial_path = ?5, transferred_bytes = 0, status = 'transferring',
                 error = NULL, updated_at = ?6
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND transfer_protocol = ?7
               AND status = 'awaitingAcceptance' AND receive_claimed = 0",
            params![
                transfer.transfer_id,
                transfer.peer_id,
                transfer.local_path,
                transfer.reservation_token,
                accepted.partial_path,
                transfer.updated_at,
                transfer.transfer_protocol,
            ],
        )?;
        transaction.commit()?;
        Ok(changed == 1)
    }

    pub fn try_cancel_unclaimed_incoming_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
        transfer_protocol: u8,
        error: &str,
    ) -> Result<bool, AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET status = 'cancelled', error = ?4, receive_claimed = 0,
                  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND status IN ('awaitingAcceptance', 'transferring', 'paused')
               AND transfer_protocol = ?3 AND ?3 IN (1, 2)
               AND receive_claimed = 0",
            params![transfer_id, peer_id, transfer_protocol, error],
        )?;
        drop(connection);
        if changed == 1 {
            self.cleanup_owned_transfer_artifacts(transfer_id)?;
        }
        Ok(changed == 1)
    }

    fn reserve_resumable_partial_for_transfer(
        transfer: &mut TransferRecord,
    ) -> Result<(), AppError> {
        if transfer.direction != Direction::Incoming
            || transfer.transfer_protocol != 2
            || !matches!(
                transfer.status,
                TransferStatus::Transferring | TransferStatus::Paused
            )
        {
            return Ok(());
        }
        let (Some(destination), Some(token)) = (
            transfer.local_path.as_deref(),
            transfer.reservation_token.as_deref(),
        ) else {
            return Ok(());
        };
        if let Some(partial) = transfer.partial_path.as_deref() {
            if resumable_partial_is_owned(
                Path::new(partial),
                Path::new(destination),
                &transfer.transfer_id,
                token,
            )? {
                return Ok(());
            }
        }
        transfer.partial_path = Some(
            reserve_resumable_partial(Path::new(destination), &transfer.transfer_id, token)?
                .to_string_lossy()
                .into_owned(),
        );
        Ok(())
    }

    pub fn try_transition_outgoing_awaiting(
        &self,
        transfer_id: &str,
        peer_id: &str,
        status: TransferStatus,
        error: Option<&str>,
    ) -> Result<bool, AppError> {
        if !matches!(
            status,
            TransferStatus::Transferring | TransferStatus::Cancelled
        ) {
            return Err(AppError::InvalidInput("发送文件状态转换无效".to_string()));
        }
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET status = ?3, error = ?4,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'outgoing'
               AND transfer_protocol IN (1, 2)
               AND status = 'awaitingAcceptance'",
            params![transfer_id, peer_id, status.as_str(), error],
        )?;
        Ok(changed == 1)
    }

    pub fn snapshot(
        &self,
        local_profile: Option<LocalProfile>,
        default_receive_directory: &Path,
    ) -> Result<BootstrapSnapshot, AppError> {
        Ok(BootstrapSnapshot {
            local_profile,
            transfer_preferences: self.load_transfer_preferences(default_receive_directory)?,
            peers: self.list_peers()?,
            friend_requests: self.list_friend_requests()?,
            friends: self.list_friends()?,
            messages: self.list_messages()?,
            transfers: self.list_transfers()?,
        })
    }

    pub fn presence_snapshot(&self) -> Result<PresenceSnapshot, AppError> {
        Ok(PresenceSnapshot {
            peers: self.list_peers()?,
            friends: self.list_friends()?,
        })
    }

    fn list_peers(&self) -> Result<Vec<PeerSummary>, AppError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT peer_id, nickname, platform, online, protocol_version, capabilities_json, last_seen
             FROM peers ORDER BY online DESC, nickname COLLATE NOCASE",
        )?;
        let rows = statement.query_map([], |row| {
            let platform: String = row.get(2)?;
            let capabilities_json: String = row.get(5)?;
            Ok((
                row.get(0)?,
                row.get(1)?,
                platform,
                row.get(3)?,
                row.get(4)?,
                capabilities_json,
                row.get(6)?,
            ))
        })?;
        rows.map(|row| {
            let (
                peer_id,
                nickname,
                platform,
                online,
                protocol_version,
                capabilities_json,
                last_seen,
            ) = row?;
            Ok(PeerSummary {
                peer_id,
                nickname,
                platform: Platform::from_str(&platform)?,
                online,
                protocol_version,
                capabilities: serde_json::from_str(&capabilities_json)
                    .map_err(|error| AppError::Storage(format!("本地设备能力信息无效：{error}")))?,
                last_seen,
            })
        })
        .collect()
    }

    fn list_friend_requests(&self) -> Result<Vec<FriendRequest>, AppError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT request_id, peer_id, nickname, direction, status, created_at, updated_at
             FROM friend_requests ORDER BY created_at DESC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        })?;
        rows.map(|row| {
            let (request_id, peer_id, nickname, direction, status, created_at, updated_at) = row?;
            Ok(FriendRequest {
                request_id,
                peer_id,
                nickname,
                direction: Direction::from_str(&direction)?,
                status: FriendRequestStatus::from_str(&status)?,
                created_at,
                updated_at,
            })
        })
        .collect()
    }

    fn list_friends(&self) -> Result<Vec<Friend>, AppError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT f.peer_id, f.nickname, f.platform, COALESCE(p.online, 0),
                    f.added_at, COALESCE(p.last_seen, f.last_seen)
             FROM friends f LEFT JOIN peers p ON p.peer_id = f.peer_id
             ORDER BY COALESCE(p.online, 0) DESC, f.nickname COLLATE NOCASE",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get::<_, String>(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })?;
        rows.map(|row| {
            let (peer_id, nickname, platform, online, added_at, last_seen) = row?;
            Ok(Friend {
                peer_id,
                nickname,
                platform: Platform::from_str(&platform)?,
                online,
                added_at,
                last_seen,
            })
        })
        .collect()
    }

    fn list_messages(&self) -> Result<Vec<ChatMessage>, AppError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT message_id, peer_id, direction, kind, body, local_path, file_name, file_size,
                    status, error, created_at
             FROM messages ORDER BY created_at ASC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get::<_, String>(8)?,
                row.get(9)?,
                row.get(10)?,
            ))
        })?;
        rows.map(|row| {
            let (
                message_id,
                peer_id,
                direction,
                kind,
                body,
                local_path,
                file_name,
                file_size,
                status,
                error,
                created_at,
            ) = row?;
            Ok(ChatMessage {
                message_id,
                peer_id,
                direction: Direction::from_str(&direction)?,
                kind: MessageKind::from_str(&kind)?,
                body,
                local_path,
                file_name,
                file_size,
                status: MessageStatus::from_str(&status)?,
                error,
                created_at,
            })
        })
        .collect()
    }

    fn list_transfers(&self) -> Result<Vec<TransferRecord>, AppError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                    sha256, local_path, destination_reserved, reservation_token, transfer_protocol,
                    chunk_size, chunk_count, manifest_sha256, partial_path, source_modified_ns,
                    send_claimed, transferred_bytes, status, error, created_at, updated_at
             FROM transfers ORDER BY created_at ASC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
                row.get(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, i64>(12)?,
                row.get::<_, i64>(13)?,
                row.get(14)?,
                row.get(15)?,
                row.get::<_, Option<i64>>(16)?,
                row.get(17)?,
                row.get(18)?,
                row.get::<_, String>(19)?,
                row.get(20)?,
                row.get(21)?,
                row.get(22)?,
            ))
        })?;
        rows.map(|row| {
            let (
                transfer_id,
                peer_id,
                direction,
                kind,
                file_name,
                file_size,
                mime_type,
                sha256,
                local_path,
                destination_reserved,
                reservation_token,
                transfer_protocol,
                chunk_size,
                chunk_count,
                manifest_sha256,
                partial_path,
                source_modified_ns,
                send_claimed,
                transferred_bytes,
                status,
                error,
                created_at,
                updated_at,
            ) = row?;
            Ok(TransferRecord {
                transfer_id,
                peer_id,
                direction: Direction::from_str(&direction)?,
                kind: TransferKind::from_str(&kind)?,
                file_name,
                file_size,
                mime_type,
                sha256,
                local_path,
                destination_reserved,
                reservation_token,
                transfer_protocol: u8::try_from(transfer_protocol)
                    .map_err(|_| AppError::Storage("本地传输协议版本无效".to_string()))?,
                chunk_size: u32::try_from(chunk_size)
                    .map_err(|_| AppError::Storage("本地分块大小无效".to_string()))?,
                chunk_count: u32::try_from(chunk_count)
                    .map_err(|_| AppError::Storage("本地分块数量无效".to_string()))?,
                manifest_sha256: validate_loaded_optional_sha256(manifest_sha256)?,
                partial_path,
                source_modified_ns: source_modified_ns
                    .map(|value| {
                        u64::try_from(value)
                            .map_err(|_| AppError::Storage("本地源文件修改时间无效".to_string()))
                    })
                    .transpose()?,
                send_claimed,
                transferred_bytes,
                status: TransferStatus::from_str(&status)?,
                error,
                created_at,
                updated_at,
            })
        })
        .collect()
    }

    fn migrate(&self) -> Result<(), AppError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS settings (
               key TEXT PRIMARY KEY NOT NULL,
               value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS peers (
               peer_id TEXT PRIMARY KEY NOT NULL,
               nickname TEXT NOT NULL,
               platform TEXT NOT NULL,
               online INTEGER NOT NULL DEFAULT 0,
               protocol_version INTEGER NOT NULL,
               capabilities_json TEXT NOT NULL DEFAULT '[]',
               last_seen TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS friend_requests (
               request_id TEXT PRIMARY KEY NOT NULL,
               peer_id TEXT NOT NULL,
               nickname TEXT NOT NULL,
               direction TEXT NOT NULL,
               status TEXT NOT NULL,
               created_at TEXT NOT NULL,
               updated_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_friend_requests_peer ON friend_requests(peer_id);
             CREATE TABLE IF NOT EXISTS friends (
               peer_id TEXT PRIMARY KEY NOT NULL,
               nickname TEXT NOT NULL,
               platform TEXT NOT NULL,
               added_at TEXT NOT NULL,
               last_seen TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS messages (
               message_id TEXT PRIMARY KEY NOT NULL,
               peer_id TEXT NOT NULL,
               direction TEXT NOT NULL,
               kind TEXT NOT NULL,
               body TEXT,
               local_path TEXT,
               file_name TEXT,
               file_size INTEGER,
               status TEXT NOT NULL,
               error TEXT,
               created_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_messages_peer_time ON messages(peer_id, created_at);
             CREATE TABLE IF NOT EXISTS transfers (
               transfer_id TEXT PRIMARY KEY NOT NULL,
               peer_id TEXT NOT NULL,
               direction TEXT NOT NULL,
               kind TEXT NOT NULL,
               file_name TEXT NOT NULL,
               file_size INTEGER NOT NULL,
               mime_type TEXT NOT NULL,
               sha256 TEXT NOT NULL,
               local_path TEXT,
               destination_reserved INTEGER NOT NULL DEFAULT 0,
               reservation_token TEXT,
               receive_claimed INTEGER NOT NULL DEFAULT 0,
               transfer_protocol INTEGER NOT NULL DEFAULT 1,
               chunk_size INTEGER NOT NULL DEFAULT 0,
               chunk_count INTEGER NOT NULL DEFAULT 0,
               manifest_sha256 TEXT,
               partial_path TEXT,
               source_modified_ns INTEGER,
               send_claimed INTEGER NOT NULL DEFAULT 0,
               transferred_bytes INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL,
               error TEXT,
               created_at TEXT NOT NULL,
               updated_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_transfers_peer_time ON transfers(peer_id, created_at);
             CREATE TABLE IF NOT EXISTS transfer_chunks (
               transfer_id TEXT NOT NULL,
               chunk_index INTEGER NOT NULL,
               chunk_length INTEGER NOT NULL,
               sha256 BLOB NOT NULL CHECK(typeof(sha256) = 'blob' AND length(sha256) = 32),
               PRIMARY KEY (transfer_id, chunk_index),
               FOREIGN KEY (transfer_id) REFERENCES transfers(transfer_id) ON DELETE CASCADE
             );
             CREATE TABLE IF NOT EXISTS transfer_cleanup (
               transfer_id TEXT PRIMARY KEY NOT NULL,
               destination TEXT,
               partial_path TEXT,
               reservation_token TEXT,
               destination_reserved INTEGER NOT NULL,
               transfer_protocol INTEGER NOT NULL,
               status TEXT NOT NULL
             );",
        )?;
        let has_destination_reserved: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transfers')
             WHERE name = 'destination_reserved'",
            [],
            |row| row.get(0),
        )?;
        if has_destination_reserved == 0 {
            transaction.execute(
                "ALTER TABLE transfers
                 ADD COLUMN destination_reserved INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        let has_reservation_token: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transfers')
             WHERE name = 'reservation_token'",
            [],
            |row| row.get(0),
        )?;
        if has_reservation_token == 0 {
            transaction.execute(
                "ALTER TABLE transfers ADD COLUMN reservation_token TEXT",
                [],
            )?;
        }
        let has_receive_claimed: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transfers')
             WHERE name = 'receive_claimed'",
            [],
            |row| row.get(0),
        )?;
        if has_receive_claimed == 0 {
            transaction.execute(
                "ALTER TABLE transfers
                 ADD COLUMN receive_claimed INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        let has_transfer_protocol: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transfers')
             WHERE name = 'transfer_protocol'",
            [],
            |row| row.get(0),
        )?;
        if has_transfer_protocol == 0 {
            transaction.execute(
                "ALTER TABLE transfers
                 ADD COLUMN transfer_protocol INTEGER NOT NULL DEFAULT 1",
                [],
            )?;
        }
        let has_chunk_size: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transfers') WHERE name = 'chunk_size'",
            [],
            |row| row.get(0),
        )?;
        if has_chunk_size == 0 {
            transaction.execute(
                "ALTER TABLE transfers ADD COLUMN chunk_size INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        let has_chunk_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transfers') WHERE name = 'chunk_count'",
            [],
            |row| row.get(0),
        )?;
        if has_chunk_count == 0 {
            transaction.execute(
                "ALTER TABLE transfers ADD COLUMN chunk_count INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        let has_manifest_sha256: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transfers')
             WHERE name = 'manifest_sha256'",
            [],
            |row| row.get(0),
        )?;
        if has_manifest_sha256 == 0 {
            transaction.execute("ALTER TABLE transfers ADD COLUMN manifest_sha256 TEXT", [])?;
        }
        let has_partial_path: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transfers') WHERE name = 'partial_path'",
            [],
            |row| row.get(0),
        )?;
        if has_partial_path == 0 {
            transaction.execute("ALTER TABLE transfers ADD COLUMN partial_path TEXT", [])?;
        }
        let has_source_modified_ns: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transfers')
             WHERE name = 'source_modified_ns'",
            [],
            |row| row.get(0),
        )?;
        if has_source_modified_ns == 0 {
            transaction.execute(
                "ALTER TABLE transfers ADD COLUMN source_modified_ns INTEGER",
                [],
            )?;
        }
        let has_send_claimed: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('transfers') WHERE name = 'send_claimed'",
            [],
            |row| row.get(0),
        )?;
        if has_send_claimed == 0 {
            transaction.execute(
                "ALTER TABLE transfers ADD COLUMN send_claimed INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        let chunk_table_sql: Option<String> = transaction
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'transfer_chunks'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let requires_chunk_table_rebuild = chunk_table_sql.is_some_and(|sql| {
            let normalized: String = sql
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect::<String>()
                .to_ascii_lowercase();
            !normalized.contains("typeof(sha256)='blob'andlength(sha256)=32")
        });
        if requires_chunk_table_rebuild {
            let invalid_chunk_hashes: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM transfer_chunks
                 WHERE typeof(sha256) != 'blob' OR length(sha256) != 32",
                [],
                |row| row.get(0),
            )?;
            if invalid_chunk_hashes != 0 {
                return Err(AppError::Storage(format!(
                    "检测到 {invalid_chunk_hashes} 条旧版传输分块哈希无效；迁移已回滚，请先保留数据并执行恢复"
                )));
            }
            transaction.execute_batch(
                "ALTER TABLE transfer_chunks RENAME TO transfer_chunks_legacy;
                 CREATE TABLE transfer_chunks (
                   transfer_id TEXT NOT NULL,
                   chunk_index INTEGER NOT NULL,
                   chunk_length INTEGER NOT NULL,
                   sha256 BLOB NOT NULL CHECK(typeof(sha256) = 'blob' AND length(sha256) = 32),
                   PRIMARY KEY (transfer_id, chunk_index),
                   FOREIGN KEY (transfer_id) REFERENCES transfers(transfer_id) ON DELETE CASCADE
                 );
                 INSERT INTO transfer_chunks (transfer_id, chunk_index, chunk_length, sha256)
                   SELECT transfer_id, chunk_index, chunk_length, sha256
                   FROM transfer_chunks_legacy;
                 DROP TABLE transfer_chunks_legacy;",
            )?;
        }
        let has_capabilities_json: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('peers')
             WHERE name = 'capabilities_json'",
            [],
            |row| row.get(0),
        )?;
        if has_capabilities_json == 0 {
            transaction.execute(
                "ALTER TABLE peers
                 ADD COLUMN capabilities_json TEXT NOT NULL DEFAULT '[]'",
                [],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn reset_ephemeral_state(&self) -> Result<(), AppError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute("UPDATE peers SET online = 0", [])?;
        transaction.execute(
            "UPDATE messages SET status = 'failed', error = '应用重新启动，请重试发送'
             WHERE status = 'sending'",
            [],
        )?;
        transaction.execute(
            "UPDATE transfers
             SET status = 'paused',
                 error = CASE WHEN status = 'transferring'
                              THEN '应用重新启动，等待自动恢复'
                              ELSE error END,
                 receive_claimed = 0, send_claimed = 0,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_protocol = 2 AND status IN ('transferring', 'paused')",
            [],
        )?;
        transaction.execute(
            "UPDATE transfers SET status = 'failed', error = '应用重新启动，请重试传输',
                                  receive_claimed = 0, send_claimed = 0,
                                  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_protocol = 1 AND status = 'transferring'",
            [],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn reconcile_resumable_partials(&self) -> Result<(), AppError> {
        let records = {
            let connection = self.connection()?;
            let mut statement = connection.prepare(
                "SELECT transfer_id, peer_id, local_path, partial_path, file_size, chunk_size,
                        chunk_count, manifest_sha256, transferred_bytes, sha256,
                        destination_reserved, reservation_token
                 FROM transfers
                 WHERE direction = 'incoming' AND transfer_protocol = 2 AND status = 'paused'",
            )?;
            statement
                .query_map([], |row| {
                    Ok(PartialRecoveryRecord {
                        transfer_id: row.get(0)?,
                        peer_id: row.get(1)?,
                        destination: row.get::<_, Option<String>>(2)?.map(PathBuf::from),
                        partial: row.get::<_, Option<String>>(3)?.map(PathBuf::from),
                        file_size: row.get(4)?,
                        chunk_size: row.get(5)?,
                        chunk_count: row.get(6)?,
                        manifest_sha256: row.get(7)?,
                        committed_bytes: row.get(8)?,
                        sha256: row.get(9)?,
                        destination_reserved: row.get(10)?,
                        reservation_token: row.get(11)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?
        };

        for record in records {
            self.reconcile_resumable_partial(&record)?;
        }
        Ok(())
    }

    fn reconcile_resumable_partial(&self, record: &PartialRecoveryRecord) -> Result<(), AppError> {
        if !self.try_claim_incoming_recovery(record)? {
            return Ok(());
        }
        if let Err(error) = self.reconcile_claimed_resumable_partial(record) {
            tracing::warn!(
                transfer_id = %record.transfer_id,
                %error,
                "failed to reconcile resumable incoming partial"
            );
            if matches!(error, AppError::IntegrityFailure) {
                self.fail_claimed_incoming_recovery(record, &error.to_string())?;
            } else {
                self.pause_claimed_incoming_recovery(record, &error.to_string())?;
            }
        }
        Ok(())
    }

    fn reconcile_claimed_resumable_partial(
        &self,
        record: &PartialRecoveryRecord,
    ) -> Result<(), AppError> {
        self.reconcile_claimed_resumable_partial_with_hook(record, || Ok(()))
    }

    fn reconcile_claimed_resumable_partial_with_hook<F>(
        &self,
        record: &PartialRecoveryRecord,
        after_owned_open: F,
    ) -> Result<(), AppError>
    where
        F: FnOnce() -> std::io::Result<()>,
    {
        let file_size = u64::try_from(record.file_size)
            .map_err(|_| AppError::Storage("可恢复文件大小无效".to_string()))?;
        let chunk_size = u32::try_from(record.chunk_size)
            .map_err(|_| AppError::Storage("可恢复文件分块大小无效".to_string()))?;
        let committed_bytes = u64::try_from(record.committed_bytes)
            .map_err(|_| AppError::Storage("可恢复文件已提交进度无效".to_string()))?;
        let chunk_count = u32::try_from(record.chunk_count)
            .map_err(|_| AppError::Storage("可恢复文件分块数量无效".to_string()))?;
        validate_transfer_metadata(
            2,
            file_size,
            chunk_size,
            chunk_count,
            record.manifest_sha256.as_deref(),
        )
        .map_err(storage_metadata_error)?;
        if committed_bytes > file_size
            || (committed_bytes != file_size && committed_bytes % u64::from(chunk_size) != 0)
        {
            return Err(AppError::Storage(
                "可恢复文件已提交进度不在分块边界".to_string(),
            ));
        }

        let Some(destination) = record.destination.as_deref() else {
            self.rollback_resumable_progress(record, 0, None)?;
            return Ok(());
        };
        let token = record
            .reservation_token
            .as_deref()
            .ok_or_else(|| AppError::Storage("可恢复接收缺少部分文件所有权凭据".to_string()))?;
        let persisted_partial = record
            .partial
            .clone()
            .unwrap_or(resumable_partial_path(destination, &record.transfer_id)?);
        let finalized_destination = if record.destination_reserved {
            owned_finalized_receive_path(destination, &record.transfer_id, token)?
        } else {
            None
        };
        if record.committed_bytes == record.file_size
            && finalized_destination.as_deref().is_some_and(Path::is_file)
        {
            self.complete_claimed_finalized_incoming(
                record,
                finalized_destination
                    .as_deref()
                    .expect("checked finalized destination"),
                &persisted_partial,
                token,
            )?;
            return Ok(());
        }

        let (active_partial, active_file) = match open_owned_resumable_partial_file(
            &persisted_partial,
            destination,
            &record.transfer_id,
            token,
        )? {
            Some(file) => (persisted_partial, file),
            None => {
                let partial = reserve_resumable_partial(destination, &record.transfer_id, token)?;
                let file = open_owned_resumable_partial_file(
                    &partial,
                    destination,
                    &record.transfer_id,
                    token,
                )?
                .ok_or_else(|| AppError::Storage("新建部分文件的所有权验证失败".to_string()))?;
                (partial, file)
            }
        };
        after_owned_open()?;
        let partial_length = active_file.metadata()?.len();
        let available_bytes = partial_length.min(committed_bytes);
        let trusted_bytes = self.largest_complete_received_boundary(
            record,
            file_size,
            chunk_size,
            available_bytes,
        )?;
        if trusted_bytes < committed_bytes {
            truncate_file_and_sync(&active_file, trusted_bytes)?;
            self.rollback_resumable_progress(record, trusted_bytes, Some(&active_partial))?;
        } else if partial_length > committed_bytes {
            truncate_file_and_sync(&active_file, committed_bytes)?;
            self.finish_claimed_incoming_recovery(record, &active_partial)?;
        } else {
            self.finish_claimed_incoming_recovery(record, &active_partial)?;
        }
        Ok(())
    }

    fn try_claim_incoming_recovery(
        &self,
        record: &PartialRecoveryRecord,
    ) -> Result<bool, AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET receive_claimed = 1
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND transfer_protocol = 2 AND status = 'paused' AND receive_claimed = 0
               AND transferred_bytes = ?3 AND local_path IS ?4
               AND destination_reserved = ?5 AND reservation_token IS ?6
               AND partial_path IS ?7",
            params![
                record.transfer_id,
                record.peer_id,
                record.committed_bytes,
                record
                    .destination
                    .as_deref()
                    .map(|path| path.to_string_lossy().into_owned()),
                record.destination_reserved,
                record.reservation_token,
                record
                    .partial
                    .as_deref()
                    .map(|path| path.to_string_lossy().into_owned()),
            ],
        )?;
        Ok(changed == 1)
    }

    fn largest_complete_received_boundary(
        &self,
        record: &PartialRecoveryRecord,
        file_size: u64,
        chunk_size: u32,
        available_bytes: u64,
    ) -> Result<u64, AppError> {
        let chunks = self.list_transfer_chunks(&record.transfer_id)?;
        let mut boundary = 0_u64;
        for (expected_index, chunk) in chunks.iter().enumerate() {
            let expected_index = u32::try_from(expected_index)
                .map_err(|_| AppError::Storage("可恢复文件分块索引溢出".to_string()))?;
            let expected_length = expected_chunk_length(file_size, chunk_size, expected_index)
                .map_err(storage_metadata_error)?;
            if chunk.index != expected_index || chunk.length != expected_length {
                break;
            }
            let next_boundary = boundary
                .checked_add(u64::from(chunk.length))
                .ok_or_else(|| AppError::Storage("可恢复文件分块边界溢出".to_string()))?;
            if next_boundary > available_bytes {
                break;
            }
            boundary = next_boundary;
        }
        Ok(boundary)
    }

    fn rollback_resumable_progress(
        &self,
        record: &PartialRecoveryRecord,
        rollback_bytes: u64,
        partial_path: Option<&Path>,
    ) -> Result<(), AppError> {
        let rollback_bytes = i64::try_from(rollback_bytes)
            .map_err(|_| AppError::Storage("可恢复文件回退进度超出存储范围".to_string()))?;
        let retained_chunks = rollback_bytes
            .checked_div(record.chunk_size)
            .ok_or_else(|| AppError::Storage("可恢复文件分块大小无效".to_string()))?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let changed = transaction.execute(
            "UPDATE transfers
             SET transferred_bytes = ?2, partial_path = COALESCE(?3, partial_path),
                 receive_claimed = 0,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?5 AND direction = 'incoming'
               AND transfer_protocol = 2 AND status = 'paused' AND receive_claimed = 1
               AND transferred_bytes = ?4",
            params![
                record.transfer_id,
                rollback_bytes,
                partial_path.map(|path| path.to_string_lossy().into_owned()),
                record.committed_bytes,
                record.peer_id,
            ],
        )?;
        if changed != 1 {
            return Err(AppError::Storage(
                "可恢复接收回退状态已变化，未修改分块记录".to_string(),
            ));
        }
        transaction.execute(
            "DELETE FROM transfer_chunks WHERE transfer_id = ?1 AND chunk_index >= ?2",
            params![record.transfer_id, retained_chunks],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn finish_claimed_incoming_recovery(
        &self,
        record: &PartialRecoveryRecord,
        partial_path: &Path,
    ) -> Result<(), AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET partial_path = ?3, receive_claimed = 0,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND transfer_protocol = 2 AND status = 'paused' AND receive_claimed = 1
               AND transferred_bytes = ?4",
            params![
                record.transfer_id,
                record.peer_id,
                partial_path.to_string_lossy(),
                record.committed_bytes,
            ],
        )?;
        if changed != 1 {
            return Err(AppError::Storage("可恢复接收恢复状态已变化".to_string()));
        }
        Ok(())
    }

    fn pause_claimed_incoming_recovery(
        &self,
        record: &PartialRecoveryRecord,
        error: &str,
    ) -> Result<(), AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET error = ?3, receive_claimed = 0,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND transfer_protocol = 2 AND status = 'paused' AND receive_claimed = 1",
            params![record.transfer_id, record.peer_id, error],
        )?;
        if changed != 1 {
            return Err(AppError::Storage("可恢复接收错误状态已变化".to_string()));
        }
        Ok(())
    }

    fn fail_claimed_incoming_recovery(
        &self,
        record: &PartialRecoveryRecord,
        error: &str,
    ) -> Result<(), AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET status = 'failed', error = ?3, receive_claimed = 0,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND transfer_protocol = 2 AND status = 'paused' AND receive_claimed = 1",
            params![record.transfer_id, record.peer_id, error],
        )?;
        drop(connection);
        if changed == 1 {
            self.cleanup_owned_transfer_artifacts(&record.transfer_id)?;
        }
        Ok(())
    }

    fn complete_claimed_finalized_incoming(
        &self,
        record: &PartialRecoveryRecord,
        destination: &Path,
        expected_partial: &Path,
        token: &str,
    ) -> Result<(), AppError> {
        let metadata = fs::metadata(destination)?;
        if !metadata.is_file() || i64::try_from(metadata.len()).ok() != Some(record.file_size) {
            return Err(AppError::IntegrityFailure);
        }
        let mut file = fs::File::open(destination)?;
        let mut hasher = sha2::Sha256::new();
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        let actual_sha256 = hex::encode(hasher.finalize());
        if actual_sha256 != record.sha256 {
            return Err(AppError::IntegrityFailure);
        }

        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET status = 'completed', local_path = ?3, receive_claimed = 0,
                 transferred_bytes = file_size, error = NULL,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND transfer_protocol = 2 AND status = 'paused' AND receive_claimed = 1
               AND transferred_bytes = file_size AND local_path = ?5
               AND destination_reserved = 1 AND reservation_token = ?4
               AND partial_path IS ?6",
            params![
                record.transfer_id,
                record.peer_id,
                destination.to_string_lossy(),
                token,
                record
                    .destination
                    .as_deref()
                    .map(|path| path.to_string_lossy().into_owned()),
                expected_partial.to_string_lossy(),
            ],
        )?;
        drop(connection);
        if changed != 1 {
            return Err(AppError::Storage("已完成接收的恢复状态已变化".to_string()));
        }
        self.cleanup_owned_transfer_artifacts(&record.transfer_id)?;
        Ok(())
    }

    fn cleanup_stale_owned_artifacts(&self) -> Result<(), AppError> {
        self.cleanup_pending_terminal_artifacts()?;
        let records = {
            let connection = self.connection()?;
            let mut statement = connection.prepare(
                "SELECT transfer_id, local_path, partial_path, reservation_token,
                        destination_reserved, transfer_protocol, status
                 FROM transfers
                 WHERE destination_reserved = 1
                    OR (partial_path IS NOT NULL AND status IN ('cancelled', 'failed'))",
            )?;
            statement
                .query_map([], |row| {
                    Ok(OwnedArtifactRecord {
                        transfer_id: row.get(0)?,
                        destination: row.get::<_, Option<String>>(1)?.map(PathBuf::from),
                        partial: row.get::<_, Option<String>>(2)?.map(PathBuf::from),
                        reservation_token: row.get(3)?,
                        destination_reserved: row.get(4)?,
                        transfer_protocol: row.get(5)?,
                        status: row.get(6)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        for record in records {
            if record.transfer_protocol == 2 && record.status == "paused" {
                continue;
            }
            self.cleanup_owned_artifact_record(&record)?;
        }
        Ok(())
    }

    fn cleanup_owned_transfer_artifacts(&self, transfer_id: &str) -> Result<(), AppError> {
        if let Some(record) = self.pending_terminal_cleanup(transfer_id)? {
            self.cleanup_pending_terminal_artifact(&record)?;
            return Ok(());
        }
        let record = {
            let connection = self.connection()?;
            connection
                .query_row(
                    "SELECT transfer_id, local_path, partial_path, reservation_token,
                            destination_reserved, transfer_protocol, status
                     FROM transfers WHERE transfer_id = ?1",
                    [transfer_id],
                    |row| {
                        Ok(OwnedArtifactRecord {
                            transfer_id: row.get(0)?,
                            destination: row.get::<_, Option<String>>(1)?.map(PathBuf::from),
                            partial: row.get::<_, Option<String>>(2)?.map(PathBuf::from),
                            reservation_token: row.get(3)?,
                            destination_reserved: row.get(4)?,
                            transfer_protocol: row.get(5)?,
                            status: row.get(6)?,
                        })
                    },
                )
                .optional()?
        };
        if let Some(record) = record {
            self.cleanup_owned_artifact_record(&record)?;
        }
        Ok(())
    }

    pub(crate) fn cleanup_completed_incoming_artifacts(
        &self,
        transfer_id: &str,
    ) -> Result<(), AppError> {
        let completed: bool = self.connection()?.query_row(
            "SELECT EXISTS(
               SELECT 1 FROM transfers
               WHERE transfer_id = ?1 AND direction = 'incoming'
                 AND transfer_protocol = 2 AND status = 'completed'
             )",
            [transfer_id],
            |row| row.get(0),
        )?;
        if completed {
            self.cleanup_owned_transfer_artifacts(transfer_id)?;
        }
        Ok(())
    }

    fn cleanup_owned_artifact_record(&self, record: &OwnedArtifactRecord) -> Result<(), AppError> {
        let completed = record.status == "completed";
        let terminal = matches!(record.status.as_str(), "cancelled" | "failed");
        if terminal {
            self.stage_terminal_cleanup(record)?;
            self.cleanup_pending_terminal_artifact(record)?;
            return Ok(());
        }
        let mut finalized_destination = None;
        let mut journal_reservations = Vec::new();
        let mut finalization_marker_cleaned = true;
        if let (Some(destination), Some(token)) = (
            record.destination.as_deref(),
            record.reservation_token.as_deref(),
        ) {
            match owned_finalized_receive_path(destination, &record.transfer_id, token) {
                Ok(candidate) => {
                    finalized_destination = candidate;
                    journal_reservations =
                        owned_finalization_reservations(destination, &record.transfer_id, token)?;
                }
                Err(error) => {
                    finalization_marker_cleaned = false;
                    tracing::warn!(
                        transfer_id = %record.transfer_id,
                        %error,
                        "failed to inspect receive finalization marker"
                    );
                }
            }
        }
        let artifact_destination = finalized_destination
            .as_deref()
            .or(record.destination.as_deref());
        let mut partial_cleaned = record.partial.is_none();

        if completed {
            if let (Some(destination), Some(token)) =
                (artifact_destination, record.reservation_token.as_deref())
            {
                let expected_partial = record
                    .partial
                    .clone()
                    .unwrap_or(resumable_partial_path(destination, &record.transfer_id)?);
                match remove_owned_partial(
                    &expected_partial,
                    destination,
                    &record.transfer_id,
                    token,
                ) {
                    Ok(true) => partial_cleaned = true,
                    Ok(false) => {
                        partial_cleaned = remove_owned_partial_marker_after_file_cleanup(
                            &expected_partial,
                            destination,
                            &record.transfer_id,
                            token,
                        )?;
                    }
                    Err(error) => {
                        finalization_marker_cleaned = false;
                        tracing::warn!(
                            transfer_id = %record.transfer_id,
                            %error,
                            "failed to clean completed receive partial marker"
                        );
                    }
                }
            }
        }

        let mut reservation_cleaned = !record.destination_reserved;
        if record.destination_reserved {
            if let Some(token) = record.reservation_token.as_deref() {
                let destinations = if journal_reservations.is_empty() {
                    artifact_destination
                        .into_iter()
                        .map(Path::to_path_buf)
                        .collect::<Vec<_>>()
                } else {
                    journal_reservations.clone()
                };
                reservation_cleaned = true;
                for destination in destinations {
                    if let Err(error) =
                        remove_owned_reservation(&destination, &record.transfer_id, token)
                    {
                        reservation_cleaned = false;
                        tracing::warn!(
                            transfer_id = %record.transfer_id,
                            %error,
                            "failed to clean stale receive reservation"
                        );
                    }
                }
                if let Some(destination) = artifact_destination {
                    if let Err(error) = remove_owned_reservations_in_directory(
                        destination,
                        &record.transfer_id,
                        token,
                    ) {
                        reservation_cleaned = false;
                        tracing::warn!(
                            transfer_id = %record.transfer_id,
                            %error,
                            "failed to clean receive reservation set"
                        );
                    }
                }
            }
        }

        if completed && !journal_reservations.is_empty() {
            if let (Some(destination), Some(token)) = (
                record.destination.as_deref(),
                record.reservation_token.as_deref(),
            ) {
                match remove_owned_finalization_stage(destination, &record.transfer_id, token) {
                    Ok(true) => {
                        if let Err(error) = remove_owned_finalization_marker(
                            destination,
                            &record.transfer_id,
                            token,
                        ) {
                            finalization_marker_cleaned = false;
                            tracing::warn!(
                                transfer_id = %record.transfer_id,
                                %error,
                                "failed to clean receive finalization marker"
                            );
                        }
                    }
                    Ok(false) => finalization_marker_cleaned = false,
                    Err(error) => {
                        finalization_marker_cleaned = false;
                        tracing::warn!(
                            transfer_id = %record.transfer_id,
                            %error,
                            "failed to clean receive finalization stage"
                        );
                    }
                }
            }
        }

        if terminal {
            if let (Some(partial), Some(destination), Some(token)) = (
                record.partial.as_deref(),
                artifact_destination,
                record.reservation_token.as_deref(),
            ) {
                match remove_owned_partial(partial, destination, &record.transfer_id, token) {
                    Ok(_) => partial_cleaned = true,
                    Err(error) => tracing::warn!(
                        transfer_id = %record.transfer_id,
                        %error,
                        "failed to clean stale resumable partial"
                    ),
                }
            }
        }

        let connection = self.connection()?;
        if reservation_cleaned && finalization_marker_cleaned {
            connection.execute(
                "UPDATE transfers
                 SET destination_reserved = 0, reservation_token = NULL
                 WHERE transfer_id = ?1 AND destination_reserved = 1",
                [&record.transfer_id],
            )?;
        }
        if terminal && partial_cleaned {
            connection.execute(
                "UPDATE transfers SET partial_path = NULL WHERE transfer_id = ?1
                 AND status IN ('cancelled', 'failed')",
                [&record.transfer_id],
            )?;
            connection.execute(
                "DELETE FROM transfer_chunks WHERE transfer_id = ?1",
                [&record.transfer_id],
            )?;
        }
        if completed && partial_cleaned {
            connection.execute(
                "UPDATE transfers SET partial_path = NULL WHERE transfer_id = ?1
                 AND status = 'completed'",
                [&record.transfer_id],
            )?;
        }
        Ok(())
    }

    fn stage_terminal_cleanup(&self, record: &OwnedArtifactRecord) -> Result<(), AppError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO transfer_cleanup
               (transfer_id, destination, partial_path, reservation_token,
                destination_reserved, transfer_protocol, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(transfer_id) DO NOTHING",
            params![
                record.transfer_id,
                record
                    .destination
                    .as_deref()
                    .map(|path| path.to_string_lossy().into_owned()),
                record
                    .partial
                    .as_deref()
                    .map(|path| path.to_string_lossy().into_owned()),
                record.reservation_token,
                record.destination_reserved,
                record.transfer_protocol,
                record.status,
            ],
        )?;
        let changed = transaction.execute(
            "UPDATE transfers
             SET destination_reserved = 0, reservation_token = NULL, partial_path = NULL
             WHERE transfer_id = ?1 AND status IN ('cancelled', 'failed')
               AND local_path IS ?2 AND partial_path IS ?3 AND reservation_token IS ?4
               AND destination_reserved = ?5",
            params![
                record.transfer_id,
                record
                    .destination
                    .as_deref()
                    .map(|path| path.to_string_lossy().into_owned()),
                record
                    .partial
                    .as_deref()
                    .map(|path| path.to_string_lossy().into_owned()),
                record.reservation_token,
                record.destination_reserved,
            ],
        )?;
        if changed != 1 {
            return Err(AppError::Storage(
                "终态传输所有权清理状态已变化".to_string(),
            ));
        }
        transaction.execute(
            "DELETE FROM transfer_chunks WHERE transfer_id = ?1",
            [&record.transfer_id],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn cleanup_pending_terminal_artifacts(&self) -> Result<(), AppError> {
        let records = {
            let connection = self.connection()?;
            let mut statement = connection.prepare(
                "SELECT transfer_id, destination, partial_path, reservation_token,
                        destination_reserved, transfer_protocol, status
                 FROM transfer_cleanup",
            )?;
            statement
                .query_map([], |row| {
                    Ok(OwnedArtifactRecord {
                        transfer_id: row.get(0)?,
                        destination: row.get::<_, Option<String>>(1)?.map(PathBuf::from),
                        partial: row.get::<_, Option<String>>(2)?.map(PathBuf::from),
                        reservation_token: row.get(3)?,
                        destination_reserved: row.get(4)?,
                        transfer_protocol: row.get(5)?,
                        status: row.get(6)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        for record in records {
            if let Err(error) = self.cleanup_pending_terminal_artifact(&record) {
                if terminal_cleanup_media_is_unavailable(&error) {
                    tracing::warn!(
                        transfer_id = %record.transfer_id,
                        %error,
                        "deferring terminal artifact cleanup until destination media returns"
                    );
                    continue;
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn pending_terminal_cleanup(
        &self,
        transfer_id: &str,
    ) -> Result<Option<OwnedArtifactRecord>, AppError> {
        self.connection()?
            .query_row(
                "SELECT transfer_id, destination, partial_path, reservation_token,
                        destination_reserved, transfer_protocol, status
                 FROM transfer_cleanup WHERE transfer_id = ?1",
                [transfer_id],
                |row| {
                    Ok(OwnedArtifactRecord {
                        transfer_id: row.get(0)?,
                        destination: row.get::<_, Option<String>>(1)?.map(PathBuf::from),
                        partial: row.get::<_, Option<String>>(2)?.map(PathBuf::from),
                        reservation_token: row.get(3)?,
                        destination_reserved: row.get(4)?,
                        transfer_protocol: row.get(5)?,
                        status: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(AppError::from)
    }

    fn cleanup_pending_terminal_artifact(
        &self,
        record: &OwnedArtifactRecord,
    ) -> Result<(), AppError> {
        let mut finalized_destination = None;
        let mut journal_reservations = Vec::new();
        if let (Some(destination), Some(token)) = (
            record.destination.as_deref(),
            record.reservation_token.as_deref(),
        ) {
            finalized_destination =
                owned_finalized_receive_path(destination, &record.transfer_id, token)?;
            journal_reservations =
                owned_finalization_reservations(destination, &record.transfer_id, token)?;
        }
        let artifact_destination = finalized_destination
            .as_deref()
            .or(record.destination.as_deref());
        if record.destination_reserved {
            if let (Some(destination), Some(token)) =
                (artifact_destination, record.reservation_token.as_deref())
            {
                if journal_reservations.is_empty() {
                    remove_owned_reservation(destination, &record.transfer_id, token)?;
                } else {
                    for destination in &journal_reservations {
                        remove_owned_reservation(destination, &record.transfer_id, token)?;
                    }
                }
                remove_owned_reservations_in_directory(destination, &record.transfer_id, token)?;
            }
        }
        let mut partial_cleaned = record.partial.is_none();
        if let (Some(partial), Some(destination), Some(token)) = (
            record.partial.as_deref(),
            artifact_destination,
            record.reservation_token.as_deref(),
        ) {
            partial_cleaned =
                remove_owned_partial(partial, destination, &record.transfer_id, token)?
                    || remove_owned_partial_marker_after_file_cleanup(
                        partial,
                        destination,
                        &record.transfer_id,
                        token,
                    )?;
        }
        if !partial_cleaned {
            return Ok(());
        }
        if let (Some(destination), Some(token)) = (
            record.destination.as_deref(),
            record.reservation_token.as_deref(),
        ) {
            if !remove_owned_finalization_stage(destination, &record.transfer_id, token)? {
                return Ok(());
            }
            remove_owned_finalization_marker(destination, &record.transfer_id, token)?;
        }
        self.connection()?.execute(
            "DELETE FROM transfer_cleanup WHERE transfer_id = ?1",
            [&record.transfer_id],
        )?;
        Ok(())
    }

    fn delete_terminal_transfer_chunks(&self, transfer_id: &str) -> Result<(), AppError> {
        let connection = self.connection()?;
        connection.execute(
            "DELETE FROM transfer_chunks
             WHERE transfer_id = ?1
               AND EXISTS (
                   SELECT 1 FROM transfers
                   WHERE transfer_id = ?1 AND status IN ('cancelled', 'failed')
               )",
            [transfer_id],
        )?;
        Ok(())
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, AppError> {
        self.connection.lock().map_err(|_| {
            AppError::Storage("本地数据锁异常，请重新启动 Weline Localnet".to_string())
        })
    }
}

#[derive(Debug)]
struct PartialRecoveryRecord {
    transfer_id: String,
    peer_id: String,
    destination: Option<PathBuf>,
    partial: Option<PathBuf>,
    file_size: i64,
    chunk_size: i64,
    chunk_count: i64,
    manifest_sha256: Option<String>,
    committed_bytes: i64,
    sha256: String,
    destination_reserved: bool,
    reservation_token: Option<String>,
}

#[derive(Debug)]
struct OwnedArtifactRecord {
    transfer_id: String,
    destination: Option<PathBuf>,
    partial: Option<PathBuf>,
    reservation_token: Option<String>,
    destination_reserved: bool,
    transfer_protocol: i64,
    status: String,
}

fn truncate_file_and_sync(file: &fs::File, length: u64) -> Result<(), AppError> {
    file.set_len(length)?;
    file.sync_all()?;
    Ok(())
}

fn terminal_cleanup_media_is_unavailable(error: &AppError) -> bool {
    let AppError::Io(error) = error else {
        return false;
    };
    if error.kind() == std::io::ErrorKind::NotFound {
        return true;
    }
    #[cfg(windows)]
    if matches!(error.raw_os_error(), Some(15 | 21 | 1167)) {
        return true;
    }
    #[cfg(unix)]
    if matches!(
        error.raw_os_error(),
        Some(libc::ENODEV | libc::ENXIO | libc::ESTALE)
    ) {
        return true;
    }
    false
}

#[derive(Debug)]
struct ManifestMetadata {
    direction: Direction,
    transfer_protocol: u8,
    file_size: i64,
    chunk_size: u32,
    chunk_count: u32,
    manifest_sha256: Option<String>,
    transferred_bytes: i64,
}

fn validate_transfer_for_storage(transfer: &TransferRecord) -> Result<Option<i64>, AppError> {
    validate_transfer_metadata(
        transfer.transfer_protocol,
        transfer.file_size,
        transfer.chunk_size,
        transfer.chunk_count,
        transfer.manifest_sha256.as_deref(),
    )
    .map_err(storage_metadata_error)?;
    transfer
        .source_modified_ns
        .map(|value| {
            i64::try_from(value)
                .map_err(|_| AppError::Storage("源文件修改时间超出存储范围".to_string()))
        })
        .transpose()
}

fn validate_outgoing_manifest(
    transfer: &TransferRecord,
    chunks: &[TransferChunk],
) -> Result<(), AppError> {
    if transfer.direction != Direction::Outgoing {
        return Err(AppError::Storage(
            "只有发送中的 v2 传输可以保存清单".to_string(),
        ));
    }
    if transfer.transfer_protocol != 2 {
        return Err(AppError::Storage("旧版传输不能保存分块清单".to_string()));
    }
    validate_chunk_geometry(
        i64::try_from(transfer.file_size)
            .map_err(|_| AppError::Storage("传输文件大小超出存储范围".to_string()))?,
        transfer.chunk_size,
        transfer.chunk_count,
        transfer.manifest_sha256.as_deref(),
        chunks,
    )
}

fn validate_stored_manifest_metadata(
    metadata: &ManifestMetadata,
    direction: Direction,
) -> Result<(), AppError> {
    if metadata.direction != direction {
        return Err(AppError::Storage("传输方向不允许此分块操作".to_string()));
    }
    if metadata.transfer_protocol != 2 {
        return Err(AppError::Storage("旧版传输不能执行分块操作".to_string()));
    }
    if metadata.file_size < 0 {
        return Err(AppError::Storage("传输文件大小无效".to_string()));
    }
    validate_transfer_metadata(
        metadata.transfer_protocol,
        u64::try_from(metadata.file_size)
            .map_err(|_| AppError::Storage("传输文件大小无效".to_string()))?,
        metadata.chunk_size,
        metadata.chunk_count,
        metadata.manifest_sha256.as_deref(),
    )
    .map_err(storage_metadata_error)
}

fn validate_chunk_geometry(
    file_size: i64,
    chunk_size: u32,
    chunk_count: u32,
    manifest_sha256: Option<&str>,
    chunks: &[TransferChunk],
) -> Result<(), AppError> {
    let file_size =
        u64::try_from(file_size).map_err(|_| AppError::Storage("传输文件大小无效".to_string()))?;
    let expected_count =
        expected_chunk_count(file_size, chunk_size).map_err(storage_metadata_error)?;
    if chunk_count != expected_count || usize::try_from(chunk_count).ok() != Some(chunks.len()) {
        return Err(AppError::Storage("分块数量与传输清单不匹配".to_string()));
    }
    let mut total = 0_u64;
    for (expected_index, chunk) in chunks.iter().enumerate() {
        let expected_index = u32::try_from(expected_index)
            .map_err(|_| AppError::Storage("分块数量超出协议限制".to_string()))?;
        let expected_length = expected_chunk_length(file_size, chunk_size, expected_index)
            .map_err(storage_metadata_error)?;
        if chunk.index != expected_index || chunk.length != expected_length {
            return Err(AppError::Storage("分块索引或长度与清单不匹配".to_string()));
        }
        total = total
            .checked_add(u64::from(chunk.length))
            .ok_or_else(|| AppError::Storage("分块长度总和溢出".to_string()))?;
    }
    if total != file_size {
        return Err(AppError::Storage(
            "分块长度总和与文件大小不匹配".to_string(),
        ));
    }
    let expected_root = decode_sha256(
        manifest_sha256.ok_or_else(|| AppError::Storage("缺少传输清单哈希".to_string()))?,
    )
    .map_err(storage_metadata_error)?;
    if manifest_root(chunks) != expected_root {
        return Err(AppError::Storage("传输清单根哈希不匹配".to_string()));
    }
    Ok(())
}

fn replace_chunks_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    transfer_id: &str,
    chunks: &[TransferChunk],
) -> Result<(), AppError> {
    transaction.execute(
        "DELETE FROM transfer_chunks WHERE transfer_id = ?1",
        [transfer_id],
    )?;
    for chunk in chunks {
        transaction.execute(
            "INSERT INTO transfer_chunks (transfer_id, chunk_index, chunk_length, sha256)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                transfer_id,
                i64::from(chunk.index),
                i64::from(chunk.length),
                chunk.sha256.as_slice(),
            ],
        )?;
    }
    Ok(())
}

fn load_manifest_metadata(
    transaction: &rusqlite::Transaction<'_>,
    transfer_id: &str,
) -> Result<Option<ManifestMetadata>, AppError> {
    transaction
        .query_row(
            "SELECT direction, transfer_protocol, file_size, chunk_size, chunk_count,
                    manifest_sha256, transferred_bytes
             FROM transfers WHERE transfer_id = ?1",
            [transfer_id],
            |row| {
                let direction: String = row.get(0)?;
                let transfer_protocol: i64 = row.get(1)?;
                let chunk_size: i64 = row.get(3)?;
                let chunk_count: i64 = row.get(4)?;
                Ok(ManifestMetadata {
                    direction: Direction::from_str(&direction).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                    transfer_protocol: u8::try_from(transfer_protocol).map_err(|_| {
                        rusqlite::Error::IntegralValueOutOfRange(1, transfer_protocol)
                    })?,
                    file_size: row.get(2)?,
                    chunk_size: u32::try_from(chunk_size)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, chunk_size))?,
                    chunk_count: u32::try_from(chunk_count)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, chunk_count))?,
                    manifest_sha256: row.get(5)?,
                    transferred_bytes: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn list_transfer_chunks_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    transfer_id: &str,
) -> Result<Vec<TransferChunk>, AppError> {
    let mut statement = transaction.prepare(
        "SELECT chunk_index, chunk_length, sha256 FROM transfer_chunks
         WHERE transfer_id = ?1 ORDER BY chunk_index ASC",
    )?;
    let rows = statement.query_map([transfer_id], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, Vec<u8>>(2)?,
        ))
    })?;
    rows.map(|row| {
        let (index, length, sha256) = row?;
        let sha256: [u8; 32] = sha256
            .try_into()
            .map_err(|_| AppError::Storage("传输分块哈希长度无效，必须为 32 字节".to_string()))?;
        Ok(TransferChunk {
            index: u32::try_from(index)
                .map_err(|_| AppError::Storage("传输分块索引无效".to_string()))?,
            length: u32::try_from(length)
                .map_err(|_| AppError::Storage("传输分块长度无效".to_string()))?,
            sha256,
        })
    })
    .collect()
}

fn storage_metadata_error(error: AppError) -> AppError {
    AppError::Storage(format!("传输元数据无效：{error}"))
}

fn validate_loaded_optional_sha256(value: Option<String>) -> Result<Option<String>, AppError> {
    if let Some(value) = value.as_deref() {
        decode_sha256(value).map_err(storage_metadata_error)?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::{
        fs, io,
        path::{Path, PathBuf},
        sync::{Arc, Barrier},
        thread,
    };

    use rusqlite::Connection;
    use sha2::{Digest, Sha256};

    use super::{OwnedArtifactRecord, PartialRecoveryRecord, Storage};
    use crate::{
        domain::{
            Direction, PeerSummary, Platform, TransferKind, TransferPreferences, TransferRecord,
            TransferStatus,
        },
        receive_paths::{
            FinalizationPhase, create_legacy_finalized_stage_fixture,
            finalize_reserved_receive_copy_fallback_with_hooks, finalize_reserved_receive_durable,
            finalize_reserved_receive_durable_with_hooks, reservation_is_owned,
            reserve_receive_path, reserve_resumable_partial, resumable_partial_path,
        },
        transfer_manifest::{TransferChunk, manifest_root},
        transfer_policy::{TRANSFER_CHUNK_BYTES, TransferProtocol},
    };

    fn initialize_database(database: &std::path::Path) {
        drop(Storage::open(database).expect("initialize current storage schema"));
    }

    fn seed_resumable_incoming(
        database: &std::path::Path,
        destination: &std::path::Path,
        transfer_id: &str,
        committed_bytes: u64,
        partial_length: Option<u64>,
    ) -> std::path::PathBuf {
        let token = format!("token-{transfer_id}");
        reserve_receive_path(destination, transfer_id, &token)
            .expect("reserve incoming destination");
        let partial = reserve_resumable_partial(destination, transfer_id, &token)
            .expect("reserve deterministic partial path");
        if let Some(length) = partial_length {
            let file = fs::OpenOptions::new()
                .write(true)
                .open(&partial)
                .expect("open partial fixture");
            file.set_len(length).expect("size partial fixture");
            file.sync_all().expect("sync partial fixture");
        } else {
            fs::remove_file(&partial).expect("simulate missing owned partial");
        }

        let connection = Connection::open(database).expect("open startup fixture database");
        connection
            .execute(
                "INSERT INTO transfers
                   (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                    sha256, local_path, destination_reserved, reservation_token,
                    receive_claimed, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                    partial_path, transferred_bytes, status, created_at, updated_at)
                 VALUES (?1, 'peer-one', 'incoming', 'file', 'report.bin', ?2,
                         'application/octet-stream', ?3, ?4, 1, ?5, 1, 2, ?6, 3, ?3, ?7, ?8,
                         'transferring', ?9, ?9)",
                rusqlite::params![
                    transfer_id,
                    u64::from(TRANSFER_CHUNK_BYTES) * 2 + 2,
                    "0".repeat(64),
                    destination.to_string_lossy(),
                    token,
                    TRANSFER_CHUNK_BYTES,
                    partial.to_string_lossy(),
                    committed_bytes,
                    "2026-08-24T00:00:00.000Z",
                ],
            )
            .expect("insert resumable startup fixture");
        let committed_chunks = committed_bytes / u64::from(TRANSFER_CHUNK_BYTES);
        for index in 0..committed_chunks {
            connection
                .execute(
                    "INSERT INTO transfer_chunks
                       (transfer_id, chunk_index, chunk_length, sha256)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        transfer_id,
                        index,
                        TRANSFER_CHUNK_BYTES,
                        [u8::try_from(index + 1).expect("small chunk index"); 32].as_slice(),
                    ],
                )
                .expect("insert committed chunk fixture");
        }
        partial
    }

    fn receive_claimed(storage: &Storage, transfer_id: &str) -> bool {
        storage
            .connection()
            .expect("inspect receive claim")
            .query_row(
                "SELECT receive_claimed FROM transfers WHERE transfer_id = ?1",
                [transfer_id],
                |row| row.get(0),
            )
            .expect("read receive claim")
    }

    #[test]
    fn transfer_preferences_default_to_manual_and_persist_selected_directory() {
        let fixture =
            std::env::temp_dir().join(format!("weline-localnet-storage-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let default_directory = fixture.join("Downloads").join("Weline Localnet");
        let selected_directory = fixture.join("Shared Files");

        {
            let storage = Storage::open(&database).expect("open storage");
            let defaults = storage
                .load_transfer_preferences(&default_directory)
                .expect("load transfer preference defaults");
            assert!(!defaults.auto_receive_files);
            assert_eq!(
                defaults.receive_directory,
                default_directory.to_string_lossy()
            );

            storage
                .save_transfer_preferences(&TransferPreferences {
                    auto_receive_files: true,
                    receive_directory: selected_directory.to_string_lossy().into_owned(),
                })
                .expect("save transfer preferences");
        }

        {
            let storage = Storage::open(&database).expect("reopen storage");
            let persisted = storage
                .load_transfer_preferences(&default_directory)
                .expect("reload transfer preferences");
            assert!(persisted.auto_receive_files);
            assert_eq!(
                persisted.receive_directory,
                selected_directory.to_string_lossy()
            );
        }

        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn existing_transfer_table_is_migrated_for_destination_reservations() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-storage-migration-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        {
            let connection = Connection::open(&database).expect("open legacy database");
            connection
                .execute_batch(
                    "CREATE TABLE transfers (
                       transfer_id TEXT PRIMARY KEY NOT NULL,
                       peer_id TEXT NOT NULL,
                       direction TEXT NOT NULL,
                       kind TEXT NOT NULL,
                       file_name TEXT NOT NULL,
                       file_size INTEGER NOT NULL,
                       mime_type TEXT NOT NULL,
                       sha256 TEXT NOT NULL,
                       local_path TEXT,
                       transferred_bytes INTEGER NOT NULL DEFAULT 0,
                       status TEXT NOT NULL,
                       error TEXT,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL
                     );",
                )
                .expect("create legacy transfers table");
        }

        let storage = Storage::open(&database).expect("migrate legacy database");
        let connection = storage.connection().expect("inspect migrated database");
        let columns: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('transfers')
                 WHERE name IN ('destination_reserved', 'reservation_token', 'receive_claimed')",
                [],
                |row| row.get(0),
            )
            .expect("query migrated column");
        assert_eq!(columns, 3);
        drop(connection);
        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn v2_transfer_migration_creates_manifest_metadata_and_chunk_table() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-transfer-manifest-migration-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        {
            let connection = Connection::open(&database).expect("open legacy database");
            connection
                .execute_batch(
                    "CREATE TABLE transfers (
                       transfer_id TEXT PRIMARY KEY NOT NULL,
                       peer_id TEXT NOT NULL,
                       direction TEXT NOT NULL,
                       kind TEXT NOT NULL,
                       file_name TEXT NOT NULL,
                       file_size INTEGER NOT NULL,
                       mime_type TEXT NOT NULL,
                       sha256 TEXT NOT NULL,
                       local_path TEXT,
                       transferred_bytes INTEGER NOT NULL DEFAULT 0,
                       status TEXT NOT NULL,
                       error TEXT,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL
                     );",
                )
                .expect("create legacy transfers table");
        }

        let storage = Storage::open(&database).expect("open storage");
        let connection = storage.connection().expect("inspect migrated database");
        let columns: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('transfers')
                 WHERE name IN ('transfer_protocol', 'manifest_sha256', 'chunk_size',
                                'chunk_count', 'partial_path', 'source_modified_ns', 'send_claimed')",
                [],
                |row| row.get(0),
            )
            .expect("query v2 transfer columns");
        assert_eq!(columns, 7);
        let chunk_table: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'transfer_chunks'",
                [],
                |row| row.get(0),
            )
            .expect("query chunk table");
        assert_eq!(chunk_table, 1);
        connection
            .execute(
                "INSERT INTO transfers
                   (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                    sha256, transferred_bytes, status, created_at, updated_at)
                 VALUES ('valid-transfer', 'peer-one', 'incoming', 'file', 'report.txt', 1,
                         'text/plain', ?1, 0, 'transferring', ?2, ?2)",
                rusqlite::params!["0".repeat(64), "2026-08-24T00:00:00.000Z"],
            )
            .expect("insert valid transfer");
        assert!(
            connection
                .execute(
                    "INSERT INTO transfer_chunks (transfer_id, chunk_index, chunk_length, sha256)
                     VALUES ('valid-transfer', 0, 1, ?1)",
                    [[7_u8; 31].as_slice()],
                )
                .is_err(),
            "chunk hashes must be exactly 32-byte blobs"
        );
        assert!(
            connection
                .execute(
                    "INSERT INTO transfer_chunks (transfer_id, chunk_index, chunk_length, sha256)
                     VALUES ('valid-transfer', 1, 1, ?1)",
                    ["x".repeat(32)],
                )
                .is_err(),
            "chunk hashes must be BLOB values, not 32-character TEXT"
        );
        connection
            .execute_batch("PRAGMA ignore_check_constraints = ON")
            .expect("allow corrupted legacy chunk fixture");
        connection
            .execute(
                "INSERT INTO transfer_chunks (transfer_id, chunk_index, chunk_length, sha256)
                 VALUES ('valid-transfer', 0, 1, ?1)",
                [[7_u8; 31].as_slice()],
            )
            .expect("insert corrupted legacy chunk fixture");
        connection
            .execute_batch("PRAGMA ignore_check_constraints = OFF")
            .expect("restore chunk constraints");

        drop(connection);
        assert!(
            storage.list_transfer_chunks("valid-transfer").is_err(),
            "reading corrupted chunk hashes must fail"
        );
        drop(storage);
        Storage::open(&database).expect("repeat migration is idempotent");
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn migration_rebuilds_old_chunk_table_with_blob_type_enforcement() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-chunk-table-rebuild-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        {
            let connection = Connection::open(&database).expect("open legacy database");
            connection
                .execute_batch(
                    "CREATE TABLE transfers (
                       transfer_id TEXT PRIMARY KEY NOT NULL,
                       peer_id TEXT NOT NULL,
                       direction TEXT NOT NULL,
                       kind TEXT NOT NULL,
                       file_name TEXT NOT NULL,
                       file_size INTEGER NOT NULL,
                       mime_type TEXT NOT NULL,
                       sha256 TEXT NOT NULL,
                       local_path TEXT,
                       transferred_bytes INTEGER NOT NULL DEFAULT 0,
                       status TEXT NOT NULL,
                       error TEXT,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL
                     );
                     CREATE TABLE transfer_chunks (
                       transfer_id TEXT NOT NULL,
                       chunk_index INTEGER NOT NULL,
                       chunk_length INTEGER NOT NULL,
                       sha256 BLOB NOT NULL CHECK(length(sha256) = 32),
                       PRIMARY KEY (transfer_id, chunk_index)
                     );
                     INSERT INTO transfers
                       VALUES ('legacy-transfer', 'peer-one', 'incoming', 'file', 'report.txt', 1,
                               'text/plain', '0000000000000000000000000000000000000000000000000000000000000000',
                               NULL, 0, 'transferring', NULL,
                               '2026-08-24T00:00:00.000Z', '2026-08-24T00:00:00.000Z');",
                )
                .expect("create old chunk table");
            connection
                .execute(
                    "INSERT INTO transfer_chunks VALUES ('legacy-transfer', 0, 1, ?1)",
                    [[8_u8; 32].as_slice()],
                )
                .expect("insert valid legacy chunk");
        }

        let storage = Storage::open(&database).expect("rebuild old chunk table");
        assert_eq!(
            storage
                .list_transfer_chunks("legacy-transfer")
                .expect("preserve valid legacy chunk")
                .len(),
            1
        );
        let connection = storage.connection().expect("inspect rebuilt table");
        let schema: String = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'transfer_chunks'",
                [],
                |row| row.get(0),
            )
            .expect("read rebuilt schema");
        assert!(schema.contains("typeof(sha256)"));
        assert!(
            connection
                .execute(
                    "INSERT INTO transfer_chunks VALUES ('legacy-transfer', 1, 1, ?1)",
                    ["x".repeat(32)],
                )
                .is_err(),
            "rebuilt table must reject 32-character text"
        );

        drop(connection);
        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn migration_fails_closed_and_preserves_invalid_legacy_chunk_state() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-chunk-table-fail-closed-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        Storage::open(&database).expect("create current schema");
        {
            let connection = Connection::open(&database).expect("open weak-schema database");
            connection
                .execute_batch(
                    "DROP TABLE transfer_chunks;
                     CREATE TABLE transfer_chunks (
                       transfer_id TEXT NOT NULL,
                       chunk_index INTEGER NOT NULL,
                       chunk_length INTEGER NOT NULL,
                       sha256 BLOB NOT NULL CHECK(length(sha256) = 32),
                       PRIMARY KEY (transfer_id, chunk_index)
                     );
                     INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES
                       ('invalid-legacy-transfer', 'peer-one', 'incoming', 'file', 'report.txt',
                        4194304, 'text/plain',
                        '0000000000000000000000000000000000000000000000000000000000000000',
                        2, 4194304, 1,
                        '0000000000000000000000000000000000000000000000000000000000000000',
                        1048576, 'transferring',
                        '2026-08-24T00:00:00.000Z', '2026-08-24T00:00:00.000Z');
                     INSERT INTO transfer_chunks
                       VALUES ('invalid-legacy-transfer', 0, 4194304,
                               'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');",
                )
                .expect("seed weak table with invalid text hash");
        }

        let error = match Storage::open(&database) {
            Ok(_) => panic!("invalid old chunks must block migration"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("分块哈希"));

        let connection = Connection::open(&database).expect("read failed migration database");
        let schema: String = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'transfer_chunks'",
                [],
                |row| row.get(0),
            )
            .expect("read unchanged old schema");
        assert!(schema.contains("CHECK(length(sha256) = 32)"));
        let state: (String, i64, i64, i64) = connection
            .query_row(
                "SELECT typeof(chunk.sha256), transfer.chunk_count, transfer.transferred_bytes,
                        COUNT(*)
                 FROM transfer_chunks chunk
                 JOIN transfers transfer ON transfer.transfer_id = chunk.transfer_id
                 WHERE chunk.transfer_id = 'invalid-legacy-transfer'
                 GROUP BY chunk.transfer_id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("read unchanged invalid row and transfer progress");
        assert_eq!(state, ("text".to_string(), 1, 1_048_576, 1));

        drop(connection);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn committed_chunk_advances_progress_only_after_matching_chunk_is_persisted() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-transfer-chunk-commit-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let chunks = vec![
            TransferChunk {
                index: 0,
                length: TRANSFER_CHUNK_BYTES,
                sha256: [1; 32],
            },
            TransferChunk {
                index: 1,
                length: 2,
                sha256: [2; 32],
            },
        ];
        let manifest_sha256 = hex::encode(manifest_root(&chunks));
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transferred_bytes, status, created_at, updated_at)
                     VALUES ('transfer-one', 'peer-one', 'incoming', 'file', 'report.txt', ?1,
                             'text/plain', ?2, 0, 'transferring', ?3, ?3)",
                    rusqlite::params![
                        i64::from(TRANSFER_CHUNK_BYTES) + 2,
                        "0".repeat(64),
                        "2026-08-24T00:00:00.000Z"
                    ],
                )
                .expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transferred_bytes, status, created_at, updated_at)
                     VALUES ('outgoing-one', 'peer-one', 'outgoing', 'file', 'report.txt', ?1,
                             'text/plain', ?2, 0, 'awaitingAcceptance', ?3, ?3)",
                    rusqlite::params![
                        i64::from(TRANSFER_CHUNK_BYTES) + 2,
                        "0".repeat(64),
                        "2026-08-24T00:00:00.000Z"
                    ],
                )
                .expect("insert outgoing transfer fixture");
            connection
                .execute(
                    "UPDATE transfers
                     SET transfer_protocol = 2, chunk_size = ?1, chunk_count = 2,
                         manifest_sha256 = ?2,
                         receive_claimed = CASE WHEN transfer_id = 'transfer-one' THEN 1
                                                ELSE receive_claimed END
                     WHERE transfer_id IN ('transfer-one', 'outgoing-one')",
                    rusqlite::params![TRANSFER_CHUNK_BYTES, manifest_sha256],
                )
                .expect("configure v2 transfer fixtures");
        }
        storage
            .replace_outgoing_chunks("outgoing-one", &chunks)
            .expect("persist chunks");
        assert_eq!(
            storage
                .list_transfer_chunks("outgoing-one")
                .expect("list persisted chunks"),
            chunks
        );

        assert!(
            !storage
                .commit_received_chunk(
                    "transfer-one",
                    "peer-one",
                    &TransferChunk {
                        index: 1,
                        length: 2,
                        sha256: [2; 32],
                    },
                    u64::from(TRANSFER_CHUNK_BYTES) + 2,
                )
                .expect("reject out-of-order chunk")
        );
        assert!(
            storage
                .commit_received_chunk(
                    "transfer-one",
                    "peer-one",
                    &chunks[0],
                    u64::from(TRANSFER_CHUNK_BYTES),
                )
                .expect("commit first chunk")
        );
        assert!(
            !storage
                .commit_received_chunk(
                    "transfer-one",
                    "peer-one",
                    &TransferChunk {
                        index: 1,
                        length: 2,
                        sha256: [9; 32],
                    },
                    u64::from(TRANSFER_CHUNK_BYTES) + 1,
                )
                .expect("reject mismatched committed offset")
        );

        let connection = storage.connection().expect("inspect committed progress");
        let progress: (i64, i64) = connection
            .query_row(
                "SELECT transferred_bytes,
                        (SELECT COUNT(*) FROM transfer_chunks WHERE transfer_id = 'transfer-one')
                 FROM transfers WHERE transfer_id = 'transfer-one'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("read committed progress");
        assert_eq!(progress, (i64::from(TRANSFER_CHUNK_BYTES), 1));

        drop(connection);
        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn v2_chunk_storage_rejects_legacy_direction_and_geometry_mismatches() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-v2-chunk-boundaries-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        let connection = storage.connection().expect("insert transfer fixtures");
        connection
            .execute_batch(
                "INSERT INTO transfers
                   (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                    sha256, transferred_bytes, status, created_at, updated_at)
                 VALUES
                   ('legacy-outgoing', 'peer-one', 'outgoing', 'file', 'legacy.txt', 1,
                    'text/plain', '0000000000000000000000000000000000000000000000000000000000000000',
                    0, 'awaitingAcceptance', '2026-08-24T00:00:00.000Z', '2026-08-24T00:00:00.000Z'),
                   ('legacy-incoming', 'peer-one', 'incoming', 'file', 'legacy.txt', 1,
                    'text/plain', '0000000000000000000000000000000000000000000000000000000000000000',
                    0, 'transferring', '2026-08-24T00:00:00.000Z', '2026-08-24T00:00:00.000Z'),
                   ('v2-outgoing', 'peer-one', 'outgoing', 'file', 'v2.txt', 4194304,
                    'text/plain', '0000000000000000000000000000000000000000000000000000000000000000',
                    0, 'awaitingAcceptance', '2026-08-24T00:00:00.000Z', '2026-08-24T00:00:00.000Z');
                 UPDATE transfers SET transfer_protocol = 2, chunk_size = 4194304, chunk_count = 1,
                    manifest_sha256 = '0000000000000000000000000000000000000000000000000000000000000000'
                  WHERE transfer_id = 'v2-outgoing';",
            )
            .expect("insert fixtures");
        drop(connection);

        assert!(
            storage
                .replace_outgoing_chunks("legacy-outgoing", &[])
                .is_err()
        );
        assert!(
            !storage
                .commit_received_chunk(
                    "legacy-incoming",
                    "peer-one",
                    &TransferChunk {
                        index: 0,
                        length: 1,
                        sha256: [1; 32],
                    },
                    1,
                )
                .expect("legacy commit rejection")
        );
        assert!(
            !storage
                .commit_received_chunk(
                    "v2-outgoing",
                    "peer-one",
                    &TransferChunk {
                        index: 0,
                        length: TRANSFER_CHUNK_BYTES,
                        sha256: [1; 32],
                    },
                    u64::from(TRANSFER_CHUNK_BYTES),
                )
                .expect("outgoing commit rejection")
        );
        assert!(
            storage
                .replace_outgoing_chunks(
                    "v2-outgoing",
                    &[TransferChunk {
                        index: 0,
                        length: 1,
                        sha256: [1; 32],
                    }],
                )
                .is_err()
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn failed_outgoing_manifest_creation_leaves_no_partial_v2_transfer_row() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-v2-manifest-atomicity-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        let expected = TransferChunk {
            index: 0,
            length: TRANSFER_CHUNK_BYTES,
            sha256: [3; 32],
        };
        let transfer = TransferRecord {
            transfer_id: "atomic-v2".to_string(),
            peer_id: "peer-one".to_string(),
            direction: Direction::Outgoing,
            kind: TransferKind::File,
            file_name: "report.txt".to_string(),
            file_size: u64::from(TRANSFER_CHUNK_BYTES),
            mime_type: "text/plain".to_string(),
            sha256: "0".repeat(64),
            local_path: None,
            destination_reserved: false,
            reservation_token: None,
            transfer_protocol: 2,
            chunk_size: TRANSFER_CHUNK_BYTES,
            chunk_count: 1,
            manifest_sha256: Some(hex::encode(manifest_root(&[expected.clone()]))),
            partial_path: None,
            source_modified_ns: Some(1),
            send_claimed: false,
            transferred_bytes: 0,
            status: TransferStatus::AwaitingAcceptance,
            error: None,
            created_at: "2026-08-24T00:00:00.000Z".to_string(),
            updated_at: "2026-08-24T00:00:00.000Z".to_string(),
        };

        assert!(
            storage
                .create_outgoing_transfer_with_manifest(
                    &transfer,
                    &[TransferChunk {
                        length: 1,
                        ..expected
                    }],
                )
                .is_err()
        );
        assert!(
            storage
                .get_transfer("atomic-v2")
                .expect("query transfer")
                .is_none()
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn existing_peer_table_migrates_and_persists_capabilities() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-peer-capabilities-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        {
            let connection = Connection::open(&database).expect("open legacy database");
            connection
                .execute_batch(
                    "CREATE TABLE peers (
                       peer_id TEXT PRIMARY KEY NOT NULL,
                       nickname TEXT NOT NULL,
                       platform TEXT NOT NULL,
                       online INTEGER NOT NULL DEFAULT 0,
                       protocol_version INTEGER NOT NULL,
                       last_seen TEXT NOT NULL
                     );",
                )
                .expect("create legacy peers table");
        }

        let storage = Storage::open(&database).expect("migrate legacy database");
        let peer = PeerSummary {
            peer_id: "peer-one".to_string(),
            nickname: "Peer One".to_string(),
            platform: Platform::Windows,
            online: true,
            protocol_version: 1,
            capabilities: vec!["file-resume-v2".to_string()],
            last_seen: "2026-08-24T00:00:00.000Z".to_string(),
        };
        storage
            .upsert_peer(&peer)
            .expect("persist peer capabilities");

        let connection = storage.connection().expect("inspect migrated database");
        let columns: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('peers') WHERE name = 'capabilities_json'",
                [],
                |row| row.get(0),
            )
            .expect("query capabilities column");
        assert_eq!(columns, 1);
        drop(connection);
        assert_eq!(
            storage
                .get_peer("peer-one")
                .expect("load peer")
                .expect("persisted peer")
                .capabilities,
            vec!["file-resume-v2"]
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn incoming_transfer_stream_can_only_be_claimed_once() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-stream-claim-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, transferred_bytes, status, created_at, updated_at)
                     VALUES ('transfer-one', 'peer-one', 'incoming', 'file', 'report.txt', 3,
                             'text/plain', ?1, ?2, 0, 'transferring', ?3, ?3)",
                    rusqlite::params![
                        "0".repeat(64),
                        fixture.join("report.txt").to_string_lossy(),
                        "2026-08-24T00:00:00.000Z"
                    ],
                )
                .expect("insert transfer fixture");
        }

        assert!(
            storage
                .try_claim_incoming_transfer("transfer-one", "peer-one")
                .expect("claim first stream")
        );
        assert!(
            !storage
                .try_claim_incoming_transfer("transfer-one", "peer-one")
                .expect("reject duplicate stream")
        );
        storage
            .release_incoming_transfer_claim("transfer-one")
            .expect("release stream claim");
        assert!(
            storage
                .try_claim_incoming_transfer("transfer-one", "peer-one")
                .expect("claim stream after release")
        );
        assert!(
            !storage
                .try_cancel_unclaimed_incoming_transfer("transfer-one", "peer-one", 1, "cancelled")
                .expect("active stream must win cancellation race")
        );
        storage
            .release_incoming_transfer_claim("transfer-one")
            .expect("release second stream claim");
        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer("transfer-one", "peer-one", 1, "cancelled")
                .expect("cancel unclaimed incoming transfer")
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn cancelled_outgoing_transfer_cannot_be_revived_by_late_acceptance() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-outgoing-transition-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, transferred_bytes, status, created_at, updated_at)
                     VALUES ('outgoing-one', 'peer-one', 'outgoing', 'file', 'report.txt', 3,
                             'text/plain', ?1, ?2, 0, 'awaitingAcceptance', ?3, ?3)",
                    rusqlite::params![
                        "0".repeat(64),
                        fixture.join("report.txt").to_string_lossy(),
                        "2026-08-24T00:00:00.000Z"
                    ],
                )
                .expect("insert outgoing transfer fixture");
        }

        assert!(
            storage
                .try_transition_outgoing_awaiting(
                    "outgoing-one",
                    "peer-one",
                    TransferStatus::Cancelled,
                    Some("cancelled"),
                )
                .expect("cancel outgoing transfer")
        );
        assert!(
            !storage
                .try_transition_outgoing_awaiting(
                    "outgoing-one",
                    "peer-one",
                    TransferStatus::Transferring,
                    None,
                )
                .expect("ignore late acceptance")
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn cancelled_incoming_transfer_cannot_be_revived_by_manual_acceptance() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-incoming-transition-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transferred_bytes, status, created_at, updated_at)
                     VALUES ('incoming-awaiting', 'peer-one', 'incoming', 'file', 'report.txt', 3,
                             'text/plain', ?1, 0, 'awaitingAcceptance', ?2, ?2)",
                    rusqlite::params!["0".repeat(64), "2026-08-24T00:00:00.000Z"],
                )
                .expect("insert incoming transfer fixture");
        }

        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    "incoming-awaiting",
                    "peer-one",
                    1,
                    "cancelled",
                )
                .expect("cancel incoming transfer")
        );
        let mut cancelled = storage
            .get_transfer("incoming-awaiting")
            .expect("load cancelled transfer")
            .expect("cancelled transfer exists");
        cancelled.local_path = Some(fixture.join("report.txt").to_string_lossy().into_owned());
        cancelled.destination_reserved = true;
        cancelled.reservation_token = Some("random-token".to_string());
        cancelled.status = TransferStatus::Transferring;
        assert!(
            !storage
                .try_accept_incoming_transfer(&cancelled)
                .expect("ignore manual acceptance after cancellation")
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn incoming_cancel_rejects_a_stale_caller_protocol_snapshot() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-incoming-cancel-protocol-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let storage = Storage::open(&fixture.join("localnet.sqlite3")).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES ('incoming-protocol-cancel', 'peer-one', 'incoming', 'file',
                             'report.bin', 4, 'application/octet-stream', ?1, 2, ?2, 1, ?1, 0,
                             'paused', ?3, ?3)",
                    rusqlite::params![
                        "0".repeat(64),
                        TRANSFER_CHUNK_BYTES,
                        "2026-08-24T00:00:00.000Z",
                    ],
                )
                .expect("insert paused v2 transfer");
        }

        assert!(
            !storage
                .try_cancel_unclaimed_incoming_transfer(
                    "incoming-protocol-cancel",
                    "peer-one",
                    TransferProtocol::LegacyV1 as u8,
                    "stale caller cancellation",
                )
                .expect("stale protocol snapshot must lose")
        );
        assert_eq!(
            storage
                .get_transfer("incoming-protocol-cancel")
                .expect("load transfer")
                .expect("transfer exists")
                .status,
            TransferStatus::Paused
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn accepting_v2_persists_a_collision_derived_owned_partial() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-v2-partial-collision-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        let destination = fixture.join("report.bin");
        let token = "token-collision";
        reserve_receive_path(&destination, "incoming-collision", token)
            .expect("reserve destination");
        let partial = resumable_partial_path(&destination, "incoming-collision")
            .expect("derive deterministic partial");
        fs::write(&partial, b"unrelated-user-data").expect("write unrelated partial");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES ('incoming-collision', 'peer-one', 'incoming', 'file', 'report.bin',
                             4, 'application/octet-stream', ?1, 2, ?2, 1, ?1, 0,
                             'awaitingAcceptance', ?3, ?3)",
                    rusqlite::params![
                        "0".repeat(64),
                        TRANSFER_CHUNK_BYTES,
                        "2026-08-24T00:00:00.000Z",
                    ],
                )
                .expect("insert awaiting transfer");
        }
        let mut accepted = storage
            .get_transfer("incoming-collision")
            .expect("load awaiting transfer")
            .expect("awaiting transfer exists");
        accepted.local_path = Some(destination.to_string_lossy().into_owned());
        accepted.destination_reserved = true;
        accepted.reservation_token = Some(token.to_string());
        accepted.status = TransferStatus::Transferring;

        assert!(
            storage
                .try_accept_incoming_transfer(&accepted)
                .expect("accept with a collision-derived partial")
        );

        assert_eq!(
            fs::read(&partial).expect("read preserved unrelated partial"),
            b"unrelated-user-data"
        );
        let transferring = storage
            .get_transfer("incoming-collision")
            .expect("load accepted transfer")
            .expect("accepted transfer exists");
        assert_eq!(transferring.status, TransferStatus::Transferring);
        let derived = Path::new(
            transferring
                .partial_path
                .as_deref()
                .expect("persist collision-derived partial"),
        );
        assert_ne!(derived, partial);
        assert!(
            crate::receive_paths::resumable_partial_is_owned(
                derived,
                &destination,
                "incoming-collision",
                token,
            )
            .expect("derived partial is owned")
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn automatic_v2_acceptance_persists_a_collision_derived_owned_partial() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-v2-auto-partial-collision-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        let destination = fixture.join("report.bin");
        let token = "token-auto-collision";
        reserve_receive_path(&destination, "incoming-auto-collision", token)
            .expect("reserve destination");
        let partial = resumable_partial_path(&destination, "incoming-auto-collision")
            .expect("derive deterministic partial");
        fs::write(&partial, b"unrelated-user-data").expect("write unrelated partial");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES ('incoming-auto-collision', 'peer-one', 'incoming', 'file',
                             'report.bin', 4, 'application/octet-stream', ?1, 2, ?2, 1, ?1, 0,
                             'awaitingAcceptance', ?3, ?3)",
                    rusqlite::params![
                        "0".repeat(64),
                        TRANSFER_CHUNK_BYTES,
                        "2026-08-24T00:00:00.000Z",
                    ],
                )
                .expect("insert awaiting transfer");
        }
        let mut accepted = storage
            .get_transfer("incoming-auto-collision")
            .expect("load awaiting transfer")
            .expect("awaiting transfer exists");
        accepted.local_path = Some(destination.to_string_lossy().into_owned());
        accepted.destination_reserved = true;
        accepted.reservation_token = Some(token.to_string());
        accepted.status = TransferStatus::Transferring;

        storage
            .upsert_transfer(&accepted)
            .expect("automatic acceptance selects a collision-derived partial");

        assert_eq!(
            fs::read(&partial).expect("read preserved unrelated partial"),
            b"unrelated-user-data"
        );
        let transferring = storage
            .get_transfer("incoming-auto-collision")
            .expect("load accepted transfer")
            .expect("accepted transfer exists");
        assert_eq!(transferring.status, TransferStatus::Transferring);
        assert!(transferring.destination_reserved);
        assert_eq!(transferring.reservation_token.as_deref(), Some(token));
        let derived = Path::new(
            transferring
                .partial_path
                .as_deref()
                .expect("persist collision-derived partial"),
        );
        assert_ne!(derived, partial);
        assert!(
            crate::receive_paths::resumable_partial_is_owned(
                derived,
                &destination,
                "incoming-auto-collision",
                token,
            )
            .expect("derived partial is owned")
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn outgoing_resumable_claim_has_one_winner_and_pause_releases_atomically() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-outgoing-resume-claim-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES ('outgoing-resume', 'peer-one', 'outgoing', 'file', 'report.bin',
                             4194304, 'application/octet-stream', ?1, 2, 4194304, 1, ?1,
                             0, 'paused', ?2, ?2)",
                    rusqlite::params!["0".repeat(64), "2026-08-24T00:00:00.000Z"],
                )
                .expect("insert paused outgoing transfer");
        }

        assert!(
            storage
                .try_claim_outgoing_transfer("outgoing-resume", "peer-one")
                .expect("claim paused outgoing transfer")
        );
        assert!(
            !storage
                .try_claim_outgoing_transfer("outgoing-resume", "peer-one")
                .expect("reject duplicate outgoing claim")
        );
        assert!(
            storage
                .try_pause_claimed_outgoing_transfer(
                    "outgoing-resume",
                    "peer-one",
                    "network disconnected",
                )
                .expect("pause claimed outgoing transfer")
        );

        let paused = storage
            .get_transfer("outgoing-resume")
            .expect("load paused outgoing transfer")
            .expect("outgoing transfer exists");
        assert_eq!(paused.status, TransferStatus::Paused);
        assert!(!paused.send_claimed);
        assert_eq!(paused.error.as_deref(), Some("network disconnected"));

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn stale_v2_upsert_cannot_overwrite_an_active_outgoing_claim() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-stale-outgoing-upsert-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        let transfer = TransferRecord {
            transfer_id: "stale-outgoing".to_string(),
            peer_id: "peer-one".to_string(),
            direction: Direction::Outgoing,
            kind: TransferKind::File,
            file_name: "report.bin".to_string(),
            file_size: u64::from(TRANSFER_CHUNK_BYTES),
            mime_type: "application/octet-stream".to_string(),
            sha256: "0".repeat(64),
            local_path: Some(fixture.join("report.bin").to_string_lossy().into_owned()),
            destination_reserved: false,
            reservation_token: None,
            transfer_protocol: 2,
            chunk_size: TRANSFER_CHUNK_BYTES,
            chunk_count: 1,
            manifest_sha256: Some("0".repeat(64)),
            partial_path: None,
            source_modified_ns: Some(1),
            send_claimed: false,
            transferred_bytes: 0,
            status: TransferStatus::Transferring,
            error: None,
            created_at: "2026-08-24T00:00:00.000Z".to_string(),
            updated_at: "2026-08-24T00:00:00.000Z".to_string(),
        };
        storage
            .upsert_transfer(&transfer)
            .expect("insert outgoing transfer");
        let mut stale = transfer.clone();
        assert!(
            storage
                .try_claim_outgoing_transfer("stale-outgoing", "peer-one")
                .expect("claim outgoing transfer")
        );
        stale.status = TransferStatus::Cancelled;
        stale.error = Some("stale cancellation".to_string());
        stale.updated_at = "2026-08-24T00:00:01.000Z".to_string();
        storage
            .upsert_transfer(&stale)
            .expect("ignore stale v2 upsert while claimed");

        let active = storage
            .get_transfer("stale-outgoing")
            .expect("load active transfer")
            .expect("active transfer exists");
        assert_eq!(active.status, TransferStatus::Transferring);
        assert!(active.send_claimed);
        assert_eq!(active.error, None);

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn stale_v2_upsert_cannot_resurrect_a_cancelled_outgoing_transfer() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-stale-cancelled-upsert-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let storage = Storage::open(&fixture.join("localnet.sqlite3")).expect("open storage");
        let transfer = TransferRecord {
            transfer_id: "stale-cancelled".to_string(),
            peer_id: "peer-one".to_string(),
            direction: Direction::Outgoing,
            kind: TransferKind::File,
            file_name: "report.bin".to_string(),
            file_size: u64::from(TRANSFER_CHUNK_BYTES),
            mime_type: "application/octet-stream".to_string(),
            sha256: "0".repeat(64),
            local_path: Some(fixture.join("report.bin").to_string_lossy().into_owned()),
            destination_reserved: false,
            reservation_token: None,
            transfer_protocol: 2,
            chunk_size: TRANSFER_CHUNK_BYTES,
            chunk_count: 1,
            manifest_sha256: Some("0".repeat(64)),
            partial_path: None,
            source_modified_ns: Some(1),
            send_claimed: false,
            transferred_bytes: 0,
            status: TransferStatus::Cancelled,
            error: Some("cancelled".to_string()),
            created_at: "2026-08-24T00:00:00.000Z".to_string(),
            updated_at: "2026-08-24T00:00:00.000Z".to_string(),
        };
        storage
            .upsert_transfer(&transfer)
            .expect("insert cancelled outgoing transfer");
        let mut stale = transfer.clone();
        stale.status = TransferStatus::Transferring;
        stale.error = None;
        stale.updated_at = "2026-08-24T00:00:01.000Z".to_string();

        storage
            .upsert_transfer(&stale)
            .expect("ignore stale update after cancellation");

        let cancelled = storage
            .get_transfer("stale-cancelled")
            .expect("load cancelled transfer")
            .expect("cancelled transfer exists");
        assert_eq!(cancelled.status, TransferStatus::Cancelled);
        assert_eq!(cancelled.error.as_deref(), Some("cancelled"));
        assert!(!cancelled.send_claimed);

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn generic_upsert_cannot_mutate_paused_v2_recovery_metadata() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-paused-metadata-upsert-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let storage = Storage::open(&fixture.join("localnet.sqlite3")).expect("open storage");
        let mut transfer = TransferRecord {
            transfer_id: "paused-metadata".to_string(),
            peer_id: "peer-one".to_string(),
            direction: Direction::Outgoing,
            kind: TransferKind::File,
            file_name: "report.bin".to_string(),
            file_size: u64::from(TRANSFER_CHUNK_BYTES),
            mime_type: "application/octet-stream".to_string(),
            sha256: "0".repeat(64),
            local_path: Some(fixture.join("source.bin").to_string_lossy().into_owned()),
            destination_reserved: false,
            reservation_token: None,
            transfer_protocol: 2,
            chunk_size: TRANSFER_CHUNK_BYTES,
            chunk_count: 1,
            manifest_sha256: Some("0".repeat(64)),
            partial_path: None,
            source_modified_ns: Some(1),
            send_claimed: false,
            transferred_bytes: 0,
            status: TransferStatus::Paused,
            error: Some("network disconnected".to_string()),
            created_at: "2026-08-24T00:00:00.000Z".to_string(),
            updated_at: "2026-08-24T00:00:00.000Z".to_string(),
        };
        storage
            .upsert_transfer(&transfer)
            .expect("insert paused transfer");
        let original = storage
            .get_transfer(&transfer.transfer_id)
            .unwrap()
            .unwrap();
        transfer.local_path = Some(
            fixture
                .join("stale-source.bin")
                .to_string_lossy()
                .into_owned(),
        );
        transfer.status = TransferStatus::Transferring;
        transfer.error = None;
        transfer.updated_at = "2026-08-24T00:00:01.000Z".to_string();

        storage
            .upsert_transfer(&transfer)
            .expect("ignore generic paused v2 mutation");

        let retained = storage
            .get_transfer(&transfer.transfer_id)
            .unwrap()
            .unwrap();
        assert_eq!(retained.local_path, original.local_path);
        assert_eq!(retained.status, TransferStatus::Paused);
        assert_eq!(retained.error, original.error);
        assert_eq!(retained.updated_at, original.updated_at);

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[test]
    fn outgoing_ack_progress_requires_the_active_claim_and_cannot_race_cancel() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-claimed-outgoing-progress-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES ('claimed-progress', 'peer-one', 'outgoing', 'file', 'report.bin',
                             8388608, 'application/octet-stream', ?1, 2, 4194304, 2, ?1,
                             0, 'transferring', ?2, ?2)",
                    rusqlite::params!["0".repeat(64), "2026-08-24T00:00:00.000Z"],
                )
                .expect("insert outgoing transfer");
        }

        assert!(
            !storage
                .commit_claimed_outgoing_progress(
                    "claimed-progress",
                    "peer-one",
                    0,
                    u64::from(TRANSFER_CHUNK_BYTES),
                )
                .expect("reject progress before claim")
        );
        assert!(
            storage
                .try_claim_outgoing_transfer("claimed-progress", "peer-one")
                .expect("claim outgoing transfer")
        );
        assert!(
            !storage
                .try_cancel_unclaimed_outgoing_transfer(
                    "claimed-progress",
                    "peer-one",
                    "cancelled",
                )
                .expect("active claim wins cancellation")
        );
        assert!(
            storage
                .commit_claimed_outgoing_progress(
                    "claimed-progress",
                    "peer-one",
                    0,
                    u64::from(TRANSFER_CHUNK_BYTES),
                )
                .expect("commit acknowledged progress")
        );
        assert!(
            storage
                .try_pause_claimed_outgoing_transfer(
                    "claimed-progress",
                    "peer-one",
                    "connection reset",
                )
                .expect("pause claimed transfer")
        );
        assert!(
            !storage
                .commit_claimed_outgoing_progress(
                    "claimed-progress",
                    "peer-one",
                    u64::from(TRANSFER_CHUNK_BYTES),
                    u64::from(TRANSFER_CHUNK_BYTES) * 2,
                )
                .expect("reject stale progress after pause")
        );

        let paused = storage
            .get_transfer("claimed-progress")
            .expect("load paused transfer")
            .expect("paused transfer exists");
        assert_eq!(paused.status, TransferStatus::Paused);
        assert!(!paused.send_claimed);
        assert_eq!(paused.transferred_bytes, u64::from(TRANSFER_CHUNK_BYTES));

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn separate_sqlite_connections_have_one_claim_or_cancel_winner() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-concurrent-claim-cancel-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let claimant = Storage::open(&database).expect("open claimant connection");
        let canceller = Storage::open(&database).expect("open canceller connection");
        {
            let connection = claimant.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES ('concurrent-race', 'peer-one', 'outgoing', 'file', 'report.bin',
                             4194304, 'application/octet-stream', ?1, 2, 4194304, 1, ?1,
                             0, 'transferring', ?2, ?2)",
                    rusqlite::params!["0".repeat(64), "2026-08-24T00:00:00.000Z"],
                )
                .expect("insert outgoing transfer");
        }
        let inspect = claimant.clone();
        let barrier = Arc::new(Barrier::new(3));
        let claim_barrier = barrier.clone();
        let cancel_barrier = barrier.clone();
        let claim_thread = thread::spawn(move || {
            claim_barrier.wait();
            claimant
                .try_claim_outgoing_transfer("concurrent-race", "peer-one")
                .expect("run concurrent claim")
        });
        let cancel_thread = thread::spawn(move || {
            cancel_barrier.wait();
            canceller
                .try_cancel_unclaimed_outgoing_transfer("concurrent-race", "peer-one", "cancelled")
                .expect("run concurrent cancel")
        });
        barrier.wait();
        let claim_won = claim_thread.join().expect("join claimant");
        let cancel_won = cancel_thread.join().expect("join canceller");

        assert_ne!(claim_won, cancel_won);
        let transfer = inspect
            .get_transfer("concurrent-race")
            .expect("load race result")
            .expect("race transfer exists");
        if claim_won {
            assert_eq!(transfer.status, TransferStatus::Transferring);
            assert!(transfer.send_claimed);
        } else {
            assert_eq!(transfer.status, TransferStatus::Cancelled);
            assert!(!transfer.send_claimed);
        }

        drop(inspect);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn stale_incoming_chunk_callback_cannot_commit_without_an_active_claim() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-stale-incoming-progress-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        let chunk = TransferChunk {
            index: 0,
            length: 4,
            sha256: [7; 32],
        };
        let root = hex::encode(manifest_root(std::slice::from_ref(&chunk)));
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, receive_claimed, created_at, updated_at)
                     VALUES ('stale-incoming-progress', 'peer-one', 'incoming', 'file',
                             'report.bin', 4, 'application/octet-stream', ?1, 2, ?2, 1, ?3,
                             0, 'cancelled', 0, ?4, ?4)",
                    rusqlite::params![
                        "0".repeat(64),
                        TRANSFER_CHUNK_BYTES,
                        root,
                        "2026-08-24T00:00:00.000Z",
                    ],
                )
                .expect("insert cancelled incoming transfer");
        }

        assert!(
            !storage
                .commit_received_chunk("stale-incoming-progress", "peer-one", &chunk, 4)
                .expect("reject stale chunk callback")
        );
        assert_eq!(
            storage
                .get_transfer("stale-incoming-progress")
                .expect("load incoming transfer")
                .expect("incoming transfer exists")
                .transferred_bytes,
            0
        );
        assert!(
            storage
                .list_transfer_chunks("stale-incoming-progress")
                .expect("load persisted chunks")
                .is_empty()
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn incoming_resume_claim_and_cancel_have_one_cas_winner() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-incoming-resume-cancel-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, transfer_protocol, chunk_size, chunk_count,
                        manifest_sha256, transferred_bytes, status, created_at, updated_at)
                     VALUES ('incoming-resume', 'peer-one', 'incoming', 'file', 'report.bin',
                             4194304, 'application/octet-stream', ?1, ?2, 2, 4194304, 1, ?1,
                             0, 'paused', ?3, ?3)",
                    rusqlite::params![
                        "0".repeat(64),
                        fixture.join("report.bin").to_string_lossy(),
                        "2026-08-24T00:00:00.000Z"
                    ],
                )
                .expect("insert paused incoming transfer");
        }

        assert!(
            storage
                .try_claim_incoming_transfer("incoming-resume", "peer-one")
                .expect("claim paused incoming transfer")
        );
        assert!(
            !storage
                .try_cancel_unclaimed_incoming_transfer(
                    "incoming-resume",
                    "peer-one",
                    2,
                    "cancelled"
                )
                .expect("active resume claim wins cancellation race")
        );
        assert!(
            storage
                .try_pause_claimed_incoming_transfer(
                    "incoming-resume",
                    "peer-one",
                    "network disconnected",
                )
                .expect("pause incoming resume")
        );
        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    "incoming-resume",
                    "peer-one",
                    2,
                    "cancelled"
                )
                .expect("cancel after pause releases claim")
        );
        assert!(
            !storage
                .try_claim_incoming_transfer("incoming-resume", "peer-one")
                .expect("cancelled incoming transfer cannot resume")
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn raw_claim_release_cannot_open_a_v2_status_transition_window() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-v2-raw-release-guard-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES ('v2-release-guard', 'peer-one', 'incoming', 'file', 'report.bin',
                             4194304, 'application/octet-stream', ?1, 2, 4194304, 1, ?1,
                             0, 'transferring', ?2, ?2)",
                    rusqlite::params!["0".repeat(64), "2026-08-24T00:00:00.000Z"],
                )
                .expect("insert v2 transfer");
        }
        assert!(
            storage
                .try_claim_incoming_transfer("v2-release-guard", "peer-one")
                .expect("claim v2 transfer")
        );

        storage
            .release_incoming_transfer_claim("v2-release-guard")
            .expect_err("v2 claims require a status-and-claim CAS");

        assert!(receive_claimed(&storage, "v2-release-guard"));
        assert_eq!(
            storage
                .get_transfer("v2-release-guard")
                .expect("load guarded transfer")
                .expect("guarded transfer exists")
                .status,
            TransferStatus::Transferring
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn completed_claimed_transfer_ignores_late_failure() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-completed-late-failure-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES ('outgoing-complete', 'peer-one', 'outgoing', 'file', 'report.bin',
                             4194304, 'application/octet-stream', ?1, 2, 4194304, 1, ?1,
                             0, 'paused', ?2, ?2)",
                    rusqlite::params!["0".repeat(64), "2026-08-24T00:00:00.000Z"],
                )
                .expect("insert paused outgoing transfer");
        }

        assert!(
            storage
                .try_claim_outgoing_transfer("outgoing-complete", "peer-one")
                .expect("claim outgoing transfer")
        );
        assert!(
            storage
                .try_complete_claimed_outgoing_transfer("outgoing-complete", "peer-one")
                .expect("complete claimed outgoing transfer")
        );
        assert!(
            !storage
                .try_fail_claimed_outgoing_transfer(
                    "outgoing-complete",
                    "peer-one",
                    "late stream failure",
                )
                .expect("ignore late failure")
        );

        let completed = storage
            .get_transfer("outgoing-complete")
            .expect("load completed transfer")
            .expect("completed transfer exists");
        assert_eq!(completed.status, TransferStatus::Completed);
        assert_eq!(completed.transferred_bytes, completed.file_size);
        assert!(!completed.send_claimed);
        assert_eq!(completed.error, None);

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn incoming_completion_persists_finalized_path_and_clears_claim_in_one_cas() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-incoming-completion-cas-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixture");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, destination_reserved, reservation_token,
                        receive_claimed, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        partial_path, transferred_bytes, status, created_at, updated_at)
                     VALUES ('incoming-complete', 'peer-one', 'incoming', 'file', 'report.bin',
                             4194304, 'application/octet-stream', ?1, ?2, 1, 'token-one', 1,
                             2, 4194304, 1, ?1, ?3, 4194304, 'transferring', ?4, ?4)",
                    rusqlite::params![
                        "0".repeat(64),
                        fixture.join("report.bin").to_string_lossy(),
                        fixture.join("partial.part").to_string_lossy(),
                        "2026-08-24T00:00:00.000Z",
                    ],
                )
                .expect("insert claimed incoming transfer");
        }
        let mut finalized = storage
            .get_transfer("incoming-complete")
            .expect("load claimed incoming transfer")
            .expect("incoming transfer exists");
        let final_path = fixture.join("report (1).bin");
        finalized.local_path = Some(final_path.to_string_lossy().into_owned());
        finalized.destination_reserved = false;
        finalized.reservation_token = None;
        finalized.partial_path = None;
        finalized.status = TransferStatus::Completed;
        finalized.error = None;

        assert!(
            storage
                .try_complete_claimed_incoming_transfer(&finalized)
                .expect("complete finalized incoming transfer")
        );

        let completed = storage
            .get_transfer("incoming-complete")
            .expect("load completed incoming transfer")
            .expect("completed transfer exists");
        assert_eq!(completed.status, TransferStatus::Completed);
        assert_eq!(
            completed.local_path.as_deref(),
            Some(final_path.to_string_lossy().as_ref())
        );
        assert!(!completed.destination_reserved);
        assert_eq!(completed.reservation_token, None);
        assert_eq!(completed.partial_path, None);
        assert!(!receive_claimed(&storage, "incoming-complete"));
        assert!(
            !storage
                .try_fail_claimed_incoming_transfer(
                    "incoming-complete",
                    "peer-one",
                    "late final acknowledgement failure",
                )
                .expect("ignore late failure after completion")
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn resumable_outgoing_query_is_scoped_to_paused_v2_peer_records() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-resumable-outgoing-query-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert transfer fixtures");
            connection
                .execute_batch(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES
                       ('wanted', 'peer-one', 'outgoing', 'file', 'wanted.bin', 4194304,
                        'application/octet-stream',
                        '0000000000000000000000000000000000000000000000000000000000000000',
                        2, 4194304, 1,
                        '0000000000000000000000000000000000000000000000000000000000000000',
                        0, 'paused', '2026-08-24T00:00:00.000Z', '2026-08-24T00:00:00.000Z'),
                       ('legacy', 'peer-one', 'outgoing', 'file', 'legacy.bin', 1,
                        'application/octet-stream',
                        '0000000000000000000000000000000000000000000000000000000000000000',
                        1, 0, 0, NULL, 0, 'paused', '2026-08-24T00:00:00.000Z',
                        '2026-08-24T00:00:00.000Z'),
                       ('other-peer', 'peer-two', 'outgoing', 'file', 'other.bin', 4194304,
                        'application/octet-stream',
                        '0000000000000000000000000000000000000000000000000000000000000000',
                        2, 4194304, 1,
                        '0000000000000000000000000000000000000000000000000000000000000000',
                        0, 'paused', '2026-08-24T00:00:00.000Z', '2026-08-24T00:00:00.000Z'),
                       ('incoming', 'peer-one', 'incoming', 'file', 'incoming.bin', 4194304,
                        'application/octet-stream',
                        '0000000000000000000000000000000000000000000000000000000000000000',
                        2, 4194304, 1,
                        '0000000000000000000000000000000000000000000000000000000000000000',
                        0, 'paused', '2026-08-24T00:00:00.000Z', '2026-08-24T00:00:00.000Z');",
                )
                .expect("insert query fixtures");
        }

        let resumable = storage
            .list_resumable_outgoing("peer-one")
            .expect("list resumable outgoing transfers");
        assert_eq!(resumable.len(), 1);
        assert_eq!(resumable[0].transfer_id, "wanted");

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn startup_pauses_v2_and_preserves_equal_committed_partial_and_reservation() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-startup-equal-partial-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let committed = u64::from(TRANSFER_CHUNK_BYTES);
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "startup-equal",
            committed,
            Some(committed),
        );

        let storage = Storage::open(&database).expect("reopen and reconcile startup state");
        let transfer = storage
            .get_transfer("startup-equal")
            .expect("load reconciled transfer")
            .expect("transfer exists");
        assert_eq!(transfer.status, TransferStatus::Paused);
        assert_eq!(transfer.transferred_bytes, committed);
        assert!(!receive_claimed(&storage, "startup-equal"));
        assert_eq!(
            storage.list_transfer_chunks("startup-equal").unwrap().len(),
            1
        );
        assert_eq!(
            fs::metadata(&partial).expect("inspect partial").len(),
            committed
        );
        assert!(
            reservation_is_owned(&destination, "startup-equal", "token-startup-equal")
                .expect("inspect preserved reservation")
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn startup_never_accepts_an_incomplete_copy_candidate_and_completes_after_retry() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-copy-candidate-restart-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let transfer_id = "copy-candidate-restart";
        let token = "token-copy-candidate-restart";
        let payload = vec![0x6d; 192 * 1024];
        let chunk = TransferChunk {
            index: 0,
            length: u32::try_from(payload.len()).expect("small payload"),
            sha256: Sha256::digest(&payload).into(),
        };
        reserve_receive_path(&destination, transfer_id, token).expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, transfer_id, token)
            .expect("reserve owned partial");
        fs::write(&partial, &payload).expect("write completed partial");
        {
            let connection = Connection::open(&database).expect("open fixture database");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, destination_reserved, reservation_token,
                        receive_claimed, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        partial_path, transferred_bytes, status, created_at, updated_at)
                     VALUES (?1, 'peer-one', 'incoming', 'file', 'report.bin', ?2,
                             'application/octet-stream', ?3, ?4, 1, ?5, 1, 2, ?6, 1, ?7,
                             ?8, ?2, 'transferring', ?9, ?9)",
                    rusqlite::params![
                        transfer_id,
                        payload.len(),
                        hex::encode(Sha256::digest(&payload)),
                        destination.to_string_lossy(),
                        token,
                        TRANSFER_CHUNK_BYTES,
                        hex::encode(manifest_root(std::slice::from_ref(&chunk))),
                        partial.to_string_lossy(),
                        "2026-08-24T00:00:00.000Z",
                    ],
                )
                .expect("insert claimed incoming transfer");
            connection
                .execute(
                    "INSERT INTO transfer_chunks
                       (transfer_id, chunk_index, chunk_length, sha256)
                     VALUES (?1, 0, ?2, ?3)",
                    rusqlite::params![transfer_id, chunk.length, chunk.sha256.as_slice()],
                )
                .expect("insert committed chunk");
        }
        let first_link_calls = std::cell::Cell::new(0_u32);
        let error = finalize_reserved_receive_copy_fallback_with_hooks(
            &partial,
            &destination,
            "report.bin",
            transfer_id,
            token,
            |_, _| Ok(()),
            |_, _| {
                first_link_calls.set(first_link_calls.get() + 1);
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "representative filesystem does not support hard links",
                ))
            },
            |phase| {
                if phase == FinalizationPhase::DuringCopy {
                    Err(io::Error::other("injected copy interruption"))
                } else {
                    Ok(())
                }
            },
        )
        .err()
        .expect("copy interruption precedes candidate sync and materialized journal");
        assert!(error.to_string().contains("injected copy interruption"));
        assert_eq!(first_link_calls.get(), 1);
        assert!(destination.metadata().unwrap().len() < payload.len() as u64);

        let storage = Storage::open(&database).expect("restart with incomplete copy candidate");
        let paused = storage
            .get_transfer(transfer_id)
            .unwrap()
            .expect("transfer remains recoverable");
        assert_eq!(paused.status, TransferStatus::Paused);
        assert!(paused.destination_reserved);
        assert_eq!(paused.transferred_bytes, payload.len() as u64);
        assert!(destination.metadata().unwrap().len() < payload.len() as u64);
        drop(storage);

        let retry_link_calls = std::cell::Cell::new(0_u32);
        let finalized = finalize_reserved_receive_copy_fallback_with_hooks(
            &partial,
            &destination,
            "report.bin",
            transfer_id,
            token,
            |_, _| Ok(()),
            |_, _| {
                retry_link_calls.set(retry_link_calls.get() + 1);
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "representative filesystem does not support hard links",
                ))
            },
            |_| Ok(()),
        )
        .expect("retry restarts through the same owned candidate identity");
        assert_eq!(retry_link_calls.get(), 0);
        assert_eq!(finalized.path, destination);
        assert_eq!(fs::read(&destination).unwrap(), payload);

        let storage = Storage::open(&database).expect("restart after candidate materialization");
        let completed = storage
            .get_transfer(transfer_id)
            .unwrap()
            .expect("transfer completes from durable journal");
        assert_eq!(completed.status, TransferStatus::Completed);
        assert_eq!(
            completed.local_path.as_deref(),
            Some(destination.to_string_lossy().as_ref())
        );
        assert!(!completed.destination_reserved);
        assert!(completed.reservation_token.is_none());
        assert!(completed.partial_path.is_none());
        assert_eq!(fs::read(&destination).unwrap(), payload);

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[test]
    fn startup_completes_an_owned_v2_file_finalized_before_database_commit() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-finalize-crash-recovery-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let transfer_id = "finalize-crash";
        let token = "token-finalize-crash";
        let payload = b"completed-before-database";
        let file_sha256 = hex::encode(Sha256::digest(payload));
        let chunk = TransferChunk {
            index: 0,
            length: u32::try_from(payload.len()).expect("small payload"),
            sha256: Sha256::digest(payload).into(),
        };
        let manifest_sha256 = hex::encode(manifest_root(std::slice::from_ref(&chunk)));
        reserve_receive_path(&destination, transfer_id, token).expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, transfer_id, token)
            .expect("reserve owned partial");
        fs::write(&partial, payload).expect("write completed partial");
        fs::write(&destination, b"late-user-collision").expect("create late destination collision");
        {
            let connection = Connection::open(&database).expect("open fixture database");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, destination_reserved, reservation_token,
                        receive_claimed, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        partial_path, transferred_bytes, status, created_at, updated_at)
                     VALUES (?1, 'peer-one', 'incoming', 'file', 'report.bin', ?2,
                             'application/octet-stream', ?3, ?4, 1, ?5, 1, 2, ?6, 1, ?7,
                             ?8, ?2, 'transferring', ?9, ?9)",
                    rusqlite::params![
                        transfer_id,
                        payload.len(),
                        file_sha256,
                        destination.to_string_lossy(),
                        token,
                        TRANSFER_CHUNK_BYTES,
                        manifest_sha256,
                        partial.to_string_lossy(),
                        "2026-08-24T00:00:00.000Z",
                    ],
                )
                .expect("insert claimed incoming transfer");
            connection
                .execute(
                    "INSERT INTO transfer_chunks
                       (transfer_id, chunk_index, chunk_length, sha256)
                     VALUES (?1, 0, ?2, ?3)",
                    rusqlite::params![transfer_id, chunk.length, chunk.sha256.as_slice(),],
                )
                .expect("insert committed chunk");
        }
        let finalized = finalize_reserved_receive_durable_with_hooks(
            &partial,
            &destination,
            "report.bin",
            transfer_id,
            token,
            |previous, next| {
                let connection = Connection::open(&database).map_err(std::io::Error::other)?;
                let changed = connection
                    .execute(
                        "UPDATE transfers SET local_path = ?2
                         WHERE transfer_id = ?1 AND local_path = ?3
                           AND status = 'transferring' AND receive_claimed = 1",
                        rusqlite::params![
                            transfer_id,
                            next.to_string_lossy(),
                            previous.to_string_lossy()
                        ],
                    )
                    .map_err(std::io::Error::other)?;
                if changed == 1 {
                    Ok(())
                } else {
                    Err(std::io::Error::other("metadata switch CAS lost"))
                }
            },
            |_| Ok(()),
        )
        .expect("simulate filesystem finalization before database commit");
        let finalized_path = finalized.path;
        assert_ne!(finalized_path, destination);
        assert!(
            partial.exists(),
            "partial remains until database completion"
        );

        let storage = Storage::open(&database).expect("recover finalized receive on startup");
        let completed = storage
            .get_transfer(transfer_id)
            .expect("load recovered transfer")
            .expect("recovered transfer exists");
        assert_eq!(completed.status, TransferStatus::Completed);
        assert_eq!(completed.transferred_bytes, payload.len() as u64);
        assert_eq!(
            completed.local_path.as_deref(),
            Some(finalized_path.to_string_lossy().as_ref())
        );
        assert_eq!(
            fs::read(&destination).expect("read preserved collision"),
            b"late-user-collision"
        );
        assert_eq!(fs::read(&finalized_path).expect("read final file"), payload);
        assert!(!completed.destination_reserved);
        assert_eq!(completed.reservation_token, None);
        assert_eq!(completed.partial_path, None);
        assert!(
            !partial.exists(),
            "completed database state precedes cleanup"
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn startup_cleans_a_finalized_receive_completed_before_partial_cleanup() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-complete-before-cleanup-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage");
        let destination = fixture.join("report.bin");
        let transfer_id = "complete-before-cleanup";
        let token = "token-complete-before-cleanup";
        let payload = b"durably completed payload";
        let chunk = TransferChunk {
            index: 0,
            length: u32::try_from(payload.len()).unwrap(),
            sha256: Sha256::digest(payload).into(),
        };
        reserve_receive_path(&destination, transfer_id, token).expect("reserve destination");
        let partial = reserve_resumable_partial(&destination, transfer_id, token)
            .expect("reserve owned partial");
        fs::write(&partial, payload).expect("write partial");
        {
            let connection = storage.connection().expect("insert transfer");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, destination_reserved, reservation_token,
                        receive_claimed, transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        partial_path, transferred_bytes, status, created_at, updated_at)
                     VALUES (?1, 'peer-one', 'incoming', 'file', 'report.bin', ?2,
                             'application/octet-stream', ?3, ?4, 1, ?5, 1, 2, ?6, 1, ?7,
                             ?8, ?2, 'transferring', ?9, ?9)",
                    rusqlite::params![
                        transfer_id,
                        payload.len(),
                        hex::encode(Sha256::digest(payload)),
                        destination.to_string_lossy(),
                        token,
                        TRANSFER_CHUNK_BYTES,
                        hex::encode(manifest_root(std::slice::from_ref(&chunk))),
                        partial.to_string_lossy(),
                        "2026-08-24T00:00:00.000Z",
                    ],
                )
                .expect("insert claimed transfer");
        }
        let finalized = finalize_reserved_receive_durable(
            &partial,
            &destination,
            "report.bin",
            transfer_id,
            token,
        )
        .expect("finalize owned payload");
        let mut completed = storage
            .get_transfer(transfer_id)
            .expect("load transfer")
            .expect("transfer exists");
        completed.local_path = Some(finalized.path.to_string_lossy().into_owned());
        completed.transferred_bytes = completed.file_size;
        completed.status = TransferStatus::Completed;
        assert!(
            storage
                .try_complete_claimed_incoming_transfer(&completed)
                .expect("commit completed database state")
        );
        assert!(
            partial.exists(),
            "injected crash occurs before partial cleanup"
        );
        drop(storage);

        let storage = Storage::open(&database).expect("retry completed cleanup at startup");
        let completed = storage
            .get_transfer(transfer_id)
            .unwrap()
            .expect("completed transfer remains");
        assert_eq!(completed.status, TransferStatus::Completed);
        assert_eq!(completed.partial_path, None);
        assert!(!completed.destination_reserved);
        assert!(!partial.exists());
        assert_eq!(fs::read(finalized.path).unwrap(), payload);

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[test]
    fn lost_recovery_claim_leaves_the_partial_filesystem_untouched() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-recovery-cas-lost-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "recovery-cas-lost",
            u64::from(TRANSFER_CHUNK_BYTES),
            Some(u64::from(TRANSFER_CHUNK_BYTES) + 17),
        );
        let first = Storage::open(&database).expect("perform initial startup reconciliation");
        fs::OpenOptions::new()
            .write(true)
            .open(&partial)
            .expect("open partial")
            .set_len(u64::from(TRANSFER_CHUNK_BYTES) + 31)
            .expect("restore uncommitted tail");
        {
            let connection = first.connection().expect("reset paused recovery fixture");
            connection
                .execute(
                    "UPDATE transfers SET status = 'paused', receive_claimed = 1
                     WHERE transfer_id = 'recovery-cas-lost'",
                    [],
                )
                .expect("make recovery CAS lose to another owner");
        }
        let record = PartialRecoveryRecord {
            transfer_id: "recovery-cas-lost".to_string(),
            peer_id: "peer-one".to_string(),
            destination: Some(destination),
            partial: Some(partial.clone()),
            file_size: i64::from(TRANSFER_CHUNK_BYTES) * 2 + 2,
            chunk_size: i64::from(TRANSFER_CHUNK_BYTES),
            chunk_count: 3,
            manifest_sha256: Some("0".repeat(64)),
            committed_bytes: i64::from(TRANSFER_CHUNK_BYTES),
            sha256: "0".repeat(64),
            destination_reserved: true,
            reservation_token: Some("token-recovery-cas-lost".to_string()),
        };

        first
            .reconcile_resumable_partial(&record)
            .expect("lost recovery claim is a no-op");

        assert_eq!(
            fs::metadata(&partial).expect("partial metadata").len(),
            u64::from(TRANSFER_CHUNK_BYTES) + 31
        );

        drop(first);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn stale_recovery_snapshot_cannot_touch_the_old_partial_after_metadata_changes() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-recovery-stale-metadata-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "recovery-stale-metadata",
            u64::from(TRANSFER_CHUNK_BYTES),
            Some(u64::from(TRANSFER_CHUNK_BYTES) + 17),
        );
        let storage = Storage::open(&database).expect("perform initial startup reconciliation");
        fs::OpenOptions::new()
            .write(true)
            .open(&partial)
            .expect("open partial")
            .set_len(u64::from(TRANSFER_CHUNK_BYTES) + 41)
            .expect("restore uncommitted tail");
        let replacement_partial = fixture.join("replacement-owned-partial.part");
        let external = Connection::open(&database).expect("open concurrent metadata connection");
        external
            .execute(
                "UPDATE transfers SET partial_path = ?2
                 WHERE transfer_id = ?1 AND status = 'paused'",
                rusqlite::params![
                    "recovery-stale-metadata",
                    replacement_partial.to_string_lossy(),
                ],
            )
            .expect("replace recovery metadata concurrently");
        let record = PartialRecoveryRecord {
            transfer_id: "recovery-stale-metadata".to_string(),
            peer_id: "peer-one".to_string(),
            destination: Some(destination),
            partial: Some(partial.clone()),
            file_size: i64::from(TRANSFER_CHUNK_BYTES) * 2 + 2,
            chunk_size: i64::from(TRANSFER_CHUNK_BYTES),
            chunk_count: 3,
            manifest_sha256: Some("0".repeat(64)),
            committed_bytes: i64::from(TRANSFER_CHUNK_BYTES),
            sha256: "0".repeat(64),
            destination_reserved: true,
            reservation_token: Some("token-recovery-stale-metadata".to_string()),
        };

        storage
            .reconcile_resumable_partial(&record)
            .expect("stale recovery metadata must lose its CAS");

        assert_eq!(
            fs::metadata(&partial).expect("old partial metadata").len(),
            u64::from(TRANSFER_CHUNK_BYTES) + 41
        );
        assert_eq!(
            storage
                .get_transfer("recovery-stale-metadata")
                .expect("load transfer")
                .expect("transfer exists")
                .partial_path
                .as_deref(),
            Some(replacement_partial.to_string_lossy().as_ref())
        );

        drop(external);
        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn recovery_truncates_the_verified_handle_not_a_replacement_path() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-recovery-handle-race-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        let storage = Storage::open(&database).expect("open storage before fixture insertion");
        let destination = fixture.join("report.bin");
        let committed = u64::from(TRANSFER_CHUNK_BYTES);
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "recovery-handle-race",
            committed,
            Some(committed + 73),
        );
        {
            let connection = storage.connection().expect("pause recovery fixture");
            connection
                .execute(
                    "UPDATE transfers SET status = 'paused', receive_claimed = 0
                     WHERE transfer_id = 'recovery-handle-race'",
                    [],
                )
                .expect("pause transfer");
        }
        let record = PartialRecoveryRecord {
            transfer_id: "recovery-handle-race".to_string(),
            peer_id: "peer-one".to_string(),
            destination: Some(destination),
            partial: Some(partial.clone()),
            file_size: i64::from(TRANSFER_CHUNK_BYTES) * 2 + 2,
            chunk_size: i64::from(TRANSFER_CHUNK_BYTES),
            chunk_count: 3,
            manifest_sha256: Some("0".repeat(64)),
            committed_bytes: i64::from(TRANSFER_CHUNK_BYTES),
            sha256: "0".repeat(64),
            destination_reserved: true,
            reservation_token: Some("token-recovery-handle-race".to_string()),
        };
        assert!(storage.try_claim_incoming_recovery(&record).unwrap());
        let detached_owned = fixture.join("detached-owned-partial");
        let replacement_length = committed + 211;

        storage
            .reconcile_claimed_resumable_partial_with_hook(&record, || {
                fs::rename(&partial, &detached_owned)?;
                fs::File::create(&partial)?.set_len(replacement_length)?;
                Ok(())
            })
            .expect("reconcile through the verified open handle");

        assert_eq!(
            fs::metadata(&partial).expect("replacement metadata").len(),
            replacement_length,
            "the unowned replacement path must not be truncated"
        );
        assert_eq!(
            fs::metadata(&detached_owned)
                .expect("detached owned metadata")
                .len(),
            committed,
            "the originally verified handle receives the safe truncation"
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[test]
    fn startup_never_truncates_an_unowned_deterministic_partial() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-unowned-recovery-partial-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let transfer_id = "unowned-recovery";
        let token = "token-unowned-recovery";
        reserve_receive_path(&destination, transfer_id, token).expect("reserve destination");
        let partial = resumable_partial_path(&destination, transfer_id)
            .expect("derive deterministic partial");
        let unowned_length = u64::from(TRANSFER_CHUNK_BYTES) + 57;
        fs::File::create(&partial)
            .expect("create unrelated deterministic file")
            .set_len(unowned_length)
            .expect("size unrelated deterministic file");
        {
            let connection = Connection::open(&database).expect("open fixture database");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, destination_reserved, reservation_token,
                        transfer_protocol, chunk_size, chunk_count, manifest_sha256, partial_path,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES (?1, 'peer-one', 'incoming', 'file', 'report.bin', ?2,
                             'application/octet-stream', ?3, ?4, 1, ?5, 2, ?6, 2, ?3, ?7,
                             ?6, 'transferring', ?8, ?8)",
                    rusqlite::params![
                        transfer_id,
                        u64::from(TRANSFER_CHUNK_BYTES) * 2,
                        "0".repeat(64),
                        destination.to_string_lossy(),
                        token,
                        TRANSFER_CHUNK_BYTES,
                        partial.to_string_lossy(),
                        "2026-08-24T00:00:00.000Z",
                    ],
                )
                .expect("insert recovery transfer");
            connection
                .execute(
                    "INSERT INTO transfer_chunks
                       (transfer_id, chunk_index, chunk_length, sha256)
                     VALUES (?1, 0, ?2, ?3)",
                    rusqlite::params![transfer_id, TRANSFER_CHUNK_BYTES, [1_u8; 32].as_slice()],
                )
                .expect("insert committed chunk");
        }

        let storage = Storage::open(&database).expect("reconcile unowned partial safely");
        let paused = storage
            .get_transfer(transfer_id)
            .expect("load paused transfer")
            .expect("paused transfer exists");
        assert_eq!(paused.status, TransferStatus::Paused);
        assert!(!receive_claimed(&storage, transfer_id));
        assert_eq!(paused.transferred_bytes, 0);
        let replacement = PathBuf::from(
            paused
                .partial_path
                .as_deref()
                .expect("replacement owned partial is persisted"),
        );
        assert_ne!(replacement, partial);
        assert_eq!(fs::metadata(&replacement).unwrap().len(), 0);
        assert_eq!(
            fs::metadata(&partial)
                .expect("unowned partial metadata")
                .len(),
            unowned_length
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn startup_truncates_v2_partial_tail_beyond_committed_offset() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-startup-long-partial-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let committed = u64::from(TRANSFER_CHUNK_BYTES);
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "startup-long",
            committed,
            Some(committed + 321),
        );

        let storage = Storage::open(&database).expect("reopen and truncate durable tail");
        let transfer = storage
            .get_transfer("startup-long")
            .expect("load reconciled transfer")
            .expect("transfer exists");
        assert_eq!(transfer.status, TransferStatus::Paused);
        assert_eq!(transfer.transferred_bytes, committed);
        assert_eq!(
            fs::metadata(&partial).expect("inspect partial").len(),
            committed
        );
        assert_eq!(
            storage.list_transfer_chunks("startup-long").unwrap().len(),
            1
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn startup_rolls_short_v2_partial_back_to_complete_chunk_boundary() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-startup-short-partial-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let chunk = u64::from(TRANSFER_CHUNK_BYTES);
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "startup-short",
            chunk * 2,
            Some(chunk + 321),
        );

        let storage = Storage::open(&database).expect("reopen and roll back short partial");
        let transfer = storage
            .get_transfer("startup-short")
            .expect("load reconciled transfer")
            .expect("transfer exists");
        assert_eq!(transfer.status, TransferStatus::Paused);
        assert_eq!(transfer.transferred_bytes, chunk);
        assert_eq!(
            fs::metadata(&partial).expect("inspect partial").len(),
            chunk
        );
        let chunks = storage
            .list_transfer_chunks("startup-short")
            .expect("load retained chunks");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].index, 0);
        assert!(
            reservation_is_owned(&destination, "startup-short", "token-startup-short")
                .expect("inspect preserved reservation")
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn startup_short_partial_never_invents_a_missing_committed_chunk_boundary() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-startup-missing-chunk-row-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let chunk = u64::from(TRANSFER_CHUNK_BYTES);
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "startup-missing-chunk",
            chunk * 2,
            Some(chunk + 321),
        );
        let connection = Connection::open(&database).expect("open startup fixture database");
        connection
            .execute(
                "DELETE FROM transfer_chunks WHERE transfer_id = 'startup-missing-chunk'
                 AND chunk_index = 0",
                [],
            )
            .expect("remove first committed chunk row");
        drop(connection);

        let storage = Storage::open(&database).expect("reopen and distrust missing chunk row");
        let transfer = storage
            .get_transfer("startup-missing-chunk")
            .expect("load reconciled transfer")
            .expect("transfer exists");
        assert_eq!(transfer.status, TransferStatus::Paused);
        assert_eq!(transfer.transferred_bytes, 0);
        assert_eq!(fs::metadata(&partial).expect("inspect partial").len(), 0);
        assert!(
            storage
                .list_transfer_chunks("startup-missing-chunk")
                .unwrap()
                .is_empty()
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn startup_missing_v2_partial_resets_only_unavailable_committed_data() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-startup-missing-partial-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "startup-missing",
            u64::from(TRANSFER_CHUNK_BYTES),
            None,
        );

        let storage = Storage::open(&database).expect("reopen and reconcile missing partial");
        let transfer = storage
            .get_transfer("startup-missing")
            .expect("load reconciled transfer")
            .expect("transfer exists");
        assert_eq!(transfer.status, TransferStatus::Paused);
        assert_eq!(transfer.transferred_bytes, 0);
        let replacement = PathBuf::from(
            transfer
                .partial_path
                .as_deref()
                .expect("replacement owned partial is persisted"),
        );
        assert_ne!(replacement, partial);
        assert_eq!(fs::metadata(&replacement).unwrap().len(), 0);
        assert!(
            storage
                .list_transfer_chunks("startup-missing")
                .unwrap()
                .is_empty()
        );
        assert!(
            reservation_is_owned(&destination, "startup-missing", "token-startup-missing",)
                .expect("inspect preserved reservation")
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn startup_keeps_v1_fail_and_owned_reservation_cleanup_behavior() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-startup-v1-cleanup-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("legacy.bin");
        reserve_receive_path(&destination, "startup-v1", "token-startup-v1")
            .expect("reserve legacy destination");
        let connection = Connection::open(&database).expect("open startup fixture database");
        connection
            .execute(
                "INSERT INTO transfers
                   (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                    sha256, local_path, destination_reserved, reservation_token, receive_claimed,
                    transferred_bytes, status, created_at, updated_at)
                 VALUES ('startup-v1', 'peer-one', 'incoming', 'file', 'legacy.bin', 3,
                         'application/octet-stream', ?1, ?2, 1, 'token-startup-v1', 1, 0,
                         'transferring', ?3, ?3)",
                rusqlite::params![
                    "0".repeat(64),
                    destination.to_string_lossy(),
                    "2026-08-24T00:00:00.000Z"
                ],
            )
            .expect("insert legacy startup fixture");
        drop(connection);

        let storage = Storage::open(&database).expect("reopen and fail legacy transfer");
        let transfer = storage
            .get_transfer("startup-v1")
            .expect("load reconciled transfer")
            .expect("transfer exists");
        assert_eq!(transfer.status, TransferStatus::Failed);
        assert!(!transfer.destination_reserved);
        assert_eq!(transfer.reservation_token, None);
        assert!(!receive_claimed(&storage, "startup-v1"));
        assert!(
            !reservation_is_owned(&destination, "startup-v1", "token-startup-v1")
                .expect("inspect cleaned reservation")
        );
        assert!(!destination.exists());

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn claimed_terminal_failure_cleans_only_owned_incoming_artifacts_after_cas() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-terminal-owned-cleanup-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let storage = Storage::open(&database).expect("open storage");
        let destination = fixture.join("report.bin");
        let committed = u64::from(TRANSFER_CHUNK_BYTES);
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "terminal-failure",
            committed,
            Some(committed),
        );
        fs::write(&destination, b"completed-or-user-data").expect("write protected destination");
        let unrelated_destination = fixture.join("unrelated-legacy.bin");
        reserve_receive_path(
            &unrelated_destination,
            "unrelated-legacy",
            "token-unrelated-legacy",
        )
        .expect("reserve unrelated legacy destination");
        {
            let connection = storage.connection().expect("insert unrelated transfer");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, destination_reserved, reservation_token,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES ('unrelated-legacy', 'peer-two', 'incoming', 'file', 'legacy.bin', 3,
                             'application/octet-stream', ?1, ?2, 1, 'token-unrelated-legacy', 0,
                             'transferring', ?3, ?3)",
                    rusqlite::params![
                        "0".repeat(64),
                        unrelated_destination.to_string_lossy(),
                        "2026-08-24T00:00:00.000Z",
                    ],
                )
                .expect("insert unrelated legacy transfer");
        }

        assert!(
            storage
                .try_fail_claimed_incoming_transfer(
                    "terminal-failure",
                    "peer-one",
                    "integrity failure",
                )
                .expect("persist terminal failure")
        );

        let failed = storage
            .get_transfer("terminal-failure")
            .expect("load failed transfer")
            .expect("failed transfer exists");
        assert_eq!(failed.status, TransferStatus::Failed);
        assert!(!failed.destination_reserved);
        assert_eq!(failed.reservation_token, None);
        assert_eq!(failed.partial_path, None);
        assert!(!receive_claimed(&storage, "terminal-failure"));
        assert!(!partial.exists());
        assert!(
            storage
                .list_transfer_chunks("terminal-failure")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            fs::read(&destination).expect("read protected destination"),
            b"completed-or-user-data"
        );
        assert!(
            reservation_is_owned(
                &unrelated_destination,
                "unrelated-legacy",
                "token-unrelated-legacy",
            )
            .expect("inspect unrelated reservation")
        );
        assert!(
            storage
                .get_transfer("unrelated-legacy")
                .expect("load unrelated transfer")
                .expect("unrelated transfer exists")
                .destination_reserved
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn terminal_cleanup_rolls_back_ownership_fields_when_chunk_deletion_aborts() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-terminal-cleanup-rollback-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        seed_resumable_incoming(
            &database,
            &destination,
            "terminal-cleanup-rollback",
            u64::from(TRANSFER_CHUNK_BYTES),
            Some(u64::from(TRANSFER_CHUNK_BYTES)),
        );
        let storage = Storage::open(&database).expect("open storage");
        assert!(
            storage
                .try_claim_incoming_transfer("terminal-cleanup-rollback", "peer-one")
                .expect("claim paused transfer")
        );
        {
            let connection = storage.connection().expect("install rollback trigger");
            connection
                .execute_batch(
                    "CREATE TRIGGER block_terminal_chunk_cleanup
                     BEFORE DELETE ON transfer_chunks
                     WHEN OLD.transfer_id = 'terminal-cleanup-rollback'
                     BEGIN
                       SELECT RAISE(ABORT, 'injected chunk cleanup crash');
                     END;",
                )
                .expect("install chunk cleanup abort");
        }

        storage
            .try_fail_claimed_incoming_transfer(
                "terminal-cleanup-rollback",
                "peer-one",
                "terminal failure",
            )
            .expect_err("injected delete failure must abort ownership clearing");

        let retained = storage
            .get_transfer("terminal-cleanup-rollback")
            .expect("load failed transfer")
            .expect("failed transfer exists");
        assert_eq!(retained.status, TransferStatus::Failed);
        assert!(retained.destination_reserved);
        assert!(retained.reservation_token.is_some());
        assert!(retained.partial_path.is_some());
        assert_eq!(
            storage
                .list_transfer_chunks("terminal-cleanup-rollback")
                .expect("load retained chunks")
                .len(),
            1
        );

        {
            let connection = storage.connection().expect("remove rollback trigger");
            connection
                .execute_batch("DROP TRIGGER block_terminal_chunk_cleanup;")
                .expect("remove chunk cleanup abort");
        }
        storage
            .cleanup_owned_transfer_artifacts("terminal-cleanup-rollback")
            .expect("retry terminal cleanup");
        let cleaned = storage
            .get_transfer("terminal-cleanup-rollback")
            .expect("load cleaned transfer")
            .expect("cleaned transfer exists");
        assert!(!cleaned.destination_reserved);
        assert!(cleaned.reservation_token.is_none());
        assert!(cleaned.partial_path.is_none());
        assert!(
            storage
                .list_transfer_chunks("terminal-cleanup-rollback")
                .expect("load cleaned chunks")
                .is_empty()
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }

    #[test]
    fn startup_retries_filesystem_cleanup_from_the_durable_terminal_snapshot() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-terminal-cleanup-retry-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "terminal-cleanup-retry",
            u64::from(TRANSFER_CHUNK_BYTES),
            Some(u64::from(TRANSFER_CHUNK_BYTES)),
        );
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("make transfer terminal");
            connection
                .execute(
                    "UPDATE transfers SET status = 'failed', receive_claimed = 0
                     WHERE transfer_id = 'terminal-cleanup-retry'",
                    [],
                )
                .expect("persist terminal state");
        }
        let record = OwnedArtifactRecord {
            transfer_id: "terminal-cleanup-retry".to_string(),
            destination: Some(destination.clone()),
            partial: Some(partial.clone()),
            reservation_token: Some("token-terminal-cleanup-retry".to_string()),
            destination_reserved: true,
            transfer_protocol: 2,
            status: "failed".to_string(),
        };
        storage
            .stage_terminal_cleanup(&record)
            .expect("atomically stage terminal filesystem cleanup");
        assert!(
            partial.exists(),
            "injected crash precedes filesystem cleanup"
        );
        let staged = storage
            .get_transfer("terminal-cleanup-retry")
            .unwrap()
            .expect("staged transfer exists");
        assert_eq!(staged.partial_path, None);
        assert!(!staged.destination_reserved);
        assert!(
            storage
                .list_transfer_chunks("terminal-cleanup-retry")
                .unwrap()
                .is_empty()
        );
        drop(storage);

        let storage = Storage::open(&database).expect("retry staged cleanup on startup");
        assert!(!partial.exists());
        assert!(
            !reservation_is_owned(
                &destination,
                "terminal-cleanup-retry",
                "token-terminal-cleanup-retry"
            )
            .unwrap()
        );
        assert!(
            storage
                .pending_terminal_cleanup("terminal-cleanup-retry")
                .unwrap()
                .is_none()
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[cfg(any(windows, target_os = "macos"))]
    #[test]
    fn startup_retires_a_completed_legacy_stage_and_clears_database_ownership() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-completed-legacy-stage-cleanup-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let transfer_id = "completed-legacy-stage";
        let token = "token-completed-legacy-stage";
        let payload = b"final hard link must remain intact";
        reserve_receive_path(&destination, transfer_id, token).expect("reserve destination");
        let staged =
            create_legacy_finalized_stage_fixture(&destination, transfer_id, token, payload)
                .expect("create legacy finalization fixture");
        {
            let connection = Connection::open(&database).expect("open fixture database");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, destination_reserved, reservation_token,
                        transfer_protocol, chunk_size, chunk_count, manifest_sha256,
                        transferred_bytes, status, created_at, updated_at)
                     VALUES (?1, 'peer-one', 'incoming', 'file', 'report.bin', ?2,
                             'application/octet-stream', ?3, ?4, 1, ?5,
                             2, ?6, 1, ?7, ?2, 'completed', ?8, ?8)",
                    rusqlite::params![
                        transfer_id,
                        payload.len(),
                        hex::encode(Sha256::digest(payload)),
                        destination.to_string_lossy(),
                        token,
                        TRANSFER_CHUNK_BYTES,
                        "0".repeat(64),
                        "2026-08-25T00:00:00.000Z",
                    ],
                )
                .expect("insert completed legacy transfer");
        }

        let storage = Storage::open(&database).expect("retry completed cleanup at startup");
        let completed = storage
            .get_transfer(transfer_id)
            .unwrap()
            .expect("completed transfer remains");
        assert!(!staged.exists());
        assert_eq!(fs::read(&destination).unwrap(), payload);
        assert!(!completed.destination_reserved);
        assert!(completed.reservation_token.is_none());

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[cfg(any(windows, target_os = "macos"))]
    #[test]
    fn legacy_stage_path_replacement_is_preserved_and_terminal_cleanup_stays_pending() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-terminal-legacy-stage-replacement-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let transfer_id = "terminal-legacy-stage-replacement";
        let token = "token-terminal-legacy-stage-replacement";
        let payload = b"final hard link remains owned data";
        reserve_receive_path(&destination, transfer_id, token).expect("reserve destination");
        let staged =
            create_legacy_finalized_stage_fixture(&destination, transfer_id, token, payload)
                .expect("create legacy finalization fixture");
        fs::remove_file(&staged).expect("detach originally owned stage link");
        fs::write(&staged, b"concurrent replacement data").expect("replace stage pathname");

        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert terminal transfer");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, destination_reserved, reservation_token,
                        transfer_protocol, transferred_bytes, status, created_at, updated_at)
                     VALUES (?1, 'peer-one', 'incoming', 'file', 'report.bin', ?2,
                             'application/octet-stream', ?3, ?4, 1, ?5,
                             2, ?2, 'failed', ?6, ?6)",
                    rusqlite::params![
                        transfer_id,
                        payload.len(),
                        hex::encode(Sha256::digest(payload)),
                        destination.to_string_lossy(),
                        token,
                        "2026-08-25T00:00:00.000Z",
                    ],
                )
                .expect("insert terminal legacy transfer");
        }
        let record = OwnedArtifactRecord {
            transfer_id: transfer_id.to_string(),
            destination: Some(destination.clone()),
            partial: None,
            reservation_token: Some(token.to_string()),
            destination_reserved: true,
            transfer_protocol: 2,
            status: "failed".to_string(),
        };
        storage
            .stage_terminal_cleanup(&record)
            .expect("durably stage terminal cleanup");
        drop(storage);

        let storage = Storage::open(&database).expect("startup keeps unresolved cleanup");
        assert_eq!(fs::read(&staged).unwrap(), b"concurrent replacement data");
        assert_eq!(fs::read(&destination).unwrap(), payload);
        assert!(
            storage
                .pending_terminal_cleanup(transfer_id)
                .unwrap()
                .is_some()
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[cfg(any(windows, target_os = "macos"))]
    #[test]
    fn unavailable_legacy_stage_media_defers_then_finishes_terminal_cleanup() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-terminal-legacy-stage-media-{}",
            uuid::Uuid::now_v7()
        ));
        let media = fixture.join("selected-media");
        let detached_media = fixture.join("detached-media");
        fs::create_dir_all(&media).expect("create selected media fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = media.join("report.bin");
        let transfer_id = "terminal-legacy-stage-media";
        let token = "token-terminal-legacy-stage-media";
        let payload = b"terminal legacy hard-link payload";
        reserve_receive_path(&destination, transfer_id, token).expect("reserve destination");
        let staged =
            create_legacy_finalized_stage_fixture(&destination, transfer_id, token, payload)
                .expect("create legacy finalization fixture");
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("insert terminal transfer");
            connection
                .execute(
                    "INSERT INTO transfers
                       (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type,
                        sha256, local_path, destination_reserved, reservation_token,
                        transfer_protocol, transferred_bytes, status, created_at, updated_at)
                     VALUES (?1, 'peer-one', 'incoming', 'file', 'report.bin', ?2,
                             'application/octet-stream', ?3, ?4, 1, ?5,
                             2, ?2, 'failed', ?6, ?6)",
                    rusqlite::params![
                        transfer_id,
                        payload.len(),
                        hex::encode(Sha256::digest(payload)),
                        destination.to_string_lossy(),
                        token,
                        "2026-08-25T00:00:00.000Z",
                    ],
                )
                .expect("insert terminal legacy transfer");
        }
        let record = OwnedArtifactRecord {
            transfer_id: transfer_id.to_string(),
            destination: Some(destination.clone()),
            partial: None,
            reservation_token: Some(token.to_string()),
            destination_reserved: true,
            transfer_protocol: 2,
            status: "failed".to_string(),
        };
        storage
            .stage_terminal_cleanup(&record)
            .expect("durably stage terminal cleanup");
        drop(storage);
        fs::rename(&media, &detached_media).expect("simulate unavailable media");

        let storage = Storage::open(&database).expect("startup defers unavailable media");
        assert!(
            storage
                .pending_terminal_cleanup(transfer_id)
                .unwrap()
                .is_some()
        );
        drop(storage);

        fs::rename(&detached_media, &media).expect("restore media");
        let storage = Storage::open(&database).expect("retry cleanup after media returns");
        assert!(
            storage
                .pending_terminal_cleanup(transfer_id)
                .unwrap()
                .is_none()
        );
        assert!(!staged.exists());
        assert_eq!(fs::read(&destination).unwrap(), payload);

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[test]
    fn startup_defers_terminal_tombstone_cleanup_while_media_is_unavailable() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-terminal-missing-media-{}",
            uuid::Uuid::now_v7()
        ));
        let media = fixture.join("selected-media");
        let detached_media = fixture.join("detached-media");
        fs::create_dir_all(&media).expect("create selected media fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = media.join("report.bin");
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "terminal-missing-media",
            u64::from(TRANSFER_CHUNK_BYTES),
            Some(u64::from(TRANSFER_CHUNK_BYTES)),
        );
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("make transfer terminal");
            connection
                .execute(
                    "UPDATE transfers SET status = 'failed', receive_claimed = 0
                     WHERE transfer_id = 'terminal-missing-media'",
                    [],
                )
                .expect("persist terminal state");
        }
        let record = OwnedArtifactRecord {
            transfer_id: "terminal-missing-media".to_string(),
            destination: Some(destination.clone()),
            partial: Some(partial.clone()),
            reservation_token: Some("token-terminal-missing-media".to_string()),
            destination_reserved: true,
            transfer_protocol: 2,
            status: "failed".to_string(),
        };
        storage
            .stage_terminal_cleanup(&record)
            .expect("stage terminal cleanup");
        drop(storage);
        fs::rename(&media, &detached_media).expect("simulate removed destination media");

        let storage = Storage::open(&database)
            .expect("unavailable owned media must not prevent application startup");
        assert!(
            storage
                .pending_terminal_cleanup("terminal-missing-media")
                .unwrap()
                .is_some()
        );
        drop(storage);

        fs::rename(&detached_media, &media).expect("restore destination media");
        let storage = Storage::open(&database).expect("retry cleanup after media returns");
        assert!(
            storage
                .pending_terminal_cleanup("terminal-missing-media")
                .unwrap()
                .is_none()
        );
        assert!(!partial.exists());
        assert!(
            !reservation_is_owned(
                &destination,
                "terminal-missing-media",
                "token-terminal-missing-media"
            )
            .unwrap()
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[test]
    fn terminal_cleanup_identity_mismatch_preserves_data_and_tombstone() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-terminal-identity-mismatch-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let destination = fixture.join("report.bin");
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "terminal-identity-mismatch",
            u64::from(TRANSFER_CHUNK_BYTES),
            Some(u64::from(TRANSFER_CHUNK_BYTES)),
        );
        let storage = Storage::open(&database).expect("open storage");
        {
            let connection = storage.connection().expect("make transfer terminal");
            connection
                .execute(
                    "UPDATE transfers SET status = 'failed', receive_claimed = 0
                     WHERE transfer_id = 'terminal-identity-mismatch'",
                    [],
                )
                .expect("persist terminal state");
        }
        let record = OwnedArtifactRecord {
            transfer_id: "terminal-identity-mismatch".to_string(),
            destination: Some(destination),
            partial: Some(partial.clone()),
            reservation_token: Some("token-terminal-identity-mismatch".to_string()),
            destination_reserved: true,
            transfer_protocol: 2,
            status: "failed".to_string(),
        };
        storage.stage_terminal_cleanup(&record).unwrap();
        fs::remove_file(&partial).expect("remove originally owned inode");
        fs::write(&partial, b"replacement user data").expect("replace partial path");
        drop(storage);

        let storage = Storage::open(&database).expect("startup keeps unresolved tombstone");
        assert_eq!(fs::read(&partial).unwrap(), b"replacement user data");
        assert!(
            storage
                .pending_terminal_cleanup("terminal-identity-mismatch")
                .unwrap()
                .is_some()
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove fixture");
    }

    #[test]
    fn paused_incoming_cancel_cleans_owned_partial_reservation_and_chunks() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-cancel-owned-cleanup-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create storage fixture");
        let database = fixture.join("localnet.sqlite3");
        initialize_database(&database);
        let storage = Storage::open(&database).expect("open storage");
        let destination = fixture.join("report.bin");
        let committed = u64::from(TRANSFER_CHUNK_BYTES);
        let partial = seed_resumable_incoming(
            &database,
            &destination,
            "cancel-cleanup",
            committed,
            Some(committed),
        );
        assert!(
            storage
                .try_pause_claimed_incoming_transfer(
                    "cancel-cleanup",
                    "peer-one",
                    "network disconnected",
                )
                .expect("pause claimed transfer")
        );

        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    "cancel-cleanup",
                    "peer-one",
                    2,
                    "cancelled"
                )
                .expect("cancel paused transfer")
        );

        let cancelled = storage
            .get_transfer("cancel-cleanup")
            .expect("load cancelled transfer")
            .expect("cancelled transfer exists");
        assert_eq!(cancelled.status, TransferStatus::Cancelled);
        assert!(!cancelled.destination_reserved);
        assert_eq!(cancelled.partial_path, None);
        assert!(!partial.exists());
        assert!(
            storage
                .list_transfer_chunks("cancel-cleanup")
                .unwrap()
                .is_empty()
        );
        assert!(
            !reservation_is_owned(&destination, "cancel-cleanup", "token-cancel-cleanup")
                .expect("inspect cleaned reservation")
        );

        drop(storage);
        fs::remove_dir_all(fixture).expect("remove storage fixture");
    }
}
