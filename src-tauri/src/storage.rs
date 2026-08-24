use std::{
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard},
};

use rusqlite::{Connection, OptionalExtension, params};

use crate::{
    domain::{
        BootstrapSnapshot, ChatMessage, Direction, Friend, FriendRequest, FriendRequestStatus,
        LocalProfile, MessageKind, MessageStatus, PeerSummary, Platform, PresenceSnapshot,
        TransferKind, TransferPreferences, TransferRecord, TransferStatus,
    },
    error::AppError,
    receive_paths::remove_owned_reservation,
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
        let source_modified_ns = validate_transfer_for_storage(transfer)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        Self::upsert_transfer_in_transaction(&transaction, transfer, source_modified_ns)?;
        transaction.commit()?;
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
        Self::upsert_transfer_in_transaction(&transaction, transfer, source_modified_ns)?;
        replace_chunks_in_transaction(&transaction, &transfer.transfer_id, chunks)?;
        transaction.commit()?;
        Ok(())
    }

    fn upsert_transfer_in_transaction(
        transaction: &rusqlite::Transaction<'_>,
        transfer: &TransferRecord,
        source_modified_ns: Option<i64>,
    ) -> Result<(), AppError> {
        transaction.execute(
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
               updated_at = excluded.updated_at",
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
        Ok(())
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

    #[allow(dead_code)] // Consumed by resumable receive framing in the next protocol task.
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

    #[allow(dead_code)] // Consumed by resumable receive framing in the next protocol task.
    pub fn commit_received_chunk(
        &self,
        transfer_id: &str,
        chunk: &TransferChunk,
        committed_bytes: u64,
    ) -> Result<bool, AppError> {
        let committed_bytes = i64::try_from(committed_bytes)
            .map_err(|_| AppError::Storage("已接收字节数超出存储范围".to_string()))?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
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
            "UPDATE transfers SET transferred_bytes = ?2 WHERE transfer_id = ?1",
            params![transfer_id, committed_bytes],
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
            "UPDATE transfers SET receive_claimed = 1
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND status = 'transferring' AND receive_claimed = 0",
            params![transfer_id, peer_id],
        )?;
        Ok(changed == 1)
    }

    pub fn release_incoming_transfer_claim(&self, transfer_id: &str) -> Result<(), AppError> {
        let connection = self.connection()?;
        connection.execute(
            "UPDATE transfers SET receive_claimed = 0 WHERE transfer_id = ?1",
            [transfer_id],
        )?;
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
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET local_path = ?3, destination_reserved = 1, reservation_token = ?4,
                 transferred_bytes = 0, status = 'transferring', error = NULL, updated_at = ?5
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND status = 'awaitingAcceptance' AND receive_claimed = 0",
            params![
                transfer.transfer_id,
                transfer.peer_id,
                transfer.local_path,
                transfer.reservation_token,
                transfer.updated_at,
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn try_cancel_unclaimed_incoming_transfer(
        &self,
        transfer_id: &str,
        peer_id: &str,
        error: &str,
    ) -> Result<bool, AppError> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE transfers
             SET status = 'cancelled', error = ?3,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE transfer_id = ?1 AND peer_id = ?2 AND direction = 'incoming'
               AND status IN ('awaitingAcceptance', 'transferring') AND receive_claimed = 0",
            params![transfer_id, peer_id, error],
        )?;
        Ok(changed == 1)
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
                   FROM transfer_chunks_legacy
                   WHERE typeof(sha256) = 'blob' AND length(sha256) = 32;
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
        let connection = self.connection()?;
        let reservations = {
            let mut statement = connection.prepare(
                "SELECT transfer_id, local_path, reservation_token FROM transfers
                 WHERE destination_reserved = 1 AND local_path IS NOT NULL",
            )?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        for (transfer_id, local_path, reservation_token) in reservations {
            let cleanup = match reservation_token.as_deref() {
                Some(token) => {
                    remove_owned_reservation(Path::new(&local_path), &transfer_id, token)
                }
                None => Ok(false),
            };
            match cleanup {
                Ok(_) => {
                    connection.execute(
                        "UPDATE transfers
                         SET destination_reserved = 0, reservation_token = NULL
                         WHERE transfer_id = ?1",
                        [&transfer_id],
                    )?;
                }
                Err(error) => {
                    tracing::warn!(%transfer_id, %error, "failed to clean stale receive reservation");
                }
            }
        }
        connection.execute("UPDATE peers SET online = 0", [])?;
        connection.execute(
            "UPDATE messages SET status = 'failed', error = '应用重新启动，请重试发送'
             WHERE status = 'sending'",
            [],
        )?;
        connection.execute(
            "UPDATE transfers SET status = 'failed', error = '应用重新启动，请重试传输',
                                  receive_claimed = 0,
                                  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE status = 'transferring'",
            [],
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
    use std::fs;

    use rusqlite::Connection;

    use super::Storage;
    use crate::{
        domain::{
            Direction, PeerSummary, Platform, TransferKind, TransferPreferences, TransferRecord,
            TransferStatus,
        },
        transfer_manifest::{TransferChunk, manifest_root},
        transfer_policy::TRANSFER_CHUNK_BYTES,
    };

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
                         manifest_sha256 = ?2
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
                .commit_received_chunk("transfer-one", &chunks[0], u64::from(TRANSFER_CHUNK_BYTES))
                .expect("commit first chunk")
        );
        assert!(
            !storage
                .commit_received_chunk(
                    "transfer-one",
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
                .try_cancel_unclaimed_incoming_transfer("transfer-one", "peer-one", "cancelled",)
                .expect("active stream must win cancellation race")
        );
        storage
            .release_incoming_transfer_claim("transfer-one")
            .expect("release second stream claim");
        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer("transfer-one", "peer-one", "cancelled",)
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
}
