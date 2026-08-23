use std::{
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard},
};

use rusqlite::{Connection, OptionalExtension, params};

use crate::{
    domain::{
        BootstrapSnapshot, ChatMessage, Direction, Friend, FriendRequest, FriendRequestStatus,
        LocalProfile, MessageKind, MessageStatus, PeerSummary, Platform, TransferKind,
        TransferRecord, TransferStatus,
    },
    error::AppError,
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

    pub fn upsert_peer(&self, peer: &PeerSummary) -> Result<(), AppError> {
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO peers (peer_id, nickname, platform, online, protocol_version, last_seen)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(peer_id) DO UPDATE SET
               nickname = excluded.nickname,
               platform = excluded.platform,
               online = excluded.online,
               protocol_version = excluded.protocol_version,
               last_seen = excluded.last_seen",
            params![
                peer.peer_id,
                peer.nickname,
                peer.platform.as_str(),
                peer.online,
                peer.protocol_version,
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
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO transfers
               (transfer_id, peer_id, direction, kind, file_name, file_size, mime_type, sha256,
                local_path, transferred_bytes, status, error, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             ON CONFLICT(transfer_id) DO UPDATE SET
               local_path = excluded.local_path,
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
                transfer.transferred_bytes,
                transfer.status.as_str(),
                transfer.error,
                transfer.created_at,
                transfer.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn get_transfer(&self, transfer_id: &str) -> Result<Option<TransferRecord>, AppError> {
        Ok(self
            .list_transfers()?
            .into_iter()
            .find(|transfer| transfer.transfer_id == transfer_id))
    }

    pub fn snapshot(
        &self,
        local_profile: Option<LocalProfile>,
    ) -> Result<BootstrapSnapshot, AppError> {
        Ok(BootstrapSnapshot {
            local_profile,
            peers: self.list_peers()?,
            friend_requests: self.list_friend_requests()?,
            friends: self.list_friends()?,
            messages: self.list_messages()?,
            transfers: self.list_transfers()?,
        })
    }

    fn list_peers(&self) -> Result<Vec<PeerSummary>, AppError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT peer_id, nickname, platform, online, protocol_version, last_seen
             FROM peers ORDER BY online DESC, nickname COLLATE NOCASE",
        )?;
        let rows = statement.query_map([], |row| {
            let platform: String = row.get(2)?;
            Ok((
                row.get(0)?,
                row.get(1)?,
                platform,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })?;
        rows.map(|row| {
            let (peer_id, nickname, platform, online, protocol_version, last_seen) = row?;
            Ok(PeerSummary {
                peer_id,
                nickname,
                platform: Platform::from_str(&platform)?,
                online,
                protocol_version,
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
                    sha256, local_path, transferred_bytes, status, error, created_at, updated_at
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
                row.get::<_, String>(10)?,
                row.get(11)?,
                row.get(12)?,
                row.get(13)?,
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
               transferred_bytes INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL,
               error TEXT,
               created_at TEXT NOT NULL,
               updated_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_transfers_peer_time ON transfers(peer_id, created_at);",
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn reset_ephemeral_state(&self) -> Result<(), AppError> {
        let connection = self.connection()?;
        connection.execute("UPDATE peers SET online = 0", [])?;
        connection.execute(
            "UPDATE messages SET status = 'failed', error = '应用重新启动，请重试发送'
             WHERE status = 'sending'",
            [],
        )?;
        connection.execute(
            "UPDATE transfers SET status = 'failed', error = '应用重新启动，请重试传输',
                                  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE status = 'transferring'",
            [],
        )?;
        Ok(())
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, AppError> {
        self.connection
            .lock()
            .map_err(|_| AppError::Storage("本地数据锁异常，请重新启动 Localnet".to_string()))
    }
}
