use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    time::{Duration, Instant},
};

use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder, mdns,
    multiaddr::Protocol,
    noise, request_response,
    swarm::{
        SwarmEvent,
        dial_opts::{DialOpts, PeerCondition},
    },
    tcp, yamux,
};
use serde::Serialize;
use tauri::{AppHandle, Emitter};
use tokio::sync::{mpsc, watch};

use super::{
    behaviour::{LocalnetBehaviour, LocalnetBehaviourEvent},
    discovery::{DiscoveryEvent, DiscoveryService},
    transfer,
};
use crate::{
    domain::{
        ChatMessage, Direction, Friend, FriendRequest, FriendRequestStatus, LocalProfile,
        MAX_AUTO_IMAGE_BYTES, MAX_FILE_BYTES, MessageKind, MessageStatus, PROTOCOL_VERSION,
        PeerSummary, Platform, TransferKind, TransferRecord, TransferStatus, now_rfc3339,
        validate_nickname, validate_text,
    },
    error::AppError,
    identity::LocalIdentity,
    protocol::{ControlRequest, ControlResponse, FILE_PROTOCOL, HelloPayload, TransferOffer},
    storage::Storage,
};

const EVENT_NAME: &str = "localnet://event";
const FRIEND_REQUEST_LIMIT: usize = 5;
const FRIEND_REQUEST_WINDOW: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub enum NetworkCommand {
    SetProfile(LocalProfile),
    SendFriendRequest(FriendRequest),
    ResolveFriendRequest {
        peer_id: String,
        request_id: String,
        accepted: bool,
    },
    SendText(ChatMessage),
    OfferTransfer(TransferRecord),
    ResolveTransfer {
        peer_id: String,
        transfer_id: String,
        accepted: bool,
    },
    CancelTransfer {
        peer_id: String,
        transfer_id: String,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum NetworkEvent {
    PeerDiscovered {
        peer: PeerSummary,
    },
    PeerOffline {
        peer_id: String,
        last_seen: String,
    },
    FriendRequestReceived {
        request: FriendRequest,
    },
    FriendRequestResolved {
        request: FriendRequest,
        friend: Option<Friend>,
    },
    MessageReceived {
        message: ChatMessage,
    },
    MessageStatusChanged {
        message_id: String,
        status: MessageStatus,
        error: Option<String>,
    },
    TransferUpdated {
        transfer: TransferRecord,
    },
    NetworkError {
        code: String,
        message: String,
    },
}

#[derive(Clone)]
pub struct NetworkHandle {
    sender: mpsc::Sender<NetworkCommand>,
}

impl NetworkHandle {
    pub fn try_send(&self, command: NetworkCommand) -> Result<(), AppError> {
        self.sender
            .try_send(command)
            .map_err(|error| AppError::Network(format!("网络服务暂时不可用，请稍后重试：{error}")))
    }
}

pub fn spawn_network(
    identity: LocalIdentity,
    profile: LocalProfile,
    storage: Storage,
    app_handle: AppHandle,
    app_data_dir: PathBuf,
) -> NetworkHandle {
    let (sender, receiver) = mpsc::channel(128);
    tauri::async_runtime::spawn(async move {
        if let Err(error) = NetworkRuntime::run(
            identity,
            profile,
            storage,
            app_handle.clone(),
            app_data_dir,
            receiver,
        )
        .await
        {
            emit_event(
                &app_handle,
                &NetworkEvent::NetworkError {
                    code: error.code().to_string(),
                    message: error.to_string(),
                },
            );
            tracing::error!(error = %error, "Weline Chat network runtime stopped");
        }
    });
    NetworkHandle { sender }
}

#[derive(Debug)]
enum PendingAction {
    Hello,
    FriendRequest { request_id: String },
    FriendDecision,
    Text { message_id: String },
    TransferOffer { transfer_id: String },
    TransferDecision,
    TransferCancel,
}

struct NetworkRuntime {
    local_profile: LocalProfile,
    storage: Storage,
    app_handle: AppHandle,
    app_data_dir: PathBuf,
    swarm: Swarm<LocalnetBehaviour>,
    stream_control: libp2p_stream::Control,
    receiver: mpsc::Receiver<NetworkCommand>,
    discovery_receiver: mpsc::Receiver<DiscoveryEvent>,
    listen_port_sender: watch::Sender<Option<u16>>,
    pending: HashMap<request_response::OutboundRequestId, PendingAction>,
    mdns_addresses: HashMap<PeerId, HashSet<Multiaddr>>,
    beacon_addresses: HashMap<PeerId, HashMap<Multiaddr, Instant>>,
    active_connections: HashMap<PeerId, usize>,
    friend_request_times: HashMap<PeerId, VecDeque<Instant>>,
    mdns_enabled: bool,
}

impl NetworkRuntime {
    async fn run(
        identity: LocalIdentity,
        profile: LocalProfile,
        storage: Storage,
        app_handle: AppHandle,
        app_data_dir: PathBuf,
        receiver: mpsc::Receiver<NetworkCommand>,
    ) -> Result<(), AppError> {
        let keypair = identity.keypair();
        let peer_id = identity.peer_id();
        let behaviour = LocalnetBehaviour::new(peer_id, keypair.public())?;
        let mut swarm = SwarmBuilder::with_existing_identity(keypair.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default().nodelay(true),
                noise::Config::new,
                yamux::Config::default,
            )
            .map_err(|error| AppError::Network(format!("无法创建加密局域网连接：{error}")))?
            .with_behaviour(|_| behaviour)
            .map_err(|error| AppError::Network(format!("无法初始化局域网协议：{error}")))?
            .with_swarm_config(|config| {
                config.with_idle_connection_timeout(Duration::from_secs(120))
            })
            .build();
        let mut stream_control = swarm.behaviour().stream.new_control();
        let incoming_streams = stream_control
            .accept(StreamProtocol::new(FILE_PROTOCOL))
            .map_err(|error| AppError::Network(format!("无法注册文件接收协议：{error}")))?;
        transfer::spawn_incoming_transfers(incoming_streams, storage.clone(), app_handle.clone());
        swarm
            .listen_on("/ip4/0.0.0.0/tcp/0".parse().expect("static listen address"))
            .map_err(|error| AppError::Network(format!("无法监听局域网端口：{error}")))?;
        let (listen_port_sender, listen_port_receiver) = watch::channel(None);
        let discovery_receiver = DiscoveryService::spawn(peer_id, listen_port_receiver);
        let mdns_enabled =
            !(cfg!(debug_assertions) && std::env::var_os("LOCALNET_DISABLE_MDNS").is_some());

        tracing::info!(peer_id = %peer_id, mdns_enabled, "Weline Chat network runtime started");
        let mut runtime = Self {
            local_profile: profile,
            storage,
            app_handle,
            app_data_dir,
            swarm,
            stream_control,
            receiver,
            discovery_receiver,
            listen_port_sender,
            pending: HashMap::new(),
            mdns_addresses: HashMap::new(),
            beacon_addresses: HashMap::new(),
            active_connections: HashMap::new(),
            friend_request_times: HashMap::new(),
            mdns_enabled,
        };
        runtime.event_loop().await
    }

    async fn event_loop(&mut self) -> Result<(), AppError> {
        let mut discovery_cleanup = tokio::time::interval(Duration::from_secs(1));
        discovery_cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                command = self.receiver.recv() => {
                    match command {
                        None => return Ok(()),
                        Some(command) => {
                            if let Err(error) = self.handle_command(command.clone()) {
                                self.handle_command_failure(&command, &error)?;
                                self.emit(NetworkEvent::NetworkError {
                                    code: error.code().to_string(),
                                    message: error.to_string(),
                                });
                            }
                        },
                    }
                }
                event = self.swarm.select_next_some() => self.handle_swarm_event(event)?,
                discovery = self.discovery_receiver.recv() => {
                    if let Some(discovery) = discovery {
                        self.handle_discovery_event(discovery)?;
                    }
                }
                _ = discovery_cleanup.tick() => self.expire_beacon_addresses()?,
            }
        }
    }

    fn handle_command(&mut self, command: NetworkCommand) -> Result<(), AppError> {
        match command {
            NetworkCommand::SetProfile(profile) => {
                self.local_profile = profile;
            }
            NetworkCommand::SendFriendRequest(request) => {
                let peer_id = parse_peer_id(&request.peer_id)?;
                self.ensure_connected(&peer_id)?;
                let request_id = request.request_id.clone();
                let outbound_id = self.swarm.behaviour_mut().control.send_request(
                    &peer_id,
                    ControlRequest::FriendRequest {
                        request_id: request.request_id,
                        nickname: self.local_profile.nickname.clone(),
                    },
                );
                self.pending
                    .insert(outbound_id, PendingAction::FriendRequest { request_id });
            }
            NetworkCommand::ResolveFriendRequest {
                peer_id,
                request_id,
                accepted,
            } => {
                let peer_id = parse_peer_id(&peer_id)?;
                self.ensure_connected(&peer_id)?;
                let outbound_id = self.swarm.behaviour_mut().control.send_request(
                    &peer_id,
                    ControlRequest::FriendDecision {
                        request_id,
                        accepted,
                        nickname: self.local_profile.nickname.clone(),
                    },
                );
                self.pending
                    .insert(outbound_id, PendingAction::FriendDecision);
            }
            NetworkCommand::SendText(message) => {
                let peer_id = parse_peer_id(&message.peer_id)?;
                self.ensure_connected(&peer_id)?;
                let message_id = message.message_id.clone();
                let outbound_id = self.swarm.behaviour_mut().control.send_request(
                    &peer_id,
                    ControlRequest::TextMessage {
                        message_id: message.message_id,
                        sent_at: message.created_at,
                        body: message.body.unwrap_or_default(),
                    },
                );
                self.pending
                    .insert(outbound_id, PendingAction::Text { message_id });
            }
            NetworkCommand::OfferTransfer(transfer) => {
                let peer_id = parse_peer_id(&transfer.peer_id)?;
                self.ensure_connected(&peer_id)?;
                let transfer_id = transfer.transfer_id.clone();
                let outbound_id = self.swarm.behaviour_mut().control.send_request(
                    &peer_id,
                    ControlRequest::TransferOffer {
                        offer: TransferOffer {
                            transfer_id: transfer.transfer_id,
                            kind: transfer.kind,
                            file_name: transfer.file_name,
                            file_size: transfer.file_size,
                            mime_type: transfer.mime_type,
                            sha256: transfer.sha256,
                        },
                    },
                );
                self.pending
                    .insert(outbound_id, PendingAction::TransferOffer { transfer_id });
            }
            NetworkCommand::ResolveTransfer {
                peer_id,
                transfer_id,
                accepted,
            } => {
                let peer_id = parse_peer_id(&peer_id)?;
                self.ensure_connected(&peer_id)?;
                let outbound_id = self.swarm.behaviour_mut().control.send_request(
                    &peer_id,
                    ControlRequest::TransferDecision {
                        transfer_id,
                        accepted,
                    },
                );
                self.pending
                    .insert(outbound_id, PendingAction::TransferDecision);
            }
            NetworkCommand::CancelTransfer {
                peer_id,
                transfer_id,
            } => {
                let peer_id = parse_peer_id(&peer_id)?;
                self.ensure_connected(&peer_id)?;
                let outbound_id = self
                    .swarm
                    .behaviour_mut()
                    .control
                    .send_request(&peer_id, ControlRequest::TransferCancel { transfer_id });
                self.pending
                    .insert(outbound_id, PendingAction::TransferCancel);
            }
        }
        Ok(())
    }

    fn handle_command_failure(
        &self,
        command: &NetworkCommand,
        error: &AppError,
    ) -> Result<(), AppError> {
        match command {
            NetworkCommand::SendText(message) => self.set_message_status(
                &message.message_id,
                MessageStatus::Failed,
                Some(error.to_string()),
            )?,
            NetworkCommand::OfferTransfer(transfer) => {
                self.set_transfer_failed(&transfer.transfer_id, error.to_string())?;
            }
            NetworkCommand::SetProfile(_)
            | NetworkCommand::SendFriendRequest(_)
            | NetworkCommand::ResolveFriendRequest { .. }
            | NetworkCommand::ResolveTransfer { .. }
            | NetworkCommand::CancelTransfer { .. } => {}
        }
        Ok(())
    }

    fn handle_swarm_event(
        &mut self,
        event: SwarmEvent<LocalnetBehaviourEvent>,
    ) -> Result<(), AppError> {
        match event {
            SwarmEvent::Behaviour(LocalnetBehaviourEvent::Mdns(mdns::Event::Discovered(peers))) => {
                if !self.mdns_enabled {
                    return Ok(());
                }
                for (peer_id, address) in peers {
                    if peer_id == *self.swarm.local_peer_id() {
                        continue;
                    }
                    self.mdns_addresses
                        .entry(peer_id)
                        .or_default()
                        .insert(address.clone());
                    self.dial_discovered_peer(peer_id, address, "mdns");
                }
            }
            SwarmEvent::Behaviour(LocalnetBehaviourEvent::Mdns(mdns::Event::Expired(peers))) => {
                if !self.mdns_enabled {
                    return Ok(());
                }
                for (peer_id, address) in peers {
                    if let Some(addresses) = self.mdns_addresses.get_mut(&peer_id) {
                        addresses.remove(&address);
                        if addresses.is_empty() {
                            self.mdns_addresses.remove(&peer_id);
                        }
                    }
                    self.mark_offline_if_unreachable(peer_id)?;
                }
            }
            SwarmEvent::Behaviour(LocalnetBehaviourEvent::Control(event)) => {
                self.handle_control_event(event)?;
            }
            SwarmEvent::Behaviour(LocalnetBehaviourEvent::Identify(event)) => {
                tracing::trace!(?event, "identify event");
            }
            SwarmEvent::Behaviour(LocalnetBehaviourEvent::Stream(())) => {}
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                *self.active_connections.entry(peer_id).or_default() += 1;
                let outbound_id = self.swarm.behaviour_mut().control.send_request(
                    &peer_id,
                    ControlRequest::Hello {
                        version: PROTOCOL_VERSION,
                        nickname: self.local_profile.nickname.clone(),
                        platform: self.local_profile.platform,
                    },
                );
                self.pending.insert(outbound_id, PendingAction::Hello);
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                if let Some(count) = self.active_connections.get_mut(&peer_id) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.active_connections.remove(&peer_id);
                    }
                }
                self.mark_offline_if_unreachable(peer_id)?;
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!(%address, "Weline Chat listening");
                if let Some(port) = address.iter().find_map(|protocol| match protocol {
                    Protocol::Tcp(port) => Some(port),
                    _ => None,
                }) {
                    self.listen_port_sender.send_replace(Some(port));
                }
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                tracing::debug!(?peer_id, %error, "outgoing connection error");
            }
            SwarmEvent::IncomingConnectionError { error, .. } => {
                tracing::debug!(%error, "incoming connection error");
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_discovery_event(&mut self, event: DiscoveryEvent) -> Result<(), AppError> {
        match event {
            DiscoveryEvent::PeerHint {
                peer_id,
                address,
                expires_at,
            } => {
                if peer_id == *self.swarm.local_peer_id() {
                    return Ok(());
                }
                let is_new = self
                    .beacon_addresses
                    .entry(peer_id)
                    .or_default()
                    .insert(address.clone(), expires_at)
                    .is_none();
                if is_new {
                    tracing::debug!(peer_id = %peer_id, %address, "LAN beacon peer hint received");
                }
                self.dial_discovered_peer(peer_id, address, "lan-beacon");
            }
        }
        Ok(())
    }

    fn dial_discovered_peer(&mut self, peer_id: PeerId, address: Multiaddr, source: &'static str) {
        self.swarm.add_peer_address(peer_id, address.clone());
        let options = DialOpts::peer_id(peer_id)
            .condition(PeerCondition::DisconnectedAndNotDialing)
            .addresses(vec![address])
            .build();
        if let Err(error) = self.swarm.dial(options) {
            tracing::trace!(peer_id = %peer_id, source, error = %error, "peer dial deferred");
        }
    }

    fn expire_beacon_addresses(&mut self) -> Result<(), AppError> {
        let now = Instant::now();
        let mut expired_peers = Vec::new();
        self.beacon_addresses.retain(|peer_id, addresses| {
            addresses.retain(|_, expires_at| *expires_at > now);
            if addresses.is_empty() {
                expired_peers.push(*peer_id);
                false
            } else {
                true
            }
        });
        for peer_id in expired_peers {
            self.mark_offline_if_unreachable(peer_id)?;
        }
        Ok(())
    }

    fn handle_control_event(
        &mut self,
        event: request_response::Event<ControlRequest, ControlResponse>,
    ) -> Result<(), AppError> {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    let response = self.handle_inbound_request(peer, request);
                    let response = match response {
                        Ok(response) => response,
                        Err(error) => ControlResponse::Rejected {
                            code: error.code().to_string(),
                            message: error.to_string(),
                        },
                    };
                    self.swarm
                        .behaviour_mut()
                        .control
                        .send_response(channel, response)
                        .map_err(|_| {
                            AppError::Network("无法回复对端请求，连接可能已经关闭".to_string())
                        })?;
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => self.handle_outbound_response(peer, request_id, response)?,
            },
            request_response::Event::OutboundFailure {
                request_id, error, ..
            } => self.handle_outbound_failure(request_id, error)?,
            request_response::Event::InboundFailure { peer, error, .. } => {
                tracing::warn!(peer_id = %peer, %error, "inbound control request failed");
            }
            request_response::Event::ResponseSent { .. } => {}
        }
        Ok(())
    }

    fn handle_inbound_request(
        &mut self,
        peer_id: PeerId,
        request: ControlRequest,
    ) -> Result<ControlResponse, AppError> {
        match request {
            ControlRequest::Hello {
                version,
                nickname,
                platform,
            } => {
                self.record_hello(peer_id, version, nickname, platform)?;
                Ok(ControlResponse::Hello {
                    payload: HelloPayload {
                        version: PROTOCOL_VERSION,
                        nickname: self.local_profile.nickname.clone(),
                        platform: self.local_profile.platform,
                    },
                })
            }
            ControlRequest::FriendRequest {
                request_id,
                nickname,
            } => {
                self.enforce_friend_request_rate(peer_id)?;
                uuid::Uuid::parse_str(&request_id).map_err(|_| {
                    AppError::InvalidInput("好友申请编号无效，已拒绝该请求".to_string())
                })?;
                let nickname = validate_nickname(&nickname)?;
                if self.storage.is_friend(&peer_id.to_string())? {
                    return Err(AppError::InvalidInput(
                        "双方已经是好友，无需重复申请".to_string(),
                    ));
                }
                if let Some(existing) = self.storage.get_friend_request(&request_id)? {
                    if existing.peer_id != peer_id.to_string() {
                        return Err(AppError::InvalidInput(
                            "好友申请编号冲突，已拒绝该请求".to_string(),
                        ));
                    }
                    return Ok(ControlResponse::Accepted);
                }
                let now = now_rfc3339();
                let friend_request = FriendRequest {
                    request_id,
                    peer_id: peer_id.to_string(),
                    nickname,
                    direction: Direction::Incoming,
                    status: FriendRequestStatus::Pending,
                    created_at: now.clone(),
                    updated_at: now,
                };
                self.storage.put_friend_request(&friend_request)?;
                self.emit(NetworkEvent::FriendRequestReceived {
                    request: friend_request,
                });
                Ok(ControlResponse::Accepted)
            }
            ControlRequest::FriendDecision {
                request_id,
                accepted,
                nickname,
            } => {
                let request = self
                    .storage
                    .get_friend_request(&request_id)?
                    .ok_or_else(|| AppError::InvalidInput("找不到对应的好友申请".to_string()))?;
                if request.peer_id != peer_id.to_string()
                    || request.direction != Direction::Outgoing
                {
                    return Err(AppError::InvalidInput(
                        "好友申请与发送者不匹配，已拒绝处理".to_string(),
                    ));
                }
                let status = if accepted {
                    FriendRequestStatus::Accepted
                } else {
                    FriendRequestStatus::Rejected
                };
                let nickname = validate_nickname(&nickname)?;
                let peer = self.storage.get_peer(&peer_id.to_string())?;
                let now = now_rfc3339();
                let friend = accepted.then(|| Friend {
                    peer_id: peer_id.to_string(),
                    nickname: nickname.clone(),
                    platform: peer
                        .as_ref()
                        .map_or(Platform::Unknown, |peer| peer.platform),
                    online: true,
                    added_at: now.clone(),
                    last_seen: now.clone(),
                });
                self.storage
                    .resolve_friend_request(&request_id, status, friend.as_ref(), &now)?;
                let mut resolved = request;
                resolved.nickname = nickname;
                resolved.status = status;
                resolved.updated_at = now;
                self.emit(NetworkEvent::FriendRequestResolved {
                    request: resolved,
                    friend,
                });
                Ok(ControlResponse::Accepted)
            }
            ControlRequest::TextMessage {
                message_id,
                sent_at: _,
                body,
            } => {
                self.ensure_friend(&peer_id)?;
                uuid::Uuid::parse_str(&message_id).map_err(|_| {
                    AppError::InvalidInput("消息编号无效，已拒绝该消息".to_string())
                })?;
                let body = validate_text(&body)?;
                let message = ChatMessage {
                    message_id,
                    peer_id: peer_id.to_string(),
                    direction: Direction::Incoming,
                    kind: MessageKind::Text,
                    body: Some(body),
                    local_path: None,
                    file_name: None,
                    file_size: None,
                    status: MessageStatus::Delivered,
                    error: None,
                    created_at: now_rfc3339(),
                };
                if self.storage.insert_message(&message)? {
                    self.emit(NetworkEvent::MessageReceived { message });
                }
                Ok(ControlResponse::Accepted)
            }
            ControlRequest::TransferOffer { offer } => {
                self.ensure_friend(&peer_id)?;
                validate_transfer_offer(&offer)?;
                if let Some(existing) = self.storage.get_transfer(&offer.transfer_id)? {
                    if existing.peer_id != peer_id.to_string() {
                        return Err(AppError::InvalidInput(
                            "传输编号冲突，已拒绝该文件".to_string(),
                        ));
                    }
                    return Ok(ControlResponse::Accepted);
                }
                let auto_accept =
                    offer.kind == TransferKind::Image && offer.file_size <= MAX_AUTO_IMAGE_BYTES;
                let local_path = auto_accept.then(|| {
                    automatic_receive_path(&self.app_data_dir, &offer.transfer_id, &offer.file_name)
                        .to_string_lossy()
                        .into_owned()
                });
                let now = now_rfc3339();
                let transfer = TransferRecord {
                    transfer_id: offer.transfer_id.clone(),
                    peer_id: peer_id.to_string(),
                    direction: Direction::Incoming,
                    kind: offer.kind,
                    file_name: offer.file_name,
                    file_size: offer.file_size,
                    mime_type: offer.mime_type,
                    sha256: offer.sha256,
                    local_path,
                    transferred_bytes: 0,
                    status: if auto_accept {
                        TransferStatus::Transferring
                    } else {
                        TransferStatus::AwaitingAcceptance
                    },
                    error: None,
                    created_at: now.clone(),
                    updated_at: now,
                };
                self.storage.upsert_transfer(&transfer)?;
                self.emit(NetworkEvent::TransferUpdated {
                    transfer: transfer.clone(),
                });
                if auto_accept {
                    let outbound_id = self.swarm.behaviour_mut().control.send_request(
                        &peer_id,
                        ControlRequest::TransferDecision {
                            transfer_id: transfer.transfer_id,
                            accepted: true,
                        },
                    );
                    self.pending
                        .insert(outbound_id, PendingAction::TransferDecision);
                }
                Ok(ControlResponse::Accepted)
            }
            ControlRequest::TransferDecision {
                transfer_id,
                accepted,
            } => {
                self.ensure_friend(&peer_id)?;
                let mut transfer = self
                    .storage
                    .get_transfer(&transfer_id)?
                    .ok_or_else(|| AppError::InvalidInput("找不到对应的文件传输".to_string()))?;
                if transfer.peer_id != peer_id.to_string()
                    || transfer.direction != Direction::Outgoing
                {
                    return Err(AppError::InvalidInput(
                        "文件传输与发送者不匹配，已拒绝处理".to_string(),
                    ));
                }
                transfer.updated_at = now_rfc3339();
                if accepted {
                    transfer.status = TransferStatus::Transferring;
                    transfer.error = None;
                    self.storage.upsert_transfer(&transfer)?;
                    self.emit(NetworkEvent::TransferUpdated {
                        transfer: transfer.clone(),
                    });
                    transfer::spawn_outgoing_transfer(
                        self.stream_control.clone(),
                        peer_id,
                        transfer,
                        self.storage.clone(),
                        self.app_handle.clone(),
                    );
                } else {
                    transfer.status = TransferStatus::Cancelled;
                    transfer.error = Some("对方拒绝了这次传输".to_string());
                    self.storage.upsert_transfer(&transfer)?;
                    self.storage.update_message_status(
                        &transfer.transfer_id,
                        MessageStatus::Failed,
                        transfer.error.as_deref(),
                    )?;
                    self.emit(NetworkEvent::TransferUpdated { transfer });
                }
                Ok(ControlResponse::Accepted)
            }
            ControlRequest::TransferCancel { transfer_id } => {
                self.ensure_friend(&peer_id)?;
                let mut transfer = self
                    .storage
                    .get_transfer(&transfer_id)?
                    .ok_or_else(|| AppError::InvalidInput("找不到对应的文件传输".to_string()))?;
                if transfer.peer_id != peer_id.to_string() {
                    return Err(AppError::InvalidInput(
                        "文件传输与发送者不匹配，已拒绝处理".to_string(),
                    ));
                }
                transfer.status = TransferStatus::Cancelled;
                transfer.error = Some("对方取消了传输".to_string());
                transfer.updated_at = now_rfc3339();
                self.storage.upsert_transfer(&transfer)?;
                self.emit(NetworkEvent::TransferUpdated { transfer });
                Ok(ControlResponse::Accepted)
            }
        }
    }

    fn handle_outbound_response(
        &mut self,
        peer_id: PeerId,
        request_id: request_response::OutboundRequestId,
        response: ControlResponse,
    ) -> Result<(), AppError> {
        let Some(action) = self.pending.remove(&request_id) else {
            return Ok(());
        };
        match (action, response) {
            (PendingAction::Hello, ControlResponse::Hello { payload }) => {
                self.record_hello(peer_id, payload.version, payload.nickname, payload.platform)?;
            }
            (PendingAction::Text { message_id }, ControlResponse::Accepted) => {
                self.set_message_status(&message_id, MessageStatus::Delivered, None)?;
            }
            (PendingAction::Text { message_id }, ControlResponse::Rejected { message, .. }) => {
                self.set_message_status(&message_id, MessageStatus::Failed, Some(message))?;
            }
            (
                PendingAction::TransferOffer { transfer_id },
                ControlResponse::Rejected { message, .. },
            ) => {
                self.set_transfer_failed(&transfer_id, message)?;
            }
            (
                PendingAction::FriendRequest { request_id },
                ControlResponse::Rejected { message, .. },
            ) => {
                self.emit(NetworkEvent::NetworkError {
                    code: "friend_request_rejected".to_string(),
                    message: format!("好友申请 {request_id} 未送达：{message}"),
                });
            }
            (PendingAction::Hello, ControlResponse::Rejected { message, .. }) => {
                self.emit(NetworkEvent::NetworkError {
                    code: "hello_rejected".to_string(),
                    message,
                });
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_outbound_failure(
        &mut self,
        request_id: request_response::OutboundRequestId,
        error: request_response::OutboundFailure,
    ) -> Result<(), AppError> {
        let Some(action) = self.pending.remove(&request_id) else {
            return Ok(());
        };
        let message = format!("发送失败，请确认对方在线后重试：{error}");
        match action {
            PendingAction::Text { message_id } => {
                self.set_message_status(&message_id, MessageStatus::Failed, Some(message))?;
            }
            PendingAction::FriendRequest { request_id } => {
                self.emit(NetworkEvent::NetworkError {
                    code: "friend_request_failed".to_string(),
                    message: format!("好友申请 {request_id} 发送失败，请稍后重试"),
                });
            }
            PendingAction::TransferOffer { transfer_id } => {
                self.set_transfer_failed(&transfer_id, message)?;
            }
            PendingAction::Hello
            | PendingAction::FriendDecision
            | PendingAction::TransferDecision
            | PendingAction::TransferCancel => {
                tracing::debug!(%error, "control request failed");
            }
        }
        Ok(())
    }

    fn record_hello(
        &self,
        peer_id: PeerId,
        version: u16,
        nickname: String,
        platform: Platform,
    ) -> Result<(), AppError> {
        let peer = PeerSummary {
            peer_id: peer_id.to_string(),
            nickname: validate_nickname(&nickname)?,
            platform,
            online: true,
            protocol_version: version,
            last_seen: now_rfc3339(),
        };
        self.storage.upsert_peer(&peer)?;
        self.emit(NetworkEvent::PeerDiscovered { peer });
        Ok(())
    }

    fn mark_offline_if_unreachable(&self, peer_id: PeerId) -> Result<(), AppError> {
        let has_mdns_address = self
            .mdns_addresses
            .get(&peer_id)
            .is_some_and(|addresses| !addresses.is_empty());
        let has_beacon_address = self
            .beacon_addresses
            .get(&peer_id)
            .is_some_and(|addresses| !addresses.is_empty());
        let has_connection = self.active_connections.get(&peer_id).copied().unwrap_or(0) > 0;
        if !has_mdns_address && !has_beacon_address && !has_connection {
            let last_seen = now_rfc3339();
            self.storage
                .set_peer_offline(&peer_id.to_string(), &last_seen)?;
            self.emit(NetworkEvent::PeerOffline {
                peer_id: peer_id.to_string(),
                last_seen,
            });
        }
        Ok(())
    }

    fn ensure_connected(&self, peer_id: &PeerId) -> Result<(), AppError> {
        if self.swarm.is_connected(peer_id) {
            Ok(())
        } else {
            Err(AppError::OfflinePeer)
        }
    }

    fn ensure_friend(&self, peer_id: &PeerId) -> Result<(), AppError> {
        if self.storage.is_friend(&peer_id.to_string())? {
            Ok(())
        } else {
            Err(AppError::NotFriend)
        }
    }

    fn enforce_friend_request_rate(&mut self, peer_id: PeerId) -> Result<(), AppError> {
        let now = Instant::now();
        let times = self.friend_request_times.entry(peer_id).or_default();
        while times
            .front()
            .is_some_and(|time| now.duration_since(*time) > FRIEND_REQUEST_WINDOW)
        {
            times.pop_front();
        }
        if times.len() >= FRIEND_REQUEST_LIMIT {
            return Err(AppError::InvalidInput(
                "好友申请过于频繁，请稍后重试".to_string(),
            ));
        }
        times.push_back(now);
        Ok(())
    }

    fn set_message_status(
        &self,
        message_id: &str,
        status: MessageStatus,
        error: Option<String>,
    ) -> Result<(), AppError> {
        self.storage
            .update_message_status(message_id, status, error.as_deref())?;
        self.emit(NetworkEvent::MessageStatusChanged {
            message_id: message_id.to_string(),
            status,
            error,
        });
        Ok(())
    }

    fn set_transfer_failed(&self, transfer_id: &str, error: String) -> Result<(), AppError> {
        let Some(mut transfer) = self.storage.get_transfer(transfer_id)? else {
            return Ok(());
        };
        transfer.status = TransferStatus::Failed;
        transfer.error = Some(error.clone());
        transfer.updated_at = now_rfc3339();
        self.storage.upsert_transfer(&transfer)?;
        self.storage
            .update_message_status(transfer_id, MessageStatus::Failed, Some(&error))?;
        self.emit(NetworkEvent::TransferUpdated { transfer });
        Ok(())
    }

    fn emit(&self, event: NetworkEvent) {
        emit_event(&self.app_handle, &event);
    }
}

fn parse_peer_id(value: &str) -> Result<PeerId, AppError> {
    value
        .parse()
        .map_err(|_| AppError::InvalidInput("用户身份无效，请刷新附近用户列表".to_string()))
}

pub(super) fn emit_event(app_handle: &AppHandle, event: &NetworkEvent) {
    if let Err(error) = app_handle.emit(EVENT_NAME, event) {
        tracing::warn!(%error, "failed to emit Weline Chat event");
    }
}

fn validate_transfer_offer(offer: &TransferOffer) -> Result<(), AppError> {
    uuid::Uuid::parse_str(&offer.transfer_id)
        .map_err(|_| AppError::InvalidInput("文件传输编号无效".to_string()))?;
    if offer.file_size > MAX_FILE_BYTES {
        return Err(AppError::InvalidInput("文件大小超出允许范围".to_string()));
    }
    if offer.file_name.trim().is_empty()
        || offer.file_name.chars().count() > 255
        || offer.file_name.chars().any(char::is_control)
    {
        return Err(AppError::InvalidInput("文件名无效".to_string()));
    }
    if offer.sha256.len() != 64 || !offer.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(AppError::InvalidInput("文件校验信息无效".to_string()));
    }
    if offer.mime_type.len() > 128 || offer.mime_type.chars().any(char::is_control) {
        return Err(AppError::InvalidInput("文件类型信息无效".to_string()));
    }
    Ok(())
}

fn automatic_receive_path(
    app_data_dir: &std::path::Path,
    transfer_id: &str,
    file_name: &str,
) -> PathBuf {
    let safe_name: String = file_name
        .chars()
        .map(|character| {
            if character.is_alphanumeric() || matches!(character, '.' | '-' | '_' | ' ') {
                character
            } else {
                '_'
            }
        })
        .take(120)
        .collect();
    app_data_dir
        .join("received")
        .join(format!("{transfer_id}-{safe_name}"))
}
