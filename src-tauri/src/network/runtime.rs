use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex},
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
use tauri::{AppHandle, Emitter, Manager, UserAttentionType};
use tokio::sync::{mpsc, oneshot, watch};

use super::{
    behaviour::{LocalnetBehaviour, LocalnetBehaviourEvent},
    discovery::{DiscoveryEvent, DiscoveryService},
    transfer,
};
use crate::{
    domain::{
        ChatMessage, Direction, Friend, FriendRequest, FriendRequestStatus, LocalProfile,
        MessageKind, MessageStatus, PROTOCOL_VERSION, PeerSummary, Platform, TransferPreferences,
        TransferRecord, TransferStatus, now_rfc3339, validate_nickname, validate_text,
    },
    error::AppError,
    identity::LocalIdentity,
    protocol::{ControlRequest, ControlResponse, FILE_PROTOCOL, HelloPayload, TransferOffer},
    receive_paths::{
        preflight_receive_directory, remove_owned_reservation, reserve_available_receive_path,
    },
    storage::Storage,
    transfer_manifest::validate_transfer_metadata,
    transfer_policy::FILE_RESUME_V2_CAPABILITY,
};

const EVENT_NAME: &str = "localnet://event";
const FRIEND_REQUEST_LIMIT: usize = 5;
const FRIEND_REQUEST_WINDOW: Duration = Duration::from_secs(60);
const INCOMING_START_TIMEOUT: Duration = Duration::from_secs(35);

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
        completion: Option<TransferDecisionCompletion>,
    },
    CancelTransfer {
        peer_id: String,
        transfer_id: String,
    },
    ExpireIncomingDecision {
        transfer_id: String,
        decision_token: String,
    },
}

pub type TransferDecisionCompletion = Arc<Mutex<Option<oneshot::Sender<Result<(), String>>>>>;

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
    FriendRequestDelivered {
        request_id: String,
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

fn complete_transfer_decision(
    completion: &Option<TransferDecisionCompletion>,
    result: Result<(), String>,
) {
    let Some(completion) = completion else {
        return;
    };
    let sender = completion
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
    if let Some(sender) = sender {
        let _ = sender.send(result);
    }
}

#[cfg(not(test))]
pub fn spawn_network(
    identity: LocalIdentity,
    profile: LocalProfile,
    storage: Storage,
    app_handle: AppHandle,
    default_receive_directory: PathBuf,
) -> NetworkHandle {
    let (sender, receiver) = mpsc::channel(128);
    let runtime_sender = sender.downgrade();
    tauri::async_runtime::spawn(async move {
        if let Err(error) = NetworkRuntime::run(
            identity,
            profile,
            storage,
            app_handle.clone(),
            default_receive_directory,
            receiver,
            runtime_sender,
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
            tracing::error!(error = %error, "Weline Localnet network runtime stopped");
        }
    });
    NetworkHandle { sender }
}

#[cfg(test)]
pub fn spawn_network(
    _identity: LocalIdentity,
    _profile: LocalProfile,
    _storage: Storage,
    _app_handle: AppHandle,
    _default_receive_directory: PathBuf,
) -> NetworkHandle {
    let (sender, _receiver) = mpsc::channel(1);
    NetworkHandle { sender }
}

#[derive(Debug)]
enum PendingAction {
    Hello,
    FriendRequest {
        request_id: String,
    },
    FriendDecision,
    Text {
        message_id: String,
    },
    TransferOffer {
        transfer_id: String,
    },
    TransferDecision {
        transfer_id: String,
        accepted: bool,
        decision_token: Option<String>,
    },
    TransferCancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PendingRequestId {
    Network(request_response::OutboundRequestId),
    #[cfg(test)]
    Test(u64),
}

#[cfg(test)]
#[derive(Default)]
struct TestControlTransport {
    next_request_id: u64,
    fail_next: Option<String>,
    requests: Vec<(PeerId, ControlRequest)>,
}

struct NetworkRuntime {
    local_profile: LocalProfile,
    storage: Storage,
    #[cfg(not(test))]
    app_handle: AppHandle,
    default_receive_directory: PathBuf,
    #[cfg(not(test))]
    swarm: Swarm<LocalnetBehaviour>,
    #[cfg(not(test))]
    stream_control: libp2p_stream::Control,
    receiver: mpsc::Receiver<NetworkCommand>,
    command_sender: mpsc::WeakSender<NetworkCommand>,
    discovery_receiver: mpsc::Receiver<DiscoveryEvent>,
    listen_port_sender: watch::Sender<Option<u16>>,
    pending: HashMap<PendingRequestId, PendingAction>,
    mdns_addresses: HashMap<PeerId, HashSet<Multiaddr>>,
    beacon_addresses: HashMap<PeerId, HashMap<Multiaddr, Instant>>,
    active_connections: HashMap<PeerId, usize>,
    friend_request_times: HashMap<PeerId, VecDeque<Instant>>,
    mdns_enabled: bool,
    #[cfg(test)]
    test_control_transport: Option<TestControlTransport>,
    #[cfg(test)]
    test_events: Mutex<Vec<NetworkEvent>>,
    #[cfg(test)]
    test_order: Mutex<Vec<&'static str>>,
}

impl NetworkRuntime {
    #[cfg(not(test))]
    async fn run(
        identity: LocalIdentity,
        profile: LocalProfile,
        storage: Storage,
        app_handle: AppHandle,
        default_receive_directory: PathBuf,
        receiver: mpsc::Receiver<NetworkCommand>,
        command_sender: mpsc::WeakSender<NetworkCommand>,
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

        tracing::info!(peer_id = %peer_id, mdns_enabled, "Weline Localnet network runtime started");
        let mut runtime = Self {
            local_profile: profile,
            storage,
            app_handle,
            default_receive_directory,
            swarm,
            stream_control,
            receiver,
            command_sender,
            discovery_receiver,
            listen_port_sender,
            pending: HashMap::new(),
            mdns_addresses: HashMap::new(),
            beacon_addresses: HashMap::new(),
            active_connections: HashMap::new(),
            friend_request_times: HashMap::new(),
            mdns_enabled,
            #[cfg(test)]
            test_control_transport: None,
            #[cfg(test)]
            test_events: Mutex::new(Vec::new()),
            #[cfg(test)]
            test_order: Mutex::new(Vec::new()),
        };
        runtime.event_loop().await
    }

    #[cfg(not(test))]
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
                let outbound_id = self.send_control_request(
                    peer_id,
                    ControlRequest::FriendRequest {
                        request_id: request.request_id,
                        nickname: self.local_profile.nickname.clone(),
                    },
                )?;
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
                let outbound_id = self.send_control_request(
                    peer_id,
                    ControlRequest::FriendDecision {
                        request_id,
                        accepted,
                        nickname: self.local_profile.nickname.clone(),
                    },
                )?;
                self.pending
                    .insert(outbound_id, PendingAction::FriendDecision);
            }
            NetworkCommand::SendText(message) => {
                let peer_id = parse_peer_id(&message.peer_id)?;
                self.ensure_connected(&peer_id)?;
                let message_id = message.message_id.clone();
                let outbound_id = self.send_control_request(
                    peer_id,
                    ControlRequest::TextMessage {
                        message_id: message.message_id,
                        sent_at: message.created_at,
                        body: message.body.unwrap_or_default(),
                    },
                )?;
                self.pending
                    .insert(outbound_id, PendingAction::Text { message_id });
            }
            NetworkCommand::OfferTransfer(transfer) => {
                let peer_id = parse_peer_id(&transfer.peer_id)?;
                self.ensure_connected(&peer_id)?;
                let transfer_id = transfer.transfer_id.clone();
                let outbound_id = self.send_control_request(
                    peer_id,
                    ControlRequest::TransferOffer {
                        offer: TransferOffer {
                            transfer_id: transfer.transfer_id,
                            kind: transfer.kind,
                            file_name: transfer.file_name,
                            file_size: transfer.file_size,
                            mime_type: transfer.mime_type,
                            sha256: transfer.sha256,
                            transfer_protocol: transfer.transfer_protocol,
                            chunk_size: transfer.chunk_size,
                            chunk_count: transfer.chunk_count,
                            manifest_sha256: transfer.manifest_sha256,
                        },
                    },
                )?;
                self.pending
                    .insert(outbound_id, PendingAction::TransferOffer { transfer_id });
            }
            NetworkCommand::ResolveTransfer {
                peer_id,
                transfer_id,
                accepted,
                completion,
            } => {
                let peer_id = parse_peer_id(&peer_id)?;
                self.ensure_connected(&peer_id)?;
                let storage = self.storage.clone();
                let submission = Self::handle_transfer_decision_submission(
                    &storage,
                    &peer_id.to_string(),
                    &transfer_id,
                    accepted,
                    || {
                        self.send_control_request(
                            peer_id,
                            ControlRequest::TransferDecision {
                                transfer_id: transfer_id.clone(),
                                accepted,
                            },
                        )
                    },
                )?;
                self.insert_pending_transfer_decision(
                    submission.request_id,
                    transfer_id.clone(),
                    accepted,
                    submission.decision_token.clone(),
                );
                if accepted {
                    let submitted = submission
                        .transfer
                        .expect("accepted submission is prepared");
                    let decision_token = submission
                        .decision_token
                        .expect("accepted submission has a durable decision token");
                    self.emit(NetworkEvent::TransferUpdated {
                        transfer: submitted,
                    });
                    self.spawn_incoming_start_timeout(transfer_id, decision_token);
                }
                #[cfg(test)]
                self.record_test_order("completion");
                complete_transfer_decision(&completion, Ok(()));
            }
            NetworkCommand::CancelTransfer {
                peer_id,
                transfer_id,
            } => {
                let peer_id = parse_peer_id(&peer_id)?;
                self.ensure_connected(&peer_id)?;
                let outbound_id = self.send_control_request(
                    peer_id,
                    ControlRequest::TransferCancel { transfer_id },
                )?;
                self.pending
                    .insert(outbound_id, PendingAction::TransferCancel);
            }
            NetworkCommand::ExpireIncomingDecision {
                transfer_id,
                decision_token,
            } => {
                self.pending.retain(|_, action| {
                    !matches!(
                        action,
                        PendingAction::TransferDecision {
                            transfer_id: pending_transfer_id,
                            accepted: true,
                            decision_token: Some(pending_token),
                        } if pending_transfer_id == &transfer_id && pending_token == &decision_token
                    )
                });
                self.handle_incoming_start_timeout(&transfer_id, &decision_token)?;
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
            NetworkCommand::ResolveTransfer {
                transfer_id,
                accepted,
                completion,
                ..
            } => {
                let compensation = if *accepted {
                    if self
                        .storage
                        .pending_incoming_decision_token(transfer_id)?
                        .is_some()
                    {
                        Ok(None)
                    } else {
                        let message = format!("接收确认未提交，请重新确认：{error}");
                        match self.storage.get_transfer(transfer_id)? {
                            Some(current)
                                if current.direction == Direction::Incoming
                                    && current.status == TransferStatus::AwaitingAcceptance =>
                            {
                                Ok(Some(current))
                            }
                            _ => transfer::return_pending_incoming_decision_to_manual(
                                transfer_id,
                                &self.storage,
                                message,
                            ),
                        }
                    }
                } else {
                    Ok(None)
                };
                if let Some(transfer) = compensation? {
                    self.emit(NetworkEvent::TransferUpdated { transfer });
                }
                #[cfg(test)]
                self.record_test_order("completion");
                complete_transfer_decision(completion, Err(error.to_string()));
            }
            NetworkCommand::SetProfile(_)
            | NetworkCommand::SendFriendRequest(_)
            | NetworkCommand::ResolveFriendRequest { .. }
            | NetworkCommand::CancelTransfer { .. }
            | NetworkCommand::ExpireIncomingDecision { .. } => {}
        }
        Ok(())
    }

    #[cfg(not(test))]
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
                        capabilities: vec![FILE_RESUME_V2_CAPABILITY.to_string()],
                    },
                );
                self.pending
                    .insert(PendingRequestId::Network(outbound_id), PendingAction::Hello);
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
                tracing::info!(%address, "Weline Localnet listening");
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

    #[cfg(not(test))]
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

    #[cfg(not(test))]
    fn dial_discovered_peer(&mut self, peer_id: PeerId, address: Multiaddr, source: &'static str) {
        let swarm = &mut self.swarm;
        swarm.add_peer_address(peer_id, address.clone());
        let options = DialOpts::peer_id(peer_id)
            .condition(PeerCondition::DisconnectedAndNotDialing)
            .addresses(vec![address])
            .build();
        if let Err(error) = swarm.dial(options) {
            tracing::trace!(peer_id = %peer_id, source, error = %error, "peer dial deferred");
        }
    }

    #[cfg(not(test))]
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

    #[cfg(not(test))]
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
                } => self.handle_outbound_response(
                    peer,
                    PendingRequestId::Network(request_id),
                    response,
                )?,
            },
            request_response::Event::OutboundFailure {
                request_id, error, ..
            } => self.handle_outbound_failure(PendingRequestId::Network(request_id), error)?,
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
                capabilities,
            } => {
                self.record_hello(peer_id, version, nickname, platform, capabilities)?;
                Ok(ControlResponse::Hello {
                    payload: HelloPayload {
                        version: PROTOCOL_VERSION,
                        nickname: self.local_profile.nickname.clone(),
                        platform: self.local_profile.platform,
                        capabilities: vec![FILE_RESUME_V2_CAPABILITY.to_string()],
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
                let preferences = self
                    .storage
                    .load_transfer_preferences(&self.default_receive_directory)?;
                let outcome = persist_incoming_offer_with_preflight(
                    &self.storage,
                    &peer_id.to_string(),
                    &offer,
                    &preferences,
                    &preflight_receive_directory,
                )?;
                let transfer = outcome.transfer;
                if outcome.transfer_decision == Some(true) {
                    let storage = self.storage.clone();
                    match Self::handle_transfer_decision_submission(
                        &storage,
                        &peer_id.to_string(),
                        &transfer.transfer_id,
                        true,
                        || {
                            self.send_control_request(
                                peer_id,
                                ControlRequest::TransferDecision {
                                    transfer_id: transfer.transfer_id.clone(),
                                    accepted: true,
                                },
                            )
                        },
                    ) {
                        Ok(submission) => {
                            let decision_token = submission
                                .decision_token
                                .clone()
                                .expect("automatic acceptance has a durable decision token");
                            let submitted = submission
                                .transfer
                                .expect("automatic acceptance is locally prepared");
                            self.insert_pending_transfer_decision(
                                submission.request_id,
                                submitted.transfer_id.clone(),
                                true,
                                Some(decision_token.clone()),
                            );
                            self.emit(NetworkEvent::TransferUpdated {
                                transfer: submitted.clone(),
                            });
                            self.spawn_incoming_start_timeout(
                                submitted.transfer_id,
                                decision_token,
                            );
                        }
                        Err(error) => {
                            let message = format!("自动接收确认未提交，请手动重新确认：{error}");
                            if let Some(current) =
                                self.storage.get_transfer(&transfer.transfer_id)?
                            {
                                let corrected = if current.status == TransferStatus::Transferring {
                                    transfer::return_pending_incoming_decision_to_manual(
                                        &current.transfer_id,
                                        &self.storage,
                                        message.clone(),
                                    )?
                                    .unwrap_or(current)
                                } else {
                                    current
                                };
                                self.emit(NetworkEvent::TransferUpdated {
                                    transfer: corrected,
                                });
                            }
                            self.emit(NetworkEvent::NetworkError {
                                code: error.code().to_string(),
                                message,
                            });
                        }
                    }
                } else {
                    self.emit(NetworkEvent::TransferUpdated {
                        transfer: transfer.clone(),
                    });
                    if let Some(message) = outcome.automatic_receive_error {
                        self.emit(NetworkEvent::NetworkError {
                            code: "transfer.auto_receive_unavailable".to_string(),
                            message: format!(
                                "自动接收目录当前不可用，请手动选择保存位置：{message}"
                            ),
                        });
                    }
                }
                Ok(ControlResponse::Accepted)
            }
            ControlRequest::TransferDecision {
                transfer_id,
                accepted,
            } => {
                self.ensure_friend(&peer_id)?;
                let transfer = self
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
                let next_status = if accepted {
                    TransferStatus::Transferring
                } else {
                    TransferStatus::Cancelled
                };
                let transition_error = if accepted {
                    None
                } else {
                    Some("对方拒绝了这次传输")
                };
                if !self.storage.try_transition_outgoing_awaiting(
                    &transfer.transfer_id,
                    &transfer.peer_id,
                    next_status,
                    transition_error,
                )? {
                    return Ok(ControlResponse::Accepted);
                }
                let transfer = self
                    .storage
                    .get_transfer(&transfer_id)?
                    .ok_or_else(|| AppError::InvalidInput("找不到对应的文件传输".to_string()))?;
                if accepted {
                    self.emit(NetworkEvent::TransferUpdated {
                        transfer: transfer.clone(),
                    });
                    #[cfg(not(test))]
                    transfer::spawn_outgoing_transfer(
                        self.stream_control.clone(),
                        peer_id,
                        transfer,
                        self.storage.clone(),
                        self.app_handle.clone(),
                    );
                } else {
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
                if transfer.peer_id != peer_id.to_string()
                    || transfer.direction != Direction::Incoming
                {
                    return Err(AppError::InvalidInput(
                        "文件传输与发送者不匹配，已拒绝处理".to_string(),
                    ));
                }
                if !self.storage.try_cancel_unclaimed_incoming_transfer(
                    &transfer.transfer_id,
                    &transfer.peer_id,
                    transfer.transfer_protocol,
                    "对方取消了传输",
                )? {
                    return Ok(ControlResponse::Accepted);
                }
                transfer = self
                    .storage
                    .get_transfer(&transfer_id)?
                    .ok_or_else(|| AppError::InvalidInput("找不到对应的文件传输".to_string()))?;
                if transfer.destination_reserved {
                    if let (Some(path), Some(token)) = (
                        transfer.local_path.as_deref(),
                        transfer.reservation_token.as_deref(),
                    ) {
                        match remove_owned_reservation(
                            std::path::Path::new(path),
                            &transfer.transfer_id,
                            token,
                        ) {
                            Ok(_) => {
                                transfer.destination_reserved = false;
                                transfer.reservation_token = None;
                            }
                            Err(error) => {
                                tracing::warn!(
                                    transfer_id = %transfer.transfer_id,
                                    %error,
                                    "failed to clean cancelled receive reservation"
                                );
                            }
                        }
                    }
                }
                self.storage.upsert_transfer(&transfer)?;
                self.emit(NetworkEvent::TransferUpdated { transfer });
                Ok(ControlResponse::Accepted)
            }
        }
    }

    fn handle_outbound_response(
        &mut self,
        peer_id: PeerId,
        request_id: PendingRequestId,
        response: ControlResponse,
    ) -> Result<(), AppError> {
        let Some(action) = self.pending.remove(&request_id) else {
            return Ok(());
        };
        match (action, response) {
            (PendingAction::Hello, ControlResponse::Hello { payload }) => {
                self.record_hello(
                    peer_id,
                    payload.version,
                    payload.nickname,
                    payload.platform,
                    payload.capabilities,
                )?;
            }
            (PendingAction::Text { message_id }, ControlResponse::Accepted) => {
                self.set_message_status(&message_id, MessageStatus::Delivered, None)?;
            }
            (PendingAction::FriendRequest { request_id }, ControlResponse::Accepted) => {
                self.emit(NetworkEvent::FriendRequestDelivered { request_id });
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
                self.fail_friend_request(
                    &request_id,
                    "friend_request_rejected",
                    format!("好友申请未送达：{message}"),
                )?;
            }
            (PendingAction::Hello, ControlResponse::Rejected { message, .. }) => {
                self.emit(NetworkEvent::NetworkError {
                    code: "hello_rejected".to_string(),
                    message,
                });
            }
            (
                PendingAction::TransferDecision {
                    transfer_id,
                    accepted: true,
                    decision_token: Some(decision_token),
                },
                ControlResponse::Rejected { message, .. },
            ) => {
                self.fail_pending_incoming_decision(
                    &transfer_id,
                    &decision_token,
                    format!("接收确认未送达，请重新确认：{message}"),
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_outbound_failure(
        &mut self,
        request_id: PendingRequestId,
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
                self.fail_friend_request(
                    &request_id,
                    "friend_request_failed",
                    "好友申请发送失败，请确认对方在线后重试".to_string(),
                )?;
            }
            PendingAction::TransferOffer { transfer_id } => {
                self.set_transfer_failed(&transfer_id, message)?;
            }
            PendingAction::TransferDecision {
                transfer_id,
                accepted: true,
                decision_token: Some(decision_token),
            } => {
                self.fail_pending_incoming_decision(
                    &transfer_id,
                    &decision_token,
                    format!("接收确认发送失败，请重新确认：{error}"),
                )?;
            }
            PendingAction::Hello
            | PendingAction::FriendDecision
            | PendingAction::TransferDecision {
                accepted: false, ..
            }
            | PendingAction::TransferDecision {
                accepted: true,
                decision_token: None,
                ..
            }
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
        capabilities: Vec<String>,
    ) -> Result<(), AppError> {
        let peer = PeerSummary {
            peer_id: peer_id.to_string(),
            nickname: validate_nickname(&nickname)?,
            platform,
            online: true,
            protocol_version: version,
            capabilities,
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

    fn send_control_request(
        &mut self,
        peer_id: PeerId,
        request: ControlRequest,
    ) -> Result<PendingRequestId, AppError> {
        #[cfg(test)]
        if let Some(transport) = self.test_control_transport.as_mut() {
            self.test_order
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("send_request");
            transport.requests.push((peer_id, request));
            if let Some(message) = transport.fail_next.take() {
                return Err(AppError::Network(message));
            }
            transport.next_request_id += 1;
            return Ok(PendingRequestId::Test(transport.next_request_id));
        }

        #[cfg(test)]
        return Err(AppError::Network(
            "test runtime control transport is unavailable".to_string(),
        ));

        #[cfg(not(test))]
        {
            let request_id = self
                .swarm
                .behaviour_mut()
                .control
                .send_request(&peer_id, request);
            Ok(PendingRequestId::Network(request_id))
        }
    }

    fn insert_pending_transfer_decision(
        &mut self,
        request_id: PendingRequestId,
        transfer_id: String,
        accepted: bool,
        decision_token: Option<String>,
    ) {
        self.pending.insert(
            request_id,
            PendingAction::TransferDecision {
                transfer_id,
                accepted,
                decision_token,
            },
        );
        #[cfg(test)]
        self.record_test_order("pending_registered");
    }

    fn spawn_incoming_start_timeout(&self, transfer_id: String, decision_token: String) {
        #[cfg(test)]
        {
            let _ = (transfer_id, decision_token);
        }
        #[cfg(not(test))]
        {
            let sender = self.command_sender.clone();
            tauri::async_runtime::spawn(async move {
                tokio::time::sleep(INCOMING_START_TIMEOUT).await;
                let Some(sender) = sender.upgrade() else {
                    return;
                };
                if let Err(error) = sender
                    .send(NetworkCommand::ExpireIncomingDecision {
                        transfer_id: transfer_id.clone(),
                        decision_token,
                    })
                    .await
                {
                    tracing::warn!(%transfer_id, %error, "failed to enqueue incoming transfer timeout");
                }
            });
        }
    }

    fn fail_pending_incoming_decision(
        &self,
        transfer_id: &str,
        decision_token: &str,
        message: String,
    ) -> Result<(), AppError> {
        let Some(candidate) = self.storage.get_transfer(transfer_id)? else {
            return Ok(());
        };
        if let Some(updated) = self.storage.rollback_pending_incoming_decision(
            transfer_id,
            &candidate.peer_id,
            decision_token,
            &message,
        )? {
            self.emit(NetworkEvent::TransferUpdated { transfer: updated });
            self.emit(NetworkEvent::NetworkError {
                code: "transfer.receive_not_started".to_string(),
                message,
            });
        }
        Ok(())
    }

    fn handle_incoming_start_timeout(
        &self,
        transfer_id: &str,
        decision_token: &str,
    ) -> Result<(), AppError> {
        self.fail_pending_incoming_decision(
            transfer_id,
            decision_token,
            "对方未在限定时间内开始传输，请重新确认接收".to_string(),
        )
    }

    fn ensure_connected(&self, peer_id: &PeerId) -> Result<(), AppError> {
        let connected = self.active_connections.get(peer_id).copied().unwrap_or(0) > 0;
        #[cfg(not(test))]
        let connected = connected || self.swarm.is_connected(peer_id);
        if connected {
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

    fn fail_friend_request(
        &self,
        request_id: &str,
        code: &str,
        message: String,
    ) -> Result<(), AppError> {
        self.storage
            .remove_pending_outgoing_friend_request(request_id)?;
        self.emit(NetworkEvent::NetworkError {
            code: code.to_string(),
            message,
        });
        Ok(())
    }

    fn emit(&self, event: NetworkEvent) {
        #[cfg(test)]
        {
            self.test_order
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("event");
            self.test_events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(event.clone());
            return;
        }
        #[cfg(not(test))]
        {
            if matches!(&event, NetworkEvent::FriendRequestReceived { .. }) {
                if let Some(window) = self.app_handle.get_webview_window("main") {
                    if let Err(error) =
                        window.request_user_attention(Some(UserAttentionType::Critical))
                    {
                        tracing::debug!(%error, "unable to request attention for incoming friend request");
                    }
                }
            }
            emit_event(&self.app_handle, &event);
        }
    }

    #[cfg(test)]
    fn record_test_order(&self, step: &'static str) {
        self.test_order
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(step);
    }
}

fn parse_peer_id(value: &str) -> Result<PeerId, AppError> {
    value
        .parse()
        .map_err(|_| AppError::InvalidInput("用户身份无效，请刷新附近用户列表".to_string()))
}

pub(super) fn emit_event(app_handle: &AppHandle, event: &NetworkEvent) {
    if let Err(error) = app_handle.emit(EVENT_NAME, event) {
        tracing::warn!(%error, "failed to emit Weline Localnet event");
    }
}

fn validate_transfer_offer(offer: &TransferOffer) -> Result<(), AppError> {
    uuid::Uuid::parse_str(&offer.transfer_id)
        .map_err(|_| AppError::InvalidInput("文件传输编号无效".to_string()))?;
    validate_transfer_metadata(
        offer.transfer_protocol,
        offer.file_size,
        offer.chunk_size,
        offer.chunk_count,
        offer.manifest_sha256.as_deref(),
    )?;
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

#[cfg(test)]
fn automatic_receive_path(
    preferences: &TransferPreferences,
    file_name: &str,
    transfer_id: &str,
    transfer_protocol: u8,
    file_size: u64,
) -> Result<(PathBuf, String), AppError> {
    automatic_receive_path_with_preflight(
        preferences,
        file_name,
        transfer_id,
        transfer_protocol,
        file_size,
        &preflight_receive_directory,
    )
}

fn automatic_receive_path_with_preflight<P>(
    preferences: &TransferPreferences,
    file_name: &str,
    transfer_id: &str,
    transfer_protocol: u8,
    file_size: u64,
    preflight: &P,
) -> Result<(PathBuf, String), AppError>
where
    P: Fn(&std::path::Path, u64, u64) -> Result<(), AppError>,
{
    let configured = std::path::Path::new(&preferences.receive_directory);
    if !matches!(transfer_protocol, 1 | 2) {
        return Err(AppError::InvalidInput("接收文件协议版本无效".to_string()));
    }
    if !configured.is_absolute() {
        return Err(AppError::InvalidInput(
            "文件接收目录必须是绝对路径".to_string(),
        ));
    }
    preflight(configured, file_size, 0)?;
    let directory = configured.to_path_buf();
    let reservation_token = uuid::Uuid::new_v4().to_string();
    let path =
        reserve_available_receive_path(&directory, file_name, transfer_id, &reservation_token)?;
    Ok((path, reservation_token))
}

struct IncomingOfferOutcome {
    transfer: TransferRecord,
    automatic_receive_error: Option<String>,
    transfer_decision: Option<bool>,
}

#[derive(Debug)]
struct SubmittedTransferDecision<R> {
    request_id: R,
    transfer: Option<TransferRecord>,
    decision_token: Option<String>,
}

impl NetworkRuntime {
    fn handle_transfer_decision_submission<R, S>(
        storage: &Storage,
        peer_id: &str,
        transfer_id: &str,
        accepted: bool,
        submit_once: S,
    ) -> Result<SubmittedTransferDecision<R>, AppError>
    where
        S: FnOnce() -> Result<R, AppError>,
    {
        if !accepted {
            return submit_once().map(|request_id| SubmittedTransferDecision {
                request_id,
                transfer: None,
                decision_token: None,
            });
        }

        let prepared = storage.prepare_incoming_acceptance_decision(transfer_id, peer_id)?;
        match submit_once() {
            Ok(request_id) => Ok(SubmittedTransferDecision {
                request_id,
                transfer: Some(prepared.transfer),
                decision_token: Some(prepared.decision_token),
            }),
            Err(error) => {
                let message = format!("接收确认未提交，请重新确认：{error}");
                let reverted = storage.rollback_pending_incoming_decision(
                    transfer_id,
                    peer_id,
                    &prepared.decision_token,
                    &message,
                )?;
                if reverted.is_none() {
                    return Err(AppError::Storage(
                        "接收确认提交失败但本地待处理状态已变化，请刷新后重试".to_string(),
                    ));
                }
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
enum AcceptedSubmissionOutcome {
    Submitted {
        transfer: TransferRecord,
    },
    Reverted {
        transfer: TransferRecord,
        error: AppError,
    },
}

#[cfg(test)]
fn finalize_accepted_transfer_submission(
    storage: &Storage,
    transfer_id: &str,
    submission: Result<(), AppError>,
) -> Result<AcceptedSubmissionOutcome, AppError> {
    match submission {
        Ok(()) => {
            let transfer = storage
                .get_transfer(transfer_id)?
                .ok_or_else(|| AppError::Storage("已提交的接收确认记录不存在".to_string()))?;
            if transfer.direction != Direction::Incoming
                || transfer.status != TransferStatus::Transferring
            {
                return Err(AppError::Storage(
                    "接收确认提交后状态不一致，请刷新后重试".to_string(),
                ));
            }
            Ok(AcceptedSubmissionOutcome::Submitted { transfer })
        }
        Err(error) => {
            let message = format!("接收确认未提交，请重新确认：{error}");
            let transfer = transfer::return_pending_incoming_decision_to_manual(
                transfer_id,
                storage,
                message,
            )?
            .ok_or_else(|| {
                AppError::Storage("接收确认失败时传输状态已变化，请刷新后重试".to_string())
            })?;
            Ok(AcceptedSubmissionOutcome::Reverted { transfer, error })
        }
    }
}

fn persist_incoming_offer_with_preflight<P>(
    storage: &Storage,
    peer_id: &str,
    offer: &TransferOffer,
    preferences: &TransferPreferences,
    preflight: &P,
) -> Result<IncomingOfferOutcome, AppError>
where
    P: Fn(&std::path::Path, u64, u64) -> Result<(), AppError>,
{
    persist_incoming_offer_with_preflight_and_accept(
        storage,
        peer_id,
        offer,
        preferences,
        preflight,
        &|storage, accepted| storage.try_accept_incoming_transfer(accepted),
    )
}

fn persist_incoming_offer_with_preflight_and_accept<P, A>(
    storage: &Storage,
    peer_id: &str,
    offer: &TransferOffer,
    preferences: &TransferPreferences,
    preflight: &P,
    accept: &A,
) -> Result<IncomingOfferOutcome, AppError>
where
    P: Fn(&std::path::Path, u64, u64) -> Result<(), AppError>,
    A: Fn(&Storage, &TransferRecord) -> Result<bool, AppError>,
{
    let now = now_rfc3339();
    let mut transfer = TransferRecord {
        transfer_id: offer.transfer_id.clone(),
        peer_id: peer_id.to_string(),
        direction: Direction::Incoming,
        kind: offer.kind,
        file_name: offer.file_name.clone(),
        file_size: offer.file_size,
        mime_type: offer.mime_type.clone(),
        sha256: offer.sha256.clone(),
        local_path: None,
        destination_reserved: false,
        reservation_token: None,
        transfer_protocol: offer.transfer_protocol,
        chunk_size: offer.chunk_size,
        chunk_count: offer.chunk_count,
        manifest_sha256: offer.manifest_sha256.clone(),
        partial_path: None,
        source_modified_ns: None,
        send_claimed: false,
        transferred_bytes: 0,
        status: TransferStatus::AwaitingAcceptance,
        error: None,
        created_at: now.clone(),
        updated_at: now,
    };
    storage.upsert_transfer(&transfer)?;
    if !preferences.auto_receive_files {
        return Ok(IncomingOfferOutcome {
            transfer,
            automatic_receive_error: None,
            transfer_decision: None,
        });
    }

    if let Err(error) = storage.drain_incoming_cleanup_before_acceptance(&transfer.transfer_id) {
        let message = error.to_string();
        transfer = storage
            .get_transfer(&transfer.transfer_id)?
            .ok_or_else(|| AppError::Storage("自动接收清理等待记录不存在".to_string()))?;
        return Ok(IncomingOfferOutcome {
            transfer,
            automatic_receive_error: Some(message),
            transfer_decision: None,
        });
    }

    let (path, reservation_token) = match automatic_receive_path_with_preflight(
        preferences,
        &offer.file_name,
        &offer.transfer_id,
        offer.transfer_protocol,
        offer.file_size,
        preflight,
    ) {
        Ok(prepared) => prepared,
        Err(error) => {
            let message = error.to_string();
            transfer.error = Some(message.clone());
            transfer.updated_at = now_rfc3339();
            storage.upsert_transfer(&transfer)?;
            return Ok(IncomingOfferOutcome {
                transfer,
                automatic_receive_error: Some(message),
                transfer_decision: None,
            });
        }
    };

    let mut accepted = transfer.clone();
    accepted.local_path = Some(path.to_string_lossy().into_owned());
    accepted.destination_reserved = true;
    accepted.reservation_token = Some(reservation_token.clone());
    accepted.status = TransferStatus::Transferring;
    accepted.updated_at = now_rfc3339();
    let setup_result = accept(storage, &accepted);
    match setup_result {
        Ok(true) => {
            let accepted = storage
                .get_transfer(&accepted.transfer_id)?
                .ok_or_else(|| AppError::Storage("自动接收记录在确认后消失".to_string()))?;
            Ok(IncomingOfferOutcome {
                transfer: accepted,
                automatic_receive_error: None,
                transfer_decision: Some(true),
            })
        }
        Ok(false) => {
            let error = AppError::InvalidInput(
                "自动接收准备期间传输状态已变化，请手动选择保存位置".to_string(),
            );
            compensate_automatic_acceptance_setup(
                storage,
                transfer,
                &path,
                &reservation_token,
                error,
            )
        }
        Err(error) => compensate_automatic_acceptance_setup(
            storage,
            transfer,
            &path,
            &reservation_token,
            error,
        ),
    }
}

fn compensate_automatic_acceptance_setup(
    storage: &Storage,
    awaiting: TransferRecord,
    destination: &std::path::Path,
    reservation_token: &str,
    error: AppError,
) -> Result<IncomingOfferOutcome, AppError> {
    let message = format!("自动接收准备失败，请手动选择保存位置：{error}");
    let current = storage
        .get_transfer(&awaiting.transfer_id)?
        .unwrap_or(awaiting);
    let mut ownership = current;
    if ownership.local_path.is_none() {
        ownership.local_path = Some(destination.to_string_lossy().into_owned());
    }
    if ownership.reservation_token.is_none() {
        ownership.reservation_token = Some(reservation_token.to_string());
    }
    ownership.destination_reserved = true;
    if !storage.persist_incoming_acceptance_fallback(&ownership, &message)? {
        return Err(AppError::Storage(
            "自动接收回退期间传输状态已变化，请刷新后重试".to_string(),
        ));
    }
    let transfer = storage
        .get_transfer(&ownership.transfer_id)?
        .ok_or_else(|| AppError::Storage("自动接收回退记录不存在".to_string()))?;
    Ok(IncomingOfferOutcome {
        transfer,
        automatic_receive_error: Some(message),
        transfer_decision: None,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        collections::HashMap,
        fs,
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use tokio::sync::{mpsc, oneshot, watch};

    use super::{
        AcceptedSubmissionOutcome, NetworkCommand, NetworkEvent, NetworkRuntime, PendingAction,
        PendingRequestId, TestControlTransport, automatic_receive_path,
        finalize_accepted_transfer_submission, persist_incoming_offer_with_preflight,
        persist_incoming_offer_with_preflight_and_accept, validate_transfer_offer,
    };
    use crate::{
        domain::{
            Direction, Friend, FriendRequest, FriendRequestStatus, LocalProfile, PROTOCOL_VERSION,
            Platform, TransferKind, TransferPreferences, TransferStatus, now_rfc3339,
        },
        error::AppError,
        protocol::TransferOffer,
        receive_paths::{preflight_receive_directory, reservation_is_owned, reserve_receive_path},
        storage::{IncomingAcceptancePhase, Storage},
        transfer_policy::{TRANSFER_CHUNK_BYTES, TransferProtocol},
        volume_preflight::{VolumeSnapshot, validate_volume},
    };

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    const DESTINATION_RESERVE_BYTES: u64 = 64 * MIB;

    fn automatic_fixture(name: &str) -> (PathBuf, Storage, TransferPreferences) {
        let directory = std::env::temp_dir().join(format!(
            "weline-localnet-automatic-preflight-{name}-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&directory).expect("create automatic preflight fixture");
        let storage = Storage::open(&directory.join("localnet.sqlite3"))
            .expect("open automatic preflight storage");
        let preferences = TransferPreferences {
            auto_receive_files: true,
            receive_directory: directory.to_string_lossy().into_owned(),
        };
        (directory, storage, preferences)
    }

    fn prepare_manual_runtime_acceptance(
        directory: &std::path::Path,
        storage: &Storage,
        peer_id: &str,
        offer: &TransferOffer,
    ) -> crate::domain::TransferRecord {
        let preferences = TransferPreferences {
            auto_receive_files: false,
            receive_directory: directory.to_string_lossy().into_owned(),
        };
        let awaiting = persist_incoming_offer_with_preflight(
            storage,
            peer_id,
            offer,
            &preferences,
            &|_, _, _| Ok(()),
        )
        .expect("persist manual awaiting transfer")
        .transfer;
        let destination = directory.join(format!("manual-{}", offer.file_name));
        let token = format!("manual-token-{}", offer.transfer_id);
        reserve_receive_path(&destination, &offer.transfer_id, &token)
            .expect("reserve manual destination");
        let mut accepted = awaiting;
        accepted.local_path = Some(destination.to_string_lossy().into_owned());
        accepted.destination_reserved = true;
        accepted.reservation_token = Some(token);
        accepted.status = TransferStatus::Transferring;
        assert!(
            storage
                .try_accept_incoming_transfer(&accepted)
                .expect("persist manual acceptance")
        );
        storage
            .get_transfer(&offer.transfer_id)
            .expect("reload manual acceptance")
            .expect("manual acceptance exists")
    }

    fn production_test_runtime(
        storage: Storage,
        directory: &std::path::Path,
        remote_peer: libp2p::PeerId,
    ) -> NetworkRuntime {
        let local_peer = deterministic_peer_id(1);
        let (command_sender, receiver) = mpsc::channel(1);
        let (_discovery_sender, discovery_receiver) = mpsc::channel(1);
        let (listen_port_sender, _listen_port_receiver) = watch::channel(None);
        let mut active_connections = HashMap::new();
        active_connections.insert(remote_peer, 1);
        NetworkRuntime {
            local_profile: LocalProfile {
                peer_id: local_peer.to_string(),
                nickname: "Runtime Test".to_string(),
                platform: Platform::current(),
                protocol_version: PROTOCOL_VERSION,
            },
            storage,
            default_receive_directory: directory.to_path_buf(),
            receiver,
            command_sender: command_sender.downgrade(),
            discovery_receiver,
            listen_port_sender,
            pending: HashMap::new(),
            mdns_addresses: HashMap::new(),
            beacon_addresses: HashMap::new(),
            active_connections,
            friend_request_times: HashMap::new(),
            mdns_enabled: false,
            test_control_transport: Some(TestControlTransport::default()),
            test_events: Mutex::new(Vec::new()),
            test_order: Mutex::new(Vec::new()),
        }
    }

    fn deterministic_peer_id(marker: u8) -> libp2p::PeerId {
        let mut bytes = vec![0, 36, 8, 1, 18, 32];
        bytes.extend([marker; 32]);
        libp2p::PeerId::from_bytes(&bytes).expect("valid deterministic ed25519 peer id")
    }

    fn add_runtime_friend(storage: &Storage, peer_id: &str) {
        let now = now_rfc3339();
        let request = FriendRequest {
            request_id: uuid::Uuid::now_v7().to_string(),
            peer_id: peer_id.to_string(),
            nickname: "Remote Test".to_string(),
            direction: Direction::Incoming,
            status: FriendRequestStatus::Pending,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        storage
            .put_friend_request(&request)
            .expect("put friend request");
        storage
            .resolve_friend_request(
                &request.request_id,
                FriendRequestStatus::Accepted,
                Some(&Friend {
                    peer_id: peer_id.to_string(),
                    nickname: "Remote Test".to_string(),
                    platform: Platform::current(),
                    online: true,
                    added_at: now.clone(),
                    last_seen: now.clone(),
                }),
                &now,
            )
            .expect("accept runtime friend");
    }

    fn reprepare_manual_runtime_acceptance(
        directory: &std::path::Path,
        storage: &Storage,
        transfer_id: &str,
    ) -> crate::domain::TransferRecord {
        storage
            .drain_incoming_cleanup_before_acceptance(transfer_id)
            .expect("drain prior exact cleanup");
        let mut transfer = storage
            .get_transfer(transfer_id)
            .expect("reload transfer for reacceptance")
            .expect("transfer exists for reacceptance");
        let token = uuid::Uuid::now_v7().to_string();
        let destination = directory.join(format!("reaccept-{token}.bin"));
        reserve_receive_path(&destination, transfer_id, &token)
            .expect("reserve reacceptance destination");
        transfer.local_path = Some(destination.to_string_lossy().into_owned());
        transfer.destination_reserved = true;
        transfer.reservation_token = Some(token);
        transfer.status = TransferStatus::Transferring;
        transfer.error = None;
        assert!(
            storage
                .try_accept_incoming_transfer(&transfer)
                .expect("persist reacceptance")
        );
        storage
            .get_transfer(transfer_id)
            .expect("reload reacceptance")
            .expect("reacceptance exists")
    }

    fn transfer_completion() -> (
        super::TransferDecisionCompletion,
        oneshot::Receiver<Result<(), String>>,
    ) {
        let (sender, receiver) = oneshot::channel();
        (Arc::new(Mutex::new(Some(sender))), receiver)
    }

    #[test]
    fn production_manual_handler_registers_request_before_event_and_completion() {
        let (directory, storage, _) = automatic_fixture("production-manual-success");
        let remote_peer = deterministic_peer_id(2);
        let offer = v2_offer();
        prepare_manual_runtime_acceptance(&directory, &storage, &remote_peer.to_string(), &offer);
        let mut runtime = production_test_runtime(storage.clone(), &directory, remote_peer);
        let (completion, mut completed) = transfer_completion();

        runtime
            .handle_command(NetworkCommand::ResolveTransfer {
                peer_id: remote_peer.to_string(),
                transfer_id: offer.transfer_id.clone(),
                accepted: true,
                completion: Some(completion),
            })
            .expect("actual manual handler submits acceptance");

        let transport = runtime.test_control_transport.as_ref().unwrap();
        assert_eq!(transport.requests.len(), 1);
        assert!(matches!(
            transport.requests.first(),
            Some((peer, crate::protocol::ControlRequest::TransferDecision {
                transfer_id,
                accepted: true,
            })) if *peer == remote_peer && transfer_id == &offer.transfer_id
        ));
        assert!(matches!(
            runtime.pending.get(&PendingRequestId::Test(1)),
            Some(PendingAction::TransferDecision { accepted: true, .. })
        ));
        assert_eq!(completed.try_recv(), Ok(Ok(())));
        assert_eq!(
            *runtime.test_order.lock().unwrap(),
            vec!["send_request", "pending_registered", "event", "completion"]
        );
        let events = runtime.test_events.lock().unwrap();
        assert!(matches!(
            events.as_slice(),
            [NetworkEvent::TransferUpdated { transfer }]
                if transfer.status == TransferStatus::Transferring
        ));
        drop(events);
        runtime
            .handle_outbound_response(
                remote_peer,
                PendingRequestId::Test(1),
                crate::protocol::ControlResponse::Accepted,
            )
            .expect("accepted response retires the exact pending request");
        assert!(runtime.pending.is_empty());
        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::Transferring
        );
        drop(runtime);
        drop(storage);
        fs::remove_dir_all(directory).expect("remove production manual fixture");
    }

    #[test]
    fn production_manual_handler_local_validation_failure_sends_nothing_and_completes_error() {
        let (directory, storage, mut preferences) =
            automatic_fixture("production-manual-local-failure");
        preferences.auto_receive_files = false;
        let remote_peer = deterministic_peer_id(3);
        let offer = v2_offer();
        persist_incoming_offer_with_preflight(
            &storage,
            &remote_peer.to_string(),
            &offer,
            &preferences,
            &|_, _, _| Ok(()),
        )
        .expect("persist awaiting transfer");
        let mut runtime = production_test_runtime(storage.clone(), &directory, remote_peer);
        let (completion, mut completed) = transfer_completion();
        let command = NetworkCommand::ResolveTransfer {
            peer_id: remote_peer.to_string(),
            transfer_id: offer.transfer_id.clone(),
            accepted: true,
            completion: Some(completion),
        };

        let error = runtime
            .handle_command(command.clone())
            .expect_err("actual handler rejects unprepared acceptance");
        runtime
            .handle_command_failure(&command, &error)
            .expect("actual failure handler completes command");

        assert!(
            runtime
                .test_control_transport
                .as_ref()
                .unwrap()
                .requests
                .is_empty()
        );
        assert!(completed.try_recv().unwrap().is_err());
        assert!(runtime.pending.is_empty());
        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::AwaitingAcceptance
        );
        drop(runtime);
        drop(storage);
        fs::remove_dir_all(directory).expect("remove local failure fixture");
    }

    #[test]
    fn production_manual_handler_submission_failure_rolls_back_and_emits_corrected_state() {
        let (directory, storage, _) = automatic_fixture("production-manual-send-failure");
        let remote_peer = deterministic_peer_id(4);
        let offer = v2_offer();
        prepare_manual_runtime_acceptance(&directory, &storage, &remote_peer.to_string(), &offer);
        let mut runtime = production_test_runtime(storage.clone(), &directory, remote_peer);
        runtime.test_control_transport.as_mut().unwrap().fail_next =
            Some("injected send_request failure".to_string());
        let (completion, mut completed) = transfer_completion();
        let command = NetworkCommand::ResolveTransfer {
            peer_id: remote_peer.to_string(),
            transfer_id: offer.transfer_id.clone(),
            accepted: true,
            completion: Some(completion),
        };

        let error = runtime
            .handle_command(command.clone())
            .expect_err("actual send injection fails");
        runtime
            .handle_command_failure(&command, &error)
            .expect("actual failure handler compensates");

        assert_eq!(
            runtime
                .test_control_transport
                .as_ref()
                .unwrap()
                .requests
                .len(),
            1
        );
        assert!(runtime.pending.is_empty());
        assert!(completed.try_recv().unwrap().is_err());
        let reverted = storage.get_transfer(&offer.transfer_id).unwrap().unwrap();
        assert_eq!(reverted.status, TransferStatus::AwaitingAcceptance);
        let events = runtime.test_events.lock().unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            NetworkEvent::TransferUpdated { transfer }
                if transfer.status == TransferStatus::AwaitingAcceptance
        )));
        drop(events);
        drop(runtime);
        drop(storage);
        fs::remove_dir_all(directory).expect("remove production send failure fixture");
    }

    #[test]
    fn production_automatic_offer_handler_submits_once_after_pending_registration() {
        let (directory, storage, preferences) = automatic_fixture("production-auto-success");
        storage
            .save_transfer_preferences(&preferences)
            .expect("enable automatic receive");
        let remote_peer = deterministic_peer_id(5);
        add_runtime_friend(&storage, &remote_peer.to_string());
        let offer = v2_offer();
        let mut runtime = production_test_runtime(storage.clone(), &directory, remote_peer);

        let response = runtime
            .handle_inbound_request(
                remote_peer,
                crate::protocol::ControlRequest::TransferOffer {
                    offer: offer.clone(),
                },
            )
            .expect("actual automatic offer handler accepts");

        assert!(matches!(
            response,
            crate::protocol::ControlResponse::Accepted
        ));
        assert_eq!(
            runtime
                .test_control_transport
                .as_ref()
                .unwrap()
                .requests
                .len(),
            1
        );
        assert!(matches!(
            runtime.pending.get(&PendingRequestId::Test(1)),
            Some(PendingAction::TransferDecision { accepted: true, .. })
        ));
        assert_eq!(
            *runtime.test_order.lock().unwrap(),
            vec!["send_request", "pending_registered", "event"]
        );
        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::Transferring
        );
        drop(runtime);
        drop(storage);
        fs::remove_dir_all(directory).expect("remove automatic success fixture");
    }

    #[test]
    fn production_automatic_offer_submission_failure_emits_only_durable_fallback() {
        let (directory, storage, preferences) = automatic_fixture("production-auto-send-failure");
        storage
            .save_transfer_preferences(&preferences)
            .expect("enable automatic receive");
        let remote_peer = deterministic_peer_id(6);
        add_runtime_friend(&storage, &remote_peer.to_string());
        let offer = v2_offer();
        let mut runtime = production_test_runtime(storage.clone(), &directory, remote_peer);
        runtime.test_control_transport.as_mut().unwrap().fail_next =
            Some("automatic send_request injection".to_string());

        let response = runtime
            .handle_inbound_request(
                remote_peer,
                crate::protocol::ControlRequest::TransferOffer {
                    offer: offer.clone(),
                },
            )
            .expect("offer response remains accepted for manual fallback");

        assert!(matches!(
            response,
            crate::protocol::ControlResponse::Accepted
        ));
        assert!(runtime.pending.is_empty());
        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::AwaitingAcceptance
        );
        let events = runtime.test_events.lock().unwrap();
        assert!(!events.iter().any(|event| matches!(
            event,
            NetworkEvent::TransferUpdated { transfer }
                if transfer.status == TransferStatus::Transferring
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            NetworkEvent::TransferUpdated { transfer }
                if transfer.status == TransferStatus::AwaitingAcceptance
        )));
        drop(events);
        drop(runtime);
        drop(storage);
        fs::remove_dir_all(directory).expect("remove automatic failure fixture");
    }

    #[test]
    fn production_outbound_rejection_rolls_back_the_registered_decision_token() {
        let (directory, storage, _) = automatic_fixture("production-outbound-rejection");
        let remote_peer = deterministic_peer_id(7);
        let offer = v2_offer();
        prepare_manual_runtime_acceptance(&directory, &storage, &remote_peer.to_string(), &offer);
        let mut runtime = production_test_runtime(storage.clone(), &directory, remote_peer);
        runtime
            .handle_command(NetworkCommand::ResolveTransfer {
                peer_id: remote_peer.to_string(),
                transfer_id: offer.transfer_id.clone(),
                accepted: true,
                completion: None,
            })
            .expect("submit pending acceptance");
        runtime.test_events.lock().unwrap().clear();

        runtime
            .handle_outbound_response(
                remote_peer,
                PendingRequestId::Test(1),
                crate::protocol::ControlResponse::Rejected {
                    code: "remote_rejected".to_string(),
                    message: "remote rejected decision".to_string(),
                },
            )
            .expect("actual response handler compensates rejection");

        assert!(runtime.pending.is_empty());
        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::AwaitingAcceptance
        );
        let events = runtime.test_events.lock().unwrap();
        assert!(matches!(
            events.as_slice(),
            [NetworkEvent::TransferUpdated { transfer }, NetworkEvent::NetworkError { .. }]
                if transfer.status == TransferStatus::AwaitingAcceptance
        ));
        drop(events);
        drop(runtime);
        drop(storage);
        fs::remove_dir_all(directory).expect("remove rejection fixture");
    }

    #[test]
    fn production_start_timeout_and_body_claim_have_one_exact_token_winner() {
        let (directory, storage, _) = automatic_fixture("production-timeout-claim-race");
        let remote_peer = deterministic_peer_id(8);
        let offer = v2_offer();
        let accepted = prepare_manual_runtime_acceptance(
            &directory,
            &storage,
            &remote_peer.to_string(),
            &offer,
        );
        let mut runtime = production_test_runtime(storage.clone(), &directory, remote_peer);
        runtime
            .handle_command(NetworkCommand::ResolveTransfer {
                peer_id: remote_peer.to_string(),
                transfer_id: offer.transfer_id.clone(),
                accepted: true,
                completion: None,
            })
            .expect("submit acceptance before body race");
        let decision_token = match runtime.pending.get(&PendingRequestId::Test(1)) {
            Some(PendingAction::TransferDecision {
                decision_token: Some(token),
                ..
            }) => token.clone(),
            other => panic!("missing exact pending decision: {other:?}"),
        };
        runtime.test_events.lock().unwrap().clear();

        assert!(
            storage
                .try_claim_incoming_transfer(&offer.transfer_id, &accepted.peer_id)
                .expect("body claim wins")
        );
        runtime
            .handle_command(NetworkCommand::ExpireIncomingDecision {
                transfer_id: offer.transfer_id.clone(),
                decision_token,
            })
            .expect("late start timeout loses harmlessly");

        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::Transferring
        );
        assert!(runtime.test_events.lock().unwrap().is_empty());
        assert!(
            storage
                .try_pause_claimed_incoming_transfer(
                    &offer.transfer_id,
                    &accepted.peer_id,
                    "test cleanup",
                )
                .unwrap()
        );
        drop(runtime);
        drop(storage);
        fs::remove_dir_all(directory).expect("remove timeout claim fixture");
    }

    #[test]
    fn production_late_response_cannot_roll_back_a_newer_acceptance() {
        let (directory, storage, _) = automatic_fixture("production-late-response-token");
        let remote_peer = deterministic_peer_id(9);
        let offer = v2_offer();
        prepare_manual_runtime_acceptance(&directory, &storage, &remote_peer.to_string(), &offer);
        let mut runtime = production_test_runtime(storage.clone(), &directory, remote_peer);
        runtime
            .handle_command(NetworkCommand::ResolveTransfer {
                peer_id: remote_peer.to_string(),
                transfer_id: offer.transfer_id.clone(),
                accepted: true,
                completion: None,
            })
            .expect("submit old acceptance");
        let old_token = match runtime.pending.get(&PendingRequestId::Test(1)) {
            Some(PendingAction::TransferDecision {
                decision_token: Some(token),
                ..
            }) => token.clone(),
            other => panic!("missing old decision: {other:?}"),
        };
        runtime
            .handle_command(NetworkCommand::ExpireIncomingDecision {
                transfer_id: offer.transfer_id.clone(),
                decision_token: old_token,
            })
            .expect("old timeout rolls back old decision");
        reprepare_manual_runtime_acceptance(&directory, &storage, &offer.transfer_id);
        runtime
            .handle_command(NetworkCommand::ResolveTransfer {
                peer_id: remote_peer.to_string(),
                transfer_id: offer.transfer_id.clone(),
                accepted: true,
                completion: None,
            })
            .expect("submit newer acceptance");
        let new_token = match runtime.pending.get(&PendingRequestId::Test(2)) {
            Some(PendingAction::TransferDecision {
                decision_token: Some(token),
                ..
            }) => token.clone(),
            other => panic!("missing new decision: {other:?}"),
        };

        runtime
            .handle_outbound_response(
                remote_peer,
                PendingRequestId::Test(1),
                crate::protocol::ControlResponse::Rejected {
                    code: "late_old_response".to_string(),
                    message: "late old rejection".to_string(),
                },
            )
            .expect("late old response loses exact-token CAS");

        assert_eq!(
            storage
                .pending_incoming_decision_token(&offer.transfer_id)
                .unwrap()
                .as_deref(),
            Some(new_token.as_str())
        );
        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::Transferring
        );
        runtime
            .handle_outbound_failure(
                PendingRequestId::Test(2),
                libp2p::request_response::OutboundFailure::ConnectionClosed,
            )
            .expect("new exact outbound failure rolls back newer acceptance");
        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::AwaitingAcceptance
        );
        drop(runtime);
        drop(storage);
        fs::remove_dir_all(directory).expect("remove late response fixture");
    }

    #[test]
    fn runtime_handler_local_validation_failure_submits_zero_requests() {
        let (directory, storage, mut preferences) = automatic_fixture("runtime-zero-send");
        preferences.auto_receive_files = false;
        let offer = v2_offer();
        let awaiting = persist_incoming_offer_with_preflight(
            &storage,
            "runtime-peer",
            &offer,
            &preferences,
            &|_, _, _| Ok(()),
        )
        .expect("persist an unprepared acceptance")
        .transfer;
        let requests = Cell::new(0_u8);

        let error = NetworkRuntime::handle_transfer_decision_submission(
            &storage,
            &awaiting.peer_id,
            &awaiting.transfer_id,
            true,
            || {
                requests.set(requests.get() + 1);
                Ok(1_u8)
            },
        )
        .expect_err("local validation must reject before transport submission");

        assert_eq!(requests.get(), 0);
        assert!(error.to_string().contains("未向对方发送决定"));
        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::AwaitingAcceptance
        );
        assert!(
            storage
                .pending_incoming_decision_token(&offer.transfer_id)
                .unwrap()
                .is_none()
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove zero-send fixture");
    }

    #[test]
    fn runtime_handler_manual_send_failure_durably_rolls_back_without_active_state() {
        let (directory, storage, _) = automatic_fixture("runtime-manual-send-failure");
        let offer = v2_offer();
        let accepted =
            prepare_manual_runtime_acceptance(&directory, &storage, "runtime-peer", &offer);
        let requests = Cell::new(0_u8);

        let error = NetworkRuntime::handle_transfer_decision_submission(
            &storage,
            &accepted.peer_id,
            &accepted.transfer_id,
            true,
            || {
                requests.set(requests.get() + 1);
                Err::<u8, _>(AppError::Network(
                    "injected send_request failure".to_string(),
                ))
            },
        )
        .expect_err("send_request failure must durably compensate the manual acceptance");

        assert_eq!(requests.get(), 1);
        assert!(error.to_string().contains("send_request"));
        let reverted = storage.get_transfer(&offer.transfer_id).unwrap().unwrap();
        assert_eq!(reverted.status, TransferStatus::AwaitingAcceptance);
        assert!(
            reverted
                .error
                .as_deref()
                .is_some_and(|message| message.contains("send_request"))
        );
        assert!(
            storage
                .pending_incoming_decision_token(&offer.transfer_id)
                .unwrap()
                .is_none()
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove manual send failure fixture");
    }

    #[test]
    fn runtime_handler_automatic_send_failure_durably_falls_back_without_active_state() {
        let (directory, storage, preferences) = automatic_fixture("runtime-auto-send-failure");
        let offer = v2_offer();
        let accepted = persist_incoming_offer_with_preflight(
            &storage,
            "runtime-peer",
            &offer,
            &preferences,
            &|_, _, _| Ok(()),
        )
        .expect("prepare automatic acceptance")
        .transfer;
        let requests = Cell::new(0_u8);

        let error = NetworkRuntime::handle_transfer_decision_submission(
            &storage,
            &accepted.peer_id,
            &accepted.transfer_id,
            true,
            || {
                requests.set(requests.get() + 1);
                Err::<u8, _>(AppError::Network(
                    "automatic send_request failed".to_string(),
                ))
            },
        )
        .expect_err("automatic transport failure must revert the exact pending action");

        assert_eq!(requests.get(), 1);
        assert!(error.to_string().contains("send_request"));
        let reverted = storage.get_transfer(&offer.transfer_id).unwrap().unwrap();
        assert_eq!(reverted.status, TransferStatus::AwaitingAcceptance);
        assert!(
            storage
                .pending_incoming_decision_token(&offer.transfer_id)
                .unwrap()
                .is_none()
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove automatic send failure fixture");
    }

    #[test]
    fn runtime_handler_automatic_success_submits_once_and_timeout_races_are_token_scoped() {
        let (directory, storage, preferences) = automatic_fixture("runtime-auto-success-races");
        let offer = v2_offer();
        let accepted = persist_incoming_offer_with_preflight(
            &storage,
            "runtime-peer",
            &offer,
            &preferences,
            &|_, _, _| Ok(()),
        )
        .expect("prepare automatic acceptance")
        .transfer;
        let requests = Cell::new(0_u8);

        let submitted = NetworkRuntime::handle_transfer_decision_submission(
            &storage,
            &accepted.peer_id,
            &accepted.transfer_id,
            true,
            || {
                requests.set(requests.get() + 1);
                Ok(41_u8)
            },
        )
        .expect("automatic acceptance submits after durable preparation");

        assert_eq!(requests.get(), 1);
        assert_eq!(submitted.request_id, 41);
        let token = submitted.decision_token.expect("durable pending token");
        assert_eq!(
            storage
                .pending_incoming_decision_token(&offer.transfer_id)
                .unwrap()
                .as_deref(),
            Some(token.as_str())
        );
        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::Transferring
        );

        let duplicate = NetworkRuntime::handle_transfer_decision_submission(
            &storage,
            &accepted.peer_id,
            &accepted.transfer_id,
            true,
            || {
                requests.set(requests.get() + 1);
                Ok(42_u8)
            },
        )
        .expect_err("a durable pending action prevents a second decision submission");
        assert!(duplicate.to_string().contains("未重复发送决定"));
        assert_eq!(requests.get(), 1);
        assert_eq!(
            storage
                .pending_incoming_decision_token(&offer.transfer_id)
                .unwrap()
                .as_deref(),
            Some(token.as_str())
        );

        assert!(
            storage
                .try_claim_incoming_transfer(&offer.transfer_id, &accepted.peer_id)
                .expect("body claim wins against timeout")
        );
        assert!(
            storage
                .pending_incoming_decision_token(&offer.transfer_id)
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .rollback_pending_incoming_decision(
                    &offer.transfer_id,
                    &accepted.peer_id,
                    &token,
                    "late timeout must lose",
                )
                .expect("late timeout is harmless")
                .is_none()
        );
        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::Transferring
        );
        assert!(
            storage
                .try_pause_claimed_incoming_transfer(
                    &offer.transfer_id,
                    &accepted.peer_id,
                    "test cleanup",
                )
                .expect("pause claimed fixture")
        );
        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    &offer.transfer_id,
                    &accepted.peer_id,
                    accepted.transfer_protocol,
                    "test cleanup",
                )
                .expect("cancel fixture")
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove automatic success fixture");
    }

    #[test]
    fn runtime_handler_timeout_then_late_response_rolls_back_only_once() {
        let (directory, storage, _) = automatic_fixture("runtime-timeout-response-race");
        let offer = v2_offer();
        let accepted =
            prepare_manual_runtime_acceptance(&directory, &storage, "runtime-peer", &offer);
        let submitted = NetworkRuntime::handle_transfer_decision_submission(
            &storage,
            &accepted.peer_id,
            &accepted.transfer_id,
            true,
            || Ok(43_u8),
        )
        .expect("submit pending acceptance");
        let token = submitted.decision_token.expect("pending token");

        let timed_out = storage
            .rollback_pending_incoming_decision(
                &offer.transfer_id,
                &accepted.peer_id,
                &token,
                "timeout wins",
            )
            .expect("timeout rollback")
            .expect("timeout changes the transfer");
        assert_eq!(timed_out.status, TransferStatus::AwaitingAcceptance);
        assert!(
            storage
                .rollback_pending_incoming_decision(
                    &offer.transfer_id,
                    &accepted.peer_id,
                    &token,
                    "late rejected response",
                )
                .expect("late response is harmless")
                .is_none()
        );
        assert_eq!(
            storage
                .get_transfer(&offer.transfer_id)
                .unwrap()
                .unwrap()
                .status,
            TransferStatus::AwaitingAcceptance
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove timeout race fixture");
    }

    fn large_v2_offer() -> TransferOffer {
        let mut offer = v2_offer();
        offer.transfer_id = uuid::Uuid::now_v7().to_string();
        offer.file_name = "archive.bin".to_string();
        offer.file_size = 5 * GIB;
        offer.chunk_count =
            u32::try_from(offer.file_size.div_ceil(u64::from(TRANSFER_CHUNK_BYTES)))
                .expect("large offer chunk count");
        offer
    }

    fn large_v1_offer() -> TransferOffer {
        let mut offer = large_v2_offer();
        offer.file_size = GIB;
        offer.transfer_protocol = TransferProtocol::LegacyV1 as u8;
        offer.chunk_size = 0;
        offer.chunk_count = 0;
        offer.manifest_sha256 = None;
        offer
    }

    fn assert_automatic_fallback(storage: &Storage, transfer_id: &str, expected_error: &str) {
        let stored = storage
            .get_transfer(transfer_id)
            .expect("reload automatic fallback")
            .expect("automatic fallback transfer exists");
        assert_eq!(stored.status, TransferStatus::AwaitingAcceptance);
        assert!(!stored.destination_reserved);
        assert!(stored.reservation_token.is_none());
        assert!(stored.local_path.is_none());
        assert!(stored.partial_path.is_none());
        assert!(
            stored
                .error
                .as_deref()
                .is_some_and(|error| error.contains(expected_error))
        );
    }

    fn v2_offer() -> TransferOffer {
        TransferOffer {
            transfer_id: "018e6d7d-21ff-7cc7-9fdd-110f5b0d0b11".to_string(),
            kind: TransferKind::File,
            file_name: "report.txt".to_string(),
            file_size: u64::from(TRANSFER_CHUNK_BYTES),
            mime_type: "text/plain".to_string(),
            sha256: "0".repeat(64),
            transfer_protocol: TransferProtocol::ResumableV2 as u8,
            chunk_size: TRANSFER_CHUNK_BYTES,
            chunk_count: 1,
            manifest_sha256: Some("1".repeat(64)),
        }
    }

    #[test]
    fn rejects_untrusted_v2_offer_metadata_before_persistence() {
        let mut offer = v2_offer();
        offer.chunk_size = 1024;

        assert!(validate_transfer_offer(&offer).is_err());

        offer.chunk_size = TRANSFER_CHUNK_BYTES;
        offer.manifest_sha256 = None;
        assert!(validate_transfer_offer(&offer).is_err());
    }

    #[test]
    fn rejects_legacy_offers_with_v2_metadata_shape() {
        let mut offer = v2_offer();
        offer.transfer_protocol = TransferProtocol::LegacyV1 as u8;

        assert!(validate_transfer_offer(&offer).is_err());
    }

    #[test]
    fn automatic_acceptance_insufficient_space_falls_back_without_transfer_decision_or_reservation()
    {
        let (directory, storage, preferences) = automatic_fixture("insufficient");
        let offer = large_v2_offer();
        let snapshot = VolumeSnapshot::known(
            "NTFS",
            offer.file_size + DESTINATION_RESERVE_BYTES - 1,
            None,
        );

        let outcome = persist_incoming_offer_with_preflight(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|_, size, committed| validate_volume(&snapshot, size, committed),
        )
        .expect("preflight failure must persist a manual fallback");

        assert!(outcome.transfer_decision.is_none());
        assert!(
            outcome
                .automatic_receive_error
                .as_deref()
                .is_some_and(|error| error.contains("可用空间不足"))
        );
        assert_automatic_fallback(&storage, &offer.transfer_id, "可用空间不足");
        drop(storage);
        fs::remove_dir_all(directory).expect("remove insufficient automatic fixture");
    }

    #[test]
    fn automatic_acceptance_fat32_falls_back_without_transfer_decision_or_reservation() {
        let (directory, storage, preferences) = automatic_fixture("fat32");
        let offer = large_v2_offer();
        let snapshot = VolumeSnapshot::known("MSDOS", 10 * GIB, Some(4 * GIB - 1));

        let outcome = persist_incoming_offer_with_preflight(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|_, size, committed| validate_volume(&snapshot, size, committed),
        )
        .expect("FAT32 failure must persist a manual fallback");

        assert!(outcome.transfer_decision.is_none());
        assert_automatic_fallback(&storage, &offer.transfer_id, "MSDOS");
        drop(storage);
        fs::remove_dir_all(directory).expect("remove FAT32 automatic fixture");
    }

    #[test]
    fn automatic_acceptance_allows_exact_remaining_plus_64_mib() {
        let (directory, storage, preferences) = automatic_fixture("exact-margin");
        let offer = large_v2_offer();
        let snapshot =
            VolumeSnapshot::known("NTFS", offer.file_size + DESTINATION_RESERVE_BYTES, None);

        let outcome = persist_incoming_offer_with_preflight(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|_, size, committed| validate_volume(&snapshot, size, committed),
        )
        .expect("exact capacity margin must permit automatic acceptance");

        assert_eq!(outcome.transfer_decision, Some(true));
        assert!(outcome.automatic_receive_error.is_none());
        let stored = storage
            .get_transfer(&offer.transfer_id)
            .expect("reload accepted automatic transfer")
            .expect("accepted automatic transfer exists");
        assert_eq!(stored.status, TransferStatus::Transferring);
        assert!(stored.destination_reserved);
        assert!(stored.partial_path.is_some());
        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    &stored.transfer_id,
                    &stored.peer_id,
                    stored.transfer_protocol,
                    "test cleanup",
                )
                .expect("clean accepted automatic fixture")
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove exact automatic fixture");
    }

    #[test]
    fn automatic_acceptance_missing_destination_falls_back_to_manual_action() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-automatic-missing-preflight-{}",
            uuid::Uuid::now_v7()
        ));
        fs::create_dir_all(&fixture).expect("create missing destination fixture root");
        let storage = Storage::open(&fixture.join("localnet.sqlite3"))
            .expect("open missing destination storage");
        let missing = fixture.join("unplugged-volume");
        let preferences = TransferPreferences {
            auto_receive_files: true,
            receive_directory: missing.to_string_lossy().into_owned(),
        };
        let offer = large_v2_offer();

        let outcome = persist_incoming_offer_with_preflight(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &preflight_receive_directory,
        )
        .expect("missing media must persist a manual fallback");

        assert!(outcome.transfer_decision.is_none());
        assert_automatic_fallback(&storage, &offer.transfer_id, "请选择可访问且可写入的目录");
        assert!(!missing.exists());
        drop(storage);
        fs::remove_dir_all(fixture).expect("remove missing automatic fixture");
    }

    #[test]
    fn automatic_acceptance_unwritable_destination_falls_back_to_manual_action() {
        let (directory, storage, preferences) = automatic_fixture("unwritable");
        let offer = large_v2_offer();

        let outcome = persist_incoming_offer_with_preflight(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|target, _, _| {
                Err(AppError::Storage(format!(
                    "无法写入接收目录 {}；请选择可写入的目录后重试",
                    target.display()
                )))
            },
        )
        .expect("unwritable media must persist a manual fallback");

        assert!(outcome.transfer_decision.is_none());
        assert_automatic_fallback(&storage, &offer.transfer_id, "无法写入接收目录");
        drop(storage);
        fs::remove_dir_all(directory).expect("remove unwritable automatic fixture");
    }

    #[test]
    fn automatic_v1_acceptance_preflights_space_and_missing_directory_without_a_decision() {
        for unavailable in ["insufficient", "missing"] {
            let (directory, storage, mut preferences) =
                automatic_fixture(&format!("v1-{unavailable}"));
            let offer = large_v1_offer();
            if unavailable == "missing" {
                preferences.receive_directory = directory
                    .join("missing-volume")
                    .to_string_lossy()
                    .into_owned();
            }
            let snapshot = VolumeSnapshot::known(
                "NTFS",
                offer.file_size + DESTINATION_RESERVE_BYTES - 1,
                None,
            );

            let outcome = persist_incoming_offer_with_preflight(
                &storage,
                "automatic-preflight-peer",
                &offer,
                &preferences,
                &|target, size, committed| {
                    if unavailable == "missing" {
                        preflight_receive_directory(target, size, committed)
                    } else {
                        validate_volume(&snapshot, size, committed)
                    }
                },
            )
            .expect("legacy automatic preflight failure must persist manual fallback");

            assert!(outcome.transfer_decision.is_none());
            let expected = if unavailable == "missing" {
                "请选择可访问且可写入的目录"
            } else {
                "可用空间不足"
            };
            assert_automatic_fallback(&storage, &offer.transfer_id, expected);
            assert!(!directory.join("missing-volume").exists());
            drop(storage);
            fs::remove_dir_all(directory).expect("remove legacy automatic fixture");
        }
    }

    #[test]
    fn automatic_v1_acceptance_allows_exact_remaining_plus_64_mib() {
        let (directory, storage, preferences) = automatic_fixture("v1-exact-margin");
        let offer = large_v1_offer();
        let snapshot =
            VolumeSnapshot::known("NTFS", offer.file_size + DESTINATION_RESERVE_BYTES, None);
        let probes = std::cell::Cell::new(0_u8);

        let outcome = persist_incoming_offer_with_preflight(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|_, size, committed| {
                probes.set(probes.get() + 1);
                validate_volume(&snapshot, size, committed)
            },
        )
        .expect("legacy automatic acceptance must permit the exact safety margin");

        assert_eq!(probes.get(), 1);
        assert_eq!(outcome.transfer_decision, Some(true));
        let accepted = storage
            .get_transfer(&offer.transfer_id)
            .expect("reload legacy automatic transfer")
            .expect("legacy automatic transfer exists");
        assert_eq!(accepted.status, TransferStatus::Transferring);
        assert!(accepted.destination_reserved);
        assert!(
            storage
                .try_cancel_unclaimed_incoming_transfer(
                    &accepted.transfer_id,
                    &accepted.peer_id,
                    accepted.transfer_protocol,
                    "test cleanup",
                )
                .expect("clean legacy automatic fixture")
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove exact legacy automatic fixture");
    }

    #[test]
    fn automatic_setup_failure_after_reservation_compensates_to_manual_fallback() {
        let (directory, storage, preferences) = automatic_fixture("after-reservation-failure");
        let offer = v2_offer();
        let reservation_observed = std::cell::Cell::new(false);

        let outcome = persist_incoming_offer_with_preflight_and_accept(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|_, _, _| Ok(()),
            &|_, accepted| {
                let destination = std::path::Path::new(
                    accepted
                        .local_path
                        .as_deref()
                        .expect("prepared destination"),
                );
                let token = accepted
                    .reservation_token
                    .as_deref()
                    .expect("prepared reservation token");
                reservation_observed.set(
                    reservation_is_owned(destination, &accepted.transfer_id, token)
                        .expect("inspect prepared reservation"),
                );
                Err(AppError::Storage(
                    "injected failure after reservation".to_string(),
                ))
            },
        )
        .expect("post-reservation setup failure must become manual fallback");

        assert!(reservation_observed.get());
        assert!(outcome.transfer_decision.is_none());
        assert_automatic_fallback(&storage, &offer.transfer_id, "after reservation");
        assert!(
            fs::read_dir(&directory)
                .expect("list compensated directory")
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".weline-localnet"))
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove post-reservation fixture");
    }

    #[test]
    fn automatic_fallback_is_durable_before_unavailable_media_cleanup() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-automatic-durable-fallback-{}",
            uuid::Uuid::now_v7()
        ));
        let directory = fixture.join("selected-media");
        fs::create_dir_all(&directory).expect("create selected media fixture");
        let storage = Storage::open(&fixture.join("localnet.sqlite3"))
            .expect("open durable fallback storage");
        let preferences = TransferPreferences {
            auto_receive_files: true,
            receive_directory: directory.to_string_lossy().into_owned(),
        };
        let offer = v2_offer();
        let detached = fixture.join("detached-media");

        let outcome = persist_incoming_offer_with_preflight_and_accept(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|_, _, _| Ok(()),
            &|_, _| {
                fs::rename(&directory, &detached).expect("detach selected media before fallback");
                Err(AppError::Storage(
                    "injected setup failure while media is unavailable".to_string(),
                ))
            },
        )
        .expect("filesystem cleanup failure must not erase the durable manual fallback");

        assert!(outcome.transfer_decision.is_none());
        assert_eq!(outcome.transfer.status, TransferStatus::AwaitingAcceptance);
        assert!(
            outcome
                .transfer
                .error
                .as_deref()
                .is_some_and(|error| error.contains("media is unavailable"))
        );
        drop(storage);

        let storage = Storage::open(&fixture.join("localnet.sqlite3"))
            .expect("reopen database while selected media remains detached");
        let durable = storage
            .get_transfer(&offer.transfer_id)
            .expect("reload durable fallback")
            .expect("fallback transfer exists");
        assert_eq!(durable.status, TransferStatus::AwaitingAcceptance);
        assert!(
            durable
                .error
                .as_deref()
                .is_some_and(|error| error.contains("media is unavailable"))
        );

        drop(storage);
        fs::rename(&detached, &directory).expect("restore selected media");
        let storage = Storage::open(&fixture.join("localnet.sqlite3"))
            .expect("reopen storage to drain deferred cleanup");
        assert!(
            fs::read_dir(&directory)
                .expect("list restored media")
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".weline-localnet"))
        );
        drop(storage);
        fs::remove_dir_all(fixture).expect("remove durable fallback fixture");
    }

    #[test]
    fn automatic_partial_setup_failure_cleans_owned_partial_and_retains_awaiting_row() {
        let (directory, storage, preferences) = automatic_fixture("partial-setup-failure");
        let offer = v2_offer();
        let partial_observed = std::cell::Cell::new(false);

        let outcome = persist_incoming_offer_with_preflight_and_accept(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|_, _, _| Ok(()),
            &|storage, accepted| {
                storage.try_accept_incoming_transfer_with_hook(accepted, &mut |phase| {
                    if phase == IncomingAcceptancePhase::AfterPersistence {
                        partial_observed.set(true);
                        return Err(AppError::Storage(
                            "injected failure during partial persistence".to_string(),
                        ));
                    }
                    Ok(())
                })
            },
        )
        .expect("partial setup failure must become manual fallback");

        assert!(partial_observed.get());
        assert!(outcome.transfer_decision.is_none());
        assert_automatic_fallback(&storage, &offer.transfer_id, "partial persistence");
        assert!(
            fs::read_dir(&directory)
                .expect("list partial compensation directory")
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".weline-localnet"))
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove partial compensation fixture");
    }

    #[test]
    fn automatic_partial_failure_persists_tombstone_before_unavailable_media_cleanup() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-partial-cleanup-pending-{}",
            uuid::Uuid::now_v7()
        ));
        let media = fixture.join("selected-media");
        let detached = fixture.join("detached-media");
        fs::create_dir_all(&media).expect("create selected media");
        let storage = Storage::open(&fixture.join("localnet.sqlite3")).expect("open storage");
        let preferences = TransferPreferences {
            auto_receive_files: true,
            receive_directory: media.to_string_lossy().into_owned(),
        };
        let offer = v2_offer();

        let outcome = persist_incoming_offer_with_preflight_and_accept(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|_, _, _| Ok(()),
            &|storage, accepted| {
                storage.try_accept_incoming_transfer_with_hook(accepted, &mut |phase| {
                    if phase == IncomingAcceptancePhase::AfterPartialSetup {
                        fs::rename(&media, &detached)
                            .expect("detach media after owned partial setup");
                        return Err(AppError::Storage(
                            "injected partial persistence failure".to_string(),
                        ));
                    }
                    Ok(())
                })
            },
        )
        .expect("partial cleanup failure must retain an actionable manual fallback");

        assert_eq!(outcome.transfer.status, TransferStatus::AwaitingAcceptance);
        assert!(outcome.transfer_decision.is_none());
        let pending = storage
            .drain_incoming_cleanup_before_acceptance(&offer.transfer_id)
            .expect_err("detached media retains the exact cleanup tombstone");
        assert!(pending.to_string().contains("cleanup pending"));

        fs::rename(&detached, &media).expect("restore selected media");
        storage
            .drain_incoming_cleanup_before_acceptance(&offer.transfer_id)
            .expect("restored media drains the owned partial and reservation");
        assert!(
            fs::read_dir(&media)
                .expect("list restored media")
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".weline-localnet"))
        );
        drop(storage);
        fs::remove_dir_all(fixture).expect("remove partial cleanup pending fixture");
    }

    #[test]
    fn automatic_failure_after_acceptance_persistence_reverts_exact_owned_state() {
        let (directory, storage, preferences) = automatic_fixture("after-persistence-failure");
        let offer = v2_offer();

        let outcome = persist_incoming_offer_with_preflight_and_accept(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|_, _, _| Ok(()),
            &|storage, accepted| {
                assert!(
                    storage
                        .try_accept_incoming_transfer(accepted)
                        .expect("persist injected accepted state")
                );
                Err(AppError::Storage(
                    "injected decision construction failure".to_string(),
                ))
            },
        )
        .expect("post-persistence failure must compensate exact accepted state");

        assert!(outcome.transfer_decision.is_none());
        assert_automatic_fallback(&storage, &offer.transfer_id, "decision construction");
        assert!(
            fs::read_dir(&directory)
                .expect("list post-persistence compensation directory")
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".weline-localnet"))
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove post-persistence fixture");
    }

    #[test]
    fn manual_command_offline_submission_reverts_v2_without_a_transferring_event() {
        let (directory, storage, preferences) = automatic_fixture("manual-command-offline");
        let offer = v2_offer();
        let prepared = persist_incoming_offer_with_preflight(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|_, _, _| Ok(()),
        )
        .expect("prepare accepted transfer before runtime submission");
        assert_eq!(prepared.transfer.status, TransferStatus::Transferring);

        let outcome = finalize_accepted_transfer_submission(
            &storage,
            &offer.transfer_id,
            Err(AppError::OfflinePeer),
        )
        .expect("offline submission must compensate accepted state");

        let AcceptedSubmissionOutcome::Reverted { transfer, error } = outcome else {
            panic!("offline command must not produce a submitted/transferring outcome");
        };
        assert_eq!(transfer.status, TransferStatus::AwaitingAcceptance);
        assert!(matches!(error, AppError::OfflinePeer));
        assert_automatic_fallback(&storage, &offer.transfer_id, "不在线");
        assert!(
            fs::read_dir(&directory)
                .expect("list offline compensation directory")
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".weline-localnet"))
        );
        drop(storage);
        fs::remove_dir_all(directory).expect("remove manual command offline fixture");
    }

    #[test]
    fn automatic_send_request_failure_reverts_v1_without_a_transferring_event() {
        let (directory, storage, preferences) = automatic_fixture("automatic-send-failure");
        let offer = large_v1_offer();
        let prepared = persist_incoming_offer_with_preflight(
            &storage,
            "automatic-preflight-peer",
            &offer,
            &preferences,
            &|_, _, _| Ok(()),
        )
        .expect("prepare automatic legacy acceptance");
        assert_eq!(prepared.transfer.status, TransferStatus::Transferring);

        let outcome = finalize_accepted_transfer_submission(
            &storage,
            &offer.transfer_id,
            Err(AppError::Network(
                "injected send_request failure".to_string(),
            )),
        )
        .expect("automatic send failure must compensate accepted state");

        let AcceptedSubmissionOutcome::Reverted { transfer, error } = outcome else {
            panic!("failed automatic send must not produce a transferring event payload");
        };
        assert_eq!(transfer.status, TransferStatus::AwaitingAcceptance);
        assert!(error.to_string().contains("send_request"));
        assert_automatic_fallback(&storage, &offer.transfer_id, "send_request");
        drop(storage);
        fs::remove_dir_all(directory).expect("remove automatic send failure fixture");
    }

    #[test]
    fn automatic_v2_acceptance_never_recreates_a_missing_selected_directory() {
        let fixture = std::env::temp_dir().join(format!(
            "weline-localnet-auto-v2-missing-directory-{}",
            uuid::Uuid::now_v7()
        ));
        let missing = fixture.join("unplugged-volume");
        let preferences = TransferPreferences {
            auto_receive_files: true,
            receive_directory: missing.to_string_lossy().into_owned(),
        };

        let error = automatic_receive_path(
            &preferences,
            "report.bin",
            "transfer-one",
            TransferProtocol::ResumableV2 as u8,
            u64::from(TRANSFER_CHUNK_BYTES),
        )
        .expect_err("missing selected media must reject automatic v2 acceptance");

        assert_eq!(error.code(), "storage_error");
        assert!(error.to_string().contains("请选择可访问且可写入的目录"));
        assert!(!fixture.exists());
        let _ = fs::remove_dir_all(fixture);
    }
}
