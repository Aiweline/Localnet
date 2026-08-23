use std::time::Duration;

use libp2p::{
    PeerId, StreamProtocol, identify, identity::PublicKey, mdns, request_response,
    swarm::NetworkBehaviour,
};

use crate::{
    error::AppError,
    protocol::{CONTROL_PROTOCOL, ControlRequest, ControlResponse},
};

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "LocalnetBehaviourEvent")]
pub struct LocalnetBehaviour {
    pub mdns: mdns::tokio::Behaviour,
    pub identify: identify::Behaviour,
    pub control: request_response::cbor::Behaviour<ControlRequest, ControlResponse>,
    pub stream: libp2p_stream::Behaviour,
}

impl LocalnetBehaviour {
    pub fn new(peer_id: PeerId, public_key: PublicKey) -> Result<Self, AppError> {
        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
            .map_err(|error| AppError::Network(format!("无法启动局域网自动发现：{error}")))?;
        let identify = identify::Behaviour::new(
            identify::Config::new("/localnet/identify/1".to_string(), public_key)
                .with_agent_version(format!("Localnet/{}", env!("CARGO_PKG_VERSION")))
                .with_interval(Duration::from_secs(60)),
        );
        let codec = request_response::cbor::codec::Codec::default()
            .set_request_size_maximum(128 * 1024)
            .set_response_size_maximum(128 * 1024);
        let control = request_response::cbor::Behaviour::with_codec(
            codec,
            [(
                StreamProtocol::new(CONTROL_PROTOCOL),
                request_response::ProtocolSupport::Full,
            )],
            request_response::Config::default()
                .with_request_timeout(Duration::from_secs(15))
                .with_max_concurrent_streams(64),
        );
        Ok(Self {
            mdns,
            identify,
            control,
            stream: libp2p_stream::Behaviour::new(),
        })
    }
}

#[derive(Debug)]
pub enum LocalnetBehaviourEvent {
    Mdns(mdns::Event),
    Identify(identify::Event),
    Control(request_response::Event<ControlRequest, ControlResponse>),
    Stream(()),
}

impl From<mdns::Event> for LocalnetBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        Self::Mdns(event)
    }
}

impl From<identify::Event> for LocalnetBehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(event)
    }
}

impl From<request_response::Event<ControlRequest, ControlResponse>> for LocalnetBehaviourEvent {
    fn from(event: request_response::Event<ControlRequest, ControlResponse>) -> Self {
        Self::Control(event)
    }
}

impl From<()> for LocalnetBehaviourEvent {
    fn from(event: ()) -> Self {
        Self::Stream(event)
    }
}
