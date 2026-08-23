use std::{
    collections::HashSet,
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use if_addrs::{IfAddr, get_if_addrs};
use libp2p::{Multiaddr, PeerId, multiaddr::Protocol};
use socket2::{Domain, Protocol as SocketProtocol, SockAddr, Socket, Type};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, watch},
    time,
};

const DISCOVERY_MAGIC: &str = "LOCALNET";
const DISCOVERY_VERSION: u8 = 1;
const DISCOVERY_PORT: u16 = 43_821;
const MAX_BEACON_BYTES: usize = 512;
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(3);
pub(super) const BEACON_LEASE: Duration = Duration::from_secs(12);

#[derive(Debug)]
pub(super) enum DiscoveryEvent {
    PeerHint {
        peer_id: PeerId,
        address: Multiaddr,
        expires_at: Instant,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LanInterface {
    name: String,
    ip: Ipv4Addr,
    broadcast: Ipv4Addr,
}

pub(super) struct DiscoveryService;

impl DiscoveryService {
    pub(super) fn spawn(
        peer_id: PeerId,
        listen_port: watch::Receiver<Option<u16>>,
    ) -> mpsc::Receiver<DiscoveryEvent> {
        let (event_sender, event_receiver) = mpsc::channel(128);

        tauri::async_runtime::spawn(receive_beacons(peer_id, event_sender));
        tauri::async_runtime::spawn(announce_beacons(peer_id, listen_port));

        event_receiver
    }
}

async fn receive_beacons(peer_id: PeerId, sender: mpsc::Sender<DiscoveryEvent>) {
    let socket = loop {
        match bind_receiver() {
            Ok(socket) => break socket,
            Err(error) => {
                tracing::warn!(%error, port = DISCOVERY_PORT, "LAN beacon receiver unavailable; retrying");
                time::sleep(Duration::from_secs(3)).await;
            }
        }
    };
    let mut buffer = [0_u8; MAX_BEACON_BYTES + 1];

    loop {
        let (length, source) = match socket.recv_from(&mut buffer).await {
            Ok(received) => received,
            Err(error) => {
                tracing::debug!(%error, "LAN beacon receive failed");
                time::sleep(Duration::from_millis(250)).await;
                continue;
            }
        };
        if length > MAX_BEACON_BYTES {
            tracing::trace!(length, "oversized LAN beacon ignored");
            continue;
        }
        let SocketAddr::V4(source) = source else {
            continue;
        };
        if !is_private_lan_ip(*source.ip()) {
            tracing::trace!(address = %source.ip(), "non-private LAN beacon ignored");
            continue;
        }
        let Some((remote_peer, listen_port)) = decode_beacon(&buffer[..length]) else {
            tracing::trace!(address = %source.ip(), "malformed LAN beacon ignored");
            continue;
        };
        if remote_peer == peer_id {
            continue;
        }
        let address = Multiaddr::empty()
            .with(Protocol::Ip4(*source.ip()))
            .with(Protocol::Tcp(listen_port))
            .with(Protocol::P2p(remote_peer));
        if sender
            .send(DiscoveryEvent::PeerHint {
                peer_id: remote_peer,
                address,
                expires_at: Instant::now() + BEACON_LEASE,
            })
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn announce_beacons(peer_id: PeerId, listen_port: watch::Receiver<Option<u16>>) {
    time::sleep(startup_jitter(peer_id)).await;
    let mut interval = time::interval(ANNOUNCE_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        let Some(port) = *listen_port.borrow() else {
            continue;
        };
        let payload = encode_beacon(peer_id, port);
        let interfaces = match eligible_interfaces() {
            Ok(interfaces) => interfaces,
            Err(error) => {
                tracing::debug!(%error, "unable to enumerate LAN interfaces");
                continue;
            }
        };
        if interfaces.is_empty() {
            tracing::debug!("no eligible RFC1918 broadcast interface found");
            continue;
        }
        for interface in interfaces {
            if let Err(error) = announce_on_interface(&interface, &payload).await {
                tracing::debug!(
                    interface = %interface.name,
                    address = %interface.ip,
                    %error,
                    "LAN beacon send failed on interface"
                );
            }
        }
    }
}

fn bind_receiver() -> io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(SocketProtocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_broadcast(true)?;
    socket.bind(&SockAddr::from(SocketAddrV4::new(
        Ipv4Addr::UNSPECIFIED,
        DISCOVERY_PORT,
    )))?;
    socket.set_nonblocking(true)?;
    UdpSocket::from_std(socket.into())
}

async fn announce_on_interface(interface: &LanInterface, payload: &[u8]) -> io::Result<()> {
    let socket = UdpSocket::bind(SocketAddrV4::new(interface.ip, 0)).await?;
    socket.set_broadcast(true)?;

    let mut targets = HashSet::from([interface.broadcast]);
    targets.insert(Ipv4Addr::BROADCAST);
    let mut delivered = false;
    let mut last_error = None;
    for target in targets {
        match socket
            .send_to(payload, SocketAddrV4::new(target, DISCOVERY_PORT))
            .await
        {
            Ok(_) => delivered = true,
            Err(error) => last_error = Some(error),
        }
    }
    if delivered {
        Ok(())
    } else {
        Err(last_error.unwrap_or_else(|| io::Error::other("no LAN beacon target accepted")))
    }
}

fn eligible_interfaces() -> io::Result<Vec<LanInterface>> {
    let mut interfaces = HashSet::new();
    for interface in get_if_addrs()? {
        if !interface.is_oper_up()
            || interface.is_loopback()
            || interface.is_link_local()
            || interface.is_p2p()
        {
            continue;
        }
        let IfAddr::V4(address) = interface.addr else {
            continue;
        };
        if !is_private_lan_ip(address.ip) || !(8..=30).contains(&address.prefixlen) {
            continue;
        }
        let broadcast = address
            .broadcast
            .unwrap_or_else(|| subnet_broadcast(address.ip, address.netmask));
        if broadcast == address.ip || broadcast.is_unspecified() {
            continue;
        }
        interfaces.insert(LanInterface {
            name: interface.name,
            ip: address.ip,
            broadcast,
        });
    }
    let mut interfaces: Vec<_> = interfaces.into_iter().collect();
    interfaces.sort_by_key(|interface| (interface.ip, interface.name.clone()));
    Ok(interfaces)
}

fn is_private_lan_ip(ip: Ipv4Addr) -> bool {
    ip.is_private()
        && !ip.is_loopback()
        && !ip.is_link_local()
        && !ip.is_unspecified()
        && !ip.is_broadcast()
}

fn subnet_broadcast(ip: Ipv4Addr, netmask: Ipv4Addr) -> Ipv4Addr {
    Ipv4Addr::from(u32::from(ip) | !u32::from(netmask))
}

fn encode_beacon(peer_id: PeerId, listen_port: u16) -> Vec<u8> {
    format!("{DISCOVERY_MAGIC}|{DISCOVERY_VERSION}|{peer_id}|{listen_port}").into_bytes()
}

fn decode_beacon(payload: &[u8]) -> Option<(PeerId, u16)> {
    if payload.is_empty() || payload.len() > MAX_BEACON_BYTES {
        return None;
    }
    let mut parts = str::from_utf8(payload).ok()?.split('|');
    if parts.next()? != DISCOVERY_MAGIC || parts.next()?.parse::<u8>().ok()? != DISCOVERY_VERSION {
        return None;
    }
    let peer_id = parts.next()?.parse().ok()?;
    let listen_port = parts.next()?.parse::<u16>().ok()?;
    if listen_port == 0 || parts.next().is_some() {
        return None;
    }
    Some((peer_id, listen_port))
}

fn startup_jitter(peer_id: PeerId) -> Duration {
    let peer_entropy = peer_id
        .to_bytes()
        .into_iter()
        .fold(0_u64, |total, byte| total.wrapping_add(u64::from(byte)));
    let clock_entropy = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| u64::from(duration.subsec_millis()));
    Duration::from_millis(100 + (peer_entropy ^ clock_entropy) % 700)
}
