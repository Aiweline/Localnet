use std::{
    collections::{HashMap, HashSet},
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

#[cfg(target_os = "windows")]
mod mdns_compat;

const DISCOVERY_MAGIC: &str = "LOCALNET";
const DISCOVERY_VERSION: u8 = 1;
const DISCOVERY_PROBE_VERSION: u8 = 2;
const DISCOVERY_PORT: u16 = 43_821;
const MAX_BEACON_BYTES: usize = 512;
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(3);
const PROBE_INTERVAL: Duration = Duration::from_secs(12);
const REMEMBERED_PROBE_INTERVAL: Duration = Duration::from_secs(2);
const PROBE_RESPONSE_WINDOW: Duration = Duration::from_millis(900);
const PROBE_RESPONSE_RATE_LIMIT: Duration = Duration::from_millis(500);
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
pub(super) struct LanInterface {
    name: String,
    ip: Ipv4Addr,
    broadcast: Ipv4Addr,
    prefixlen: u8,
}

#[derive(Debug)]
enum DiscoveryPacket {
    Beacon { peer_id: PeerId, listen_port: u16 },
    Probe { peer_id: PeerId, listen_port: u16 },
}

pub(super) struct DiscoveryService;

#[derive(Clone)]
pub(super) struct DiscoveryRefresh {
    generation: watch::Sender<u64>,
}

impl DiscoveryRefresh {
    pub(super) fn new() -> Self {
        let (generation, _) = watch::channel(0);
        Self { generation }
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }

    pub(super) fn trigger(&self) -> u64 {
        let mut triggered = 0;
        self.generation.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
            if *generation == 0 {
                *generation = 1;
            }
            triggered = *generation;
        });
        triggered
    }
}

impl DiscoveryService {
    pub(super) fn spawn(
        peer_id: PeerId,
        listen_port: watch::Receiver<Option<u16>>,
        remembered_targets: watch::Receiver<Vec<Ipv4Addr>>,
    ) -> (mpsc::Receiver<DiscoveryEvent>, DiscoveryRefresh) {
        let (event_sender, event_receiver) = mpsc::channel(128);
        let refresh = DiscoveryRefresh::new();

        tauri::async_runtime::spawn(receive_beacons(
            peer_id,
            listen_port.clone(),
            event_sender.clone(),
        ));
        tauri::async_runtime::spawn(announce_beacons(
            peer_id,
            listen_port.clone(),
            refresh.subscribe(),
        ));
        tauri::async_runtime::spawn(probe_peers(
            peer_id,
            listen_port.clone(),
            event_sender.clone(),
            refresh.subscribe(),
        ));
        tauri::async_runtime::spawn(probe_remembered_peers(
            peer_id,
            listen_port,
            remembered_targets,
            event_sender.clone(),
        ));

        #[cfg(target_os = "windows")]
        mdns_compat::spawn(peer_id, event_sender.clone());

        (event_receiver, refresh)
    }
}

async fn receive_beacons(
    peer_id: PeerId,
    listen_port: watch::Receiver<Option<u16>>,
    sender: mpsc::Sender<DiscoveryEvent>,
) {
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
    let mut probe_responses = HashMap::<Ipv4Addr, Instant>::new();

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
        let Some(packet) = decode_packet(&buffer[..length]) else {
            tracing::trace!(address = %source.ip(), "malformed LAN beacon ignored");
            continue;
        };
        let (remote_peer, remote_port, should_respond) = match packet {
            DiscoveryPacket::Beacon {
                peer_id,
                listen_port,
            } => (peer_id, listen_port, false),
            DiscoveryPacket::Probe {
                peer_id,
                listen_port,
            } => (peer_id, listen_port, true),
        };
        if remote_peer != peer_id
            && emit_peer_hint(&sender, remote_peer, *source.ip(), remote_port)
                .await
                .is_err()
        {
            return;
        }
        if should_respond && remote_peer != peer_id {
            let now = Instant::now();
            let allowed = probe_responses
                .get(source.ip())
                .is_none_or(|last_response| {
                    now.duration_since(*last_response) >= PROBE_RESPONSE_RATE_LIMIT
                });
            if allowed {
                probe_responses.insert(*source.ip(), now);
                let local_port = *listen_port.borrow();
                if let Some(local_port) = local_port {
                    let response = encode_beacon(peer_id, local_port);
                    if let Err(error) = socket.send_to(&response, source).await {
                        tracing::debug!(%error, address = %source, "LAN probe response failed");
                    }
                }
            }
        }
        probe_responses
            .retain(|_, responded_at| Instant::now().duration_since(*responded_at) < BEACON_LEASE);
    }
}

async fn announce_beacons(
    peer_id: PeerId,
    listen_port: watch::Receiver<Option<u16>>,
    mut refresh: watch::Receiver<u64>,
) {
    time::sleep(startup_jitter(peer_id)).await;
    let mut interval = time::interval(ANNOUNCE_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            changed = refresh.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
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

async fn probe_peers(
    peer_id: PeerId,
    listen_port: watch::Receiver<Option<u16>>,
    sender: mpsc::Sender<DiscoveryEvent>,
    mut refresh: watch::Receiver<u64>,
) {
    time::sleep(startup_jitter(peer_id) + Duration::from_millis(250)).await;
    let mut interval = time::interval(PROBE_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            changed = refresh.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
        let Some(port) = *listen_port.borrow() else {
            continue;
        };
        let interfaces = match eligible_interfaces() {
            Ok(interfaces) => interfaces,
            Err(error) => {
                tracing::debug!(%error, "unable to enumerate LAN interfaces for active probe");
                continue;
            }
        };
        let payload = encode_probe(peer_id, port);
        let probes = interfaces
            .iter()
            .map(|interface| probe_on_interface(interface, &payload, peer_id, sender.clone()));
        futures::future::join_all(probes).await;
    }
}

async fn probe_remembered_peers(
    peer_id: PeerId,
    listen_port: watch::Receiver<Option<u16>>,
    remembered_targets: watch::Receiver<Vec<Ipv4Addr>>,
    sender: mpsc::Sender<DiscoveryEvent>,
) {
    let mut interval = time::interval(REMEMBERED_PROBE_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        let Some(port) = *listen_port.borrow() else {
            continue;
        };
        let remembered_targets = remembered_targets
            .borrow()
            .iter()
            .copied()
            .filter(|ip| is_private_lan_ip(*ip))
            .collect::<HashSet<_>>()
            .into_iter()
            .take(256)
            .collect::<Vec<_>>();
        if remembered_targets.is_empty() {
            continue;
        }
        let interfaces = match eligible_interfaces() {
            Ok(interfaces) => interfaces,
            Err(error) => {
                tracing::debug!(%error, "unable to enumerate LAN interfaces for remembered peers");
                continue;
            }
        };
        let payload = encode_probe(peer_id, port);
        let probes = interfaces.iter().filter_map(|interface| {
            let targets = remembered_probe_targets(interface, &remembered_targets);
            (!targets.is_empty()).then(|| {
                probe_targets_on_interface(interface, &payload, peer_id, sender.clone(), targets)
            })
        });
        futures::future::join_all(probes).await;
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

async fn probe_on_interface(
    interface: &LanInterface,
    payload: &[u8],
    local_peer: PeerId,
    sender: mpsc::Sender<DiscoveryEvent>,
) {
    probe_targets_on_interface(
        interface,
        payload,
        local_peer,
        sender,
        unicast_probe_targets(interface),
    )
    .await;
}

async fn probe_targets_on_interface(
    interface: &LanInterface,
    payload: &[u8],
    local_peer: PeerId,
    sender: mpsc::Sender<DiscoveryEvent>,
    targets: Vec<Ipv4Addr>,
) {
    let socket = match UdpSocket::bind(SocketAddrV4::new(interface.ip, 0)).await {
        Ok(socket) => socket,
        Err(error) => {
            tracing::debug!(
                interface = %interface.name,
                address = %interface.ip,
                %error,
                "LAN active probe socket unavailable"
            );
            return;
        }
    };
    tracing::trace!(
        interface = %interface.name,
        address = %interface.ip,
        targets = targets.len(),
        "LAN active probe started"
    );
    for target in targets {
        if let Err(error) = socket
            .send_to(payload, SocketAddrV4::new(target, DISCOVERY_PORT))
            .await
        {
            tracing::trace!(%error, %target, "LAN active probe send failed");
        }
        tokio::task::yield_now().await;
    }

    let deadline = Instant::now() + PROBE_RESPONSE_WINDOW;
    let mut buffer = [0_u8; MAX_BEACON_BYTES + 1];
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        let received = time::timeout(remaining, socket.recv_from(&mut buffer)).await;
        let Ok(Ok((length, SocketAddr::V4(source)))) = received else {
            break;
        };
        if length > MAX_BEACON_BYTES || !is_private_lan_ip(*source.ip()) {
            continue;
        }
        let Some(DiscoveryPacket::Beacon {
            peer_id,
            listen_port,
        }) = decode_packet(&buffer[..length])
        else {
            continue;
        };
        if peer_id == local_peer {
            continue;
        }
        if emit_peer_hint(&sender, peer_id, *source.ip(), listen_port)
            .await
            .is_err()
        {
            return;
        }
    }
}

fn remembered_probe_targets(interface: &LanInterface, targets: &[Ipv4Addr]) -> Vec<Ipv4Addr> {
    let mut targets = targets
        .iter()
        .copied()
        .filter(|target| {
            *target != interface.ip
                && is_private_lan_ip(*target)
                && interface_contains(interface, *target)
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    targets.sort_unstable();
    targets
}

pub(super) fn eligible_interfaces() -> io::Result<Vec<LanInterface>> {
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
            prefixlen: address.prefixlen,
        });
    }
    let mut interfaces: Vec<_> = interfaces.into_iter().collect();
    interfaces.sort_by_key(|interface| {
        (
            interface_priority(&interface.name),
            interface.ip,
            interface.name.clone(),
        )
    });
    Ok(interfaces)
}

pub(super) fn ranked_dial_addresses(
    addresses: impl IntoIterator<Item = Multiaddr>,
    preferred_ip: Option<Ipv4Addr>,
    interfaces: &[LanInterface],
) -> Vec<Multiaddr> {
    let mut addresses = addresses
        .into_iter()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    addresses.sort_by_key(|address| {
        let ip = address.iter().find_map(|protocol| match protocol {
            Protocol::Ip4(ip) => Some(ip),
            _ => None,
        });
        let remembered = match (ip, preferred_ip) {
            (Some(ip), Some(preferred)) if ip == preferred => 0,
            (Some(_), _) => 1,
            (None, _) => 2,
        };
        let interface = ip
            .and_then(|ip| {
                interfaces
                    .iter()
                    .filter(|interface| interface_contains(interface, ip))
                    .map(|interface| interface_priority(&interface.name))
                    .min()
            })
            .unwrap_or(u8::MAX);
        (remembered, interface, ip, address.to_string())
    });
    addresses
}

fn interface_contains(interface: &LanInterface, target: Ipv4Addr) -> bool {
    if !(1..=32).contains(&interface.prefixlen) {
        return false;
    }
    let mask = u32::MAX << (32 - u32::from(interface.prefixlen));
    u32::from(interface.ip) & mask == u32::from(target) & mask
}

fn interface_priority(name: &str) -> u8 {
    let normalized = name.to_ascii_lowercase();
    const VIRTUAL_MARKERS: [&str; 14] = [
        "utun",
        "tun",
        "tap",
        "vmnet",
        "vmware",
        "vethernet",
        "hyper-v",
        "wsl",
        "docker",
        "tailscale",
        "zerotier",
        "vpn",
        "bridge",
        "virtual",
    ];
    if VIRTUAL_MARKERS
        .iter()
        .any(|marker| normalized.contains(marker))
    {
        1
    } else {
        0
    }
}

fn unicast_probe_targets(interface: &LanInterface) -> Vec<Ipv4Addr> {
    let prefixlen = interface.prefixlen.max(24);
    let mask = u32::MAX << (32 - u32::from(prefixlen));
    let network = u32::from(interface.ip) & mask;
    let broadcast = network | !mask;
    ((network + 1)..broadcast)
        .map(Ipv4Addr::from)
        .filter(|candidate| *candidate != interface.ip && is_private_lan_ip(*candidate))
        .collect()
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

fn encode_probe(peer_id: PeerId, listen_port: u16) -> Vec<u8> {
    format!("{DISCOVERY_MAGIC}|{DISCOVERY_PROBE_VERSION}|{peer_id}|{listen_port}").into_bytes()
}

fn decode_packet(payload: &[u8]) -> Option<DiscoveryPacket> {
    if payload.is_empty() || payload.len() > MAX_BEACON_BYTES {
        return None;
    }
    let mut parts = str::from_utf8(payload).ok()?.split('|');
    if parts.next()? != DISCOVERY_MAGIC {
        return None;
    }
    let version = parts.next()?.parse::<u8>().ok()?;
    let peer_id = parts.next()?.parse().ok()?;
    let listen_port = parts.next()?.parse::<u16>().ok()?;
    if listen_port == 0 || parts.next().is_some() {
        return None;
    }
    match version {
        DISCOVERY_VERSION => Some(DiscoveryPacket::Beacon {
            peer_id,
            listen_port,
        }),
        DISCOVERY_PROBE_VERSION => Some(DiscoveryPacket::Probe {
            peer_id,
            listen_port,
        }),
        _ => None,
    }
}

async fn emit_peer_hint(
    sender: &mpsc::Sender<DiscoveryEvent>,
    peer_id: PeerId,
    ip: Ipv4Addr,
    listen_port: u16,
) -> Result<(), mpsc::error::SendError<DiscoveryEvent>> {
    let address = Multiaddr::empty()
        .with(Protocol::Ip4(ip))
        .with(Protocol::Tcp(listen_port))
        .with(Protocol::P2p(peer_id));
    sender
        .send(DiscoveryEvent::PeerHint {
            peer_id,
            address,
            expires_at: Instant::now() + BEACON_LEASE,
        })
        .await
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

#[cfg(test)]
mod tests {
    use super::*;

    fn dial_address(ip: Ipv4Addr, port: u16, peer_id: PeerId) -> Multiaddr {
        Multiaddr::empty()
            .with(Protocol::Ip4(ip))
            .with(Protocol::Tcp(port))
            .with(Protocol::P2p(peer_id))
    }

    #[test]
    fn discovery_candidate_prefers_remembered_physical_path_and_deduplicates() {
        let peer_id = PeerId::random();
        let physical_ip = Ipv4Addr::new(192, 168, 31, 22);
        let virtual_ip = Ipv4Addr::new(172, 29, 96, 2);
        let physical = dial_address(physical_ip, 45_001, peer_id);
        let virtual_path = dial_address(virtual_ip, 45_002, peer_id);
        let interfaces = vec![
            LanInterface {
                name: "vEthernet (WSL)".to_string(),
                ip: Ipv4Addr::new(172, 29, 96, 1),
                broadcast: Ipv4Addr::new(172, 29, 111, 255),
                prefixlen: 20,
            },
            LanInterface {
                name: "Ethernet".to_string(),
                ip: Ipv4Addr::new(192, 168, 31, 213),
                broadcast: Ipv4Addr::new(192, 168, 31, 255),
                prefixlen: 24,
            },
        ];

        let ranked = ranked_dial_addresses(
            vec![virtual_path.clone(), physical.clone(), physical.clone()],
            Some(physical_ip),
            &interfaces,
        );

        assert_eq!(ranked, vec![physical, virtual_path]);
    }

    #[test]
    fn discovery_candidate_prefers_physical_interface_without_history() {
        let peer_id = PeerId::random();
        let physical = dial_address(Ipv4Addr::new(192, 168, 31, 22), 45_001, peer_id);
        let virtual_path = dial_address(Ipv4Addr::new(172, 29, 96, 2), 45_002, peer_id);
        let interfaces = vec![
            LanInterface {
                name: "vEthernet (Hyper-V)".to_string(),
                ip: Ipv4Addr::new(172, 29, 96, 1),
                broadcast: Ipv4Addr::new(172, 29, 111, 255),
                prefixlen: 20,
            },
            LanInterface {
                name: "Wi-Fi".to_string(),
                ip: Ipv4Addr::new(192, 168, 31, 213),
                broadcast: Ipv4Addr::new(192, 168, 31, 255),
                prefixlen: 24,
            },
        ];

        assert_eq!(
            ranked_dial_addresses(
                vec![virtual_path.clone(), physical.clone()],
                None,
                &interfaces,
            ),
            vec![physical, virtual_path],
        );
    }

    #[test]
    fn discovery_candidate_places_non_ipv4_after_physical_ipv4_without_history() {
        let peer_id = PeerId::random();
        let physical = dial_address(Ipv4Addr::new(192, 168, 31, 22), 45_001, peer_id);
        let ipv6 = Multiaddr::empty()
            .with(Protocol::Ip6(std::net::Ipv6Addr::LOCALHOST))
            .with(Protocol::Tcp(45_002))
            .with(Protocol::P2p(peer_id));
        let interfaces = vec![LanInterface {
            name: "Wi-Fi".to_string(),
            ip: Ipv4Addr::new(192, 168, 31, 213),
            broadcast: Ipv4Addr::new(192, 168, 31, 255),
            prefixlen: 24,
        }];

        assert_eq!(
            ranked_dial_addresses(vec![ipv6.clone(), physical.clone()], None, &interfaces),
            vec![physical, ipv6],
        );
    }

    #[test]
    fn remembered_friend_probe_reaches_the_real_subnet_without_expanding_the_generic_scan() {
        let interface = LanInterface {
            name: "Ethernet".to_string(),
            ip: Ipv4Addr::new(10, 20, 30, 40),
            broadcast: Ipv4Addr::new(10, 20, 255, 255),
            prefixlen: 16,
        };
        let remembered_outside_local_24 = Ipv4Addr::new(10, 20, 99, 8);

        assert_eq!(
            remembered_probe_targets(
                &interface,
                &[
                    remembered_outside_local_24,
                    remembered_outside_local_24,
                    Ipv4Addr::new(10, 21, 1, 9),
                    interface.ip,
                ],
            ),
            vec![remembered_outside_local_24]
        );
        assert!(!unicast_probe_targets(&interface).contains(&remembered_outside_local_24));
    }

    #[test]
    fn manual_refresh_wakes_every_discovery_path_and_can_be_reused() {
        let refresh = DiscoveryRefresh::new();
        let mut announce = refresh.subscribe();
        let mut generic_probe = refresh.subscribe();

        assert_eq!(refresh.trigger(), 1);
        assert!(
            announce
                .has_changed()
                .expect("announce subscriber remains open")
        );
        assert!(
            generic_probe
                .has_changed()
                .expect("probe subscriber remains open")
        );
        assert_eq!(*announce.borrow_and_update(), 1);
        assert_eq!(*generic_probe.borrow_and_update(), 1);

        assert_eq!(refresh.trigger(), 2);
        assert!(announce.has_changed().expect("announce can refresh again"));
        assert!(
            generic_probe
                .has_changed()
                .expect("probe can refresh again")
        );
    }
}
