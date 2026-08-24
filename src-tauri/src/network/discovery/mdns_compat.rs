use std::{
    collections::HashSet,
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str,
    time::{Duration, Instant},
};

use libp2p::{Multiaddr, PeerId, multiaddr::Protocol};
use socket2::{Domain, Protocol as SocketProtocol, SockAddr, Socket, Type};
use tokio::{net::UdpSocket, sync::mpsc, time};

use super::{BEACON_LEASE, DiscoveryEvent, LanInterface, eligible_interfaces, is_private_lan_ip};

const MDNS_PORT: u16 = 5_353;
const MDNS_MULTICAST: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 251);
const MAX_DNS_PACKET_BYTES: usize = 9_000;
const MAX_POINTER_DEPTH: usize = 16;
const QUERY_INTERVAL: Duration = Duration::from_secs(3);
const RETRY_INTERVAL: Duration = Duration::from_secs(3);
const DNS_TYPE_PTR: u16 = 12;
const DNS_TYPE_TXT: u16 = 16;
const DNS_CLASS_IN: u16 = 1;

pub(super) fn spawn(local_peer: PeerId, sender: mpsc::Sender<DiscoveryEvent>) {
    tauri::async_runtime::spawn(async move {
        loop {
            if let Err(error) = receive_mdns(local_peer, sender.clone()).await {
                tracing::warn!(%error, "Windows mDNS compatibility receiver unavailable; retrying");
            }
            time::sleep(RETRY_INTERVAL).await;
        }
    });
}

async fn receive_mdns(local_peer: PeerId, sender: mpsc::Sender<DiscoveryEvent>) -> io::Result<()> {
    let interfaces = eligible_interfaces()?;
    if interfaces.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "no eligible RFC1918 interface for mDNS compatibility",
        ));
    }
    let socket = bind_receiver(&interfaces)?;
    tracing::debug!(
        interfaces = ?interfaces
            .iter()
            .map(|interface| format!("{}={}", interface.name, interface.ip))
            .collect::<Vec<_>>(),
        "Windows mDNS compatibility receiver started"
    );
    let mut query_interval = time::interval(QUERY_INTERVAL);
    query_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut buffer = [0_u8; MAX_DNS_PACKET_BYTES + 1];

    loop {
        tokio::select! {
            _ = query_interval.tick() => send_queries(&interfaces).await,
            received = socket.recv_from(&mut buffer) => {
                let (length, source) = received?;
                if length > MAX_DNS_PACKET_BYTES {
                    tracing::trace!(length, "oversized mDNS packet ignored");
                    continue;
                }
                let SocketAddr::V4(source) = source else {
                    continue;
                };
                tracing::trace!(source = %source, length, "mDNS packet received");
                let hints = parse_peer_hints(&buffer[..length], *source.ip(), local_peer);
                tracing::trace!(source = %source, hints = hints.len(), "mDNS packet parsed");
                for (peer_id, address) in hints {
                    tracing::debug!(%peer_id, %address, source = %source, "mDNS peer hint accepted");
                    if sender
                        .send(DiscoveryEvent::PeerHint {
                            peer_id,
                            address,
                            expires_at: Instant::now() + BEACON_LEASE,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
        }
    }
}

fn bind_receiver(interfaces: &[LanInterface]) -> io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(SocketProtocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.bind(&SockAddr::from(SocketAddrV4::new(
        Ipv4Addr::UNSPECIFIED,
        MDNS_PORT,
    )))?;

    let mut joined = 0_usize;
    for interface in interfaces {
        match socket.join_multicast_v4(&MDNS_MULTICAST, &interface.ip) {
            Ok(()) => joined += 1,
            Err(error) => tracing::debug!(
                interface = %interface.name,
                address = %interface.ip,
                %error,
                "unable to join mDNS multicast group on interface"
            ),
        }
    }
    if joined == 0 {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "unable to join mDNS multicast group on any LAN interface",
        ));
    }
    socket.set_nonblocking(true)?;
    UdpSocket::from_std(socket.into())
}

async fn send_queries(interfaces: &[LanInterface]) {
    let query = mdns_query();
    for interface in interfaces {
        let socket = match UdpSocket::bind(SocketAddrV4::new(interface.ip, 0)).await {
            Ok(socket) => socket,
            Err(error) => {
                tracing::trace!(
                    interface = %interface.name,
                    address = %interface.ip,
                    %error,
                    "mDNS query socket unavailable"
                );
                continue;
            }
        };
        if let Err(error) = socket
            .send_to(&query, SocketAddrV4::new(MDNS_MULTICAST, MDNS_PORT))
            .await
        {
            tracing::trace!(
                interface = %interface.name,
                address = %interface.ip,
                %error,
                "mDNS compatibility query failed"
            );
        } else {
            tracing::trace!(
                interface = %interface.name,
                address = %interface.ip,
                "mDNS compatibility query sent"
            );
        }
    }
}

fn mdns_query() -> Vec<u8> {
    let mut packet = vec![
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    for label in [b"_p2p".as_slice(), b"_udp".as_slice(), b"local".as_slice()] {
        packet.push(label.len() as u8);
        packet.extend_from_slice(label);
    }
    packet.push(0);
    packet.extend_from_slice(&DNS_TYPE_PTR.to_be_bytes());
    packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
    packet
}

fn parse_peer_hints(
    packet: &[u8],
    source_ip: Ipv4Addr,
    local_peer: PeerId,
) -> Vec<(PeerId, Multiaddr)> {
    if packet.len() < 12 || packet.len() > MAX_DNS_PACKET_BYTES || !is_private_lan_ip(source_ip) {
        return Vec::new();
    }

    let Some(question_count) = read_u16(packet, 4) else {
        return Vec::new();
    };
    let Some(answer_count) = read_u16(packet, 6) else {
        return Vec::new();
    };
    let Some(authority_count) = read_u16(packet, 8) else {
        return Vec::new();
    };
    let Some(additional_count) = read_u16(packet, 10) else {
        return Vec::new();
    };
    let mut cursor = 12_usize;

    for _ in 0..question_count {
        if skip_dns_name(packet, &mut cursor).is_none() || take(packet, &mut cursor, 4).is_none() {
            return Vec::new();
        }
    }

    let record_count = usize::from(answer_count)
        .saturating_add(usize::from(authority_count))
        .saturating_add(usize::from(additional_count));
    let mut hints = Vec::new();
    let mut seen = HashSet::new();

    for _ in 0..record_count {
        if skip_dns_name(packet, &mut cursor).is_none() {
            return Vec::new();
        }
        let Some(record_type) = read_u16_at_cursor(packet, &mut cursor) else {
            return Vec::new();
        };
        let Some(record_class) = read_u16_at_cursor(packet, &mut cursor) else {
            return Vec::new();
        };
        if take(packet, &mut cursor, 4).is_none() {
            return Vec::new();
        }
        let Some(data_length) = read_u16_at_cursor(packet, &mut cursor) else {
            return Vec::new();
        };
        let Some(data) = take(packet, &mut cursor, usize::from(data_length)) else {
            return Vec::new();
        };
        if record_type != DNS_TYPE_TXT || record_class & 0x7fff != DNS_CLASS_IN {
            continue;
        }
        for value in txt_values(data) {
            let Some(value) = value.strip_prefix("dnsaddr=") else {
                continue;
            };
            let Ok(address) = value.parse::<Multiaddr>() else {
                continue;
            };
            let Some((peer_id, advertised_ip)) = validated_peer_address(&address) else {
                continue;
            };
            if peer_id == local_peer
                || advertised_ip != source_ip
                || !is_private_lan_ip(advertised_ip)
            {
                continue;
            }
            if seen.insert((peer_id, address.clone())) {
                hints.push((peer_id, address));
            }
        }
    }
    hints
}

fn validated_peer_address(address: &Multiaddr) -> Option<(PeerId, Ipv4Addr)> {
    let mut protocols = address.iter();
    let Protocol::Ip4(ip) = protocols.next()? else {
        return None;
    };
    let Protocol::Tcp(port) = protocols.next()? else {
        return None;
    };
    let Protocol::P2p(peer_id) = protocols.next()? else {
        return None;
    };
    if port == 0 || protocols.next().is_some() {
        return None;
    }
    Some((peer_id, ip))
}

fn txt_values(data: &[u8]) -> Vec<&str> {
    let mut values = Vec::new();
    let mut cursor = 0_usize;
    while cursor < data.len() {
        let length = usize::from(data[cursor]);
        cursor += 1;
        let Some(end) = cursor.checked_add(length) else {
            return Vec::new();
        };
        let Some(value) = data
            .get(cursor..end)
            .and_then(|value| str::from_utf8(value).ok())
        else {
            return Vec::new();
        };
        values.push(value);
        cursor = end;
    }
    values
}

fn skip_dns_name(packet: &[u8], cursor: &mut usize) -> Option<()> {
    let mut position = *cursor;
    let mut labels = 0_usize;
    loop {
        labels += 1;
        if labels > 128 {
            return None;
        }
        let length = *packet.get(position)?;
        if length & 0xc0 == 0xc0 {
            let next = *packet.get(position + 1)?;
            let pointer = (usize::from(length & 0x3f) << 8) | usize::from(next);
            validate_dns_name(packet, pointer, 1)?;
            *cursor = position.checked_add(2)?;
            return Some(());
        }
        if length & 0xc0 != 0 || length > 63 {
            return None;
        }
        position = position.checked_add(1)?;
        if length == 0 {
            *cursor = position;
            return Some(());
        }
        position = position.checked_add(usize::from(length))?;
        if position > packet.len() {
            return None;
        }
    }
}

fn validate_dns_name(packet: &[u8], mut position: usize, depth: usize) -> Option<()> {
    if depth > MAX_POINTER_DEPTH {
        return None;
    }
    let mut labels = 0_usize;
    loop {
        labels += 1;
        if labels > 128 {
            return None;
        }
        let length = *packet.get(position)?;
        if length & 0xc0 == 0xc0 {
            let next = *packet.get(position + 1)?;
            let pointer = (usize::from(length & 0x3f) << 8) | usize::from(next);
            if pointer == position {
                return None;
            }
            return validate_dns_name(packet, pointer, depth + 1);
        }
        if length & 0xc0 != 0 || length > 63 {
            return None;
        }
        position = position.checked_add(1)?;
        if length == 0 {
            return Some(());
        }
        position = position.checked_add(usize::from(length))?;
        if position > packet.len() {
            return None;
        }
    }
}

fn read_u16(packet: &[u8], offset: usize) -> Option<u16> {
    let bytes = packet.get(offset..offset.checked_add(2)?)?;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u16_at_cursor(packet: &[u8], cursor: &mut usize) -> Option<u16> {
    let bytes = take(packet, cursor, 2)?;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn take<'a>(packet: &'a [u8], cursor: &mut usize, length: usize) -> Option<&'a [u8]> {
    let end = cursor.checked_add(length)?;
    let value = packet.get(*cursor..end)?;
    *cursor = end;
    Some(value)
}
