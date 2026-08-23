# Localnet Resilient LAN Discovery Design

## Outcome

Localnet 0.1.1 must automatically discover another Localnet device on the same physical IPv4 LAN while system proxy or TUN mode remains enabled on Windows and macOS. Discovery must not depend on a cloud rendezvous server and must not weaken the existing libp2p Noise identity check.

## Confirmed root causes

The 0.1.0 client has one discovery path: rust-libp2p mDNS. TUN software can redirect or suppress multicast traffic even when direct RFC1918 routes still exist. The UI only marks a peer discovered after TCP, Noise, and Hello complete, so an inbound firewall block is indistinguishable from mDNS failure.

The current Windows host also provides direct evidence of a second failure mode: its physical `192.168.31.0/24` network is classified as Public and no Localnet firewall rule exists. macOS 15 and newer can block local-network operations until the user grants the Local Network privilege; the 0.1.0 bundle has no `NSLocalNetworkUsageDescription` or Bonjour service declaration.

MAC addresses are not a discovery solution. An application cannot route to a MAC address, and TUN interfaces intentionally do not expose the physical layer-2 neighbor relationship. Localnet keeps its Ed25519/libp2p Peer ID as the stable device identity.

## Chosen architecture

Localnet will use two independent discovery paths that converge on the same authenticated libp2p connection:

1. Keep libp2p mDNS for networks where multicast works.
2. Add a Localnet UDP beacon service on a fixed high port. It enumerates non-loopback RFC1918 IPv4 addresses, binds each sender socket to that concrete source address, and sends a small beacon to that interface's subnet-directed broadcast address plus `255.255.255.255`.

Binding to the concrete LAN address and using the subnet-directed broadcast route avoids relying on the TUN-owned default route. Virtual adapters may receive harmless beacons, but they cannot become trusted peers without completing the Noise handshake.

The beacon contains only:

- protocol magic and discovery version;
- claimed libp2p Peer ID;
- current libp2p TCP listen port.

The receiver ignores oversized, malformed, self, non-private-source, and unsupported-version datagrams. It derives the dial IP from the UDP packet source rather than trusting an advertised IP, constructs `/ip4/<source>/tcp/<port>/p2p/<peer-id>`, rate-limits duplicate hints, and asks the existing swarm to dial. The remote Noise identity must match the claimed Peer ID before Hello can make the device visible.

## Address lifetime and network changes

UDP hints are leases, not permanent addresses. Each valid beacon refreshes a short expiry. An interval removes expired beacon leases and marks a peer offline only when it has no mDNS address, no live beacon lease, and no active libp2p connection.

The beacon task refreshes interface enumeration periodically so Wi-Fi changes, Ethernet changes, proxy reconnects, sleep/wake, and TUN recreation do not require restarting Localnet. Socket-level errors on one interface are logged but do not stop the swarm or other discovery paths.

## Platform permissions

### Windows

The NSIS installer runs per-machine and adds one program-scoped inbound firewall rule for the installed `Localnet.exe`. The rule is limited to remote addresses in `LocalSubnet` and applies to the active Windows profile, including a home LAN incorrectly classified as Public. Uninstall removes only that named Localnet rule.

The portable executable is not allowed to silently elevate itself. It remains useful when the user has already allowed it through the firewall; the supported automatic path is the installer.

### macOS

The app bundle includes `NSLocalNetworkUsageDescription` and declares libp2p's `_p2p._udp` Bonjour service. On first local-network access, macOS can therefore show a meaningful Local Network permission prompt. Localnet keeps retrying discovery so a datagram denied while the prompt is pending does not permanently stop discovery.

## Security and traffic limits

- Beacon payload is bounded to 512 bytes.
- UDP hints never create friendships or deliver chat data.
- Source IP must be RFC1918 and the TCP port must be non-zero.
- Duplicate `(peer, address)` hints are coalesced.
- Broadcast cadence is several seconds with startup jitter; no full subnet sweep is performed.
- Text, image, and file traffic continues over TCP + Noise + Yamux with the current request and stream protocols.

## User experience

No new setup field is required. Nearby users continue to appear in the existing sidebar after authenticated Hello. If no peer appears after a grace period, the empty state explains that Windows Firewall or macOS Local Network permission may need approval, without telling the user to turn off the proxy.

## Acceptance

1. With a TUN/proxy adapter present, each client still sends beacons from its concrete physical RFC1918 address to that subnet's broadcast address.
2. A valid beacon produces an authenticated libp2p dial hint and a completed Hello exposes the peer.
3. Malformed, public-source, self, duplicate, and expired beacons are safely ignored or cleaned up.
4. Existing mDNS discovery still works.
5. Windows NSIS installs and removes the `LocalSubnet` firewall rule.
6. The macOS app bundle contains the Local Network usage description and `_p2p._udp` declaration.
7. Windows and universal macOS 0.1.1 packages build successfully.

