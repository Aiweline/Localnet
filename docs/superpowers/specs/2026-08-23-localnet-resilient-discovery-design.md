# Localnet Resilient LAN Discovery Design

## Outcome

Weline Chat 0.1.3 must automatically discover another Weline Chat device on the same physical IPv4 LAN while system proxy or TUN mode remains enabled on Windows and macOS. Discovery must not depend on a cloud rendezvous server and must not weaken the existing libp2p Noise identity check.

## Confirmed root causes

The original client relied on rust-libp2p mDNS. TUN software can redirect or suppress multicast traffic even when direct RFC1918 routes still exist. Broadcast fallback can be suppressed by the same network stack, and Windows libp2p mDNS can send a query from an ephemeral source port that the peer does not answer in a multi-adapter environment. The UI only marks a peer discovered after TCP, Noise, and Hello complete, so an inbound firewall block is indistinguishable from discovery failure.

The affected Windows host provided direct evidence of a second failure mode: its physical `192.168.31.0/24` network is classified as Public and the installed firewall rule contains literal `$` characters around both its name and executable path, so the rule cannot match the installed program. macOS 15 and newer can block local-network operations until the user grants the Local Network privilege.

The same host could resolve and connect directly to the real Mac at `192.168.31.22`, and a direct TCP + Noise + Identify diagnostic completed successfully. The fault is therefore discovery and Windows inbound policy, not Wi-Fi client isolation, routing, or a protocol-version mismatch.

MAC addresses are not a discovery solution. An application cannot route to a MAC address, and TUN interfaces intentionally do not expose the physical layer-2 neighbor relationship. Localnet keeps its Ed25519/libp2p Peer ID as the stable device identity.

## Chosen architecture

Weline Chat uses three discovery paths that converge on the same authenticated libp2p connection:

1. Keep libp2p mDNS for networks where multicast works.
2. Add a Localnet UDP beacon service on a fixed high port. It enumerates non-loopback RFC1918 IPv4 addresses, binds each sender socket to that concrete source address, and sends a small beacon to that interface's subnet-directed broadcast address plus `255.255.255.255`.
3. Add a bounded active UDP probe on the same port. Every 12 seconds, each eligible interface sends one small service-specific probe to at most the local `/24` host slice (maximum 253 targets), from that concrete interface IP. A Weline Chat peer responds to the exact source address with its normal beacon. This is not a generic TCP or port scan; it tests only Weline Chat's discovery port and waits 900 ms for responses.

Binding to the concrete LAN address avoids relying on the TUN-owned default route. The active probe supplies a unicast fallback when both multicast and broadcast are lost. Virtual adapters may receive harmless probes or beacons, but they cannot become trusted peers without completing the Noise handshake.

The beacon contains only:

- protocol magic and discovery version;
- claimed libp2p Peer ID;
- current libp2p TCP listen port.

The receiver ignores oversized, malformed, self, non-private-source, and unsupported-version datagrams. It derives the dial IP from the UDP packet source rather than trusting an advertised IP, constructs `/ip4/<source>/tcp/<port>/p2p/<peer-id>`, rate-limits probe responses and duplicate hints, and asks the existing swarm to dial. The remote Noise identity must match the claimed Peer ID before Hello can make the device visible.

## Address lifetime and network changes

UDP hints are leases, not permanent addresses. Each valid beacon refreshes a short expiry. An interval removes expired beacon leases and marks a peer offline only when it has no mDNS address, no live beacon lease, and no active libp2p connection.

The beacon task refreshes interface enumeration periodically so Wi-Fi changes, Ethernet changes, proxy reconnects, sleep/wake, and TUN recreation do not require restarting Localnet. Socket-level errors on one interface are logged but do not stop the swarm or other discovery paths.

## Platform permissions

### Windows

The NSIS installer runs for the current user and never requests elevation during installation. It does not run localized `netsh` output through the NSIS log, which avoids mojibake on non-English Windows systems. Windows may still show its native firewall consent once when Weline Chat first accepts inbound LAN traffic; this is a first-run network permission, not an installer permission.

The portable executable follows the same first-run network permission behavior. Neither packaging path silently elevates itself.

### macOS

The app bundle includes `NSLocalNetworkUsageDescription` and declares libp2p's `_p2p._udp` Bonjour service. On first local-network access, macOS can therefore show a meaningful Local Network permission prompt. Localnet keeps retrying discovery so a datagram denied while the prompt is pending does not permanently stop discovery.

## Security and traffic limits

- Beacon payload is bounded to 512 bytes.
- UDP hints never create friendships or deliver chat data.
- Source IP must be RFC1918 and the TCP port must be non-zero.
- Duplicate `(peer, address)` hints are coalesced.
- Broadcast cadence is several seconds with startup jitter.
- Active probing is limited to one `/24` slice per eligible RFC1918 interface, runs every 12 seconds, sends only the bounded Weline Chat discovery payload, and has a 900 ms response window.
- Probe responses are rate-limited per source address; a probe cannot create a friendship or bypass Noise authentication.
- Text, image, and file traffic continues over TCP + Noise + Yamux with the current request and stream protocols.

## User experience

No new setup field is required. Nearby users continue to appear in the existing sidebar after authenticated Hello. If no peer appears after a grace period, the empty state explains that Windows Firewall or macOS Local Network permission may need approval, without telling the user to turn off the proxy.

## Acceptance

1. With a TUN/proxy adapter present, each client sends broadcast beacons and bounded active probes from its concrete RFC1918 interface address.
2. A valid beacon or probe response produces an authenticated libp2p dial hint and a completed Hello exposes the peer.
3. Malformed, public-source, self, duplicate, and expired beacons are safely ignored or cleaned up.
4. Existing mDNS discovery still works.
5. Windows NSIS installs without UAC; Windows owns any first-run firewall consent.
6. The macOS app bundle contains the Local Network usage description and `_p2p._udp` declaration.
7. With mDNS disabled, two isolated desktop identities still discover one another, add a friend, exchange text, and transfer a file whose received SHA-256 matches the source.
8. Windows and universal macOS 0.1.3 packages build successfully.
