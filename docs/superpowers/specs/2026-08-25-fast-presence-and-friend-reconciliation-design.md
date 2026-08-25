# Weline Localnet Fast Presence and Friend Reconciliation Design

## Outcome

Weline Localnet 0.2.1 must discover a newly online device without minute-scale delays, prefer a previously authenticated friend's LAN path, and converge friendship on both devices after a temporary disconnect. Existing 0.1.x and 0.2.0 peers remain discoverable through `/localnet/control/1`; resumable file capability negotiation remains independent.

Targets:

- a previously authenticated friend that returns on the same LAN IP is actively probed immediately and every 2 seconds while offline;
- fresh discovery candidates received in one 75 ms window are dialed as one concurrent libp2p attempt instead of serial single-address attempts;
- ordinary same-subnet discovery remains within the existing 15-second acceptance ceiling;
- a peer that is already a friend, has a pending outgoing request, or has an accepted request never exposes an active Add Friend button.

## Confirmed root causes

The current UDP sweep is not waiting on every host: interface sweeps run concurrently and each `/24` target is sent without a per-host timeout. The delay occurs after discovery. `dial_discovered_peer` starts a dial with one address and `DisconnectedAndNotDialing`; every better address arriving while that 10-second libp2p transport attempt is active is discarded. `OutgoingConnectionError` only logs, so the runtime waits for a later beacon and can repeatedly choose a virtual/TUN path.

The current machine exposes one physical LAN plus VMware and Hyper-V RFC1918 interfaces, all of which are currently eligible. They must remain fallbacks, but they must not precede common physical-interface paths.

Friend acceptance is persisted locally before `FriendDecision` is sent. `PendingAction::FriendDecision` carries no request ID, successful delivery is not recorded, failure is only logged, and reconnect does not retry. A disconnect can therefore leave one device with a friend and the other with no friend. The inspected 0.1.5 Windows data has one peer but zero friends and zero requests, matching this asymmetric-state failure rather than a renderer-only error.

The renderer also requires `protocolVersion === 1` before enabling Add Friend even though completing `/localnet/control/1` Hello already proves the base friendship protocol is available. This couples discovery/friendship to file-feature versioning unnecessarily.

## Discovery architecture

### Candidate batching

Every mDNS, compatibility-mDNS, beacon, and probe hint refreshes its existing lease and queues the peer for a dial 75 ms later. The runtime gathers all fresh addresses for that peer, removes duplicates, sorts them by remembered authenticated IP and interface priority, and submits one `DialOpts` containing the full list. libp2p's concurrent dialer races the candidates and cancels the losers after the first authenticated Noise connection succeeds.

An outgoing connection failure requeues the peer immediately when a fresh lease remains. Duplicate beacon refreshes update the lease but do not create unbounded simultaneous attempts. A connected peer is never redialed.

### Interface priority

Interface priority is a best-effort ordering only. Common virtual/TUN names such as `utun`, `tun`, `tap`, `vmnet`, `vEthernet`, `Hyper-V`, `WSL`, `Docker`, `Tailscale`, `ZeroTier`, `VPN`, and `bridge` sort after other eligible RFC1918 interfaces. No eligible interface is removed, so localized physical names and unusual corporate adapters continue to work.

### Remembered friend path

After TCP and Noise authenticate a Peer ID and Hello records the peer, the runtime stores only the RFC1918 IPv4 address from the established connection in `peers.last_lan_ip`. It never trusts or persists an unauthenticated beacon claim.

At startup, accepted friends' remembered IPs are supplied to the discovery service. A separate bounded task sends the existing version-2 Localnet UDP probe to those exact IPs immediately and every 2 seconds, bound only to an eligible interface whose real subnet contains the target. This obtains the peer's current ephemeral TCP listen port without scanning unrelated hosts and also reaches remembered friends elsewhere in a real `/8`-`/23` LAN that the generic bounded `/24` sweep intentionally does not scan.

The generic broadcast, mDNS, Windows compatibility mDNS, and 12-second `/24` sweep remain enabled for new devices and old versions.

## Durable friendship convergence

`pending_friend_decisions` is a SQLite outbox keyed by request ID. Resolving an incoming request writes the friend/request state and the pending accepted-or-rejected decision in the same transaction. Only the exact peer's `ControlResponse::Accepted` removes the outbox row. Send failure, application restart, and reconnect retain it.

After every successful Hello, the runtime flushes bounded pending decisions for that peer. Existing 0.1.x clients already understand the same `FriendDecision` request, and their existing idempotent request resolution accepts a repeated decision with the same final status.

Migration seeds accepted/rejected incoming historical requests into the outbox. If an authenticated peer sends a new request but is already a local friend, Weline Localnet treats it as reconciliation rather than a new trust decision: it persists that exact request as accepted, queues an accepted `FriendDecision`, and acknowledges the request. This repairs the older one-sided-friend state without auto-accepting any unknown Peer ID.

## Renderer behavior

Nearby-peer selection is a pure helper shared with tests. It excludes the local Peer ID, accepted friends, pending outgoing requests, and accepted requests. The Add Friend control is enabled for every peer that completed the negotiated control Hello; file-transfer capabilities continue to decide only which file protocol is used.

## Security and limits

- UDP hints remain untrusted routing hints; Noise Peer ID plus Hello remains the online boundary.
- Remembered IPs are written only after an authenticated connection and are restricted to RFC1918 IPv4.
- Known-friend probes are bounded to 256 unique targets and never expand to a subnet sweep.
- Interface-name heuristics only order candidates and never bypass address, Peer ID, friendship, or Noise validation.
- Friendship auto-reconciliation applies only when the exact authenticated Peer ID already exists in the local `friends` table.

## Acceptance

1. A batch containing a virtual address followed by a physical address yields one concurrent dial plan with the physical/remembered address first.
2. New hints received during a 75 ms batch are included; duplicate hints do not start duplicate dials.
3. A failed dial with live leases is immediately eligible for another aggregated attempt.
4. A remembered friend's probe is sent immediately and every 2 seconds, while the generic scan remains every 12 seconds.
5. Last LAN IP persists only after authenticated connection plus Hello and survives restart.
6. Accepted and rejected friend decisions survive restart, retry on Hello, and retire only after the exact peer acknowledges.
7. A new request from an already trusted Peer ID heals the remote side without another prompt.
8. Existing friends, pending/accepted requests, and any control-compatible application version never show an active Add Friend button.
9. Existing 0.1.x presence, friendship, text, and v1 file paths remain compatible; v2 file capability behavior is unchanged.

