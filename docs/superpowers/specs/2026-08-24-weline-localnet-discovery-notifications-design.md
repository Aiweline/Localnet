# Weline Localnet Discovery and Friend Notification Design

## Outcome

Weline Localnet 0.1.4 will replace the Weline Chat product name, discover Windows and macOS peers in both directions across ordinary LAN, proxy, and TUN environments, and make every incoming friend request visible and actionable. A Windows user must be able to receive a request from a Mac, bring the app forward from a desktop notification or taskbar attention signal, and accept or reject the request from a persistent in-app confirmation card.

The release keeps all traffic on the LAN. It does not add a cloud rendezvous service, identify devices by MAC address, weaken libp2p Noise authentication, or auto-accept a request.

## Confirmed failure evidence

The affected Windows installation was running from `%LocalAppData%\Weline Chat\localnet.exe`, listening on TCP and UDP, but had no established connection to the Mac. Its SQLite snapshot contained no Mac peer and no incoming friend request, proving that the request did not reach the Windows control protocol or UI.

A raw Windows mDNS listener captured a valid reply from the Mac at `192.168.31.22`. The reply arrived from an ephemeral UDP source port and advertised:

```text
dnsaddr=/ip4/192.168.31.22/tcp/55957/p2p/12D3KooWR6hsCtwDeBgMH53bibCNphp5cMQ3uFTFgVnmMds2NJHR
```

The current libp2p mDNS behavior did not surface that address to the Weline Localnet swarm. The Mac did not respond to the version-2 Weline UDP probe, so the active-unicast fallback could not bridge this older discovery implementation. Direct TCP reachability from Windows to the advertised Mac port succeeded.

Two state-management problems amplified the network failure:

- peer `online` state persisted across application restarts, so an old peer could appear online before a fresh Noise and Hello exchange;
- a queued friend request was stored as pending before the network runtime confirmed delivery, while an asynchronous send failure neither removed the pending row nor produced a visible UI error.

## Product identity and migration

The visible product name becomes **Weline Localnet** everywhere: window title, onboarding, runtime messages, native notifications, installer metadata, DMG name, package names, capability descriptions, and active product documentation.

The following stable technical identities do not change:

- Tauri identifier: `com.aiweline.localnet`;
- app-data directory and SQLite database;
- keyring/service identity and Ed25519 key;
- libp2p Peer ID;
- control protocol `/localnet/control/1`;
- file protocol `/localnet/file/1`;
- event channel `localnet://event`;
- UDP discovery magic and port.

Preserving those identities keeps nicknames, trusted device identity, friends, messages, transfers, and received files across the visible rename. The Windows current-user installer upgrade path must be exercised from Weline Chat 0.1.3 to Weline Localnet 0.1.4. The final `release` directory contains only 0.1.4 artifacts; old packages remain outside the delivery directory.

## Discovery architecture

Four discovery signals converge on the same authenticated libp2p dial path:

1. existing rust-libp2p mDNS;
2. existing Weline UDP broadcast beacon;
3. existing bounded `/24` active UDP probe;
4. a Windows raw-mDNS compatibility receiver for peers whose replies are missed by rust-libp2p mDNS and which do not implement the version-2 UDP probe.

The compatibility receiver is a focused addition to the existing `DiscoveryService`, not a second network runtime. On Windows it:

- binds UDP port 5353 with address reuse;
- joins `224.0.0.251` on each eligible RFC1918 interface;
- sends a PTR query for `_p2p._udp.local` on startup and every three seconds;
- accepts replies regardless of the remote UDP source port;
- parses DNS names with strict packet bounds and a compression-pointer depth limit;
- reads only TXT values beginning with `dnsaddr=`;
- parses each value as a libp2p `Multiaddr`;
- requires an RFC1918 IPv4 address, nonzero TCP port, and a valid `/p2p/<PeerId>` component;
- ignores loopback, link-local, TUN-only non-RFC1918 addresses, the local Peer ID, malformed records, and public addresses;
- emits the same lease-bound `DiscoveryEvent::PeerHint` used by the existing UDP paths.

Discovery data remains only a routing hint. The peer becomes visible as online only after TCP, Noise, and Hello validate the advertised Peer ID and profile.

The compatibility receiver is Windows-specific in 0.1.4 because the reproduced loss occurs in the Windows mDNS socket path. macOS continues to use libp2p mDNS and Weline UDP discovery. A parser or socket failure is logged and retried without stopping the swarm.

## Online-state correctness

At application startup, before the first bootstrap snapshot is returned, storage marks every persisted peer and friend offline with the current timestamp. Discovery hints alone do not set a peer online. `record_hello` remains the single transition to online, and connection/address expiry remains the transition back to offline.

This prevents a restarted Mac or Windows client from showing a stale device as currently reachable. Existing peer metadata remains available for friend history and conversations; only transient reachability is reset.

## Reliable friend-request state

Outgoing requests use these observable stages:

1. the command validates a freshly online peer and stores the request as pending;
2. the network runtime queues `ControlRequest::FriendRequest`;
3. `ControlResponse::Accepted` emits `FriendRequestDelivered` and the UI reports that the request reached the other device;
4. `FriendDecision` moves the request to accepted or rejected;
5. an outbound failure or protocol rejection removes the pending outgoing request, emits a localized visible error, and restores the Add Friend action so the user can retry.

The UI must not say “sent” merely because a command entered the network queue. Existing request-ID validation, duplicate handling, peer matching, and rate limiting remain in place.

Incoming requests are persisted before notification. Application restart, a missed webview event, or a temporarily hidden window cannot lose the confirmation action because the bootstrap snapshot reconstructs the pending request card from SQLite.

## Notification experience

The primary confirmation surface is a persistent in-app card above the main content. It shows the requester nickname and platform, the pending count when more than one request exists, and clear **Accept** and **Reject** actions. Accepting selects the new friend and opens the conversation. Rejecting removes the card and leaves no friendship.

When a new request arrives:

- the main window requests user attention so Windows flashes the taskbar and macOS can draw attention to the Dock icon;
- if the main window is not focused and notification permission has already been granted, Weline Localnet sends a native notification containing the requester nickname and “Open Weline Localnet to accept or reject”;
- activating the app or notification brings the pending confirmation card into view;
- if native notifications are unavailable or denied, the in-app card and attention request still work.

Notification permission is never requested by the installer. Weline Localnet explains the benefit inside the application and requests notification permission once when the user enables system notifications. Denial is nonfatal and does not block discovery, friendship, chat, or transfers.

Desktop notification action buttons are deliberately excluded. Tauri's documented Actions API is mobile-only, and accepting a LAN identity directly from an OS toast would also make accidental approval easier. Confirmation remains an explicit in-app action.

## Event and UI data flow

The frontend listens to `localnet://event` with a typed event payload instead of discarding it. Every event still schedules a storage refresh. In addition:

- `friendRequestReceived` reveals the confirmation card and triggers accessible live-region text;
- `friendRequestDelivered` shows delivery success;
- `networkError` shows the backend message in the existing toast/error surface;
- `friendRequestResolved` selects the accepted friend when appropriate.

The snapshot remains the source of truth. Event payloads provide immediacy but never replace persisted state.

## Security and privacy

- Native and in-app notifications show only the requester nickname and request type, never Peer IDs, IP addresses, message contents, filenames, or paths.
- Nicknames continue through the existing validation and are rendered as text, not HTML.
- Discovery TXT data is bounded and untrusted. Parsing rejects invalid lengths, pointer cycles, invalid UTF-8, invalid multiaddresses, public addresses, and mismatched Peer IDs.
- The raw-mDNS path cannot add friends or deliver application data. It can only request an authenticated libp2p dial.
- No automatic firewall elevation returns. The Windows installer remains `currentUser` and `asInvoker`.

## Failure behavior

- Raw-mDNS bind or join failure retries without disabling the existing three discovery paths.
- A malformed DNS packet is ignored without terminating the receiver.
- A discovered address that fails Noise or Peer-ID authentication never appears online.
- A friend-request delivery failure becomes visible and retryable; it does not remain falsely pending.
- Native notification permission denial falls back to the in-app card and taskbar/Dock attention.
- Rename migration failure must preserve the old app data and identity and report the exact packaging blocker; it must not create a fresh user identity as a fallback.

## Acceptance criteria

1. The visible name is Weline Localnet across the application, Windows installer, macOS DMG, notifications, and active documentation.
2. `com.aiweline.localnet`, the existing SQLite database, and the existing Peer ID survive upgrade from 0.1.3.
3. On the reproduced LAN, Windows parses the Mac's raw mDNS TXT record and completes TCP, Noise, and Hello using the physical `192.168.31.22` address.
4. With TUN on and off, Windows discovers Mac and Mac discovers Windows within 15 seconds after both current clients start.
5. Restarting either application does not show a peer online before a new Hello completes.
6. Mac-to-Windows friend request creates a Windows attention signal, native notification when permitted, and a persistent in-app Accept/Reject card.
7. Accepting the request creates the same friendship on both devices and opens the Windows conversation.
8. Rejecting a request creates no friendship; a failed outgoing request becomes visibly retryable.
9. After acceptance, text and a checksum-verified file transfer succeed in both directions.
10. Existing mDNS, UDP broadcast, and active-probe paths remain functional in isolated diagnostics.
11. Windows setup remains current-user, `asInvoker`, and contains the ten configured installer languages.
12. Windows x64 setup, Windows portable executable, and universal macOS DMG build as version 0.1.4.
13. The final `release` directory contains only Weline Localnet 0.1.4 artifacts and matching SHA-256 files.

## Non-goals

- cloud discovery, relay, accounts, push service, or internet dependency;
- MAC-address discovery or identity;
- automatic friendship or acceptance from an OS notification;
- group chat, presence status beyond online/offline, or background launch at login;
- changing message, transfer, identity, or encryption wire formats;
- redesigning the full application shell beyond the actionable request card and renamed product copy.

## References

- [Tauri Notifications plugin](https://v2.tauri.app/plugin/notification/)
- [libp2p mDNS specification](https://github.com/libp2p/specs/blob/master/discovery/mdns.md)
