# Weline Localnet Discovery and Friend Notifications Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Parallel subagents are disabled for this repository session.

**Goal:** Deliver Weline Localnet 0.1.4 with bidirectional Windows/macOS LAN discovery, reliable friend-request delivery state, actionable incoming-request notifications, and preserved 0.1.3 user identity/data.

**Architecture:** Add a Windows-only raw-mDNS compatibility socket as a fourth producer of the existing authenticated `DiscoveryEvent::PeerHint`; keep Noise, Peer ID, and Hello as the online trust boundary. Make outbound friend-request persistence follow the actual request-response result, and let typed runtime events drive an always-recoverable in-app request banner plus optional native notifications.

**Tech Stack:** Rust 1.85+, Tokio, socket2, if-addrs, rust-libp2p 0.56, SQLite/rusqlite, Tauri 2, official Tauri notification plugin, React 19, TypeScript 5.9, CSS, NSIS, GitHub Actions macOS.

**Spec:** `docs/superpowers/specs/2026-08-24-weline-localnet-discovery-notifications-design.md`

## Global Constraints

- Set every visible product name and 0.1.4 artifact to **Weline Localnet**.
- Preserve `com.aiweline.localnet`, the existing app-data/SQLite path, keyring identity, Peer ID, protocol names, UDP magic/port, and `localnet://event`.
- Keep all discovery and transfer traffic on the LAN; do not add cloud discovery, relay, MAC-address identity, or automatic friendship.
- Treat mDNS/TXT/UDP data only as dial hints; peers become online only after TCP, Noise, and Hello.
- Keep the Windows installer `currentUser` and `asInvoker`; the only installer hook may migrate the old Weline Chat current-user install directory/registry keys after a successful copy. Do not add firewall automation, process execution, elevation, or permission prompts.
- Do not add persistent unit or E2E test files. Use temporary diagnostics, remove them before each commit, and finish with real-process/desktop acceptance.
- Do not modify historical 0.1.3 design records merely to replace their historical product/version text.

---

### Task 1: Windows raw-mDNS compatibility discovery

**Files:**
- Create: `src-tauri/src/network/discovery/mdns_compat.rs`
- Modify: `src-tauri/src/network/discovery.rs`

**Interfaces:**
- Consumes: `eligible_interfaces()`, `is_private_lan_ip(Ipv4Addr)`, `DiscoveryEvent`, and `BEACON_LEASE` from `network::discovery`.
- Produces: `mdns_compat::spawn(local_peer: PeerId, sender: mpsc::Sender<DiscoveryEvent>)` on Windows.
- Produces: `parse_peer_hints(packet: &[u8], source_ip: Ipv4Addr, local_peer: PeerId) -> Vec<(PeerId, Multiaddr)>` with strict DNS bounds and compression-depth checks.
- Preserves: `DiscoveryService::spawn(...) -> mpsc::Receiver<DiscoveryEvent>` as the only runtime discovery interface.

- [ ] **Step 1: Add a temporary failing parser diagnostic**

  Temporarily add a `#[cfg(test)]` block to `mdns_compat.rs` that builds one bounded DNS response and asserts both the reproduced address and malformed-pointer behavior:

  ```rust
  #[test]
  fn accepts_ephemeral_source_port_txt_and_rejects_pointer_cycle() {
      let peer: PeerId = "12D3KooWR6hsCtwDeBgMH53bibCNphp5cMQ3uFTFgVnmMds2NJHR".parse().unwrap();
      let txt = format!("dnsaddr=/ip4/192.168.31.22/tcp/55957/p2p/{peer}");
      let packet = minimal_txt_response(&txt);
      let hints = parse_peer_hints(&packet, Ipv4Addr::new(192, 168, 31, 22), PeerId::random());
      assert_eq!(hints, vec![(peer, format!("/ip4/192.168.31.22/tcp/55957/p2p/{peer}").parse().unwrap())]);

      let mut cyclic = packet;
      cyclic[12] = 0xc0;
      cyclic[13] = 0x0c;
      assert!(parse_peer_hints(&cyclic, Ipv4Addr::new(192, 168, 31, 22), PeerId::random()).is_empty());
  }
  ```

- [ ] **Step 2: Run the diagnostic and confirm the compatibility module is missing**

  Run: `cargo test --manifest-path src-tauri/Cargo.toml accepts_ephemeral_source_port_txt_and_rejects_pointer_cycle -- --nocapture`

  Expected before implementation: compilation fails because `parse_peer_hints` and the socket/parser implementation do not exist.

- [ ] **Step 3: Implement the bounded DNS parser and physical-address filter**

  Implement these rules in `mdns_compat.rs`:

  ```rust
  const MDNS_PORT: u16 = 5353;
  const MAX_DNS_PACKET_BYTES: usize = 9_000;
  const MAX_POINTER_DEPTH: usize = 16;
  const QUERY_INTERVAL: Duration = Duration::from_secs(3);

  // For each TXT dnsaddr: parse Multiaddr, require exactly one IPv4, nonzero TCP,
  // valid P2P PeerId, advertised IPv4 == UDP source IPv4, RFC1918, and non-self.
  // Matching the datagram source rejects loopback/TUN addresses bundled in the
  // same Mac reply while accepting replies from any UDP source port.
  ```

  Parse the 12-byte header, all question records, and answer/authority/additional resource records. Every label length, resource length, TXT segment, compression pointer, and cursor advance must remain inside `packet`; reject the whole malformed record without panicking.

- [ ] **Step 4: Implement the Windows receiver/query loop**

  Create one reusable UDP receiver bound to `0.0.0.0:5353`, join `224.0.0.251` for every eligible RFC1918 interface, and send a PTR query for `_p2p._udp.local` from an ephemeral socket bound to each concrete interface IP. Use `tokio::select!` to receive continuously and query every three seconds; on bind/join failure log and retry after three seconds without stopping other discovery paths.

  For every valid parsed hint, emit:

  ```rust
  DiscoveryEvent::PeerHint {
      peer_id,
      address,
      expires_at: Instant::now() + BEACON_LEASE,
  }
  ```

- [ ] **Step 5: Wire the compatibility receiver into the existing service**

  In `discovery.rs`, add `#[cfg(target_os = "windows")] mod mdns_compat;` and call `mdns_compat::spawn(peer_id, event_sender.clone())` inside `DiscoveryService::spawn`. Do not add another swarm or mark storage online from this path.

- [ ] **Step 6: Verify parser Green and remove the temporary test block**

  Run the focused test and expect PASS, then remove the temporary `#[cfg(test)]` diagnostic. Run:

  ```powershell
  cargo check --manifest-path src-tauri/Cargo.toml
  cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
  git diff --check
  ```

- [ ] **Step 7: Commit discovery independently**

  ```powershell
  git add src-tauri/src/network/discovery.rs src-tauri/src/network/discovery/mdns_compat.rs
  git commit -m "fix(discovery): receive Mac mDNS hints on Windows"
  ```

### Task 2: Reliable friend-request delivery state

**Files:**
- Modify: `src-tauri/src/storage.rs`
- Modify: `src-tauri/src/network/runtime.rs`
- Modify: `src-tauri/src/commands.rs`

**Interfaces:**
- Produces: `Storage::remove_pending_outgoing_friend_request(request_id: &str) -> Result<bool, AppError>`.
- Produces: `NetworkEvent::FriendRequestDelivered { request_id: String }`.
- Produces: `NetworkRuntime::fail_friend_request(request_id: &str, message: String) -> Result<(), AppError>`.
- Preserves: `send_friend_request(peer_id) -> FriendRequest`, but command completion means queued, not delivered.

- [ ] **Step 1: Add a temporary failing storage diagnostic**

  Temporarily add a storage test that inserts one outgoing pending request, calls `remove_pending_outgoing_friend_request`, asserts it returns `true`, and verifies `find_pending_friend_request` returns `None`; also insert an incoming pending request and verify the method cannot remove it.

- [ ] **Step 2: Run the focused diagnostic and confirm the method is absent**

  Run: `cargo test --manifest-path src-tauri/Cargo.toml remove_pending_outgoing_friend_request -- --nocapture`

  Expected before implementation: compilation fails with no method named `remove_pending_outgoing_friend_request`.

- [ ] **Step 3: Add atomic pending-outgoing cleanup**

  Implement one direction/status-scoped statement:

  ```sql
  DELETE FROM friend_requests
  WHERE request_id = ?1 AND direction = 'outgoing' AND status = 'pending'
  ```

  Return `changed == 1`. Do not delete incoming, accepted, or rejected rows.

- [ ] **Step 4: Emit delivery only after protocol acceptance**

  Extend the serialized event enum:

  ```rust
  FriendRequestDelivered { request_id: String },
  ```

  In `handle_outbound_response`, map `(PendingAction::FriendRequest { request_id }, ControlResponse::Accepted)` to this event. For a rejected response, call `fail_friend_request`, which deletes the pending outgoing row and emits `NetworkError { code: "friend_request_rejected", message }`.

- [ ] **Step 5: Make transport failure visible and retryable**

  In `handle_outbound_failure`, route `PendingAction::FriendRequest` through the same cleanup helper and emit `friend_request_failed`. Keep text and transfer failure behavior unchanged. In `commands.rs`, retain the initial pending write so a restart cannot lose an in-flight request, but remove the UI claim that command return equals remote delivery in Task 3.

- [ ] **Step 6: Verify state transitions and remove the temporary test**

  Run the focused test and expect PASS, remove the temporary test, then run `cargo check`, `cargo fmt --check`, and `git diff --check`.

- [ ] **Step 7: Commit request-state changes independently**

  ```powershell
  git add src-tauri/src/storage.rs src-tauri/src/network/runtime.rs src-tauri/src/commands.rs
  git commit -m "fix(friends): confirm delivery before reporting success"
  ```

### Task 3: Actionable in-app and native request notifications

**Files:**
- Modify: `package.json`
- Modify: `pnpm-lock.yaml`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/Cargo.lock`
- Modify: `src-tauri/src/lib.rs`
- Modify: `src-tauri/src/network/runtime.rs`
- Modify: `src-tauri/capabilities/default.json`
- Modify: `src/main.tsx`
- Modify: `src/styles.css`

**Interfaces:**
- Consumes: typed `NetworkEvent` variants `friendRequestReceived`, `friendRequestDelivered`, `friendRequestResolved`, and `networkError`.
- Produces: `IncomingRequestBanner` with persistent Accept/Reject actions reconstructed from `BootstrapSnapshot.friendRequests`.
- Produces: opt-in `enableSystemNotifications() -> Promise<void>` using the official Tauri notification permission API.
- Produces: backend `request_user_attention(Some(UserAttentionType::Critical))` for every persisted incoming request event.

- [ ] **Step 1: Add the official notification plugin and permissions**

  Run:

  ```powershell
  pnpm add @tauri-apps/plugin-notification@^2
  cargo add --manifest-path src-tauri/Cargo.toml tauri-plugin-notification@2
  ```

  Register `tauri_plugin_notification::init()` in `lib.rs` and add `"notification:default"` to the main-window capability. This is the only new production dependency and supplies the explicitly requested Windows/macOS system notification feature.

- [ ] **Step 2: Request attention from the persisted backend event**

  Before emitting `NetworkEvent::FriendRequestReceived`, obtain the `main` webview window from `AppHandle` and call:

  ```rust
  window.request_user_attention(Some(tauri::UserAttentionType::Critical))
  ```

  Log failure at debug level; never fail or roll back the already-persisted incoming request.

- [ ] **Step 3: Replace the discarded event listener with a typed handler**

  Define a discriminated TypeScript `NetworkEvent` union matching the Rust camelCase payload. The listener must always call `scheduleRefresh()` and additionally:

  ```ts
  friendRequestReceived  -> announce requester; notify only when unfocused and already permitted
  friendRequestDelivered -> toast "好友申请已送达"
  friendRequestResolved  -> select the accepted friend after snapshot refresh
  networkError           -> toast the backend message
  ```

  The Add Friend command must show no delivery-success toast; the `friendRequestDelivered` event is the only success signal.

- [ ] **Step 4: Implement opt-in native notification permission**

  Import `isPermissionGranted`, `requestPermission`, and `sendNotification` from `@tauri-apps/plugin-notification`, plus `getCurrentWindow` from `@tauri-apps/api/window`. Add a user-initiated control in the existing profile/settings dialog. It explains that notifications reveal only the requester nickname, requests permission only on click, and renders enabled/denied/not-enabled state. Incoming events call `sendNotification` only when `isPermissionGranted()` is already true and `isFocused()` is false.

- [ ] **Step 5: Add the persistent main-content confirmation banner**

  Wrap the right-side content in `.content-shell` and render the first pending incoming request above the conversation/welcome content. The banner shows nickname, platform from the matching peer record, total pending count, and text buttons **拒绝** and **接受**. Use `role="region"`, an accessible label, and a separate `aria-live="polite"` status node. Keep the existing sidebar request list as a secondary surface.

- [ ] **Step 6: Style complete interaction states**

  Add scoped styles for banner hierarchy, pending count, Accept/Reject hover/focus/disabled/loading states, notification setting status, narrow widths, and the new content grid. Preserve the current sidebar, conversation scrolling, light palette, and minimum 880×600 window behavior.

- [ ] **Step 7: Verify frontend, capabilities, and Rust integration**

  Run:

  ```powershell
  pnpm typecheck
  pnpm build
  cargo check --manifest-path src-tauri/Cargo.toml
  cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
  git diff --check
  ```

- [ ] **Step 8: Commit the notification flow independently**

  ```powershell
  git add package.json pnpm-lock.yaml src/main.tsx src/styles.css src-tauri/Cargo.toml src-tauri/Cargo.lock src-tauri/src/lib.rs src-tauri/src/network/runtime.rs src-tauri/capabilities/default.json
  git commit -m "feat(friends): add actionable request notifications"
  ```

### Task 4: Product rename, version 0.1.4, and package metadata

**Files:**
- Modify: `package.json`
- Modify: `pnpm-lock.yaml`
- Modify: `index.html`
- Modify: `src/main.tsx`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/Cargo.lock`
- Modify: `src-tauri/tauri.conf.json`
- Create: `src-tauri/windows/installer-hooks.nsh`
- Modify: `src-tauri/Info.plist`
- Modify: `src-tauri/capabilities/default.json`
- Modify: `src-tauri/src/network/behaviour.rs`
- Modify: `src-tauri/src/error.rs`
- Modify: `src-tauri/src/identity.rs`
- Modify: `src-tauri/src/lib.rs`
- Modify: `src-tauri/src/network/runtime.rs`
- Modify: `src-tauri/src/state.rs`
- Modify: `src-tauri/src/storage.rs`
- Modify: `.github/workflows/build-macos.yml`

**Interfaces:**
- Produces: visible product and agent name `Weline Localnet` and version `0.1.4`.
- Preserves: identifier `com.aiweline.localnet`, data filenames, keyring/service identity, Peer ID, and wire protocols.
- Produces artifacts named `Weline_Localnet_0.1.4_x64-setup.exe`, `Weline_Localnet_0.1.4_x64-portable.exe`, and `Weline_Localnet_0.1.4_universal.dmg`.

- [ ] **Step 1: Replace active product copy and bump all package versions**

  Update UI copy, runtime errors/logs, window title, HTML title, Cargo description/agent version, capability description, Local Network purpose text, `package.json`, `Cargo.toml`, `tauri.conf.json`, and generated lockfile package versions. Preserve the historical specs/plans and do not change `identifier`.

- [ ] **Step 2: Rename macOS workflow outputs**

  Change the collection output to:

  ```bash
  output="release/macos/Weline_Localnet_${version}_universal.dmg"
  ```

  Keep bundle-version, architecture, mounted-app, Local Network purpose, and Bonjour verification. Rename the uploaded artifact to `weline-localnet-macos-universal`.

- [ ] **Step 3: Prove the stable-identity migration inputs did not change**

  Run:

  ```powershell
  rg -n 'com\.aiweline\.localnet|localnet\.sqlite3|CONTROL_PROTOCOL|FILE_PROTOCOL|EVENT_NAME|DISCOVERY_MAGIC' src-tauri src
  rg -n 'Weline Chat|WELINE CHAT|0\.1\.3' index.html package.json src src-tauri .github/workflows
  ```

  Expected: stable identifiers/protocol strings remain; no active-source old product name or old app version remains. Historical documentation is excluded from this assertion.

- [ ] **Step 4: Verify and commit the rename/version boundary**

  Run all typecheck/build/check/fmt/diff checks, then commit only the listed metadata/copy files with `git commit -m "release: rename app to Weline Localnet 0.1.4"`.

### Task 5: Real-process, upgrade, cross-device, and package acceptance

**Files:**
- Generate ignored: `release/windows/Weline_Localnet_0.1.4_*`
- Generate ignored: `release/macos/Weline_Localnet_0.1.4_*`
- Do not persist diagnostics or test-only configuration.

**Interfaces:**
- Consumes: versioned Windows build and GitHub macOS universal workflow.
- Produces: only the three 0.1.4 binaries and matching SHA-256 files in `release/`.

- [ ] **Step 1: Run full local gates from clean source state**

  Run `pnpm install --frozen-lockfile`, `pnpm typecheck`, `pnpm build`, `cargo check`, `cargo fmt --check`, `git diff --check`, and `git status --short`. Confirm no temporary tests remain.

- [ ] **Step 2: Exercise discovery without mutating host routes**

  Start isolated debug instances with distinct `LOCALNET_DATA_DIR` values and the existing mDNS-disable diagnostic flag to confirm UDP broadcast/active probe still reach Hello. Separately run the Windows raw-mDNS listener against the physical Mac response and confirm the log/dial uses `/ip4/192.168.31.22/tcp/...`, not loopback or TUN addresses.

- [ ] **Step 3: Build and inspect Windows packages**

  Run `pnpm tauri build --bundles nsis`, copy the setup binary, and copy the release executable as portable. Inspect the setup manifest for `asInvoker`, confirm current-user install mode and all ten configured installer languages, and calculate SHA-256. Install 0.1.4 over the existing 0.1.3 current-user installation; verify the same SQLite records and Peer ID remain and no installer/UAC loop appears.

- [ ] **Step 4: Perform real Windows GUI smoke**

  Launch the installed app and verify the Weline Localnet title, persistent request banner rendering, Accept/Reject interaction, notification opt-in control, no mojibake, no repeated authorization dialogs, and normal conversation layout at 1180×760 and minimum 880×600.

- [ ] **Step 5: Build and inspect the universal macOS DMG**

  Push the scoped branch, dispatch `.github/workflows/build-macos.yml`, download the artifact, verify its SHA-256 and mounted `.app` metadata, and confirm both `arm64` and `x86_64` slices plus the Local Network purpose/Bonjour declarations.

- [ ] **Step 6: Perform final two-device acceptance with TUN on and off**

  Install current 0.1.4 on the physical Windows and Mac devices. In both TUN states, require bidirectional discovery within 15 seconds, Mac-to-Windows friend-request attention plus persistent Accept/Reject, acceptance on both peers, bidirectional text, and a bidirectional file whose SHA-256 matches at sender and receiver. Also reject one request and force one delivery failure to prove no friendship/false pending state remains.

- [ ] **Step 7: Publish only latest artifacts**

  Move older 0.1.3 files out of ignored `release/` into the existing recoverable release archive, then leave only 0.1.4 Windows setup, Windows portable, macOS universal DMG, and their checksum files. Verify final names, sizes, hashes, commit IDs, and `git status` before reporting completion.
