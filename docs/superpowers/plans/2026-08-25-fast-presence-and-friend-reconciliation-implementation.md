# Fast Presence and Friend Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Weline Localnet discover online peers promptly across physical and virtual LAN paths and keep accepted friendship consistent on both devices.

**Architecture:** Batch all fresh addresses per peer and let libp2p dial them concurrently, while remembered authenticated friend IPs receive a small priority probe. Persist friend decisions in a SQLite outbox and reconcile an already-trusted Peer ID automatically; derive the Add Friend UI from durable relationship state rather than an exact numeric protocol version.

**Tech Stack:** Rust 2024, Tokio, rust-libp2p 0.56, rusqlite, Tauri 2, React 19, TypeScript, Node test runner.

**Spec:** `docs/superpowers/specs/2026-08-25-fast-presence-and-friend-reconciliation-design.md`

## Global Constraints

- Keep discovery and application traffic LAN-only; no cloud rendezvous, relay, MAC-address identity, or automatic trust of unknown peers.
- Preserve `/localnet/control/1`, discovery magic/port, Peer ID, existing SQLite data, v1 files, and v2 capability negotiation.
- Treat discovery packets only as route hints; only Noise-authenticated Hello makes a peer online.
- Runtime and UI changes require stable patch version `0.2.1` in all four version files before merge.
- Do not publish from this feature branch; release only after merge to `main` and GitHub Actions readback.

---

### Task 1: Concurrent discovery candidate planning

**Files:**
- Modify: `src-tauri/src/network/discovery.rs`
- Modify: `src-tauri/src/network/runtime.rs`
- Test: inline Rust tests in those modules

**Interfaces:**
- Produces: `ranked_dial_addresses(addresses, preferred_ip, interfaces) -> Vec<Multiaddr>`.
- Produces: runtime `pending_dials: HashMap<PeerId, Instant>` and a 75 ms flush tick.
- Preserves: `DiscoveryEvent::PeerHint` and existing lease/offline semantics.

- [ ] **Step 1: Write the failing planner tests**

  Add literal tests proving a remembered physical address sorts before TUN/VMware candidates, duplicate addresses collapse, and a second candidate inside the batch is present in the same dial list. The production mutation these catch is returning to one-address `DialOpts` or virtual-first ordering.

- [ ] **Step 2: Run the focused tests and confirm RED**

  Run `cargo test --manifest-path src-tauri/Cargo.toml --lib discovery_candidate --locked -- --nocapture` and require failure because the planner/batch API is absent.

- [ ] **Step 3: Implement minimal ranking and batching**

  Queue peers until `Instant::now() + 75 ms`, collect fresh mDNS/beacon addresses, sort/deduplicate, and build one `DialOpts::peer_id(peer).addresses(all).extend_addresses_through_behaviour()` with `DisconnectedAndNotDialing`. Requeue on `OutgoingConnectionError` only when a fresh lease remains.

- [ ] **Step 4: Run focused tests and existing discovery regressions**

  Run the same focused command, then `cargo test --manifest-path src-tauri/Cargo.toml --lib discovery --locked -- --nocapture`.

### Task 2: Remembered friend probes

**Files:**
- Modify: `src-tauri/src/storage.rs`
- Modify: `src-tauri/src/network/discovery.rs`
- Modify: `src-tauri/src/network/runtime.rs`
- Test: inline Rust storage/discovery/runtime tests

**Interfaces:**
- Produces: nullable `peers.last_lan_ip` migration.
- Produces: `Storage::remember_authenticated_lan_ip(peer_id, ip)` and `Storage::list_friend_lan_targets()`.
- Produces: a bounded 2-second targeted-probe task using the existing probe payload.

- [ ] **Step 1: Write failing persistence and target-selection tests**

  Assert that only private authenticated IPv4 is stored, only accepted friends are returned, matching a `/16` target uses that interface even outside its `/24`, and virtual paths remain fallback candidates.

- [ ] **Step 2: Verify focused RED**

  Run `cargo test --manifest-path src-tauri/Cargo.toml --lib remembered_friend --locked -- --nocapture` and require missing migration/API failures.

- [ ] **Step 3: Implement migration, authenticated persistence, and probe cadence**

  Capture `ConnectedPoint::get_remote_address()` in memory; after Hello upserts the peer, persist its RFC1918 IP. Supply remembered friend targets to `DiscoveryService::spawn`, probe them immediately/every 2 seconds, and keep the full `/24` interval at 12 seconds.

- [ ] **Step 4: Verify GREEN and storage restart behavior**

  Rerun the focused suite and reopen the SQLite fixture before asserting target recovery.

### Task 3: Durable friend-decision reconciliation

**Files:**
- Modify: `src-tauri/src/storage.rs`
- Modify: `src-tauri/src/network/runtime.rs`
- Modify: `src-tauri/src/commands.rs` only if command completion semantics require adjustment
- Test: inline Rust storage/runtime/command tests

**Interfaces:**
- Produces: `pending_friend_decisions(request_id, peer_id, accepted, created_at)` outbox.
- Produces: exact list/ack APIs and `PendingAction::FriendDecision { request_id, peer_id }`.
- Produces: already-friend inbound request reconciliation for the same authenticated Peer ID.

- [ ] **Step 1: Write failing crash/reconnect tests**

  Cover acceptance persisted before send failure, SQLite reopen, Hello-triggered retry, wrong-peer ACK retention, exact-peer ACK retirement, and already-friend duplicate request healing. These tests must exercise real SQLite transactions and production runtime handlers.

- [ ] **Step 2: Verify RED**

  Run `cargo test --manifest-path src-tauri/Cargo.toml --lib friend_decision --locked -- --nocapture` and require failures for the missing outbox/retry behavior.

- [ ] **Step 3: Implement the transactional outbox and retry path**

  Insert the decision in the same transaction as request/friend resolution, seed historical incoming terminal requests during migration, retry after Hello, and delete only on exact accepted response. Keep failed sends durable.

- [ ] **Step 4: Implement already-trusted reconciliation and verify GREEN**

  For an inbound request from a Peer ID already in `friends`, persist that request as accepted plus outbox, acknowledge delivery, and send the durable decision without showing a new approval prompt. Run the focused tests.

### Task 4: Cross-version nearby-user presentation

**Files:**
- Modify: `src/presence.ts`
- Modify: `src/main.tsx`
- Modify: `tests/presence.test.ts`

**Interfaces:**
- Produces: `nearbyPeerPresentation(peers, friends, requests, localPeerId)` with explicit addability.
- Preserves: snapshot reconciliation and backend command validation.

- [ ] **Step 1: Write failing table tests**

  Include literal cases for accepted friend, pending outgoing, accepted request, self, protocol version 1, and a future version. The mutation caught is rendering an active Add Friend button for any non-addable relationship or exact-version gating.

- [ ] **Step 2: Verify RED**

  Run `pnpm test:presence` and require failure because the presentation helper is absent/current exact-version behavior is wrong.

- [ ] **Step 3: Implement the helper and render its state**

  Remove `protocolVersion === 1` from Add Friend enablement. Use durable friend/request state to exclude or disable Add Friend, while leaving file capabilities untouched.

- [ ] **Step 4: Verify GREEN and TypeScript inclusion**

  Run `pnpm test:presence` and `pnpm typecheck`; confirm `tsconfig.tests.json` still includes the test.

### Task 5: Version, documentation, and release gate

**Files:**
- Modify: `package.json`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/Cargo.lock`
- Create: `docs/releases/v0.2.1.md`
- Modify: `docs/releases/README.md`
- Modify: `README.md` only for behavior that changed

**Interfaces:**
- Produces: synchronized stable version `0.2.1` and bilingual release notes.

- [ ] **Step 1: Synchronize version**

  Run `pnpm release:version 0.2.1` and inspect all four exact files.

- [ ] **Step 2: Document observable fixes**

  Record concurrent best-candidate dialing, remembered-friend priority probing, durable friend reconciliation, cross-version discovery/friend Add behavior, and the unchanged LAN-only/Noise trust boundary.

- [ ] **Step 3: Run complete local gates**

  Run `pnpm test:presence`, `pnpm test:transfer-status`, release-workflow tests, `pnpm typecheck`, `pnpm build`, `cargo fmt --check`, `cargo test --locked`, `cargo check --locked`, `pnpm release:check`, and `git diff --check`.

- [ ] **Step 4: Perform non-destructive runtime acceptance**

  Reuse isolated `LOCALNET_DATA_DIR` fixtures only if doing so does not trigger new firewall/permission prompts. Otherwise report that real Windows-to-macOS timing remains a required installed-package acceptance and rely on focused socket/planner tests plus GitHub macOS/Windows builds.

- [ ] **Step 5: Finish branch and publish through main**

  Review the scoped diff, commit, push the feature branch, merge only after checks pass, then verify the `v0.2.1` tag, immutable GitHub Release, exact five assets, checksums, and public download endpoints.

