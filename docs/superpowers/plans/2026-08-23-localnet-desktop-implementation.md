# Localnet Desktop Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build, package, and exercise a Windows/macOS Localnet client that discovers peers on the LAN, requires accepted friend relationships, and transfers text, images, and files directly between peers.

**Architecture:** A React/TypeScript renderer calls a narrow Tauri command API and subscribes to application events. A Rust core owns a persistent Ed25519 identity, SQLite state, a rust-libp2p swarm using mDNS + TCP + Noise + Yamux, CBOR control messages, and stream-oriented file transfer. Windows is built and launched locally; a private GitHub repository and macOS runner produce the universal `.dmg` artifact.

**Tech Stack:** Tauri 2, React 19, TypeScript, Vite, Rust stable, rust-libp2p 0.56, libp2p-stream 0.4 alpha, Tokio, rusqlite, keyring, serde, pnpm, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-23-localnet-desktop-design.md`

## Global Constraints

- Target Windows 10/11 x64 and macOS 12+ on Apple Silicon and Intel.
- No account, cloud service, central server, internet relay, cross-subnet fallback, or offline delivery.
- Only accepted friends can send text, images, or files.
- Text is limited to 16 KiB, images to 25 MB for automatic receipt, and files to 2 GB.
- Identity, friends, messages, and transfers survive restart in local storage.
- Network and file access remain in Rust; the renderer receives only typed commands/events.
- Do not load remote scripts, pages, fonts, or CDN assets.
- Do not add automated unit or E2E test files; use formatting, type checks, Rust checks, builds, and real two-instance/manual acceptance.
- Never claim macOS completion from source inspection; download and inspect the actual GitHub Actions artifact.

---

### Task 1: Toolchain and Buildable Tauri Shell

**Files:**
- Create: `package.json`
- Create: `pnpm-lock.yaml`
- Create: `index.html`
- Create: `tsconfig.json`
- Create: `tsconfig.node.json`
- Create: `vite.config.ts`
- Create: `assets/app-icon.svg`
- Create: `src/main.tsx`
- Create: `src/vite-env.d.ts`
- Create: `src-tauri/Cargo.toml`
- Create: `src-tauri/build.rs`
- Create: `src-tauri/tauri.conf.json`
- Create: `src-tauri/capabilities/default.json`
- Create: `src-tauri/src/main.rs`
- Create: `src-tauri/src/lib.rs`

**Interfaces:**
- Produces: Vite dev server on `127.0.0.1:1420`; Rust crate `localnet`; Tauri library entry `localnet::run()`.
- Produces: npm scripts `dev`, `build`, `typecheck`, `tauri`, and `check`.

- [ ] **Step 1: Install the Windows native toolchain**

Run in PowerShell:

```powershell
winget install --id Rustlang.Rustup -e --source winget --accept-package-agreements --accept-source-agreements
winget install --id Microsoft.VisualStudio.2022.BuildTools -e --source winget --accept-package-agreements --accept-source-agreements --override "--wait --passive --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"
```

Open a new PowerShell process and run:

```powershell
rustup default stable
rustc --version
cargo --version
```

Expected: stable `rustc` and `cargo` versions print successfully.

- [ ] **Step 2: Create the frontend manifest and compiler configuration**

Use this dependency boundary in `package.json`:

```json
{
  "name": "localnet",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build",
    "typecheck": "tsc -b --pretty false",
    "tauri": "tauri",
    "check": "pnpm typecheck && pnpm build"
  },
  "dependencies": {
    "@tauri-apps/api": "^2",
    "@tauri-apps/plugin-dialog": "^2",
    "@tauri-apps/plugin-opener": "^2",
    "lucide-react": "^0.542.0",
    "react": "^19.1.1",
    "react-dom": "^19.1.1"
  },
  "devDependencies": {
    "@tauri-apps/cli": "^2",
    "@types/react": "^19",
    "@types/react-dom": "^19",
    "@vitejs/plugin-react": "^5",
    "typescript": "^5.9.2",
    "vite": "^7.1.3"
  }
}
```

Configure Vite to bind only `127.0.0.1`, use strict port `1420`, and ignore `src-tauri` during file watching.

- [ ] **Step 3: Create the Rust crate and Tauri configuration**

Use these Rust features in `src-tauri/Cargo.toml`:

```toml
[dependencies]
tauri = { version = "2", features = [] }
tauri-plugin-dialog = "2"
tauri-plugin-opener = "2"
tauri-plugin-single-instance = "2"
tokio = { version = "1", features = ["fs", "io-util", "macros", "rt-multi-thread", "sync", "time"] }
libp2p = { version = "0.56", features = ["cbor", "dns", "identify", "macros", "mdns", "noise", "request-response", "serde", "tcp", "tokio", "yamux"] }
libp2p-stream = "0.4.0-alpha"
rusqlite = { version = "0.37", features = ["bundled"] }
keyring = { version = "3", features = ["apple-native", "windows-native"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["serde", "v7"] }
chrono = { version = "0.4", features = ["serde"] }
sha2 = "0.10"
hex = "0.4"
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
futures = "0.3"
```

Set Tauri identifier to `com.aiweline.localnet`, product name to `Localnet`, CSP to local assets only, minimum window size to `880x600`, and bundle targets to `nsis`, `msi`, `app`, and `dmg`.

- [ ] **Step 4: Generate icons and install locked dependencies**

Run:

```powershell
pnpm install
pnpm tauri icon assets/app-icon.svg
```

Expected: `pnpm-lock.yaml` and `src-tauri/icons/` exist.

- [ ] **Step 5: Verify the empty desktop shell**

Run:

```powershell
pnpm typecheck
cargo check --manifest-path src-tauri/Cargo.toml
pnpm build
```

Expected: all commands exit `0`.

- [ ] **Step 6: Commit the shell**

```powershell
git add package.json pnpm-lock.yaml index.html tsconfig.json tsconfig.node.json vite.config.ts assets src src-tauri
git commit -m "build: scaffold Localnet desktop client"
```

---

### Task 2: Domain Model, Identity, and SQLite Persistence

**Files:**
- Create: `src-tauri/src/error.rs`
- Create: `src-tauri/src/domain.rs`
- Create: `src-tauri/src/identity.rs`
- Create: `src-tauri/src/storage.rs`
- Create: `src-tauri/src/state.rs`
- Modify: `src-tauri/src/lib.rs`

**Interfaces:**
- Produces: `AppError`, serializable `ErrorPayload`, `LocalProfile`, `PeerSummary`, `FriendRequest`, `Friend`, `ChatMessage`, `TransferRecord`, and their status enums.
- Produces: `IdentityService::load_or_create(app_data_dir: &Path) -> Result<LocalIdentity, AppError>`.
- Produces: `Storage::open(path: &Path)`, `snapshot()`, `save_profile()`, `upsert_peer()`, `put_friend_request()`, `resolve_friend_request()`, `insert_message()`, `update_message_status()`, and `upsert_transfer()`.
- Produces: `AppState { storage, identity, network }` using `Arc` and Tokio synchronization primitives.

- [ ] **Step 1: Define serializable domain types and limits**

Define exact constants in `domain.rs`:

```rust
pub const MAX_NICKNAME_CHARS: usize = 32;
pub const MAX_TEXT_BYTES: usize = 16 * 1024;
pub const MAX_AUTO_IMAGE_BYTES: u64 = 25 * 1024 * 1024;
pub const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const PROTOCOL_VERSION: u16 = 1;
```

All IDs crossing the GUI boundary are strings. Timestamps are RFC 3339 strings. Model friend state as `PendingOutgoing`, `PendingIncoming`, `Accepted`, or `Rejected`; model message state as `Sending`, `Delivered`, or `Failed`; model transfer state as `AwaitingAcceptance`, `Transferring`, `Completed`, `Cancelled`, or `Failed`.

- [ ] **Step 2: Implement application errors**

Create `AppError` variants for invalid input, storage, identity, network, permission, offline peer, non-friend peer, incompatible protocol, transfer rejection, integrity failure, and IO. Implement `serde::Serialize` via a stable payload:

```rust
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorPayload {
    pub code: &'static str,
    pub message: String,
}
```

User-facing error strings must describe a recovery action and must never contain a private key or full local path.

- [ ] **Step 3: Implement stable identity storage**

Use keyring service `com.aiweline.localnet` and username `device-ed25519`; store the protobuf-encoded libp2p keypair as base64. Fall back to `identity.key` under the app data directory only when the platform keyring is unavailable, create it atomically, and restrict its Windows ACL/macOS mode to the current user as far as the platform API permits. Derive Peer ID from the public key and never regenerate an existing identity on parse failure; return a visible identity error instead.

- [ ] **Step 4: Create and migrate SQLite storage**

On `Storage::open`, execute one transaction that enables foreign keys and creates `settings`, `peers`, `friend_requests`, `friends`, `messages`, and `transfers`. Add unique constraints on Peer ID, request ID, message ID, and transfer ID. Store status strings from the Rust enums and reject unknown values on read instead of silently defaulting.

- [ ] **Step 5: Wire startup state**

`localnet::run()` resolves Tauri's app data directory, initializes logging, identity, SQLite, and managed `AppState` before creating the main window. In debug builds only, honor `LOCALNET_DATA_DIR` and disable the single-instance plugin when that variable is present so two isolated local clients can run for acceptance. Startup failure shows one native fatal error dialog and exits before network services start.

- [ ] **Step 6: Verify persistence code**

Run:

```powershell
cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
cargo check --manifest-path src-tauri/Cargo.toml
```

Expected: formatting and type checks succeed without warnings from Localnet source.

- [ ] **Step 7: Commit identity and storage**

```powershell
git add src-tauri/src
git commit -m "feat: persist Localnet identity and conversations"
```

---

### Task 3: LAN Discovery and Typed Network Runtime

**Files:**
- Create: `src-tauri/src/protocol.rs`
- Create: `src-tauri/src/network/mod.rs`
- Create: `src-tauri/src/network/behaviour.rs`
- Create: `src-tauri/src/network/runtime.rs`
- Modify: `src-tauri/src/state.rs`
- Modify: `src-tauri/src/lib.rs`

**Interfaces:**
- Produces: `ControlRequest`, `ControlResponse`, `HelloPayload`, `TransferOffer`, and protocol paths `/localnet/control/1` and `/localnet/file/1`.
- Produces: `NetworkCommand::{SetProfile, SendFriendRequest, ResolveFriendRequest, SendText, OfferTransfer, AcceptTransfer, CancelTransfer, Shutdown}`.
- Produces: `NetworkEvent::{PeerDiscovered, PeerOffline, FriendRequestReceived, FriendRequestResolved, MessageReceived, MessageStatusChanged, TransferOffered, TransferProgress, TransferStatusChanged, NetworkError}`.
- Produces: `NetworkHandle::send(NetworkCommand) -> Result<(), AppError>` and `spawn_network(identity, profile, event_sink) -> NetworkHandle`.

- [ ] **Step 1: Define versioned protocol envelopes**

Use CBOR request/response with an explicit version field:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ControlRequest {
    Hello { version: u16, nickname: String, platform: Platform },
    FriendRequest { request_id: String, nickname: String },
    FriendDecision { request_id: String, accepted: bool, nickname: String },
    TextMessage { message_id: String, sent_at: String, body: String },
    TransferOffer { offer: TransferOffer },
    TransferDecision { transfer_id: String, accepted: bool },
    TransferCancel { transfer_id: String },
}
```

Responses are `Accepted`, `Rejected { code, message }`, or `Hello(HelloPayload)`. Validate version, nickname, body size, file size, file name, and MIME string before touching storage.

- [ ] **Step 2: Compose libp2p behaviours**

Derive `NetworkBehaviour` for mDNS, Identify, CBOR request-response, and `libp2p_stream::Behaviour`. Build the swarm with Tokio + TCP + Noise + Yamux, listen on `/ip4/0.0.0.0/tcp/0`, and insert mDNS addresses through `Swarm::add_peer_address`.

- [ ] **Step 3: Implement the network event loop**

Run one Tokio task that `select!`s swarm events and `mpsc` commands. On discovery, dial only if not self; after connection, exchange hello data and emit a `PeerDiscovered`. Track each peer's addresses and active connection count. Emit `PeerOffline` only after all connections are closed and mDNS expiry has removed all addresses.

- [ ] **Step 4: Enforce trust boundaries**

Before dispatching inbound text or transfer requests, query `Storage::is_friend(peer_id)`. Allow `Hello` and `FriendRequest` from non-friends, rate-limit new friend requests per Peer ID, and reject all other requests with code `not_friend`. Never accept the nickname inside a chat message as identity evidence.

- [ ] **Step 5: Expose runtime events to Tauri**

Serialize network events under one event name, `localnet://event`, using a tagged payload. Store each accepted event before emitting it to the renderer so a renderer refresh cannot lose a completed friend decision or received message.

- [ ] **Step 6: Verify discovery in two local processes**

Run two development instances with separate data roots:

```powershell
$env:LOCALNET_DATA_DIR = "$env:TEMP\localnet-a"
pnpm tauri dev
```

Start the second process with `LOCALNET_DATA_DIR` ending in `localnet-b`. Expected: each window shows the other's nickname within 10 seconds and does not show itself.

- [ ] **Step 7: Commit discovery runtime**

```powershell
git add src-tauri/src
git commit -m "feat: discover Localnet peers on the LAN"
```

---

### Task 4: Friend Requests and Text Messaging Commands

**Files:**
- Create: `src-tauri/src/commands.rs`
- Modify: `src-tauri/src/network/runtime.rs`
- Modify: `src-tauri/src/storage.rs`
- Modify: `src-tauri/src/lib.rs`

**Interfaces:**
- Produces Tauri commands: `bootstrap()`, `complete_onboarding(nickname)`, `update_nickname(nickname)`, `send_friend_request(peer_id)`, `resolve_friend_request(request_id, accept)`, `send_text(peer_id, body)`, and `retry_message(message_id)`.
- `bootstrap()` returns one `BootstrapSnapshot` containing local profile, peers, requests, friends, messages for the selected friend, and transfers.

- [ ] **Step 1: Implement command input validation**

Normalize nicknames with `trim`, require 1–32 Unicode scalar values, reject control characters, and normalize text line endings. Reject empty text or body over 16 KiB. Parse every Peer ID and UUID at the command boundary and return typed errors.

- [ ] **Step 2: Implement friend request state transitions**

Create outgoing request records before network dispatch. Accept only incoming pending requests. On acceptance, write the friend row and resolved request in one SQLite transaction, then notify the peer. Handle duplicate decisions idempotently and do not create duplicate friend rows.

- [ ] **Step 3: Implement text send and receipt**

Insert outgoing messages with `Sending`, send only to an online accepted friend, and mark `Delivered` only after a positive remote response. Convert timeouts, connection loss, or rejection into `Failed` with a retryable reason. Insert inbound message before acknowledging it and deduplicate by message ID.

- [ ] **Step 4: Register the narrow Tauri API**

Register only the named commands with `tauri::generate_handler!`. Do not expose arbitrary SQL, shell, file read, or network address commands.

- [ ] **Step 5: Run the two-instance friend and text smoke**

Verify: request appears once, rejection leaves both non-friends, re-request can be accepted, accepted users move to Friends, two-way messages deliver once, and closing one app makes sending unavailable with an honest offline reason.

- [ ] **Step 6: Commit friend and text flows**

```powershell
git add src-tauri/src
git commit -m "feat: add accepted-friend text messaging"
```

---

### Task 5: Streamed Image and File Transfer

**Files:**
- Create: `src-tauri/src/transfer.rs`
- Modify: `src-tauri/src/protocol.rs`
- Modify: `src-tauri/src/network/runtime.rs`
- Modify: `src-tauri/src/commands.rs`
- Modify: `src-tauri/src/storage.rs`
- Modify: `src-tauri/src/lib.rs`

**Interfaces:**
- Produces commands: `offer_file(peer_id, path, kind)`, `accept_transfer(transfer_id, destination)`, `reject_transfer(transfer_id)`, `cancel_transfer(transfer_id)`, `retry_transfer(transfer_id)`, and `reveal_in_file_manager(path)`.
- Produces: `TransferHeader { transfer_id, size, sha256 }` encoded as a length-prefixed CBOR frame followed by raw chunks up to 256 KiB.
- Produces: transfer progress events throttled to at most 10 per second per transfer.

- [ ] **Step 1: Validate outbound file metadata**

Resolve metadata without following unsafe links, require a regular readable file, enforce 2 GB maximum and 25 MB automatic-image maximum, sanitize the display name, and compute SHA-256 using streaming reads. Never load the whole file into memory.

- [ ] **Step 2: Implement transfer offers and decisions**

Persist an `AwaitingAcceptance` record and send `TransferOffer`. Images from accepted friends at or below 25 MB choose a collision-safe path under `app_data/images`; normal files remain pending until the recipient selects a destination. Reject non-friends, invalid metadata, existing transfer IDs, and unavailable disk locations.

- [ ] **Step 3: Implement outbound stream writing**

Open `/localnet/file/1` with `Control::open_stream`, write the framed header, then copy exactly `size` bytes from the file using a 256 KiB buffer. Check cancellation between chunks, update progress through a bounded channel, and close the stream cleanly.

- [ ] **Step 4: Implement inbound stream writing and integrity checks**

Use `Control::accept` continuously. Match the transfer ID to an accepted pending record and the connected Peer ID, write to `destination.part` with create-new semantics, enforce the offered byte count, compute SHA-256 while writing, flush and sync, verify the hash, then atomically rename. On cancellation, overflow, EOF, hash mismatch, or write failure, remove the partial file and persist a failed/cancelled state.

- [ ] **Step 5: Implement retry and recovery**

On startup, mark in-progress records failed and remove only `.part` paths owned by those records. Retry creates a new stream for the same immutable metadata, resets transferred bytes to zero, and never overwrites a completed destination.

- [ ] **Step 6: Run transfer smokes**

Between two local instances, verify a small image preview, a 1 MB file, a file larger than 256 KiB, cancellation, recipient rejection, disconnect failure, retry, duplicate filename collision handling, and byte-for-byte SHA-256 equality. Observe process memory during a large transfer and confirm it stays bounded.

- [ ] **Step 7: Commit transfers**

```powershell
git add src-tauri/src
git commit -m "feat: stream images and files between friends"
```

---

### Task 6: Production GUI and Interaction States

**Files:**
- Create: `src/app/types.ts`
- Create: `src/app/api.ts`
- Create: `src/app/useLocalnet.ts`
- Create: `src/app/App.tsx`
- Create: `src/components/Onboarding.tsx`
- Create: `src/components/Sidebar.tsx`
- Create: `src/components/FriendRequests.tsx`
- Create: `src/components/ChatPane.tsx`
- Create: `src/components/MessageBubble.tsx`
- Create: `src/components/TransferCard.tsx`
- Create: `src/components/EmptyState.tsx`
- Create: `src/styles/tokens.css`
- Create: `src/styles/app.css`
- Modify: `src/main.tsx`

**Interfaces:**
- `api.ts` exposes one typed function per Tauri command and `subscribeToEvents(handler) -> Promise<UnlistenFn>`.
- `useLocalnet()` owns bootstrap, event reduction, selection, optimistic states, toast queue, and action methods.
- Presentational components receive data and callbacks only; they never call Tauri directly.

- [ ] **Step 1: Mirror Rust payloads in TypeScript**

Use discriminated unions for event, request, message, and transfer status. No `any`; unknown event tags are logged once and ignored without corrupting existing state.

- [ ] **Step 2: Build onboarding and application state**

Show onboarding only when `bootstrap.localProfile` is absent. Disable submission until nickname is valid, show the error beside the field, and focus the nickname input on mount. `useLocalnet` performs a full bootstrap once and applies subsequent events idempotently by ID.

- [ ] **Step 3: Build the left discovery sidebar**

Render the local profile, pending-request badge, accepted friends with online dots/unread counts, and nearby non-friends with platform icon and Add button. Disable Add while pending. Sort incoming requests first, online friends second, offline friends third, and nearby users last.

- [ ] **Step 4: Build friend request and chat panes**

Friend requests expose Accept and Reject with inline progress. Chat header shows nickname and honest online status. The composer supports Enter to send, Shift+Enter for newline, image/file pickers, disabled/offline explanation, and visible send errors. The message list keeps the user's scroll position unless already near the bottom.

- [ ] **Step 5: Build image and transfer presentation**

Render safe local image URLs through Tauri's asset conversion. Transfer cards show file name, human-readable size, progress, current state, cancel/retry, and Reveal after completion. Failed states include the actionable reason without exposing sensitive paths.

- [ ] **Step 6: Apply the visual system and accessibility**

Use CSS custom properties for light/dark colors, 8px spacing rhythm, a 300px sidebar, 44px minimum interactive targets, visible focus rings, reduced-motion handling, and a responsive collapse below 760px. Use only bundled system fonts and lucide icons. Provide empty, loading, error, offline, pending, sending, success, and cancelled states.

- [ ] **Step 7: Verify frontend and real Windows interaction**

Run:

```powershell
pnpm typecheck
pnpm build
cargo check --manifest-path src-tauri/Cargo.toml
pnpm tauri dev
```

Inspect onboarding, nearby discovery, request decisions, two-way chat, image preview, file progress, light/dark mode, keyboard use, narrow window, sleep/wake, and app restart in real Tauri windows.

- [ ] **Step 8: Commit the GUI**

```powershell
git add src package.json pnpm-lock.yaml src-tauri
git commit -m "feat: deliver the Localnet desktop experience"
```

---

### Task 7: Windows Installer, macOS Artifact, and Delivery Audit

**Files:**
- Create: `.github/workflows/build-desktop.yml`
- Create: `README.md`
- Create: `docs/verification/2026-08-23-release-evidence.md`
- Modify: `src-tauri/tauri.conf.json`

**Interfaces:**
- Produces: local Windows `.exe` app plus NSIS/MSI installer under `src-tauri/target/release/bundle/`.
- Produces: GitHub Actions artifact `Localnet-macOS-universal` containing `.app` and `.dmg`.

- [ ] **Step 1: Add reproducible platform builds**

Create a manually dispatched workflow with least-privilege `contents: read`, pnpm cache, locked install, Rust stable, `pnpm typecheck`, `pnpm build`, and platform matrix. On Windows run `pnpm tauri build --bundles nsis,msi`; on macOS add both Apple targets and run:

```bash
rustup target add aarch64-apple-darwin x86_64-apple-darwin
pnpm tauri build --target universal-apple-darwin --bundles app,dmg
```

Upload bundles with `actions/upload-artifact@v4`, `if-no-files-found: error`, and a 14-day retention.

- [ ] **Step 2: Commit release automation**

```powershell
git add .github README.md src-tauri/tauri.conf.json
git commit -m "ci: package Localnet for Windows and macOS"
```

- [ ] **Step 3: Create and push a private GitHub repository**

Verify the account and root, then run:

```powershell
gh repo create Aiweline/Localnet --private --source . --remote origin --push
```

If the name already exists, inspect it before adding the remote; never overwrite an unrelated repository.

- [ ] **Step 4: Build and launch Windows release**

Run:

```powershell
pnpm check
cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
pnpm tauri build --bundles nsis,msi
```

Verify bundle files exist, install the NSIS/MSI package, launch Localnet from the installed location, and keep the Windows client running for the user.

- [ ] **Step 5: Generate and download the macOS installer**

Run:

```powershell
gh workflow run build-desktop.yml --repo Aiweline/Localnet
$runId = gh run list --repo Aiweline/Localnet --workflow build-desktop.yml --limit 1 --json databaseId --jq '.[0].databaseId'
gh run watch $runId --repo Aiweline/Localnet --exit-status
gh run download $runId --repo Aiweline/Localnet --name Localnet-macOS-universal --dir dist/macos
```

Expected: `dist/macos` contains a non-empty `.dmg` and `.app` bundle archive or directory. Record artifact names, sizes, SHA-256 values, workflow URL, commit, and unsigned/notarization status.

- [ ] **Step 6: Perform the completion audit**

For each spec success criterion, attach evidence from the current build: discovery timing, friend acceptance, text delivery, image preview, file hash, restart persistence, Windows installed launch, and actual macOS artifact. Mark Windows↔macOS live LAN behavior as awaiting user confirmation until the user runs the provided Mac package.

- [ ] **Step 7: Commit and push release evidence**

```powershell
git add docs/verification
git commit -m "docs: record Localnet release evidence"
git push origin main
```
