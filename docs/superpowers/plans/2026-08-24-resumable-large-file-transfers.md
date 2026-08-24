# Resumable Large File Transfers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver Weline Localnet 0.2.0 with 100 GiB transfers, per-chunk SHA-256 validation, automatic network/app-restart resume, destination-volume preflight, and exact bilingual GitHub release notes.

**Architecture:** Keep `/localnet/file/1` for 0.1.x compatibility and negotiate `/localnet/file/2` through an optional Hello capability. Protocol v2 persists a 4 MiB chunk manifest, receiver-acknowledged offsets, claims, partial paths, and pause state in SQLite; a platform volume module gates acceptance and resume. Runtime reconnect events query receiver-authoritative progress and reopen only the uncommitted suffix.

**Tech Stack:** Rust 1.85, Tokio, libp2p request-response/stream, rusqlite, SHA-256, target-specific Windows APIs and macOS `statfs`, Tauri 2, React 19, TypeScript, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-24-resumable-large-file-transfers-design.md`

## Global Constraints

- Protocol v2 supports files up to and including 100 GiB; all size/offset arithmetic is checked `u64` arithmetic.
- Protocol v1 remains available and keeps its 2 GiB maximum for 0.1.x peers.
- Chunks are 4 MiB except the final chunk; committed progress advances only after disk synchronization, SQLite commit, and acknowledgement.
- Recoverable network/app restart preserves v2 partial files and reservations; cancellation, source mutation, identity mismatch, and integrity failure are terminal.
- Acceptance and resume require remaining bytes plus 64 MiB available and reject FAT32 files above `4 GiB - 1 byte`.
- No cloud relay, folder transfer, compression, deduplication, multi-source download, or user-configurable maximum is added.
- Production changes use Red → Green → Refactor and no release is created outside `main`.

---

### Task 1: Capability negotiation and transfer-size policy

**Files:**
- Create: `src-tauri/src/transfer_policy.rs`
- Modify: `src-tauri/src/domain.rs`
- Modify: `src-tauri/src/protocol.rs`
- Modify: `src-tauri/src/lib.rs`
- Modify: `src-tauri/src/network/runtime.rs`
- Modify: `src-tauri/src/storage.rs`
- Test: `src-tauri/src/transfer_policy.rs`
- Test: `src-tauri/src/protocol.rs`
- Test: `src-tauri/src/storage.rs`

**Interfaces:**
- Produces `LEGACY_MAX_FILE_BYTES`, `DEFAULT_MAX_FILE_BYTES`, `TRANSFER_CHUNK_BYTES`, and `FILE_RESUME_V2_CAPABILITY`.
- Produces `TransferProtocol::{LegacyV1, ResumableV2}` and `select_transfer_protocol(capabilities, file_size)`.
- Extends Hello with default-empty `capabilities: Vec<String>` and persists `peers.capabilities_json`.

- [ ] **Step 1: Write failing size-policy tests**

```rust
#[test]
fn upgraded_peer_accepts_exactly_100_gib() {
    let caps = vec![FILE_RESUME_V2_CAPABILITY.to_string()];
    assert_eq!(select_transfer_protocol(&caps, 100 * GIB).unwrap(), TransferProtocol::ResumableV2);
}

#[test]
fn local_policy_rejects_100_gib_plus_one() {
    assert!(select_transfer_protocol(&[FILE_RESUME_V2_CAPABILITY.into()], 100 * GIB + 1)
        .unwrap_err().to_string().contains("100 GiB"));
}

#[test]
fn legacy_peer_above_2_gib_gets_upgrade_error() {
    assert!(select_transfer_protocol(&[], LEGACY_MAX_FILE_BYTES + 1)
        .unwrap_err().to_string().contains("升级"));
}
```

- [ ] **Step 2: Run `cargo test --manifest-path src-tauri/Cargo.toml transfer_policy --locked` and observe RED because the module is absent.**
- [ ] **Step 3: Implement the minimal policy.**

```rust
pub const LEGACY_MAX_FILE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_MAX_FILE_BYTES: u64 = 100 * 1024 * 1024 * 1024;
pub const TRANSFER_CHUNK_BYTES: u32 = 4 * 1024 * 1024;
pub const FILE_RESUME_V2_CAPABILITY: &str = "file-resume-v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferProtocol { LegacyV1 = 1, ResumableV2 = 2 }
```

- [ ] **Step 4: Write failing compatibility tests that deserialize Hello without `capabilities` and migrate/persist peer capabilities.**
- [ ] **Step 5: Run focused compatibility/storage tests and observe RED from missing fields/columns.**
- [ ] **Step 6: Add optional Hello capability and idempotent peer migration; keep `PROTOCOL_VERSION` and `/localnet/control/1` unchanged.**
- [ ] **Step 7: Run `cargo test --manifest-path src-tauri/Cargo.toml --lib --locked` and require all tests to pass.**
- [ ] **Step 8: Commit `feat: negotiate resumable file transfers`.**

### Task 2: Destination-volume and file-system preflight

**Files:**
- Create: `src-tauri/src/volume_preflight.rs`
- Modify: `src-tauri/src/lib.rs`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/Cargo.lock`
- Test: `src-tauri/src/volume_preflight.rs`

**Interfaces:**
- Produces `VolumeSnapshot { filesystem, available_bytes, max_file_bytes }`.
- Produces pure `validate_volume(snapshot, file_size, committed_bytes)`.
- Produces platform `inspect_volume(directory)` and `preflight_destination(directory, file_size, committed_bytes)`.

- [ ] **Step 1: Write failing policy tests for exact remaining-plus-64-MiB success, one byte short, FAT32 above 4 GiB, 100 GiB on NTFS/APFS/exFAT, and committed-byte subtraction.**

```rust
#[test]
fn fat32_rejects_a_five_gib_file() {
    let snapshot = VolumeSnapshot::known("FAT32", 10 * GIB, Some(4 * GIB - 1));
    assert!(validate_volume(&snapshot, 5 * GIB, 0).unwrap_err().to_string().contains("FAT32"));
}
```

- [ ] **Step 2: Run `cargo test --manifest-path src-tauri/Cargo.toml volume_preflight --locked` and observe RED.**
- [ ] **Step 3: Implement pure checked policy and actionable errors.**
- [ ] **Step 4: Add Windows `windows-sys 0.61.2` with `Win32_Storage_FileSystem`; call `GetVolumePathNameW`, `GetVolumeInformationW`, and `GetDiskFreeSpaceExW`.**
- [ ] **Step 5: Add macOS `libc 0.2.189`; call `statfs`, calculate `f_bavail * f_bsize`, and normalize `f_fstypename`.**
- [ ] **Step 6: Run volume tests, `cargo check --locked`, and `cargo fmt --check`.**
- [ ] **Step 7: Commit `feat: validate transfer destination volumes`.**

### Task 3: Chunk manifest preparation and SQLite migration

**Files:**
- Create: `src-tauri/src/transfer_manifest.rs`
- Modify: `src-tauri/src/lib.rs`
- Modify: `src-tauri/src/domain.rs`
- Modify: `src-tauri/src/storage.rs`
- Modify: `src-tauri/src/commands.rs`
- Test: `src-tauri/src/transfer_manifest.rs`
- Test: `src-tauri/src/storage.rs`

**Interfaces:**
- Produces `TransferChunk { index, length, sha256 }` and `TransferManifest { file_size, file_sha256, manifest_sha256, chunks, source_modified_ns }`.
- Produces `build_manifest(path, chunk_size)` and canonical `manifest_root(chunks)`.
- Produces `replace_outgoing_chunks`, `list_transfer_chunks`, and atomic `commit_received_chunk` storage methods.

- [ ] **Step 1: Write failing deterministic two-chunk tests for exact hashes, lengths, canonical manifest root, empty files, and chunk-count overflow.**
- [ ] **Step 2: Run `cargo test --manifest-path src-tauri/Cargo.toml transfer_manifest --locked` and observe RED.**
- [ ] **Step 3: Implement a single streaming pass that builds whole-file and per-chunk hashes without loading the file.**
- [ ] **Step 4: Write failing migration tests for all v2 columns plus `transfer_chunks`, and atomic next-chunk commit/rejection.**
- [ ] **Step 5: Run storage tests and observe RED.**
- [ ] **Step 6: Add idempotent transfer migrations and 32-byte BLOB validation.**
- [ ] **Step 7: Make `prepare_source` select protocol before hashing, persist v2 manifests/source metadata, and keep legacy hashing for v1.**
- [ ] **Step 8: Run all Rust tests and commit `feat: persist transfer chunk manifests`.**

### Task 4: File protocol v2 framing and integrity validation

**Files:**
- Create: `src-tauri/src/network/resumable_transfer.rs`
- Modify: `src-tauri/src/network/mod.rs`
- Modify: `src-tauri/src/protocol.rs`
- Modify: `src-tauri/src/network/transfer.rs`
- Test: `src-tauri/src/network/resumable_transfer.rs`

**Interfaces:**
- Produces `FILE_PROTOCOL_V2 = "/localnet/file/2"`.
- Extends `TransferStreamHeader` with defaultable `version`, `start_offset`, and `chunk_size`.
- Produces `ChunkFrameHeader::encode/decode`, bounded frame read/write, and `validate_resume_offset`.

- [ ] **Step 1: Write failing tests for canonical 40-byte headers, final short chunks, bad hashes, wrong/reordered indexes, truncated frames, zero non-final length, overflow, and unaligned resume offsets.**
- [ ] **Step 2: Run `cargo test --manifest-path src-tauri/Cargo.toml resumable_transfer --locked` and observe RED.**
- [ ] **Step 3: Implement checked framing that validates length before allocation/read.**
- [ ] **Step 4: Write a failing integration test that acknowledges two chunks, disconnects, resumes at the reported offset, and proves the prefix is not rewritten.**
- [ ] **Step 5: Run `cargo test --manifest-path src-tauri/Cargo.toml interrupted_stream_resumes --locked` and observe RED.**
- [ ] **Step 6: Implement generic acknowledged send/receive loops: validate → write → `sync_data` → SQLite commit → 8-byte acknowledgement.**
- [ ] **Step 7: Run focused/full tests and commit `feat: stream acknowledged integrity chunks`.**

### Task 5: Persistent pause, claims, and startup recovery

**Files:**
- Modify: `src-tauri/src/domain.rs`
- Modify: `src-tauri/src/storage.rs`
- Modify: `src-tauri/src/network/resumable_transfer.rs`
- Modify: `src-tauri/src/receive_paths.rs`
- Test: `src-tauri/src/storage.rs`
- Test: `src-tauri/src/network/resumable_transfer.rs`

**Interfaces:**
- Adds `TransferStatus::Paused`.
- Produces outbound/inbound conditional claim/pause methods, `list_resumable_outgoing(peer_id)`, and `reconcile_resumable_partials`.
- Persists deterministic `partial_path` and owned cleanup that never deletes a completed destination.

- [ ] **Step 1: Write failing tests for one-winner claims, recoverable Paused transitions, terminal errors, cancel/resume races, and completed records ignoring late failures.**
- [ ] **Step 2: Run storage tests and observe RED.**
- [ ] **Step 3: Implement every transition as one SQL compare-and-set over direction, peer, status, and claim.**
- [ ] **Step 4: Write failing startup tests for equal/longer/shorter/missing partials, v2 transferring → paused, and unchanged v1 fail-and-clean behavior.**
- [ ] **Step 5: Run recovery tests and observe RED.**
- [ ] **Step 6: Implement startup truncation/rollback and permanent-vs-recoverable owned-artifact cleanup.**
- [ ] **Step 7: Run all Rust tests and commit `feat: persist paused transfer progress`.**

### Task 6: Acceptance and resume destination preflight

**Files:**
- Modify: `src-tauri/src/commands.rs`
- Modify: `src-tauri/src/network/runtime.rs`
- Modify: `src-tauri/src/network/resumable_transfer.rs`
- Modify: `src-tauri/src/receive_paths.rs`
- Test: `src-tauri/src/commands.rs`
- Test: `src-tauri/src/network/runtime.rs`

**Interfaces:**
- Manual/automatic acceptance preflight before reservation/confirmation.
- Resume preflight uses receiver-committed bytes.
- Initial failure remains AwaitingAcceptance; resume failure remains Paused with an actionable error.

- [ ] **Step 1: Write failing acceptance tests for insufficient space, FAT32, exact margin, auto-receive fallback, and absence of TransferDecision on failure.**
- [ ] **Step 2: Run command tests and observe RED.**
- [ ] **Step 3: Implement shared acceptance preflight without reintroducing unconditional settings-directory probes.**
- [ ] **Step 4: Write failing resume tests that block when space shrinks and pass after space becomes available.**
- [ ] **Step 5: Implement resume preflight and run all tests.**
- [ ] **Step 6: Commit `feat: preflight transfer acceptance and resume`.**

### Task 7: Reconnect-driven resume control flow

**Files:**
- Modify: `src-tauri/src/protocol.rs`
- Modify: `src-tauri/src/network/runtime.rs`
- Modify: `src-tauri/src/network/resumable_transfer.rs`
- Modify: `src-tauri/src/network/transfer.rs`
- Test: `src-tauri/src/network/runtime.rs`
- Test: `src-tauri/src/network/resumable_transfer.rs`

**Interfaces:**
- Adds `TransferResumeQuery { transfer_id }` and `TransferResume { transfer_id, state, committed_bytes }`.
- Produces `resume_outgoing_for_peer(peer_id)` after capability-bearing Hello.
- Routes incoming v1/v2 stream acceptors separately.

- [ ] **Step 1: Write failing tests for receiving/completed responses, wrong peer, non-friend, legacy capability absence, invalid offset, duplicate Hello, lost final acknowledgement, and cancelled-race behavior.**
- [ ] **Step 2: Run runtime tests and observe RED.**
- [ ] **Step 3: Implement resume query/response and claim-safe pending actions.**
- [ ] **Step 4: Route v2 EOF/timeout/connection loss to Paused while keeping integrity/source/identity errors terminal and v1 behavior unchanged.**
- [ ] **Step 5: Accept `/localnet/file/1` and `/localnet/file/2` separately; verify duplicate streams cannot both claim.**
- [ ] **Step 6: Run all Rust tests and commit `feat: resume transfers when peers reconnect`.**

### Task 8: Paused-transfer user experience

**Files:**
- Create: `src/transfer-status.ts`
- Create: `tests/transfer-status.test.ts`
- Modify: `src/main.tsx`
- Modify: `src/styles.css`
- Modify: `package.json`
- Modify: `.github/workflows/release.yml`

**Interfaces:**
- Extends frontend `TransferStatus` with `paused`.
- Produces production `transferStatusPresentation(transfer)` returning label, tone, progress/cancel visibility.
- Adds `test:transfer-status` to release preparation.

- [ ] **Step 1: Write failing tests for `网络中断，等待自动恢复`, disk errors, retained percentage, visible Cancel, and unchanged terminal states.**
- [ ] **Step 2: Run Node test and observe RED because production presentation is absent.**
- [ ] **Step 3: Implement the production-wired status module and transfer-card UI.**
- [ ] **Step 4: Run both Node tests, `tsc --noEmit`, and `vite build`.**
- [ ] **Step 5: Commit `feat: show resumable transfer states`.**

### Task 9: Release notes, documentation, and 0.2.0

**Files:**
- Create: `docs/releases/v0.2.0.md`
- Modify: `.github/workflows/release.yml`
- Modify: `README.md`
- Modify: `package.json`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/Cargo.lock`

**Interfaces:**
- Release workflow uses `docs/releases/v${version}.md` exactly when present.
- Four authoritative version files become 0.2.0.

- [ ] **Step 1: Add exact Chinese/English notes for 100 GiB, chunk resume, volume/FAT32 preflight, and automatic network recovery; state both peers need 0.2.0 for resume.**
- [ ] **Step 2: Write and run a failing release-note selection check before workflow wiring.**
- [ ] **Step 3: Wire versioned notes into the release job while preserving generic fallback, asset verification, and tag safety.**
- [ ] **Step 4: Update stale README version/capabilities and legacy compatibility in Chinese and English.**
- [ ] **Step 5: Run `node scripts/release-version.mjs 0.2.0` and then `--check`.**
- [ ] **Step 6: Commit `docs: prepare Weline Localnet 0.2.0 release`.**

### Task 10: Review, merge, and public release

**Files:**
- Verify all files changed in Tasks 1–9; introduce no new feature files.

**Interfaces:**
- Produces merged `main`, tag `v0.2.0`, five required public assets, matching checksums, and the exact release body.

- [ ] **Step 1: Run the complete local matrix.**

```text
node scripts/release-version.mjs --check
node --test --experimental-strip-types tests/presence.test.ts tests/transfer-status.test.ts
tsc --noEmit
vite build
cargo fmt --manifest-path src-tauri/Cargo.toml --check
cargo test --manifest-path src-tauri/Cargo.toml --lib --locked
cargo check --manifest-path src-tauri/Cargo.toml --locked
git diff --check
```

- [ ] **Step 2: Build local Windows NSIS and verify `Weline Localnet_0.2.0_x64-setup.exe`.**
- [ ] **Step 3: Review offset authority, claim lifetime, cancel/resume races, startup truncation, source mutation, final-ack loss, cleanup ownership, overflow, and legacy compatibility; fix Critical/Important findings through failing tests.**
- [ ] **Step 4: Push branch and create a PR listing the four requested features, compatibility, migrations, tests, and bundle evidence.**
- [ ] **Step 5: Merge only after checks pass; never create/move the tag manually.**
- [ ] **Step 6: Watch preparation, Windows, macOS Universal, asset verification, and Release publication to completion.**
- [ ] **Step 7: Read back tag target, non-draft Release, five asset names/digests, checksum contents, exact release body, and unauthenticated public access.**
- [ ] **Step 8: Report automated/local/CI evidence separately; do not claim a real physical 100 GiB Windows↔macOS transfer unless actually performed.**
