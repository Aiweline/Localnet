# Task 6 report — Acceptance and resume destination preflight

Date: 2026-08-25
Branch: `feat/resumable-large-transfers`
Required target directory: `G:\codex-localnet-target`

## Implementation

- Added one production `preflight_receive_directory(directory, file_size, committed_bytes)` adapter over Task 2's `volume_preflight::preflight_destination`; platform filesystem naming, writable probing, capacity arithmetic, and FAT32 aliases remain centralized in the existing module.
- Manual v2 acceptance now runs destination preflight with zero committed bytes before generating a reservation token, creating a reservation/partial, executing the acceptance CAS, or dispatching `TransferDecision`. A failure returns the actionable probe error to the caller and leaves the durable transfer `AwaitingAcceptance` without ownership fields.
- Automatic v2 acceptance runs the same preflight before reservation. A failure persists the offer as `AwaitingAcceptance`, stores the actionable reason on the transfer, emits the existing automatic-receive error event, and produces no transfer-decision outcome. Exact-margin success keeps the existing collision-safe reservation and Task 5 partial identity setup.
- Paused v2 receive now acquires the existing receive claim, reloads receiver-authoritative metadata, verifies destination/partial co-location, and preflights with the persisted committed offset before opening the owned partial or starting the receive body. Failure atomically transitions the claimed row back to `Paused`, persists the reason, clears the claim, closes the stream, and emits transfer/error events. A later retry repeats the probe and can claim again.
- Legacy v1 acceptance/stream behavior is unchanged: the new volume gate is conditional on protocol v2. No resume query, reconnect scheduler, or protocol message from Task 7 was added.

## Files

- `src-tauri/src/commands.rs` — manual acceptance gate, decision-order seam, and real storage/filesystem tests.
- `src-tauri/src/network/runtime.rs` — automatic acceptance outcome/fallback, decision suppression, and real storage/filesystem tests.
- `src-tauri/src/network/resumable_transfer.rs` — claim-scoped injectable resume gate and receiver-authoritative resume tests.
- `src-tauri/src/network/transfer.rs` — minimal live v2 receive integration so the resume gate runs before body/partial access; this production wiring is necessary even though the abbreviated Task 6 file list omitted the caller.
- `src-tauri/src/receive_paths.rs` — shared production adapter to Task 2's preflight.

## RED → GREEN evidence

1. Manual acceptance RED: `cargo test --manifest-path src-tauri/Cargo.toml --locked --lib commands::tests` failed with `E0432` for missing `accept_incoming_transfer_with_preflight`. GREEN: 8 passed, covering one-byte-short capacity, FAT32, exact 64 MiB margin, missing/unwritable destination, no reservation/partial, no decision dispatch, and unchanged settings-disable/nickname paths.
2. Automatic acceptance RED: `cargo test --manifest-path src-tauri/Cargo.toml --locked --lib network::runtime::tests` failed with `E0432` for missing `persist_incoming_offer_with_preflight`. GREEN: 8 passed, covering insufficient space, FAT32/MSDOS, exact margin, missing/unwritable destination, manual fallback with persisted error, and `None` transfer decision.
3. Resume RED: `cargo test --manifest-path src-tauri/Cargo.toml --locked --lib resume_preflight` failed with `E0432` for missing `claim_paused_incoming_with_preflight`. GREEN: 2 passed with real SQLite claims: shrunken space stays paused/actionable with no body start; restored exact space claims successfully and the injected observation receives `(file_size, persisted 4 MiB committed_bytes)`.
4. Refactor review removed an accidental unrelated outgoing-status broadening before final verification and routed all post-claim resume validation errors through the atomic pause/claim-clear transition.

## Exact verification

- `cargo test --manifest-path src-tauri/Cargo.toml --locked --lib commands::tests` — PASS, 8 passed.
- `cargo test --manifest-path src-tauri/Cargo.toml --locked --lib network::runtime::tests` — PASS, 8 passed.
- `cargo test --manifest-path src-tauri/Cargo.toml --locked --lib resume_preflight` — PASS, 2 passed.
- `cargo test --manifest-path src-tauri/Cargo.toml --locked` — PASS, 140 library tests, 0 binary tests, 0 doc tests; only the existing informational Windows linker import-library message.
- `cargo check --manifest-path src-tauri/Cargo.toml --locked` — PASS, debug profile without compiler warnings.
- `cargo check --manifest-path src-tauri/Cargo.toml --release --locked` — PASS, optimized profile.
- `cargo fmt --manifest-path src-tauri/Cargo.toml -- --check` — PASS.
- `git diff --check` — PASS.
- `pnpm release:check` with `npm_config_script_shell=powershell.exe` — PASS, synchronized `0.1.7`.

## Self-review

- TOCTOU compensation: the probe precedes every acceptance ownership side effect. If the directory changes afterward, existing create-new reservation/identity setup fails closed; no `TransferDecision` is dispatched until reservation, partial setup, and the acceptance CAS succeed. Automatic reservation failure becomes manual fallback; later setup/CAS failure emits no acceptance decision.
- Resume ordering: the claim prevents competing receive bodies while the probe runs. The probe uses metadata reloaded after the claim, not the stale stream/header snapshot. Every actionable probe/path error is persisted by the same CAS that clears the claim, and the receive body/owned partial open is downstream of the gate.
- Error visibility: manual failures return directly to the Tauri caller; automatic failures are present both on the persisted `TransferRecord.error` and the network error event; resume failures are present on the paused row and emitted transfer/error events.
- Side effects: tests exercise real SQLite transitions and real reservation/partial ownership. Only the platform volume observation is injected; capacity/filesystem policy still runs through the real `validate_volume` function.
- Settings path: disabling auto-receive and editing a nickname with an unchanged unavailable configured directory still bypass destination probing. Enabling continues its existing writable-directory validation, while actual v2 acceptance always runs full volume preflight.
- Compatibility: v1 skips the new gate and retains its existing directory-creation and stream behavior. Task 5 claim, cancellation, finalization, identity, and cleanup APIs are reused without weakening their CAS predicates.

## Concerns and deferred ownership

- No known Task 6 blocker.
- Task 7 still owns reconnect resume query/response messages, scheduling, and v2 acceptor registration. Task 6 only gates the already-exposed live paused receive entry and provides the reusable claim/preflight boundary.
- The Windows implementation compiled and used the production probe for real missing-directory coverage, but this host did not mount a physical FAT32 volume; Task 2's production alias mapping and pure policy tests remain the evidence for FAT32 behavior. macOS runtime probing remains native-CI/device evidence.
