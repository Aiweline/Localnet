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

## Review fix round 1 — 2026-08-25

This section supersedes the earlier compatibility statement that v1 skipped preflight. Review round 1 requires the same acceptance destination gate for both protocols while retaining v1's legacy size ceiling and stream format.

### Implementation and files

- `src-tauri/src/commands.rs`: manual v1 and v2 acceptance now both preflight an existing selected parent before reservation; v1 no longer recreates a missing selected directory. Accepted manual commands carry a runtime completion channel and return `Transferring` only after `send_request` has been submitted. Queue/runtime failure returns the exact unclaimed acceptance to `AwaitingAcceptance` with its actionable error.
- `src-tauri/src/network/runtime.rs`: automatic offers first persist a durable `AwaitingAcceptance` base row, then treat reservation, partial setup, acceptance CAS, decision construction, and submission as one compensated operation. `TransferUpdated(Transferring)` is emitted only after the request is submitted. `ResolveTransfer` is no longer ignored by `handle_command_failure`.
- `src-tauri/src/storage.rs`: failed pre-decision acceptance rollback claims the exact zero-byte row, journals proven reservation/partial ownership, atomically clears ownership and returns it to `AwaitingAcceptance`, then performs retryable identity-checked filesystem cleanup. Partial/CAS failures clean unpersisted owned partials.
- `src-tauri/src/network/resumable_transfer.rs` and `src-tauri/src/network/transfer.rs`: the production receive body boundary now claims, reloads, validates both the authoritative committed boundary and incoming header offset, and preflights before body/file access. A stale header is atomically paused with its claim cleared; only the reloaded committed offset can reach `open_owned_resumable_partial`.
- `src-tauri/src/network/mod.rs`: exposes only the crate-private rollback primitive needed by manual command compensation.

### Genuine RED → GREEN evidence

1. V1 RED: `cargo test --manifest-path src-tauri/Cargo.toml --lib manual_v1_acceptance -- --nocapture` failed both new tests: insufficient space was accepted and the exact-margin probe count was `0` instead of `1`. The first automatic RED also caught an invalid 5 GiB legacy fixture; it was corrected to the existing 1 GiB v1 policy range before implementation. GREEN: manual and automatic v1 focused suites each pass 2 tests for insufficient space, missing directory/no creation/no decision/no reservation, and the exact 64 MiB margin.
2. Authoritative-offset RED: `cargo test --manifest-path src-tauri/Cargo.toml --lib production_receive_boundary -- --nocapture` ran the real owned-partial/SQLite boundary; the stale stream reached its body and failed at `stale stream must lose ...: ()`. GREEN: 2 tests pass; a competing stream advances and pauses, the stale header loses without truncation, the fresh offset later reaches the body, and shrink/restore probes observe persisted committed bytes.
3. Decision-dispatch RED: `cargo test --manifest-path src-tauri/Cargo.toml --lib submission_reverts -- --nocapture` failed to compile with unresolved `AcceptedSubmissionOutcome` and `finalize_accepted_transfer_submission`. GREEN: production submission completion tests pass for manual offline v2 and automatic `send_request` failure v1, returning only corrected `AwaitingAcceptance` payloads.
4. Green/refactor coverage additionally injects failure after reservation, after partial SQL persistence before commit, and after a successful acceptance CAS/decision construction; every case retains an actionable `AwaitingAcceptance` row, produces no decision, and removes only token/identity-owned artifacts. A focused run exposed a lock-lifetime deadlock in journal cleanup; dropping the committed SQLite guard before retryable cleanup fixed it, and both focused compensation tests then passed.

### Exact final verification

- Nine locked focused commands covering v1 manual/automatic, production receive boundary, manual enqueue failure, three automatic setup/CAS compensation phases, manual offline submission, and automatic send failure — PASS, 12 tests total.
- `cargo test --manifest-path src-tauri/Cargo.toml --locked` with `CARGO_TARGET_DIR=G:\codex-localnet-target` — PASS, 152 library tests, 0 binary tests, 0 doc tests; only the existing informational Windows linker import-library message.
- `cargo check --manifest-path src-tauri/Cargo.toml --locked` — PASS, debug profile without compiler warnings.
- `cargo check --manifest-path src-tauri/Cargo.toml --release --locked` — PASS, optimized profile.
- `cargo fmt --manifest-path src-tauri/Cargo.toml -- --check` — PASS.
- `git diff --check` — PASS.
- `pnpm release:check` with `npm_config_script_shell=powershell.exe` — PASS, synchronized `0.1.7`.

### Review conclusions and concerns

- TOCTOU compensation: probes precede all ownership. Directory changes after a successful probe fail in create-new reservation/partial setup, and automatic failures compensate through the durable awaiting row. Decision rollback uses a receive-claim CAS plus durable ownership journal, so a missing disk cannot leave the UI in `Transferring` and cleanup retries without deleting unrelated files.
- Error/event visibility: manual callers await runtime submission; automatic code does not emit active state before submission; outbound rejection/failure/start timeout uses the same exact rollback and corrected event. Successful/declined behavior remains single-decision, and Task 5 claim/finalization predicates are unchanged.
- Resume safety: the test partial length matches committed SQLite chunks. Space failure and stale-offset failure both stop before body access, preserve bytes/chunks, persist an actionable paused error, and release the claim atomically; restored space uses receiver-authoritative subtraction.
- Settings safety: disabling auto-receive and nickname updates with an unchanged unavailable directory still do not probe it. Validation occurs only when enabling or accepting.
- No known Task 6 blocker. Task 7 still owns reconnect query/response scheduling and v2 acceptor registration; none was added here.

## Review fix round 2 — 2026-08-25

### Implementation and files

- `src-tauri/src/storage.rs`: automatic setup failure now first commits the exact `AwaitingAcceptance` row, actionable error, cleared ownership/claim state, and an identity/token-owned `transfer_cleanup` tombstone in one immediate SQLite transaction. Filesystem cleanup runs only after commit; unavailable media or an identity mismatch retains the tombstone. A new acceptance must drain that exact tombstone before preflight/reservation, and acceptance/claim CAS predicates independently reject a surviving cleanup record.
- `src-tauri/src/storage.rs`: added durable `incoming_transfer_decisions`. The `submissionPending` token is inserted only after exact local acceptance validation and before transport submission. Exact send/rejection/failure/timeout rollback atomically creates the cleanup tombstone, restores `AwaitingAcceptance`, deletes the token, and then attempts filesystem cleanup. Receive claim and incoming cancellation consume the token in their own transaction; the token survives restart until claim/cancel consumes it.
- `src-tauri/src/network/runtime.rs`: manual and automatic production handlers both use the same runtime submission boundary. All fallible SQLite validation/pending registration precedes the single `send_request`; after it returns, only infallible in-memory registration, event emission, timeout spawn, and completion remain. No `Transferring` event/completion is produced before successful submission. Rejection and outbound failure carry the exact durable token, preventing stale responses from reverting a later acceptance.
- `src-tauri/src/network/transfer.rs`: the start timeout carries the durable decision token and can only roll back that exact still-unclaimed pending action. A body claim wins atomically and makes a late timeout harmless.
- `src-tauri/src/commands.rs`: manual acceptance drains deferred cleanup before preflight or reservation, and a lost completion channel never reverts an acceptance whose durable submission token still exists.

### Genuine RED → GREEN evidence

1. Durable automatic fallback RED: `cargo test --manifest-path src-tauri/Cargo.toml automatic_fallback_is_durable_before_unavailable_media_cleanup -- --nocapture` failed at the fallback expectation with `Io(Custom { kind: NotFound, error: "接收目录或磁盘当前不可用" })`; reservation cleanup ran before the actionable row was persisted. GREEN: the same test passes, the row is durably `AwaitingAcceptance` while media is detached, and startup drains the retained tombstone after media returns.
2. Runtime ordering RED was replayed with the reviewed send-before-prepare ordering: `cargo test --manifest-path src-tauri/Cargo.toml --lib runtime_handler_local_validation_failure_submits_zero_requests -- --nocapture` failed `left: 1, right: 0`, proving an invalid local acceptance submitted one request. GREEN after restoring durable-first ordering: the same test passes with zero submissions.
3. Additional GREEN coverage uses the production runtime handler for manual and automatic send failure, success with exactly one request, duplicate suppression, body-claim-versus-timeout, and timeout-versus-late-response. Storage coverage detaches media after partial creation, blocks reacceptance with `cleanup pending` and no claim/decision, drains after media returns, accepts again, and proves a later rollback creates a fresh tombstone for the new token.

### Pending-decision state ordering

1. Existing Task 5 acceptance CAS owns the destination/reservation/partial but remains invisible to the caller while its command is pending.
2. The runtime transaction revalidates the exact unclaimed zero-byte row, verifies no cleanup/pending conflict, and inserts one unique `submissionPending` token.
3. `send_request` is invoked exactly once. Synchronous submission failure consumes that token via durable rollback; after successful submission no fallible local operation can reinterpret it as unsent.
4. Control rejection, outbound failure, and start timeout may consume only their exact token. A receive claim or cancel deletes the token atomically; an accepted control response intentionally leaves it until body start or timeout resolves the action.

### Exact final verification

- Locked focused suites: runtime 22 passed; storage/reacceptance coverage passed; commands 11 passed; production receive boundary 2 passed.
- `cargo test --manifest-path src-tauri/Cargo.toml --locked` with `CARGO_TARGET_DIR=G:\codex-localnet-target` — PASS, 161 library tests, 0 binary tests, 0 doc tests; only the existing informational Windows linker import-library message.
- `cargo check --manifest-path src-tauri/Cargo.toml --locked` — PASS, debug profile without compiler warnings.
- `cargo check --manifest-path src-tauri/Cargo.toml --release --locked` — PASS, optimized profile.
- `cargo fmt --manifest-path src-tauri/Cargo.toml -- --check` — PASS.
- `git diff --check` — PASS.
- `pnpm release:check` with `npm_config_script_shell=powershell.exe` — PASS, synchronized `0.1.7`.

### Self-review and concerns

- TOCTOU/side effects: volume preflight ordering from rounds 1–2 remains unchanged. Deferred cleanup is drained before a new preflight/reservation; create-new reservation and identity checks still fail safely if media changes afterward. No acceptance signal is possible before durable setup and pending-decision validation.
- Error visibility: cleanup-pending, automatic setup, and submission errors remain on the durable awaiting row and reach the existing caller/event paths. Filesystem cleanup failure is logged but no longer replaces or prevents the actionable state.
- Ownership: tombstones can be updated only for the same destination/token/protocol identity; unrelated or later ownership is never overwritten. New receive claims cannot coexist with a cleanup tombstone, and stale decision tokens cannot roll back a fresh acceptance.
- Settings/resume: the settings-disable/nickname paths remain probe-free; v1/v2 preflight and the authoritative post-claim resume-offset/body boundary from round 1 remain covered and unchanged.
- No known Task 6 blocker. Libp2p's synchronous `send_request` returns an outbound ID rather than a `Result`; the production handler's injected submission closure tests the failure boundary, while actual asynchronous rejection/outbound failure use the same durable token rollback. Task 7 reconnect query/scheduler work remains out of scope.
