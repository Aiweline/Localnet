# Task 5 report — Persistent pause, claims, and startup recovery

Date: 2026-08-24
Branch: `feat/resumable-large-transfers`
Required target directory: `G:\codex-localnet-target`

## Implementation

- Added `TransferStatus::Paused` and atomic SQLite compare-and-set methods for one-winner inbound/outbound v2 claims, recoverable pause, terminal failure, completion, unclaimed cancellation, and peer-scoped paused-outbound lookup.
- Every claimed v2 transition checks transfer ID, direction, peer, protocol, current status, and claim in the same `UPDATE` that persists the new status/error and clears the claim. Completed rows therefore ignore late failures, and cancel cannot win after resume has claimed the row.
- Restricted raw `release_incoming_transfer_claim` to legacy protocol rows. V2 completion, recoverable receive errors, terminal receive errors, and unstarted-receive timeout now use status-and-claim CAS methods; terminal cleanup begins only after the terminal CAS succeeds.
- Added the recoverable transport classifier: network/offline and connection-style I/O failures pause, while integrity, changed source/invalid input, identity/permission, storage, compatibility, and local missing/invalid-data I/O failures remain terminal.
- Added deterministic, hidden, case-distinct partial names derived from the exact transfer ID in the selected destination directory. This makes the partial and final reservation structurally same-volume and prevents transfer IDs differing only by case from sharing an artifact name.
- Added exact partial ownership cleanup. It removes only the deterministic path recorded for that transfer, never the final destination or an unrelated path; reservation cleanup retains the existing ownership sidecar rules.
- Startup now transactionally changes v2 `transferring` to `paused`, clears both claims, and preserves v2 reservations/partials. Legacy v1 `transferring` rows retain fail-and-clean behavior.
- Startup reconciliation preserves equal partials, truncates/synchronizes longer durable tails, and rolls shorter/missing partial progress plus later chunk rows back transactionally to the largest physically present, contiguous committed chunk boundary. Missing media/destination parents are retained as recoverable errors rather than guessed absent data.
- Terminal cleanup is scoped to the winning transfer. Crash after terminal CAS but before artifact deletion is recoverable because startup retries exact owned cleanup; completed and unowned destinations are never deleted.

## Files

- `src-tauri/src/domain.rs` — paused status.
- `src-tauri/src/storage.rs` — SQL CAS APIs, startup state reset/reconciliation, exact cleanup, and state-machine tests.
- `src-tauri/src/network/resumable_transfer.rs` — recoverable/terminal error classifier and tests.
- `src-tauri/src/receive_paths.rs` — deterministic same-directory partial and ownership-safe cleanup with tests.
- `src-tauri/src/network/transfer.rs` — approved scoped integration expansion: v2 receive completion/pause/failure/timeout must not use the legacy raw claim release. No reconnect query/orchestrator or volume gate was added.

## RED → GREEN evidence

1. Initial claim tests were RED at compile time for missing outbound claim/pause APIs and `TransferStatus::Paused`; the focused claim/cancel/completed cases became GREEN after the SQL CAS implementation.
2. Startup recovery was RED with an equal v2 transfer reopening as `Failed` instead of `Paused`; longer/shorter/missing/equal and unchanged-v1 cases became GREEN after startup reset and reconciliation were added.
3. Partial-path and error-classification tests were RED on unresolved helpers; they became GREEN after deterministic ownership helpers and explicit transport classification were implemented.
4. Terminal cleanup was RED while `destination_reserved` remained set; a later regression was RED because terminal cleanup touched an unrelated reservation. Scoped post-CAS artifact cleanup made both GREEN.
5. Corrupt short-partial coverage was RED because a missing committed chunk row incorrectly retained 4 MiB; reconciliation now rolls it to zero without inventing a chunk boundary.
6. The raw-release regression was RED because v2 could clear `receive_claimed` independently; the legacy-only guard now rejects that call and leaves both claim and status unchanged.
7. The specialized incoming-completion test was RED before a single CAS could persist the finalized path/status/progress and clear the claim; it now passes, and a late failure cannot overwrite completion.
8. The first full suite run exposed one existing synthetic manifest fixture without a destination (`InvalidInput("可恢复接收缺少保存位置")`). Auto-derivation was narrowed to rows that actually have a selected destination; the focused regression and full suite then passed.

## Exact verification

- `cargo test --manifest-path src-tauri/Cargo.toml --lib storage::tests --locked` — PASS, 26 passed.
- `cargo test --manifest-path src-tauri/Cargo.toml --lib network::resumable_transfer::tests::production_manifest_verifier_loads_ordered_records_and_rejects_a_substitution --locked` — PASS, 1 passed after the full-suite RED correction.
- `cargo test --manifest-path src-tauri/Cargo.toml --locked` — PASS, 82 library tests, 0 binary tests, 0 doc tests; one informational Windows linker import-library message only.
- `cargo check --manifest-path src-tauri/Cargo.toml --locked` — PASS with no compiler warnings.
- `cargo fmt --manifest-path src-tauri/Cargo.toml --check` — PASS.
- `git diff --check` — PASS.
- `pnpm release:check` with `npm_config_script_shell=powershell.exe` — PASS, synchronized version `0.1.7` (the default `cmd.exe` child had no usable Node path in this Codex shell; the PowerShell script shell uses the same package script).

## Self-review

- Crash windows: claim release is part of the pause/terminal/completed SQL write; filesystem cleanup follows terminal persistence and is startup-retriable. Truncation precedes progress rollback, so a crash can only leave SQLite temporarily ahead of disk and the next startup repeats the safe rollback.
- Case collisions: exact transfer-ID bytes are hashed to lowercase hex names; case-different IDs produce different filenames even on case-insensitive filesystems.
- Rollback/truncation: retained progress is bounded by physical length and ordered persisted chunk rows; longer tails never become committed progress, and missing chunk rows are never synthesized.
- Claim release: code search leaves raw release only in legacy timeout/v1 receive paths; the storage guard rejects every v2 raw release attempt.
- Artifact ownership: cleanup requires the exact deterministic partial path or the existing reservation token/sidecar. Completed destinations and unrelated files are preserved.

## Concerns and deferred ownership

- No known Task 5 blocker.
- Task 7 still owns v2 acceptor registration, reconnect resume queries/scheduling, and consumption of the outbound claim APIs. Task 6 still owns destination-volume preflight gates. Neither was implemented early.

---

## Review fix round 1 — 2026-08-24

### Corrected implementation

- Wired the live v2 sender through `try_claim_outgoing_transfer` before opening the source or stream. Every acknowledged offset now uses a peer-, direction-, protocol-, status-, claim-, and previous-offset-scoped SQL CAS. Recoverable stream errors pause and clear `send_claimed`; source/integrity/identity failures fail and clear it; final acknowledgement completes and clears it before stream close is observed. The legacy v1 sender remains on its existing path.
- Hardened generic transfer upsert so it cannot mutate a claimed v2 row or resurrect a cancelled, failed, or completed v2 row. A guarded no-op also skips filesystem setup, and manifest replacement aborts if the protected transfer row was not inserted/updated.
- Made `commit_received_chunk` peer- and active-claim-scoped in the same transaction as chunk insertion and receiver-authoritative progress. A cancel, pause, failure, completion, wrong peer, or lost claim makes the stale callback return `false` without inserting a chunk or advancing bytes.
- Added exact-token partial ownership sidecars. Acceptance requires the matching final-destination reservation, exclusively creates the owner marker and empty deterministic partial, and rejects a pre-existing partial/marker without truncating or deleting it. Hashing uses exact transfer-ID bytes, so whitespace-distinct IDs remain distinct.
- Manual and automatic v2 acceptance now compensate a partial collision by CASing the unclaimed row to `failed` and cleaning only proven owned artifacts. An unrelated deterministic-path file is preserved and the database no longer remains spuriously `transferring`.
- Removed destination-parent creation from v2 receive. A vanished selected directory or removable medium returns a recoverable receive error and leaves the directory, reservation metadata, and progress untouched.
- Split send and receive error classification. Network `UnexpectedEof` remains recoverable, while an actual short source-file `read_exact` becomes terminal source mutation (`invalid_input`).
- Added a durable finalization journal tied to transfer ID, reservation token, and the actual collision-renumbered destination. V2 filesystem finalization retains both the journal and final reservation through the completed-state CAS; completed cleanup removes the journal, partial owner, and reservation without ever deleting the final file.
- Startup recovery first wins a peer/protocol/status/progress-scoped receive claim. Only then may it inspect, truncate, hash, or roll back filesystem state. A lost CAS therefore leaves the partial byte-for-byte untouched. An owned final file with no partial is size/hash validated, completed in SQL without retransmission, and then has only its ownership metadata removed.

### Crash ordering and ownership proof

1. Acceptance already owns the final reservation; it persists the v2 transfer transition, then creates a token-bound owner sidecar with `create_new`/sync and an empty partial with `create_new`/sync before an acceptance is reported. A process crash between DB and file setup is recovered only after startup wins the recovery claim and revalidates the final reservation. A collision instead produces a durable failed row and preserves the colliding file.
2. Finalization writes and syncs the token-bound journal before the no-clobber filesystem commit. A crash before commit leaves the owned partial resumable; a crash after commit leaves an unambiguous journal plus final reservation. Startup validates the actual recorded candidate, exact size, and whole-file SHA-256 before its completed CAS.
3. After the completed CAS, cleanup removes the finalization journal, partial owner marker, and reservation marker, then clears the DB ownership fields. A crash during cleanup is retried from the completed row; the completed destination is never a cleanup target.

### Files in review fix

- `src-tauri/src/network/transfer.rs` — approved integration expansion for live claimed v2 send, receive finalization, and claimed error/completion persistence. Raw claim release remains legacy-only.
- `src-tauri/src/network/resumable_transfer.rs` — owned partial open, no media recreation, send/receive error context split, and terminal production source EOF.
- `src-tauri/src/receive_paths.rs` — exact-ID partial names, exclusive token owner sidecars, durable finalization journal, and ownership-gated cleanup.
- `src-tauri/src/storage.rs` — terminal-safe upsert, outbound/inbound progress CAS, acceptance compensation, claim-first startup reconciliation, crash-finalized completion, and race/recovery tests.

### RED → GREEN evidence for review findings

1. `cargo test ... stale_v2_upsert_cannot_overwrite_an_active_outgoing_claim` was RED with `left: Cancelled`, `right: Transferring`; the claimed-row SQL guard made it GREEN.
2. `cargo test ... stale_v2_upsert_cannot_resurrect_a_cancelled_outgoing_transfer` was RED with `left: Transferring`, `right: Cancelled`; terminal-state upsert predicates made it GREEN.
3. The stale incoming callback test was RED because a cancelled row still accepted a chunk; peer/status/claim predicates in `commit_received_chunk` made it GREEN with unchanged chunks/progress.
4. The outbound acknowledgement test initially failed to compile with `E0599` because no claim-scoped progress API existed; `commit_claimed_outgoing_progress` and the live callback made it GREEN, including cancel-vs-active and post-pause stale ACK rejection.
5. Live sender tests initially failed to compile for missing claim/error-persistence helpers; they are GREEN for claim-before-work, recoverable disconnect pause, terminal source failure, and cancelled-row no-claim behavior. A production `send_acknowledged_chunks` BrokenPipe test also leaves a paused row with its claim cleared.
6. The production source EOF test initially failed to compile for missing context classifiers/read helper; a real short Tokio file read now produces terminal `invalid_input`, while network EOF remains recoverable.
7. Manual acceptance collision was RED because acceptance returned success over unrelated deterministic data; it now fails, preserves the data, and records `failed`.
8. `automatic_v2_acceptance_collision_fails_and_cleans_the_reserved_state` was RED with `left: Transferring`, `right: Failed`; automatic upsert compensation now makes it GREEN and clears only owned reservation metadata.
9. Receive-path ownership tests initially failed to compile for missing reservation/sidecar APIs. They are GREEN for pre-existing partial preservation, token-gated cleanup, exact whitespace-distinct IDs, and same-directory deterministic placement.
10. Missing-media coverage initially failed to compile for missing owned-open behavior; it is GREEN and proves the vanished parent is not recreated.
11. The crash-after-filesystem-finalize recovery test was RED with `left: Paused`, `right: Completed`, including a late collision that renumbered the final file. The durable journal recovery is GREEN, preserves the user collision, completes the actual candidate, and requires no retransmission.
12. `lost_recovery_claim_leaves_the_partial_filesystem_untouched` was RED because the tail was truncated (`4194304` vs `4194335`) despite losing the CAS. Claim-first reconciliation makes it GREEN.
13. `startup_never_truncates_an_unowned_deterministic_partial` is GREEN and proves a deterministic filename alone never authorizes truncation or deletion.
14. `separate_sqlite_connections_have_one_claim_or_cancel_winner` is GREEN with simultaneous barrier-released contenders against a file-backed database; exactly one claim/cancel CAS wins.

### Final verification

- `cargo test --target-dir G:\codex-localnet-target --manifest-path src-tauri/Cargo.toml --lib --locked receive_paths::tests` — PASS, 13 passed.
- `cargo test --target-dir G:\codex-localnet-target --manifest-path src-tauri/Cargo.toml --lib --locked storage::tests` — PASS, 35 passed before the terminal-resurrection addition; the final full suite includes 36 storage tests.
- `cargo test --target-dir G:\codex-localnet-target --manifest-path src-tauri/Cargo.toml --lib --locked network::resumable_transfer::tests` — PASS, 27 passed.
- `cargo test --target-dir G:\codex-localnet-target --manifest-path src-tauri/Cargo.toml --lib --locked network::transfer::tests` — PASS, 3 passed.
- `cargo test --target-dir G:\codex-localnet-target --manifest-path src-tauri/Cargo.toml --locked` — PASS, 100 library tests, 0 binary tests, 0 doc tests; only the informational Windows linker import-library message.
- `cargo check --target-dir G:\codex-localnet-target --manifest-path src-tauri/Cargo.toml --locked` — PASS, no compiler warnings.
- `cargo fmt --manifest-path src-tauri/Cargo.toml -- --check` — PASS.
- `git diff --check` — PASS.
- `pnpm release:check` with `npm_config_script_shell` set to Windows PowerShell — PASS, `Weline Localnet release version: 0.1.7`.

### Review concerns

- No known Task 5 blocker remains. The finalization journal is intentionally a sidecar rather than a schema field: it must survive the exact window where the filesystem commit exists but the database completed CAS does not.
- Reconnect discovery/scheduling remains Task 7, and capacity/volume-format gates remain Task 6. This review fix does not implement either early.

---

## Review fix round 2 — 2026-08-24

### Corrected implementation

- Partial ownership is now bound to the exact exclusively-created file identity, not its path. Windows reads `BY_HANDLE_FILE_INFORMATION` from the open handle through `windows-sys` and compares volume serial plus the full high/low file index; Unix/macOS compares `dev` plus `ino` from the open handle. Symlinks, replaced regular files, marker-only states, and file-only states fail closed. The exact verified handle is also the handle used for truncation and receive writes.
- A deterministic collision is preserved and a numbered hidden partial candidate is exclusively created in the selected destination directory. The file is created and synced before the identity-bearing owner marker; a crash between those steps therefore leaves an unowned orphan that later runs never touch. Transfer IDs are hashed byte-for-byte without trimming.
- Manual and automatic v2 acceptance require the selected parent to already exist and be writable. Live v2 receive uses the same no-create rule; a vanished mount/directory becomes a recoverable pause. Legacy v1 retains its prior directory-creation behavior.
- Startup recovery claims with a SQL predicate over the complete observed snapshot: peer/direction/protocol/status/claim, committed bytes, destination, reservation bit/token, and partial path. A stale snapshot loses before any filesystem inspection. Generic upsert treats paused v2 recovery metadata as immutable.
- Incoming cancellation now takes the caller-observed protocol and includes that exact protocol in its CAS, preserving an explicit v1 branch while rejecting stale v1/v2 observations.
- Finalization is an append-and-sync journal. A collision reserves the replacement while retaining the old reservation, journals old and new candidates plus stable identity, CASes the claimed DB destination, and only then releases the old reservation. The partial remains until the completed-state CAS. Startup accepts a journal-owned, size/hash-valid final candidate even while the owned partial remains, completes SQL first, then removes the partial/marker/journal/reservation set. Exact injected phases cover before/after replacement reserve, journal update, metadata switch, old reservation release, final-name creation, final journal sync, DB completion, and partial cleanup.
- Terminal ownership clearing and chunk deletion now occur in one SQLite transaction after an exact status/metadata CAS. That transaction first records a durable cleanup tombstone; filesystem cleanup runs afterward and startup retries from the tombstone. A failed chunk delete rolls back both the tombstone and ownership clearing, while a crash after commit cannot make chunk rows unreachable.
- The actual `send_resumable_transfer` and `send_claimed_resumable_transfer` production functions now use a minimal injectable stream opener/event sink seam. Production-path tests prove open/write disconnect pauses and clears the claim, while cancellation at the ACK boundary makes the stale progress CAS lose without resurrecting the row.

### Crash ordering and ownership proof

1. Acceptance creates the exact partial with `create_new`, syncs it, captures identity from that still-open handle, then creates/syncs the marker. File-only and marker-only crash states are collision inputs, never cleanup authority.
2. Finalization syncs `Prepared` before creating the final name. On collision it keeps the old reservation while reserving and journaling the replacement, persists the claimed metadata switch, then releases the old reservation. A `Prepared` record is sufficient for startup only when the current final handle identity, reservation token, size, and SHA-256 all validate.
3. The DB completed CAS retains `partial_path`; completed cleanup then removes only the identity-owned partial, all exact-token reservation markers, and the journal before clearing DB ownership metadata. Every interruption is repeatable from either the completed row or journal.
4. Terminal cleanup commits tombstone creation, ownership-field clearing, and chunk deletion together. It then removes only identity/token-proven filesystem artifacts and deletes the tombstone last. Replacement data is left untouched and an unresolved tombstone remains retriable.

### RED → GREEN evidence for round 2

1. `file_only_partial_crash_state_is_preserved_and_a_new_candidate_is_reserved` was RED with `AlreadyExists`; `replacing_an_owned_partial_with_a_regular_file_revokes_ownership` was RED because ownership incorrectly remained true. Stable identity plus numbered candidates made both GREEN, alongside marker-only and symlink replacement cases.
2. The first Windows identity build was RED with `E0658: use of unstable library feature windows_by_handle`. The approved exact-handle `windows-sys 0.61.2` implementation compiled GREEN without a path-only fallback.
3. Manual and automatic missing-media tests were initially RED at compile time for absent strict helpers/protocol routing. They are GREEN and prove neither production acceptance path recreates the selected directory.
4. `stale_recovery_snapshot_cannot_touch_the_old_partial_after_metadata_changes` was RED because the old tail changed from 4,194,345 to 4,194,304 bytes. Full snapshot predicates make it GREEN with the old file unchanged.
5. `incoming_cancel_rejects_a_stale_caller_protocol_snapshot` was RED at compile time before the protocol argument existed; it is GREEN with explicit v1/v2 CAS behavior.
6. `terminal_cleanup_rolls_back_ownership_fields_when_chunk_deletion_aborts` was RED because ownership fields cleared even though a trigger aborted chunk deletion. The cleanup transaction now leaves fields and chunks together on rollback; the retry and post-commit startup tests are GREEN.
7. `startup_completes_an_owned_v2_file_finalized_before_database_commit` exposed the old partial-removal assumption (`assertion failed: !partial.exists()`). It now proves the partial remains before DB completion and disappears only after startup completes SQL and cleanup.
8. The exact finalization phase test first failed to compile because the success value lacked `Debug`; after using an explicit injected-error assertion, all seven replacement/final-name crash points pass and preserve the user collision.
9. The first amended storage suite was RED in missing/unowned partial expectations: persisted recovery correctly selected a derived owned candidate and rolled unavailable committed bytes to zero. Updated assertions now verify the unrelated deterministic file remains byte-for-byte intact and the derived candidate is persisted.
10. The real production sender tests are GREEN: BrokenPipe pauses with `send_claimed = 0`, and cancellation during ACK delivery keeps `cancelled`, zero progress, and a rejected stale callback.

### Exact round-2 verification

- `cargo test --manifest-path src-tauri/Cargo.toml --target-dir G:\codex-localnet-target --lib receive_paths::tests -- --nocapture` — PASS, 18 passed.
- `cargo test --manifest-path src-tauri/Cargo.toml --target-dir G:\codex-localnet-target --lib storage::tests` — PASS, 41 passed at the focused checkpoint; the final suite includes the additional paused-metadata test.
- Focused production tests `production_resumable_send_disconnect_pauses_the_claimed_database_row` and `production_resumable_send_rejects_stale_ack_progress_after_cancel` — PASS, 1 each.
- `cargo test --manifest-path src-tauri/Cargo.toml --target-dir G:\codex-localnet-target --locked --lib` — PASS, 114 passed, 0 failed.
- `cargo check --manifest-path src-tauri/Cargo.toml --target-dir G:\codex-localnet-target --locked` — PASS, no warnings.
- `cargo check --manifest-path src-tauri/Cargo.toml --target-dir G:\codex-localnet-target --release --locked` — PASS, optimized profile.
- `cargo fmt --manifest-path src-tauri/Cargo.toml -- --check` — PASS.
- `git diff --check` — PASS.
- `pnpm release:check` with PowerShell as `npm_config_script_shell` — PASS, `Weline Localnet release version: 0.1.7`.

### Round-2 files and concerns

- `src-tauri/src/commands.rs` and `src-tauri/src/network/runtime.rs` — strict existing-directory v2 acceptance and explicit protocol-aware cancellation.
- `src-tauri/src/network/transfer.rs` and `src-tauri/src/network/resumable_transfer.rs` — production sender seam/CAS behavior, exact owned receive handle, two-phase finalization integration, and context-specific transport/source errors.
- `src-tauri/src/receive_paths.rs` and `src-tauri/src/storage.rs` — stable file identity, journal/reservation ordering, full recovery snapshot CAS, transactional terminal tombstones, and crash/race regressions.
- No known Task 5 blocker remains. Finalization still uses the structurally same-volume hard-link path first; its exclusive-copy fallback retains the same journal/identity validation and never overwrites a candidate. Task 6 volume gates and Task 7 reconnect scheduling remain deliberately out of scope.

---

## Review fix round 3 — 2026-08-24

### Corrected implementation

- The copy fallback no longer creates or writes the visible final pathname. It exclusively creates a hidden same-directory stage, captures stable identity from that exact handle, and appends/syncs a `CopyPrepared` journal before the first byte is copied. Copy, truncate, seek, flush, and sync all use the owned stage handle. Only a fully synced stage is hard-linked to an exact-token reservation, and only then is the materialized `Created` identity journal synced.
- A `CopyPrepared` record is never accepted as a completed file. Retry reopens the journaled stage with no-follow semantics, verifies its exact stable identity, and restarts the copy safely. If the complete stage was already linked but the process crashed before the `Created` journal, retry verifies candidate identity and exact source length and finishes the journal without overwriting or retransmitting. A collision arriving after copy sync is reserved/renumbered through the existing two-phase metadata switch and the user file remains unchanged.
- Completed/terminal cleanup removes a journal-owned copy stage only after validating the exact transfer, token, deterministic stage leaf, and stable file identity. Removing the hidden link cannot remove the finalized hard link. Missing, replaced, or non-regular stages fail closed and retain retriable ownership metadata.
- Partial recovery now opens once with platform no-follow behavior, verifies type and stable identity from that exact handle, and performs length inspection, truncation, sync, and recovery reads through that same handle. Unix/macOS uses `O_NOFOLLOW` and open-handle `dev`/`ino`; Windows uses `FILE_FLAG_OPEN_REPARSE_POINT`, rejects reparse/directory attributes from `GetFileInformationByHandle`, and compares volume serial plus the complete high/low file index. A pathname swapped after proof is never reopened or truncated.
- Startup terminal-tombstone cleanup defers only explicitly unavailable-media I/O (`NotFound`, Windows invalid-drive/not-ready/device-not-connected, or Unix `ENODEV`/`ENXIO`/`ESTALE`). `Storage::open` continues and retains the tombstone; later startup retries after the media returns. Corrupt ownership or identity mismatch is not treated as cleaned and leaves both replacement data and tombstone intact.

### Crash and no-follow ordering

1. Copy fallback: create-new hidden stage → sync stage creation → capture exact-handle identity → append/sync `CopyPrepared` → truncate/seek/copy through that handle → flush/sync → hard-link into the token-owned reservation → append/sync `Created` → completed DB CAS → identity-gated stage/partial/marker cleanup.
2. A crash before the prepared journal leaves only a hidden unowned orphan, which is never cleanup or recovery authority. A crash during copy leaves a `CopyPrepared` incomplete hidden stage that is never accepted. A crash after sync or final link but before `Created` remains journal/identity-owned and retryable; exact identity plus length distinguishes the complete linked case.
3. Recovery opens the partial with no-follow flags, validates regular-file attributes and marker identity from the returned handle, then retains that handle for metadata, truncation, sync, and reads. A concurrent rename/replacement changes only the pathname and cannot redirect an already-open operation.
4. A retryable unavailable-media cleanup error exits that tombstone attempt without deleting its SQLite row. Startup proceeds; identity mismatch remains unresolved rather than being downgraded to success.

### RED → GREEN evidence for round 3

1. `copy_fallback_never_accepts_an_incomplete_candidate_and_retries_idempotently` was RED at compile time with `E0432` for missing `finalize_reserved_receive_copy_fallback_with_hooks` and `E0599` for missing `BeforeCopy`, `DuringCopy`, `AfterCopySync`, and `BeforeCopyMaterializedJournal` phases. The journaled hidden-stage fallback made every injected before/during/after-sync/before-materialized-journal interruption GREEN.
2. The first no-follow implementation was RED with `E0658: use of unstable library feature io_error_more` for `ErrorKind::FilesystemLoop`; raw platform `ELOOP` classification removed the unstable API while retaining fail-closed behavior. Existing symlink/reparse replacement coverage and the full Windows suite are GREEN.
3. Stage-cleanup coverage was RED at compile time with `E0432` for missing `remove_owned_finalization_stage`; exact journal/token/identity cleanup made it GREEN while preserving the finalized hard link.
4. `recovery_truncates_the_verified_handle_not_a_replacement_path` was RED at compile time with `E0599` for the missing injected recovery seam. It is GREEN after same-handle recovery: the detached owned inode is truncated to committed length and the concurrent replacement path retains all bytes.
5. `startup_defers_terminal_tombstone_cleanup_while_media_is_unavailable` was RED at runtime because `Storage::open` returned `Io(NotFound, "接收目录或磁盘当前不可用")`. Narrow unavailable-media deferral makes startup GREEN, retains the tombstone while absent, and completes exact owned cleanup when the directory returns.
6. `copy_fallback_preserves_a_collision_arriving_after_stage_sync` and `terminal_cleanup_identity_mismatch_preserves_data_and_tombstone` are GREEN production-path regressions: no late collision is overwritten, and ownership corruption is neither swallowed nor marked cleaned.

### Exact round-3 verification

- `cargo test --manifest-path src-tauri/Cargo.toml --target-dir G:\codex-localnet-target --locked --lib receive_paths::tests` — PASS, 20 passed, 0 failed.
- `cargo test --manifest-path src-tauri/Cargo.toml --target-dir G:\codex-localnet-target --locked --lib storage::tests` — PASS, 45 passed, 0 failed.
- `cargo test --manifest-path src-tauri/Cargo.toml --target-dir G:\codex-localnet-target --locked --lib network::resumable_transfer::tests` — PASS, 27 passed, 0 failed.
- `cargo test --manifest-path src-tauri/Cargo.toml --target-dir G:\codex-localnet-target --locked --lib` — PASS, 119 passed, 0 failed.
- `cargo check --manifest-path src-tauri/Cargo.toml --target-dir G:\codex-localnet-target --locked` — PASS, debug profile with no warnings.
- `cargo check --manifest-path src-tauri/Cargo.toml --target-dir G:\codex-localnet-target --release --locked` — PASS, optimized profile.
- `cargo fmt --manifest-path src-tauri/Cargo.toml -- --check` — PASS.
- `git diff --check` — PASS.
- `pnpm release:check` with Windows PowerShell as `npm_config_script_shell` — PASS, `Weline Localnet release version: 0.1.7`.

### Round-3 files, self-review, and concerns

- `src-tauri/src/receive_paths.rs` — owned hidden copy stage, copy crash phases/idempotent retry, stage cleanup, and platform no-follow exact-handle identity helpers/tests.
- `src-tauri/src/storage.rs` — same-handle recovery/truncation, identity-gated stage cleanup integration, unavailable-media tombstone deferral, and replacement/media regressions.
- Self-review confirms incomplete `CopyPrepared` state is not completion authority; visible candidates appear only after stage sync; collisions remain no-clobber; no filesystem mutation follows a second path open after identity proof; tombstones are deleted only after exact owned cleanup.
- No known Task 5 blocker remains. Task 6 volume gates and Task 7 reconnect discovery/scheduling remain out of scope.

---

## Review fix round 4 — 2026-08-25

### Corrected implementation

- The unsupported-hard-link fallback now uses the exact token-reserved final candidate as its only materialization target. It exclusively creates that candidate with platform no-follow semantics, syncs the empty file, captures its stable handle identity, appends/syncs a `CopyPrepared` row containing that candidate identity, then truncates, copies, flushes, and syncs through the same open handle. The fallback never creates a hidden stage and never invokes `hard_link` again.
- Retry opens the journaled candidate with no-follow writable semantics, verifies both the reservation token and stable handle identity, and restarts the copy through that same handle. `CopyPrepared` remains non-completion authority; only a fully synced `Created` row can be recovered as finalized. Startup coverage proves an interrupted candidate remains paused and incomplete, then completes only after an idempotent retry and a later restart.
- A candidate that is replaced after reservation or after copy sync is never overwritten. The still-open owned handle receives the copy, pathname identity validation detects the replacement, and the existing two-phase flow reserves/creates a numbered candidate, journals its identity, persists the metadata switch, releases the old reservation marker, and restarts. User data at the replaced name remains byte-for-byte unchanged.
- The fast hard-link path remains first choice. Its post-link identity mismatch no longer deletes by pathname; a concurrent replacement is preserved and the attempt fails closed.
- New finalization rows have no `staged` pathname. Old round-3 `CopyPrepared` rows remain readable and recoverable; a minimal optional `staged_identity` preserves separate legacy-stage cleanup authority after recovery into a different direct candidate. Legacy stage cleanup performs no pathname unlink after handle verification. Missing, replaced, or still-present legacy stages therefore remain pending rather than risking deletion of a concurrent replacement.

### Crash ordering and ownership proof

1. Fast path: append/sync `Prepared` → attempt one hard link → verify candidate identity → append/sync `Created`. An identity mismatch aborts without deleting any pathname.
2. Copy fallback: create-new/no-follow candidate → sync candidate creation → verify reservation token and capture handle identity → append/sync `CopyPrepared` → truncate/seek/copy/flush/sync through that same handle → reverify reservation and pathname identity → append/sync `Created` → completed DB CAS.
3. A crash before or during copy leaves only `CopyPrepared`, which startup never accepts. Retry reopens the same identity and starts from offset zero. A crash after candidate sync but before `Created` also restarts; a crash after the synced `Created` row is recoverable by startup after size/SHA-256 validation.
4. A replacement collision is handled before any write to the replacement: reserve a numbered marker and create-new candidate, journal its identity, persist the claimed destination switch, release the old marker, then restart copy. Any untrusted or detached old inode is left untouched.
5. Legacy stage rows can still drive recovery, but cleanup retains the identity journal/tombstone instead of re-resolving and unlinking the stage pathname. This is intentionally fail-closed and affects only backward rows; round-4 fallback creates no stage.

### RED → GREEN evidence for round 4

1. The first focused RED failed to compile for the new injected hard-link seam and post-verification cleanup hook, proving the new tests were absent from production behavior.
2. With only the seams wired to the old implementation, `unsupported_hard_link_fallback_materializes_without_a_second_link_attempt` failed with `Unsupported`, proving the stage path made a second hard-link call.
3. `copy_fallback_never_accepts_an_incomplete_candidate_and_retries_idempotently` failed because no exact candidate existed during copy; `copy_fallback_preserves_a_collision_arriving_after_stage_sync` failed because the stage design had no candidate handle to protect from replacement.
4. `legacy_stage_cleanup_never_unlinks_a_concurrent_path_replacement` failed after the old cleanup removed the replacement pathname. It is GREEN with deletion removed from the legacy path.
5. The final regressions are GREEN for one unsupported link probe, before/during/after-sync/before-materialized/after-materialized crash points, restart non-acceptance and retry, post-sync collision renumbering, fast-link replacement preservation, legacy-stage recovery, and legacy-stage replacement preservation.

### Exact round-4 verification

- `$env:CARGO_TARGET_DIR='G:\codex-localnet-target'; cargo test --manifest-path src-tauri/Cargo.toml --locked --lib receive_paths::tests` — PASS, 24 passed, 0 failed.
- `$env:CARGO_TARGET_DIR='G:\codex-localnet-target'; cargo test --manifest-path src-tauri/Cargo.toml --locked --lib storage::tests` — PASS, 46 passed, 0 failed.
- `$env:CARGO_TARGET_DIR='G:\codex-localnet-target'; cargo test --manifest-path src-tauri/Cargo.toml --locked --lib` — PASS, 124 passed, 0 failed.
- `$env:CARGO_TARGET_DIR='G:\codex-localnet-target'; cargo check --manifest-path src-tauri/Cargo.toml --locked` — PASS, debug profile with no warnings.
- `$env:CARGO_TARGET_DIR='G:\codex-localnet-target'; cargo check --manifest-path src-tauri/Cargo.toml --release --locked` — PASS, optimized profile.
- `cargo fmt --manifest-path src-tauri/Cargo.toml -- --check` — PASS.
- `git diff --check` — PASS.
- `pnpm release:check` with Windows PowerShell as `npm_config_script_shell` — PASS, `Weline Localnet release version: 0.1.7`.

### Round-4 files, self-review, and concerns

- `src-tauri/src/receive_paths.rs` — direct owned-candidate copy state machine, no-follow create/open, injected hard-link seam, collision renumbering, backward journal identity, and race/crash tests.
- `src-tauri/src/storage.rs` — production startup regression proving incomplete candidates remain paused and a fully materialized retry completes idempotently.
- Self-review confirms the normal fallback has one hard-link probe only, every candidate write stays on its verified handle, no identity mismatch path deletes a candidate/stage name, `CopyPrepared` cannot complete, and reservation/metadata switching remains journal-first and no-clobber.
- No known round-4 blocker. Intentional compatibility tradeoff: a pre-round-4 legacy stage pathname is retained with its journal/tombstone instead of being unlinked after handle verification; new fallback executions never create such a stage. Task 6 volume gates and Task 7 reconnect discovery/scheduling remain out of scope.

---

## Fix Round 5 — 2026-08-25

### Corrected implementation

- Backward finalization rows now append a checksummed, synced legacy-stage cleanup record before any namespace mutation. `Prepared` binds the exact journaled stage identity and, on macOS, its cryptographically random quarantine leaf; `Removed` is appended only after the owned link is gone. Startup reads the latest valid record and repeats an interrupted cleanup. A durable `Removed` record lets journal/tombstone/DB ownership cleanup finish after a crash between link removal and metadata cleanup.
- Windows opens the legacy stage with `DELETE | FILE_READ_ATTRIBUTES`, full read/write/delete sharing, and `FILE_FLAG_OPEN_REPARSE_POINT`; reparse points and directories are rejected. The still-open handle is checked against the journaled volume serial and complete 64-bit file index, the path identity is rechecked around durable preparation, then `SetFileInformationByHandle(FileDispositionInfoEx)` requests delete plus POSIX semantics on that exact handle. Only explicit API/filesystem unsupported errors use the compatible `FileDispositionInfo` fallback. Sharing/access/delete-pending failures defer without clearing ownership.
- Windows never performs `verify handle; remove_file(path)`. After disposition the verified handle is closed, the stage namespace result is inspected, and only confirmed absence advances the journal to `Removed`. A replacement at the old pathname is never a deletion target and leaves cleanup pending; the finalized hard link remains byte-for-byte intact.
- macOS uses a process-wide cleanup lock plus a same-directory UUIDv4 quarantine leaf recorded durably before mutation. `renameatx_np(..., RENAME_EXCL)` moves without overwrite, the quarantine is immediately opened with `O_NOFOLLOW` and checked by `dev + ino`, then the unpredictable name alone is unlinked and the directory is synced. Identity mismatch is atomically restored without overwrite; a conflicting restore remains journaled and unresolved, never deleted. Missing exclusive-rename support also remains fail-closed and pending.
- Completed startup cleanup now reaches the existing journal/reservation/DB ownership clear only after exact stage retirement. Terminal startup cleanup likewise deletes its tombstone only after exact retirement. Missing media keeps the durable state; returning media retries and completes.
- Added only feature gates to existing dependencies: UUID v4 generation and the `windows-sys` Foundation constants needed for explicit disposition/fallback error handling.

### Crash ordering and restart proof

1. Windows: open no-follow with delete access → verify handle identity → append/sync `Prepared` → recheck pathname identity → set disposition on that handle → close handle → confirm stage absence → append/sync `Removed` → caller removes finalization journal and clears ownership state. A crash after disposition closes the handle; `Prepared + absent` is completed on restart. A crash after `Removed` returns completion immediately.
2. macOS: open/verify stage → generate random same-directory quarantine leaf → append/sync `Prepared` → atomic no-replace rename → open no-follow and verify quarantine identity → unlink only quarantine → fsync directory → append/sync `Removed`. Restart handles stage-present, quarantine-present, and post-unlink absence idempotently.
3. A pre-mutation stage replacement fails identity validation. A post-open replacement is detected by the second pathname identity check. On Windows disposition remains handle-targeted; on macOS a quarantine mismatch is restored without overwrite. Every mismatch retains the journal/tombstone and user data.
4. The final destination is a separate hard link to the same data identity. Retiring only the legacy stage link reduces the link count without changing final bytes.

### RED → GREEN evidence for round 5

1. `identity_owned_legacy_stage_is_removed_while_the_final_hard_link_remains` was RED at runtime because `remove_owned_finalization_stage` returned `false`; it is GREEN with the stage absent and final hard-link bytes unchanged.
2. `startup_retires_a_completed_legacy_stage_and_clears_database_ownership` was RED because the stage still existed and completed ownership remained set. It is GREEN with the stage/journal retired and `destination_reserved = 0`, `reservation_token = NULL`.
3. `unavailable_legacy_stage_media_defers_then_finishes_terminal_cleanup` was RED after media return because the terminal tombstone remained. It is GREEN: unavailable startup defers, restored media removes the exact stage, clears the tombstone, and preserves the final link.
4. The crash matrix was RED at compile time for missing `LegacyStageCleanupPhase` and the phase-hook cleanup seam. Windows is GREEN for crashes after open verification, durable preparation, disposition, and journal completion; every restart converges without touching the final link.
5. The prior replacement regression was strengthened to require durable `Prepared` state. The filesystem and storage tests prove a concurrent replacement is preserved while both finalization and terminal cleanup remain pending.
6. macOS-gated tests cover preparation, rename, quarantine verification, unlink, journal completion, and mismatch restoration. The production and `cfg(test)` paths type-check for `aarch64-apple-darwin` through a minimal crate using the exact real dependencies.

### Exact round-5 verification

- `$env:CARGO_TARGET_DIR='G:\codex-localnet-target'; cargo test --manifest-path src-tauri/Cargo.toml --locked --lib receive_paths::tests` — PASS, 26 passed, 0 failed.
- `$env:CARGO_TARGET_DIR='G:\codex-localnet-target'; cargo test --manifest-path src-tauri/Cargo.toml --locked --lib storage::tests` — PASS, 49 passed, 0 failed.
- `$env:CARGO_TARGET_DIR='G:\codex-localnet-target'; cargo test --manifest-path src-tauri/Cargo.toml --locked --lib network::resumable_transfer::tests` — PASS, 27 passed, 0 failed.
- `$env:CARGO_TARGET_DIR='G:\codex-localnet-target'; cargo test --manifest-path src-tauri/Cargo.toml --locked --lib` — PASS, 129 passed, 0 failed.
- `$env:CARGO_TARGET_DIR='G:\codex-localnet-target'; cargo check --manifest-path src-tauri/Cargo.toml --locked` — PASS, debug profile with no warnings.
- `$env:CARGO_TARGET_DIR='G:\codex-localnet-target'; cargo check --manifest-path src-tauri/Cargo.toml --release --locked` — PASS, optimized profile.
- Minimal real-dependency `receive_paths.rs` check with `--target aarch64-apple-darwin --tests --offline` — PASS; production and macOS-gated tests type-check. The temporary check crate was removed afterward.
- Full Tauri `cargo check --target aarch64-apple-darwin` on Windows reached Apple dependencies but stopped before Localnet code because no Apple Objective-C `cc` toolchain is available; this is an environment-only cross-check limit, not a passing macOS runtime claim.
- `cargo fmt --manifest-path src-tauri/Cargo.toml -- --check` — PASS.
- `git diff --check` — PASS.
- `pnpm release:check` with Windows PowerShell as `npm_config_script_shell` — PASS, synchronized version `0.1.7`.

### Round-5 self-review and concerns

- Windows cleanup uses the strongest available exact-handle mechanism and a narrow compatibility fallback; the required real hard-link and replacement races run on Windows. No pathname delete follows identity proof.
- macOS follows the required unpredictable-quarantine design and fails closed when exclusive rename or identity proof is unavailable. Its code and crash tests type-check for Apple ARM, but macOS runtime execution still belongs to native CI because this Windows host has no Apple Objective-C toolchain.
- No known unsafe namespace race remains beyond the deliberately accepted macOS quarantine protocol. The in-process lock, random name, no-replace rename/restore, immediate no-follow identity check, and directory fsync are all present.
- Task 6 volume gates and Task 7 reconnect discovery/scheduling remain out of scope.
