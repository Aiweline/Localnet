# Weline Localnet 100 GiB Resumable Transfer Design

Date: 2026-08-24
Status: approved for implementation planning
Target release: 0.2.0

## 1. Outcome

Weline Localnet will support files up to and including 100 GiB between upgraded peers, validate every transferred chunk, preserve confirmed progress across network loss and application restart, and resume automatically after the friend becomes reachable again. Before accepting or resuming a transfer, the receiver will verify that the destination volume has enough free space and that its file system can represent the requested file.

The existing text, friend, discovery, and legacy file-transfer flows remain compatible with 0.1.x peers. A 0.2.0 peer uses the resumable protocol only after both sides advertise support for it.

## 2. Goals

- Allow a single file of at least 100 GiB. The local product policy is 100 GiB inclusive; protocol size fields remain `u64` and contain no 2 GiB constant.
- Transfer protocol v2 uses fixed-size chunks with SHA-256 validation and acknowledged committed offsets.
- Preserve partial data and committed offsets after recoverable network failures and application restart.
- Resume automatically when the same friend reconnects, without a second acceptance prompt.
- Check free space and known file-system single-file limits before initial acceptance and every resume.
- Keep 0.1.x interoperability for friendship, text, presence, and legacy files at or below 2 GiB.
- Publish a bilingual `v0.2.0` GitHub changelog containing the four user-requested capabilities.

## 3. Non-goals

- There is no cloud relay, offline server queue, cross-network relay, folder transfer, deduplication, compression, or multi-source download.
- The 100 GiB policy is not exposed as a user preference in this release.
- Legacy protocol v1 transfers do not gain resume support.
- A transfer is not resumed if the source file changed, the friendship was removed, the user cancelled it, the destination became unsafe, or integrity validation failed.

## 4. Compatibility and capability negotiation

`CONTROL_PROTOCOL` remains `/localnet/control/1`, and the existing numeric `PROTOCOL_VERSION` remains `1`. `Hello` requests and responses gain an optional, default-empty `capabilities` list. CBOR map decoding ignores fields unknown to 0.1.x clients, so presence, friendship, and text remain interoperable.

The new capability is `file-resume-v2`. The peer table persists the last advertised capabilities. The sender selects:

- `/localnet/file/1` with the existing 2 GiB policy when the peer does not advertise `file-resume-v2`;
- `/localnet/file/2` with the 100 GiB policy when both peers advertise `file-resume-v2`.

A file above 2 GiB selected for a legacy peer is rejected before hashing with an actionable “the other device must upgrade” message. Existing v1 transfers continue to use their current behavior and storage semantics.

## 5. Transfer offer and persisted model

Protocol v2 extends `TransferOffer` with defaultable fields:

- `transfer_protocol: u16` (`1` or `2`);
- `chunk_size: u32` (exactly 4 MiB for v2);
- `chunk_count: u32`;
- `manifest_sha256: Option<String>`.

The existing `file_size: u64` and `sha256` remain. `sha256` is the conventional whole-file SHA-256 calculated during preparation. `manifest_sha256` commits to the ordered chunk list and is calculated as SHA-256 over repeated canonical records `(chunk_index: u32 big-endian, chunk_length: u32 big-endian, chunk_sha256: 32 bytes)`.

SQLite migrations add:

- `peers.capabilities_json TEXT NOT NULL DEFAULT '[]'`;
- transfer columns `transfer_protocol`, `chunk_size`, `chunk_count`, `manifest_sha256`, `partial_path`, `source_modified_ns`, and `send_claimed`;
- `transfer_chunks(transfer_id, chunk_index, chunk_length, sha256, PRIMARY KEY(transfer_id, chunk_index))`.

During outbound preparation, one streaming pass computes the whole-file SHA-256, each 4 MiB chunk hash, and the manifest hash. Chunk hashes are inserted in one SQLite transaction. The source size and nanosecond modification timestamp are persisted. Before every initial send or resume, metadata must still match; while reading each chunk, its hash must match the stored hash. A changed source permanently fails the transfer instead of sending mixed content.

Incoming v2 transfers store a deterministic, hidden `.part` path in the selected destination directory. The final destination reservation and `.part` file survive recoverable disconnects and restart. Received chunk records are committed only after the bytes are synchronized to disk.

## 6. File protocol v2

The v2 stream begins with the existing length-prefixed CBOR header mechanism. `TransferStreamHeader` contains:

- `transfer_id: String`;
- `version: u16`;
- `start_offset: u64`;
- `chunk_size: u32`.

After the header, each data frame is:

1. `chunk_index: u32` big-endian;
2. `chunk_length: u32` big-endian;
3. `chunk_sha256: [u8; 32]`;
4. exactly `chunk_length` data bytes.

The receiver rejects a zero-length non-final frame, an oversized chunk, an index or offset that is not the next committed chunk, a length inconsistent with the advertised file size, or a hash mismatch. It writes the chunk, calls `sync_data`, inserts the chunk record and advances `transferred_bytes` in one database transaction, then returns an 8-byte big-endian committed offset acknowledgement. The sender does not count progress as resumable until this acknowledgement arrives.

After the last chunk, the receiver recomputes the manifest hash from its persisted ordered chunk records and compares it with the offer. It also verifies total size, atomically finalizes without overwriting an existing destination, marks the transfer completed, and returns the final committed offset. This avoids rereading a 100 GiB partial file after a resume while retaining a cryptographic commitment to every byte and its order.

If the final acknowledgement is lost, a later resume query reports `completed`; the sender then marks its matching record completed without retransmitting data.

## 7. Resume state machine

`TransferStatus` gains `Paused`. Only recoverable transport failures produce `Paused`; cancellation, source mutation, integrity failure, unsafe destination, and explicit refusal remain terminal.

The control protocol gains a request that is sent only to peers advertising `file-resume-v2`:

```text
TransferResumeQuery { transfer_id }
```

Its response contains one of:

```text
TransferResume { transfer_id, state: receiving, committed_bytes }
TransferResume { transfer_id, state: completed, committed_bytes }
Rejected { code, message }
```

When a v2 peer is recorded online, the runtime queries all paused outgoing v2 transfers for that peer. A storage compare-and-set on `send_claimed` prevents duplicate streams when mDNS, beacon discovery, Identify, and Hello events arrive together. The receiver retains the existing `receive_claimed` compare-and-set so only one stream writes a transfer.

For a `receiving` response, the sender verifies that the offset is not greater than the file size and is aligned to a chunk boundary unless it equals the complete size. It updates local acknowledged progress and opens `/localnet/file/2` at that offset. For a `completed` response, it marks the outgoing transfer and message delivered.

On application startup:

- v2 `transferring` rows become `paused`, claims are cleared, and partial files and reservations are retained;
- if an incoming partial file is longer than the committed offset, it is truncated to that offset;
- if it is shorter, the committed offset rolls back to the largest complete existing chunk boundary and later chunk rows are deleted;
- v1 `transferring` rows keep the current fail-and-clean behavior.

Manual cancel uses a conditional state transition, interrupts active streams, removes only the owned partial/reservation artifacts, deletes chunk rows, and notifies the peer. A completed destination is never removed.

## 8. Destination and volume preflight

A focused `volume_preflight` module exposes a pure policy function plus platform probes. The policy requires:

- `available_bytes >= remaining_bytes + 64 MiB`;
- the known maximum single-file size is at least the advertised file size;
- the destination resolves to the same volume used for the partial file;
- the destination directory remains writable.

Windows uses `GetVolumePathNameW`, `GetVolumeInformationW`, and `GetDiskFreeSpaceExW` through target-specific `windows-sys`. macOS uses `statfs` through target-specific `libc`. The probe returns a normalized file-system name, available bytes, and an optional maximum file size.

Known FAT32 names (`FAT32`, `MSDOS`, and `msdos`) use a maximum file size of `4 GiB - 1 byte`. NTFS, APFS, HFS+, and exFAT support the 100 GiB product policy. Other local or network file systems are allowed only when the platform can report available space; an unavailable probe blocks acceptance or resume with a recoverable message rather than guessing.

Preflight runs before manual acceptance, before automatic acceptance, and before every resume. Initial failure leaves the request awaiting manual action. Resume failure leaves it paused and exposes the reason; reconnect or a later retry re-runs the probe.

## 9. Error and UI behavior

The transfer card displays:

- `等待对方接受` for awaiting acceptance;
- progress for active transfer;
- `网络中断，等待自动恢复` for a paused transfer without a destination error;
- an actionable disk/file-system error while paused;
- the existing completed, cancelled, and failed states.

Paused transfers retain the Cancel action. The UI does not offer a misleading “restart from zero” action. Presence reconciliation remains independent; a friend becoming online triggers the backend resume query even if the WebView missed the presence event.

User-facing errors distinguish: peer needs upgrade, insufficient free space, FAT32 file-size limit, source changed, invalid resume offset, chunk integrity failure, destination unavailable, and network waiting state.

## 10. Concurrency and security invariants

- Exactly one outbound send claim and one inbound receive claim may exist per transfer.
- Incoming bytes are accepted only from the friend and peer ID recorded on the transfer.
- A resume offset is receiver-authoritative but must match a persisted chunk boundary.
- Every chunk is bounded to 4 MiB and validated before acknowledgement.
- Persisted progress never exceeds bytes synchronized to disk.
- Finalization never overwrites an existing file and never follows an unsafe path.
- Recoverable errors preserve partial state; permanent integrity, identity, source, or cancellation errors clean owned temporary artifacts.
- All arithmetic uses checked `u64`/`u32` conversions and rejects overflow.

## 11. Verification

Automated Rust tests cover:

- exactly 100 GiB accepted and 100 GiB plus one byte rejected by local policy;
- a v2 offer has no 2 GiB protocol rejection, while a legacy peer gets the upgrade error;
- deterministic chunk and manifest hashes, corrupted chunks, reordered chunks, truncated frames, and overflow;
- interrupted in-memory/file streams resuming from the last acknowledged chunk without rewriting the prefix;
- duplicate send/receive claims and stale resume responses;
- startup reconciliation for longer, shorter, missing, and completed partial files;
- insufficient space, the 64 MiB margin, FAT32 above 4 GiB, and supported NTFS/APFS/exFAT policy;
- cancellation and permanent-error cleanup without deleting a completed destination;
- capability decoding with missing fields to protect 0.1.x compatibility.

The release gate continues to run all Rust tests, the presence regression, TypeScript checks, and production builds. Windows CI builds the x64 installer and portable binary; macOS CI builds and inspects the Universal DMG. A local codec integration test deliberately drops the stream after several chunks, reconnects with the reported committed offset, and verifies byte-for-byte equality.

## 12. Release and changelog

The feature release is `0.2.0`, synchronized through `scripts/release-version.mjs`. A bilingual `docs/releases/v0.2.0.md` contains these release items:

- default support for files up to 100 GiB with no 2 GiB protocol hard limit;
- per-chunk integrity validation and resumable transfers;
- disk-space and FAT32/file-system preflight;
- automatic continuation after network recovery instead of restarting.

The release workflow uses `docs/releases/v<version>.md` as the GitHub Release body when present and falls back to the existing generic bilingual notes otherwise. The formal release is created only after merge to `main`, and the resulting tag, five required assets, checksums, public visibility, and Release body are read back before completion is reported.
