# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] — 2026-11-08

### Added

- Optional `native` feature: a bounded native append-log facade (`NativeLog`,
  `NativeNamespace`, `NativeWriter`, `NativeAppend`, `NativeHead` and related
  types) over Local or S3 storage, independent of the legacy sequencer port.
  Appends publish authenticated state and log groups atomically, hold caller
  admission until physical I/O completes, and resolve ambiguous publications
  through `submit` / `resolve`. `NativeLog::local`, `NativeLog::s3` and
  `NativeLog::preflight_s3` construct it.
- `NativeLog::plan_read` seals a read plan, and `NativeReadPlan::requirements`
  returns a `NativeReadRequirements` (identity, validation and frame bytes,
  hydration reads and bytes), so a caller can reserve exact admission before
  `read_planned`. `read_bounded` and `ObjectLogError::ReadLimitExceeded`
  reject an authenticated payload larger than the caller's limit before
  hydrating it.
- `BoundedMetadataStore`, `MetadataSnapshot`, `MetadataSwap`, `MetadataLimits`
  and `MetadataOwners`: bounded, version-bound metadata reads and conditional
  writes for the Local and S3 stores.
- Bounded replay: `FetchPlan`, the `BoundedSequencer` page-lookup port, and
  strict S3 range reads that fail unless the response is `206` with the exact
  requested length.
- Native conditional log-head sequencing: `Sequencer::commit_conditionally`,
  `Sequencer::supports_conditional_commit`, `ConditionalCommitOutcome`,
  `ObjectLogError::HeadRejected`, and `LogEngine::produce_conditional_at_epoch`.
  `ManifestSequencer` implements them. Sequencers that do not implement them
  reject any head condition.
- `Sequencer::acquire_authoritative_epoch` and
  `Sequencer::authoritative_partition_state` (with
  `AuthoritativePartitionState`) read durable epoch and offset bounds directly.
  Both fail on sequencers that do not implement them.
- `AppendOwner` and `LogEngine::produce_owned_at_epoch`: an append owner stays
  alive until the append's physical uploads finish.
- `S3ClientOptions` and `S3BlobStore::new_owned_with_options` configure an
  S3 client that the store owns, with an explicit transport and root
  certificates.
- `NativeLog::advance_writer` adopts the head a stored publication wrote as the
  writer's captured head without any storage request, so a single-owner writer
  appends without `refresh_writer`. It refuses a publication that was not
  stored, does not directly extend the captured head, or belongs to another
  writer.
- The native writer keeps its right index spine across appends. With it, a
  healthy single-entry append makes no storage read (it previously made
  `2h + 6`) and `h + 5` writes. After `acquire` or `refresh_writer` the first
  append reads the spine (`h + 1` pages) once.
- `NativeLog::plan_range` plans consecutive entries of a cut with one
  root-to-leaf walk, for rebuilds that fetch payloads in parallel.

- `s3_engine_produce_fetch_round_trip` live test; `scripts/s3-evidence.sh`
  operator evidence runner (MinIO/Garage/AWS/R2).
- Garage operator evidence recorded (path-style multipart + engine path).
- Public microsite (`website/`, Hugo+Hextra) with product-microsite-ia sections
  (Why / Get Started / Concepts / Reference), Playwright e2e (screenshots +
  dead links), and GitHub Pages deploy workflow.

### Changed

- **Breaking:** `MetadataSwap::Stored` is now `MetadataSwap::Stored(MetadataSnapshot)`,
  the snapshot of what was stored with its new version token (the local store's
  file identity, or the ETag of the conditional PUT response). A successful S3
  PUT reply with no ETag is now an error (outcome unknown), never a reread.
  Implementers of `BoundedMetadataStore` and code matching on `Stored` must
  update.
- **Breaking:** the minimum supported Rust version is now 1.99.0 (was 1.88).
- `Sequencer::commit` no longer promises that an `Err` leaves nothing indexed:
  the manifest sequencer publishes each partition independently, so an error
  can follow a durable earlier partition. `InMemorySequencer` validates the
  whole slice before changing any partition.
- Native immutable pages and data frames are created with a conditional write
  first and read only if the store rejects it, still validated byte for byte.
- One native append uploads its immutable objects with up to eight concurrent
  writes and returns only when all are stored; the head is written afterwards.
- CI: release-mode `perf_throughput` ratio gate; live MinIO `s3-minio` job.
- MSRV CI builds with `--all-features`.

### Fixed

- `ManifestSequencer` drives its blob I/O on a one-worker multi-thread runtime
  rather than a current-thread runtime, so pooled connection tasks keep running
  between synchronous calls instead of stalling.
- S3 conditional writes no longer retry inside the SDK, and a precondition
  failure is detected from the HTTP status or error code rather than from
  message text. A lost reply to a conditional write is reported as unknown,
  not as a definite conflict.
- The manifest sequencer preserves native authority evidence across the loss
  of its catalog.

## [0.3.6] — 2026-09-24

### Changed

- Rejection classification moves behind the `Sequencer` seam, as ADR-002
  intends: the engine reports whatever `Sequencer::rejection_error` returns for
  a `Rejected` batch (default: `ObjectLogError::Sequencer`), and
  `ManifestSequencer` returns `ObjectLogError::Fenced` when the partition was
  fenced past the batch's epoch. 0.3.5 compared epochs in the engine.

### Deprecated

- `Sequencer::partition_epoch`; the engine no longer reads it.

### Docs

- ADR-002, the PRD, CONTRACT-001 and the feature registry record the fence epoch
  the seam has carried since 0.3.3 and keep fencing policy in the sequencer.

## [0.3.5] — 2026-09-24

### Added

- `ObjectLogError::Fenced { partition, epoch, current }`: a write whose
  partition was fenced past its epoch now fails with this definite rejection
  instead of a generic `Sequencer` error. The write was not committed.
- `Sequencer::partition_epoch` (default `None`), implemented by
  `ManifestSequencer`, reports the fence epoch a handle last read. The engine
  uses it to classify rejected commits.

## [0.3.4] — 2026-09-24

### Fixed

- `ManifestSequencer::fence_epoch` fenced only the handle's cached view of a
  partition index. A handle that had not seen the index (for example a standby
  opened before the owner's first commit) stored nothing, so the old owner could
  still commit after the takeover. The fence now reads the durable index, creates
  it at the new epoch when it is missing, and retries a lost compare-and-swap.

### Added

- `Sequencer::refresh_partition`, implemented by `ManifestSequencer`, reloads one
  partition's durable index so a handle sees other writers' commits since it
  opened. The default does nothing.

## [0.3.3] — 2026-09-22

### Changed

- `ManifestSequencer` stores one conditional index object per partition instead
  of an append-only manifest per seal. Open reads that catalog by key and does
  not list the manifest prefix once an index exists. A seal that loses a
  partition's compare-and-swap fails that partition closed; it does not skip
  an offset. Data objects created with `LogEngine::new_with_writer` include
  the writer id and fence epoch in the key.

## [0.3.1] — 2026-08-01

### Fixed

- LogEngine data-object counter now resumes past existing keys under the data
  prefix on reopen. Restarting at 0 overwrote sealed objects while manifests
  still referenced the old ranges, causing `RangeOutOfBounds` / mid-JSON EOF on
  `fetch` after process restart (fireweed-481d3e43).

## [0.3.0] — 2026-07-31

Additive release on the ADR-002 engine: conformance hardening, streaming fetch,
orphan reaping, S3 evidence tooling, and a produce/consume diagnostics CLI.
No intentional break of the 0.2.0 public engine API.

### Added

- Sequencer conformance suite (`tests/sequencer_conformance.rs`) for InMemory and
  Manifest sequencers.
- Engine test: per-producer send-order contiguity on a shared partition
  (`per_producer_send_order_is_contiguous_on_shared_partition`).
- README + TD-003 consumer integration binding sketches (fjord / Niflheim / pqueue).
- Live S3 multipart + get_range test; `OBJECT_LOG_S3_*` env aliases; MinIO
  evidence runbook in CONTRIBUTING/TD-002.
- `LogEngine::fetch_stream` for bounded-RAM ordered replay (visitor API).
- Orphan reaping: `reap_orphans`, `LogEngine::reap_orphans`, and
  `live_object_ids()` on `InMemorySequencer` / `ManifestSequencer` (quiescent-only).
- `LogEngine::data_prefix`, `blob_store`, and `sequencer` accessors.
- Diagnostics CLI (`cli` feature): `object-log produce|consume|roundtrip|list|
  inspect|orphans|fetch` with file/stdin framing (`lines`/`nul`/`framed`) for
  black-box testing; Local and S3 (`cli,s3`).
- `ManifestSequencer::snapshot` / `IndexSnapshot` for inspect tooling.
- HELIX frame/contracts/TDs/test plans evolved to match ADR-002 (docs only).

### Changed

- Honest local throughput ratio assert runs in `--release` (or with
  `OBJECT_LOG_PERF_ASSERT=1`); debug builds still print the table.
- Library user-story AC floor waived in concerns; P0 FR→named-test mapping is
  the coverage gate for this embeddable library.

## [0.2.0] — 2026-06-17

**Breaking re-foundation (ADR-002).** object-log is now a buffered, multiplexing,
durability-aware object-storage **log engine** with a pluggable sequencing seam,
replacing the one-object-per-append `ObjectLogBackend`.

### Added

- `BlobStore` storage port — durable-on-return `put` (multipart on S3), `get`,
  `get_range`, `list`, `delete` — with `MemoryBlobStore`, `LocalBlobStore`, and
  `S3BlobStore` (behind the `s3` feature) adapters.
- `LogEngine<S: Sequencer>` — group-commits many batches into one object, PUTs it
  durably (durable-then-sequence), and resolves produce futures at
  `Durability::{Buffered, Durable, Sequenced}`. `FlushConfig`, `fetch`,
  `truncate_before`.
- `Sequencer` seam (sync, generic over `Meta`) + `InMemorySequencer` and a
  `BlobStore`-persisted `ManifestSequencer` (crash-durable standalone log).
- Durable-ops budget controller and `pipeline_snapshot` (TD-004).
- `#![deny(missing_docs)]` with rustdoc on the public API; crate-level quickstart
  doctest and `examples/quickstart.rs`.

### Removed

- The per-append `ObjectLogBackend`, the segment codec, `EpochGuard`, the record
  model (`AppendRecord`/`RecordHeader`/`TimestampPolicy`/`AckMode`), and the
  CAS `ObjectStore` port.

## [0.1.0]

- Initial extraction from the [fjord](https://github.com/easel/fjord) broker:
  `ObjectStore` port with `MemoryObjectStore` / `LocalObjectStore`, the
  `LogBackend` trait, and the segmented, content-addressed `ObjectLogBackend`
  with idempotent producers and an `EpochGuard` fencing hook.
- CONTRIBUTING, changelog, and CI jobs for docs, MSRV, and `cargo-deny` landed
  during the 0.1 line.
