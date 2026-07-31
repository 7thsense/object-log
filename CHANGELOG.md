# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `s3_engine_produce_fetch_round_trip` live test; `scripts/s3-evidence.sh`
  operator evidence runner (MinIO/Garage/AWS/R2).
- Garage operator evidence recorded (path-style multipart + engine path).

### Changed

- CI: release-mode `perf_throughput` ratio gate; live MinIO `s3-minio` job.
- MSRV CI builds with `--all-features`.

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
