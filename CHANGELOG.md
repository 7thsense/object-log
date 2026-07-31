# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- HELIX frame and design stack evolved to match ADR-002 / 0.2.x: vision, PRD
  (FR-1..FR-30), concerns, feature registry, CONTRACT-001/002 v2, TD-001..003,
  test plan, and implementation plan. Kafka-shaped core, CAS `ObjectStore`, and
  epoch-guard requirements removed from governing docs.
- Library user-story AC floor waived in concerns; P0 FR→named-test mapping is
  the coverage gate.

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
