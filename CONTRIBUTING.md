# Contributing to object-log

Thanks for your interest in improving object-log! This is a small, focused crate
— an append-only log core over a pluggable object store — and contributions that
keep it small and well-tested are very welcome.

## Development

Requires Rust **1.88+** (edition 2024; MSRV in `Cargo.toml`). Before opening a
PR, please run the same checks CI runs:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

All public items must be documented (the crate sets `#![deny(missing_docs)]`).

### Optional: diagnostics / black-box CLI

```sh
cargo run --features cli --bin object-log -- --help
printf 'a\nb\n' | cargo run --features cli --bin object-log -- \
  produce --root /tmp/olog --partition t --lines
cargo run --features cli --bin object-log -- \
  consume --root /tmp/olog --partition t --lines
cargo test --features cli --test cli_smoke
```

### Optional: live S3 operator evidence

Hermetic unit tests never require S3. CI runs MinIO automatically (`s3-minio`
job). To record **provider evidence** (and get a paste-ready TD-002 row):

```sh
./scripts/s3-evidence.sh minio
OBJECT_LOG_S3_KEY_ID=… OBJECT_LOG_S3_SECRET=… ./scripts/s3-evidence.sh garage
# AWS / R2:
OBJECT_LOG_S3_ENDPOINT=… OBJECT_LOG_S3_BUCKET=… \
OBJECT_LOG_S3_KEY_ID=… OBJECT_LOG_S3_SECRET=… \
  ./scripts/s3-evidence.sh aws
```

Paste the printed markdown row into
`docs/helix/02-design/technical-designs/TD-002-s3-adapter-retention-and-snapshots.md`
(evidence log). Legacy `FJORD_GARAGE_*` env names still work.

### Optional: honest local throughput

CI runs a release-mode floor (`perf` job, 16 MiB). Locally:

```sh
cargo test --release --test perf_throughput honest -- --nocapture
# larger sample:
OBJECT_LOG_PERF_BYTES=$((256*1024*1024)) cargo test --release --test perf_throughput honest -- --nocapture
```

## Guidelines

- Keep the dependency footprint minimal.
- New behavior should come with tests. Prefer the engine / BlobStore /
  sequencer suites under `tests/` (`engine`, `blob`, `sequencer_conformance`).
- Discuss larger changes (new traits, layout changes) in an issue first.
  Payload framing is consumer-owned; do not add Kafka or product schemas to core.

## Reporting bugs / security issues

Open a GitHub issue for bugs. For anything security-sensitive (e.g. a way to make
the segment decoder panic or read out of bounds on untrusted input), please
report it privately to the maintainer rather than in a public issue.

## License

By contributing, you agree that your contributions will be dual-licensed under
the MIT and Apache-2.0 licenses, as described in `README.md`, without any
additional terms or conditions.
