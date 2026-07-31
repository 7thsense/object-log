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

### Optional: diagnostics CLI

```sh
cargo run --features cli --bin object-log -- --help
cargo test --features cli --test cli_smoke
```

### Optional: live S3 / MinIO evidence

Hermetic tests never require S3. To exercise `S3BlobStore` (including multipart):

```sh
# Example against a local MinIO (path-style) with a pre-created bucket:
export OBJECT_LOG_S3_ENDPOINT=http://127.0.0.1:19000
export OBJECT_LOG_S3_BUCKET=object-log-test
export OBJECT_LOG_S3_KEY_ID=minioadmin
export OBJECT_LOG_S3_SECRET=minioadmin
export OBJECT_LOG_S3_REGION=us-east-1
cargo test --features s3 --test s3 -- --nocapture
```

Legacy `FJORD_GARAGE_*` env names are accepted as aliases. Without these vars the
S3 tests skip and pass.

### Optional: honest local throughput

```sh
cargo test --release --test perf_throughput honest -- --nocapture
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
