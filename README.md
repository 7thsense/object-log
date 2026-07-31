# object-log

A buffered, multiplexing **append log over pluggable object storage**.

`object-log` stores an ordered, offset-addressed log as immutable objects in any
`BlobStore` (memory, local filesystem, or — behind the `s3` feature — S3). It
deals only in **opaque payload bytes**: it knows nothing about record formats,
Kafka, or brokers. Many produce calls group-commit into one object, so PUT count
is decoupled from produce count. It is the storage engine extracted from the
[fjord](https://github.com/easel/fjord) Kafka-compatible broker.

## Highlights

- **`BlobStore` port** — minimal async trait with durable-on-return writes.
  `LocalBlobStore`: temp → `sync_data` → rename → dir `fsync`; streaming
  `put_chunks`. Optional `S3BlobStore` (`s3` feature).
- **`LogEngine`** — group-commit: many `produce` calls → one object. Ack via
  `Durability::{Buffered,Durable,Sequenced}` or pipeline + **`flush()`**.
  **Linger** packs under load (default 50 ms); **`max_bytes` default 1 GiB** is
  only a safety ceiling. Default-on durable-ops budget; early-flush only when
  idle (`pipeline_snapshot()`). **`fetch_stream`** for bounded-RAM replay;
  **`reap_orphans`** for crash-between-PUT-and-commit cleanup (quiescent only).
- **`Sequencer` seam** — offsets + index; `InMemorySequencer` and
  `ManifestSequencer` (both expose `live_object_ids()` for reaping). Engine
  forwards `Meta` uninterpreted.

## Performance check

```bash
OBJECT_LOG_PERF_BYTES=$((256*1024*1024)) \
  cargo test --release --test perf_throughput honest -- --nocapture
```

Prints dd / B0 / B1 / B2 (zeros, fair timers). See TD-004.

## Usage

```toml
[dependencies]
object-log = "0.2"
```

```rust
use object_log::{
    Durability, FlushConfig, InMemorySequencer, LogEngine, MemoryBlobStore, PartitionKey,
};
use bytes::Bytes;
use std::sync::Arc;

let engine = LogEngine::new(
    Arc::new(MemoryBlobStore::new()),
    Arc::new(InMemorySequencer::new()),
    FlushConfig::default(),
    "log/",
);
let p = PartitionKey("events-0".into());

let out = engine
    .produce(p.clone(), Bytes::from_static(b"hello"), 1, (), Durability::Sequenced)
    .await?;
assert_eq!(out.base_offset, Some(0));

let read = engine.fetch(&p, 0, 1 << 20).await?;
assert_eq!(read[0].payload, "hello");
```

(A runnable version is the crate-level doctest — see [docs.rs](https://docs.rs/object-log).)

To target S3/Garage/MinIO, enable the `s3` feature and use `S3BlobStore`, or
implement the `BlobStore` trait for your client.

## Diagnostics CLI

Optional binary (feature `cli`):

```bash
cargo install --path . --features cli
# or: cargo run --features cli --bin object-log -- --help

object-log list --root /var/lib/mylog --prefix log/
object-log inspect --root /var/lib/mylog --manifest-prefix _manifest/ --summary
object-log orphans --root /var/lib/mylog --data-prefix log/ --manifest-prefix _manifest/
object-log fetch --root /var/lib/mylog --partition events-0 --text
```

S3 stores: build with `--features cli,s3` and pass `--s3-endpoint` / `--s3-bucket` /
`OBJECT_LOG_S3_KEY_ID` / `OBJECT_LOG_S3_SECRET`.

## Consumer integration

object-log is a **storage engine**, not a Kafka broker or WAL codec.

| Consumer role | What you implement | What object-log provides |
|---------------|--------------------|---------------------------|
| Broker (e.g. fjord) | `Sequencer` with your producer/`Meta` fields; map external acks → `Durability`; record framing in a protocol crate | `LogEngine` group-commit + `BlobStore` |
| Cold-tier WAL (e.g. Niflheim) | Your hot tier + codecs/checksums; optional `Sequencer` or raw `BlobStore` | Durable put, `get_range`, list |
| Queue projection (pqueue-class) | Opaque command bytes; ownership/fencing in your control plane or `Meta` | Produce/fetch by offset |

See `docs/helix/02-design/technical-designs/TD-003-conformance-kafka-backend-and-extraction.md` for conformance cases and binding sketches. Sequencer implementors can mirror `tests/sequencer_conformance.rs`.

## Status

`0.2.x` — pre-1.0; the API may evolve. Requires Rust 1.88+ (edition 2024).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state otherwise,
any contribution intentionally submitted for inclusion in this crate by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
