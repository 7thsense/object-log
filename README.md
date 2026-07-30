# object-log

A buffered, multiplexing **append log over pluggable object storage**.

`object-log` stores an ordered, offset-addressed log as immutable objects in any
`BlobStore` (memory, local filesystem, or — behind the `s3` feature — S3). It
deals only in **opaque payload bytes**: it knows nothing about record formats,
Kafka, or brokers. Many produce calls group-commit into one object, so PUT count
is decoupled from produce count. It is the storage engine extracted from the
[fjord](https://github.com/easel/fjord) Kafka-compatible broker.

## Highlights

- **`BlobStore` port** — a minimal async trait (`put` / `get` / `get_range` /
  `list` / `delete`) with durable-on-return writes. Ships with `MemoryBlobStore`,
  `LocalBlobStore`, and `S3BlobStore` (feature `s3`; multipart + range reads).
- **`LogEngine`** — buffers and group-commits many batches into one object, PUTs
  it durably, then sequences it; `produce` resolves at a chosen `Durability`
  (`Buffered` / `Durable` / `Sequenced`), or clients can pipeline with
  `Buffered` and wait on `flush()`. A default-on **durable-ops budget** adapts
  effective linger (early flush when media is idle; co-buffer under load within
  `FlushConfig.linger`) so Fjord/S3 and Local share one latency↔throughput
  controller (`pipeline_snapshot()`).
- **`Sequencer` seam** — a sync trait that assigns offsets and owns the index;
  ships `InMemorySequencer` and a crash-durable `ManifestSequencer`. Plug your own
  (e.g. a Kafka coordinator); the engine forwards its `Meta` uninterpreted.

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

## Status

`0.2.x` — pre-1.0; the API may evolve. Requires Rust 1.88+ (edition 2024).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state otherwise,
any contribution intentionally submitted for inclusion in this crate by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
