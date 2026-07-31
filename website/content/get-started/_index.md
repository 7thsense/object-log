---
title: Get started
weight: 2
---

**Question this section answers:** How do I go from zero to a working
produce/fetch loop?

## Install

Requires **Rust 1.88+** (edition 2024).

```toml
[dependencies]
object-log = "0.3"
```

Optional features:

| Feature | What you get |
|---------|----------------|
| `s3` | `S3BlobStore` (AWS S3, MinIO, Garage, …) |
| `cli` | `object-log` diagnostics / produce-consume binary |

```bash
cargo add object-log
# or
cargo add object-log --features s3
cargo install object-log --features cli
```

## Minimal produce / fetch

```rust
use object_log::{
    Durability, FlushConfig, InMemorySequencer, LogEngine, MemoryBlobStore, PartitionKey,
};
use bytes::Bytes;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), object_log::ObjectLogError> {
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

    let batches = engine.fetch(&p, 0, 1 << 20).await?;
    assert_eq!(batches[0].payload, "hello");
    Ok(())
}
```

Notes:

- `MemoryBlobStore` is **not** crash-durable—tests and demos only.
- For durability on disk use `LocalBlobStore`; for S3 enable feature `s3`.
- For a crash-durable standalone index use `ManifestSequencer` instead of
  `InMemorySequencer`.

## Durability levels

| Level | Resolves when | Offsets? |
|-------|---------------|----------|
| `Buffered` | Batch is in the flush buffer | No |
| `Durable` | Containing object PUT succeeded | No |
| `Sequenced` | PUT **and** `Sequencer::commit` succeeded | Yes |

After `Buffered` produces, call `engine.flush()` to barrier work already enqueued.

## CLI black-box (optional)

```bash
cargo install --path . --features cli   # from a checkout
# or: cargo install object-log --features cli

printf 'a\nb\nc\n' | object-log produce --root /tmp/olog --partition demo --lines
object-log consume --root /tmp/olog --partition demo --lines
```

## Next

{{< cards >}}
  {{< card title="Concepts" link="../concepts/" subtitle="BlobStore, engine, sequencer" >}}
  {{< card title="Reference" link="../reference/" subtitle="API + CLI surface" >}}
  {{< card title="docs.rs" link="https://docs.rs/object-log" subtitle="Full rustdoc" >}}
{{< /cards >}}
