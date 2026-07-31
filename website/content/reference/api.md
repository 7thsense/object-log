---
title: Library API
weight: 1
---

Authoritative detail: [docs.rs/object-log](https://docs.rs/object-log).

## Primary types

| Type | Module role |
|------|-------------|
| `BlobStore` | Storage port |
| `MemoryBlobStore` / `LocalBlobStore` / `S3BlobStore` | Adapters |
| `LogEngine<S>` | Group-commit engine |
| `Durability` | Buffered / Durable / Sequenced |
| `FlushConfig` / `BudgetConfig` | Packing + media-ops budget |
| `Sequencer` | Offset seam |
| `InMemorySequencer` / `ManifestSequencer` | Default sequencers |
| `PartitionKey` | Opaque log identity |
| `FetchedBatch` / `AppendOutcome` | Results |
| `reap_orphans` | Quiescent orphan delete helper |

## Cargo features

| Feature | Default | Purpose |
|---------|---------|---------|
| _(none)_ | yes | Core engine + Memory/Local |
| `s3` | no | `S3BlobStore` |
| `cli` | no | `object-log` binary |

## MSRV

Rust **1.88** (edition 2024), as declared in `Cargo.toml`.
