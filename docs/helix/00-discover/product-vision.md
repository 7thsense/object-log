---
ddx:
  id: product-vision
---

# Product Vision

## Mission Statement

object-log gives infrastructure teams an embeddable, buffered append log over pluggable object storage—opaque payloads, group-committed durable objects, and a pluggable sequencing seam—so systems can share one storage engine without coupling to a broker or product-specific WAL.

## Positioning

For teams building high-throughput services that need durable ordered logs and want to trade small-write latency for object-storage cost,
object-log is a Rust library that multiplexes many produces into few object PUTs, assigns offsets through a pluggable `Sequencer`, and never inspects payload formats.
Unlike product-specific S3 WAL code or a Kafka-shaped storage library, object-log stays generic: Kafka record framing, producer identity, and coordination live in the consumer (e.g. fjord/heimq); cold-tier WAL semantics live in the consumer (e.g. Niflheim).

## Vision

Durable log-backed systems should not each re-prove object-storage durability, group-commit, and offset→location indexing. object-log succeeds when brokers, queue engines, and ingestion systems share one audited storage engine—bytes in objects, offsets from a sequencer—while owning their own formats and control planes.

**North Star**: object-log is the default embeddable object-storage log engine for systems that need amortized durable appends and can plug their own sequencing (in-memory, manifest-persisted, or coordinator-backed).

## User Experience

A service author constructs a `LogEngine` over a `BlobStore` (memory, local disk, or S3) and a `Sequencer` (in-memory, manifest-backed, or their own). They `produce` opaque batch bytes under a partition key at a chosen durability level (`Buffered`, `Durable`, or `Sequenced`), receive offsets only when sequenced, and `fetch` by offset. Under load, many produces group-commit into one object so PUT count tracks flushes, not produces.

## Target Market

| Attribute | Description |
|-----------|-------------|
| Who | Rust infrastructure teams building brokers, queue engines, ingestion systems, or embedded storage services |
| Pain | They need durable ordered replay on object storage without one PUT per produce or a second copy of fjord-style write-path code |
| Current Solution | Kafka/Redpanda for low latency; product-local S3 WAL; one-object-per-batch uploads; forked broker storage paths |
| Why They Switch | Shared engine + sequencer seam lets them amortize object writes, keep formats and fencing above the library, and reuse Memory/Local/S3 adapters |

## Key Value Propositions

| Value Proposition | Customer Benefit |
|-------------------|------------------|
| Group-committed object log | PUT count decoupled from produce count; viable economics on S3-class storage |
| Opaque payloads + partition keys | No product schemas or Kafka types in the library; consumers own framing |
| Pluggable `Sequencer` | In-process, crash-durable manifest, or coordinator-backed offset authority without forking the engine |
| Durability levels | Map consumer acks (e.g. Kafka `acks=0/1/-1`) onto `Buffered` / `Durable` / `Sequenced` outside object-log |
| Shared BlobStore port | Memory, Local (durable-on-return), and S3 adapters for test, dev, and production |

## Success Definition

| Metric | Target |
|--------|--------|
| Primary KPI | Fjord (and similar consumers) can run produce/fetch on object-log without reimplementing BlobStore, group-commit, or multiplexed fetch |
| Contract coverage | 100% of P0 FRs have named executable tests |
| Batch efficiency | Under continuous produce with default linger, PUT count tracks flushes (not produce or partition count); measured in conformance/perf tests |
| Replay reliability | PUT-before-commit failure, atomic multiplex commit, and ManifestSequencer restart are covered by deterministic tests |
| Layer purity | object-log source exports no Kafka producer/record/acks vocabulary |

## Why Now

Fjord rejected the prior one-object-per-append, Kafka-shaped object-log (ADR-001) and duplicated a buffered write path. Extracting the generic engine (ADR-002) before more consumers fork storage code gives one open-source durability substrate for broker and cold-tier WAL use cases.

## Review Checklist

- [x] Mission statement is specific — names the user, the problem, and the approach
- [x] Positioning statement differentiates from the current alternative
- [x] Vision describes a desired end state, not a feature list
- [x] North star is a single measurable sentence
- [x] User experience section describes a concrete scenario, not abstract benefits
- [x] Target market identifies specific pain points and switching triggers
- [x] Value propositions map to customer benefits, not internal capabilities
- [x] Success metrics are measurable
- [x] Why Now section names a specific change
- [x] Business case details, competitor matrices, requirements, and technical choices are left to their own artifacts
- [x] No implementation details beyond product-level vocabulary (engine, store, sequencer)
