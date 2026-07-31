---
ddx:
  id: product-voice
  depends_on:
    - product-vision
    - prd
---

# Product Voice — object-log

Governing voice for the public microsite, README hero copy, and release notes.
Derived from product vision + PRD; not marketing fluff.

## Who we speak to

Rust infrastructure engineers embedding durable storage under brokers, queue
engines, WAL cold tiers, and ingestion systems. They already understand
offsets, durability, and object storage cost. They do not need a Kafka tutorial.

## Who we are not speaking to

- Application developers looking for a drop-in message bus
- Operators seeking a managed Kafka replacement
- Readers who want product schemas, tenancy models, or wire protocols in-box

## Positioning sentence (use verbatim or lightly adapted)

> object-log is an embeddable Rust log engine: opaque batches over pluggable
> object storage, group-committed so PUTs track flushes—not produces—with a
> pluggable sequencer for offsets.

## Voice attributes

| Attribute | Do | Don't |
|-----------|----|-------|
| Precise | Name BlobStore, LogEngine, Sequencer, Durability | Vague “cloud-native log platform” |
| Honest | State what lives above the library (Kafka, codecs, fencing) | Claim Kafka compatibility in-core |
| Operator-minded | Cost = PUTs amortized; durability = put + commit | Promise sub-ms local fsync tiers |
| Layered | “Consumers own Meta and framing” | “All-in-one streaming stack” |
| Calm | Short sentences; concrete nouns | Hype, exclamation marks, emojis |

## Words we prefer

amortize, opaque payload, partition key, group-commit, linger, durable-on-return,
sequencer seam, crash-durable index, path-style S3, quiescent reaper

## Words we avoid (or qualify hard)

- “Kafka-compatible” without “in the consumer / above object-log”
- “drop-in broker”, “serverless streaming”, “zero-ops”
- “manifest CAS” (superseded; ManifestSequencer uses ordinary durable puts)
- “exactly-once” as a core library claim (belongs in sequencer Meta)

## Homepage first viewport must answer

1. **What** — embeddable object-storage log engine (Rust library)
2. **Why** — one PUT per produce is unaffordable; forked write paths duplicate bugs
3. **Next** — Get started (cargo add) + secondary proof (concepts / docs.rs)

## Trust claims (only if evidenced)

- MinIO + Garage operator evidence (TD-002)
- CI: unit tests, release perf floor, live MinIO job
- Layer purity: no Kafka types in public API

## Microsite reader modes → sections

| Mode | Question | Section |
|------|----------|---------|
| Evaluate | What is this? | Home, Why |
| Start | How do I try it? | Get Started |
| Decide | Which concept next? | Concepts |
| Operate | Exact behavior? | Reference (API, CLI) |
