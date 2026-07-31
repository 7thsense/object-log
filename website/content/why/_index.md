---
title: Why object-log
weight: 1
---

**Question this section answers:** Why does a shared object-storage log engine
exist, and when should I use it instead of a broker or a home-grown WAL?

## The problem

Brokers and ingestion systems need **durable ordered appends** on object
storage. Two failure modes keep showing up:

1. **One PUT per produce** wrecks cost and tail latency on S3-class media.
2. **Product-local write paths** re-prove the same group-commit and index bugs
   in every codebase (broker storage forks, cold-tier WAL copies).

Kafka and Redpanda solve ordered durability with lower commit latency—and a
broker cluster. Many workloads can tolerate higher commit latency in exchange
for cheaper durable media. Those systems still need a correct, audited engine.

## The thesis

Durable log-backed systems should not each re-implement object-storage
durability, group-commit, and offset→location indexing. They should share one
small library that:

- deals only in **opaque bytes** and **partition keys**,
- **amortizes PUTs** via buffered group-commit,
- plugs **offset authority** through a sequencer seam.

Formats, Kafka wire, producer identity, epoch fencing, and projection logic
stay in the consumer—where they belong.

## Who it is for

| Fits | Does not fit |
|------|----------------|
| Rust brokers embedding object durability | Teams needing a full Kafka broker |
| Cold-tier WAL behind a hot local tier | Sub-millisecond local fsync as the library's job |
| Queue engines with opaque command bytes | In-library product schemas or tenancy models |
| Teams that can map acks → Durability outside | Expecting consumer groups / transactions in-core |

## Principles

1. **Layer purity** — No Kafka producer/record/acks vocabulary in public API types.
2. **Opaque payloads** — The engine never inspects application framing.
3. **PUT before commit** — Sequenced acknowledgements require durable object + sequencer success.
4. **Amortize under load** — Linger packs; max bytes is a high safety ceiling.
5. **Mechanism, not policy** — `truncate_before` and reaping are tools; consumers decide when.

## Proof path

{{< cards >}}
  {{< card title="Concepts" link="../concepts/" subtitle="How the three pieces compose" >}}
  {{< card title="Get started" link="../get-started/" subtitle="Try the library" >}}
  {{< card title="Repository" link="https://github.com/7thsense/object-log" subtitle="Tests, ADRs, evidence" >}}
{{< /cards >}}
