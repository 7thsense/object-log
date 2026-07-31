---
title: object-log
layout: hextra-home
---

{{< hextra/hero-badge >}}
  <div class="hx:w-2 hx:h-2 hx:rounded-full hx:bg-primary-400"></div>
  <span>v0.3 · Rust embeddable log engine</span>
{{< /hextra/hero-badge >}}

<div class="hx:mt-6 hx:mb-6">
{{< hextra/hero-headline >}}
  Object storage as an append log.
{{< /hextra/hero-headline >}}
</div>

<div class="hx:mb-6">
{{< hextra/hero-subtitle >}}
  Multiplex many produces into few durable object PUTs. Opaque payloads.
  Pluggable sequencing. No Kafka types in the library—consumers own framing
  and coordination.
{{< /hextra/hero-subtitle >}}
</div>

<div class="hx:mb-12">
{{< hextra/hero-button text="Get started" link="get-started" >}}
{{< hextra/hero-button text="Why object-log" link="why" style="outline" >}}
</div>

<div class="hx:mt-16"></div>

{{% hextra/feature-grid %}}
  {{% hextra/feature-card
    title="Group-commit economics"
    subtitle="PUT count tracks flushes, not produces or partitions. Linger packs under load so S3-class storage stays affordable."
  %}}
  {{% hextra/feature-card
    title="Opaque by design"
    subtitle="Payloads are raw bytes. Partition keys are strings. No product schemas, no Kafka record model in core."
  %}}
  {{% hextra/feature-card
    title="Sequencer seam"
    subtitle="Offsets and the index live behind a sync Sequencer. Ship InMemory or Manifest—or plug a coordinator."
  %}}
  {{% hextra/feature-card
    title="Durability you can map"
    subtitle="Buffered, Durable, Sequenced. Consumers map external acks (e.g. Kafka acks) outside the library."
  %}}
  {{% hextra/feature-card
    title="BlobStore port"
    subtitle="Memory for tests, Local durable-on-return, S3 (feature-gated). MinIO and Garage operator-evidenced."
  %}}
  {{% hextra/feature-card
    title="Black-box CLI"
    subtitle="Optional produce/consume tool for files and stdin—handy for operator visibility and smoke tests."
  %}}
{{% /hextra/feature-grid %}}

## How it works

```text
produce(partition, opaque bytes, durability)
        │
        ▼
   LogEngine buffer  ──linger / size / flush──►  seal multiplexed object
        │                                              │
        │                                              ▼
        │                                       BlobStore::put  (durable)
        │                                              │
        │                                              ▼
        └──────────────────────────────►  Sequencer::commit  (offsets)
                                                   │
                                                   ▼
                                        fetch by offset → get_range
```

Under continuous load, many produces share one object. Crash between PUT and
commit can leave orphans; reaping is explicit and quiescent-only.

## Trust

| Signal | Where |
|--------|--------|
| Crates.io | [object-log 0.3](https://crates.io/crates/object-log) |
| API docs | [docs.rs/object-log](https://docs.rs/object-log) |
| S3 evidence | MinIO (CI) + Garage (operator) — see project TD-002 |
| Source | [github.com/7thsense/object-log](https://github.com/7thsense/object-log) |

## Next

{{< cards >}}
  {{< card title="Get started" link="get-started/" subtitle="cargo add → produce/fetch in minutes" >}}
  {{< card title="Concepts" link="concepts/" subtitle="BlobStore, engine, sequencer, durability" >}}
  {{< card title="Reference" link="reference/" subtitle="API surface and CLI commands" >}}
{{< /cards >}}
