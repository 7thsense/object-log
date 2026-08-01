---
ddx:
  id: product-voice
  depends_on:
    - product-vision
    - prd
---

# Product Voice — object-log

Governing voice for the microsite, README hero, and release notes.
Paired with `website/DESIGN.md` (visual system).

## Audience

**Primary:** Rust infrastructure engineers embedding durable storage under
brokers, queue engines, cold-tier WALs, and ingestion services.

They already understand offsets, fsync, and S3 request pricing. Speak as a
peer who has paid the PUT bill—not as a tutorial or a sales deck.

**Out of audience:** app devs wanting a managed bus; teams that need a full
Kafka broker or wire protocol in this crate.

## Positioning (canonical)

> Many writes. Few objects.  
> An embeddable log engine that group-commits opaque batches onto pluggable
> object storage. A sequencer you control assigns the offsets.

Shorter badge line: `v0.3 · Rust · object storage`

## Register

| Do | Don't |
|----|-------|
| Concrete: PUT, flush, seal, commit, offset | Vague: “cloud-native platform” |
| Name seams: BlobStore, LogEngine, Sequencer | Invent marketing product tiers |
| State layer boundaries clearly | Imply Kafka drop-in or EOS in-core |
| Prefer short declarative sentences | Hype, emoji, exclamation |
| Active voice for actions | “It can be used to…” filler |

Tone is **calm, dry, precise**—closer to a well-written ADR than a launch post.

## Vocabulary

**Prefer:** amortize, opaque payload, partition key, group-commit, linger,
durable-on-return, sequencer seam, sealed object, crash-durable index,
path-style, quiescent reaper, media ops

**Avoid or qualify:** Kafka-compatible (only “in the consumer”), drop-in
broker, serverless streaming, zero-ops, manifest CAS (obsolete), exactly-once
as a library claim

## Microsite copy rules

1. **Home headline** carries the thesis (many → few), not the product name alone.
2. **First supporting sentence** names the mechanism (group-commit + opaque + sequencer).
3. **Primary CTA** = Get started; **secondary** = Why / proof.
4. Section openers answer a **reader question** in one line (product-microsite-ia).
5. Trust claims only when evidenced (CI, TD-002, crates.io).

## Example lines (approved)

- “Under load, PUT count tracks flushes—not produces.”
- “Payloads are raw bytes. Framing lives above the library.”
- “Sequenced means durable put **and** sequencer commit.”
- “Kafka types, codecs, and fencing stay in the consumer.”

## Example lines (rejected)

- “The last log library you’ll ever need.”
- “Kafka-compatible out of the box.”
- “Blazing-fast cloud-native durability.”
