---
ddx:
  id: concerns
---

# Project Concerns

Project Concerns declare active cross-cutting context for downstream work. They are not principles, requirements, ADRs, test plans, or implementation tasks.

## Active Concerns

| Concern | Source | Areas | Why Active | Key Practices |
|---------|--------|-------|------------|---------------|
| rust-library | project-local | `area:api`, `area:data` | object-log is a Rust embeddable library first | Keep core types small; no product-specific dependencies; expose testable traits; deny accidental panics in library paths; `#![deny(missing_docs)]` on public API |
| durability | project-local | `area:data` | The library owns durable-on-return puts and durability-level produce resolution | `Sequenced` only after PUT + commit; `Durable` only after PUT; document Memory as non-crash-durable; Local durable protocol is normative |
| object-storage | project-local | `area:data`, `area:infra` | Object storage is the production durability substrate | Group-commit amortizes PUTs; linger packs under load; unique object keys (no store CAS); S3 via feature-gated BlobStore |
| sequencing-seam | project-local | `area:api`, `area:data` | Offsets and index authority are pluggable | Sync `Sequencer`; generic uninterpreted `Meta`; atomic commit; in-order per-partition presentation; engine authors locations |
| layer-purity | project-local | `area:api` | Kafka/WAL product types must not leak into the storage engine | No Kafka record/acks/producer triple in public API; consumers map external semantics onto Durability + Meta |
| tenancy-and-isolation | project-local | `area:api`, `area:data` | Consumers need prefix isolation without a baked-in tenant model | Caller-supplied key prefixes; reject path traversal; no authorization inside the core library |
| verification | project-local | `area:api`, `area:data`, `area:infra` | Correctness-critical shared infrastructure | Every P0 FR has a named test; BlobStore + engine + sequencer failure modes covered; no phantom claims about Kafka/CAS/wire support |

## Framing Exception: User Stories

object-log is an **embeddable library** (no operator UI, no multi-tenant account product). HELIX user-story + `@covers US-n-ACm` floor is **waived** for this repo.

**Substitute gate** (recorded as practice):

- Every P0 `FR-n` maps to ≥1 named test in `docs/helix/03-test/test-plan.md`.
- Tests exercise the FR (not citation-only).
- Phantom claims (docs asserting tests/features that do not exist) remain a blocking verification failure.

If a future operator-facing surface is added, re-open user stories under `frame`.

## Project Overrides

| Concern | Practice | Override | Authority |
|---------|----------|----------|-----------|
| object-storage | “reject one-record-per-object config” | Amortization is via **group-commit + linger**, not a min-records reject flag. Under continuous load, PUTs track flushes. Idle early-flush may seal a small object for latency (intended). | ADR-002, TD-004, PRD FR-10/FR-24 |
| layer-purity | “Kafka-compatible core log API” | Kafka producer/log compatibility is a **consumer** concern (map acks→Durability; Meta carries producer identity). object-log stays generic. | ADR-002, PRD FR-29/FR-30 |
| durability | “manifest CAS as ack boundary” | Durable boundary is **BlobStore put** (and Sequenced = put + commit). ManifestSequencer persists index with ordinary puts of unique keys. | ADR-002 |

## Area Labels

- `area:api` — Rust API, Sequencer seam, Durability, engine surface
- `area:data` — payloads, objects, index, replay, checksums (consumer-owned framing)
- `area:infra` — S3-compatible backends, local filesystem backend, CI and benchmarks
- `area:cli` — future diagnostics and repair tooling

## Concern Conflicts

| Conflict | Resolution |
|----------|------------|
| Low-latency single produce vs amortized PUTs | Linger + optional early-flush when idle (TD-004); consumers choose Durability |
| Generic library vs broker/WAL needs | Engine owns storage/group-commit; Sequencer + Meta + codecs stay in consumers |
| Crash-durable standalone vs simple tests | Ship both InMemorySequencer and ManifestSequencer; document which is durable |
| Completeness vs orphan reaper scope | Orphans deferred; list enables external reaper |
