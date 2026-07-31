---
ddx:
  id: adr-object-storage-log-engine-and-sequencer-seam
  depends_on:
    - product-vision
    - prd
    - concerns
  links:
    - adr-kafka-compatible-core-object-storage-backend
---

# ADR-002: Object-Storage Log Engine with a Pluggable Sequencer Seam

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-06-17 (rev. after adversarial review) | Accepted | Erik LaBianca | ADR-001 (superseded) | High |

## Context

| Aspect | Description |
|--------|-------------|
| Problem | ADR-001's backend writes **one object per append** (a sealed segment + a manifest compare-and-set per `append()`). On object storage this is fatal: one PUT per produce wrecks cost and tail latency. Separately, ADR-001 made the core **Kafka-shaped** (topics, `acks`, idempotent-producer triple, epoch fencing), so object-log is a Kafka coordinator wearing a trench coat — the wrong layering. |
| Current State | The per-append model (`ObjectLogBackend`, segment codec, manifest CAS, `EpochGuard`, the rich record model) ships in 0.1.x but is used by no production consumer. fjord rejected it and built its own buffered, multiplexing, coordinator-sequenced write path (`fjord-log`), duplicating storage concerns. |
| Requirements | (1) Viable on object storage: amortize many batches into few PUTs. (2) `acks=all` must not return until durably persisted **and** sequenced. (3) Be the engine fjord runs on **without** re-proving fjord's verified Kafka parity. (4) **Clean three-layer separation** (below). |
| Decision Drivers | Object-storage economics; one write path instead of two; a genuinely generic OSS package; verified-parity preservation. |

## The three layers (separation of concerns)

This ADR's north star is a clean cut across three crates/repos, dependencies pointing **down only**:

- **object-log** (this repo, public) — *"object storage as an append log."* Owns: a `BlobStore` port (Memory/Local/S3), a buffered/multiplexing/ack-aware `LogEngine`, and a pluggable `Sequencer` seam. Deals only in **opaque payload bytes**, partition keys, record counts, byte ranges, and offsets. **Knows nothing of Kafka, records, producers, or brokers.**
- **heimq** (public) — Kafka wire protocol + broker request engine. **Owns the Kafka record-batch byte format**, including stamping `base_offset` into a v2 batch. Knows nothing of object storage.
- **fjord** (private) — broker coordination + binding. The coordinator (offset authority, idempotent-producer dedup, EOS/txn fencing, consumer groups) **implements object-log's `Sequencer`**; the binding layer **implements heimq's `LogBackend`** over object-log's engine. Owns all Kafka semantics that aren't wire-format.

Dependency DAG (no cycles, no up-edges):

```
        object-log  ◄─────────────┐
            ▲                      │
heimq-broker│ (no edge either way) │
   (traits) │                      │
            │              fjord-coordinator  (impl object_log::Sequencer)
            │                      ▲
            └── fjord-heimq-backend ┘  (depends on heimq-broker + object-log + fjord-coordinator — the binding)
                       ▲
                    fjord (bin)
```

## Decision

We will **re-found object-log as a generic, buffered, multiplexing, durability-aware object-storage log engine that is generic over a pluggable `Sequencer`.** This supersedes ADR-001's per-append, Kafka-shaped model.

**1. `BlobStore` port — the only storage abstraction.** Async over immutable objects keyed by string. Adapters: `Memory` (test/dev — **not** crash-durable), `Local`, `S3` (feature-gated). Replaces both the CAS `ObjectStore` and fjord's private `BlobStore`/`S3BlobStore`. No conditional writes needed — object keys are unique per flush.

```rust
trait BlobStore: Send + Sync {
    /// PUT `value` at `key`. On `Local`/`S3` this is **durable-on-return**: when
    /// it resolves Ok the bytes survive process/host crash, so a caller may treat
    /// Ok as a durability barrier. (The `Memory` adapter is test/dev only and is
    /// NOT crash-durable — do not use it where the durability contract matters.)
    /// The `Local` adapter MUST persist in EXACTLY this order, or it loses data on
    /// power loss while returning Ok: write a temp file → `sync_data`/`fdatasync`
    /// on the temp → `rename` into place → `fsync(parent dir)`. (macOS may need
    /// `F_FULLFSYNC` for true device durability.) `value` may be ARBITRARILY LARGE:
    /// the `S3` adapter dispatches multipart upload above a size threshold so a
    /// 100 MiB+ object is not one whole-buffer PUT — object-log owns multipart
    /// internally; callers never see it. Local `put_chunks` streams chunk bytes
    /// to the temp file without a full pre-merge copy.
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError>;
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError>;
    /// Read a byte sub-range (S3 Range GET; local pread) — pull one batch/chunk
    /// out of a large multiplexed object without fetching the whole thing.
    /// `None` if the key is absent; `range.end > len` or `start > end` is an
    /// error; an empty `n..n` returns `Ok(Some(empty))`. The port does **no
    /// integrity check**: payloads are opaque, so verifying a slice is the
    /// consumer's concern over its own per-batch/per-chunk framing (heimq's v2
    /// batch CRC; niflheim's per-chunk checksum). object-log never
    /// whole-object-checksums — that would be incompatible with slice reads.
    async fn get_range(&self, key: &str, range: Range<u64>)
        -> Result<Option<Bytes>, ObjectLogError>;
    /// Keys under `prefix`. Paginates internally (S3 continuation tokens) and
    /// returns the full set. (At very large key counts the whole-`Vec` result is
    /// a memory consideration; a streaming/paginated variant can be added if a
    /// consumer needs it — e.g. niflheim's hundreds-of-thousands-of-segments boot
    /// scan.)
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError>;
    async fn delete(&self, key: &str) -> Result<(), ObjectLogError>;
}
```

Three generic contract points carry the dual-consumer (fjord + niflheim) load,
none leaking Kafka or WAL concepts: **durable-on-return `put`** (a consumer marks a
write committed once `put` resolves — niflheim's `cold_durable`, fjord's
`acks=all`→`Sequenced`), **large-object/multipart `put`** (100 MiB+ coalesced
segments and big multiplexed objects don't become a single PUT), and **`get_range`**
(sub-object byte addressing — niflheim loads one chunk from a coalesced segment; the
engine's `fetch` reads only the needed slice). `list` paginates internally.

**2. `LogEngine<S: Sequencer>` — buffered group-commit.** Accepts opaque batch payloads with a partition key, a record count, and `S`'s metadata; **group-commits**: many batches across many partitions are multiplexed into **one** object, PUT durably, then handed to the sequencer in one call. Owns `FlushConfig` (linger is the packing control; `max_bytes` is a high safety ceiling — default 1 GiB), durable-ops budget / early-flush policy (TD-004), and `flush()` barriers. The engine **authors each batch's `BatchLocation { object_id, byte_start, byte_len }`** (its concern); it never inspects `S`'s metadata.

**3. Durability levels — object-log's own vocabulary, not Kafka `acks`.** A produce future resolves at the requested level:
- `Buffered` — in the flush buffer (may be lost on crash).
- `Durable` — the containing object is PUT (survives crash; **no offset yet**).
- `Sequenced` — `commit` returned (durable **and** has a stable offset).
fjord maps Kafka `acks=0/1/-1` onto these.

**4. `Sequencer` seam — SYNC, generic over metadata, owns the index.**

```rust
pub trait Sequencer: Send + Sync {
    /// Per-batch sequencing metadata the engine forwards UNINTERPRETED.
    /// object-log's default impl uses (); fjord uses its Kafka producer fields.
    type Meta: Send + Sync;

    /// The engine has ALREADY PUT the object durably and computed each batch's
    /// location. Assign offsets ATOMICALLY across the whole object (all-or-nothing)
    /// and persist the offset→location index. One outcome per batch, in order.
    fn commit(&self, batches: &[(Self::Meta, BatchLocation)])
        -> Result<Vec<CommitOutcome>, ObjectLogError>;

    /// Resolve an offset to ordered index entries (the sequencer owns the
    /// offset namespace and the offset→location index).
    fn lookup(&self, partition: &PartitionKey, fetch_offset: i64)
        -> Result<Vec<IndexEntry>, ObjectLogError>;

    /// Cheap, index-only offset bounds (no object reads).
    fn high_watermark(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError>;
    fn log_start_offset(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError>;

    /// Retention MECHANISM (not policy): drop index entries below `offset` and
    /// return the object ids that now have NO live references from ANY partition
    /// — multiplexed objects are shared, so an object is reclaimable only when its
    /// last referencing entry across all partitions is truncated. The engine
    /// deletes the returned ids via `BlobStore::delete`. The *policy* (when
    /// truncation is safe) lives in the consumer. Serves Kafka retention /
    /// DeleteRecords (fjord) and watermark-driven WAL retirement (niflheim) alike.
    fn truncate_before(&self, partition: &PartitionKey, offset: i64)
        -> Result<Vec<ObjectId>, ObjectLogError>;
}
```

`BatchLocation`, `CommitOutcome { Assigned { base_offset, record_count } | Duplicate { base_offset } }`, `IndexEntry { location, base_offset, record_count }`, and `PartitionKey` are **generic** and live in object-log. **`Meta` is an associated type — Kafka's `producer_id`/`producer_epoch`/`base_sequence` live only in fjord's `Meta`, never in object-log.**

**5. Sync seam, async engine.** The `Sequencer` is sync because a linearization point is a critical section, not async I/O — and fjord's sequencer does blocking Postgres I/O. The engine owns the flush worker (a dedicated thread / blocking task, per fjord's proven `Flusher`) and resolves async produce futures via waker. No `async_trait`/`spawn_blocking` tax on the lin-point.

**6. Fetch.** `engine.fetch(partition, offset, max_bytes)` calls `sequencer.lookup` → reads the covering objects via `BlobStore::get_range(object_id, byte_start..byte_start+byte_len)` (only the needed slice, not the whole object) → returns the bytes. Pure storage IO; the index is the sequencer's. The engine also exposes `engine.truncate_before(partition, offset)` (calls the sequencer, then deletes the returned dead object ids) and an optional **streaming read** (`fetch_stream`, visitor/`Stream`) so a consumer can replay a wide offset window without materializing it (niflheim's bounded-RAM replay; optional for fjord, whose fetch responses are already size-bounded).

**7. Offset stamping stays in heimq.** object-log stores and returns **opaque** payload bytes plus the assigned `base_offset`. Writing that offset into a Kafka v2 record batch is heimq's job (it owns the format). fjord must stop poking bytes `[0..8]`.

**Key Points**: amortized PUTs | durable-before-ack | generic over `Sequencer::Meta` (zero Kafka in object-log) | sync seam, async engine | engine authors locations, sequencer owns the index.

### Invariants the seam contract MUST state (from adversarial review)

- **In-order, un-split per-partition presentation.** For any single `PartitionKey`, the engine presents batches to `commit` in arrival (send) order, and never splits one partition's batches across two concurrent in-flight `commit` calls. (Else idempotent sequence-gap detection and epoch fencing misfire — a producer's sequence space is per-partition.) The engine enforces this **without reading `Meta`**, using only the engine-visible `PartitionKey`: satisfied by a single flush worker (FIFO buffer, no sorting), or — if concurrent flush workers are ever added — by sharding concurrency on **`PartitionKey`** while preserving per-key arrival order. Never shard on producer identity (it lives inside the opaque `Meta`, which the engine must not read) and never partition-sort the buffer. The engine **moves** each `Meta` out of its FIFO buffer into `commit` (so `Meta: Send + Sync` suffices — no `Clone` bound).
- **Atomic commit.** `commit` is all-or-nothing across every batch in the object; on `Err` the engine acks nothing (no co-multiplexed survivor is acked).
- **Unique object id, fresh on retry, durable-before-index.** Each flush uses a unique object key; a retry uses a *fresh* key so a crashed PUT is never aliased; a `lookup` entry implies its byte range is durably readable.

## Scope boundary & second consumer (niflheim)

object-log is the **durable object-storage log tier**: durability == the object PUT. It deliberately has **no local hot tier and no sub-PUT fsync latency tiers**. A consumer needing sub-millisecond local durability (niflheim's `FireAndForget`/`Tracked` hot-disk path) fronts object-log with its own buffer — that tier is explicitly out of object-log's scope. This is why the durability levels (`Buffered`/`Durable`/`Sequenced`) describe the object PUT + sequence, not a disk fsync.

A full in-object-log **n-tier** (hot + cold + migration) model was formally proposed and **rejected** under adversarial review (see `../ntier-proposal.md`): the hot-tier port could not be made generic without becoming niflheim's WAL renamed (intra-segment byte positions, an fsync-mode taxonomy, framing-aware seal/read), and the migrator reintroduced the very location-mutation crash windows this immutable-append-only design eliminates. niflheim therefore integrates at the **cold tier** (swapping its cold WAL backend to object-log) and keeps its own hot tier — real code-sharing where the use cases genuinely coincide, with the boundary drawn where they diverge.

niflheim's WAL was evaluated as a second consumer to test genericity (object-log's own vision names it). Result: **its cold tier maps ~1:1 onto this design** — immutable, checksummed, offset-indexed segments coalesced onto an object store — which validates that the design is genuinely generic and not fjord-shaped. Three of our decisions are confirmed by the exercise: payloads are **opaque** (niflheim serializes its tenant/offset/event-id structure into the payload; object-log never learns those fields), epoch fencing lives **behind the Sequencer** (not in core), and record framing is **not** a core concern. The dual-consumer requirement is exactly what motivated the generic primitives above — `truncate_before` (Kafka retention *and* WAL retirement), index-only offset bounds, and the optional streaming read — none of which leak Kafka or WAL concepts. Two niflheim concerns are correctly **excluded**: its hot/fsync tier (above) and its record codec (a niflheim-side concern over opaque payloads).

niflheim's **chunk tracking** (`chunk_seq`, offset bounds, `cold_durable`/`backing_store_flushed` state) and **durable-commit** (epoch-leased "publish only after durable") live in its control plane + segment index — *above* the storage port — so they are unaffected by swapping the cold backend to object-log; niflheim keeps them. They depend on object-log only via the generic `BlobStore` guarantees folded into the port above: **durable-on-return `put`** (mark `cold_durable` once the PUT resolves), **large-object/multipart `put`** (100 MiB+ coalesced segments are not a single whole-buffer PUT), and **`get_range`** (load one chunk from a coalesced cold object by byte range), with `list` paginating internally for the boot-recovery scan. Everything else in niflheim's richer cold port is **adapter-local** over those primitives — `FileRef` = a key plus the written length; the SHA-256 sidecar = a second object; `state`/`sync_to_cold`/`stage_to_hot`/`prefetch_hint`/`refresh` = trivial cold-only bookkeeping (verified: no HEAD, no CopyObject in niflheim's cold path). So the thin-adapter claim holds: object-log is the cold blob backend, niflheim owns everything above it.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Keep ADR-001 per-append model | Simple | 1 PUT/produce — unaffordable; Kafka-shaped; unused | Rejected: violates its own cost model |
| Buffered engine, **concrete** `BatchMeta` with Kafka fields in object-log (the pre-review ADR-002) | Matches fjord's types 1:1 | Kafka producer/idempotency vocab leaks into the generic OSS lib; forces Kafka types to move into object-log | Rejected: layer leak (Reviewer A/B) |
| Opaque `dedup_token: Bytes` in the seam | object-log stays generic | Sequencer must decode it to compare epoch/sequence ordering → serialization tax + lost type safety; risks EOS | Rejected: Reviewer C — opacity can't be ordered |
| **Engine generic over `Sequencer::Meta` (associated type)** | object-log names zero Kafka types; fjord's `Meta` keeps typed fields for EOS; no cycle; no type move | Engine is generic (`LogEngine<S>`); one extra type parameter at fjord's bind site | **Selected: satisfies purity (A) AND correctness (C) AND acyclicity (B)** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | PUT count decoupled from produce/partition count; `acks=all` durably correct; one write path; object-log is genuinely generic (opaque bytes, no Kafka) and standalone-usable; S3 adapter shared. |
| Negative | Breaking → object-log 0.2.0. Removed from object-log: `ObjectLogBackend`, segment codec, `EpochGuard`, the record model (`AppendRecord`/`RecordHeader`/`TimestampPolicy`), `AckMode`, CAS `ObjectStore`. Deleted from fjord: `fjord-object-log` (layer-conflating experiment), `fjord-log`'s `BlobStore`/`WritePath`/`ReadPath`/`MemoryBlobStore`/`S3BlobStore`. fjord-coordinator gains a dep on object-log and `impl Sequencer`. heimq gains an offset-stamping seam; fjord's byte-poking is removed. |
| Neutral | object-log stores opaque multiplexed batch bytes (no framed segment format). Orphan-object reclamation (objects PUT then crashed before `commit`) is **out of scope for 0.2.0**; `BlobStore::list` + the sequencer's referenced-object set make an external reaper buildable. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Rebuilt write path regresses fjord's proven parity (EOS, idempotency, ordering) | M | H | Keep ALL sequencing logic in fjord's coordinator behind the seam; write the in-order/atomic/unique-id invariants into the contract; re-run fjord's full conformance/EOS/group/multi-broker suite as the merge gate. |
| Engine "improvements" (concurrent flush workers, partition-sharded buffers) silently reorder a producer's batches | M | H | Contract invariant above; add a non-Docker engine ordering test (many producers, in-flight=1, assert per-producer contiguity). No current non-Docker test catches this. |
| Default in-memory sequencer's index is not crash-durable → "standalone durable log" is half-true (bytes survive, index doesn't) | M | M | Ship two default sequencers: a trivial in-memory one (tests) AND a `BlobStore`-persisted-index sequencer (writes a manifest object on `commit`) so standalone object-log is crash-durable end-to-end. Amortization: **one manifest PUT per group-commit**, co-amortized with the data object (PUTs stay ∝ flushes, not ∝ produces) — or fold the index into a trailing footer of the data object for exactly 1 PUT/flush. Never a manifest PUT per produce. |
| Crash between PUT and `commit` leaks orphan objects | M | L | Documented out-of-scope for 0.2.0; `BlobStore::list` enables a reaper; fresh-key-on-retry prevents aliasing. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| object-log source contains zero Kafka identifiers (producer_id/epoch/sequence/acks/topic) | Any reappearance |
| PUT count independent of partition count (N flushes → N objects) | Regression |
| `acks=all`→`Sequenced` returns only after durable PUT + commit | Any early-ack path |
| New `Sequencer`-conformance + fault-between-PUT-and-commit + engine-ordering tests pass | — |
| fjord's hermetic parity suite stays green on the rebuilt path | Any conformance/EOS/group/multi-broker failure |

## Supersession

- **Supersedes**: ADR-001 — its one-object-per-append backend, manifest-CAS commit boundary, **and its Kafka-shaped core vocabulary** (topics/`acks`/producer triple now live in fjord/heimq, not object-log).
- **Superseded by**: None.

## Concern Impact

- **Concern selection**: object storage remains the durable tier; adds a pluggable sequencer as the linearization point and an opaque-payload contract.
- **Practice override**: None.

## References

- ADR-001 (this repo) — superseded.
- fjord TD-005 / TD-006 — the multiplexed write/fetch path generalized here.
- fjord ADR-008 — central-coordinator sequencing (fjord's `Sequencer` impl).
- Adversarial review 2026-06-17 — four lenses (layer purity, dependency graph, correctness preservation, seam shape); findings folded into §"Decision" and §"Invariants".

## Review Checklist

- [x] Context names a specific problem (1 PUT/produce; Kafka-shaped core).
- [x] Decision is actionable and resolves the purity-vs-correctness contradiction (generic `Meta`).
- [x] Alternatives include the rejected concrete-`BatchMeta` and opaque-token options with reasons.
- [x] Consequences list exactly what is removed/deleted/added across all three layers.
- [x] Risks carry the adversarial-review ordering/durability findings with mitigations.
- [x] Validation includes a "zero Kafka identifiers in object-log" gate.
- [x] Supersession recorded against ADR-001.
