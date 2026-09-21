# Reference: The LogDrive (OSDI 2026)

Captured 2026-09-21. This is a reading note for object-log. It is not a decision and it does not change [ADR-002](../adr/ADR-002-object-storage-log-engine-and-sequencer-seam.md).

## Citation

Gardner Vickers, Lucas Bradstreet, Mahesh Balakrishnan, Prince Mahajan, David Mao, Xavier Léauté, Ismael Juma, Nikhil Bhatia, Jack Vanlightly, Prateek Jindal, Sumit Arrawatia, Andrew Grant, Dhruvil Shah, Dimitar Dimitrov, Gaurav Badoni, Shimiao Zhang, and Yang Yu. **The LogDrive: Composable Durability for Cloud-Based Shared Logs.** 20th USENIX Symposium on Operating Systems Design and Implementation (OSDI 26), 2026, pages 1663–1682.

- PDF: https://www.usenix.org/system/files/osdi26-vickers.pdf
- Talk page: https://www.usenix.org/conference/osdi26/presentation/vickers
- Walkthrough: Jack Vanlightly, [The LogDrive: Flexible Composition Through Abstraction in Shared Logs](https://jack-vanlightly.com/blog/2026/8/25/the-logdrive-flexible-composition-through-abstraction-in-shared-logs), 25 August 2026.

Not on arXiv. The production system in the paper is Conflux, the metadata and sequencer service inside Confluent's K2 (Kafka-on-S3 / Freight clusters).

## Name collision

On 21 September 2026 James Ross described a Cloudflare product also called K2: a named append-only record log with retention, written from a Worker binding or over HTTP. Jack Vanlightly replied that the LogDrive paper is about Confluent's K2, and that Cloudflare's K2 is a different system. Cloudflare R2 is only relevant here as another S3-compatible store a primitive LogDrive could sit on. The paper does not evaluate R2.

## What the paper splits

A shared-log `append` both assigns the next address and stores the bytes. That composes for striping and breaks for quorum replication, because each child assigns its own address.

They decompose the Delos loglet:

- **AtomicLog** sequences. A soft-state sequencer hands out addresses. Up to K appends may be in flight. Completions return in address order, so the log the caller sees has no holes.
- **LogDrive** only stores. `write(address, bytes)`, `read(address)`, and `weakTail`. `weakTail` returns the non-contiguous tail and the holes inside the window, so a lost sequencer can be rebuilt by scanning at most K slots.

Composition is below sequencing. A primitive LogDrive is a thin shim over S3, S3 Express, or DynamoDB. Striped and quorum LogDrives are built from those primitives, including a quorum across regions. Their representative workload reports about 10× lower metadata cost than using DynamoDB directly, and about 3× lower overall cost, at the same latency target.

## Where this sits relative to object-log

This note belongs here, not in fjord. Fjord implements `Sequencer` and the Kafka binding. The paper's contribution is the durability substrate under sequencing, which is this repository's job.

ADR-002 already separates three things: a `BlobStore` that is durable on return, a `LogEngine` that buffers and multiplexes, and a pluggable `Sequencer` that assigns offsets. That is the same family as AtomicLog over a primitive store. It is not the same cut.

| | LogDrive paper | object-log today |
| --- | --- | --- |
| Who assigns the address | Soft-state sequencer, then `write(address)` | The `Sequencer` on `commit`, after the flush has durable bytes |
| Store API | Numbered single-value registers plus `weakTail` | `BlobStore::put/get/get_range` of opaque objects, plus a manifest |
| In-flight holes | Allowed inside a window of size K, recovered by `weakTail` | A failed PUT is a barrier. The published tail does not contain holes a later writer fills at a preassigned address |
| Composition | Stripe and quorum LogDrives under one sequencer | One adapter per engine: Memory, Local, or S3. Not a RAID of stores |
| Multi-writer | Shared log, sequencer can be replaced | One publication authority per log. Conditional PUT where the adapter needs it |

The piece to reuse, if object-log ever stripes across buckets or quorum-copies across regions, is their placement of composition. It has to sit below offset assignment. A second engine or a second sequencer that each invents addresses does not compose. The primitive they isolate is `write(address)` plus a bounded tail scan. That would be a new store port beside `BlobStore`, not a change to fjord and not a replacement for the manifest-sealed engine in ADR-002.
