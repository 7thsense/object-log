---
ddx:
  id: contract-object-store-api
  depends_on:
    - contract-core-log-api
    - adr-object-storage-log-engine-and-sequencer-seam
---

# Contract

**Contract ID**: CONTRACT-002  
**Type**: boundary  
**Version**: v2  
**Status**: accepted  
**Related**: CONTRACT-001 v2, ADR-002, PRD FR-1..FR-6, FR-26  
**Supersedes**: CONTRACT-002 v1 (`ObjectStore` with `put_if_absent` / `compare_and_set` / `capabilities`)

## Purpose

This contract defines the **BlobStore** storage port used by `LogEngine` and `ManifestSequencer`. It is intentionally smaller than a full object-store SDK. Conditional writes (CAS) are **not** required: the engine uses unique object keys per flush.

## Scope and Boundaries

- **In scope**: string-keyed immutable object put/get/get_range/list/delete; durable-on-return semantics for production adapters; media-op stats for budget control; key validation rules.
- **Out of scope**: authentication, bucket creation, lifecycle policies, provider-specific retries beyond adapter configuration, manifest CAS, put-if-absent as a required primitive.
- **Owning system**: object-log storage adapter layer.

## Normative Surface

| Element | Type / Shape | Required | Rules | Notes |
|---------|---------------|----------|-------|-------|
| object key | non-empty `&str` | yes | MUST reject empty, NUL, and `..` path segments | Prevent prefix escape |
| object value | `Bytes` / chunk list | yes | Adapter MUST preserve bytes exactly | Opaque to store |
| `put(key, value)` | async | yes | Crash-durable adapters: durable-on-return on `Ok` | Memory is **not** crash-durable |
| `put_chunks(key, chunks)` | async | yes (default ok) | Default may concat then `put`; Local/S3 SHOULD stream/multipart without full double materialization | Large objects |
| `get(key)` | async | yes | Returns `None` if missing vs `Some(bytes)` | |
| `get_range(key, range)` | async | yes | `None` if missing; `RangeOutOfBounds` if inverted or end &gt; len; empty `n..n` → empty bytes | No integrity check |
| `list(prefix)` | async | yes | Keys under prefix; SHOULD be complete (paginate internally) | Temp keys MAY be omitted |
| `delete(key)` | async | yes | Missing key is success (idempotent preferred) | |
| `take_media_op_stats()` | sync | no | Snapshot+reset durable media ops/bytes; default `None` ⇒ engine counts 1 op per successful put | Budget controller |

### Adapter requirements

| Adapter | Crash-durable put | Notes |
|---------|-------------------|-------|
| `MemoryBlobStore` | no | Tests/dev only |
| `LocalBlobStore` | yes | MUST: write temp → `sync_data`/`fdatasync` → rename → fsync parent dir (macOS may need `F_FULLFSYNC` for true device durability) |
| `S3BlobStore` (feature `s3`) | yes (service semantics) | Path-style capable; multipart above size threshold; no CAS API required |

## Precedence and Compatibility

- **Versioning**: v2 removes CAS/`capabilities` as required surface. Adapters must not claim CAS-based manifest commits as part of this port.
- **Ordering**: engine seals unique keys; overwrite of an existing key is not part of the engine’s normal path and is adapter-defined if it occurs.
- **Backward compatibility**: v1 `ObjectStore` CAS methods are removed. Callers use BlobStore only.
- **Deprecation**: none within v2; additive optional methods allowed if defaulted.

## Error Semantics

| Condition | Error / Outcome | Retry | Recovery Expectation |
|-----------|------------------|-------|----------------------|
| Invalid key | `InvalidObjectKey` | no | Fix key construction |
| Transient I/O / service failure | `StorageUnavailable` | yes | Retry within caller/engine policy |
| Range out of bounds | `RangeOutOfBounds` | no | Fix range |
| Missing object on get/get_range | `Ok(None)` | n/a | Caller decides |
| Missing object expected present (fetch path) | surfaced as `MissingObject` by engine | no | Repair |

## Examples

```text
put("log/obj-01HZ...", <multiplexed batch bytes>)
get_range("log/obj-01HZ...", 128..512) -> Some(<slice>)
list("log/") -> ["log/obj-01HZ...", "log/manifest-..."]
delete("log/obj-old...")
```

## Non-Normative Notes

- Manifest CAS from CONTRACT-002 v1 is obsolete. `ManifestSequencer` writes new manifest object keys (or equivalent) with ordinary durable puts, amortized per group-commit.
- Whole-object checksums are intentionally not part of this port so `get_range` remains valid; consumers that need integrity put checksums in their own framing.
- At very large key counts, a streaming list API may be added later without breaking put/get semantics.

## Validation Checklist

- [x] Normative fields and rules are explicit.
- [x] No CAS requirement remains.
- [x] Error handling is explicit.
- [x] Conformance tests can exercise Memory and Local (and optional S3).
- [x] Non-normative notes cannot be mistaken for contract requirements.
