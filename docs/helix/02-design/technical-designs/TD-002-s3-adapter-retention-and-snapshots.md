---
ddx:
  id: td-s3-adapter-retention-snapshots
  depends_on:
    - td-core-and-object-backend
    - contract-object-store-api
    - adr-object-storage-log-engine-and-sequencer-seam
    - prd
---

# Technical Design: TD-002 S3 BlobStore and Retention Mechanism

**Status**: accepted (rewritten for object-log 0.2.x / ADR-002)  
**Related**: CONTRACT-002 v2, ADR-002, PRD FR-5/FR-17/FR-26  
**Supersedes**: TD-002 v1 (CAS `ObjectStore`, delegated manifest CAS, snapshot policy types in core)

## Scope

**In scope**

- Feature-gated `S3BlobStore` implementing `BlobStore`
- Multipart upload for large puts
- Retention **mechanism** via `Sequencer::truncate_before` + engine deletes
- What remains consumer-owned (snapshot policy, when to truncate)

**Out of scope**

- Manifest compare-and-set on the object store
- Built-in snapshot registry or projection state
- Provider capability matrix for CAS
- pqueue SQLite projection / Niflheim codecs

## S3-Compatible BlobStore

`S3BlobStore` (`src/s3.rs`, feature `s3`) implements CONTRACT-002 v2:

| Behavior | Requirement |
|----------|-------------|
| Durable put | Service `PutObject` / multipart success ⇒ durable-on-return under S3 semantics |
| Large objects | Multipart above configured size threshold; callers never manage parts |
| `get_range` | HTTP Range GET |
| `list` | Prefix list with internal pagination |
| `delete` | Idempotent preferred |
| CAS / put_if_absent | **Not required** and not exposed |

Configuration is runtime (endpoint, bucket, credentials, path-style). Production validation is “can put/get/get_range,” not “supports conditional writes.”

Optional tests: `tests/s3.rs` (env-gated against a real endpoint when configured).

## Retention and Snapshots

### Mechanism (in object-log)

```text
engine.truncate_before(partition, offset)
  → sequencer.truncate_before drops index entries with end <= offset
  → returns object_ids with zero remaining references across ALL partitions
  → engine deletes those object ids via BlobStore::delete
```

Multiplexed objects are shared; an object is reclaimable only when **no** partition still references it.

### Policy (consumer-owned)

- When truncation is safe (consumer watermark, Kafka DeleteRecords, WAL retire).
- Snapshot markers and projection high-watermarks live in the consumer’s control plane.
- object-log does **not** store `SnapshotRef` / `RetentionPolicy` types in core.

### Orphans

Crash between put and commit can leave unreferenced objects. **Out of scope for 0.2.x.** Consumers or operators may build a reaper with `BlobStore::list` minus the sequencer’s referenced set.

## Cost Guardrails

Amortization is **group-commit + linger** (PRD FR-10/FR-24), not a min-records reject flag. Under continuous produce, objects ≈ flushes. Idle early-flush may create smaller objects for latency (TD-004).

## Integration Notes

| Consumer | S3 role | Retention |
|----------|---------|-----------|
| Fjord | BlobStore for multiplexed produce objects | Coordinator policy → `truncate_before` |
| Niflheim cold tier | BlobStore for coalesced cold objects; may use get_range for chunks | Watermark-driven truncate or consumer-side GC |
| pqueue-class | Optional S3 log storage | Snapshot HW then truncate |

## Testing

- Unit/integration without S3: Memory/Local port suite.
- Optional live S3: `tests/s3.rs` (`s3_blob_store_round_trip`, `s3_multipart_put_get_range_round_trip`).
- Engine tests cover truncate_before delete of dead objects.

### Operator runbook (evidence)

Preferred helper (prints a paste-ready evidence row; never prints secrets):

```bash
# MinIO (defaults: 127.0.0.1:19000, minioadmin/minioadmin)
./scripts/s3-evidence.sh minio

# Garage (path-style; supply a key with R/W on the bucket)
OBJECT_LOG_S3_KEY_ID=… OBJECT_LOG_S3_SECRET=… \
  ./scripts/s3-evidence.sh garage

# AWS / R2 / other S3-compatible (path-style endpoint URL)
OBJECT_LOG_S3_ENDPOINT=… OBJECT_LOG_S3_BUCKET=… \
OBJECT_LOG_S3_KEY_ID=… OBJECT_LOG_S3_SECRET=… \
OBJECT_LOG_S3_REGION=… \
  ./scripts/s3-evidence.sh aws   # or r2 / custom
```

Suite (`tests/s3.rs`, feature `s3`):

1. `s3_blob_store_round_trip` — put/get/get_range/list/delete  
2. `s3_multipart_put_get_range_round_trip` — 6 MiB multipart + range GET  
3. `s3_engine_produce_fetch_round_trip` — LogEngine + ManifestSequencer over S3  

Claim a provider class only when **all three** are green. Default CI remains
hermetic without env; the `s3-minio` job runs continuously against MinIO.

### Evidence log

| Date | Target | Result |
|------|--------|--------|
| 2026-07-31 | MinIO on `127.0.0.1:19000` (bucket `object-log-test`) | adapter put/get/range green (pre-engine suite) |
| 2026-07-31+ | GitHub Actions `s3-minio` job (MinIO docker, path-style) | Continuous; blocks merge on failure |
| 2026-07-31 | **minio** @ `http://127.0.0.1:19000` (bucket `object-log-evidence`, host `sindri`) | all three suite tests green via `./scripts/s3-evidence.sh minio` |
| 2026-07-31 | **garage** v2.2.0 @ `http://127.0.0.1:3900` (bucket `object-log-evidence`, host `sindri`) | all three suite tests green via `./scripts/s3-evidence.sh garage`; CLI produce/consume `--lines` also green |

### Provider matrix (claims)

| Provider class | Path-style | Evidence path | Status |
|----------------|------------|---------------|--------|
| MinIO | yes | CI `s3-minio` + operator script | **Supported (evidenced)** |
| Garage (dxflrs ≥2.2) | yes | Operator script on live Garage | **Supported (evidenced)** |
| AWS S3 | path-style forced in `S3BlobStore::new`; some regions prefer virtual-hosted | Operator: `./scripts/s3-evidence.sh aws` | **Candidate** — no credentials in this environment |
| Cloudflare R2 | S3-compatible account endpoint | Operator: `./scripts/s3-evidence.sh r2` | **Candidate** — no credentials in this environment |

Do **not** claim production support for a class without a green full suite against that class.

## Review Checklist

- [x] S3 is BlobStore, not CAS ObjectStore
- [x] Retention is mechanism-only
- [x] Aligns with ADR-002 and CONTRACT-002 v2
