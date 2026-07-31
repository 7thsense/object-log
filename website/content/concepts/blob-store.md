---
title: BlobStore
weight: 1
---

Minimal async port over string-keyed immutable objects.

## Operations

| Method | Role |
|--------|------|
| `put` / `put_chunks` | Durable-on-return for Local/S3; Memory is not crash-durable |
| `get` | Whole object |
| `get_range` | Byte slice without full download |
| `list` | Prefix listing (adapters paginate internally) |
| `delete` | Idempotent preferred |
| `take_media_op_stats` | Optional durable-ops accounting for the budget controller |

## Adapters

| Adapter | Crash-durable put | Notes |
|---------|-------------------|-------|
| `MemoryBlobStore` | No | Tests / demos |
| `LocalBlobStore` | Yes | temp → `sync_data` → rename → dir fsync |
| `S3BlobStore` (`s3`) | Service semantics | Multipart above threshold; path-style |

**No store CAS.** The engine uses unique keys per seal. `ManifestSequencer`
persists index objects with ordinary puts.

## Operator evidence

MinIO and Garage have green multipart + engine suites (see repository TD-002).
AWS S3 and R2 remain candidates until the same suite is run with credentials.
