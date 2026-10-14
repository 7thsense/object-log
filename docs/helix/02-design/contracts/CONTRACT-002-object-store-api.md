---
ddx:
  id: contract-object-store-api
  depends_on:
    - contract-core-log-api
    - adr-object-storage-log-engine-and-sequencer-seam
---

# Object storage and bounded metadata authority

Contract ID: CONTRACT-002. Version: v3. Status: accepted.

The immutable data-object port remains `BlobStore`: durable put/put_chunks,
get, range read, list, delete and media-operation accounting. Local publication
writes a temporary file, syncs data, renames and syncs the parent directory.
S3 success follows the selected provider's durable-write semantics; Memory is a
test backend. These guarantees require provider/process-loss qualification.

Adapters preserve complete opaque bytes. `get` and range reads return `None` for
missing objects, distinct from present empty bytes. Inverted or past-EOF ranges
return `RangeOutOfBounds`; valid empty `n..n` ranges return empty bytes. Listing
is complete for the requested prefix, including provider pagination; publication
temporary keys may be omitted. Deleting a missing key succeeds. Optional media
statistics snapshot and reset durable operation/byte counters; absent statistics
use the engine's default one-operation-per-successful-put accounting.

Object keys retain full literal identity and reject empty, NUL and `..` path
segments. Payloads remain opaque. Unique data keys do not require conditional
publication; the sequencer's mutable authority does. Legacy `BlobStore` CAS
remains compatible, including its unsafe-for-native default read/put fallback.
Native authority must never use that fallback.

## Bounded authority port

`BoundedMetadataStore` is a separate required storage port with no defaults.
Local and S3 implement `read_metadata` and `swap_metadata`. Callers supply a
positive payload budget and a physical lifetime owner before I/O. Oversized
input is rejected without publication. A returned snapshot binds the exact
adapter instance, literal key, observed version and bytes/absence; aliases
retain payload ownership. Wrong-provider/key tokens fail before I/O.

Local reads check regular-file size before allocation and reject changes during
read. Conditional replacement compares file identity/version and complete bytes
under the cross-process lock, then uses durable atomic publication. A stale
version conflicts even when replacement bytes happen to match.

S3 reads validate status, Content-Length and ETag before collecting a successful
body. Unsupported transfer/content encoding is rejected. Missing responses and
412 conflicts are classified before error-body collection. Only bounded 409
bodies enter SDK parsing, preserving exact ConditionalRequestConflict semantics.
Every conditional PUT has one attempt. Conflict rereads use the same bound;
transport failure, malformed/truncated response or failed conflict reread remains
unknown, including a lost reply after possible publication. HTTP 200 alone does
not establish success when the SDK reports an embedded error.

Payload, token and upload aliases retain the supplied physical owner until their
source bytes and version/key allocations are released. Local blocking closures
retain it across caller/runtime cancellation. S3 source upload bytes retain it
when the HTTP connection outlives the response future. This bounds admitted
payloads; it does not certify all key/ETag/HTTP/TLS allocation or copied transport
buffer lifetimes. Total resource and provider-effect qualification remain gates.

## Fixed-head qualification boundary

The test-internal head reader exercises the production port with an exact
192-byte versioned/checksummed head: namespace and incarnation commitments,
unsigned epoch, nonnegative signed-offset-compatible retained bounds, immutable
index-root identity and an opaque semantic-anchor slot. Checked acquisition
increments only epoch, preserving roots/bounds exactly. Missing known authority,
legacy JSON, scope mismatch, corrupt bytes, regressed conflict observations and
epoch exhaustion fail closed. It cannot create heads, migrate manifests, publish
arbitrary roots or enable Fireweed prefix reuse.

A production native-v2 sequencer must subsequently own this component, publish
immutable authenticated pages before its head CAS, and commit log and semantic
roots together. Lazy open/refresh/acquire/install, bounded page proofs and explicit
retention/reclamation are required; paging lookup alone is insufficient. Consumer
semantics stay outside the SDK. Checksums detect corruption, not authenticated
provenance or valid-record substitution.

## Verification

Native-head tests exercise actual Local I/O and the pinned S3 SDK transport,
including exact/N-1 budgets, oversized reads/conflicts, malformed framing,
competing and regressed fences, both signing modes, one-attempt lost replies,
runtime shutdown during a physical Local write, panic/unlock, aliased result
cleanup and backpressured upload cancellation. Hermetic HTTP controls prove the
adapter protocol, not remote provider durability. The bounded CAS model covers
root preservation, scope/budget admission, ambiguous outcomes and physical-owner
release; its code mapping is documentary rather than a refinement proof.
