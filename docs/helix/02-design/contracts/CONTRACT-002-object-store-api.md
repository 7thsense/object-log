---
ddx:
  id: contract-object-store-api
  depends_on:
    - contract-core-log-api
    - adr-object-storage-log-engine-and-sequencer-seam
---

# Object storage and bounded metadata authority

Contract ID: CONTRACT-002. Version: v5. Status: accepted.

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
Local and S3 implement `read_metadata` and `swap_metadata`. Callers supply an
explicit `MetadataLimits` with positive payload and opaque version-token byte
bounds, plus a physical lifetime owner, before I/O. Both methods use these limits;
Local requires 56 bytes for its fixed file-version identity. Oversized payload or
observed token input is rejected without publication. A returned snapshot binds the exact
adapter instance, literal key, observed version and bytes/absence; aliases
share one immutable allocation for payload, full key and version; cloning a
snapshot does not copy their buffers. Wrong-provider/key tokens fail before I/O.

Local reads check regular-file size before allocation and reject changes during
read. Conditional replacement compares file identity/version and complete bytes
under the cross-process lock, then uses durable atomic publication. A stale
version conflicts even when replacement bytes happen to match.

S3 reads validate status, Content-Length and exact ETag byte length before
successful-body collection or SDK token deserialization. Tokens preserve quotes
and all opaque bytes; they are not interpreted as MD5. Any present ETag on a
successful PUT is also bounded before SDK deserialization; an oversized 200
response remains ambiguous after one conditional write. Missing PUT ETag remains
allowed by the existing success protocol. Unsupported transfer/content encoding is rejected. Missing responses and
412 conflicts are classified before error-body collection. Only bounded 409
bodies enter SDK parsing, preserving exact ConditionalRequestConflict semantics.
Every conditional PUT has one attempt. Conflict rereads use the same payload
and token bounds;
transport failure, malformed/truncated response or failed conflict reread remains
unknown, including a lost reply after possible publication. HTTP 200 alone does
not establish success when the SDK reports an embedded error.

Returned payload/token observations retain their byte reservation, independently
of physical operation admission. Request closures and upload source bytes retain
both owners until physical work and its source allocations finish. Local blocking
closures preserve them across caller/runtime cancellation; S3 source uploads
preserve them when the HTTP connection outlives the response future. Retaining a
completed observation cannot prevent operation drain. This bounds admitted payloads and retained opaque tokens; callers also reserve
key/provider source-copy scratch. HTTP header-parser, URI, connection/TLS,
allocator and copied transport residency/lifetimes remain separately qualified. Total resource and provider-effect qualification remain gates.

## Fixed-head qualification boundary

The test-internal head reader exercises the production port with an exact
240-byte versioned/checksummed head: namespace and incarnation commitments,
unsigned epoch, append-only signed-offset-compatible bounds, a complete 64-byte
immutable index descriptor, and explicitly tagged semantic coverage. An empty
index uses only the canonical zero descriptor; nonempty roots cover exactly
[0, next_offset). Uninitialized semantic coverage contains zero coverage/root
bytes; covered state contains a nonzero seal root and coverage exactly equal to
the positive head next offset. Unknown tags, reserved bytes, mixed uninitialized
fields, and the previous 192-byte format fail closed. Legacy append-only
publication retains uninitialized coverage. Checked acquisition
increments only epoch, preserving roots/bounds exactly. Missing known authority,
legacy JSON, scope mismatch, corrupt bytes, regressed conflict observations and
epoch exhaustion fail closed. It cannot create heads, migrate manifests, publish
arbitrary roots or enable Fireweed prefix reuse.

The `native` feature owns the production head codec and authority core through
`NativeLog`, publishes immutable authenticated pages before its head CAS, and
provides lazy open, explicit refresh/acquire, fixed cuts and bounded page proofs.
The separate head-reader harness remains test-internal. Coupled log and ordered
byte-map publication is available through the same authority core. Explicit
retention/reclamation remains required. Consumer queue policy stays outside the SDK. Checksums detect corruption, not authenticated
provenance or valid-record substitution.

## Captured index and immutable data

The native-v2 implementation exposed through the `native` feature uses 4096-byte versioned
pages. Version 2 leaf slots are 192 bytes and separately commit complete physical
and logical identity overflow graphs, positive logical ranges, exact physical byte
ranges, and a closed frame-kind tag. Legacy certificates hash the payload;
coupled certificates hash the complete versioned frame, including its transition
metadata and full literal identity. Unknown frame kinds fail closed. Fixed fanout and height bounds constrain page/path
allocation; page reads verify the complete descriptor, scope, checksum and shape.
A captured head validates its actual root page before authorizing index reads.
Missing or corrupt touched pages fail closed; absence requires authenticated bounds.

Physical data names are bounded within disjoint scoped page/data namespaces.
Hashes select names; they do not establish identity equality. Versioned data frames
contain the full UTF-8 literal identity and exact payload length. Coupled frames
add a fixed 224-byte extension committing scope, before/after manifest hashes,
generation, group bounds, entry count, ordinal and assigned logical range. Their
separate physical namespace derives names from the complete frame certificate. Reuse compares
complete identity and content. Captured payload reads check both authenticated
identities, derived physical key, exact frame range, and digest. Logical identity
length does not expand the physical filename; configured prefix/provider limits
remain admission requirements.

One store executor and byte pool serve all namespace bindings. Nested page/head
work reuses the admitted outer operation. Returned cuts and payloads retain byte
reservations independently of physical admission, allowing shutdown while those
results remain alive. Conditional publication is single-attempt against the exact
original observation. Ambiguous resolution proves original-prefix preservation
with a bounded right-spine comparison before inspecting the exact assigned range.
Append groups reject empty inputs, invalid counts/frames/offsets, aggregate limit
violations, and duplicate full literal identities before immutable writes. The
retained ordered certificates are byte-admitted before sequential frame work.
One original head CAS publishes the complete group. Single-entry append uses the
same preparation path. Duplicate rejection within a group does not establish
cross-history deduplication.
Resolution walks authenticated actual entry boundaries under an explicit proof-work
budget, retaining any mismatch until the complete assigned range is covered.
Merged or split competing entries may prove rejection only after full coverage;
partial, missing, corrupt, regressed, or over-budget coverage remains unknown.
Matching every ordered certificate proves durable content, not historical writer
identity. Writer clones share serialized, monotonic knowledge of complete head
prefixes and sticky revocation on a higher fence; refreshing does not reacquire
or replace the original publication token.
Committed and ambiguous pages remain retained; production reclamation and migration
are unsupported. Legacy append refuses covered heads. A nonempty uninitialized
head cannot be implicitly promoted; migration requires an explicitly verified
consumer bootstrap. Index proofs alone do not authorize skipping Fireweed semantic replay.

Namespace initialization creates a fresh candidate head before publishing its
immutable full-literal namespace binding. Incarnations come from admitted OS
entropy; callers cannot supply a production seed. Candidate and binding attempts
are single-use. Existing candidate bytes are collisions, even if identical.
Lost candidate replies leave unreachable candidates; they cannot grant binding
rights. Competing binding publications resolve the actual stored winner and
validate its scoped head and root. Missing known heads never authorize creation.
Published bindings and candidates remain retained. Detecting deletion of the
sole registry across a cold reopen requires a surviving catalog anchor and is
not established by this private protocol.

A published namespace owns a sealed, byte-admitted page binding without a store
session or physical permit. Writer handoff consumes its original captured token;
it cannot replace that observation with a fresh one. Known refresh also consumes
the prior cap and validates non-regression and its authenticated prefix. Returned
writers use the existing shared lineage. The handoff returns a writer and sealed
binding; operation-scoped stores are reconstructed only inside admitted work.
Namespace publication alone is not Fireweed provision success. For a newly
selected empty namespace, the consumer fences its initial epoch and publishes
or adopts offset-zero genesis. It validates the full address and schema and
preserves the winning original cursor secret. Existing winners may already
be fenced or contain genesis.

The focused finite election model separates candidate and binding storage from
observed replies. Independent transition oracles check selected-head validity
at capability emission, immutable selection, and refusal to reset missing or
corrupt published authority, including empty heads. It assumes distinct fresh
incarnations and does not prove entropy quality, Rust correspondence, physical
durability, registry deletion detection, or a shared authority floor across
separately opened handles and cold reopen.

## Coupled state and work publication

`prepare_with_edits` accepts an exact captured cut, a nonempty append group and
sorted unique byte-key edits. Keys and values may contain arbitrary bytes; an
empty value differs from deletion. The caller supplies resolved policy updates,
not a replacement root. Preparation checks the complete cut, binding, provider
owner, literal head key and original metadata version under the shared writer
lock. Refreshing a writer alias cannot silently rebase edits prepared from an
older cut. One original-token CAS publishes all state and work together.

Publication order is map pages, manifest, coupled data frames, append-index
pages, seal, then head CAS. A seal commits the exact complete append descriptor,
manifest and next offset. Manifests commit the resulting map, generation, group
bounds and canonical predecessor skip links. The links give logarithmic lineage
proofs under the canonical committed-writer assumption; they do not replay every
skipped edit. Same-offset refresh requires identical index and seal roots even
when the epoch increases. Missing proof objects, insufficient work and malformed
or mismatched roots cannot install a new authority floor. Higher observed epochs
remain sticky even when their roots are unreadable.

The ordered map uses scoped 4096-byte immutable pages and length-checked overflow
chains, with no logical key/value truncation. Reads verify touched descriptors,
ordered full keys and inherited bounds. `semantic_lookup` proves presence or
absence at the supplied covered cut. Uninitialized coverage cannot prove either;
a covered empty map can. Copy-on-write edits preserve untouched subtrees and
publish children before parents. Failed preparation may leave immutable orphans,
but cannot expose a partially updated map or append group.

`semantic_scan` and its opaque continuation retain the original cut, range and
last returned full key. Resumption is exclusive of that key and cannot move to a
new head. `Complete` means verified exhaustion of the requested interval;
`LimitReached` leaves exhaustion unproven. A row or byte limit is separate from
source proof work. The authenticated value length is checked against output
capacity before fetching overflow bytes. A first row that cannot fit fails;
a returned prefix may carry a continuation. Missing/corrupt source or exhausted
proof work fails the whole page, never producing a successful completion marker.
Cancellation cannot mutate a retained input continuation.

Semantic work limits count map/manifest/seal reads and hydrated bytes; append
index preparation has its separately bounded group path. Input edit bytes,
returned rows, copied bounds and continuations also hold shared byte admission.
Returned values and tokens retain byte credits independently of physical permits.
Cancellation after dispatch does not release physical ownership before accepted
work finishes. These component bounds do not establish total transport residency.

## Verification

Native-head tests exercise actual Local I/O and the pinned S3 SDK transport,
including exact/N-1 budgets, oversized reads/conflicts, malformed framing,
competing and regressed fences, both signing modes, one-attempt lost replies,
runtime shutdown during a physical Local write, panic/unlock, aliased result
cleanup and backpressured upload cancellation. Hermetic HTTP controls prove the
adapter protocol, not remote provider durability. The bounded CAS model covers
root preservation, scope/budget admission, ambiguous outcomes and physical-owner
release; its code mapping is documentary rather than a refinement proof.

The COW gate executes a bounded independent model with a flat ghost-history oracle,
separate storage/reply transitions, page-shape checks, completed witnesses and
intentionally faulty variants. Reports bind the executed model/helper bytes to
captured source hashes and invalidate old success before dependency loading.
This is abstract tree/CAS/content-resolution evidence, not executed Rust
correspondence, physical page reclamation, frame/provider durability, total memory,
consumer semantic, or concrete multi-entry/fenced-writer qualification. The finite
model includes unequal-count groups, all-or-none CAS, bounded actual-entry
resolution, merged/split competitors, and mismatch before incomplete coverage. Physical tests and
release performance gates remain separate obligations. S3 response-loss controls
inject failure after validated HTTP 200 before SDK response processing; they do
not claim TCP loss or process-crash coverage.

The private retained-publication carrier preserves the original cut/token,
ordered certificates, sealed binding and writer lineage before dispatch. It
retains byte admission only. Submit and resolve acquire its mutex inside the
shared executor's admitted operation, then reconstruct an operation-scoped store.
The existing publication attempted bit is the sole terminal submission state;
caller cancellation while waiting for admission does not claim it. A dispatched
attempt remains available for fresh durable resolution after caller cancellation,
lost reply or owned-task panic. Cloned submitters may issue at most one original
conditional write. Construction verifies the admitting store, selected binding
and literal head key before allocating the carrier. Durable-content resolution
proves the original complete bytes, not historical writer identity or a workflow
business outcome. The `native` feature exposes this retained carrier as `NativePublication`. This
SDK API exposure does not activate the Fireweed queue backend or qualify its
queue policy, registry deletion, reclamation or release performance.

The coupled semantic gate exhausts 8000 finite competing-publication pairs and
checks 17 completed witnesses against independent flat map and log ghosts. Its
25 faulty variants require deterministically reproduced counterexamples; exploration exhaustion
fails the gate. Nine seeded generations exercise independently computed exact
and N-1 lineage budgets. Captured scans, stale cuts, sticky fences and retained
resolution floors are checked in this protocol abstraction. Source-bound reports
and stale-success controls qualify the gate itself; this is not a refinement
proof of Rust, the page codec, providers, cancellation resources or performance.
Actual Local and S3 tests separately exercise atomic state/work publication,
response loss and original-cut scans in both signing modes.
