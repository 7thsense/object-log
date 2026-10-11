# Fireweed API-007 bounded replay prerequisite

Main base: 8d9675b. Normal merge 9c1cba7 preserves the v0.3.6 release ancestry
(f14e07e) already pinned by Fireweed: grouping/flush barriers, conditional partition
index, durable epoch fence/refresh, moved-fence classification and rejection handling.
Main's toolchain/CI/docs are retained. The lock conflict resolves to the existing
syn 3.0.6 package; Cargo validates the coherent graph. No history rewrite.

BoundedSequencer is a separate compile-time port, with no lookup-plus-truncate
fallback. Manifest and Memory binary-search validated ordered/contiguous positive
ranges, then calculate entry/header/full object-id clone costs before allocating
only the affordable page. Persisted indexes validate on open/reload/install, with
checked counts/offsets and retained boundary/tail coverage. New commit inputs are
validated before mutation/publication. Pages do not rescan the remaining history.

FetchPlan exposes metadata/lengths before payload reads. Caller admits metadata
budget before planning and actual selected batch bytes before read. The first batch
may exceed a soft target; a separate hard per-batch bound rejects before I/O.
Plan binds sequencer and blob authority, checks returned offset ranges, and strict
reads enforce requested lengths. Native embedding must retain its own operation
admission with these owned buffers, including detached local worker completion.

BlobStore::get_range_exact defaults to immediate failure, preserving old custom
adapters without silently falling back. Local opens once and reads the checked
range; Memory returns a shared slice. S3 intercepts status/length/Content-Range
before deserialization, rejecting ignored ranges and unbounded error bodies;
then streams exact bounded success. Zero reads use HEAD, never full-object GET.
The full-object fallback environment variable is ignored by this strict path.
Legacy get_range/fetch remain compatible.

Coverage includes full-name metadata budgets, mid-batch offsets, retained-tail
boundaries, empty EOF, negative/overflow/corrupt ranges, page-concatenation parity,
5 MiB first-batch soft/hard boundaries and cold reopen. Hermetic HTTP fixtures check
206 metadata/body, ignored ranges, truncation, HEAD zero reads and withheld 1 GiB
200/404 bodies. These are protocol tests, not remote S3 durability qualification.

This bounds incremental search/copies and payload reads. Manifest still loads and
rewrites the full partition index: total residency, cold-open RAM and write scaling
remain qualification gates. Fireweed must explicitly forward strict reads through
its tracing/pausing/fault wrappers when advancing the exact dependency revision.
Raw queue append/replay, claim receipts, projector atomicity, migration, native
S3/Turso factories and comparable settled benchmarks remain outstanding.

Astra Ultra corrections: retention is checked under the same lock that copies the
page, preventing a concurrent truncation from appearing as empty EOF. Strict reads
reuse the existing transient retry policy without downgrading to legacy reads.
Regression tests cover the deterministic retention race, retry success/exhaustion,
and corrupt persisted-index open/refresh without replacing a valid cached index.

Final verification: 72 non-provider unit/integration tests and one doctest pass;
three hermetic strict S3 HTTP tests pass. The exact CI all-targets/all-features
Clippy command passes with warnings denied. Equivalent initializer, let-chain,
and test type-alias cleanup resolves existing Rust 1.99 lint findings.
