# Iteration 78: native epoch acquisition with replay bounds

An opt-in acquire_authoritative_epoch port defaults to unsupported without
legacy fence fallback. Manifest freshly reloads and validates the durable index
under commit_order, checked-increments the epoch, and CAS-publishes unchanged
head, retained boundary and entries. Successful CAS returns the matching
AuthoritativePartitionState, avoiding a second read or adopting a different
caller's epoch. Conflicts re-read up to eight times. Unknown I/O replies remain
errors, and epoch overflow or vanished known history never recreates an index.

Independent concurrent acquisitions return distinct epochs. Tests preserve the
head and exact entries, reject stale-epoch publication, and cover unsupported
implementations, lost replies, retry exhaustion, overflow and disappearance.
Legacy idempotent fence behavior and serialized types remain unchanged.

The returned state grants no perpetual exclusivity. Subsequent publication is
still fenced by native epoch and conditional head. Full manifest admission and
residency remain qualification gates. Fireweed must remove its raw append
legacy epoch-document dependency before consuming this port; no second legacy
metadata write is required by native acquisition. No factory activation here.

Validation: 82 selected non-provider tests (including four budget checks), one
doctest, three strict S3 HTTP tests and strict all-target/all-feature Clippy
passed. Throughput benchmarks were not rerun for this narrow port change.

Astra's conflict/disappearance finding was corrected before publication:
Some conflict existence is recorded before parsing; valid authority is
installed before retry. None conflict
returns unknown immediately. None replies and CAS I/O errors retain an in-memory
uncertain-epoch existence flag, so future absence cannot reset this handle to
unseen state. This flag is not synthetic durable authority or success evidence.
Valid Some, malformed Some and None conflict-then-disappearance tests assert
no replacement CAS and failure of native snapshot/conditional publication,
including a subsequent acquisition on the same handle.

Astra Ultra final actual-source review: APPROVE iteration 78. Corrected
uncertainty markers, conflict handling, source/tests/scope and validation-count
correction reviewed without remaining blockers. Final corrected selected suite
of 82 tests passed before publication; strict Clippy, doctest and HTTP checks
also passed. Independent diff hygiene verified.
