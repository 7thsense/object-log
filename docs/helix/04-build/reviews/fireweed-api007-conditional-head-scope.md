# Fireweed API-007 native conditional log head

Opt-in ConditionalCommitOutcome preserves existing CommitOutcome/CommitBatch
encodings and matches. Sequencer defaults reject conditions before invoking
legacy commit. Owned conditional produce requires Sequenced acknowledgement and
validates support/nonnegative heads before enqueue/upload. Legacy all-None
traffic follows its previous commit path without an extra result allocation.

Manifest evaluates fresh validated durable documents. Producer-order siblings
advance the tentative next offset only when accepted; rejected rows do not.
All-rejected partitions do not publish an index. CAS conflicts reconcile exact
planned location/base/count first and re-evaluate original conditions. Storage
errors, malformed state, retry exhaustion and partial reconciliation are Unknown.
Unknown wrapper errors are never relabelled as definite head/fence rejections.
Cross-partition publication remains independent, not a transaction.

A range retained away without exact surviving identity evidence is Unknown,
both during conflict reconciliation and on a later ambiguous retry. A known
observed/migrated index disappearing fails closed; it is never recreated at zero.
Genuinely never-observed empty creation remains supported.

Tests cover forced same-epoch independent head-zero races, stale-handle refresh,
ordered accepted/rejected siblings, no-CAS rejection, input validation, exact
CAS reconciliation, lost publication replies, exhaustion, malformed current
state, partial/full retention, ambiguous retry after retention, known-index
loss, unsupported conditions without fallback/upload, and sequenced-only ACK.

Fireweed must bind the condition to the fully applied authoritative log next
offset, never its insertion sequence or sparse high-water hint. No native queue
factory, first-genesis protocol, projection/cursor application, consumer cutover,
or remote durability/performance qualification is included. Full manifest
residency, metadata admission and write scaling remain qualification gates.

Astra Ultra final review: APPROVE iteration 74. Complete corrected source/scope
reviewed; no remaining blocker in this prerequisite. Final checks: 82 non-provider
tests, one doctest, three strict S3 HTTP tests, strict all-target/all-feature
Clippy and formatting pass. Independent diff check is clean.
