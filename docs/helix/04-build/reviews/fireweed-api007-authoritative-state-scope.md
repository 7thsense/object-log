# Iteration 76: native authoritative partition state

AuthoritativePartitionState exposes a point-in-time durable epoch, retained
start and next logical offset. The optional Sequencer method defaults to
InvalidConfig without invoking legacy refresh or cached getters. Existing
serialized batch and outcome types remain unchanged.

Manifest performs one durable reload under commit_order. Existing whole-index
validation precedes cache installation. Conditional publication and this read
share the known-history predicate. A missing previously observed index fails
closed, including a fenced but empty partition. A genuinely unseen missing
partition returns zero state without installing a synthetic observation.

Tests cover fresh independent-handle visibility, malformed durable bounds
preserving the last valid cache, vanished populated and fenced-empty indexes,
and unsupported implementations. This is a replay target, not exclusive writer
ownership or a pinned read session. Consumers still need prefix validation and
conditional publication. Full manifest residency, metadata admission and rewrite
scaling remain qualification gates. No Fireweed factory is activated here.

Validation: 83 non-provider tests, one doctest, three strict S3 HTTP tests,
strict all-target/all-feature Clippy and formatting passed.

Astra Ultra final actual-source review: APPROVE iteration 76. Corrected source,
tests and scope reviewed without remaining blockers. Independent diff hygiene
passed. Approval covers only the point-in-time authority port.
