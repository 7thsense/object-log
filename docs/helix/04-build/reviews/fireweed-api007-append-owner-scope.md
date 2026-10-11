# Fireweed API-007 engine-owned append lifetime

Opt-in produce_owned_at_epoch accepts an opaque shared lifetime owner and uses
the existing budget/admission/enqueue path. Legacy produce methods supply none.
Pending batches drop payload/data before their owner. Each started upload also
retains the owners from its sealed batch in a whole ordered OwnedUpload container.
The consuming upload method retains these across retrying I/O and drops them only
after upload terminal completion. Thus failure draining can drop Pending/FlushWork
and send errors while independently started copied uploads still retain owners.
No ACK, flush, fencing or sequencer protocol changes are intended.

Tests use a store that deliberately copies and drops original input chunks before
withholding PUT. Producer cancellation retains the owner until upload and flush
complete. The engine-failure path drops the pending work and sends its error, but
the owner remains until the separately retained upload is explicitly drained.

An acknowledgement is not an owner-terminal notification. Embedders must await
an owner final-drop callback when final reservation release matters, and admit
all payload/copy/header capacity before submission. This lifetime port does not
cancel an accepted append, certify remote durability, publish queue genesis,
apply a projection/cursor, or activate Fireweed's native queue writer. Started
pre-phase reads and timeout classification still need owned handling there.

Queue failure cleanup detaches rejected items under the engine lock, publishes
closed state, then releases the lock before dropping payload/lifetime owners.
A standalone Shared fixture verifies a final owner callback can acquire the
engine mutex; there is no competing flusher in this test.

Astra Ultra final review: APPROVE iteration 72. Reviewed complete source,
corrections and standalone callback test; no remaining blocker in this scope.
Final verification: 75 non-provider tests, one doctest, three strict S3 HTTP tests,
and strict all-target/all-feature Clippy pass; independent diff check is clean.
