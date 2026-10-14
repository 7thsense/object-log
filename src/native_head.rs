//! Fixed head observations only. No root publication or legacy migration port.
use crate::{AppendOwner, BoundedMetadataStore, MetadataSnapshot, MetadataSwap, ObjectLogError};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, Semaphore, oneshot};

const MAGIC: &[u8; 8] = b"ONH2\0\0\0\x01";
const HEAD_BYTES: usize = 192;
/// An exact versioned native head, obtained from bounded durable authority.
/// Roots are opaque commitments; this observation grants neither writer
/// exclusivity nor semantic membership/nonmembership proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeHead {
    namespace: [u8; 32],
    incarnation: [u8; 32],
    epoch: u64,
    log_start: u64,
    next_offset: u64,
    index_root: [u8; 32],
    semantic_anchor: [u8; 32],
}
impl NativeHead {
    /// Durable writer fence, distinct from any historical record epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Retained logical bounds from the same exact head.
    pub fn bounds(&self) -> (u64, u64) {
        (self.log_start, self.next_offset)
    }
    /// Immutable log-index root identity; not a proof of semantic results.
    pub fn index_root(&self) -> &[u8; 32] {
        &self.index_root
    }
    /// Opaque semantic-root commitment, interpreted solely by the consumer.
    pub fn semantic_anchor(&self) -> &[u8; 32] {
        &self.semantic_anchor
    }
    fn encode(&self) -> [u8; HEAD_BYTES] {
        let mut out = [0; HEAD_BYTES];
        out[..8].copy_from_slice(MAGIC);
        out[8..40].copy_from_slice(&self.namespace);
        out[40..72].copy_from_slice(&self.incarnation);
        out[72..80].copy_from_slice(&self.epoch.to_be_bytes());
        out[80..88].copy_from_slice(&self.log_start.to_be_bytes());
        out[88..96].copy_from_slice(&self.next_offset.to_be_bytes());
        out[96..128].copy_from_slice(&self.index_root);
        out[128..160].copy_from_slice(&self.semantic_anchor);
        let checksum = Sha256::digest(&out[..160]);
        out[160..].copy_from_slice(&checksum);
        out
    }
    fn decode(
        raw: &[u8],
        namespace: &[u8; 32],
        incarnation: &[u8; 32],
    ) -> Result<Self, ObjectLogError> {
        if raw.len() != HEAD_BYTES
            || &raw[..8] != MAGIC
            || Sha256::digest(&raw[..160]).as_slice() != &raw[160..]
        {
            return Err(invalid("invalid native head format, length or checksum"));
        }
        let head = Self {
            namespace: raw[8..40].try_into().expect("fixed range"),
            incarnation: raw[40..72].try_into().expect("fixed range"),
            epoch: u64::from_be_bytes(raw[72..80].try_into().expect("fixed range")),
            log_start: u64::from_be_bytes(raw[80..88].try_into().expect("fixed range")),
            next_offset: u64::from_be_bytes(raw[88..96].try_into().expect("fixed range")),
            index_root: raw[96..128].try_into().expect("fixed range"),
            semantic_anchor: raw[128..160].try_into().expect("fixed range"),
        };
        if head.namespace != *namespace
            || head.incarnation != *incarnation
            || head.log_start > head.next_offset
            || head.next_offset > i64::MAX as u64
            || (head.next_offset != 0
                && (head.index_root == [0; 32] || head.semantic_anchor == [0; 32]))
        {
            return Err(invalid("native head scope, bounds or roots mismatch"));
        }
        Ok(head)
    }
}
fn invalid(message: &str) -> ObjectLogError {
    ObjectLogError::Sequencer(message.into())
}
struct Lifecycle {
    closed: bool,
    owned: usize,
}
struct Owner {
    store: Arc<dyn BoundedMetadataStore>,
    key: String,
    namespace: [u8; 32],
    incarnation: [u8; 32],
    gate: Arc<Semaphore>,
    lifecycle: Mutex<Lifecycle>,
    drained: Notify,
}
struct PhysicalOwner {
    owner: Arc<Owner>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl Drop for PhysicalOwner {
    fn drop(&mut self) {
        drop(self.permit.take());
        let mut state = self
            .owner
            .lifecycle
            .lock()
            .expect("head lifecycle poisoned");
        state.owned -= 1;
        if state.owned == 0 {
            self.owner.drained.notify_waiters();
        }
    }
}
/// Storage-internal reader/fencer for an already provisioned native head.
/// It cannot create a missing head, migrate legacy manifests, publish roots or
/// authorize Fireweed to bypass cold recovery. Operations are physically owned
/// after dispatch, serialized, and drained even when the caller drops its reply.
#[derive(Clone)]
pub struct NativeHeadReader {
    owner: Arc<Owner>,
}
impl NativeHeadReader {
    /// Bind an existing head's literal storage key and immutable scope. This
    /// performs no I/O. Missing authority always fails closed, including known
    /// catalog membership; provisioning is a separate future publication path.
    pub fn new_known(
        store: Arc<dyn BoundedMetadataStore>,
        key: String,
        namespace: [u8; 32],
        incarnation: [u8; 32],
    ) -> Result<Self, ObjectLogError> {
        if key.is_empty()
            || key.contains('\0')
            || key.split('/').any(|part| part == "..")
            || namespace == [0; 32]
            || incarnation == [0; 32]
        {
            return Err(ObjectLogError::InvalidConfig(
                "invalid native head binding".into(),
            ));
        }
        Ok(Self {
            owner: Arc::new(Owner {
                store,
                key,
                namespace,
                incarnation,
                gate: Arc::new(Semaphore::new(1)),
                lifecycle: Mutex::new(Lifecycle {
                    closed: false,
                    owned: 0,
                }),
                drained: Notify::new(),
            }),
        })
    }
    /// Read the exact durable head. Fresh missing/corrupt authority is unknown.
    pub async fn read(&self) -> Result<NativeHead, ObjectLogError> {
        self.dispatch(false).await
    }
    /// Increment only the durable epoch, preserving bounds and both roots.
    /// Competing CAS retries reread and validate the original namespace and
    /// incarnation. An ambiguous write is returned as an error without retry.
    pub async fn acquire(&self) -> Result<NativeHead, ObjectLogError> {
        self.dispatch(true).await
    }
    async fn dispatch(&self, acquire: bool) -> Result<NativeHead, ObjectLogError> {
        let permit = Arc::clone(&self.owner.gate)
            .acquire_owned()
            .await
            .map_err(|_| invalid("native head is closed"))?;
        {
            let mut state = self
                .owner
                .lifecycle
                .lock()
                .expect("head lifecycle poisoned");
            if state.closed {
                return Err(invalid("native head is closed"));
            }
            state.owned += 1;
        }
        let physical = Arc::new(PhysicalOwner {
            owner: Arc::clone(&self.owner),
            permit: Some(permit),
        });
        let (send, receive) = oneshot::channel();
        tokio::spawn(async move {
            let result = physical
                .owner
                .execute(acquire, Arc::clone(&physical) as AppendOwner)
                .await;
            let _ = send.send(result);
            drop(physical);
        });
        receive
            .await
            .map_err(|_| invalid("native head physical operation panicked"))?
    }
    /// Close admission and await adapter-owned operations and source buffers.
    /// SDK transport internals and ambiguous remote effects are not certified.
    /// Already
    /// queued pre-dispatch callers fail without I/O; all clones share closure.
    pub async fn close_and_drain(&self) {
        self.owner
            .lifecycle
            .lock()
            .expect("head lifecycle poisoned")
            .closed = true;
        self.owner.gate.close();
        loop {
            let notified = self.owner.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .owner
                .lifecycle
                .lock()
                .expect("head lifecycle poisoned")
                .owned
                == 0
            {
                return;
            }
            notified.await;
        }
    }
}
impl Owner {
    fn decode(&self, observed: &MetadataSnapshot) -> Result<NativeHead, ObjectLogError> {
        let bytes = observed
            .bytes()
            .ok_or_else(|| invalid("known native head is missing"))?;
        NativeHead::decode(bytes, &self.namespace, &self.incarnation)
    }
    async fn execute(
        &self,
        acquire: bool,
        physical: AppendOwner,
    ) -> Result<NativeHead, ObjectLogError> {
        let mut previous: Option<NativeHead> = None;
        let mut observed = self
            .store
            .read_metadata(&self.key, HEAD_BYTES, Arc::clone(&physical))
            .await?;
        for _ in 0..8 {
            let mut head = self.decode(&observed)?;
            if previous.is_some_and(|old| {
                head.epoch < old.epoch
                    || head.log_start < old.log_start
                    || head.next_offset < old.next_offset
                    || (head.bounds() == old.bounds()
                        && (head.index_root != old.index_root
                            || head.semantic_anchor != old.semantic_anchor))
            }) {
                return Err(invalid(
                    "native head conflict authority regressed or changed roots",
                ));
            }
            previous = Some(head);
            if !acquire {
                return Ok(head);
            }
            head.epoch = head
                .epoch
                .checked_add(1)
                .ok_or_else(|| invalid("native epoch exhausted"))?;
            match self
                .store
                .swap_metadata(
                    &self.key,
                    observed,
                    Bytes::copy_from_slice(&head.encode()),
                    HEAD_BYTES,
                    Arc::clone(&physical),
                )
                .await?
            {
                MetadataSwap::Stored => return Ok(head),
                MetadataSwap::Conflict(current) => observed = current,
            }
        }
        Err(invalid("native head conditional acquisition exhausted"))
    }
}

#[cfg(test)]
mod tests;
