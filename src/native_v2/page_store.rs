//! Content-addressed immutable pages, sharing one store-level byte admission.
//! This bounds owned page/key sources; it is not a total HTTP/TLS/RSS bound.
use super::codec::{
    BRANCH_SLOTS, DESCRIPTOR_BYTES, Descriptor, HEADER, Kind, LEAF_SLOTS, LeafEntry, PAGE_BYTES,
    Page, Scope,
};
use crate::{
    AppendOwner, BlobStore, BoundedMetadataStore, MetadataLimits, MetadataOwners, MetadataSwap,
    ObjectLogError,
};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) trait NativeStore: BlobStore + BoundedMetadataStore {
    fn page_source_scratch(
        &self,
        key_len: usize,
        limits: MetadataLimits,
    ) -> Result<usize, ObjectLogError>;
}
impl NativeStore for crate::LocalBlobStore {
    fn page_source_scratch(
        &self,
        key_len: usize,
        limits: MetadataLimits,
    ) -> Result<usize, ObjectLogError> {
        limits.validate_local()?;
        self.native_page_source_scratch(key_len)
    }
}

#[cfg(feature = "s3")]
impl NativeStore for crate::S3BlobStore {
    fn page_source_scratch(
        &self,
        key_len: usize,
        limits: MetadataLimits,
    ) -> Result<usize, ObjectLogError> {
        limits.validate()?;
        self.native_page_source_scratch(key_len, limits.version_bytes)
    }
}

pub(super) struct ByteBudget {
    available: Arc<Semaphore>,
    capacity: u32,
}
impl ByteBudget {
    pub fn new(capacity: u32) -> Result<Arc<Self>, ObjectLogError> {
        if capacity == 0 || capacity as usize > Semaphore::MAX_PERMITS {
            return Err(ObjectLogError::InvalidConfig("native byte capacity".into()));
        }
        Ok(Arc::new(Self {
            available: Arc::new(Semaphore::new(capacity as usize)),
            capacity,
        }))
    }
    pub fn charge(self: &Arc<Self>, bytes: usize) -> Result<Arc<Charge>, ObjectLogError> {
        let count = u32::try_from(bytes)
            .map_err(|_| ObjectLogError::BudgetExceeded("native byte capacity".into()))?;
        if count == 0 || count > self.capacity {
            return Err(ObjectLogError::BudgetExceeded(
                "native byte capacity".into(),
            ));
        }
        let permit = self
            .available
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| ObjectLogError::BudgetExceeded("native shared byte admission".into()))?;
        Ok(Arc::new(Charge { _permit: permit }))
    }
    #[cfg(test)]
    pub fn available(&self) -> usize {
        self.available.available_permits()
    }
}
pub(super) struct Charge {
    _permit: OwnedSemaphorePermit,
}

/// Decoded capacity cannot escape its charge through the page-store API.
pub(super) struct OwnedPage {
    page: Page,
    store_identity: Arc<()>,
    _charge: Arc<Charge>,
}
pub(super) enum PageView<'a> {
    Leaf(&'a [LeafEntry]),
    Branch {
        height: u8,
        children: &'a [Descriptor],
    },
    Overflow {
        start: u64,
        bytes: &'a [u8],
        next: Option<Descriptor>,
    },
}
impl OwnedPage {
    pub fn view(&self) -> PageView<'_> {
        match &self.page {
            Page::Leaf(entries) => PageView::Leaf(entries),
            Page::Branch { height, children } => PageView::Branch {
                height: *height,
                children,
            },
            Page::Overflow { start, bytes, next } => PageView::Overflow {
                start: *start,
                bytes,
                next: *next,
            },
        }
    }
    pub fn push_leaf(&mut self, entry: LeafEntry) -> Result<(), ObjectLogError> {
        entry.end()?;
        match &mut self.page {
            Page::Leaf(entries)
                if entries.len() < LEAF_SLOTS && entries.last().unwrap().end()? == entry.base =>
            {
                entries.push(entry);
                Ok(())
            }
            _ => Err(ObjectLogError::InvalidBatch("bounded leaf append".into())),
        }
    }
    pub fn replace_last_child(&mut self, child: Descriptor) -> Result<(), ObjectLogError> {
        child.validate()?;
        match &mut self.page {
            Page::Branch { height, children }
                if child.kind != Kind::Overflow
                    && child.height + 1 == *height
                    && children.last().unwrap().start == child.start =>
            {
                *children.last_mut().unwrap() = child;
                Ok(())
            }
            _ => Err(ObjectLogError::InvalidBatch(
                "bounded branch replacement".into(),
            )),
        }
    }
    pub fn push_child(&mut self, child: Descriptor) -> Result<(), ObjectLogError> {
        child.validate()?;
        match &mut self.page {
            Page::Branch { height, children }
                if children.len() < BRANCH_SLOTS
                    && child.kind != Kind::Overflow
                    && child.height + 1 == *height
                    && children.last().unwrap().end == child.start =>
            {
                children.push(child);
                Ok(())
            }
            _ => Err(ObjectLogError::InvalidBatch("bounded branch append".into())),
        }
    }
}

struct UploadedPage {
    bytes: Vec<u8>,
    // Drop the complete source allocation before its reservation.
    _charge: Arc<Charge>,
}
impl AsRef<[u8]> for UploadedPage {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

pub(super) struct OwnedDataKey {
    key: String,
    _charge: Arc<Charge>,
}
impl OwnedDataKey {
    pub fn as_str(&self) -> &str {
        &self.key
    }
}

#[derive(Clone)]
pub(super) struct PageBinding {
    identity: Arc<()>,
    store_identity: Arc<()>,
    prefix: Arc<str>,
    scope: Scope,
    allocation: usize,
    _binding_charge: Arc<Charge>,
}
pub(super) struct PageStore {
    store: Arc<dyn NativeStore>,
    identity: Arc<()>,
    store_identity: Arc<()>,
    prefix: Arc<str>,
    scope: Scope,
    budget: Arc<ByteBudget>,
    allocation: usize,
    limits: MetadataLimits,
    physical: Option<Arc<super::operation::Physical>>,
    _binding_charge: Option<Arc<Charge>>,
    _container_charge: Option<Arc<Charge>>,
}
impl PageStore {
    pub(super) fn planned_allocation(
        store: &dyn NativeStore,
        prefix: &str,
        limits: MetadataLimits,
    ) -> Result<usize, ObjectLogError> {
        // Config-owned prefix is shared. Reserve the complete generated key,
        // adapter key copy, encoded page, decoded capacity and source scratch.
        limits.validate()?;
        if limits.payload_bytes < PAGE_BYTES {
            return Err(ObjectLogError::InvalidConfig(
                "native metadata payload limit cannot hold a page".into(),
            ));
        }
        let key_len = prefix
            .len()
            .checked_add(195)
            .ok_or_else(|| ObjectLogError::InvalidConfig("native key length overflow".into()))?;
        let allocation = store
            .page_source_scratch(key_len, limits)?
            .checked_add(PAGE_BYTES * 4)
            .ok_or_else(|| {
                ObjectLogError::InvalidConfig("native page allocation overflow".into())
            })?;
        Ok(allocation)
    }
    pub fn new(
        store: Arc<dyn NativeStore>,
        prefix: String,
        scope: Scope,
        budget: Arc<ByteBudget>,
        limits: MetadataLimits,
    ) -> Result<Arc<Self>, ObjectLogError> {
        if prefix.is_empty()
            || prefix.starts_with('/')
            || prefix.contains('\0')
            || prefix
                .split('/')
                .any(|part| part == ".." || part == "." || part.is_empty())
            || scope.namespace == [0; 32]
            || scope.incarnation == [0; 32]
        {
            return Err(ObjectLogError::InvalidConfig(
                "native page store binding".into(),
            ));
        }
        let allocation = Self::planned_allocation(store.as_ref(), &prefix, limits)?;
        if allocation > budget.capacity as usize {
            return Err(ObjectLogError::InvalidConfig(
                "native budget cannot hold one page".into(),
            ));
        }
        let identity = Arc::new(());
        Ok(Arc::new(Self {
            store,
            store_identity: identity.clone(),
            identity,
            prefix: prefix.into(),
            scope,
            budget,
            allocation,
            limits,
            physical: None,
            _binding_charge: None,
            _container_charge: None,
        }))
    }
    /// Nested page operations reuse the admitted outer operation. Returned
    /// pages contain only byte guards and binding identity, never this session.
    pub fn in_operation(
        &self,
        physical: Arc<super::operation::Physical>,
    ) -> Result<Arc<Self>, ObjectLogError> {
        physical.validate_budget(&self.budget)?;
        Ok(Arc::new(Self {
            store: self.store.clone(),
            identity: self.identity.clone(),
            store_identity: self.store_identity.clone(),
            prefix: self.prefix.clone(),
            scope: self.scope,
            budget: self.budget.clone(),
            allocation: self.allocation,
            limits: self.limits,
            physical: Some(physical),
            _binding_charge: self._binding_charge.clone(),
            _container_charge: if self._binding_charge.is_some() {
                Some(self.charge_output(
                    std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>(),
                )?)
            } else {
                None
            },
        }))
    }
    pub fn derive(&self, prefix: String, scope: Scope) -> Result<Arc<Self>, ObjectLogError> {
        self.require_operation()?;
        let bytes = prefix
            .len()
            .checked_mul(2)
            .and_then(|n| {
                n.checked_add(2 * std::mem::size_of::<Self>() + 4 * std::mem::size_of::<usize>())
            })
            .ok_or_else(|| {
                ObjectLogError::BudgetExceeded("derived binding allocation overflow".into())
            })?;
        let charge = self.charge_output(bytes)?;
        let mut pages = Self::new(
            self.store.clone(),
            prefix,
            scope,
            self.budget.clone(),
            self.limits,
        )?;
        let inner = Arc::get_mut(&mut pages).expect("new exclusive binding");
        inner.store_identity = self.store_identity.clone();
        inner._binding_charge = Some(charge);
        pages.in_operation(self.physical.as_ref().expect("checked operation").clone())
    }
    pub fn seal_binding(&self) -> Result<PageBinding, ObjectLogError> {
        let charge = self._binding_charge.clone().ok_or_else(|| {
            ObjectLogError::InvalidConfig("only admitted derived bindings can be sealed".into())
        })?;
        Ok(PageBinding {
            identity: self.identity.clone(),
            store_identity: self.store_identity.clone(),
            prefix: self.prefix.clone(),
            scope: self.scope,
            allocation: self.allocation,
            _binding_charge: charge,
        })
    }
    pub fn for_binding(&self, binding: &PageBinding) -> Result<Arc<Self>, ObjectLogError> {
        self.require_operation()?;
        if !Arc::ptr_eq(&self.store_identity, &binding.store_identity) {
            return Err(ObjectLogError::InvalidConfig(
                "selected binding belongs to another store".into(),
            ));
        }
        let charge =
            self.charge_output(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())?;
        Ok(Arc::new(Self {
            store: self.store.clone(),
            identity: binding.identity.clone(),
            store_identity: self.store_identity.clone(),
            prefix: binding.prefix.clone(),
            scope: binding.scope,
            budget: self.budget.clone(),
            allocation: binding.allocation,
            limits: self.limits,
            physical: self.physical.clone(),
            _binding_charge: Some(binding._binding_charge.clone()),
            _container_charge: Some(charge),
        }))
    }
    fn owners(&self, retained: AppendOwner) -> MetadataOwners {
        MetadataOwners {
            physical: self
                .physical
                .clone()
                .map_or_else(|| retained.clone(), |physical| physical as AppendOwner),
            retained,
        }
    }
    pub fn require_operation(&self) -> Result<(), ObjectLogError> {
        if self.physical.is_none() {
            return Err(ObjectLogError::InvalidConfig(
                "native authority requires operation admission".into(),
            ));
        }
        Ok(())
    }
    pub fn scope(&self) -> Scope {
        self.scope
    }
    pub fn binding_identity(&self) -> Arc<()> {
        self.identity.clone()
    }
    pub fn validate_authority_key(&self, key: &str) -> Result<(), ObjectLogError> {
        if key == self.prefix.as_ref()
            || key
                .strip_prefix(self.prefix.as_ref())
                .is_some_and(|suffix| suffix.starts_with('/'))
            || key.is_empty()
            || key.starts_with('/')
            || key.contains('\0')
            || key
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return Err(ObjectLogError::InvalidConfig("native head key".into()));
        }
        Ok(())
    }
    pub async fn read_head(
        &self,
        key: &str,
        bytes: usize,
    ) -> Result<crate::MetadataSnapshot, ObjectLogError> {
        self.require_operation()?;
        self.validate_authority_key(key)?;
        let charge = self.budget.charge(
            self.store
                .page_source_scratch(key.len(), self.metadata_limits(bytes))?
                .checked_add(bytes)
                .ok_or_else(|| ObjectLogError::BudgetExceeded("head allocation overflow".into()))?,
        )?;
        self.store
            .read_metadata(
                key,
                self.metadata_limits(bytes),
                self.owners(charge as AppendOwner),
            )
            .await
    }
    pub fn data_key(&self, logical: &str) -> Result<OwnedDataKey, ObjectLogError> {
        let len = self
            .prefix
            .len()
            .checked_add(200)
            .ok_or_else(|| ObjectLogError::BudgetExceeded("data key overflow".into()))?;
        let charge = self.budget.charge(
            len.checked_add(std::mem::size_of::<String>())
                .ok_or_else(|| ObjectLogError::BudgetExceeded("data key overflow".into()))?,
        )?;
        use std::fmt::Write;
        let mut key = String::with_capacity(len);
        key.push_str(&self.prefix);
        for identity in [&self.scope.namespace, &self.scope.incarnation] {
            key.push('/');
            for byte in identity {
                write!(&mut key, "{byte:02x}").expect("String formatting");
            }
        }
        key.push_str("/data/");
        use sha2::{Digest, Sha256};
        for byte in Sha256::digest(logical.as_bytes()) {
            write!(&mut key, "{byte:02x}").expect("String formatting");
        }
        Ok(OwnedDataKey {
            key,
            _charge: charge,
        })
    }
    pub fn validate_data_limit(&self, bytes: usize) -> Result<(), ObjectLogError> {
        if bytes > self.limits.payload_bytes {
            return Err(ObjectLogError::BudgetExceeded(
                "native data payload limit".into(),
            ));
        }
        Ok(())
    }
    pub fn preflight_data(&self, key: &str, bytes: usize) -> Result<(), ObjectLogError> {
        self.require_operation()?;
        self.validate_data_limit(bytes)?;
        let required =
            self.store
                .page_source_scratch(key.len(), self.metadata_limits(bytes.max(1)))?
                .checked_add(bytes.checked_mul(2).ok_or_else(|| {
                    ObjectLogError::BudgetExceeded("data allocation overflow".into())
                })?)
                .ok_or_else(|| ObjectLogError::BudgetExceeded("data allocation overflow".into()))?;
        // Capacity check before creating overflow/data objects. This probe is
        // released immediately; concurrent work may exhaust later admission.
        drop(self.budget.charge(required)?);
        Ok(())
    }
    pub async fn read_data(
        &self,
        key: &str,
        bytes: usize,
    ) -> Result<crate::MetadataSnapshot, ObjectLogError> {
        self.require_operation()?;
        if bytes == 0 || bytes > self.limits.payload_bytes {
            return Err(ObjectLogError::BudgetExceeded(
                "native data read limit".into(),
            ));
        }
        let charge = self.budget.charge(
            self.store
                .page_source_scratch(key.len(), self.metadata_limits(bytes))?
                .checked_add(bytes)
                .ok_or_else(|| {
                    ObjectLogError::BudgetExceeded("data read allocation overflow".into())
                })?,
        )?;
        self.store
            .read_metadata(
                key,
                self.metadata_limits(bytes),
                self.owners(charge as AppendOwner),
            )
            .await
    }
    pub async fn upload_data(&self, key: &str, raw: &[u8]) -> Result<(), ObjectLogError> {
        self.require_operation()?;
        super::data::DataFrame::decode(raw)?;
        if raw.len() > self.limits.payload_bytes {
            return Err(ObjectLogError::BudgetExceeded(
                "native data payload limit".into(),
            ));
        }
        let charge = self.budget.charge(
            self.store
                .page_source_scratch(key.len(), self.metadata_limits(raw.len().max(1)))?
                .checked_add(raw.len().checked_mul(2).ok_or_else(|| {
                    ObjectLogError::BudgetExceeded("data allocation overflow".into())
                })?)
                .ok_or_else(|| ObjectLogError::BudgetExceeded("data allocation overflow".into()))?,
        )?;
        let limits = self.metadata_limits(raw.len().max(1));
        let source = Bytes::from_owner(UploadedPage {
            bytes: raw.to_vec(),
            _charge: charge.clone(),
        });
        let observed = self
            .store
            .read_metadata(key, limits, self.owners(charge.clone() as AppendOwner))
            .await?;
        if let Some(existing) = observed.bytes() {
            // Framed data reuse requires exact literal identity and payload;
            // no physical-key digest can substitute for this comparison.
            {
                let expected = super::data::DataFrame::decode(&source)?;
                let actual = super::data::DataFrame::decode(existing)?;
                if actual.identity != expected.identity || actual.payload != expected.payload {
                    return Err(ObjectLogError::Sequencer(
                        "immutable data identity differs".into(),
                    ));
                }
            }
            if existing == source.as_ref() {
                return Ok(());
            }
            return Err(ObjectLogError::Sequencer(
                "immutable data location differs".into(),
            ));
        }
        match self
            .store
            .swap_metadata(
                key,
                observed,
                source.clone(),
                limits,
                self.owners(charge as AppendOwner),
            )
            .await?
        {
            MetadataSwap::Stored => Ok(()),
            MetadataSwap::Conflict(current) if current.bytes() == Some(source.as_ref()) => Ok(()),
            MetadataSwap::Conflict(_) => Err(ObjectLogError::Sequencer(
                "immutable data publication conflicts".into(),
            )),
        }
    }
    pub async fn swap_head(
        &self,
        key: &str,
        observed: crate::MetadataSnapshot,
        raw: &[u8],
    ) -> Result<MetadataSwap, ObjectLogError> {
        self.require_operation()?;
        if observed.inner.key != key {
            return Err(ObjectLogError::InvalidConfig(
                "head literal key mismatch".into(),
            ));
        }
        let charge = self.budget.charge(
            self.store
                .page_source_scratch(key.len(), self.metadata_limits(raw.len()))?
                .checked_add(raw.len())
                .ok_or_else(|| ObjectLogError::BudgetExceeded("head allocation overflow".into()))?,
        )?;
        // Admission precedes the complete upload allocation. The adapter binds
        // the original observation to this exact provider and literal key.
        let source = Bytes::from_owner(UploadedPage {
            bytes: raw.to_vec(),
            _charge: charge.clone(),
        });
        self.store
            .swap_metadata(
                key,
                observed,
                source,
                self.metadata_limits(raw.len()),
                self.owners(charge as AppendOwner),
            )
            .await
    }
    fn metadata_limits(&self, payload_bytes: usize) -> MetadataLimits {
        MetadataLimits {
            payload_bytes,
            version_bytes: self.limits.version_bytes,
        }
    }
    #[cfg(test)]
    pub fn allocation(&self) -> usize {
        self.allocation
    }
    pub fn charge_output(&self, bytes: usize) -> Result<Arc<Charge>, ObjectLogError> {
        self.budget.charge(bytes)
    }
    fn owned(&self, page: Page, charge: Arc<Charge>) -> Result<OwnedPage, ObjectLogError> {
        // Field-ordered ownership exists before any fallible codec work.
        let owned = OwnedPage {
            page,
            store_identity: self.identity.clone(),
            _charge: charge,
        };
        owned.page.encode(self.scope)?;
        Ok(owned)
    }
    pub fn overflow(
        &self,
        start: u64,
        bytes: &[u8],
        next: Option<Descriptor>,
    ) -> Result<OwnedPage, ObjectLogError> {
        if bytes.is_empty() || bytes.len() > PAGE_BYTES - HEADER - DESCRIPTOR_BYTES {
            return Err(ObjectLogError::InvalidBatch(
                "bounded overflow length".into(),
            ));
        }
        let charge = self.budget.charge(self.allocation)?;
        self.owned(
            Page::Overflow {
                start,
                bytes: bytes.to_vec(),
                next,
            },
            charge,
        )
    }
    pub fn leaf(&self, entries: &[LeafEntry]) -> Result<OwnedPage, ObjectLogError> {
        if entries.is_empty() || entries.len() > LEAF_SLOTS {
            return Err(ObjectLogError::InvalidBatch("bounded leaf slots".into()));
        }
        let charge = self.budget.charge(self.allocation)?;
        let mut owned = Vec::with_capacity(LEAF_SLOTS);
        owned.extend_from_slice(entries);
        self.owned(Page::Leaf(owned), charge)
    }
    pub fn branch(&self, height: u8, children: &[Descriptor]) -> Result<OwnedPage, ObjectLogError> {
        if children.is_empty() || children.len() > BRANCH_SLOTS {
            return Err(ObjectLogError::InvalidBatch("bounded branch slots".into()));
        }
        let charge = self.budget.charge(self.allocation)?;
        let mut owned = Vec::with_capacity(BRANCH_SLOTS);
        owned.extend_from_slice(children);
        self.owned(
            Page::Branch {
                height,
                children: owned,
            },
            charge,
        )
    }
    fn key(&self, descriptor: Descriptor) -> String {
        use std::fmt::Write;
        let mut key = String::with_capacity(self.prefix.len() + 195);
        key.push_str(&self.prefix);
        for identity in [
            &self.scope.namespace,
            &self.scope.incarnation,
            &descriptor.hash,
        ] {
            key.push('/');
            for byte in identity {
                write!(&mut key, "{byte:02x}").expect("String formatting");
            }
        }
        key
    }
    pub async fn read(&self, descriptor: Descriptor) -> Result<OwnedPage, ObjectLogError> {
        descriptor.validate()?;
        let charge = self.budget.charge(self.allocation)?;
        let key = self.key(descriptor);
        let observed = self
            .store
            .read_metadata(
                &key,
                self.metadata_limits(descriptor.encoded_len as usize),
                self.owners(charge.clone() as AppendOwner),
            )
            .await?;
        let raw = observed
            .bytes()
            .ok_or_else(|| ObjectLogError::MissingObject("native immutable page".into()))?;
        let mut page = Page::decode(raw, descriptor, self.scope)?;
        match &mut page {
            Page::Leaf(entries) => entries.reserve_exact(LEAF_SLOTS - entries.len()),
            Page::Branch { children, .. } => children.reserve_exact(BRANCH_SLOTS - children.len()),
            Page::Overflow { .. } => (),
        }
        Ok(OwnedPage {
            page,
            store_identity: self.identity.clone(),
            _charge: charge,
        })
    }
    pub async fn write(&self, page: OwnedPage) -> Result<Descriptor, ObjectLogError> {
        if !Arc::ptr_eq(&page.store_identity, &self.identity) {
            return Err(ObjectLogError::InvalidConfig(
                "page belongs to another store binding".into(),
            ));
        }
        // Encode under the existing page reservation, retaining both capacities.
        let (raw, descriptor) = page.page.encode(self.scope)?;
        let key = self.key(descriptor);
        let source = Bytes::from_owner(UploadedPage {
            bytes: raw,
            _charge: page._charge.clone(),
        });
        let observed = self
            .store
            .read_metadata(
                &key,
                self.metadata_limits(descriptor.encoded_len as usize),
                self.owners(page._charge.clone() as AppendOwner),
            )
            .await?;
        if let Some(existing) = observed.bytes() {
            Page::decode(existing, descriptor, self.scope)?;
            if existing != source.as_ref() {
                return Err(ObjectLogError::Sequencer(
                    "immutable page bytes differ".into(),
                ));
            }
            return Ok(descriptor);
        }
        match self
            .store
            .swap_metadata(
                &key,
                observed,
                source.clone(),
                self.metadata_limits(descriptor.encoded_len as usize),
                self.owners(page._charge.clone() as AppendOwner),
            )
            .await?
        {
            MetadataSwap::Stored => (),
            MetadataSwap::Conflict(current) => {
                let existing = current
                    .bytes()
                    .ok_or_else(|| ObjectLogError::MissingObject("native immutable page".into()))?;
                Page::decode(existing, descriptor, self.scope)?;
                if existing != source.as_ref() {
                    return Err(ObjectLogError::Sequencer(
                        "immutable page CAS conflict differs".into(),
                    ));
                }
            }
        }
        Ok(descriptor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalBlobStore;

    fn scope() -> Scope {
        Scope {
            namespace: [1; 32],
            incarnation: [2; 32],
        }
    }

    #[tokio::test]
    async fn real_local_pages_keep_old_roots_and_fail_missing_or_corrupt() {
        let root = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalBlobStore::new(root.path()));
        let budget = ByteBudget::new(1 << 20).unwrap();
        let store = PageStore::new(
            local.clone(),
            "pages".into(),
            scope(),
            budget.clone(),
            crate::MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 56,
            },
        )
        .unwrap();
        let first = store
            .write(store.overflow(0, b"old/key", None).unwrap())
            .await
            .unwrap();
        let second = store
            .write(store.overflow(0, b"new/key", None).unwrap())
            .await
            .unwrap();
        assert_ne!(first.hash, second.hash);
        let old = store.read(first).await.unwrap();
        assert!(matches!(
            old.view(),
            PageView::Overflow {
                bytes: b"old/key",
                ..
            }
        ));
        assert!(budget.available() < 1 << 20);
        let mut new = store.read(second).await.unwrap();
        // Mutating decoded memory cannot alter a committed immutable page.
        if let Page::Overflow { bytes, .. } = &mut new.page {
            bytes[0] ^= 1;
        }
        drop(new);
        assert!(matches!(
            store.read(second).await.unwrap().view(),
            PageView::Overflow {
                bytes: b"new/key",
                ..
            }
        ));
        drop(old);
        assert_eq!(budget.available(), 1 << 20);
        let key = store.key(first);
        local
            .put(&key, Bytes::from_static(b"corrupt"))
            .await
            .unwrap();
        assert!(store.read(first).await.is_err());
        local.delete(&key).await.unwrap();
        assert!(matches!(
            store.read(first).await,
            Err(ObjectLogError::MissingObject(_))
        ));
        assert_eq!(budget.available(), 1 << 20);
    }

    #[tokio::test]
    async fn exact_charge_precedes_owned_allocation_and_io_and_is_store_bound() {
        let root = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalBlobStore::new(root.path()));
        let sizing = PageStore::new(
            local.clone(),
            "pages".into(),
            scope(),
            ByteBudget::new(1 << 20).unwrap(),
            crate::MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 56,
            },
        )
        .unwrap();
        let bytes = sizing.allocation();
        assert!(
            PageStore::new(
                local.clone(),
                "pages".into(),
                scope(),
                ByteBudget::new(bytes as u32 - 1).unwrap(),
                crate::MetadataLimits {
                    payload_bytes: PAGE_BYTES,
                    version_bytes: 56
                }
            )
            .is_err()
        );
        let budget = ByteBudget::new(bytes as u32).unwrap();
        let store = PageStore::new(
            local.clone(),
            "pages".into(),
            scope(),
            budget.clone(),
            crate::MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 56,
            },
        )
        .unwrap();
        let held = store.overflow(0, b"key", None).unwrap();
        assert_eq!(budget.available(), 0);
        assert!(store.overflow(0, b"key", None).is_err());
        assert!(local.list("pages").await.unwrap().is_empty());
        let foreign = PageStore::new(
            local.clone(),
            "foreign".into(),
            scope(),
            ByteBudget::new(1 << 20).unwrap(),
            crate::MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 56,
            },
        )
        .unwrap();
        assert!(foreign.write(held).await.is_err());
        assert_eq!(budget.available(), bytes);
        assert!(local.list("pages").await.unwrap().is_empty());
        let descriptor = store
            .write(store.overflow(0, b"key", None).unwrap())
            .await
            .unwrap();
        assert_eq!(budget.available(), bytes);
        let read = store.read(descriptor).await.unwrap();
        assert_eq!(budget.available(), 0);
        assert!(store.read(descriptor).await.is_err());
        drop(read);
        assert_eq!(budget.available(), bytes);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_same_page_writers_create_once_and_refuse_existing_corruption() {
        let root = tempfile::tempdir().unwrap();
        let first_local = Arc::new(LocalBlobStore::new(root.path()));
        let second_local = Arc::new(LocalBlobStore::new(root.path()));
        let budget = ByteBudget::new(1 << 20).unwrap();
        let first = PageStore::new(
            first_local.clone(),
            "pages".into(),
            scope(),
            budget.clone(),
            crate::MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 56,
            },
        )
        .unwrap();
        let second = PageStore::new(
            second_local.clone(),
            "pages".into(),
            scope(),
            budget.clone(),
            crate::MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 56,
            },
        )
        .unwrap();
        let (entered, release) = first_local.pause_metadata_swap(false);
        let writer = first.clone();
        let task = tokio::spawn(async move {
            writer
                .write(writer.overflow(0, b"same/key", None).unwrap())
                .await
        });
        tokio::task::spawn_blocking(move || {
            entered
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap()
        })
        .await
        .unwrap();
        let writer = second.clone();
        let other = tokio::spawn(async move {
            writer
                .write(writer.overflow(0, b"same/key", None).unwrap())
                .await
        });
        // Closing this channel on an assertion failure also releases the first CAS.
        release.send(()).unwrap();
        let first_descriptor = task.await.unwrap().unwrap();
        assert_eq!(other.await.unwrap().unwrap(), first_descriptor);
        assert!(matches!(
            second.read(first_descriptor).await.unwrap().view(),
            PageView::Overflow {
                bytes: b"same/key",
                ..
            }
        ));
        assert_eq!(budget.available(), 1 << 20);
        let key = first.key(first_descriptor);
        let original = first_local.get(&key).await.unwrap().unwrap();
        let mut corrupt = original.to_vec();
        *corrupt.last_mut().unwrap() ^= 1;
        first_local
            .put(&key, Bytes::from(corrupt.clone()))
            .await
            .unwrap();
        assert!(
            second
                .write(second.overflow(0, b"same/key", None).unwrap())
                .await
                .is_err()
        );
        assert_eq!(
            first_local.get(&key).await.unwrap().unwrap().as_ref(),
            corrupt
        );
        assert_eq!(budget.available(), 1 << 20);
        assert!(
            PageStore::new(
                first_local,
                "/absolute".into(),
                scope(),
                budget,
                crate::MetadataLimits {
                    payload_bytes: PAGE_BYTES,
                    version_bytes: 56
                }
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod density_tests {
    use super::*;
    use crate::native_v2::operation::{ExecutionConfig, Resources};

    #[tokio::test]
    async fn thousand_known_bindings_share_executor_and_byte_admission() {
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(crate::LocalBlobStore::new(directory.path()));
        let resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 1,
            bytes: 1 << 20,
        })
        .unwrap();
        let mut bindings = Vec::with_capacity(1000);
        for namespace in 1u64..=1000 {
            let mut identity = [0; 32];
            identity[..8].copy_from_slice(&namespace.to_be_bytes());
            bindings.push(
                PageStore::new(
                    local.clone(),
                    "pages".into(),
                    Scope {
                        namespace: identity,
                        incarnation: [2; 32],
                    },
                    resources.bytes.clone(),
                    MetadataLimits {
                        payload_bytes: PAGE_BYTES,
                        version_bytes: 56,
                    },
                )
                .unwrap(),
            );
        }
        assert!(local.list("pages").await.unwrap().is_empty());
        let budget = resources.bytes.clone();
        let (held, identity) = resources
            .run(move |physical| async move {
                let mut held = Vec::new();
                let mut rejected = false;
                for binding in &bindings {
                    assert!(Arc::ptr_eq(&binding.budget, &budget));
                    let scoped = binding.in_operation(physical.clone())?;
                    match scoped.overflow(0, b"physical/key", None) {
                        Ok(page) => held.push(page),
                        Err(ObjectLogError::BudgetExceeded(_)) => {
                            rejected = true;
                            break;
                        }
                        Err(error) => return Err(error),
                    }
                }
                assert!(
                    rejected,
                    "all namespaces must share a single finite byte pool"
                );
                assert!(!held.is_empty());
                let weak = Arc::downgrade(&physical);
                Ok((held, weak))
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
        assert!(identity.upgrade().is_none());
        assert!(resources.bytes.available() < 1 << 20);
        drop(held);
        assert_eq!(resources.bytes.available(), 1 << 20);
    }
}

#[cfg(test)]
mod data_namespace_tests {
    use super::*;
    use crate::native_v2::operation::{ExecutionConfig, Resources};

    #[tokio::test]
    async fn logical_aliases_and_long_ids_have_disjoint_canonical_data_objects() {
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(crate::LocalBlobStore::new(directory.path()));
        let resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 1,
            bytes: 1 << 20,
        })
        .unwrap();
        let pages = PageStore::new(
            local.clone(),
            "pages".into(),
            Scope {
                namespace: [1; 32],
                incarnation: [2; 32],
            },
            resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 56,
            },
        )
        .unwrap();
        resources
            .run(move |physical| async move {
                let pages = pages.in_operation(physical)?;
                let before = pages.budget.available();
                let owned = pages.data_key("reservation-control")?;
                let required = before - pages.budget.available();
                assert!(required >= owned.as_str().len());
                drop(owned);
                assert_eq!(pages.budget.available(), before);
                let pressure = pages.budget.charge(before - required + 1)?;
                assert!(pages.data_key("reservation-control").is_err());
                drop(pressure);
                let pressure = pages.budget.charge(before - required)?;
                let owned = pages.data_key("reservation-control")?;
                assert_eq!(pages.budget.available(), 0);
                drop(owned);
                assert_eq!(pages.budget.available(), required);
                drop(pressure);
                let identities = [
                    "head".to_owned(),
                    "head.olog-tmp".to_owned(),
                    "head.caslock".to_owned(),
                    "/absolute".to_owned(),
                    "../head".to_owned(),
                    "a//./b".to_owned(),
                    "x".repeat(64),
                    "x".repeat(300),
                ];
                let mut keys = Vec::new();
                for (index, logical) in identities.iter().enumerate() {
                    let key = pages.data_key(logical)?;
                    assert!(!key.as_str().starts_with('/'));
                    assert!(key.as_str().split('/').all(|part| !part.is_empty()
                        && part != "."
                        && part != ".."
                        && part.len() <= 128));
                    assert!(!key.as_str().contains('.'));
                    assert!(
                        !keys
                            .iter()
                            .any(|existing: &OwnedDataKey| existing.as_str() == key.as_str())
                    );
                    let frame =
                        super::super::data::DataFrame::encode(&pages, logical, &[index as u8])?;
                    pages.upload_data(key.as_str(), frame.bytes()).await?;
                    keys.push(key);
                }
                for (index, key) in keys.iter().enumerate() {
                    assert_eq!(
                        super::super::data::DataFrame::decode(
                            &local.get(key.as_str()).await?.unwrap()
                        )?
                        .payload,
                        &[index as u8]
                    );
                    assert!(pages.read_head(key.as_str(), 240).await.is_err());
                }
                assert!(local.get("head").await?.is_none());
                assert!(local.get("head.olog-tmp").await?.is_none());
                Ok(())
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
    }
}
