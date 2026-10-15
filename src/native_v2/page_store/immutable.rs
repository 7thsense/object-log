//! Scoped immutable bytes shared by page codecs.
//! Outer callers retain operation admission and independently reserved pages.
use super::*;

impl PageStore {
    pub(super) fn immutable_key(&self, hash: &[u8; 32]) -> String {
        use std::fmt::Write;
        let mut key = String::with_capacity(self.prefix.len() + 195);
        key.push_str(&self.prefix);
        for identity in [&self.scope.namespace, &self.scope.incarnation, hash] {
            key.push('/');
            for byte in identity {
                write!(&mut key, "{byte:02x}").expect("String formatting");
            }
        }
        key
    }
    #[cfg(test)]
    pub(super) fn key(&self, descriptor: Descriptor) -> String {
        self.immutable_key(&descriptor.hash)
    }
    // Codec-independent immutable storage. Callers own the operation admission
    // and a page reservation covering encoded, decoded and provider buffers.
    // Each codec still validates its own version, shape and child descriptors.
    pub(super) fn validate_immutable(
        &self,
        raw: &[u8],
        hash: &[u8; 32],
        len: usize,
        charge: &Arc<Charge>,
    ) -> Result<(), ObjectLogError> {
        use sha2::{Digest, Sha256};
        if !Arc::ptr_eq(&charge.budget, &self.budget)
            || charge.count < self.allocation as u64
            || !(72..=PAGE_BYTES).contains(&len)
            || raw.len() != len
            || raw[8..40] != self.scope.namespace
            || raw[40..72] != self.scope.incarnation
            || Sha256::digest(raw).as_slice() != hash
        {
            return Err(ObjectLogError::Sequencer(
                "immutable page hash, scope, length or reservation differs".into(),
            ));
        }
        Ok(())
    }
    pub(super) async fn read_immutable(
        &self,
        hash: &[u8; 32],
        len: usize,
        charge: &Arc<Charge>,
    ) -> Result<crate::MetadataSnapshot, ObjectLogError> {
        if !(72..=PAGE_BYTES).contains(&len)
            || !Arc::ptr_eq(&charge.budget, &self.budget)
            || charge.count < self.allocation as u64
        {
            return Err(ObjectLogError::InvalidConfig(
                "immutable page length or reservation".into(),
            ));
        }
        let observed = self
            .store
            .read_metadata(
                &self.immutable_key(hash),
                self.metadata_limits(len),
                self.owners(charge.clone() as AppendOwner),
            )
            .await?;
        let raw = observed
            .bytes()
            .ok_or_else(|| ObjectLogError::MissingObject("native immutable page".into()))?;
        self.validate_immutable(raw, hash, len, charge)?;
        Ok(observed)
    }
    pub(super) async fn write_immutable(
        &self,
        raw: Vec<u8>,
        hash: &[u8; 32],
        charge: &Arc<Charge>,
    ) -> Result<(), ObjectLogError> {
        if raw.capacity() > PAGE_BYTES {
            return Err(ObjectLogError::BudgetExceeded(
                "immutable encoded page capacity".into(),
            ));
        }
        self.validate_immutable(&raw, hash, raw.len(), charge)?;
        let len = raw.len();
        let key = self.immutable_key(hash);
        let source = Bytes::from_owner(UploadedPage {
            bytes: raw,
            _charge: charge.clone(),
        });
        let observed = self
            .store
            .read_metadata(
                &key,
                self.metadata_limits(len),
                self.owners(charge.clone() as AppendOwner),
            )
            .await?;
        if let Some(existing) = observed.bytes() {
            self.validate_immutable(existing, hash, len, charge)?;
            if existing != source.as_ref() {
                return Err(ObjectLogError::Sequencer(
                    "immutable page bytes differ".into(),
                ));
            }
            return Ok(());
        }
        if let MetadataSwap::Conflict(current) = self
            .store
            .swap_metadata(
                &key,
                observed,
                source.clone(),
                self.metadata_limits(len),
                self.owners(charge.clone() as AppendOwner),
            )
            .await?
        {
            let existing = current
                .bytes()
                .ok_or_else(|| ObjectLogError::MissingObject("native immutable page".into()))?;
            self.validate_immutable(existing, hash, len, charge)?;
            if existing != source.as_ref() {
                return Err(ObjectLogError::Sequencer(
                    "immutable page CAS conflict differs".into(),
                ));
            }
        }
        Ok(())
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
    async fn codec_neutral_pages_require_exact_scope_hash_length_and_credit() {
        use sha2::{Digest, Sha256};
        let root = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalBlobStore::new(root.path()));
        let budget = ByteBudget::new(1 << 20).unwrap();
        let pages = PageStore::new(
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
        let charge = budget.charge(pages.allocation()).unwrap();
        let mut raw = vec![0; 76];
        raw[..8].copy_from_slice(b"MAPTEST1");
        raw[8..40].copy_from_slice(&scope().namespace);
        raw[40..72].copy_from_slice(&scope().incarnation);
        raw[72..].copy_from_slice(&[0, 255, 0, 1]);
        let hash: [u8; 32] = Sha256::digest(&raw).into();
        pages
            .write_immutable(raw.clone(), &hash, &charge)
            .await
            .unwrap();
        pages
            .write_immutable(raw.clone(), &hash, &charge)
            .await
            .unwrap();
        let observed = pages
            .read_immutable(&hash, raw.len(), &charge)
            .await
            .unwrap();
        assert_eq!(observed.bytes(), Some(raw.as_slice()));
        assert!(
            pages
                .read_immutable(&hash, raw.len() - 1, &charge)
                .await
                .is_err()
        );
        assert!(
            pages
                .read_immutable(&hash, PAGE_BYTES + 1, &charge)
                .await
                .is_err()
        );
        let foreign = ByteBudget::new(1 << 20)
            .unwrap()
            .charge(pages.allocation())
            .unwrap();
        assert!(
            pages
                .read_immutable(&hash, raw.len(), &foreign)
                .await
                .is_err()
        );
        let short = budget.charge(pages.allocation() - 1).unwrap();
        assert!(
            pages
                .read_immutable(&hash, raw.len(), &short)
                .await
                .is_err()
        );
        let mut wrong_scope = raw.clone();
        wrong_scope[8] ^= 1;
        let wrong_hash = Sha256::digest(&wrong_scope).into();
        assert!(
            pages
                .write_immutable(wrong_scope, &wrong_hash, &charge)
                .await
                .is_err()
        );
        let mut wrong_content = raw.clone();
        wrong_content[75] ^= 1;
        assert!(
            pages
                .write_immutable(wrong_content, &hash, &charge)
                .await
                .is_err()
        );
        let mut oversized_capacity = Vec::with_capacity(PAGE_BYTES + 1);
        oversized_capacity.extend_from_slice(&raw);
        assert!(
            pages
                .write_immutable(oversized_capacity, &hash, &charge)
                .await
                .is_err()
        );
        assert_eq!(local.list("pages").await.unwrap().len(), 1);
        drop(observed);
        drop(short);
        drop(charge);
        assert_eq!(budget.available(), 1 << 20);
        let charge = budget.charge(pages.allocation()).unwrap();
        let mut corrupted = raw.clone();
        corrupted[75] ^= 1;
        local
            .put(&pages.immutable_key(&hash), Bytes::from(corrupted))
            .await
            .unwrap();
        assert!(
            pages
                .read_immutable(&hash, raw.len(), &charge)
                .await
                .is_err()
        );
        local
            .put(&pages.immutable_key(&hash), Bytes::from_static(b"broken"))
            .await
            .unwrap();
        assert!(
            pages
                .read_immutable(&hash, raw.len(), &charge)
                .await
                .is_err()
        );
        local.delete(&pages.immutable_key(&hash)).await.unwrap();
        assert!(matches!(
            pages.read_immutable(&hash, raw.len(), &charge).await,
            Err(ObjectLogError::MissingObject(_))
        ));
        drop(charge);
        assert_eq!(budget.available(), 1 << 20);
    }
}
