//! Each encoded or retained page owns independently admitted byte credit.
//! Carriers cannot retain an operation session or accept caller-owned capacity.
use super::*;
use sha2::{Digest, Sha256};

pub(in crate::native_v2) struct EncodedBytes {
    raw: Vec<u8>,
    hash: [u8; 32],
    binding: Arc<()>,
    _charge: Arc<Charge>,
}
impl EncodedBytes {
    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }
}

pub(in crate::native_v2) struct RetainedBytes {
    snapshot: crate::MetadataSnapshot,
}
impl RetainedBytes {
    pub fn bytes(&self) -> &[u8] {
        self.snapshot.bytes().expect("validated immutable page")
    }
}

impl PageStore {
    pub(in crate::native_v2) fn encode_scoped_bytes(
        &self,
        len: usize,
        encode: impl FnOnce(&mut [u8], Scope) -> Result<(), ObjectLogError>,
    ) -> Result<EncodedBytes, ObjectLogError> {
        self.require_operation()?;
        if !(72..=PAGE_BYTES).contains(&len) {
            return Err(ObjectLogError::InvalidBatch(
                "immutable encoded page length".into(),
            ));
        }
        let charge = self.budget.charge(self.allocation)?;
        let mut raw = vec![0; len];
        encode(&mut raw, self.scope)?;
        let hash = Sha256::digest(&raw).into();
        self.validate_immutable(&raw, &hash, len, &charge)?;
        Ok(EncodedBytes {
            raw,
            hash,
            binding: self.identity.clone(),
            _charge: charge,
        })
    }

    pub(in crate::native_v2) async fn write_scoped_bytes(
        &self,
        encoded: EncodedBytes,
    ) -> Result<[u8; 32], ObjectLogError> {
        self.require_operation()?;
        if !Arc::ptr_eq(&encoded.binding, &self.identity) {
            return Err(ObjectLogError::InvalidConfig(
                "encoded page belongs to another binding".into(),
            ));
        }
        self.write_immutable(encoded.raw, &encoded.hash, &encoded._charge)
            .await?;
        Ok(encoded.hash)
    }

    pub(in crate::native_v2) async fn read_scoped_bytes(
        &self,
        hash: &[u8; 32],
        len: usize,
    ) -> Result<RetainedBytes, ObjectLogError> {
        self.require_operation()?;
        if !(72..=PAGE_BYTES).contains(&len) {
            return Err(ObjectLogError::InvalidBatch(
                "immutable retained page length".into(),
            ));
        }
        let charge = self.budget.charge(self.allocation)?;
        let snapshot = self.read_immutable(hash, len, &charge).await?;
        Ok(RetainedBytes { snapshot })
    }
}
