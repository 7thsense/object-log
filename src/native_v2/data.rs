//! Exact immutable data framing; physical name hashes never establish identity.
use super::page_store::{Charge, PageStore};
use crate::ObjectLogError;
use std::sync::Arc;
const MAGIC: &[u8; 8] = b"OND2\0\0\0\x01";
const HEADER: usize = 24;
mod coupled;
pub(super) use coupled::Transition;
pub(super) struct DataFrame {
    bytes: Vec<u8>,
    _charge: Arc<Charge>,
}
pub(super) struct DataView<'a> {
    raw: &'a [u8],
    identity: &'a str,
    payload: &'a [u8],
    byte_start: u32,
    transition: Option<Transition>,
}
impl<'a> DataView<'a> {
    pub fn identity(&self) -> &'a str {
        self.identity
    }
    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }
    pub fn byte_start(&self) -> u32 {
        self.byte_start
    }
    pub fn transition(&self) -> Option<Transition> {
        self.transition
    }
}
impl DataFrame {
    pub fn encoded_len(identity: &str, payload_bytes: usize) -> Result<usize, ObjectLogError> {
        if identity.is_empty() || identity.contains('\0') {
            return Err(invalid());
        }
        let start = HEADER.checked_add(identity.len()).ok_or_else(invalid)?;
        u32::try_from(start).map_err(|_| invalid())?;
        u32::try_from(payload_bytes).map_err(|_| invalid())?;
        u64::try_from(identity.len()).map_err(|_| invalid())?;
        u64::try_from(payload_bytes).map_err(|_| invalid())?;
        start.checked_add(payload_bytes).ok_or_else(invalid)
    }

    pub fn charge_frame(pages: &PageStore, length: usize) -> Result<Arc<Charge>, ObjectLogError> {
        pages.validate_data_limit(length)?;
        pages.charge_output(
            length
                .checked_add(std::mem::size_of::<Vec<u8>>())
                .ok_or_else(invalid)?,
        )
    }

    pub fn encode(
        pages: &PageStore,
        identity: &str,
        payload: &[u8],
    ) -> Result<Self, ObjectLogError> {
        if identity.is_empty() || identity.contains('\0') {
            return Err(invalid());
        }
        let start = HEADER.checked_add(identity.len()).ok_or_else(invalid)?;
        let length = Self::encoded_len(identity, payload.len())?;
        u32::try_from(start).map_err(|_| invalid())?;
        u32::try_from(payload.len()).map_err(|_| invalid())?;
        let identity_len = u64::try_from(identity.len()).map_err(|_| invalid())?;
        let payload_len = u64::try_from(payload.len()).map_err(|_| invalid())?;
        let charge = Self::charge_frame(pages, length)?;
        let mut bytes = Vec::with_capacity(length);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&identity_len.to_be_bytes());
        bytes.extend_from_slice(&payload_len.to_be_bytes());
        bytes.extend_from_slice(identity.as_bytes());
        bytes.extend_from_slice(payload);
        Ok(Self {
            bytes,
            _charge: charge,
        })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn decode(raw: &[u8]) -> Result<DataView<'_>, ObjectLogError> {
        if raw.get(..8) == Some(coupled::MAGIC.as_slice()) {
            return coupled::decode(raw);
        }
        if raw.len() < HEADER || &raw[..8] != MAGIC {
            return Err(invalid());
        }
        let identity_len = usize::try_from(u64::from_be_bytes(raw[8..16].try_into().unwrap()))
            .map_err(|_| invalid())?;
        let payload_len = usize::try_from(u64::from_be_bytes(raw[16..24].try_into().unwrap()))
            .map_err(|_| invalid())?;
        let start = HEADER.checked_add(identity_len).ok_or_else(invalid)?;
        if start.checked_add(payload_len) != Some(raw.len()) {
            return Err(invalid());
        }
        let byte_start = u32::try_from(start).map_err(|_| invalid())?;
        u32::try_from(payload_len).map_err(|_| invalid())?;
        let identity = std::str::from_utf8(&raw[HEADER..start]).map_err(|_| invalid())?;
        if identity.is_empty() || identity.contains('\0') {
            return Err(invalid());
        }
        Ok(DataView {
            raw,
            identity,
            payload: &raw[start..],
            byte_start,
            transition: None,
        })
    }
}
fn invalid() -> ObjectLogError {
    ObjectLogError::InvalidBatch("native data framing".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "s3")]
    use crate::BlobStore;
    use crate::native_v2::{
        codec::Scope,
        operation::{ExecutionConfig, Resources},
    };
    use crate::{LocalBlobStore, MetadataLimits};

    #[tokio::test]
    async fn full_literal_collision_corruption_and_zero_payload_use_actual_local_objects() {
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalBlobStore::new(directory.path()));
        exercise_frames(
            local,
            Arc::new(LocalBlobStore::new(directory.path())),
            "pages".into(),
            56,
        )
        .await;
    }
    async fn exercise_frames(
        local: Arc<dyn crate::native_v2::page_store::NativeStore>,
        reopened: Arc<dyn crate::native_v2::page_store::NativeStore>,
        prefix: String,
        version_bytes: usize,
    ) {
        let resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 1,
            bytes: 1 << 20,
        })
        .unwrap();
        let pages = PageStore::new(
            local.clone(),
            prefix,
            Scope {
                namespace: [1; 32],
                incarnation: [2; 32],
            },
            resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: 32768,
                version_bytes,
            },
        )
        .unwrap();
        resources
            .run(move |physical| async move {
                let pages = pages.in_operation(physical)?;
                let logical = "€".repeat(4000);
                let key = pages.data_key(&logical)?;
                assert!(key.as_str().len() < 1024);
                let original = DataFrame::encode(&pages, &logical, b"payload")?;
                pages.upload_data(key.as_str(), original.bytes()).await?;
                let stored = local.get(key.as_str()).await?.unwrap();
                assert_eq!(
                    reopened.get(key.as_str()).await?.unwrap().as_ref(),
                    stored.as_ref()
                );
                let decoded = DataFrame::decode(&stored)?;
                assert_eq!(decoded.identity, logical);
                assert_eq!(decoded.payload, b"payload");
                assert_eq!(
                    local
                        .get_range(
                            key.as_str(),
                            u64::from(decoded.byte_start)..u64::from(decoded.byte_start) + 7
                        )
                        .await?
                        .unwrap()
                        .as_ref(),
                    b"payload"
                );
                let collision = DataFrame::encode(&pages, &"あ".repeat(4000), b"payload")?;
                assert!(
                    pages
                        .upload_data(key.as_str(), collision.bytes())
                        .await
                        .is_err()
                );
                assert_eq!(
                    local.get(key.as_str()).await?.unwrap().as_ref(),
                    original.bytes()
                );
                for bad in [
                    original.bytes()[..original.bytes().len() - 1].to_vec(),
                    {
                        let mut extra = original.bytes().to_vec();
                        extra.push(0);
                        extra
                    },
                    {
                        let mut wrong = original.bytes().to_vec();
                        wrong[8..16].copy_from_slice(&u64::MAX.to_be_bytes());
                        wrong
                    },
                ] {
                    assert!(DataFrame::decode(&bad).is_err());
                }
                local
                    .put(key.as_str(), bytes::Bytes::from_static(b"corrupt"))
                    .await?;
                assert!(
                    pages
                        .upload_data(key.as_str(), original.bytes())
                        .await
                        .is_err()
                );
                assert_eq!(local.get(key.as_str()).await?.unwrap().as_ref(), b"corrupt");
                let empty = DataFrame::encode(&pages, "empty payload", b"")?;
                let empty_key = pages.data_key("empty payload")?;
                pages.upload_data(empty_key.as_str(), empty.bytes()).await?;
                assert!(
                    DataFrame::decode(&local.get(empty_key.as_str()).await?.unwrap())?
                        .payload
                        .is_empty()
                );
                Ok(())
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
        assert_eq!(resources.bytes.available(), 1 << 20);
    }
    #[cfg(feature = "s3")]
    #[tokio::test]
    #[ignore = "requires configured S3 provider; mandatory native-v2 qualification lane"]
    async fn real_s3_runs_identical_frame_contract_in_both_signing_modes() {
        let required = |name: &str| {
            std::env::var(name)
                .expect("native-v2 S3 frame qualification requires complete configuration")
        };
        let endpoint = required("OBJECT_LOG_S3_ENDPOINT");
        let bucket = required("OBJECT_LOG_S3_BUCKET");
        let region = required("OBJECT_LOG_S3_REGION");
        let key = required("OBJECT_LOG_S3_KEY_ID");
        let secret = required("OBJECT_LOG_S3_SECRET");
        for unsigned in [false, true] {
            let store = Arc::new(
                crate::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret)
                    .with_payload_signing_disabled(unsigned),
            );
            let reopened = Arc::new(
                crate::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret)
                    .with_payload_signing_disabled(unsigned),
            );
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let prefix = format!("native-v2-frame-qualification-{nonce}-{unsigned}");
            exercise_frames(store.clone(), reopened, prefix.clone(), 4096).await;
            let store = crate::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret)
                .with_payload_signing_disabled(unsigned);
            for object in store.list(&prefix).await.unwrap() {
                store.delete(&object).await.unwrap();
            }
        }
    }
}
