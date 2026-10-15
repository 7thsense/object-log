//! Failed publications may leave immutable objects. A reprepare at a different
//! semantic transition must therefore use a different physical data object.
use super::*;
use sha2::{Digest, Sha256};

impl PageStore {
    pub(in crate::native_v2) fn transition_data_key(
        &self,
        certificate: &[u8; 32],
    ) -> Result<OwnedDataKey, ObjectLogError> {
        self.require_operation()?;
        if *certificate == [0; 32] {
            return Err(ObjectLogError::InvalidBatch(
                "coupled data certificate".into(),
            ));
        }
        // Two scoped hex components, the domain path, and one SHA-256 digest.
        let len =
            self.prefix.len().checked_add(208).ok_or_else(|| {
                ObjectLogError::BudgetExceeded("coupled data key overflow".into())
            })?;
        let charge =
            self.budget
                .charge(
                    len.checked_add(std::mem::size_of::<String>())
                        .ok_or_else(|| {
                            ObjectLogError::BudgetExceeded("coupled data key overflow".into())
                        })?,
                )?;
        // The certificate covers the complete versioned frame, including its
        // semantic transition, ordinal, literal identity and user payload.
        let mut digest = Sha256::new();
        digest.update(b"object-log/native-v2/coupled-data/v1\0");
        digest.update(certificate);
        let digest = digest.finalize();
        use std::fmt::Write;
        let mut key = String::with_capacity(len);
        key.push_str(&self.prefix);
        for identity in [&self.scope.namespace, &self.scope.incarnation] {
            key.push('/');
            for byte in identity {
                write!(&mut key, "{byte:02x}").expect("String formatting");
            }
        }
        key.push_str("/coupled-data/");
        for byte in digest {
            write!(&mut key, "{byte:02x}").expect("String formatting");
        }
        debug_assert_eq!(key.len(), len);
        Ok(OwnedDataKey {
            key,
            _charge: charge,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_v2::operation::{ExecutionConfig, Resources};

    #[tokio::test]
    async fn certificates_isolate_orphans_and_keys_keep_exact_credit() {
        let directory = tempfile::tempdir().unwrap();
        let resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 1,
            bytes: 1 << 20,
        })
        .unwrap();
        let pages = PageStore::new(
            Arc::new(crate::LocalBlobStore::new(directory.path())),
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
        assert!(pages.transition_data_key(&[1; 32]).is_err());
        let (held, weak) = resources
            .run(move |physical| async move {
                let weak = Arc::downgrade(&physical);
                let pages = pages.in_operation(physical)?;
                assert!(pages.transition_data_key(&[0; 32]).is_err());
                let before = pages.budget.available();
                let first = pages.transition_data_key(&[1; 32])?;
                let required = before - pages.budget.available();
                assert_eq!(
                    required,
                    first.as_str().len() + std::mem::size_of::<String>()
                );
                assert_eq!(
                    first.as_str(),
                    pages.transition_data_key(&[1; 32])?.as_str()
                );
                let second = pages.transition_data_key(&[2; 32])?;
                assert_ne!(first.as_str(), second.as_str());
                assert_ne!(first.as_str(), pages.data_key("same-logical-id")?.as_str());
                assert!(first.as_str().contains("/coupled-data/"));
                drop(second);
                drop(first);
                assert_eq!(pages.budget.available(), before);
                let pressure = pages.budget.charge(before - required + 1)?;
                assert!(pages.transition_data_key(&[1; 32]).is_err());
                drop(pressure);
                let pressure = pages.budget.charge(before - required)?;
                let held = pages.transition_data_key(&[1; 32])?;
                assert_eq!(pages.budget.available(), 0);
                drop(pressure);
                Ok((held, weak))
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
        assert!(weak.upgrade().is_none());
        assert!(resources.bytes.available() < 1 << 20);
        drop(held);
        assert_eq!(resources.bytes.available(), 1 << 20);
    }
}
