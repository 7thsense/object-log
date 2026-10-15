//! Final acyclic commitment couples the already-authored semantic manifest and
//! complete append index. Frames name the manifest; HEAD names this seal.
use super::WorkBudget;
use crate::{
    ObjectLogError,
    native_v2::{
        codec::{Descriptor, Kind},
        page_store::PageStore,
    },
};
const MAGIC: &[u8; 8] = b"ONC2\0\0\0\x01";
const BYTES: usize = 192;

fn validate(manifest: [u8; 32], through: u64, index: Descriptor) -> Result<(), ObjectLogError> {
    index.validate()?;
    if manifest == [0; 32]
        || through == 0
        || through > i64::MAX as u64
        || index.kind == Kind::Overflow
        || index.start != 0
        || index.end != through
    {
        return Err(invalid());
    }
    Ok(())
}
pub(super) async fn build(
    pages: &PageStore,
    manifest: [u8; 32],
    through: u64,
    index: Descriptor,
    work: &mut WorkBudget,
) -> Result<[u8; 32], ObjectLogError> {
    validate(manifest, through, index)?;
    work.page()?;
    let encoded = pages.encode_scoped_bytes(BYTES, |raw, scope| {
        raw[..8].copy_from_slice(MAGIC);
        raw[8..40].copy_from_slice(&scope.namespace);
        raw[40..72].copy_from_slice(&scope.incarnation);
        raw[72..104].copy_from_slice(&manifest);
        index.encode_into(&mut raw[104..168]);
        raw[168..176].copy_from_slice(&through.to_be_bytes());
        Ok(())
    })?;
    pages.write_scoped_bytes(encoded).await
}
pub(in crate::native_v2) async fn read(
    pages: &PageStore,
    hash: [u8; 32],
    through: u64,
    index: Descriptor,
    work: &mut WorkBudget,
) -> Result<([u8; 32], u64), ObjectLogError> {
    if hash == [0; 32] {
        return Err(invalid());
    }
    work.page()?;
    let retained = pages.read_scoped_bytes(&hash, BYTES).await?;
    let raw = retained.bytes();
    let scope = pages.scope();
    let manifest = raw[72..104].try_into().unwrap();
    let committed_index = Descriptor::decode(&raw[104..168])?;
    let committed_through = u64::from_be_bytes(raw[168..176].try_into().unwrap());
    if &raw[..8] != MAGIC
        || raw[8..40] != scope.namespace
        || raw[40..72] != scope.incarnation
        || committed_index != index
        || committed_through != through
        || raw[176..].iter().any(|b| *b != 0)
    {
        return Err(invalid());
    }
    validate(manifest, through, index)?;
    Ok((manifest, through))
}
fn invalid() -> ObjectLogError {
    ObjectLogError::InvalidBatch("coupled publication seal".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BlobStore, LocalBlobStore,
        native_v2::semantic_map::test_support::{fixture, resources},
    };
    use std::sync::Arc;
    #[tokio::test]
    async fn seal_binds_complete_index_and_rejects_recommitted_framing() {
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalBlobStore::new(directory.path()));
        let resources = resources();
        let pages = fixture(local.clone(), &resources);
        resources
            .run(move |physical| async move {
                let pages = pages.in_operation(physical)?;
                let index = Descriptor {
                    hash: [9; 32],
                    encoded_len: 304,
                    kind: Kind::Leaf,
                    height: 0,
                    start: 0,
                    end: 3,
                    entries: 1,
                };
                let before = local.list("map").await?;
                assert!(matches!(
                    build(&pages, [8; 32], 3, index, &mut WorkBudget::new(0, 0)).await,
                    Err(ObjectLogError::BudgetExceeded(_))
                ));
                assert_eq!(local.list("map").await?, before);
                let hash = build(&pages, [8; 32], 3, index, &mut WorkBudget::new(1, 0)).await?;
                assert_eq!(
                    read(&pages, hash, 3, index, &mut WorkBudget::new(1, 0)).await?,
                    ([8; 32], 3)
                );
                assert!(matches!(
                    read(&pages, hash, 3, index, &mut WorkBudget::new(0, 0)).await,
                    Err(ObjectLogError::BudgetExceeded(_))
                ));
                assert!(
                    read(
                        &pages,
                        hash,
                        3,
                        Descriptor {
                            hash: [10; 32],
                            ..index
                        },
                        &mut WorkBudget::new(1, 0)
                    )
                    .await
                    .is_err()
                );
                assert!(
                    read(&pages, hash, 2, index, &mut WorkBudget::new(1, 0))
                        .await
                        .is_err()
                );
                let source = pages.read_scoped_bytes(&hash, BYTES).await?;
                for offset in [0, 72, 104, 136, 140, 144, 152, 160, 168, 176, BYTES - 1] {
                    let encoded = pages.encode_scoped_bytes(BYTES, |raw, _| {
                        raw.copy_from_slice(source.bytes());
                        raw[offset] ^= 1;
                        Ok(())
                    })?;
                    let changed = pages.write_scoped_bytes(encoded).await?;
                    let result = read(&pages, changed, 3, index, &mut WorkBudget::new(1, 0)).await;
                    // The manifest hash itself is opaque here; its source is checked
                    // by semantic ancestry/capture after the seal is decoded.
                    if offset == 72 {
                        assert_eq!(result?.1, 3);
                    } else {
                        assert!(result.is_err(), "offset {offset}");
                    }
                }
                assert!(
                    read(&pages, [199; 32], 3, index, &mut WorkBudget::new(1, 0))
                        .await
                        .is_err()
                );
                Ok(())
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
        assert_eq!(resources.bytes.available(), 4 << 20);
    }
}
