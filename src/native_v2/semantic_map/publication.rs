//! Internal capabilities connect authenticated captures to COW publication.
//! No caller can inject a resulting root descriptor into a prepared transition.
use super::{
    WorkBudget,
    edit::{self, Edit},
    manifest::{Manifest, Reference},
};
use crate::{ObjectLogError, native_v2::page_store::PageStore};
use std::sync::Arc;

pub(in crate::native_v2) struct CapturedMap {
    manifest: Option<Manifest>,
    binding: Arc<()>,
}
pub(in crate::native_v2) struct PreparedMap {
    before: Option<Reference>,
    after: Reference,
    binding: Arc<()>,
}
impl CapturedMap {
    /// The head layer may use this only for a genuinely empty Uninitialized cut.
    pub fn empty(pages: &PageStore) -> Result<Self, ObjectLogError> {
        pages.require_operation()?;
        Ok(Self {
            manifest: None,
            binding: pages.binding_identity(),
        })
    }
    pub async fn capture(
        pages: &PageStore,
        hash: [u8; 32],
        through: u64,
        work: &mut WorkBudget,
    ) -> Result<Self, ObjectLogError> {
        let manifest = Manifest::read_anchor(pages, hash, through, work).await?;
        if let Some(root) = manifest.root()? {
            drop(super::read::VerifiedNode::read(pages, root, None, None, work).await?);
        }
        Ok(Self {
            manifest: Some(manifest),
            binding: pages.binding_identity(),
        })
    }
    pub async fn range(
        &self,
        pages: &PageStore,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
        work: &mut WorkBudget,
    ) -> Result<super::range::Cursor, ObjectLogError> {
        if !Arc::ptr_eq(&self.binding, &pages.binding_identity()) {
            return Err(invalid("captured semantic map belongs to another binding"));
        }
        let root = self
            .manifest
            .as_ref()
            .map(Manifest::root)
            .transpose()?
            .flatten();
        super::range::Cursor::start(pages, root, lower, upper, work).await
    }
    pub async fn lookup(
        &self,
        pages: &PageStore,
        key: &[u8],
        work: &mut WorkBudget,
    ) -> Result<Option<super::bytes::OwnedBytes>, ObjectLogError> {
        if !Arc::ptr_eq(&self.binding, &pages.binding_identity()) {
            return Err(invalid("captured semantic map belongs to another binding"));
        }
        let root = self
            .manifest
            .as_ref()
            .map(Manifest::root)
            .transpose()?
            .flatten();
        super::read::lookup(pages, root, key, work).await
    }
    pub async fn prepare(
        self,
        pages: &PageStore,
        edits: &[Edit<'_>],
        limits: super::EditBounds,
        through: u64,
        entry_count: u64,
        work: &mut WorkBudget,
    ) -> Result<PreparedMap, ObjectLogError> {
        if !Arc::ptr_eq(&self.binding, &pages.binding_identity()) {
            return Err(invalid("captured semantic map belongs to another binding"));
        }
        let before = self.manifest.as_ref().map(Manifest::reference);
        let root = self
            .manifest
            .as_ref()
            .map(Manifest::root)
            .transpose()?
            .flatten();
        // Validate publication bounds before edit can create immutable objects.
        let start = before.map_or(0, Reference::through);
        if through <= start
            || through > i64::MAX as u64
            || entry_count == 0
            || entry_count > through - start
        {
            return Err(invalid("semantic publication group bounds"));
        }
        let root = edit::apply(pages, root, edits, limits.edits, limits.input_bytes, work).await?;
        let after = Manifest::build(pages, before, through, entry_count, root, work).await?;
        Ok(PreparedMap {
            before,
            after,
            binding: self.binding,
        })
    }
}
impl PreparedMap {
    pub async fn seal(
        &self,
        pages: &PageStore,
        index: crate::native_v2::codec::Descriptor,
        work: &mut WorkBudget,
    ) -> Result<[u8; 32], ObjectLogError> {
        self.validate_binding(pages)?;
        super::seal::build(pages, self.after_hash(), self.through(), index, work).await
    }

    pub fn before_hash(&self) -> [u8; 32] {
        self.before.map_or([0; 32], Reference::hash)
    }
    pub fn after_hash(&self) -> [u8; 32] {
        self.after.hash()
    }
    pub fn through(&self) -> u64 {
        self.after.through()
    }
    pub fn generation(&self) -> u64 {
        self.after.generation()
    }
    pub fn validate_binding(&self, pages: &PageStore) -> Result<(), ObjectLogError> {
        pages.require_operation()?;
        if !Arc::ptr_eq(&self.binding, &pages.binding_identity()) {
            return Err(invalid("prepared semantic map belongs to another binding"));
        }
        Ok(())
    }
}
fn invalid(message: &str) -> ObjectLogError {
    ObjectLogError::InvalidBatch(message.into())
}

/// Authenticate both anchors and the exact canonical manifest ancestry. The
/// head layer separately proves the append-index prefix and original emptiness.
pub(in crate::native_v2) async fn proves_head_extension(
    pages: &PageStore,
    before: Option<([u8; 32], u64)>,
    after: Option<([u8; 32], u64)>,
    original_empty: bool,
    work: &mut WorkBudget,
) -> Result<bool, ObjectLogError> {
    let Some((after_hash, after_through)) = after else {
        return Ok(before.is_none());
    };
    let after = Manifest::read_anchor(pages, after_hash, after_through, work).await?;
    if let Some(root) = after.root()? {
        work.page()?;
        drop(super::codec::NodePage::read(pages, root).await?);
    }
    let Some((before_hash, before_through)) = before else {
        // A cold legacy queue cannot acquire coverage by trusting a checkpoint.
        return if original_empty {
            Manifest::proves_origin(pages, after.reference(), work).await
        } else {
            Ok(false)
        };
    };
    let before = Manifest::read_anchor(pages, before_hash, before_through, work).await?;
    if let Some(root) = before.root()? {
        work.page()?;
        drop(super::codec::NodePage::read(pages, root).await?);
    }
    Manifest::proves_extension(pages, before.reference(), after.reference(), work).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BlobStore, LocalBlobStore,
        native_v2::semantic_map::{
            read,
            test_support::{fixture, resources},
        },
    };
    #[tokio::test]
    async fn edit_only_preparation_preserves_original_empty_and_noop_roots() {
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalBlobStore::new(directory.path()));
        let resources = resources();
        let pages = fixture(local.clone(), &resources);
        let foreign = fixture(local.clone(), &resources);
        let (retained, weak) = resources
            .run(move |physical| async move {
                let weak = Arc::downgrade(&physical);
                let pages = pages.in_operation(physical.clone())?;
                let foreign = foreign.in_operation(physical)?;
                let mut work = WorkBudget::new(1000, 10000);
                let objects = local.list("map").await?;
                for (through, count) in [(0, 1), (1, 2)] {
                    assert!(
                        CapturedMap::empty(&pages)?
                            .prepare(
                                &pages,
                                &[Edit {
                                    key: b"would-write",
                                    value: Some(b"payload")
                                }],
                                super::super::EditBounds {
                                    edits: 1,
                                    input_bytes: 100
                                },
                                through,
                                count,
                                &mut work
                            )
                            .await
                            .is_err()
                    );
                    assert_eq!(local.list("map").await?, objects);
                }
                let first = CapturedMap::empty(&pages)?
                    .prepare(
                        &pages,
                        &[
                            Edit {
                                key: b"a",
                                value: Some(b"one"),
                            },
                            Edit {
                                key: b"b",
                                value: Some(b"two"),
                            },
                        ],
                        super::super::EditBounds {
                            edits: 2,
                            input_bytes: 100,
                        },
                        3,
                        2,
                        &mut work,
                    )
                    .await?;
                first.validate_binding(&pages)?;
                assert!(first.validate_binding(&foreign).is_err());
                assert_eq!(first.before_hash(), [0; 32]);
                assert_eq!(first.generation(), 1);
                let before =
                    Manifest::read_anchor(&pages, first.after_hash(), first.through(), &mut work)
                        .await?;
                let root = before.root()?.unwrap();
                let suffix = root
                    .hash
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>();
                let key = local
                    .list("map")
                    .await?
                    .into_iter()
                    .find(|key| key.ends_with(&suffix))
                    .unwrap();
                let original = local.get(&key).await?.unwrap();
                local.delete(&key).await?;
                assert!(
                    CapturedMap::capture(&pages, first.after_hash(), 3, &mut work)
                        .await
                        .is_err()
                );
                let mut corrupt = original.to_vec();
                corrupt[0] ^= 1;
                local.put(&key, corrupt.into()).await?;
                assert!(
                    CapturedMap::capture(&pages, first.after_hash(), 3, &mut work)
                        .await
                        .is_err()
                );
                local.put(&key, original).await?;
                let second = CapturedMap::capture(&pages, first.after_hash(), 3, &mut work)
                    .await?
                    .prepare(
                        &pages,
                        &[
                            Edit {
                                key: b"a",
                                value: None,
                            },
                            Edit {
                                key: b"c",
                                value: Some(b"three"),
                            },
                        ],
                        super::super::EditBounds {
                            edits: 2,
                            input_bytes: 100,
                        },
                        7,
                        1,
                        &mut work,
                    )
                    .await?;
                assert_eq!(second.before_hash(), first.after_hash());
                assert_eq!(second.generation(), 2);
                let after =
                    Manifest::read_anchor(&pages, second.after_hash(), 7, &mut work).await?;
                assert_eq!(
                    read::lookup(&pages, before.root()?, b"a", &mut work)
                        .await?
                        .unwrap()
                        .as_slice(),
                    b"one"
                );
                assert!(
                    read::lookup(&pages, after.root()?, b"a", &mut work)
                        .await?
                        .is_none()
                );
                assert_eq!(
                    read::lookup(&pages, after.root()?, b"c", &mut work)
                        .await?
                        .unwrap()
                        .as_slice(),
                    b"three"
                );
                let third = CapturedMap::capture(&pages, second.after_hash(), 7, &mut work)
                    .await?
                    .prepare(
                        &pages,
                        &[],
                        super::super::EditBounds {
                            edits: 0,
                            input_bytes: 0,
                        },
                        8,
                        1,
                        &mut work,
                    )
                    .await?;
                let noop = Manifest::read_anchor(&pages, third.after_hash(), 8, &mut work).await?;
                assert_eq!(noop.root()?, after.root()?);
                assert_ne!(third.after_hash(), second.after_hash());
                assert!(
                    Manifest::proves_extension(
                        &pages,
                        before.reference(),
                        noop.reference(),
                        &mut work
                    )
                    .await?
                );
                let foreign_capture =
                    CapturedMap::capture(&foreign, third.after_hash(), 8, &mut work).await?;
                assert!(
                    foreign_capture
                        .prepare(
                            &pages,
                            &[],
                            super::super::EditBounds {
                                edits: 0,
                                input_bytes: 0
                            },
                            9,
                            1,
                            &mut work
                        )
                        .await
                        .is_err()
                );
                let empty = CapturedMap::capture(&pages, third.after_hash(), 8, &mut work)
                    .await?
                    .prepare(
                        &pages,
                        &[
                            Edit {
                                key: b"b",
                                value: None,
                            },
                            Edit {
                                key: b"c",
                                value: None,
                            },
                        ],
                        super::super::EditBounds {
                            edits: 2,
                            input_bytes: 2,
                        },
                        9,
                        1,
                        &mut work,
                    )
                    .await?;
                assert!(
                    Manifest::read_anchor(&pages, empty.after_hash(), 9, &mut work)
                        .await?
                        .root()?
                        .is_none()
                );
                assert_eq!(empty.generation(), 4);
                let retained =
                    CapturedMap::capture(&pages, empty.after_hash(), 9, &mut work).await?;
                Ok((retained, weak))
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
        assert!(weak.upgrade().is_none());
        assert!(resources.bytes.available() < 4 << 20);
        drop(retained);
        assert_eq!(resources.bytes.available(), 4 << 20);
    }
}
