//! Append-only right-spine COW. A descriptor is used only after decoding its
//! complete page relative to the captured authority. No catalog/index scan.
use super::codec::{
    BRANCH_SLOTS, DESCRIPTOR_BYTES, Descriptor, HEADER, Kind, LEAF_SLOTS, LeafEntry, MAX_HEIGHT,
    PAGE_BYTES,
};
use super::page_store::{Charge, OwnedPage, PageStore, PageView};
use crate::ObjectLogError;
use std::sync::Arc;

pub(super) struct Index {
    pages: Arc<PageStore>,
}
pub(super) struct OwnedEntry {
    page: OwnedPage,
    slot: usize,
}
impl OwnedEntry {
    pub fn entry(&self) -> &LeafEntry {
        let PageView::Leaf(entries) = self.page.view() else {
            unreachable!("validated leaf")
        };
        &entries[self.slot]
    }
}
pub(super) struct OwnedObjectId {
    value: String,
    _charge: Arc<Charge>,
}
impl OwnedObjectId {
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl Index {
    pub fn new(pages: Arc<PageStore>) -> Self {
        Self { pages }
    }

    /// Prove the canonical right-spine COW extension without scanning/repacking.
    /// Other valid layouts are not an extension proof. At most two pages are
    /// retained per iteration; exact descriptors skip complete old subtrees.
    pub async fn proves_extension(
        &self,
        original: Descriptor,
        current: Descriptor,
    ) -> Result<bool, ObjectLogError> {
        self.pages.require_operation()?;
        original.validate()?;
        current.validate()?;
        if original.kind == Kind::Overflow
            || current.kind == Kind::Overflow
            || original.start != 0
            || current.start != 0
            || current.end < original.end
        {
            return Ok(false);
        }
        let (mut old, mut new) = (original, current);
        for _ in 0..=MAX_HEIGHT * 2 {
            if old == new {
                return Ok(true);
            }
            if new.height < old.height || old.start != new.start || new.end < old.end {
                return Ok(false);
            }
            let new_page = self.pages.read(new).await?;
            if new.height > old.height {
                let PageView::Branch { children, .. } = new_page.view() else {
                    return Ok(false);
                };
                let first = children[0];
                if first.start != old.start || first.end < old.end {
                    return Ok(false);
                }
                new = first;
                continue;
            }
            let old_page = self.pages.read(old).await?;
            match (old_page.view(), new_page.view()) {
                (PageView::Leaf(before), PageView::Leaf(after)) => {
                    return Ok(after.starts_with(before));
                }
                (
                    PageView::Branch {
                        children: before, ..
                    },
                    PageView::Branch {
                        children: after, ..
                    },
                ) => {
                    if after.len() < before.len()
                        || before[..before.len() - 1] != after[..before.len() - 1]
                    {
                        return Ok(false);
                    }
                    old = *before.last().unwrap();
                    new = after[before.len() - 1];
                }
                _ => return Ok(false),
            }
        }
        Ok(false)
    }

    pub async fn lookup(
        &self,
        root: Option<Descriptor>,
        offset: u64,
    ) -> Result<Option<OwnedEntry>, ObjectLogError> {
        let Some(mut cursor) = root else {
            return Ok(None);
        };
        cursor.validate()?;
        if cursor.kind == Kind::Overflow || offset < cursor.start {
            return Err(invalid("offset root or retained range"));
        }
        if offset >= cursor.end {
            return Ok(None);
        }
        for _ in 0..=MAX_HEIGHT {
            let page = self.pages.read(cursor).await?;
            match page.view() {
                PageView::Leaf(entries) => {
                    let slot = entries
                        .iter()
                        .position(|entry| {
                            entry.base <= offset && entry.end().is_ok_and(|end| offset < end)
                        })
                        .ok_or_else(|| invalid("leaf does not cover requested offset"))?;
                    return Ok(Some(OwnedEntry { page, slot }));
                }
                PageView::Branch { children, .. } => {
                    cursor = *children
                        .iter()
                        .find(|child| child.start <= offset && offset < child.end)
                        .ok_or_else(|| invalid("branch does not cover requested offset"))?;
                }
                PageView::Overflow { .. } => return Err(invalid("overflow in offset tree")),
            }
        }
        Err(invalid("offset path exceeded height"))
    }

    /// Input is borrowed; reserve complete identity capacity before chunk copies.
    pub async fn store_object_id(&self, value: &str) -> Result<Descriptor, ObjectLogError> {
        validate_object_id(value)?;
        self.store_literal_id(value).await
    }
    pub async fn store_literal_id(&self, value: &str) -> Result<Descriptor, ObjectLogError> {
        if value.is_empty() || value.contains('\0') {
            return Err(invalid("literal identity"));
        }
        let _identity = self.pages.charge_output(
            value
                .len()
                .checked_add(std::mem::size_of::<String>())
                .ok_or_else(|| invalid("identity capacity overflow"))?,
        )?;
        let mut next = None;
        let mut start = value.len();
        for bytes in value
            .as_bytes()
            .rchunks(PAGE_BYTES - HEADER - DESCRIPTOR_BYTES)
        {
            start -= bytes.len();
            let page = self.pages.overflow(start as u64, bytes, next)?;
            next = Some(self.pages.write(page).await?);
        }
        next.ok_or_else(|| invalid("empty physical object identity"))
    }

    pub async fn object_id(&self, descriptor: Descriptor) -> Result<OwnedObjectId, ObjectLogError> {
        let value = self.literal_id(descriptor).await?;
        validate_object_id(value.as_str())?;
        Ok(value)
    }
    pub async fn literal_id(
        &self,
        descriptor: Descriptor,
    ) -> Result<OwnedObjectId, ObjectLogError> {
        descriptor.validate()?;
        if descriptor.kind != Kind::Overflow || descriptor.start != 0 {
            return Err(invalid("physical identity root"));
        }
        let len = usize::try_from(descriptor.end).map_err(|_| invalid("identity length domain"))?;
        let charge = self.pages.charge_output(
            len.checked_add(std::mem::size_of::<String>())
                .ok_or_else(|| invalid("identity capacity overflow"))?,
        )?;
        let mut value = Vec::with_capacity(len);
        let mut cursor = Some(descriptor);
        let mut chunks = 0u64;
        while let Some(current) = cursor {
            if chunks >= descriptor.entries {
                return Err(invalid("physical identity chunk limit"));
            }
            let page = self.pages.read(current).await?;
            let PageView::Overflow { start, bytes, next } = page.view() else {
                return Err(invalid("physical identity page kind"));
            };
            if start != value.len() as u64 || bytes.len() > len - value.len() {
                return Err(invalid("physical identity exact byte coverage"));
            }
            value.extend_from_slice(bytes);
            cursor = next;
            chunks += 1;
        }
        if value.len() != len || chunks != descriptor.entries {
            return Err(invalid("physical identity terminal length/count"));
        }
        let value = String::from_utf8(value).map_err(|_| invalid("physical identity UTF-8"))?;
        if value.is_empty() || value.contains('\0') {
            return Err(invalid("literal identity"));
        }
        Ok(OwnedObjectId {
            value,
            _charge: charge,
        })
    }

    /// Caller supplies an internally authored uploaded-entry certificate. This
    /// module alone cannot publish a head or authorize semantic replay skipping.
    pub async fn append(
        &self,
        root: Option<Descriptor>,
        entry: LeafEntry,
    ) -> Result<Descriptor, ObjectLogError> {
        entry.end()?;
        let Some(root) = root else {
            if entry.base != 0 {
                return Err(invalid("first append is not offset zero"));
            }
            return self.pages.write(self.pages.leaf(&[entry])?).await;
        };
        root.validate()?;
        if root.kind == Kind::Overflow || root.start != 0 || root.end != entry.base {
            return Err(invalid("append root or original assigned range"));
        }
        // Bounded frame capacity is reserved before allocating the path.
        let _path_charge = self
            .pages
            .charge_output(MAX_HEIGHT as usize * std::mem::size_of::<OwnedPage>())?;
        let mut path = Vec::with_capacity(MAX_HEIGHT as usize);
        let mut cursor = root;
        let mut leaf;
        loop {
            let page = self.pages.read(cursor).await?;
            match page.view() {
                PageView::Leaf(_) => {
                    leaf = page;
                    break;
                }
                PageView::Branch { children, .. } => {
                    if path.len() == MAX_HEIGHT as usize {
                        return Err(invalid("append path height limit"));
                    }
                    cursor = *children
                        .last()
                        .ok_or_else(|| invalid("empty append branch"))?;
                    path.push(page);
                }
                PageView::Overflow { .. } => return Err(invalid("overflow in append path")),
            }
        }
        let PageView::Leaf(entries) = leaf.view() else {
            unreachable!("validated leaf")
        };
        if root.height == MAX_HEIGHT && entries.len() == LEAF_SLOTS && path.iter().all(|page| matches!(page.view(), PageView::Branch { children, .. } if children.len()==BRANCH_SLOTS)) {
            return Err(invalid("append would exceed height before page upload"));
        }
        let (mut left, mut right) = if entries.len() < LEAF_SLOTS {
            leaf.push_leaf(entry)?;
            (self.pages.write(leaf).await?, None)
        } else {
            // A full immutable left leaf remains valid under the captured root.
            drop(leaf);
            (
                cursor,
                Some(self.pages.write(self.pages.leaf(&[entry])?).await?),
            )
        };
        while let Some(mut parent) = path.pop() {
            let PageView::Branch { height, children } = parent.view() else {
                unreachable!("validated branch")
            };
            let full = children.len() == BRANCH_SLOTS;
            parent.replace_last_child(left)?;
            let sibling = match right.take() {
                Some(child) if full => Some(
                    self.pages
                        .write(self.pages.branch(height, &[child])?)
                        .await?,
                ),
                Some(child) => {
                    parent.push_child(child)?;
                    None
                }
                None => None,
            };
            left = self.pages.write(parent).await?;
            right = sibling;
        }
        match right {
            Some(right) => {
                self.pages
                    .write(self.pages.branch(root.height + 1, &[left, right])?)
                    .await
            }
            None => Ok(left),
        }
    }
}
fn validate_object_id(value: &str) -> Result<(), ObjectLogError> {
    if value.is_empty()
        || value.starts_with('/')
        || value.contains('\0')
        || value
            .split('/')
            .any(|part| part == ".." || part == "." || part.is_empty())
    {
        return Err(ObjectLogError::InvalidObjectKey(
            "native indexed object identity".into(),
        ));
    }
    Ok(())
}
fn invalid(message: &str) -> ObjectLogError {
    ObjectLogError::Sequencer(format!("native offset index: {message}"))
}

#[cfg(test)]
mod tests {
    use super::super::codec::Scope;
    use super::super::page_store::ByteBudget;
    use super::*;
    #[cfg(feature = "s3")]
    use crate::BlobStore;
    use crate::LocalBlobStore;
    use bytes::Bytes;
    use sha2::{Digest, Sha256};

    #[tokio::test]
    async fn real_full_root_splits_right_spine_and_preserves_captured_history() {
        let directory = tempfile::tempdir().unwrap();
        exercise_full_root(
            Arc::new(LocalBlobStore::new(directory.path())),
            "pages".into(),
            crate::MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 56,
            },
        )
        .await;
    }
    async fn exercise_full_root(
        local: Arc<dyn super::super::page_store::NativeStore>,
        prefix: String,
        limits: crate::MetadataLimits,
    ) {
        let resources =
            super::super::operation::Resources::new(super::super::operation::ExecutionConfig {
                workers: 2,
                blocking_workers: 2,
                operations: 1,
                bytes: 1 << 20,
            })
            .unwrap();
        let budget = resources.bytes.clone();
        let scope = Scope {
            namespace: [1; 32],
            incarnation: [2; 32],
        };
        let pages =
            PageStore::new(local.clone(), prefix.clone(), scope, budget.clone(), limits).unwrap();
        resources
            .run(move |physical| async move {
                let pages = pages.in_operation(physical)?;
                let index = Index::new(pages.clone());
                let data_id = format!("{prefix}/data-object");
                let id = index.store_object_id(&data_id).await.unwrap();
                let count = LEAF_SLOTS * BRANCH_SLOTS;
                let data: Vec<u8> = (0..=count * 2 + 1).map(|n| (n % 251) as u8).collect();
                local
                    .put(&data_id, Bytes::from(data.clone()))
                    .await
                    .unwrap();
                let entry = |n: usize| LeafEntry {
                    object_id: id,
                    logical_id: id,
                    base: n as u64,
                    record_count: 1,
                    byte_start: n as u32,
                    byte_len: 1,
                    payload_hash: Sha256::digest(&data[n..n + 1]).into(),
                };
                // Build a complete full one-level tree through bounded immutable writes.
                let mut children = Vec::new();
                for leaf in 0..BRANCH_SLOTS {
                    let entries: Vec<_> = (leaf * LEAF_SLOTS..(leaf + 1) * LEAF_SLOTS)
                        .map(entry)
                        .collect();
                    children.push(pages.write(pages.leaf(&entries).unwrap()).await.unwrap());
                }
                let old = pages
                    .write(pages.branch(1, &children).unwrap())
                    .await
                    .unwrap();
                let mut rewritten_entries: Vec<_> = (0..LEAF_SLOTS).map(entry).collect();
                rewritten_entries[0].payload_hash[0] ^= 1;
                let rewritten_leaf = pages.write(pages.leaf(&rewritten_entries)?).await?;
                let mut rewritten_children = children.clone();
                rewritten_children[0] = rewritten_leaf;
                let rewritten_branch = pages.write(pages.branch(1, &rewritten_children)?).await?;
                assert!(!index.proves_extension(old, rewritten_branch).await?);
                let partial = pages
                    .write(pages.branch(1, &children[..2]).unwrap())
                    .await
                    .unwrap();
                let new = index.append(Some(old), entry(count)).await.unwrap();
                assert!(index.proves_extension(old, new).await?);
                assert!(index.proves_extension(partial, new).await?);
                assert!(index.proves_extension(children[0], new).await?);
                assert_eq!(
                    (new.height, new.start, new.end, new.entries),
                    (2, 0, count as u64 + 1, count as u64 + 1)
                );
                assert!(
                    index
                        .lookup(Some(old), count as u64)
                        .await
                        .unwrap()
                        .is_none()
                );
                for offset in [0, LEAF_SLOTS - 1, LEAF_SLOTS, count - 1, count] {
                    let location = index
                        .lookup(Some(new), offset as u64)
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(location.entry(), &entry(offset));
                    let object = index.object_id(location.entry().object_id).await.unwrap();
                    assert_eq!(object.as_str(), data_id);
                    let payload = local
                        .get_range(object.as_str(), offset as u64..offset as u64 + 1)
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        Sha256::digest(payload).as_slice(),
                        location.entry().payload_hash
                    );
                }
                let later = index.append(Some(new), entry(count)).await;
                assert!(
                    later.is_err(),
                    "original assigned range must match exact root end"
                );
                // Read/append through a non-full right leaf and parent with room.
                let partial = pages
                    .write(pages.leaf(&[entry(LEAF_SLOTS)]).unwrap())
                    .await
                    .unwrap();
                let parent = pages
                    .write(pages.branch(1, &[children[0], partial]).unwrap())
                    .await
                    .unwrap();
                let partial_grown = index
                    .append(Some(parent), entry(LEAF_SLOTS + 1))
                    .await
                    .unwrap();
                assert_eq!(
                    (partial_grown.height, partial_grown.end),
                    (1, LEAF_SLOTS as u64 + 2)
                );
                let single = pages
                    .write(pages.branch(1, &[children[0]]).unwrap())
                    .await
                    .unwrap();
                let split_with_room = index.append(Some(single), entry(LEAF_SLOTS)).await.unwrap();
                assert_eq!(
                    (split_with_room.height, split_with_room.end),
                    (1, LEAF_SLOTS as u64 + 1)
                );
                // A full middle parent splits into a higher ancestor that still has room.
                let mut right_children = Vec::new();
                for leaf in 0..BRANCH_SLOTS {
                    let entries: Vec<_> = (count + leaf * LEAF_SLOTS
                        ..count + (leaf + 1) * LEAF_SLOTS)
                        .map(entry)
                        .collect();
                    right_children.push(pages.write(pages.leaf(&entries).unwrap()).await.unwrap());
                }
                let right_parent = pages
                    .write(pages.branch(1, &right_children).unwrap())
                    .await
                    .unwrap();
                let upper = pages
                    .write(pages.branch(2, &[old, right_parent]).unwrap())
                    .await
                    .unwrap();
                let upper_grown = index.append(Some(upper), entry(count * 2)).await.unwrap();
                assert!(index.proves_extension(upper, upper_grown).await?);
                assert_eq!(
                    (upper_grown.height, upper_grown.end, upper_grown.entries),
                    (2, count as u64 * 2 + 1, count as u64 * 2 + 1)
                );
                for (root, offsets) in [
                    (partial_grown, vec![0, LEAF_SLOTS, LEAF_SLOTS + 1]),
                    (split_with_room, vec![0, LEAF_SLOTS - 1, LEAF_SLOTS]),
                    (
                        upper_grown,
                        vec![0, count - 1, count, count * 2 - 1, count * 2],
                    ),
                ] {
                    for offset in offsets {
                        assert_eq!(
                            index
                                .lookup(Some(root), offset as u64)
                                .await
                                .unwrap()
                                .unwrap()
                                .entry(),
                            &entry(offset)
                        );
                    }
                }
                let initial = index.append(None, entry(0)).await.unwrap();
                let grown = index.append(Some(initial), entry(1)).await.unwrap();
                assert_eq!((grown.height, grown.end), (0, 2));
                assert_eq!(
                    index
                        .lookup(Some(initial), 0)
                        .await
                        .unwrap()
                        .unwrap()
                        .entry(),
                    &entry(0)
                );
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    while budget.available() != 1 << 20 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("page source owners must drain");
                Ok(())
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    #[ignore = "requires configured S3 provider; mandatory native-v2 qualification lane"]
    async fn real_s3_runs_identical_full_root_contract_in_both_signing_modes() {
        let required = |name: &str| {
            std::env::var(name)
                .expect("native-v2 S3 qualification requires complete OBJECT_LOG_S3 configuration")
        };
        let endpoint = required("OBJECT_LOG_S3_ENDPOINT");
        let bucket = required("OBJECT_LOG_S3_BUCKET");
        let region = required("OBJECT_LOG_S3_REGION");
        let key = required("OBJECT_LOG_S3_KEY_ID");
        let secret = required("OBJECT_LOG_S3_SECRET");
        for unsigned in [false, true] {
            let local = Arc::new(
                crate::S3BlobStore::new(&endpoint, &region, &bucket, &key, &secret)
                    .with_payload_signing_disabled(unsigned),
            );
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let prefix = format!("native-v2-qualification-{nonce}-{unsigned}");
            exercise_full_root(
                local.clone(),
                prefix.clone(),
                crate::MetadataLimits {
                    payload_bytes: PAGE_BYTES,
                    version_bytes: 4096,
                },
            )
            .await;
            // Qualification-only cleanup after all captured-root checks/drain.
            // Production append-only retention/reclamation remains unsupported.
            for object in local.list(&prefix).await.unwrap() {
                local.delete(&object).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn full_utf8_identity_is_streamed_charged_and_validated_after_assembly() {
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalBlobStore::new(directory.path()));
        let budget = ByteBudget::new(1 << 20).unwrap();
        let pages = PageStore::new(
            local,
            "pages".into(),
            Scope {
                namespace: [1; 32],
                incarnation: [2; 32],
            },
            budget.clone(),
            crate::MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 56,
            },
        )
        .unwrap();
        let index = Index::new(pages.clone());
        let long = format!("key/{}", "€".repeat(4000));
        let descriptor = index.store_object_id(&long).await.unwrap();
        assert!(descriptor.entries > 1);
        let mut cursor = Some(descriptor);
        let mut split_code_point = false;
        while let Some(current) = cursor {
            let page = pages.read(current).await.unwrap();
            let PageView::Overflow { bytes, next, .. } = page.view() else {
                panic!("overflow page");
            };
            split_code_point |= std::str::from_utf8(bytes).is_err();
            cursor = next;
        }
        assert!(
            split_code_point,
            "fixture must split a UTF-8 code point across pages"
        );
        let recovered = index.object_id(descriptor).await.unwrap();
        assert_eq!(recovered.as_str(), long);
        assert!(budget.available() < 1 << 20);
        drop(recovered);
        assert_eq!(budget.available(), 1 << 20);
        let invalid = pages
            .write(pages.overflow(0, &[0xff], None).unwrap())
            .await
            .unwrap();
        assert!(index.object_id(invalid).await.is_err());
        assert!(index.store_object_id("bad/../key").await.is_err());
        assert_eq!(budget.available(), 1 << 20);
    }
}

#[cfg(test)]
mod extension_tests {
    use super::*;
    use crate::native_v2::{
        codec::Scope,
        operation::{ExecutionConfig, Resources},
    };
    use crate::{LocalBlobStore, MetadataLimits};

    #[tokio::test]
    async fn canonical_extension_checks_leaf_growth_root_growth_siblings_and_exact_budget() {
        let directory = tempfile::tempdir().unwrap();
        let resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 1,
            bytes: 1 << 20,
        })
        .unwrap();
        let pages = PageStore::new(
            Arc::new(LocalBlobStore::new(directory.path())),
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
        let budget = resources.bytes.clone();
        resources
            .run(move |physical| async move {
                let pages = pages.in_operation(physical)?;
                let index = Index::new(pages.clone());
                let id = index.store_object_id("physical/object").await?;
                let entry = |base| LeafEntry {
                    object_id: id,
                    logical_id: id,
                    base,
                    record_count: 1,
                    byte_start: 0,
                    byte_len: 1,
                    payload_hash: [3; 32],
                };
                let one = index.append(None, entry(0)).await?;
                let two = index.append(Some(one), entry(1)).await?;
                assert!(index.proves_extension(one, one).await?);
                assert!(index.proves_extension(one, two).await?);
                assert!(!index.proves_extension(two, one).await?);
                let mut changed = entry(0);
                changed.payload_hash[0] ^= 1;
                let rewritten = pages.write(pages.leaf(&[changed, entry(1)])?).await?;
                assert!(!index.proves_extension(one, rewritten).await?);
                let required = pages.allocation() * 2;
                let pressure = budget.charge(budget.available() - required)?;
                assert!(index.proves_extension(one, two).await?);
                drop(pressure);
                let pressure = budget.charge(budget.available() - required + 1)?;
                assert!(index.proves_extension(one, two).await.is_err());
                drop(pressure);
                let mut root = two;
                for n in 2..LEAF_SLOTS as u64 {
                    root = index.append(Some(root), entry(n)).await?;
                }
                let full_leaf = root;
                root = index.append(Some(root), entry(LEAF_SLOTS as u64)).await?;
                assert_eq!(root.height, 1);
                assert!(index.proves_extension(full_leaf, root).await?);
                let first_branch = root;
                for n in LEAF_SLOTS as u64 + 1..LEAF_SLOTS as u64 * 2 + 1 {
                    root = index.append(Some(root), entry(n)).await?;
                }
                assert!(index.proves_extension(first_branch, root).await?);
                assert!(index.proves_extension(one, root).await?);
                Ok(())
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
        assert_eq!(resources.bytes.available(), 1 << 20);
    }
}
