//! Scoped semantic publication history. Skip links are built from authenticated
//! predecessors; a numeric offset alone never proves transition ancestry.
//! Skip proofs assume canonical committed writers built their tables through
//! this protocol; they do not replay every skipped semantic transition.
use super::{WorkBudget, codec::Descriptor};
use crate::{
    ObjectLogError,
    native_v2::page_store::{PageStore, RetainedBytes},
};

const MAGIC: &[u8; 8] = b"ONS2\0\0\0\x01";
const HEADER: usize = 256;
const LINKS: usize = 64;
const LINK_BYTES: usize = 40;
const BYTES: usize = HEADER + LINKS * LINK_BYTES;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Reference {
    hash: [u8; 32],
    through: u64,
    generation: u64,
}
impl Reference {
    pub fn hash(self) -> [u8; 32] {
        self.hash
    }
    pub fn generation(self) -> u64 {
        self.generation
    }
    pub fn through(self) -> u64 {
        self.through
    }
}

pub(super) struct Manifest {
    reference: Reference,
    bytes: RetainedBytes,
}
impl Manifest {
    pub fn reference(&self) -> Reference {
        self.reference
    }
    pub fn group_start(&self) -> u64 {
        number(self.bytes.bytes(), 88)
    }
    pub fn entry_count(&self) -> u64 {
        number(self.bytes.bytes(), 96)
    }
    pub fn root(&self) -> Result<Option<Descriptor>, ObjectLogError> {
        let raw = self.bytes.bytes();
        match raw[152] {
            0 if raw[104..152] == [0; 48] => Ok(None),
            1 => Descriptor::decode(&raw[104..152]).map(Some),
            _ => Err(invalid("manifest map root")),
        }
    }
    fn ancestor(&self, level: usize) -> Result<Reference, ObjectLogError> {
        if level >= link_count(self.reference.generation) {
            return Err(invalid("manifest ancestor slot"));
        }
        let start = HEADER + level * LINK_BYTES;
        let raw = self.bytes.bytes();
        let reference = Reference {
            hash: raw[start..start + 32].try_into().unwrap(),
            through: number(raw, start + 32),
            generation: self
                .reference
                .generation
                .checked_sub(1u64 << level)
                .ok_or_else(|| invalid("manifest ancestor generation"))?,
        };
        if reference.hash == [0; 32]
            || reference.through >= self.reference.through
            || reference.generation == 0
            || reference.through < reference.generation
            || self.reference.through - reference.through < (1u64 << level)
        {
            return Err(invalid("manifest ancestor reference"));
        }
        Ok(reference)
    }
    pub async fn read(
        pages: &PageStore,
        reference: Reference,
        work: &mut WorkBudget,
    ) -> Result<Self, ObjectLogError> {
        if reference.generation == 0 {
            return Err(invalid("manifest generation"));
        }
        Self::read_at(
            pages,
            reference.hash,
            reference.through,
            Some(reference.generation),
            work,
        )
        .await
    }
    pub async fn read_anchor(
        pages: &PageStore,
        hash: [u8; 32],
        through: u64,
        work: &mut WorkBudget,
    ) -> Result<Self, ObjectLogError> {
        Self::read_at(pages, hash, through, None, work).await
    }
    async fn read_at(
        pages: &PageStore,
        hash: [u8; 32],
        through: u64,
        expected_generation: Option<u64>,
        work: &mut WorkBudget,
    ) -> Result<Self, ObjectLogError> {
        if hash == [0; 32] || through == 0 || through > i64::MAX as u64 {
            return Err(invalid("manifest reference"));
        }
        work.page()?;
        let bytes = pages.read_scoped_bytes(&hash, BYTES).await?;
        let raw = bytes.bytes();
        let scope = pages.scope();
        let generation = number(raw, 72);
        if &raw[..8] != MAGIC
            || raw[8..40] != scope.namespace
            || raw[40..72] != scope.incarnation
            || generation == 0
            || generation > through
            || expected_generation.is_some_and(|expected| expected != generation)
            || number(raw, 80) != through
            || raw[153..HEADER].iter().any(|b| *b != 0)
        {
            return Err(invalid("manifest framing or identity"));
        }
        let reference = Reference {
            hash,
            through,
            generation,
        };
        let manifest = Self { reference, bytes };
        manifest.root()?;
        let start = manifest.group_start();
        let count = manifest.entry_count();
        if start >= reference.through
            || count == 0
            || count > reference.through - start
            || (reference.generation == 1 && start != 0)
        {
            return Err(invalid("manifest group bounds"));
        }
        let mut last = reference.through;
        for level in 0..link_count(reference.generation) {
            let ancestor = manifest.ancestor(level)?;
            let minimum_gap = if level == 0 { 1 } else { 1u64 << (level - 1) };
            if ancestor.through >= last
                || last - ancestor.through < minimum_gap
                || (level == 0 && ancestor.through != start)
            {
                return Err(invalid("manifest ancestor offsets"));
            }
            last = ancestor.through;
        }
        if manifest.bytes.bytes()[HEADER + link_count(reference.generation) * LINK_BYTES..]
            .iter()
            .any(|b| *b != 0)
        {
            return Err(invalid("manifest unused ancestors"));
        }
        Ok(manifest)
    }

    /// This is a private storage primitive; only the coupled preparation path
    /// may supply the root produced by its own COW edit, never a cached root.
    pub async fn build(
        pages: &PageStore,
        previous: Option<Reference>,
        through: u64,
        entry_count: u64,
        root: Option<Descriptor>,
        work: &mut WorkBudget,
    ) -> Result<Reference, ObjectLogError> {
        pages.require_operation()?;
        if let Some(root) = root {
            root.validate()?;
        }
        let generation = previous
            .map_or(Some(1), |p| p.generation.checked_add(1))
            .ok_or_else(|| invalid("manifest generation overflow"))?;
        let start = previous.map_or(0, |p| p.through);
        if through <= start
            || through > i64::MAX as u64
            || generation > through
            || entry_count == 0
            || entry_count > through - start
        {
            return Err(invalid("manifest new group bounds"));
        }
        let levels = link_count(generation);
        let _charge = pages.charge_output(
            levels
                .checked_mul(std::mem::size_of::<Reference>())
                .and_then(|n| n.checked_add(std::mem::size_of::<Vec<Reference>>()))
                .ok_or_else(|| invalid("manifest ancestor allocation"))?,
        )?;
        let mut links = Vec::with_capacity(levels);
        if let Some(previous) = previous {
            // Even a two-generation history authenticates its predecessor.
            let parent = Self::read(pages, previous, work).await?;
            links.push(parent.reference());
            for level in 1..levels {
                let middle = Self::read(pages, links[level - 1], work).await?;
                links.push(middle.ancestor(level - 1)?);
            }
        }
        work.page()?;
        let encoded = pages.encode_scoped_bytes(BYTES, |raw, scope| {
            raw[..8].copy_from_slice(MAGIC);
            raw[8..40].copy_from_slice(&scope.namespace);
            raw[40..72].copy_from_slice(&scope.incarnation);
            for (offset, value) in [
                (72, generation),
                (80, through),
                (88, start),
                (96, entry_count),
            ] {
                raw[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
            }
            if let Some(root) = root {
                root.encode(&mut raw[104..152]);
                raw[152] = 1;
            }
            for (level, link) in links.iter().enumerate() {
                let offset = HEADER + level * LINK_BYTES;
                raw[offset..offset + 32].copy_from_slice(&link.hash);
                raw[offset + 32..offset + 40].copy_from_slice(&link.through.to_be_bytes());
            }
            Ok(())
        })?;
        let hash = pages.write_scoped_bytes(encoded).await?;
        Ok(Reference {
            hash,
            through,
            generation,
        })
    }

    pub async fn validate_transition(
        pages: &PageStore,
        transition: crate::native_v2::data::Transition,
        work: &mut WorkBudget,
    ) -> Result<(), ObjectLogError> {
        let manifest =
            Self::read_anchor(pages, transition.after, transition.group_end, work).await?;
        let predecessor = if manifest.reference.generation == 1 {
            [0; 32]
        } else {
            manifest.ancestor(0)?.hash
        };
        if manifest.reference.generation != transition.generation
            || manifest.group_start() != transition.group_start
            || manifest.entry_count() != transition.entry_count
            || predecessor != transition.before
        {
            return Err(invalid("indexed transition differs from semantic manifest"));
        }
        Ok(())
    }

    pub async fn proves_origin(
        pages: &PageStore,
        mut current: Reference,
        work: &mut WorkBudget,
    ) -> Result<bool, ObjectLogError> {
        loop {
            let manifest = Self::read(pages, current, work).await?;
            if current.generation == 1 {
                return Ok(manifest.group_start() == 0);
            }
            let distance = current.generation - 1;
            let level = (u64::BITS - 1 - distance.leading_zeros()) as usize;
            current = manifest.ancestor(level)?;
        }
    }

    pub async fn proves_extension(
        pages: &PageStore,
        before: Reference,
        after: Reference,
        work: &mut WorkBudget,
    ) -> Result<bool, ObjectLogError> {
        if before.generation > after.generation || before.through > after.through {
            return Ok(false);
        }
        let mut current = after;
        loop {
            let manifest = Self::read(pages, current, work).await?;
            if current.generation == before.generation {
                return Ok(current == before);
            }
            let distance = current.generation - before.generation;
            let level = (u64::BITS - 1 - distance.leading_zeros()) as usize;
            current = manifest.ancestor(level)?;
        }
    }
}
fn link_count(generation: u64) -> usize {
    (u64::BITS - generation.saturating_sub(1).leading_zeros()) as usize
}
fn number(raw: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(raw[offset..offset + 8].try_into().unwrap())
}
fn invalid(message: &str) -> ObjectLogError {
    ObjectLogError::InvalidBatch(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        LocalBlobStore,
        native_v2::semantic_map::test_support::{fixture, resources},
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn skip_history_exact_ancestry_and_logarithmic_work() {
        let directory = tempfile::tempdir().unwrap();
        let resources = resources();
        let pages = fixture(Arc::new(LocalBlobStore::new(directory.path())), &resources);
        resources
            .run(move |physical| async move {
                let pages = pages.in_operation(physical)?;
                let mut history = Vec::new();
                for generation in 1..=130u64 {
                    let mut work = WorkBudget::new(65, 0);
                    let reference = Manifest::build(
                        &pages,
                        history.last().copied(),
                        generation * 3,
                        2,
                        None,
                        &mut work,
                    )
                    .await?;
                    let expected = if generation == 1 {
                        1
                    } else {
                        1 + link_count(generation) as u64
                    };
                    assert_eq!(65 - work.pages, expected);
                    history.push(reference);
                }
                let mut dense = Vec::new();
                let mut dense_work = WorkBudget::new(100, 0);
                for generation in 1..=9 {
                    dense.push(
                        Manifest::build(
                            &pages,
                            dense.last().copied(),
                            generation,
                            1,
                            None,
                            &mut dense_work,
                        )
                        .await?,
                    );
                }
                for (source, level, through) in [(dense[1], 0, 0u64), (dense[8], 2, 6u64)] {
                    let page = Manifest::read(&pages, source, &mut dense_work).await?;
                    let encoded = pages.encode_scoped_bytes(BYTES, |raw, _| {
                        raw.copy_from_slice(page.bytes.bytes());
                        let offset = HEADER + level * LINK_BYTES + 32;
                        raw[offset..offset + 8].copy_from_slice(&through.to_be_bytes());
                        if source.generation == 2 {
                            raw[88..96].copy_from_slice(&0u64.to_be_bytes());
                        }
                        Ok(())
                    })?;
                    let hash = pages.write_scoped_bytes(encoded).await?;
                    assert!(
                        Manifest::read(&pages, Reference { hash, ..source }, &mut dense_work)
                            .await
                            .is_err()
                    );
                }
                let after = *history.last().unwrap();
                assert_eq!(
                    Manifest::read_anchor(
                        &pages,
                        after.hash(),
                        after.through(),
                        &mut WorkBudget::new(1, 0)
                    )
                    .await?
                    .reference(),
                    after
                );
                // Generation nine's level-three jump must land on generation one.
                let ninth = Manifest::read(&pages, history[8], &mut WorkBudget::new(1, 0)).await?;
                let encoded = pages.encode_scoped_bytes(BYTES, |raw, _| {
                    raw.copy_from_slice(ninth.bytes.bytes());
                    let offset = HEADER + 2 * LINK_BYTES + 32;
                    raw[offset..offset + 8].copy_from_slice(&20u64.to_be_bytes());
                    Ok(())
                })?;
                let hash = pages.write_scoped_bytes(encoded).await?;
                assert!(
                    Manifest::read(
                        &pages,
                        Reference { hash, ..history[8] },
                        &mut WorkBudget::new(1, 0)
                    )
                    .await
                    .is_err()
                );

                for (hash, through) in [
                    ([199; 32], 3u64),
                    (history[0].hash(), 4),
                    (history[1].hash(), 6),
                ] {
                    let encoded = pages.encode_scoped_bytes(BYTES, |raw, _| {
                        raw.copy_from_slice(ninth.bytes.bytes());
                        let slot = HEADER + 3 * LINK_BYTES;
                        raw[slot..slot + 32].copy_from_slice(&hash);
                        raw[slot + 32..slot + 40].copy_from_slice(&through.to_be_bytes());
                        Ok(())
                    })?;
                    let hash = pages.write_scoped_bytes(encoded).await?;
                    let changed = Reference { hash, ..history[8] };
                    assert!(
                        Manifest::proves_extension(
                            &pages,
                            history[0],
                            changed,
                            &mut WorkBudget::new(2, 0)
                        )
                        .await
                        .is_err()
                    );
                }

                for before in &history {
                    let needed = (after.generation - before.generation).count_ones() as u64 + 1;
                    let mut exact = WorkBudget::new(needed, 0);
                    assert!(Manifest::proves_extension(&pages, *before, after, &mut exact).await?);
                    assert_eq!(exact.pages, 0);
                    assert!(matches!(
                        Manifest::proves_extension(
                            &pages,
                            *before,
                            after,
                            &mut WorkBudget::new(needed - 1, 0)
                        )
                        .await,
                        Err(ObjectLogError::BudgetExceeded(_))
                    ));
                }
                let mut work = WorkBudget::new(100, 0);
                let alternate = Manifest::build(&pages, None, 3, 1, None, &mut work).await?;
                assert_ne!(alternate.hash(), history[0].hash());
                assert!(!Manifest::proves_extension(&pages, alternate, after, &mut work).await?);
                let fork = Manifest::build(&pages, Some(alternate), 6, 2, None, &mut work).await?;
                assert!(!Manifest::proves_extension(&pages, history[0], fork, &mut work).await?);
                assert!(!Manifest::proves_extension(&pages, after, history[0], &mut work).await?);
                Ok(())
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
        assert_eq!(resources.bytes.available(), 4 << 20);
    }

    #[tokio::test]
    async fn manifest_recommitted_framing_bounds_and_absence_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let resources = resources();
        let pages = fixture(Arc::new(LocalBlobStore::new(directory.path())), &resources);
        resources
            .run(move |physical| async move {
                let pages = pages.in_operation(physical)?;
                let mut work = WorkBudget::new(100, 0);
                assert!(
                    Manifest::build(&pages, None, 0, 1, None, &mut work)
                        .await
                        .is_err()
                );
                assert!(
                    Manifest::build(&pages, None, 1, 2, None, &mut work)
                        .await
                        .is_err()
                );
                let (encoded, descriptor) = super::super::codec::leaf(
                    &pages,
                    &[super::super::codec::Entry {
                        key: super::super::codec::ByteRef::inline(b"key")?,
                        value: super::super::codec::ByteRef::inline(b"value")?,
                    }],
                )?;
                pages.write_scoped_bytes(encoded).await?;
                let with_root =
                    Manifest::build(&pages, None, 3, 2, Some(descriptor), &mut work).await?;
                assert_eq!(
                    Manifest::read_anchor(&pages, with_root.hash(), 3, &mut work)
                        .await?
                        .root()?,
                    Some(descriptor)
                );
                let original = Manifest::build(&pages, None, 3, 2, None, &mut work).await?;
                let source = Manifest::read(&pages, original, &mut work).await?;
                assert!(source.root()?.is_none());
                assert_eq!(source.reference().through(), 3);
                for offset in [0, 72, 80, 88, 96, 104, 152, 153, 256, BYTES - 1] {
                    let encoded = pages.encode_scoped_bytes(BYTES, |raw, _| {
                        raw.copy_from_slice(source.bytes.bytes());
                        raw[offset] ^= 1;
                        Ok(())
                    })?;
                    let hash = pages.write_scoped_bytes(encoded).await?;
                    let corrupt = Reference { hash, ..original };
                    assert!(
                        Manifest::read(&pages, corrupt, &mut work).await.is_err(),
                        "offset {offset}"
                    );
                }
                let missing = Reference {
                    hash: [199; 32],
                    ..original
                };
                assert!(
                    Manifest::proves_extension(&pages, original, missing, &mut work)
                        .await
                        .is_err()
                );
                let invalid = Reference {
                    generation: u64::MAX,
                    ..original
                };
                assert!(
                    Manifest::build(&pages, Some(invalid), 4, 1, None, &mut work)
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
