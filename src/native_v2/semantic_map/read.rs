//! Full-key validation precedes routing, membership and absence decisions.
use super::{
    WorkBudget,
    bytes::{self, OwnedBytes},
    codec::{self, ByteRef, Descriptor, MAX_HEIGHT, NodePage},
};
use crate::{ObjectLogError, native_v2::page_store::PageStore};
use std::cmp::Ordering;

enum Key {
    Inline(ByteRef),
    Overflow(OwnedBytes),
}
impl Key {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Inline(value) => value.inline_bytes().expect("inline key"),
            Self::Overflow(value) => value.as_slice(),
        }
    }
}
async fn key(
    pages: &PageStore,
    reference: ByteRef,
    work: &mut WorkBudget,
) -> Result<Key, ObjectLogError> {
    if reference.inline_bytes().is_some() {
        Ok(Key::Inline(reference))
    } else {
        Ok(Key::Overflow(bytes::load(pages, reference, work).await?))
    }
}

pub(super) async fn compare(
    pages: &PageStore,
    reference: ByteRef,
    needle: &[u8],
    work: &mut WorkBudget,
) -> Result<Ordering, ObjectLogError> {
    Ok(key(pages, reference, work).await?.bytes().cmp(needle))
}

pub(super) struct VerifiedNode {
    page: NodePage,
}
impl VerifiedNode {
    pub fn page(&self) -> &NodePage {
        &self.page
    }
    pub async fn read(
        pages: &PageStore,
        descriptor: Descriptor,
        lower: Option<ByteRef>,
        upper: Option<ByteRef>,
        work: &mut WorkBudget,
    ) -> Result<Self, ObjectLogError> {
        work.page()?;
        let page = NodePage::read(pages, descriptor).await?;
        let lower = match lower {
            Some(value) => Some(key(pages, value, work).await?),
            None => None,
        };
        let upper = match upper {
            Some(value) => Some(key(pages, value, work).await?),
            None => None,
        };
        let mut previous: Option<Key> = None;
        for slot in 0..page.slots() {
            let reference = if descriptor.height == 0 {
                page.entry(slot)?.key
            } else {
                page.child(slot)?.first
            };
            let current = key(pages, reference, work).await?;
            if slot == 0
                && lower
                    .as_ref()
                    .is_some_and(|lower| lower.bytes() != current.bytes())
            {
                return Err(codec::invalid("map separator differs from child first key"));
            }
            if previous
                .as_ref()
                .is_some_and(|previous| previous.bytes() >= current.bytes())
            {
                return Err(codec::invalid("map keys are not strictly increasing"));
            }
            if upper
                .as_ref()
                .is_some_and(|upper| current.bytes() >= upper.bytes())
            {
                return Err(codec::invalid("map child escapes inherited upper interval"));
            }
            previous = Some(current);
        }
        Ok(Self { page })
    }
    /// Last full key <= needle; None is meaningful only after full validation.
    pub async fn position(
        &self,
        pages: &PageStore,
        needle: &[u8],
        work: &mut WorkBudget,
    ) -> Result<Option<(usize, bool)>, ObjectLogError> {
        let mut low = 0;
        let mut high = self.page.slots();
        while low < high {
            let middle = low + (high - low) / 2;
            let reference = if self.page.descriptor().height == 0 {
                self.page.entry(middle)?.key
            } else {
                self.page.child(middle)?.first
            };
            let value = key(pages, reference, work).await?;
            match value.bytes().cmp(needle) {
                Ordering::Less => low = middle + 1,
                Ordering::Equal => return Ok(Some((middle, true))),
                Ordering::Greater => high = middle,
            }
        }
        Ok(low.checked_sub(1).map(|slot| (slot, false)))
    }
}

pub(super) async fn lookup(
    pages: &PageStore,
    root: Option<Descriptor>,
    needle: &[u8],
    work: &mut WorkBudget,
) -> Result<Option<OwnedBytes>, ObjectLogError> {
    pages.require_operation()?;
    let Some(mut descriptor) = root else {
        return Ok(None);
    };
    let mut lower = None;
    let mut upper = None;
    for _ in 0..=MAX_HEIGHT {
        let node = VerifiedNode::read(pages, descriptor, lower, upper, work).await?;
        let position = node.position(pages, needle, work).await?;
        if descriptor.height == 0 {
            let Some((slot, equal)) = position else {
                return Ok(None);
            };
            return if equal {
                Ok(Some(
                    bytes::load(pages, node.page.entry(slot)?.value, work).await?,
                ))
            } else {
                Ok(None)
            };
        }
        // Even below the first separator, authenticate its child before
        // accepting absence; the separator alone cannot prove its own truth.
        let slot = position.map_or(0, |(slot, _)| slot);
        let child = node.page.child(slot)?;
        lower = Some(child.first);
        if slot + 1 < node.page.slots() {
            upper = Some(node.page.child(slot + 1)?.first);
        }
        descriptor = child.descriptor;
    }
    Err(codec::invalid("map lookup exceeded height"))
}
