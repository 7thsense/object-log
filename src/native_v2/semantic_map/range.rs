//! Root-bound ordered traversal. Errors/cancellation permanently poison a cursor.
use super::{
    WorkBudget,
    bytes::{self, OwnedBytes},
    codec::{self, ByteRef, Descriptor, MAX_HEIGHT},
    read::VerifiedNode,
};
use crate::{
    ObjectLogError,
    native_v2::page_store::{Charge, PageStore},
};
use std::sync::Arc;

struct Frame {
    node: VerifiedNode,
    index: usize,
    upper: Option<ByteRef>,
}
pub(in crate::native_v2) struct Item {
    pub key: OwnedBytes,
    pub value: OwnedBytes,
}
pub(in crate::native_v2) enum Step {
    Row(Item),
    End,
    OutputLimit { actual: usize, limit: usize },
}
pub(in crate::native_v2) struct Cursor {
    frames: Vec<Frame>,
    lower: Option<(OwnedBytes, bool)>,
    upper: Option<(OwnedBytes, bool)>,
    _root: Option<Descriptor>,
    binding: Arc<()>,
    exhausted: bool,
    poisoned: bool,
    _charge: Arc<Charge>,
}
impl Cursor {
    pub(in crate::native_v2::semantic_map) async fn start(
        pages: &PageStore,
        root: Option<Descriptor>,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
        work: &mut WorkBudget,
    ) -> Result<Self, ObjectLogError> {
        pages.require_operation()?;
        if let Some(root) = root {
            root.validate()?;
        }
        if lower
            .zip(upper)
            .is_some_and(|(lower, upper)| lower.0 > upper.0)
        {
            return Err(ObjectLogError::InvalidBatch("map range bounds".into()));
        }
        let empty = lower
            .zip(upper)
            .is_some_and(|(lower, upper)| lower.0 == upper.0 && (!lower.1 || !upper.1));
        let capacity = usize::from(MAX_HEIGHT) + 1;
        let charge = pages
            .charge_output(capacity * std::mem::size_of::<Frame>() + std::mem::size_of::<Self>())?;
        let mut cursor = Self {
            frames: Vec::with_capacity(capacity),
            lower: None,
            upper: None,
            _root: root,
            binding: pages.binding_identity(),
            exhausted: empty || root.is_none(),
            poisoned: false,
            _charge: charge,
        };
        cursor.lower = lower
            .map(|(value, inclusive)| {
                OwnedBytes::copy(pages, value, work).map(|value| (value, inclusive))
            })
            .transpose()?;
        cursor.upper = upper
            .map(|(value, inclusive)| {
                OwnedBytes::copy(pages, value, work).map(|value| (value, inclusive))
            })
            .transpose()?;
        if !cursor.exhausted {
            cursor
                .descend(pages, root.unwrap(), None, None, true, work)
                .await?;
        }
        Ok(cursor)
    }
    #[cfg(test)]
    pub(in crate::native_v2::semantic_map) fn root(&self) -> Option<Descriptor> {
        self._root
    }
    async fn descend(
        &mut self,
        pages: &PageStore,
        mut descriptor: Descriptor,
        mut lower: Option<ByteRef>,
        mut upper: Option<ByteRef>,
        seek: bool,
        work: &mut WorkBudget,
    ) -> Result<(), ObjectLogError> {
        loop {
            if self.frames.len() > usize::from(MAX_HEIGHT) {
                return Err(codec::invalid("map range height exhausted"));
            }
            let node = VerifiedNode::read(pages, descriptor, lower, upper, work).await?;
            let position = if seek {
                match self.lower.as_ref() {
                    Some((lower, _)) => node.position(pages, lower.as_slice(), work).await?,
                    None => None,
                }
            } else {
                None
            };
            let index = if descriptor.height == 0 {
                match position {
                    Some((slot, true)) if self.lower.as_ref().unwrap().1 => slot,
                    Some((slot, _)) => slot + 1,
                    None => 0,
                }
            } else {
                position.map_or(0, |(slot, _)| slot)
            };
            if descriptor.height == 0 {
                self.frames.push(Frame { node, index, upper });
                return Ok(());
            }
            let child = node.page().child(index)?;
            let next_upper = if index + 1 < node.page().slots() {
                Some(node.page().child(index + 1)?.first)
            } else {
                upper
            };
            self.frames.push(Frame { node, index, upper });
            descriptor = child.descriptor;
            lower = Some(child.first);
            upper = next_upper;
        }
    }
    #[cfg(test)]
    pub async fn next(
        &mut self,
        pages: &PageStore,
        work: &mut WorkBudget,
    ) -> Result<Option<Item>, ObjectLogError> {
        match self.next_bounded(pages, work, usize::MAX).await? {
            Step::Row(item) => Ok(Some(item)),
            Step::End => Ok(None),
            Step::OutputLimit { actual, limit } => {
                Err(ObjectLogError::ReadLimitExceeded { actual, limit })
            }
        }
    }
    pub async fn next_bounded(
        &mut self,
        pages: &PageStore,
        work: &mut WorkBudget,
        max_output_bytes: usize,
    ) -> Result<Step, ObjectLogError> {
        if self.poisoned {
            return Err(codec::invalid(
                "map cursor previously failed or was canceled",
            ));
        }
        // Set before the first await. Dropping this future cannot turn a partly
        // advanced traversal into a fresh successful cursor.
        self.poisoned = true;
        pages.require_operation()?;
        if !Arc::ptr_eq(&self.binding, &pages.binding_identity()) {
            return Err(codec::invalid("map cursor belongs to another binding"));
        }
        let result = self.next_inner(pages, work, max_output_bytes).await;
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    async fn next_inner(
        &mut self,
        pages: &PageStore,
        work: &mut WorkBudget,
        max_output_bytes: usize,
    ) -> Result<Step, ObjectLogError> {
        if self.exhausted {
            return Ok(Step::End);
        }
        loop {
            let frame = self
                .frames
                .last_mut()
                .ok_or_else(|| codec::invalid("map cursor lost traversal frontier"))?;
            if frame.index < frame.node.page().slots() {
                let entry = frame.node.page().entry(frame.index)?;
                let key = bytes::load(pages, entry.key, work).await?;
                if self.lower.as_ref().is_some_and(|(lower, inclusive)| {
                    key.as_slice() < lower.as_slice()
                        || (!inclusive && key.as_slice() == lower.as_slice())
                }) {
                    return Err(codec::invalid("map cursor escaped requested lower bound"));
                }
                if self.upper.as_ref().is_some_and(|(upper, inclusive)| {
                    key.as_slice() > upper.as_slice()
                        || (!inclusive && key.as_slice() == upper.as_slice())
                }) {
                    self.exhausted = true;
                    self.frames.clear();
                    return Ok(Step::End);
                }
                let row_bytes = usize::try_from(entry.value.len())
                    .ok()
                    .and_then(|n| n.checked_add(key.as_slice().len()))
                    .ok_or_else(|| {
                        ObjectLogError::BudgetExceeded("semantic scan row byte domain".into())
                    })?;
                if row_bytes > max_output_bytes {
                    return Ok(Step::OutputLimit {
                        actual: row_bytes,
                        limit: max_output_bytes,
                    });
                }
                frame.index += 1;
                let value = bytes::load(pages, entry.value, work).await?;
                if self.upper.as_ref().is_some_and(|(upper, inclusive)| {
                    *inclusive && key.as_slice() == upper.as_slice()
                }) {
                    self.exhausted = true;
                    self.frames.clear();
                }
                return Ok(Step::Row(Item { key, value }));
            }
            self.frames.pop();
            let successor = loop {
                let Some(frame) = self.frames.last_mut() else {
                    break None;
                };
                frame.index += 1;
                if frame.index < frame.node.page().slots() {
                    let child = frame.node.page().child(frame.index)?;
                    let upper = if frame.index + 1 < frame.node.page().slots() {
                        Some(frame.node.page().child(frame.index + 1)?.first)
                    } else {
                        frame.upper
                    };
                    break Some((child, upper));
                }
                self.frames.pop();
            };
            let Some((child, upper)) = successor else {
                self.exhausted = true;
                return Ok(Step::End);
            };
            self.descend(
                pages,
                child.descriptor,
                Some(child.first),
                upper,
                false,
                work,
            )
            .await?;
        }
    }
}
