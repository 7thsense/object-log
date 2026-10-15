//! Sparse batched COW. Stored links retain their original authenticated bounds.
use super::{
    WorkBudget, bytes,
    codec::{self, BRANCH_SLOTS, ByteRef, Child, Descriptor, Entry, LEAF_SLOTS, MAX_HEIGHT},
    read::{self, VerifiedNode},
};
use crate::{
    ObjectLogError,
    native_v2::page_store::{Charge, PageStore},
};
use std::{cmp::Ordering, sync::Arc};
#[cfg(test)]
mod tests;

const LEAF_MIN: usize = LEAF_SLOTS.div_ceil(2);
const BRANCH_MIN: usize = BRANCH_SLOTS.div_ceil(2);

pub(in crate::native_v2) struct Edit<'a> {
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
}
struct Link {
    first: ByteRef,
    state: State,
}
enum State {
    Stored {
        descriptor: Descriptor,
        lower: Option<ByteRef>,
        upper: Option<ByteRef>,
        nonroot: bool,
    },
    Dirty(Box<Node>),
}
enum Data {
    Leaf(Vec<Entry>),
    Branch(Vec<Link>),
}
struct Node {
    height: u8,
    data: Data,
    original: Option<Descriptor>,
    changed: bool,
    _charge: Arc<Charge>,
}
impl Node {
    fn new(
        pages: &PageStore,
        height: u8,
        original: Option<Descriptor>,
    ) -> Result<Box<Self>, ObjectLogError> {
        if height > MAX_HEIGHT {
            return Err(codec::invalid("map edit height exhausted"));
        }
        let allocation = if height == 0 {
            (LEAF_SLOTS + 1) * std::mem::size_of::<Entry>()
        } else {
            (BRANCH_SLOTS + 1) * std::mem::size_of::<Link>()
        };
        let charge = pages.charge_output(allocation + std::mem::size_of::<Self>())?;
        let data = if height == 0 {
            Data::Leaf(Vec::with_capacity(LEAF_SLOTS + 1))
        } else {
            Data::Branch(Vec::with_capacity(BRANCH_SLOTS + 1))
        };
        Ok(Box::new(Self {
            height,
            data,
            original,
            changed: original.is_none(),
            _charge: charge,
        }))
    }
    fn len(&self) -> usize {
        match &self.data {
            Data::Leaf(entries) => entries.len(),
            Data::Branch(children) => children.len(),
        }
    }
    fn first(&self) -> Option<ByteRef> {
        match &self.data {
            Data::Leaf(entries) => entries.first().map(|entry| entry.key),
            Data::Branch(children) => children.first().map(|child| child.first),
        }
    }
}
impl Link {
    fn dirty(node: Box<Node>) -> Self {
        Self {
            first: node.first().expect("nonempty dirty map node"),
            state: State::Dirty(node),
        }
    }
    fn changed(&self) -> bool {
        match &self.state {
            State::Stored { .. } => false,
            State::Dirty(node) => node.changed,
        }
    }
    fn height(&self) -> u8 {
        match &self.state {
            State::Stored { descriptor, .. } => descriptor.height,
            State::Dirty(node) => node.height,
        }
    }
    fn node(&self) -> &Node {
        match &self.state {
            State::Dirty(node) => node,
            State::Stored { .. } => unreachable!("materialized node"),
        }
    }
    fn node_mut(&mut self) -> &mut Node {
        match &mut self.state {
            State::Dirty(node) => node,
            State::Stored { .. } => unreachable!("materialized node"),
        }
    }
}

async fn materialize(
    pages: &PageStore,
    link: &mut Link,
    work: &mut WorkBudget,
) -> Result<(), ObjectLogError> {
    let State::Stored {
        descriptor,
        lower,
        upper,
        nonroot,
    } = &link.state
    else {
        return Ok(());
    };
    let (descriptor, lower, upper, nonroot) = (*descriptor, *lower, *upper, *nonroot);
    let source = VerifiedNode::read(pages, descriptor, lower, upper, work).await?;
    let minimum = if descriptor.height == 0 {
        LEAF_MIN
    } else {
        BRANCH_MIN
    };
    if nonroot && source.page().slots() < minimum {
        return Err(codec::invalid("stored map occupancy"));
    }
    let mut node = Node::new(pages, descriptor.height, Some(descriptor))?;
    match &mut node.data {
        Data::Leaf(entries) => {
            for slot in 0..source.page().slots() {
                entries.push(source.page().entry(slot)?);
            }
        }
        Data::Branch(children) => {
            for slot in 0..source.page().slots() {
                let child = source.page().child(slot)?;
                let slots = child.descriptor.validate()?;
                if slots
                    < if child.descriptor.height == 0 {
                        LEAF_MIN
                    } else {
                        BRANCH_MIN
                    }
                {
                    return Err(codec::invalid("stored map child occupancy"));
                }
                let next_upper = if slot + 1 < source.page().slots() {
                    Some(source.page().child(slot + 1)?.first)
                } else {
                    upper
                };
                children.push(Link {
                    first: child.first,
                    state: State::Stored {
                        descriptor: child.descriptor,
                        lower: Some(child.first),
                        upper: next_upper,
                        nonroot: true,
                    },
                });
            }
        }
    }
    link.first = node.first().expect("validated nonempty map page");
    link.state = State::Dirty(node);
    Ok(())
}

async fn position(
    pages: &PageStore,
    node: &Node,
    key: &[u8],
    work: &mut WorkBudget,
) -> Result<(usize, bool), ObjectLogError> {
    let mut low = 0;
    let mut high = node.len();
    while low < high {
        let middle = low + (high - low) / 2;
        let reference = match &node.data {
            Data::Leaf(entries) => entries[middle].key,
            Data::Branch(children) => children[middle].first,
        };
        match read::compare(pages, reference, key, work).await? {
            Ordering::Less => low = middle + 1,
            Ordering::Equal => return Ok((middle, true)),
            Ordering::Greater => high = middle,
        }
    }
    Ok((low, false))
}

struct Path {
    parent: Box<Node>,
    index: usize,
}
async fn update(
    pages: &PageStore,
    root: Link,
    key: &[u8],
    replacement: Option<ByteRef>,
    work: &mut WorkBudget,
) -> Result<Option<Link>, ObjectLogError> {
    let capacity = usize::from(MAX_HEIGHT) + 1;
    let _path_charge = pages.charge_output(capacity * std::mem::size_of::<Path>())?;
    let mut path = Vec::with_capacity(capacity);
    let mut current = root;
    loop {
        materialize(pages, &mut current, work).await?;
        if current.height() == 0 {
            break;
        }
        if path.len() >= usize::from(MAX_HEIGHT) {
            return Err(codec::invalid("map edit path height"));
        }
        let (position, equal) = position(pages, current.node(), key, work).await?;
        let index = if equal {
            position
        } else {
            position.saturating_sub(1)
        };
        let State::Dirty(mut parent) = current.state else {
            unreachable!()
        };
        let Data::Branch(children) = &mut parent.data else {
            unreachable!()
        };
        current = children.remove(index);
        path.push(Path { parent, index });
    }
    let (index, equal) = position(pages, current.node(), key, work).await?;
    let entry = if let (false, Some(value)) = (equal, replacement) {
        Some(Entry {
            key: bytes::store(pages, key, work).await?,
            value,
        })
    } else {
        None
    };
    let node = current.node_mut();
    let Data::Leaf(entries) = &mut node.data else {
        unreachable!()
    };
    match (equal, replacement) {
        (true, Some(value)) => {
            if entries[index].value != value {
                entries[index].value = value;
                node.changed = true;
            }
        }
        (true, None) => {
            entries.remove(index);
            node.changed = true;
        }
        (false, Some(_)) => {
            entries.insert(index, entry.unwrap());
            node.changed = true;
        }
        (false, None) => {}
    }
    let mut right_split = split(pages, current.node_mut())?;
    if let Some(first) = current.node().first() {
        current.first = first;
    }
    while let Some(Path { mut parent, index }) = path.pop() {
        let changed = current.changed() || right_split.is_some();
        let Data::Branch(children) = &mut parent.data else {
            unreachable!()
        };
        if current.node().len() != 0 {
            children.insert(index, current);
            if let Some(right) = right_split.take() {
                children.insert(index + 1, right);
            }
            if changed {
                rebalance(pages, children, index, work).await?;
            }
        }
        parent.changed |= changed;
        right_split = split(pages, &mut parent)?;
        if parent.len() == 0 {
            return Err(codec::invalid("map parent lost every child"));
        }
        current = Link::dirty(parent);
    }
    if current.node().len() == 0 {
        return Ok(None);
    }
    if let Some(right) = right_split {
        let height = current
            .height()
            .checked_add(1)
            .ok_or_else(|| codec::invalid("map height overflow"))?;
        let mut parent = Node::new(pages, height, None)?;
        let Data::Branch(children) = &mut parent.data else {
            unreachable!()
        };
        children.push(current);
        children.push(right);
        current = Link::dirty(parent);
    }
    loop {
        let collapse = matches!(&current.node().data,Data::Branch(children)if children.len()==1);
        if !collapse {
            break;
        }
        let State::Dirty(mut node) = current.state else {
            unreachable!()
        };
        let Data::Branch(children) = &mut node.data else {
            unreachable!()
        };
        current = children.remove(0);
        drop(node);
        if matches!(current.state, State::Stored { .. }) {
            break;
        }
    }
    Ok(Some(current))
}

fn split(pages: &PageStore, node: &mut Node) -> Result<Option<Link>, ObjectLogError> {
    let maximum = if node.height == 0 {
        LEAF_SLOTS
    } else {
        BRANCH_SLOTS
    };
    if node.len() <= maximum {
        return Ok(None);
    }
    let mut right = Node::new(pages, node.height, None)?;
    let middle = node.len() / 2;
    match (&mut node.data, &mut right.data) {
        (Data::Leaf(left), Data::Leaf(right)) => right.extend(left.drain(middle..)),
        (Data::Branch(left), Data::Branch(right)) => right.extend(left.drain(middle..)),
        _ => unreachable!(),
    }
    node.changed = true;
    Ok(Some(Link::dirty(right)))
}

async fn rebalance(
    pages: &PageStore,
    children: &mut Vec<Link>,
    index: usize,
    work: &mut WorkBudget,
) -> Result<(), ObjectLogError> {
    if children.len() < 2 {
        return Ok(());
    }
    let minimum = if children[index].height() == 0 {
        LEAF_MIN
    } else {
        BRANCH_MIN
    };
    if children[index].node().len() >= minimum {
        return Ok(());
    }
    let left = if index > 0 { index - 1 } else { index };
    let right = left + 1;
    materialize(pages, &mut children[left], work).await?;
    materialize(pages, &mut children[right], work).await?;
    let combined = children[left].node().len() + children[right].node().len();
    let maximum = if children[left].height() == 0 {
        LEAF_SLOTS
    } else {
        BRANCH_SLOTS
    };
    if children[left].height() != children[right].height() {
        return Err(codec::invalid("map rebalance sibling height"));
    }
    if combined <= maximum {
        let removed = children.remove(right);
        let State::Dirty(mut right_node) = removed.state else {
            unreachable!()
        };
        let left_node = children[left].node_mut();
        match (&mut left_node.data, &mut right_node.data) {
            (Data::Leaf(left), Data::Leaf(right)) => left.append(right),
            (Data::Branch(left), Data::Branch(right)) => left.append(right),
            _ => unreachable!(),
        }
        left_node.changed = true;
        children[left].first = children[left].node().first().expect("merged node nonempty");
    } else {
        let (left_half, right_half) = children.split_at_mut(right);
        let left_node = left_half[left].node_mut();
        let right_node = right_half[0].node_mut();
        let target = combined / 2;
        match (&mut left_node.data, &mut right_node.data) {
            (Data::Leaf(left), Data::Leaf(right)) => {
                while left.len() < target {
                    left.push(right.remove(0));
                }
                while left.len() > target {
                    right.insert(0, left.pop().unwrap());
                }
            }
            (Data::Branch(left), Data::Branch(right)) => {
                while left.len() < target {
                    left.push(right.remove(0));
                }
                while left.len() > target {
                    right.insert(0, left.pop().unwrap());
                }
            }
            _ => unreachable!(),
        }
        left_node.changed = true;
        right_node.changed = true;
        children[left].first = children[left].node().first().unwrap();
        children[right].first = children[right].node().first().unwrap();
    }
    Ok(())
}

struct FlushFrame {
    node: Box<Node>,
    done: Vec<Child>,
    _charge: Arc<Charge>,
}
async fn flush(
    pages: &PageStore,
    mut current: Link,
    work: &mut WorkBudget,
) -> Result<Descriptor, ObjectLogError> {
    let capacity = usize::from(MAX_HEIGHT) + 1;
    let _stack_charge = pages.charge_output(capacity * std::mem::size_of::<FlushFrame>())?;
    let mut stack: Vec<FlushFrame> = Vec::with_capacity(capacity);
    loop {
        let mut completed = match current.state {
            State::Stored { descriptor, .. } => Child {
                first: current.first,
                descriptor,
            },
            State::Dirty(mut node) => {
                if !node.changed {
                    Child {
                        first: current.first,
                        descriptor: node.original.expect("unchanged source node"),
                    }
                } else {
                    match &mut node.data {
                        Data::Leaf(entries) => {
                            work.page()?;
                            let (encoded, descriptor) = codec::leaf(pages, entries)?;
                            pages.write_scoped_bytes(encoded).await?;
                            Child {
                                first: entries[0].key,
                                descriptor,
                            }
                        }
                        Data::Branch(children) => {
                            if children.len() < 2 {
                                return Err(codec::invalid("uncollapsed map branch"));
                            }
                            if stack.len() >= usize::from(MAX_HEIGHT) {
                                return Err(codec::invalid("map flush height"));
                            }
                            let charge =
                                pages.charge_output(BRANCH_SLOTS * std::mem::size_of::<Child>())?;
                            let first = children.remove(0);
                            stack.push(FlushFrame {
                                node,
                                done: Vec::with_capacity(BRANCH_SLOTS),
                                _charge: charge,
                            });
                            current = first;
                            continue;
                        }
                    }
                }
            }
        };
        loop {
            let Some(frame) = stack.last_mut() else {
                return Ok(completed.descriptor);
            };
            frame.done.push(completed);
            let Data::Branch(children) = &mut frame.node.data else {
                unreachable!()
            };
            if !children.is_empty() {
                current = children.remove(0);
                break;
            }
            let frame = stack.pop().unwrap();
            work.page()?;
            let (encoded, descriptor) = codec::branch(pages, &frame.done)?;
            pages.write_scoped_bytes(encoded).await?;
            completed = Child {
                first: frame.done[0].first,
                descriptor,
            };
        }
    }
}

/// No output root is returned on failure/cancellation; only immutable prepared
/// objects may remain. The caller must couple the result to the original head.
pub(super) async fn apply(
    pages: &PageStore,
    root: Option<Descriptor>,
    edits: &[Edit<'_>],
    max_edits: u64,
    max_input_bytes: u64,
    work: &mut WorkBudget,
) -> Result<Option<Descriptor>, ObjectLogError> {
    pages.require_operation()?;
    if let Some(root) = root {
        root.validate()?;
    }
    if edits.len() as u64 > max_edits {
        return Err(ObjectLogError::BudgetExceeded("semantic edit count".into()));
    }
    let mut total = 0u64;
    for (index, edit) in edits.iter().enumerate() {
        if index > 0 && edits[index - 1].key >= edit.key {
            return Err(ObjectLogError::InvalidBatch(
                "semantic edits must have strictly increasing full keys".into(),
            ));
        }
        total = total
            .checked_add(edit.key.len() as u64)
            .and_then(|total| total.checked_add(edit.value.map_or(0, |value| value.len()) as u64))
            .ok_or_else(|| ObjectLogError::BudgetExceeded("semantic edit input overflow".into()))?;
        if total > max_input_bytes {
            return Err(ObjectLogError::BudgetExceeded(
                "semantic edit input bytes".into(),
            ));
        }
    }
    if edits.is_empty() {
        return Ok(root);
    }
    let empty = ByteRef::inline(b"")?;
    let mut root = root.map(|descriptor| Link {
        first: empty,
        state: State::Stored {
            descriptor,
            lower: None,
            upper: None,
            nonroot: false,
        },
    });
    for edit in edits {
        let replacement = match edit.value {
            Some(value) => Some(bytes::store(pages, value, work).await?),
            None => None,
        };
        root = match root {
            Some(current) => update(pages, current, edit.key, replacement, work).await?,
            None => {
                if let Some(value) = replacement {
                    let key = bytes::store(pages, edit.key, work).await?;
                    let mut node = Node::new(pages, 0, None)?;
                    let Data::Leaf(entries) = &mut node.data else {
                        unreachable!()
                    };
                    entries.push(Entry { key, value });
                    Some(Link::dirty(node))
                } else {
                    None
                }
            }
        };
    }
    match root {
        Some(root) => Ok(Some(flush(pages, root, work).await?)),
        None => Ok(None),
    }
}
