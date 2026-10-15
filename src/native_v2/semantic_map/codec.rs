//! Structural page validation. Full-key ordering and inherited intervals also
//! require the map reader's authenticated byte-reference traversal.
use super::super::{
    codec::Scope,
    page_store::{EncodedBytes, PageStore, RetainedBytes},
};
use crate::ObjectLogError;

pub(super) const HEADER: usize = 96;
pub(super) const PAGE_BYTES: usize = 4096;
pub(super) const REF_BYTES: usize = 40;
pub(super) const DESC_BYTES: usize = 48;
pub(super) const LEAF_SLOTS: usize = (PAGE_BYTES - HEADER) / (REF_BYTES * 2);
pub(super) const BRANCH_SLOTS: usize = (PAGE_BYTES - HEADER) / (REF_BYTES + DESC_BYTES);
pub(super) const CHUNK_BYTES: usize = PAGE_BYTES - HEADER - REF_BYTES;
pub(super) const MAX_HEIGHT: u8 = 64;
const MAGIC: &[u8; 8] = b"ONM2\0\0\0\x01";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ByteRef {
    len: u64,
    storage: [u8; 32],
}
impl ByteRef {
    pub fn inline(bytes: &[u8]) -> Result<Self, ObjectLogError> {
        if bytes.len() > 32 {
            return Err(invalid("inline byte-reference length"));
        }
        let mut storage = [0; 32];
        storage[..bytes.len()].copy_from_slice(bytes);
        Ok(Self {
            len: bytes.len() as u64,
            storage,
        })
    }
    pub fn overflow(len: u64, hash: [u8; 32]) -> Result<Self, ObjectLogError> {
        if len <= 32 || hash == [0; 32] {
            return Err(invalid("overflow byte-reference length or hash"));
        }
        Ok(Self { len, storage: hash })
    }
    pub fn len(self) -> u64 {
        self.len
    }
    pub fn inline_bytes(&self) -> Option<&[u8]> {
        (self.len <= 32).then(|| &self.storage[..self.len as usize])
    }
    pub fn hash(self) -> Option<[u8; 32]> {
        (self.len > 32).then_some(self.storage)
    }
    pub fn encoded_page_len(self) -> Result<usize, ObjectLogError> {
        if self.len <= 32 {
            return Err(invalid("inline reference has no page"));
        }
        Ok(HEADER + REF_BYTES + self.len.min(CHUNK_BYTES as u64) as usize)
    }
    pub fn encode(self, raw: &mut [u8]) {
        raw[..8].copy_from_slice(&self.len.to_be_bytes());
        raw[8..40].copy_from_slice(&self.storage);
    }
    pub fn decode(raw: &[u8]) -> Result<Self, ObjectLogError> {
        if raw.len() != REF_BYTES {
            return Err(invalid("byte-reference framing"));
        }
        let value = Self {
            len: u64::from_be_bytes(raw[..8].try_into().unwrap()),
            storage: raw[8..40].try_into().unwrap(),
        };
        if value.len <= 32 {
            if value.storage[value.len as usize..].iter().any(|b| *b != 0) {
                return Err(invalid("inline byte-reference padding"));
            }
        } else if value.storage == [0; 32] {
            return Err(invalid("overflow byte-reference hash"));
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Descriptor {
    pub hash: [u8; 32],
    pub encoded_len: u16,
    pub height: u8,
    pub entries: u64,
}
impl Descriptor {
    pub fn validate(self) -> Result<usize, ObjectLogError> {
        let width = if self.height == 0 {
            REF_BYTES * 2
        } else {
            REF_BYTES + DESC_BYTES
        };
        let len = usize::from(self.encoded_len);
        if self.hash == [0; 32]
            || self.height > MAX_HEIGHT
            || !(HEADER..=PAGE_BYTES).contains(&len)
            || !(len - HEADER).is_multiple_of(width)
        {
            return Err(invalid("map descriptor"));
        }
        let slots = (len - HEADER) / width;
        if (self.height == 0 && (slots == 0 || slots > LEAF_SLOTS || self.entries != slots as u64))
            || (self.height > 0
                && (!(2..=BRANCH_SLOTS).contains(&slots) || self.entries < slots as u64))
        {
            return Err(invalid("map descriptor slots or count"));
        }
        Ok(slots)
    }
    pub fn encode(self, raw: &mut [u8]) {
        raw[..32].copy_from_slice(&self.hash);
        raw[32..34].copy_from_slice(&self.encoded_len.to_be_bytes());
        raw[34] = self.height;
        raw[35..40].fill(0);
        raw[40..48].copy_from_slice(&self.entries.to_be_bytes());
    }
    pub fn decode(raw: &[u8]) -> Result<Self, ObjectLogError> {
        if raw.len() != DESC_BYTES || raw[35..40] != [0; 5] {
            return Err(invalid("map descriptor framing"));
        }
        let value = Self {
            hash: raw[..32].try_into().unwrap(),
            encoded_len: u16::from_be_bytes(raw[32..34].try_into().unwrap()),
            height: raw[34],
            entries: u64::from_be_bytes(raw[40..48].try_into().unwrap()),
        };
        value.validate()?;
        Ok(value)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Entry {
    pub key: ByteRef,
    pub value: ByteRef,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Child {
    pub first: ByteRef,
    pub descriptor: Descriptor,
}

pub(super) struct NodePage {
    bytes: RetainedBytes,
    descriptor: Descriptor,
    slots: usize,
}
impl NodePage {
    pub fn descriptor(&self) -> Descriptor {
        self.descriptor
    }
    pub async fn read(pages: &PageStore, descriptor: Descriptor) -> Result<Self, ObjectLogError> {
        let slots = descriptor.validate()?;
        let bytes = pages
            .read_scoped_bytes(&descriptor.hash, descriptor.encoded_len as usize)
            .await?;
        let kind = if descriptor.height == 0 { 1 } else { 2 };
        check_header(
            bytes.bytes(),
            pages.scope(),
            kind,
            descriptor.height,
            slots,
            descriptor.entries,
        )?;
        let node = Self {
            bytes,
            descriptor,
            slots,
        };
        let mut count = 0u64;
        for index in 0..slots {
            if descriptor.height == 0 {
                node.entry(index)?;
            } else {
                let child = node.child(index)?;
                if child.descriptor.height.checked_add(1) != Some(descriptor.height) {
                    return Err(invalid("map child height"));
                }
                count = count
                    .checked_add(child.descriptor.entries)
                    .ok_or_else(|| invalid("map child count overflow"))?;
            }
        }
        if descriptor.height != 0 && count != descriptor.entries {
            return Err(invalid("map child count mismatch"));
        }
        Ok(node)
    }
    pub fn slots(&self) -> usize {
        self.slots
    }
    pub fn entry(&self, index: usize) -> Result<Entry, ObjectLogError> {
        if self.descriptor.height != 0 || index >= self.slots {
            return Err(invalid("map leaf slot"));
        }
        let start = HEADER + index * REF_BYTES * 2;
        let raw = &self.bytes.bytes()[start..start + REF_BYTES * 2];
        Ok(Entry {
            key: ByteRef::decode(&raw[..REF_BYTES])?,
            value: ByteRef::decode(&raw[REF_BYTES..])?,
        })
    }
    pub fn child(&self, index: usize) -> Result<Child, ObjectLogError> {
        if self.descriptor.height == 0 || index >= self.slots {
            return Err(invalid("map branch slot"));
        }
        let start = HEADER + index * (REF_BYTES + DESC_BYTES);
        let raw = &self.bytes.bytes()[start..start + REF_BYTES + DESC_BYTES];
        Ok(Child {
            first: ByteRef::decode(&raw[..REF_BYTES])?,
            descriptor: Descriptor::decode(&raw[REF_BYTES..])?,
        })
    }
}

pub(super) fn leaf(
    pages: &PageStore,
    entries: &[Entry],
) -> Result<(EncodedBytes, Descriptor), ObjectLogError> {
    if entries.is_empty() || entries.len() > LEAF_SLOTS {
        return Err(invalid("map leaf capacity"));
    }
    let len = HEADER + entries.len() * REF_BYTES * 2;
    let encoded = pages.encode_scoped_bytes(len, |raw, scope| {
        header(raw, scope, 1, 0, entries.len(), entries.len() as u64);
        for (entry, slot) in entries.iter().zip(
            raw[HEADER..]
                .as_chunks_mut::<{ REF_BYTES * 2 }>()
                .0
                .iter_mut(),
        ) {
            entry.key.encode(&mut slot[..REF_BYTES]);
            entry.value.encode(&mut slot[REF_BYTES..]);
        }
        Ok(())
    })?;
    let descriptor = Descriptor {
        hash: encoded.hash(),
        encoded_len: len as u16,
        height: 0,
        entries: entries.len() as u64,
    };
    descriptor.validate()?;
    Ok((encoded, descriptor))
}
pub(super) fn branch(
    pages: &PageStore,
    children: &[Child],
) -> Result<(EncodedBytes, Descriptor), ObjectLogError> {
    if children.len() < 2 || children.len() > BRANCH_SLOTS {
        return Err(invalid("map branch capacity"));
    }
    let height = children[0]
        .descriptor
        .height
        .checked_add(1)
        .filter(|h| *h <= MAX_HEIGHT)
        .ok_or_else(|| invalid("map height exhausted"))?;
    let mut count = 0u64;
    for child in children {
        child.descriptor.validate()?;
        if child.descriptor.height + 1 != height {
            return Err(invalid("map child height"));
        }
        count = count
            .checked_add(child.descriptor.entries)
            .ok_or_else(|| invalid("map child count overflow"))?;
    }
    let len = HEADER + children.len() * (REF_BYTES + DESC_BYTES);
    let encoded = pages.encode_scoped_bytes(len, |raw, scope| {
        header(raw, scope, 2, height, children.len(), count);
        for (child, slot) in children.iter().zip(
            raw[HEADER..]
                .as_chunks_mut::<{ REF_BYTES + DESC_BYTES }>()
                .0
                .iter_mut(),
        ) {
            child.first.encode(&mut slot[..REF_BYTES]);
            child.descriptor.encode(&mut slot[REF_BYTES..]);
        }
        Ok(())
    })?;
    let descriptor = Descriptor {
        hash: encoded.hash(),
        encoded_len: len as u16,
        height,
        entries: count,
    };
    descriptor.validate()?;
    Ok((encoded, descriptor))
}

pub(super) fn blob(
    pages: &PageStore,
    chunk: &[u8],
    next: ByteRef,
) -> Result<(EncodedBytes, ByteRef), ObjectLogError> {
    let total = (chunk.len() as u64)
        .checked_add(next.len())
        .ok_or_else(|| invalid("byte-reference length overflow"))?;
    if total <= 32 || chunk.len() != total.min(CHUNK_BYTES as u64) as usize {
        return Err(invalid("canonical overflow chunk length"));
    }
    let encoded = pages.encode_scoped_bytes(HEADER + REF_BYTES + chunk.len(), |raw, scope| {
        header(raw, scope, 3, 0, chunk.len(), total);
        next.encode(&mut raw[HEADER..HEADER + REF_BYTES]);
        raw[HEADER + REF_BYTES..].copy_from_slice(chunk);
        Ok(())
    })?;
    let reference = ByteRef::overflow(total, encoded.hash())?;
    Ok((encoded, reference))
}
pub(super) fn blob_view(
    raw: &[u8],
    scope: Scope,
    expected: ByteRef,
) -> Result<(&[u8], ByteRef), ObjectLogError> {
    let chunk = expected.len().min(CHUNK_BYTES as u64) as usize;
    if raw.len() != expected.encoded_page_len()? {
        return Err(invalid("overflow page length"));
    }
    check_header(raw, scope, 3, 0, chunk, expected.len())?;
    let next = ByteRef::decode(&raw[HEADER..HEADER + REF_BYTES])?;
    if next.len() != expected.len() - chunk as u64 {
        return Err(invalid("overflow remaining length"));
    }
    Ok((&raw[HEADER + REF_BYTES..], next))
}
fn header(raw: &mut [u8], scope: Scope, kind: u8, height: u8, slots: usize, count: u64) {
    let len = raw.len();
    raw[..8].copy_from_slice(MAGIC);
    raw[8..40].copy_from_slice(&scope.namespace);
    raw[40..72].copy_from_slice(&scope.incarnation);
    raw[72] = kind;
    raw[73] = height;
    raw[74..76].copy_from_slice(&(slots as u16).to_be_bytes());
    raw[76..84].copy_from_slice(&count.to_be_bytes());
    raw[84..88].copy_from_slice(&(len as u32).to_be_bytes());
    raw[88..96].fill(0);
}
fn check_header(
    raw: &[u8],
    scope: Scope,
    kind: u8,
    height: u8,
    slots: usize,
    count: u64,
) -> Result<(), ObjectLogError> {
    if raw.len() < HEADER
        || &raw[..8] != MAGIC
        || raw[8..40] != scope.namespace
        || raw[40..72] != scope.incarnation
        || raw[72] != kind
        || raw[73] != height
        || u16::from_be_bytes(raw[74..76].try_into().unwrap()) as usize != slots
        || u64::from_be_bytes(raw[76..84].try_into().unwrap()) != count
        || u32::from_be_bytes(raw[84..88].try_into().unwrap()) as usize != raw.len()
        || raw[88..96] != [0; 8]
    {
        return Err(invalid("map page header"));
    }
    Ok(())
}
pub(super) fn invalid(message: &str) -> ObjectLogError {
    ObjectLogError::Sequencer(message.into())
}
