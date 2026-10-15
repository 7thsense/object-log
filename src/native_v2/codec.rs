//! Bounded offset-tree wire format. Hashes are relative to a captured CAS head;
//! durable linearizable CAS and immutable committed objects remain assumptions.
use crate::ObjectLogError;
use sha2::{Digest, Sha256};

pub(super) const PAGE_BYTES: usize = 4096;
pub(super) const MAX_HEIGHT: u8 = 16;
pub(super) const HEADER: usize = 112;
const MAGIC: &[u8; 8] = b"ONP2\0\0\0\x02";
pub(super) const DESCRIPTOR_BYTES: usize = 64;
const LEAF_BYTES: usize = 192;
pub(super) const LEAF_SLOTS: usize = (PAGE_BYTES - HEADER) / LEAF_BYTES;
pub(super) const BRANCH_SLOTS: usize = (PAGE_BYTES - HEADER) / DESCRIPTOR_BYTES;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Scope {
    pub namespace: [u8; 32],
    pub incarnation: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Leaf = 1,
    Branch = 2,
    Overflow = 3,
}
impl Kind {
    fn decode(value: u8) -> Result<Self, ObjectLogError> {
        match value {
            1 => Ok(Self::Leaf),
            2 => Ok(Self::Branch),
            3 => Ok(Self::Overflow),
            _ => Err(invalid("unknown page kind")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Descriptor {
    pub hash: [u8; 32],
    pub encoded_len: u32,
    pub kind: Kind,
    pub height: u8,
    pub start: u64,
    pub end: u64,
    pub entries: u64,
}
impl Descriptor {
    pub fn validate(&self) -> Result<(), ObjectLogError> {
        if self.hash == [0; 32]
            || !(HEADER as u32..=PAGE_BYTES as u32).contains(&self.encoded_len)
            || self.start >= self.end
            || self.end > i64::MAX as u64
            || self.entries == 0
            || self.entries > self.end - self.start
            || self.height > MAX_HEIGHT
            || match self.kind {
                Kind::Leaf => self.height != 0 || self.entries > LEAF_SLOTS as u64,
                Kind::Branch => self.height == 0,
                Kind::Overflow => self.height != 0,
            }
        {
            return Err(invalid("invalid page descriptor"));
        }
        Ok(())
    }
    pub(crate) fn encode_into(&self, raw: &mut [u8]) {
        raw[..32].copy_from_slice(&self.hash);
        raw[32..36].copy_from_slice(&self.encoded_len.to_be_bytes());
        raw[36] = self.kind as u8;
        raw[37] = self.height;
        raw[38..40].fill(0);
        raw[40..48].copy_from_slice(&self.start.to_be_bytes());
        raw[48..56].copy_from_slice(&self.end.to_be_bytes());
        raw[56..64].copy_from_slice(&self.entries.to_be_bytes());
    }
    pub(crate) fn decode(raw: &[u8]) -> Result<Self, ObjectLogError> {
        if raw.len() != DESCRIPTOR_BYTES || raw[38..40] != [0, 0] {
            return Err(invalid("invalid descriptor framing"));
        }
        let value = Self {
            hash: raw[..32].try_into().expect("fixed range"),
            encoded_len: u32_at(raw, 32),
            kind: Kind::decode(raw[36])?,
            height: raw[37],
            start: u64_at(raw, 40),
            end: u64_at(raw, 48),
            entries: u64_at(raw, 56),
        };
        value.validate()?;
        Ok(value)
    }
}

/// Closed interpretation of the indexed certificate digest and data framing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FrameKind {
    Legacy = 0,
    Coupled = 1,
}
impl FrameKind {
    fn decode(value: u8) -> Result<Self, ObjectLogError> {
        match value {
            0 => Ok(Self::Legacy),
            1 => Ok(Self::Coupled),
            _ => Err(invalid("unknown data frame kind")),
        }
    }
}

/// The entire physical identity is committed via the overflow graph, not a
/// truncated key/hash equality substitute. Legacy hashes cover user bytes;
/// coupled hashes cover the complete domain-separated versioned frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LeafEntry {
    pub object_id: Descriptor,
    pub logical_id: Descriptor,
    pub base: u64,
    pub record_count: u32,
    pub byte_start: u32,
    pub byte_len: u32,
    pub payload_hash: [u8; 32],
    pub frame_kind: FrameKind,
}
impl LeafEntry {
    pub fn end(&self) -> Result<u64, ObjectLogError> {
        self.object_id.validate()?;
        self.logical_id.validate()?;
        if self.object_id.kind != Kind::Overflow
            || self.object_id.start != 0
            || self.logical_id.kind != Kind::Overflow
            || self.logical_id.start != 0
            || self.record_count == 0
            || self.record_count > i32::MAX as u32
            || self.payload_hash == [0; 32]
        {
            return Err(invalid("invalid leaf physical location or span"));
        }
        self.base
            .checked_add(u64::from(self.record_count))
            .filter(|end| *end <= i64::MAX as u64)
            .ok_or_else(|| invalid("leaf offset overflow"))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Page {
    Leaf(Vec<LeafEntry>),
    Branch {
        height: u8,
        children: Vec<Descriptor>,
    },
    /// A length-framed immutable chunk of the full UTF-8 object ID. Each next
    /// descriptor commits the remainder's exact byte interval and chunk count.
    Overflow {
        start: u64,
        bytes: Vec<u8>,
        next: Option<Descriptor>,
    },
}
impl Page {
    fn shape(&self) -> Result<(Kind, u8, u64, u64, u64, usize), ObjectLogError> {
        match self {
            Self::Leaf(entries) => {
                if entries.is_empty() || entries.len() > LEAF_SLOTS {
                    return Err(invalid("leaf slot limit"));
                }
                let start = entries[0].base;
                let mut end = start;
                for entry in entries {
                    if entry.base != end {
                        return Err(invalid("leaf gap or overlapping range"));
                    }
                    end = entry.end()?;
                }
                Ok((
                    Kind::Leaf,
                    0,
                    start,
                    end,
                    entries.len() as u64,
                    entries.len(),
                ))
            }
            Self::Branch { height, children } => {
                if *height == 0
                    || *height > MAX_HEIGHT
                    || children.is_empty()
                    || children.len() > BRANCH_SLOTS
                {
                    return Err(invalid("branch height or slot limit"));
                }
                let start = children[0].start;
                let mut end = start;
                let mut entries = 0u64;
                for child in children {
                    child.validate()?;
                    if child.kind == Kind::Overflow
                        || child.height + 1 != *height
                        || child.start != end
                    {
                        return Err(invalid("branch child kind, height or coverage"));
                    }
                    end = child.end;
                    entries = entries
                        .checked_add(child.entries)
                        .ok_or_else(|| invalid("branch count overflow"))?;
                }
                Ok((Kind::Branch, *height, start, end, entries, children.len()))
            }
            Self::Overflow { start, bytes, next } => {
                if bytes.is_empty() || bytes.len() > PAGE_BYTES - HEADER - DESCRIPTOR_BYTES {
                    return Err(invalid("overflow byte limit"));
                }
                let chunk_end = start
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid("overflow byte span"))?;
                let (end, entries) = match next {
                    Some(next) => {
                        next.validate()?;
                        if next.kind != Kind::Overflow || next.start != chunk_end {
                            return Err(invalid("overflow continuation interval"));
                        }
                        (
                            next.end,
                            next.entries
                                .checked_add(1)
                                .ok_or_else(|| invalid("overflow chunk count"))?,
                        )
                    }
                    None => (chunk_end, 1),
                };
                if end > i64::MAX as u64 {
                    return Err(invalid("overflow length domain"));
                }
                Ok((Kind::Overflow, 0, *start, end, entries, bytes.len()))
            }
        }
    }

    /// Caller reserves PAGE_BYTES and decoded page capacity before calling.
    pub fn encode(&self, scope: Scope) -> Result<(Vec<u8>, Descriptor), ObjectLogError> {
        validate_scope(scope)?;
        let (kind, height, start, end, entries, slots) = self.shape()?;
        let size = HEADER
            + match self {
                Self::Leaf(_) => slots * LEAF_BYTES,
                Self::Branch { .. } => slots * DESCRIPTOR_BYTES,
                Self::Overflow { .. } => DESCRIPTOR_BYTES + slots,
            };
        let mut raw = vec![0; size];
        raw[..8].copy_from_slice(MAGIC);
        raw[8..40].copy_from_slice(&scope.namespace);
        raw[40..72].copy_from_slice(&scope.incarnation);
        raw[72] = kind as u8;
        raw[73] = height;
        raw[76..80].copy_from_slice(&(slots as u32).to_be_bytes());
        raw[80..88].copy_from_slice(&start.to_be_bytes());
        raw[88..96].copy_from_slice(&end.to_be_bytes());
        raw[96..104].copy_from_slice(&entries.to_be_bytes());
        raw[104..108].copy_from_slice(&(size as u32).to_be_bytes());
        match self {
            Self::Leaf(entries) => {
                for (entry, slot) in entries
                    .iter()
                    .zip(raw[HEADER..].as_chunks_mut::<LEAF_BYTES>().0.iter_mut())
                {
                    entry.object_id.encode_into(&mut slot[..64]);
                    entry.logical_id.encode_into(&mut slot[128..192]);
                    slot[64..72].copy_from_slice(&entry.base.to_be_bytes());
                    slot[72..76].copy_from_slice(&entry.record_count.to_be_bytes());
                    slot[76..80].copy_from_slice(&entry.byte_start.to_be_bytes());
                    slot[80..84].copy_from_slice(&entry.byte_len.to_be_bytes());
                    slot[84] = entry.frame_kind as u8;
                    slot[88..120].copy_from_slice(&entry.payload_hash);
                }
            }
            Self::Branch { children, .. } => {
                for (child, slot) in children.iter().zip(
                    raw[HEADER..]
                        .as_chunks_mut::<DESCRIPTOR_BYTES>()
                        .0
                        .iter_mut(),
                ) {
                    child.encode_into(slot);
                }
            }
            Self::Overflow { bytes, next, .. } => {
                if let Some(next) = next {
                    next.encode_into(&mut raw[HEADER..HEADER + DESCRIPTOR_BYTES]);
                }
                raw[HEADER + DESCRIPTOR_BYTES..].copy_from_slice(bytes);
            }
        }
        let descriptor = Descriptor {
            hash: Sha256::digest(&raw).into(),
            encoded_len: size as u32,
            kind,
            height,
            start,
            end,
            entries,
        };
        descriptor.validate()?;
        Ok((raw, descriptor))
    }

    /// Verify framing/hash/scope before bounded decoded allocation. Every touched
    /// child's descriptor is subsequently checked against its complete page.
    pub fn decode(raw: &[u8], expected: Descriptor, scope: Scope) -> Result<Self, ObjectLogError> {
        validate_scope(scope)?;
        expected.validate()?;
        if raw.len() != expected.encoded_len as usize
            || raw.len() < HEADER
            || raw.len() > PAGE_BYTES
            || &raw[..8] != MAGIC
            || raw[8..40] != scope.namespace
            || raw[40..72] != scope.incarnation
            || raw[74..76] != [0, 0]
            || raw[108..112] != [0; 4]
            || u32_at(raw, 104) as usize != raw.len()
            || Sha256::digest(raw).as_slice() != expected.hash
            || Kind::decode(raw[72])? != expected.kind
            || raw[73] != expected.height
            || u64_at(raw, 80) != expected.start
            || u64_at(raw, 88) != expected.end
            || u64_at(raw, 96) != expected.entries
        {
            return Err(invalid("page hash, scope, descriptor or framing mismatch"));
        }
        let slots = u32_at(raw, 76) as usize;
        let page = match expected.kind {
            Kind::Leaf => {
                if slots == 0 || slots > LEAF_SLOTS || raw.len() != HEADER + slots * LEAF_BYTES {
                    return Err(invalid("leaf encoded slot length"));
                }
                let mut entries = Vec::with_capacity(slots);
                for slot in raw[HEADER..].as_chunks::<LEAF_BYTES>().0.iter() {
                    if slot[85..88] != [0; 3] || slot[120..128] != [0; 8] {
                        return Err(invalid("leaf reserved bytes"));
                    }
                    entries.push(LeafEntry {
                        object_id: Descriptor::decode(&slot[..64])?,
                        logical_id: Descriptor::decode(&slot[128..192])?,
                        base: u64_at(slot, 64),
                        record_count: u32_at(slot, 72),
                        byte_start: u32_at(slot, 76),
                        byte_len: u32_at(slot, 80),
                        payload_hash: slot[88..120].try_into().expect("fixed range"),
                        frame_kind: FrameKind::decode(slot[84])?,
                    });
                }
                Self::Leaf(entries)
            }
            Kind::Branch => {
                if slots == 0
                    || slots > BRANCH_SLOTS
                    || raw.len() != HEADER + slots * DESCRIPTOR_BYTES
                {
                    return Err(invalid("branch encoded slot length"));
                }
                let mut children = Vec::with_capacity(slots);
                for slot in raw[HEADER..].as_chunks::<DESCRIPTOR_BYTES>().0.iter() {
                    children.push(Descriptor::decode(slot)?);
                }
                Self::Branch {
                    height: expected.height,
                    children,
                }
            }
            Kind::Overflow => {
                if slots == 0
                    || slots > PAGE_BYTES - HEADER - DESCRIPTOR_BYTES
                    || raw.len() != HEADER + DESCRIPTOR_BYTES + slots
                {
                    return Err(invalid("overflow encoded byte length"));
                }
                let next = &raw[HEADER..HEADER + DESCRIPTOR_BYTES];
                Self::Overflow {
                    start: expected.start,
                    bytes: raw[HEADER + DESCRIPTOR_BYTES..].to_vec(),
                    next: if next == [0; DESCRIPTOR_BYTES] {
                        None
                    } else {
                        Some(Descriptor::decode(next)?)
                    },
                }
            }
        };
        let (kind, height, start, end, entries, _) = page.shape()?;
        if (kind, height, start, end, entries)
            != (
                expected.kind,
                expected.height,
                expected.start,
                expected.end,
                expected.entries,
            )
        {
            return Err(invalid("decoded page coverage or count mismatch"));
        }
        Ok(page)
    }
}

fn validate_scope(scope: Scope) -> Result<(), ObjectLogError> {
    if scope.namespace == [0; 32] || scope.incarnation == [0; 32] {
        return Err(invalid("zero immutable scope"));
    }
    Ok(())
}
fn u32_at(raw: &[u8], start: usize) -> u32 {
    u32::from_be_bytes(raw[start..start + 4].try_into().expect("validated framing"))
}
fn u64_at(raw: &[u8], start: usize) -> u64 {
    u64::from_be_bytes(raw[start..start + 8].try_into().expect("validated framing"))
}
fn invalid(message: &str) -> ObjectLogError {
    ObjectLogError::Sequencer(format!("native offset index: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> Scope {
        Scope {
            namespace: [1; 32],
            incarnation: [2; 32],
        }
    }
    fn object_id() -> Descriptor {
        Page::Overflow {
            start: 0,
            bytes: b"key".to_vec(),
            next: None,
        }
        .encode(scope())
        .unwrap()
        .1
    }
    fn entry(base: u64) -> LeafEntry {
        LeafEntry {
            object_id: object_id(),
            logical_id: object_id(),
            base,
            record_count: 3,
            byte_start: 0,
            byte_len: 4,
            payload_hash: Sha256::digest(b"data").into(),
            frame_kind: FrameKind::Legacy,
        }
    }
    // Recommit malformed bytes to distinguish structural checks from hash checks.
    fn recommit(raw: &[u8], mut descriptor: Descriptor) -> Descriptor {
        descriptor.hash = Sha256::digest(raw).into();
        descriptor
    }

    #[test]
    fn independent_overflow_golden_has_exact_framing_and_scope() {
        let page = Page::Overflow {
            start: 0,
            bytes: b"key".to_vec(),
            next: None,
        };
        let (raw, descriptor) = page.encode(scope()).unwrap();
        assert_eq!(raw.len(), 179);
        // Independently packed big-endian wire bytes, not this encoder's output.
        let golden = "d3f346c8d05271ec9d57482b1f57a3f26e3cf066ddf9bc7746e08acc7fd8ba8e";
        let actual: String = descriptor.hash.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(actual, golden);
        assert_eq!(Page::decode(&raw, descriptor, scope()).unwrap(), page);
        let mut wrong_scope = scope();
        wrong_scope.incarnation[0] ^= 1;
        assert!(Page::decode(&raw, descriptor, wrong_scope).is_err());
        assert!(Page::decode(&raw[..raw.len() - 1], descriptor, scope()).is_err());
        let mut extra = raw.clone();
        extra.push(0);
        assert!(Page::decode(&extra, descriptor, scope()).is_err());
        let mut corrupt = raw.clone();
        corrupt[176] ^= 1;
        assert!(Page::decode(&corrupt, descriptor, scope()).is_err());
    }

    #[test]
    fn leaf_and_branch_have_fixed_positive_exact_coverage() {
        let leaf = Page::Leaf((0..LEAF_SLOTS).map(|n| entry(n as u64 * 3)).collect());
        let (raw, first) = leaf.encode(scope()).unwrap();
        assert_eq!(raw.len(), HEADER + LEAF_SLOTS * LEAF_BYTES);
        assert!(raw.len() <= PAGE_BYTES);
        assert_eq!(Page::decode(&raw, first, scope()).unwrap(), leaf);
        let second = Page::Leaf(vec![entry(first.end)])
            .encode(scope())
            .unwrap()
            .1;
        let branch = Page::Branch {
            height: 1,
            children: vec![first, second],
        };
        let (raw, root) = branch.encode(scope()).unwrap();
        assert_eq!(root.entries, first.entries + 1);
        assert_eq!(root.end, second.end);
        assert_eq!(Page::decode(&raw, root, scope()).unwrap(), branch);
        assert!(
            Page::Branch {
                height: 1,
                children: vec![second, first]
            }
            .encode(scope())
            .is_err()
        );
        assert!(
            Page::Branch {
                height: 2,
                children: vec![first]
            }
            .encode(scope())
            .is_err()
        );
        assert!(
            Page::Branch {
                height: 1,
                children: vec![object_id()]
            }
            .encode(scope())
            .is_err()
        );
        assert!(
            Page::Leaf(vec![entry(0); LEAF_SLOTS + 1])
                .encode(scope())
                .is_err()
        );
        assert!(
            Page::Leaf(vec![entry(0), entry(4)])
                .encode(scope())
                .is_err()
        );
    }

    #[test]
    fn frame_kind_is_committed_closed_and_preserves_legacy_zero() {
        let legacy = entry(0);
        let (raw, descriptor) = Page::Leaf(vec![legacy]).encode(scope()).unwrap();
        assert_eq!(raw[HEADER + 84], 0);
        let coupled = LeafEntry {
            frame_kind: FrameKind::Coupled,
            ..legacy
        };
        let (coupled_raw, coupled_descriptor) = Page::Leaf(vec![coupled]).encode(scope()).unwrap();
        assert_eq!(coupled_raw[HEADER + 84], 1);
        assert_ne!(descriptor.hash, coupled_descriptor.hash);
        assert_ne!(legacy, coupled);
        assert_eq!(
            Page::decode(&coupled_raw, coupled_descriptor, scope()).unwrap(),
            Page::Leaf(vec![coupled])
        );
        for kind in [2, 127, 255] {
            let mut bad = raw.clone();
            bad[HEADER + 84] = kind;
            assert!(Page::decode(&bad, recommit(&bad, descriptor), scope()).is_err());
        }
    }

    #[test]
    fn recommitted_bad_lengths_counts_reserved_bytes_and_locations_fail() {
        let (raw, descriptor) = Page::Leaf(vec![entry(0)]).encode(scope()).unwrap();
        for byte in [74, 108, HEADER + 85, HEADER + 120] {
            let mut corrupt = raw.clone();
            corrupt[byte] = 1;
            assert!(
                Page::decode(&corrupt, recommit(&corrupt, descriptor), scope()).is_err(),
                "byte={byte}"
            );
        }
        for slot_count in [0u32, 2, u32::MAX] {
            let mut corrupt = raw.clone();
            corrupt[76..80].copy_from_slice(&slot_count.to_be_bytes());
            assert!(Page::decode(&corrupt, recommit(&corrupt, descriptor), scope()).is_err());
        }
        let mut zero_count = raw.clone();
        zero_count[HEADER + 72..HEADER + 76].fill(0);
        assert!(Page::decode(&zero_count, recommit(&zero_count, descriptor), scope()).is_err());
        let mut no_digest = raw.clone();
        no_digest[HEADER + 88..HEADER + 120].fill(0);
        assert!(Page::decode(&no_digest, recommit(&no_digest, descriptor), scope()).is_err());
        let mut wrong_count = descriptor;
        wrong_count.entries = 2;
        assert!(Page::decode(&raw, wrong_count, scope()).is_err());
        let mut bad_span = entry(i64::MAX as u64 - 1);
        assert!(Page::Leaf(vec![bad_span]).encode(scope()).is_err());
        bad_span.base = 0;
        bad_span.byte_start = u32::MAX;
        // Both fields are u32; their physical range end is computed in u64.
        assert!(Page::Leaf(vec![bad_span]).encode(scope()).is_ok());
        let mut empty_payload = entry(0);
        empty_payload.byte_len = 0;
        empty_payload.payload_hash = Sha256::digest([]).into();
        assert!(Page::Leaf(vec![empty_payload]).encode(scope()).is_ok());
    }

    #[test]
    fn overflow_continuations_bind_full_intervals_and_counts() {
        let tail = Page::Overflow {
            start: 4,
            bytes: b"long/key".to_vec(),
            next: None,
        };
        let (tail_bytes, tail_descriptor) = tail.encode(scope()).unwrap();
        let first = Page::Overflow {
            start: 0,
            bytes: b"very".to_vec(),
            next: Some(tail_descriptor),
        };
        let (raw, descriptor) = first.encode(scope()).unwrap();
        assert_eq!(
            (descriptor.start, descriptor.end, descriptor.entries),
            (0, 12, 2)
        );
        assert_eq!(Page::decode(&raw, descriptor, scope()).unwrap(), first);
        assert_eq!(
            Page::decode(&tail_bytes, tail_descriptor, scope()).unwrap(),
            tail
        );
        let mut gap = tail_descriptor;
        gap.start = 5;
        assert!(
            Page::Overflow {
                start: 0,
                bytes: b"very".to_vec(),
                next: Some(gap)
            }
            .encode(scope())
            .is_err()
        );
        assert!(
            Page::Overflow {
                start: 0,
                bytes: vec![1; PAGE_BYTES - HEADER - DESCRIPTOR_BYTES + 1],
                next: None
            }
            .encode(scope())
            .is_err()
        );
        let mut changed_count = raw.clone();
        changed_count[96..104].copy_from_slice(&3u64.to_be_bytes());
        let mut expected = recommit(&changed_count, descriptor);
        expected.entries = 3;
        assert!(Page::decode(&changed_count, expected, scope()).is_err());
    }
}
