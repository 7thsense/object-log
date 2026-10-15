//! V2 keeps user-byte limits distinct from its authenticated transition frame.
use super::*;
use crate::native_v2::codec::Scope;
use sha2::{Digest, Sha256};
pub(super) const MAGIC: &[u8; 8] = b"OND2\0\0\0\x02";
pub(in crate::native_v2) const EXTENSION_BYTES: usize = 224;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::native_v2) struct Transition {
    pub scope: Scope,
    pub before: [u8; 32],
    pub after: [u8; 32],
    pub generation: u64,
    pub group_start: u64,
    pub group_end: u64,
    pub entry_count: u64,
    pub ordinal: u64,
    pub base: u64,
    pub records: u32,
}
impl Transition {
    fn validate(self) -> Result<(), ObjectLogError> {
        let end = self
            .base
            .checked_add(u64::from(self.records))
            .ok_or_else(invalid)?;
        if self.after == [0; 32]
            || self.generation == 0
            || self.group_start >= self.group_end
            || self.group_end > i64::MAX as u64
            || self.entry_count == 0
            || self.entry_count > self.group_end - self.group_start
            || self.ordinal >= self.entry_count
            || self.records == 0
            || self.records > i32::MAX as u32
            || self.base < self.group_start
            || end > self.group_end
            || (self.ordinal == 0 && self.base != self.group_start)
            || (self.ordinal + 1 == self.entry_count && end != self.group_end)
            || (self.generation == 1 && (self.before != [0; 32] || self.group_start != 0))
            || (self.generation > 1 && (self.before == [0; 32] || self.group_start == 0))
        {
            return Err(invalid());
        }
        Ok(())
    }
    fn encode(self, raw: &mut [u8]) {
        raw[..32].copy_from_slice(&self.scope.namespace);
        raw[32..64].copy_from_slice(&self.scope.incarnation);
        raw[64..96].copy_from_slice(&self.before);
        raw[96..128].copy_from_slice(&self.after);
        for (offset, value) in [
            (128, self.generation),
            (136, self.group_start),
            (144, self.group_end),
            (152, self.entry_count),
            (160, self.ordinal),
            (168, self.base),
        ] {
            raw[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
        }
        raw[176..180].copy_from_slice(&self.records.to_be_bytes());
    }
    fn decode(raw: &[u8]) -> Result<Self, ObjectLogError> {
        if raw.len() != EXTENSION_BYTES || raw[180..].iter().any(|b| *b != 0) {
            return Err(invalid());
        }
        let number = |offset| u64::from_be_bytes(raw[offset..offset + 8].try_into().unwrap());
        let value = Self {
            scope: Scope {
                namespace: raw[..32].try_into().unwrap(),
                incarnation: raw[32..64].try_into().unwrap(),
            },
            before: raw[64..96].try_into().unwrap(),
            after: raw[96..128].try_into().unwrap(),
            generation: number(128),
            group_start: number(136),
            group_end: number(144),
            entry_count: number(152),
            ordinal: number(160),
            base: number(168),
            records: u32::from_be_bytes(raw[176..180].try_into().unwrap()),
        };
        value.validate()?;
        Ok(value)
    }
}
impl DataFrame {
    pub fn coupled_encoded_len(
        identity: &str,
        payload_bytes: usize,
    ) -> Result<usize, ObjectLogError> {
        let length = Self::encoded_len(identity, payload_bytes)?
            .checked_add(EXTENSION_BYTES)
            .ok_or_else(invalid)?;
        u32::try_from(
            HEADER
                .checked_add(identity.len())
                .and_then(|n| n.checked_add(EXTENSION_BYTES))
                .ok_or_else(invalid)?,
        )
        .map_err(|_| invalid())?;
        Ok(length)
    }
    pub fn encode_coupled(
        pages: &PageStore,
        identity: &str,
        payload: &[u8],
        transition: Transition,
    ) -> Result<Self, ObjectLogError> {
        transition.validate()?;
        if transition.scope != pages.scope() {
            return Err(invalid());
        }
        let length = Self::coupled_encoded_len(identity, payload.len())?;
        let charge = Self::charge_frame(pages, length)?;
        let mut bytes = vec![0; length];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..16].copy_from_slice(&(identity.len() as u64).to_be_bytes());
        bytes[16..24].copy_from_slice(&(payload.len() as u64).to_be_bytes());
        let extension = HEADER + identity.len();
        bytes[HEADER..extension].copy_from_slice(identity.as_bytes());
        transition.encode(&mut bytes[extension..extension + EXTENSION_BYTES]);
        bytes[extension + EXTENSION_BYTES..].copy_from_slice(payload);
        Ok(Self {
            bytes,
            _charge: charge,
        })
    }
}
pub(super) fn decode(raw: &[u8]) -> Result<DataView<'_>, ObjectLogError> {
    if raw.len() < HEADER || &raw[..8] != MAGIC {
        return Err(invalid());
    }
    let identity_len = usize::try_from(u64::from_be_bytes(raw[8..16].try_into().unwrap()))
        .map_err(|_| invalid())?;
    let payload_len = usize::try_from(u64::from_be_bytes(raw[16..24].try_into().unwrap()))
        .map_err(|_| invalid())?;
    let extension = HEADER.checked_add(identity_len).ok_or_else(invalid)?;
    let start = extension.checked_add(EXTENSION_BYTES).ok_or_else(invalid)?;
    if start.checked_add(payload_len) != Some(raw.len()) {
        return Err(invalid());
    }
    let byte_start = u32::try_from(start).map_err(|_| invalid())?;
    u32::try_from(payload_len).map_err(|_| invalid())?;
    let identity = std::str::from_utf8(&raw[HEADER..extension]).map_err(|_| invalid())?;
    if identity.is_empty() || identity.contains('\0') {
        return Err(invalid());
    }
    Ok(DataView {
        raw,
        identity,
        payload: &raw[start..],
        byte_start,
        transition: Some(Transition::decode(&raw[extension..start])?),
    })
}
impl DataView<'_> {
    pub fn certificate(&self) -> [u8; 32] {
        if self.transition.is_some() {
            let mut digest = Sha256::new();
            digest.update(b"object-log/native-v2/coupled-frame/v1\0");
            digest.update(self.raw);
            digest.finalize().into()
        } else {
            Sha256::digest(self.payload).into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BlobStore, LocalBlobStore, MetadataLimits,
        native_v2::operation::{ExecutionConfig, Resources},
    };
    #[tokio::test]
    async fn complete_frames_bind_transitions_unequal_counts_and_orphan_payloads() {
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalBlobStore::new(directory.path()));
        let resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 1,
            bytes: 1 << 20,
        })
        .unwrap();
        let scope = Scope {
            namespace: [1; 32],
            incarnation: [2; 32],
        };
        let pages = PageStore::new(
            local.clone(),
            "frames".into(),
            scope,
            resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: 4096,
                version_bytes: 56,
            },
        )
        .unwrap();
        let (retained, weak) = resources
            .run(move |physical| async move {
                let weak = Arc::downgrade(&physical);
                let pages = pages.in_operation(physical)?;
                let first = Transition {
                    scope,
                    before: [3; 32],
                    after: [4; 32],
                    generation: 2,
                    group_start: 8,
                    group_end: 17,
                    entry_count: 2,
                    ordinal: 0,
                    base: 8,
                    records: 3,
                };
                let second = Transition {
                    ordinal: 1,
                    base: 11,
                    records: 6,
                    ..first
                };
                let identity = "same/雪";
                let mut certificates = Vec::new();
                for (transition, payload) in [
                    (first, b"orphan".as_slice()),
                    (first, b"retry".as_slice()),
                    (second, b"".as_slice()),
                ] {
                    let frame = DataFrame::encode_coupled(&pages, identity, payload, transition)?;
                    assert_eq!(
                        frame.bytes().len(),
                        DataFrame::coupled_encoded_len(identity, payload.len())?
                    );
                    let view = DataFrame::decode(frame.bytes())?;
                    assert_eq!(view.transition, Some(transition));
                    assert_eq!(view.payload, payload);
                    assert_eq!(view.identity, identity);
                    assert_eq!(
                        view.byte_start as usize,
                        HEADER + identity.len() + EXTENSION_BYTES
                    );
                    let mut oracle = Sha256::new();
                    oracle.update(b"object-log/native-v2/coupled-frame/v1\0");
                    oracle.update(frame.bytes());
                    assert_eq!(view.certificate(), <[u8; 32]>::from(oracle.finalize()));
                    let key = pages.transition_data_key(&view.certificate())?;
                    pages.upload_data(key.as_str(), frame.bytes()).await?;
                    assert_eq!(
                        local.get(key.as_str()).await?.unwrap().as_ref(),
                        frame.bytes()
                    );
                    certificates.push(view.certificate());
                    // A complete-frame certificate changes with every literal byte.
                    for offset in 0..frame.bytes().len() {
                        let mut changed = frame.bytes().to_vec();
                        changed[offset] ^= 1;
                        if let Ok(changed) = DataFrame::decode(&changed) {
                            assert_ne!(
                                changed.certificate(),
                                view.certificate(),
                                "offset {offset}"
                            );
                        }
                    }
                    let mut downgrade = frame.bytes().to_vec();
                    downgrade[7] = 1;
                    assert!(DataFrame::decode(&downgrade).is_err());
                }
                assert_ne!(certificates[0], certificates[1]);
                assert_ne!(certificates[1], certificates[2]);
                for bad in [
                    Transition {
                        ordinal: 2,
                        ..first
                    },
                    Transition {
                        records: 0,
                        ..first
                    },
                    Transition { base: 9, ..first },
                    Transition {
                        records: 5,
                        ..second
                    },
                    Transition {
                        before: [0; 32],
                        ..first
                    },
                    Transition {
                        group_end: u64::MAX,
                        ..first
                    },
                ] {
                    assert!(DataFrame::encode_coupled(&pages, identity, b"v", bad).is_err());
                }
                let genesis = Transition {
                    before: [0; 32],
                    generation: 1,
                    group_start: 0,
                    group_end: 1,
                    entry_count: 1,
                    ordinal: 0,
                    base: 0,
                    records: 1,
                    ..first
                };
                let retained = DataFrame::encode_coupled(&pages, identity, b"genesis", genesis)?;
                let legacy = DataFrame::encode(&pages, identity, b"legacy")?;
                assert_eq!(
                    DataFrame::decode(legacy.bytes())?.certificate(),
                    <[u8; 32]>::from(Sha256::digest(b"legacy"))
                );
                Ok((retained, weak))
            })
            .await
            .unwrap();
        resources.close_and_drain().await.unwrap();
        assert!(weak.upgrade().is_none());
        assert_eq!(
            DataFrame::decode(retained.bytes()).unwrap().payload,
            b"genesis"
        );
        assert!(resources.bytes.available() < 1 << 20);
        drop(retained);
        assert_eq!(resources.bytes.available(), 1 << 20);
    }
}
