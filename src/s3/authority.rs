//! Metadata responses are validated before SDK deserialization/body collection.
use super::*;
use crate::blob::authority::{MetadataVersion, validate_budget};
use crate::{
    AppendOwner, BoundedMetadataStore, MetadataLimits, MetadataOwners, MetadataSnapshot,
    MetadataSwap,
};
use std::sync::{Arc, atomic::AtomicU16};

// Hyper can retain the source body after the SDK response future is dropped.
// The bytes precede their owner so source payload cleanup releases it last.
struct OwnedMetadataBody {
    bytes: Bytes,
    _physical: AppendOwner,
}
impl AsRef<[u8]> for OwnedMetadataBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug)]
struct MetadataResponse {
    status: Arc<AtomicU16>,
    max_bytes: usize,
    version_bytes: usize,
    reading: bool,
    #[cfg(test)]
    loss_counter: Option<Arc<AtomicU64>>,
}
impl aws_sdk_s3::config::Intercept for MetadataResponse {
    fn name(&self) -> &'static str {
        "BoundedMetadataResponse"
    }
    fn read_before_transmit(
        &self,
        _context: &aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef<'_>,
        _runtime: &aws_sdk_s3::config::RuntimeComponents,
        _config: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.status.store(0, Ordering::Relaxed);
        #[cfg(test)]
        if let Some(counter) = &self.loss_counter {
            counter.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
    fn read_before_deserialization(
        &self,
        context: &aws_sdk_s3::config::interceptors::BeforeDeserializationInterceptorContextRef<'_>,
        _runtime: &aws_sdk_s3::config::RuntimeComponents,
        _config: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let response = context.response();
        let status = response.status().as_u16();
        self.status.store(status, Ordering::Relaxed);
        if (self.reading && status == 404) || (!self.reading && status == 412) {
            // Classify by status without letting error XML collect a body.
            return Err(Box::new(unavailable("metadata conditional status")));
        }
        if response.headers().get("transfer-encoding").is_some()
            || response.headers().get("content-encoding").is_some()
        {
            return Err(Box::new(unavailable(
                "unsupported metadata response framing",
            )));
        }
        let length = response
            .headers()
            .get("content-length")
            .and_then(|s| s.parse::<usize>().ok());
        let expected_status = if self.reading {
            status == 200
        } else {
            status == 200 || status == 409
        };
        // Only a bounded 409 error may enter SDK parsing (to distinguish its
        // exact ConditionalRequestConflict code from unrelated conflicts).
        let limit = if status == 409 { 4096 } else { self.max_bytes };
        if !expected_status || length.is_none_or(|n| n > limit) {
            return Err(Box::new(unavailable(
                "invalid or oversized metadata response",
            )));
        }
        let token = response.headers().get("etag");
        if (self.reading && token.is_none_or(str::is_empty))
            || token.is_some_and(|token| token.len() > self.version_bytes)
        {
            return Err(Box::new(unavailable("metadata response lacks version")));
        }
        #[cfg(test)]
        if !self.reading && status == 200 && self.loss_counter.is_some() {
            return Err(Box::new(unavailable("injected lost SDK metadata response")));
        }
        Ok(())
    }
}
impl S3BlobStore {
    #[cfg(test)]
    pub(crate) fn lose_next_metadata_reply(&self, key: &str) -> Arc<AtomicU64> {
        let counter = Arc::new(AtomicU64::new(0));
        *self.authority_lost_reply.lock().unwrap() = Some((key.to_owned(), counter.clone()));
        counter
    }
    async fn read_metadata_inner(
        &self,
        key: &str,
        limits: MetadataLimits,
        owners: MetadataOwners,
    ) -> Result<MetadataSnapshot, ObjectLogError> {
        limits.validate()?;
        let max_bytes = limits.payload_bytes;
        let status = Arc::new(AtomicU16::new(0));
        let request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .customize()
            .config_override(
                aws_sdk_s3::config::Builder::new()
                    .retry_config(
                        aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1),
                    )
                    .interceptor(MetadataResponse {
                        status: Arc::clone(&status),
                        max_bytes,
                        version_bytes: limits.version_bytes,
                        reading: true,
                        #[cfg(test)]
                        loss_counter: None,
                    }),
            );
        let mut response = match request.send().await {
            Ok(response) => response,
            Err(_) if status.load(Ordering::Relaxed) == 404 => {
                return Ok(MetadataSnapshot {
                    inner: Arc::new(crate::blob::authority::MetadataObservation {
                        _retained: Arc::clone(&owners.retained),
                        owner: Arc::clone(&self.authority_identity),
                        key: key.to_owned(),
                        bytes: None,
                        version: MetadataVersion::S3(None),
                    }),
                });
            }
            Err(error) => return Err(unavailable(error)),
        };
        let size = response
            .content_length()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| unavailable("invalid metadata length"))?;
        validate_budget(max_bytes, size)?;
        let etag = response
            .e_tag()
            .filter(|s| !s.is_empty() && s.len() <= limits.version_bytes)
            .ok_or_else(|| unavailable("metadata version missing"))?
            .to_owned();
        let mut bytes = Vec::with_capacity(size);
        while let Some(chunk) = response.body.next().await {
            let chunk = chunk.map_err(unavailable)?;
            if bytes
                .len()
                .checked_add(chunk.len())
                .is_none_or(|n| n > size)
            {
                return Err(unavailable("metadata body exceeds declared bound"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != size {
            return Err(unavailable("metadata body is truncated"));
        }
        Ok(MetadataSnapshot {
            inner: Arc::new(crate::blob::authority::MetadataObservation {
                _retained: Arc::clone(&owners.retained),
                owner: Arc::clone(&self.authority_identity),
                key: key.to_owned(),
                bytes: Some(Bytes::from(bytes)),
                version: MetadataVersion::S3(Some(etag)),
            }),
        })
    }
}
#[async_trait]
impl BoundedMetadataStore for S3BlobStore {
    async fn read_metadata(
        &self,
        key: &str,
        limits: MetadataLimits,
        owners: MetadataOwners,
    ) -> Result<MetadataSnapshot, ObjectLogError> {
        self.read_metadata_inner(key, limits, owners.clone()).await
    }
    async fn swap_metadata(
        &self,
        key: &str,
        observed: MetadataSnapshot,
        new_value: Bytes,
        limits: MetadataLimits,
        owners: MetadataOwners,
    ) -> Result<MetadataSwap, ObjectLogError> {
        limits.validate()?;
        let max_bytes = limits.payload_bytes;
        observed.validate(&self.authority_identity, key)?;
        if matches!(&observed.inner.version, MetadataVersion::S3(Some(token)) if token.len() > limits.version_bytes)
        {
            return Err(ObjectLogError::BudgetExceeded(
                "metadata version exceeds admitted bound".into(),
            ));
        }
        validate_budget(max_bytes, new_value.len())?;
        validate_budget(
            max_bytes,
            observed.inner.bytes.as_ref().map_or(0, Bytes::len),
        )?;
        let MetadataVersion::S3(etag) = &observed.inner.version else {
            return Err(ObjectLogError::InvalidConfig(
                "non-S3 metadata token".into(),
            ));
        };
        #[cfg(test)]
        let loss_counter = {
            let mut requested = self.authority_lost_reply.lock().unwrap();
            if requested
                .as_ref()
                .is_some_and(|(requested, _)| requested == key)
            {
                requested.take().map(|(_, counter)| counter)
            } else {
                None
            }
        };
        let size = new_value.len() as u64;
        let request =
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from(Bytes::from_owner(OwnedMetadataBody {
                    bytes: new_value,
                    _physical: owners.source(),
                })));
        let request = match etag {
            Some(etag) => request.if_match(etag.clone()),
            None => request.if_none_match("*"),
        };
        let status = Arc::new(AtomicU16::new(0));
        let request = request.customize().config_override(
            aws_sdk_s3::config::Builder::new()
                .retry_config(
                    aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1),
                )
                .interceptor(MetadataResponse {
                    status: Arc::clone(&status),
                    max_bytes: 0,
                    version_bytes: limits.version_bytes,
                    reading: false,
                    #[cfg(test)]
                    loss_counter,
                }),
        );
        let result = if self.disable_payload_signing {
            request.disable_payload_signing().send().await
        } else {
            request.send().await
        };
        match result {
            Ok(_) => {
                self.record_media(1, size);
                Ok(MetadataSwap::Stored)
            }
            Err(error)
                if status.load(Ordering::Relaxed) == 412 || s3_precondition_failed(&error) =>
            {
                // Failure/oversize here remains unknown, never a head rejection.
                Ok(MetadataSwap::Conflict(
                    self.read_metadata_inner(key, limits, owners.clone())
                        .await?,
                ))
            }
            Err(error) => Err(unavailable(error)),
        }
    }
}
