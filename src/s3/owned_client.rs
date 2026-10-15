//! Owned S3 transport configuration, trust policy and construction controls.
use crate::ObjectLogError;
use bytes::Bytes;
use std::time::Duration;

/// Explicit owned-client transport configuration. Application timeouts/signing,
/// proxy selection and TLS trust do not inherit process environment. AWS request
/// metadata interceptors are outside this transport-configuration guarantee.
/// `max_config_bytes` bounds the total borrowed input bytes before parsing or
/// copying; it does not measure AWS HTTP/TLS pools or total client residency.
#[derive(Clone)]
pub struct S3ClientOptions {
    /// Connection establishment timeout; positive and at most one day.
    pub connect_timeout: Duration,
    /// Response read timeout; positive and at most one day.
    pub read_timeout: Duration,
    /// Complete SDK operation timeout; positive and at most one day.
    pub operation_timeout: Duration,
    /// Select SigV4 unsigned-payload requests when true.
    pub disable_payload_signing: bool,
    /// Maximum total endpoint, region, bucket, credential and additive PEM input bytes; at most one MiB.
    pub max_config_bytes: usize,
    /// Optional additive CA certificates, PEM only; bounded with caller inputs.
    /// Compiled Mozilla roots are always enabled; platform trust is never loaded.
    pub additional_ca_pem: Option<Bytes>,
}
impl Default for S3ClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
            disable_payload_signing: false,
            max_config_bytes: 16 * 1024,
            additional_ca_pem: None,
        }
    }
}
impl std::fmt::Debug for S3ClientOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ClientOptions")
            .field("connect_timeout", &self.connect_timeout)
            .field("read_timeout", &self.read_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("disable_payload_signing", &self.disable_payload_signing)
            .field("max_config_bytes", &self.max_config_bytes)
            .field(
                "additional_ca_pem_bytes",
                &self.additional_ca_pem.as_ref().map(Bytes::len),
            )
            .finish()
    }
}
// Records the admitted configuration bound for the lifetime of the owning
// client/store. Client-internal copies and pools are not claimed as page credit.
pub(super) struct OwnedClientConfiguration {
    pub(super) endpoint_url: url::Url,
    _input_bytes: usize,
    _limit: usize,
}
fn validate_owned_configuration(
    endpoint: &str,
    region: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    options: &S3ClientOptions,
) -> Result<OwnedClientConfiguration, ObjectLogError> {
    let invalid = || ObjectLogError::InvalidConfig("invalid owned S3 configuration".into());
    if options.max_config_bytes == 0 || options.max_config_bytes > 1024 * 1024 {
        return Err(invalid());
    }
    let mut total = 0usize;
    for value in [endpoint, region, bucket, access_key, secret_key] {
        total = total.checked_add(value.len()).ok_or_else(invalid)?;
        if value.is_empty()
            || value.bytes().any(|b| b.is_ascii_control())
            || total > options.max_config_bytes
        {
            return Err(invalid());
        }
    }
    if let Some(pem) = &options.additional_ca_pem {
        total = total.checked_add(pem.len()).ok_or_else(invalid)?;
        if total > options.max_config_bytes {
            return Err(invalid());
        }
        validate_additional_ca(pem)?;
    }
    for timeout in [
        options.connect_timeout,
        options.read_timeout,
        options.operation_timeout,
    ] {
        if timeout.is_zero() || timeout > Duration::from_secs(24 * 60 * 60) {
            return Err(invalid());
        }
    }
    if endpoint.bytes().any(|b| b.is_ascii_whitespace())
        || endpoint.contains('\\')
        || !endpoint.contains("://")
    {
        return Err(invalid());
    }
    // All parser allocations occur after the checked input bound. Canonical
    // spelling is retained and used by AWS rather than validating one spelling
    // and submitting a different one. Its bounded expansion is separate from
    // the maximum borrowed configuration input.
    let url = url::Url::parse(endpoint).map_err(|_| invalid())?;
    let canonical_limit = options
        .max_config_bytes
        .checked_mul(3)
        .and_then(|n| n.checked_add(16))
        .ok_or_else(invalid)?;
    if url.as_str().len() > canonical_limit {
        return Err(invalid());
    }
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid());
    }
    Ok(OwnedClientConfiguration {
        endpoint_url: url,
        _input_bytes: total,
        _limit: options.max_config_bytes,
    })
}
fn validate_additional_ca(pem: &[u8]) -> Result<(), ObjectLogError> {
    let invalid = || ObjectLogError::InvalidConfig("invalid owned S3 trust configuration".into());
    use rustls::pki_types::pem::{PemObject, SectionKind};
    let mut roots = rustls::RootCertStore::empty();
    let mut count = 0usize;
    for item in <(SectionKind, Vec<u8>)>::pem_slice_iter(pem) {
        let (SectionKind::Certificate, cert) = item.map_err(|_| invalid())? else {
            return Err(invalid());
        };
        roots.add(cert.into()).map_err(|_| invalid())?;
        count += 1;
    }
    if count == 0 {
        return Err(invalid());
    }
    Ok(())
}
fn owned_tls_context(
    additional: Option<&[u8]>,
) -> Result<aws_smithy_http_client::tls::TlsContext, ObjectLogError> {
    use base64::Engine;
    let invalid =
        || ObjectLogError::InvalidConfig("invalid compiled S3 trust configuration".into());
    // Fixed package data is bounded separately from caller input. Certificate
    // memory is transport configuration, not native page-byte ledger credit.
    const MAX_COMPILED_PEM: usize = 1024 * 1024;
    let mut pem = Vec::new();
    for cert in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
        let encoded = base64::engine::general_purpose::STANDARD.encode(cert.as_ref());
        let needed = encoded
            .len()
            .checked_add(encoded.len().div_ceil(64))
            .and_then(|n| n.checked_add(56))
            .and_then(|n| n.checked_add(pem.len()))
            .ok_or_else(invalid)?;
        if needed > MAX_COMPILED_PEM {
            return Err(invalid());
        }
        pem.extend_from_slice(b"-----BEGIN CERTIFICATE-----\n");
        for line in encoded.as_bytes().chunks(64) {
            pem.extend_from_slice(line);
            pem.push(b'\n');
        }
        pem.extend_from_slice(b"-----END CERTIFICATE-----\n");
    }
    validate_additional_ca(&pem)?;
    let mut trust = aws_smithy_http_client::tls::TrustStore::empty().with_pem_certificate(pem);
    if let Some(extra) = additional {
        trust = trust.with_pem_certificate(extra.to_vec());
    }
    aws_smithy_http_client::tls::TlsContext::builder()
        .with_trust_store(trust)
        .build()
        .map_err(|_| invalid())
}
pub(super) fn build_owned(
    endpoint_url: &str,
    region: &str,
    bucket: &str,
    access_key_id: &str,
    secret_access_key: &str,
    options: &S3ClientOptions,
) -> Result<(aws_sdk_s3::Client, OwnedClientConfiguration), ObjectLogError> {
    let admitted = validate_owned_configuration(
        endpoint_url,
        region,
        bucket,
        access_key_id,
        secret_access_key,
        options,
    )?;
    let tls = owned_tls_context(options.additional_ca_pem.as_deref())?;
    let http = aws_smithy_http_client::Builder::new().build_with_connector_fn(
        move |settings, components| {
            let mut connector = aws_smithy_http_client::Connector::builder();
            connector.set_connector_settings(settings.cloned());
            if let Some(components) = components {
                connector.set_sleep_impl(components.sleep_impl());
            }
            connector
                .proxy_config(aws_smithy_http_client::proxy::ProxyConfig::disabled())
                .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                    aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
                ))
                .tls_context(tls.clone())
                .build()
        },
    );
    let client = aws_sdk_s3::Client::from_conf(
        super::s3_client_config(
            admitted.endpoint_url.as_str(),
            region,
            access_key_id,
            secret_access_key,
            options.connect_timeout,
            options.read_timeout,
            options.operation_timeout,
        )
        .http_client(http)
        .build(),
    );
    Ok((client, admitted))
}
#[cfg(test)]
#[path = "owned_client/tests.rs"]
mod owned_client_tests;
