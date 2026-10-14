use super::*;
use crate::{BlobStore, S3BlobStore};
#[test]
fn owned_configuration_bounds_and_errors_are_redacted() {
    let options = S3ClientOptions::default();
    for endpoint in [
        "",
        "file:///tmp/data",
        " http://example.com",
        "http:example.com",
        "http://example.com\\path",
        "https://user:secret@example.com",
        "https://example.com?q=secret",
        "https://example.com/#secret",
    ] {
        let error =
            validate_owned_configuration(endpoint, "region", "bucket", "key", "secret", &options)
                .err()
                .unwrap();
        assert_eq!(
            error.to_string(),
            "invalid configuration: invalid owned S3 configuration"
        );
        assert!(
            S3BlobStore::new_owned_with_options(
                endpoint,
                "region",
                "bucket",
                "key",
                "secret",
                options.clone()
            )
            .is_err()
        );
    }
    let endpoint = "http://127.0.0.1:1";
    let total = endpoint.len() + "regionbucketkeysecret".len();
    assert!(
        validate_owned_configuration(
            endpoint,
            "region",
            "bucket",
            "key",
            "secret",
            &S3ClientOptions {
                max_config_bytes: total - 1,
                ..options.clone()
            }
        )
        .is_err()
    );
    assert!(
        validate_owned_configuration(
            endpoint,
            "region",
            "bucket",
            "key",
            "secret",
            &S3ClientOptions {
                max_config_bytes: total,
                ..options.clone()
            }
        )
        .is_ok()
    );
    for timeout in [Duration::ZERO, Duration::from_secs(86401)] {
        assert!(
            S3BlobStore::new_owned_with_options(
                endpoint,
                "region",
                "bucket",
                "key",
                "secret",
                S3ClientOptions {
                    operation_timeout: timeout,
                    ..options.clone()
                }
            )
            .is_err()
        );
        assert!(
            validate_owned_configuration(
                endpoint,
                "region",
                "bucket",
                "key",
                "secret",
                &S3ClientOptions {
                    operation_timeout: timeout,
                    ..options.clone()
                }
            )
            .is_err()
        );
    }
}
#[test]
fn malformed_or_oversized_trust_is_rejected_before_client_activity() {
    for pem in [
        b"not PEM".as_slice(),
        b"-----BEGIN CERTIFICATE-----\nAQ==\n-----END CERTIFICATE-----\n",
        b"-----BEGIN PRIVATE KEY-----\nAQ==\n-----END PRIVATE KEY-----\n",
    ] {
        let options = S3ClientOptions {
            additional_ca_pem: Some(Bytes::copy_from_slice(pem)),
            ..Default::default()
        };
        let error = S3BlobStore::new_owned_with_options(
            "https://localhost",
            "region",
            "bucket",
            "key",
            "secret",
            options,
        )
        .err()
        .unwrap();
        assert!(!error.to_string().contains("secret"));
    }
    let options = S3ClientOptions {
        additional_ca_pem: Some(Bytes::from(vec![0; 16385])),
        ..Default::default()
    };
    assert!(
        S3BlobStore::new_owned_with_options(
            "https://localhost",
            "region",
            "bucket",
            "key",
            "secret",
            options
        )
        .is_err()
    );
}
#[test]
fn owned_https_explicit_roots_and_hostname_under_hostile_environment() {
    const CHILD: &str = "OBJECT_LOG_OWNED_HTTPS_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let directory = tempfile::tempdir().unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = params.self_signed(&ca_key).unwrap();
        let ca_path = directory.path().join("ambient-ca.pem");
        let key_path = directory.path().join("test-ca-key.der");
        std::fs::write(&ca_path, ca.pem()).unwrap();
        std::fs::write(&key_path, ca_key.serialize_der()).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "s3::owned_client::owned_client_tests::owned_https_explicit_roots_and_hostname_under_hostile_environment"])
                .env(CHILD, "1").env("HTTPS_PROXY", "http://127.0.0.1:1")
                .env("HTTP_PROXY", "http://127.0.0.1:1").env("ALL_PROXY", "http://127.0.0.1:1").env("NO_PROXY", "")
                .env("http_proxy", "http://127.0.0.1:1")
                .env("https_proxy", "http://127.0.0.1:1")
                .env("all_proxy", "http://127.0.0.1:1")
                .env("no_proxy", "")
                .env("SSL_CERT_FILE", &ca_path)
                .env("SSL_CERT_DIR", directory.path())
                .env("OBJECT_LOG_OWNED_HTTPS_TEST_CA_KEY", &key_path).status().unwrap();
        assert!(status.success());
        return;
    }
    use std::io::{Read, Write};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _entered = runtime.enter();
    let ca_key = rcgen::KeyPair::try_from(
        std::fs::read(std::env::var_os("OBJECT_LOG_OWNED_HTTPS_TEST_CA_KEY").unwrap()).unwrap(),
    )
    .unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf = rcgen::CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .signed_by(&leaf_key, &ca, &ca_key)
        .unwrap();
    let foreign = rcgen::generate_simple_self_signed(vec!["foreign".into()]).unwrap();
    let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![leaf.der().clone(), ca.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(leaf_key.serialize_der()).into(),
    )
    .unwrap();
    for (trust, hostname, expected) in [
        (Some(Bytes::from(ca.pem())), "localhost", true),
        (None, "localhost", false),
        (Some(Bytes::from(foreign.cert.pem())), "localhost", false),
        (Some(Bytes::from(ca.pem())), "127.0.0.1", false),
    ] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!(
            "https://{hostname}:{}",
            listener.local_addr().unwrap().port()
        );
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = stop.clone();
        let config = Arc::new(server_config.clone());
        let server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(6);
            while !server_stop.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
                match listener.accept() {
                    Ok((socket, _)) => {
                        socket
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        socket
                            .set_write_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        let mut stream = rustls::StreamOwned::new(
                            rustls::ServerConnection::new(config.clone()).unwrap(),
                            socket,
                        );
                        let mut request = Vec::new();
                        let mut bytes = [0u8; 1024];
                        let mut complete = false;
                        while request.len() < 8192 {
                            match stream.read(&mut bytes) {
                                Ok(0) | Err(_) => break,
                                Ok(n) => request.extend_from_slice(&bytes[..n]),
                            }
                            if request.windows(4).any(|w| w == b"\r\n\r\n") {
                                complete = true;
                                break;
                            }
                        }
                        if complete {
                            let body =
                                b"<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>";
                            write!(stream, "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nConnection: close\r\n\r\n", body.len()).unwrap();
                            stream.write_all(body).unwrap();
                            stream.flush().unwrap();
                            break;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10))
                    }
                    Err(e) => panic!("TLS provider accept: {e}"),
                }
            }
        });
        let options = S3ClientOptions {
            connect_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(2),
            operation_timeout: Duration::from_secs(3),
            additional_ca_pem: trust,
            ..Default::default()
        };
        let store = S3BlobStore::new_owned_with_options(
            &endpoint, "region", "bucket", "key", "secret", options,
        )
        .unwrap();
        let result = runtime.block_on(store.get("missing"));
        stop.store(true, Ordering::Release);
        server.join().unwrap();
        if expected {
            assert!(result.unwrap().is_none());
        } else {
            assert!(result.is_err());
        }
    }
}
#[cfg(feature = "native")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_provider_survives_cancel_and_drains_before_final_drop() {
    use std::future::Future;
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut request = Vec::new();
        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
            let mut bytes = [0u8; 1024];
            let count = socket.read(&mut bytes).unwrap();
            assert!(count > 0 && request.len() + count <= 8192);
            request.extend_from_slice(&bytes[..count]);
        }
        started.send(()).unwrap();
        released.recv_timeout(Duration::from_secs(5)).unwrap();
        let body = b"<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>";
        write!(socket, "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nConnection: close\r\n\r\n", body.len()).unwrap();
        socket.write_all(body).unwrap();
    });
    let store = S3BlobStore::new_owned_with_options(
        &endpoint,
        "region",
        "bucket",
        "key",
        "secret",
        S3ClientOptions {
            connect_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(4),
            operation_timeout: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .unwrap();
    let weak = std::sync::Arc::downgrade(&store.authority_identity);
    let log = crate::NativeLog::s3(
        store,
        "owned-drain".into(),
        crate::NativeLimits {
            workers: 1,
            blocking_workers: 1,
            operations: 1,
            bytes: 4 << 20,
            version_bytes: 4096,
            binding_bytes: 4096,
            group_entries: 1,
            group_bytes: 1 << 20,
        },
    )
    .unwrap();
    let caller = log.clone();
    let pending = tokio::spawn(async move { caller.open(b"queue").await });
    entered.await.unwrap();
    pending.abort();
    assert!(pending.await.err().unwrap().is_cancelled());
    let closer = log.clone();
    let mut drain = Box::pin(closer.shutdown());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(drain.as_mut().poll(&mut context).is_pending());
    assert!(weak.upgrade().is_some());
    drop(log);
    release.send(()).unwrap();
    drain.await.unwrap();
    server.join().unwrap();
    drop(closer);
    assert!(weak.upgrade().is_none());
}
#[test]
fn owned_client_explicit_options_ignore_hostile_environment() {
    const CHILD: &str = "OBJECT_LOG_OWNED_OPTIONS_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "s3::owned_client::owned_client_tests::owned_client_explicit_options_ignore_hostile_environment"])
                .env(CHILD, "1").env("OBJECT_LOG_S3_CONNECT_TIMEOUT_SECS", "99999")
                .env("OBJECT_LOG_S3_READ_TIMEOUT_SECS", "99999")
                .env("OBJECT_LOG_S3_OPERATION_TIMEOUT_SECS", "99999")
                .env("OBJECT_LOG_S3_DISABLE_PAYLOAD_SIGNING", "1")
                .env("HTTP_PROXY", "http://127.0.0.1:1")
                .env("HTTPS_PROXY", "http://127.0.0.1:1")
                .env("ALL_PROXY", "http://127.0.0.1:1")
                .env("NO_PROXY", "")
                .env("http_proxy", "http://127.0.0.1:1")
                .env("https_proxy", "http://127.0.0.1:1")
                .env("all_proxy", "http://127.0.0.1:1")
                .env("no_proxy", "")
                .env("SSL_CERT_FILE", "/does-not-exist/owned-options-test")
                .env("SSL_CERT_DIR", "/does-not-exist/owned-options-test")
                .status().unwrap();
        assert!(status.success());
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _entered = runtime.enter();
    for signing in [false, true] {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "no direct provider request"
                        );
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("provider accept: {error}"),
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let mut bytes = [0u8; 1024];
                let count = stream.read(&mut bytes).unwrap();
                assert!(count > 0 && request.len() + count <= 8192);
                request.extend_from_slice(&bytes[..count]);
            }
            let body = b"<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>";
            write!(stream, "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nConnection: close\r\n\r\n", body.len()).unwrap();
            stream.write_all(body).unwrap();
        });
        let options = S3ClientOptions {
            disable_payload_signing: signing,
            connect_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(2),
            operation_timeout: Duration::from_secs(3),
            ..Default::default()
        };
        let store = S3BlobStore::new_owned_with_options(
            &endpoint,
            "region",
            "bucket",
            "owned-options-test-key",
            "secret",
            options.clone(),
        )
        .unwrap();
        assert_eq!(store.disable_payload_signing, signing);
        let configured = store.client.config().timeout_config().unwrap();
        assert_eq!(configured.connect_timeout(), Some(options.connect_timeout));
        assert_eq!(configured.read_timeout(), Some(options.read_timeout));
        assert_eq!(
            configured.operation_timeout(),
            Some(options.operation_timeout)
        );
        assert!(store._owned_configuration.is_some());
        assert!(runtime.block_on(store.get("missing")).unwrap().is_none());
        server.join().unwrap();
        drop(store);
    }
}
