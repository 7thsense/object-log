//! Actual SDK HTTP controls, not provider durability qualification.
use super::*;
use crate::S3BlobStore;
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};
fn owned_store(endpoint: &str, region: &str, bucket: &str, key: &str, secret: &str) -> S3BlobStore {
    S3BlobStore::new_owned_with_options(endpoint, region, bucket, key, secret, Default::default())
        .unwrap()
}
fn response(status: &str, body: &[u8], etag: Option<&str>) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    )
    .into_bytes();
    if let Some(etag) = etag {
        out.extend_from_slice(format!("ETag: {etag}\r\n").as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
    out
}
fn xml(code: &str) -> Vec<u8> {
    format!("<Error><Code>{code}</Code><Message>precondition conflict 412</Message></Error>")
        .into_bytes()
}

#[tokio::test]
async fn sdk_metadata_token_exact_budget_preserves_opaque_quotes_and_rejects_before_put() {
    let token = format!("\"opaque-{}\"", "x".repeat(700));
    for token_bytes in [token.len(), token.len() - 1] {
        let (endpoint, stop, worker) = server(
            response("200 OK", b"old!", Some(&token)),
            Some(response("200 OK", b"", None)),
            response("200 OK", b"new!", Some(&token)),
        );
        let store = owned_store(&endpoint, "us-east-1", "bucket", "key", "secret");
        let limits = MetadataLimits {
            payload_bytes: 4,
            version_bytes: token_bytes,
        };
        let observed = store
            .read_metadata("head", limits, metadata_owners(Arc::new(())))
            .await;
        if token_bytes == token.len() {
            let observed = observed.unwrap();
            assert!(
                store
                    .swap_metadata(
                        "head",
                        observed.clone(),
                        Bytes::from_static(b"next"),
                        MetadataLimits {
                            version_bytes: token_bytes - 1,
                            ..limits
                        },
                        metadata_owners(Arc::new(()))
                    )
                    .await
                    .is_err()
            );
            assert!(matches!(
                store
                    .swap_metadata(
                        "head",
                        observed,
                        Bytes::from_static(b"next"),
                        limits,
                        metadata_owners(Arc::new(()))
                    )
                    .await
                    .unwrap(),
                MetadataSwap::Stored
            ));
        } else {
            assert!(observed.is_err());
        }
        stop.store(true, Ordering::SeqCst);
        let requests = worker.join().unwrap();
        let puts: Vec<_> = requests
            .iter()
            .filter(|raw| raw.starts_with(b"PUT "))
            .collect();
        assert_eq!(puts.len(), usize::from(token_bytes == token.len()));
        if let Some(put) = puts.first() {
            let request = String::from_utf8_lossy(put);
            let if_match = request
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("if-match"))
                        .map(|(_, value)| value.trim())
                })
                .unwrap();
            assert_eq!(
                if_match, token,
                "opaque token including quotes must remain exact"
            );
        }
    }
}

#[tokio::test]
async fn sdk_oversized_conflict_token_is_unknown_without_second_conditional_write() {
    let token = "\"version-one\"";
    let larger = format!("\"{}\"", "z".repeat(token.len()));
    let (endpoint, stop, worker) = server(
        response("200 OK", b"old!", Some(token)),
        Some(response(
            "412 Precondition Failed",
            &xml("PreconditionFailed"),
            None,
        )),
        response("200 OK", b"new!", Some(&larger)),
    );
    let store = owned_store(&endpoint, "us-east-1", "bucket", "key", "secret");
    let limits = MetadataLimits {
        payload_bytes: 4,
        version_bytes: token.len(),
    };
    let observed = store
        .read_metadata("head", limits, metadata_owners(Arc::new(())))
        .await
        .unwrap();
    assert!(
        store
            .swap_metadata(
                "head",
                observed,
                Bytes::from_static(b"next"),
                limits,
                metadata_owners(Arc::new(()))
            )
            .await
            .is_err()
    );
    stop.store(true, Ordering::SeqCst);
    let requests = worker.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|raw| raw.starts_with(b"PUT "))
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|raw| raw.starts_with(b"GET "))
            .count(),
        2
    );
}

#[tokio::test]
async fn sdk_put_success_token_exact_limit_or_ambiguous_after_one_write() {
    let token = format!("\"put-{}\"", "p".repeat(700));
    for unsigned in [false, true] {
        for version_bytes in [token.len(), token.len() - 1] {
            let (endpoint, stop, worker) = server(
                response("200 OK", b"old!", Some("\"v\"")),
                Some(response("200 OK", b"", Some(&token))),
                response("200 OK", b"next", Some("\"later\"")),
            );
            let store = owned_store(&endpoint, "us-east-1", "bucket", "key", "secret")
                .with_payload_signing_disabled(unsigned);
            let limits = MetadataLimits {
                payload_bytes: 4,
                version_bytes,
            };
            let original = store
                .read_metadata("head", limits, metadata_owners(Arc::new(())))
                .await
                .unwrap();
            let result = store
                .swap_metadata(
                    "head",
                    original,
                    Bytes::from_static(b"next"),
                    limits,
                    metadata_owners(Arc::new(())),
                )
                .await;
            if version_bytes == token.len() {
                assert!(matches!(result.unwrap(), MetadataSwap::Stored));
            } else {
                assert!(
                    result.is_err(),
                    "oversized success reply cannot establish Stored"
                );
            }
            stop.store(true, Ordering::SeqCst);
            let requests = worker.join().unwrap();
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.starts_with(b"PUT "))
                    .count(),
                1
            );
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.starts_with(b"GET "))
                    .count(),
                1
            );
        }
    }
}
fn server(
    initial: Vec<u8>,
    put: Option<Vec<u8>>,
    reread: Vec<u8>,
) -> (String, Arc<AtomicBool>, thread::JoinHandle<Vec<Vec<u8>>>) {
    scripted_server(initial, vec![put], reread)
}
fn scripted_server(
    initial: Vec<u8>,
    put: Vec<Option<Vec<u8>>>,
    reread: Vec<u8>,
) -> (String, Arc<AtomicBool>, thread::JoinHandle<Vec<Vec<u8>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let finished = stop.clone();
    let worker = thread::spawn(move || {
        let mut requests = Vec::new();
        let mut reads = 0;
        let mut puts = 0;
        let lost_reply_trap = response("412 Precondition Failed", &xml("PreconditionFailed"), None);
        let deadline = Instant::now() + Duration::from_secs(30);
        while !finished.load(Ordering::SeqCst) && Instant::now() < deadline {
            let (mut socket, _) = match listener.accept() {
                Ok(value) => value,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(e) => panic!("{e}"),
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut raw = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let count = socket.read(&mut chunk).unwrap();
                assert!(count > 0);
                raw.extend_from_slice(&chunk[..count]);
                assert!(raw.len() < 65536);
                if let Some(end) = raw.windows(4).position(|v| v == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&raw[..end]);
                    let len = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if raw.len() >= end + 4 + len {
                        break;
                    }
                }
            }
            let request = raw;
            let reply = if request.starts_with(b"GET ") {
                reads += 1;
                Some(if reads == 1 { &initial } else { &reread })
            } else {
                assert!(request.starts_with(b"PUT "));
                puts += 1;
                if put.first().is_some_and(Option::is_none) && puts > 1 {
                    Some(&lost_reply_trap)
                } else {
                    put.get(puts - 1)
                        .or_else(|| put.last())
                        .and_then(Option::as_ref)
                }
            };
            requests.push(request);
            if let Some(reply) = reply {
                let _ = socket.write_all(reply);
            }
        }
        requests
    });
    (endpoint, stop, worker)
}

#[tokio::test]
async fn sdk_bounded_metadata_exact_budget_tokens_and_conflict_rereads() {
    for limit in [4, 3] {
        let initial = response("200 OK", b"old!", Some("\"version-one\""));
        let (endpoint, stop, worker) = server(
            initial,
            Some(response("200 OK", b"", None)),
            response("200 OK", b"new!", Some("\"version-two\"")),
        );
        let store = owned_store(&endpoint, "us-east-1", "bucket", "key", "secret");
        let observed = store
            .read_metadata(
                "head",
                metadata_limits(limit),
                metadata_owners(Arc::new(())),
            )
            .await;
        if limit == 4 {
            let observed = observed.unwrap();
            assert_eq!(observed.bytes().unwrap(), b"old!");
            assert!(
                store
                    .swap_metadata(
                        "wrong",
                        observed.clone(),
                        Bytes::from_static(b"next"),
                        metadata_limits(4),
                        metadata_owners(Arc::new(()))
                    )
                    .await
                    .is_err()
            );
            let other = owned_store(&endpoint, "us-east-1", "bucket", "key", "secret");
            assert!(
                other
                    .swap_metadata(
                        "head",
                        observed.clone(),
                        Bytes::from_static(b"next"),
                        metadata_limits(4),
                        metadata_owners(Arc::new(()))
                    )
                    .await
                    .is_err()
            );
            assert!(
                store
                    .swap_metadata(
                        "head",
                        observed.clone(),
                        Bytes::from_static(b"large"),
                        metadata_limits(4),
                        metadata_owners(Arc::new(()))
                    )
                    .await
                    .is_err()
            );
            assert!(matches!(
                store
                    .swap_metadata(
                        "head",
                        observed,
                        Bytes::from_static(b"next"),
                        metadata_limits(4),
                        metadata_owners(Arc::new(()))
                    )
                    .await
                    .unwrap(),
                MetadataSwap::Stored
            ));
        } else {
            assert!(observed.is_err());
        }
        stop.store(true, Ordering::SeqCst);
        let requests = worker.join().unwrap();
        assert_eq!(
            requests.iter().filter(|r| r.starts_with(b"GET ")).count(),
            1
        );
        assert_eq!(
            requests.iter().filter(|r| r.starts_with(b"PUT ")).count(),
            usize::from(limit == 4)
        );
        if limit == 4 {
            assert!(
                String::from_utf8_lossy(&requests[1])
                    .to_ascii_lowercase()
                    .contains("if-match: \"version-one\"\r\n")
            );
        }
    }
    for reread in [
        response("200 OK", b"win!", Some("\"winner\"")),
        response("200 OK", b"oversized", Some("\"winner\"")),
        response("403 Forbidden", &xml("AccessDenied"), None),
    ] {
        let valid = reread.windows(4).any(|w| w == b"win!");
        let (endpoint, stop, worker) = server(
            response("200 OK", b"old!", Some("\"old\"")),
            Some(response("412 Precondition Failed", b"", None)),
            reread,
        );
        let store = owned_store(&endpoint, "us-east-1", "bucket", "key", "secret");
        let token = store
            .read_metadata("head", metadata_limits(4), metadata_owners(Arc::new(())))
            .await
            .unwrap();
        let result = store
            .swap_metadata(
                "head",
                token,
                Bytes::from_static(b"next"),
                metadata_limits(4),
                metadata_owners(Arc::new(())),
            )
            .await;
        if valid {
            assert!(
                matches!(result, Ok(MetadataSwap::Conflict(ref current)) if current.bytes().unwrap() == b"win!")
            );
        } else {
            assert!(result.is_err());
        }
        stop.store(true, Ordering::SeqCst);
        let requests = worker.join().unwrap();
        assert_eq!(
            requests.iter().filter(|r| r.starts_with(b"GET ")).count(),
            2
        );
        assert_eq!(
            requests.iter().filter(|r| r.starts_with(b"PUT ")).count(),
            1
        );
    }
}

#[tokio::test]
async fn sdk_native_head_root_preservation_missing_and_lost_reply() {
    let initial = head();
    for put in [Some(response("200 OK", b"", None)), None] {
        let success = put.is_some();
        let (endpoint, stop, worker) = server(
            response("200 OK", &initial.encode(), Some("\"old\"")),
            put,
            response(
                "200 OK",
                &NativeHead {
                    epoch: 8,
                    ..initial
                }
                .encode(),
                Some("\"new\""),
            ),
        );
        let store = Arc::new(owned_store(
            &endpoint,
            "us-east-1",
            "bucket",
            "key",
            "secret",
        ));
        let reader = NativeHeadReader::new_known(
            store,
            "head".into(),
            initial.namespace,
            initial.incarnation,
        )
        .unwrap();
        let result = reader.acquire().await;
        if success {
            assert_eq!(
                result.unwrap(),
                NativeHead {
                    epoch: 8,
                    ..initial
                }
            );
        } else {
            assert!(result.is_err());
        }
        reader.close_and_drain().await;
        stop.store(true, Ordering::SeqCst);
        let requests = worker.join().unwrap();
        assert_eq!(
            requests.iter().filter(|r| r.starts_with(b"GET ")).count(),
            1
        );
        let puts: Vec<_> = requests.iter().filter(|r| r.starts_with(b"PUT ")).collect();
        assert_eq!(puts.len(), 1, "ambiguous physical writes cannot be retried");
        // Signed SDK wire body must contain the unchanged fixed root slots.
        assert!(
            puts[0]
                .as_slice()
                .windows(32)
                .any(|w| w == initial.index_root.unwrap().hash)
        );
        assert!(puts[0].as_slice().windows(32).any(|w| w == [29; 32]));
    }
    let (endpoint, stop, worker) = server(
        response("404 Not Found", &xml("NoSuchKey"), None),
        Some(response("200 OK", b"", None)),
        response("404 Not Found", b"", None),
    );
    let reader = NativeHeadReader::new_known(
        Arc::new(owned_store(
            &endpoint,
            "us-east-1",
            "bucket",
            "key",
            "secret",
        )),
        "head".into(),
        initial.namespace,
        initial.incarnation,
    )
    .unwrap();
    assert!(reader.acquire().await.is_err());
    reader.close_and_drain().await;
    stop.store(true, Ordering::SeqCst);
    let requests = worker.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with(b"GET "));
}

#[tokio::test]
async fn sdk_native_head_conflict_floor_and_higher_fence_preserve_roots() {
    let initial = head();
    for current in [
        NativeHead {
            epoch: 6,
            ..initial
        },
        NativeHead {
            next_offset: 8,
            ..initial
        },
        NativeHead {
            index_root: Some(Descriptor {
                hash: [31; 32],
                ..initial.index_root.unwrap()
            }),
            ..initial
        },
        NativeHead {
            epoch: 10,
            ..initial
        },
    ] {
        let valid = current.epoch == 10;
        let (endpoint, stop, worker) = scripted_server(
            response("200 OK", &initial.encode(), Some("\"old\"")),
            vec![
                Some(response("412 Precondition Failed", b"", None)),
                Some(response("200 OK", b"", None)),
            ],
            response("200 OK", &current.encode(), Some("\"winner\"")),
        );
        let store = Arc::new(owned_store(
            &endpoint,
            "us-east-1",
            "bucket",
            "key",
            "secret",
        ));
        let reader = NativeHeadReader::new_known(
            store,
            "head".into(),
            initial.namespace,
            initial.incarnation,
        )
        .unwrap();
        let result = reader.acquire().await;
        if valid {
            assert_eq!(
                result.unwrap(),
                NativeHead {
                    epoch: 11,
                    ..initial
                }
            );
        } else {
            assert!(result.is_err());
        }
        reader.close_and_drain().await;
        stop.store(true, Ordering::SeqCst);
        let requests = worker.join().unwrap();
        assert_eq!(
            requests.iter().filter(|r| r.starts_with(b"GET ")).count(),
            2
        );
        assert_eq!(
            requests.iter().filter(|r| r.starts_with(b"PUT ")).count(),
            if valid { 2 } else { 1 }
        );
        if valid {
            assert!(
                String::from_utf8_lossy(requests.last().unwrap())
                    .to_ascii_lowercase()
                    .contains("if-match: \"winner\"\r\n")
            );
        }
    }
}

#[tokio::test]
async fn sdk_metadata_rejects_unsupported_framing_before_waiting_for_bodies() {
    for (reading, status, headers) in [
        (
            true,
            "200 OK",
            "Content-Length: 1073741824\r\nETag: \"v\"\r\n",
        ),
        (
            true,
            "200 OK",
            "Content-Length: 4\r\nTransfer-Encoding: chunked\r\nETag: \"v\"\r\n",
        ),
        (
            true,
            "200 OK",
            "Content-Length: 4\r\nContent-Encoding: gzip\r\nETag: \"v\"\r\n",
        ),
        (
            true,
            "206 Partial Content",
            "Content-Length: 4\r\nContent-Range: bytes 0-3/4\r\nETag: \"v\"\r\n",
        ),
        (true, "200 OK", "Content-Length: 4\r\n"),
        (
            false,
            "200 OK",
            "Content-Length: 0\r\nTransfer-Encoding: chunked\r\n",
        ),
        (
            false,
            "409 Conflict",
            "Content-Length: 3\r\nTransfer-Encoding: chunked\r\n",
        ),
        (false, "200 OK", "Content-Length: 1073741824\r\n"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            let mut requests = Vec::new();
            for index in 0..if reading { 1 } else { 2 } {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut raw = Vec::new();
                let mut buffer = [0; 4096];
                while !raw.windows(4).any(|w| w == b"\r\n\r\n") {
                    let count = socket.read(&mut buffer).unwrap();
                    assert!(count > 0);
                    raw.extend_from_slice(&buffer[..count]);
                }
                requests.push(raw);
                if !reading && index == 0 {
                    socket
                        .write_all(&response("200 OK", b"old!", Some("\"old\"")))
                        .unwrap();
                } else {
                    socket
                        .write_all(
                            format!("HTTP/1.1 {status}\r\nConnection: close\r\n{headers}\r\n")
                                .as_bytes(),
                        )
                        .unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
            }
            requests
        });
        let store = owned_store(&endpoint, "us-east-1", "bucket", "key", "secret");
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            if reading {
                store
                    .read_metadata("head", metadata_limits(4), metadata_owners(Arc::new(())))
                    .await
                    .map(|_| ())
            } else {
                let token = store
                    .read_metadata("head", metadata_limits(4), metadata_owners(Arc::new(())))
                    .await?;
                store
                    .swap_metadata(
                        "head",
                        token,
                        Bytes::from_static(b"next"),
                        metadata_limits(4),
                        metadata_owners(Arc::new(())),
                    )
                    .await
                    .map(|_| ())
            }
        })
        .await;
        release_tx.send(()).unwrap();
        let requests = worker.join().unwrap();
        assert!(
            result
                .expect("metadata rejection must precede body collection")
                .is_err()
        );
        assert_eq!(requests.len(), if reading { 1 } else { 2 });
    }
}

#[tokio::test]
async fn sdk_bounded_writes_classify_single_attempt_in_both_signing_modes() {
    for disabled in [false, true] {
        for put in [
            Some(response("200 OK", b"", None)),
            None,
            Some(response(
                "409 Conflict",
                &xml("ConditionalRequestConflict"),
                None,
            )),
            Some(response("409 Conflict", &xml("OtherConflict"), None)),
            Some(response("200 OK", &xml("InternalError"), None)),
        ] {
            let success = put
                .as_ref()
                .is_some_and(|r| r.starts_with(b"HTTP/1.1 200") && r.ends_with(b"\r\n\r\n"));
            let conflict = put
                .as_ref()
                .is_some_and(|r| r.windows(26).any(|w| w == b"ConditionalRequestConflict"));
            let (endpoint, stop, worker) = server(
                response("200 OK", b"old!", Some("\"old\"")),
                put,
                response("200 OK", b"win!", Some("\"winner\"")),
            );
            let store = owned_store(&endpoint, "us-east-1", "bucket", "key", "secret")
                .with_payload_signing_disabled(disabled);
            let token = store
                .read_metadata("head", metadata_limits(4), metadata_owners(Arc::new(())))
                .await
                .unwrap();
            let result = store
                .swap_metadata(
                    "head",
                    token,
                    Bytes::from_static(b"next"),
                    metadata_limits(4),
                    metadata_owners(Arc::new(())),
                )
                .await;
            if success {
                assert!(matches!(result, Ok(MetadataSwap::Stored)));
            } else if conflict {
                assert!(matches!(result, Ok(MetadataSwap::Conflict(_))));
            } else {
                assert!(result.is_err());
            }
            stop.store(true, Ordering::SeqCst);
            let requests = worker.join().unwrap();
            assert_eq!(
                requests.iter().filter(|r| r.starts_with(b"PUT ")).count(),
                1
            );
            assert_eq!(
                requests.iter().filter(|r| r.starts_with(b"GET ")).count(),
                if conflict { 2 } else { 1 }
            );
        }
    }
}

#[test]
fn sdk_cancelled_response_retains_owner_in_backpressured_source_upload() {
    struct Lifetime;
    struct Payload {
        bytes: Vec<u8>,
        lifetime: std::sync::Weak<Lifetime>,
        retained: std::sync::Weak<Lifetime>,
        dropped: Arc<AtomicBool>,
    }
    impl AsRef<[u8]> for Payload {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            assert!(
                self.lifetime.upgrade().is_some(),
                "source upload outlived physical admission"
            );
            assert!(
                self.retained.upgrade().is_some(),
                "source upload outlived byte reservation"
            );
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let server = thread::spawn(move || {
        let mut requests = Vec::new();
        for index in 0..2 {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut headers = Vec::new();
            let mut chunk = [0; 256];
            while !headers.windows(4).any(|w| w == b"\r\n\r\n") {
                let count = socket.read(&mut chunk).unwrap();
                assert!(count > 0);
                headers.extend_from_slice(&chunk[..count]);
            }
            requests.push(headers);
            if index == 0 {
                socket
                    .write_all(&response("200 OK", b"old!", Some("\"old\"")))
                    .unwrap();
            } else {
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        requests
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let store = owned_store(&endpoint, "us-east-1", "bucket", "key", "secret")
        .with_payload_signing_disabled(true);
    let retained = Arc::new(Lifetime);
    let retained_weak = Arc::downgrade(&retained);
    let lifetime = Arc::new(Lifetime);
    let weak = Arc::downgrade(&lifetime);
    let token = runtime
        .block_on(store.read_metadata(
            "head",
            metadata_limits(4),
            MetadataOwners {
                retained: retained.clone(),
                physical: lifetime.clone(),
            },
        ))
        .unwrap();
    assert!(retained_weak.upgrade().is_some());
    let dropped = Arc::new(AtomicBool::new(false));
    let bytes = Bytes::from_owner(Payload {
        bytes: vec![31; 32 << 20],
        lifetime: weak.clone(),
        retained: retained_weak.clone(),
        dropped: dropped.clone(),
    });
    let mut pending = Box::pin(store.swap_metadata(
        "head",
        token,
        bytes,
        metadata_limits(32 << 20),
        MetadataOwners {
            retained,
            physical: lifetime,
        },
    ));
    runtime.block_on(async {
        tokio::select! {
            result=&mut pending => panic!("blocked upload unexpectedly completed: {result:?}"),
            _=tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(5)).unwrap()) => {}
        }
    });
    // Current-thread runtime is not polling its independent Hyper connection.
    drop(pending);
    assert!(!dropped.load(Ordering::SeqCst));
    assert!(
        weak.upgrade().is_some(),
        "queued request source must retain its owner"
    );
    assert!(
        retained_weak.upgrade().is_some(),
        "queued source must retain byte reservation"
    );
    release_tx.send(()).unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while weak.upgrade().is_some() || retained_weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
    assert!(dropped.load(Ordering::SeqCst));
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with(b"GET "));
    assert!(requests[1].starts_with(b"PUT "));
    assert!(
        String::from_utf8_lossy(&requests[1])
            .to_ascii_lowercase()
            .contains("content-length: 33554432\r\n")
    );
}
