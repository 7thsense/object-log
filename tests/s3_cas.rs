#![cfg(feature = "s3")]
//! Hermetic native conditional-write protocol, not provider durability evidence.
use bytes::Bytes;
use object_log::{BlobStore, CasOutcome, S3BlobStore};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
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
fn server(
    initial: Vec<u8>,
    put: Option<Vec<u8>>,
    reread: Vec<u8>,
) -> (String, Arc<AtomicBool>, thread::JoinHandle<Vec<String>>) {
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
            let request = String::from_utf8(raw).unwrap();
            let reply = if request.starts_with("GET ") {
                reads += 1;
                Some(if reads == 1 { &initial } else { &reread })
            } else {
                assert!(request.starts_with("PUT "));
                puts += 1;
                if put.is_none() && puts > 1 {
                    Some(&lost_reply_trap)
                } else {
                    put.as_ref()
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
async fn typed_cas_conflicts_preserve_condition_and_reread_authority() {
    let missing = response("404 Not Found", &xml("NoSuchKey"), None);
    let winner = response("200 OK", b"winner", Some("\"winner\""));
    let failures = [
        (
            Some(response(
                "412 Precondition Failed",
                &xml("PreconditionFailed"),
                None,
            )),
            true,
        ),
        (Some(response("412 Precondition Failed", b"", None)), true),
        (
            Some(response("412 Precondition Failed", b"{malformed", None)),
            true,
        ),
        (
            Some(response(
                "409 Conflict",
                &xml("ConditionalRequestConflict"),
                None,
            )),
            true,
        ),
        (
            Some(response("409 Conflict", &xml("OtherConflict"), None)),
            false,
        ),
        (
            Some(response("403 Forbidden", &xml("AccessDenied"), None)),
            false,
        ),
        (
            Some(response("503 Service Unavailable", &xml("SlowDown"), None)),
            false,
        ),
        (None, false),
    ];
    for disabled in [false, true] {
        for existing in [false, true] {
            for (put, conflict) in &failures {
                let initial = if existing {
                    response("200 OK", b"old", Some("\"old\""))
                } else {
                    missing.clone()
                };
                let (endpoint, stop, worker) = server(initial, put.clone(), winner.clone());
                let result = S3BlobStore::new(&endpoint, "us-east-1", "bucket", "key", "secret")
                    .with_payload_signing_disabled(disabled)
                    .compare_and_swap(
                        "object",
                        existing.then(|| Bytes::from_static(b"old")),
                        Bytes::from_static(b"new"),
                    )
                    .await;
                stop.store(true, Ordering::SeqCst);
                let requests = worker.join().unwrap();
                if *conflict {
                    assert!(
                        matches!(result,Ok(CasOutcome::Conflict{current:Some(ref value)}) if value.as_ref()==b"winner")
                    );
                } else {
                    assert!(result.is_err());
                }
                let puts: Vec<_> = requests
                    .iter()
                    .filter(|request| request.starts_with("PUT "))
                    .collect();
                assert_eq!(puts.len(), 1, "conditional PUT must not retry a lost reply");
                for request in puts {
                    let headers = request.to_ascii_lowercase();
                    assert!(headers.contains(if existing {
                        "if-match: \"old\"\r\n"
                    } else {
                        "if-none-match: *\r\n"
                    }));
                }
                assert_eq!(
                    requests.iter().filter(|r| r.starts_with("GET ")).count(),
                    if *conflict { 2 } else { 1 }
                );
            }
        }
    }
    for (reread, absent) in [
        (missing, true),
        (response("403 Forbidden", &xml("AccessDenied"), None), false),
    ] {
        let (endpoint, stop, worker) = server(
            response("200 OK", b"old", Some("\"old\"")),
            Some(response("412 Precondition Failed", b"", None)),
            reread,
        );
        let result = S3BlobStore::new(&endpoint, "us-east-1", "bucket", "key", "secret")
            .compare_and_swap(
                "object",
                Some(Bytes::from_static(b"old")),
                Bytes::from_static(b"new"),
            )
            .await;
        stop.store(true, Ordering::SeqCst);
        let requests = worker.join().unwrap();
        if absent {
            assert!(matches!(result, Ok(CasOutcome::Conflict { current: None })));
        } else {
            assert!(result.is_err());
        }
        assert_eq!(requests.iter().filter(|r| r.starts_with("GET ")).count(), 2);
    }
}
