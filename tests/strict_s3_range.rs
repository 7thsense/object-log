#![cfg(feature = "s3")]
//! Hermetic HTTP protocol tests, not provider durability qualification.
use object_log::{BlobStore, S3BlobStore};
use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::Duration,
};
fn server(response: Vec<u8>) -> (String, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let worker = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = vec![];
        let mut chunk = [0; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = socket.read(&mut chunk).unwrap();
            assert!(count > 0);
            request.extend_from_slice(&chunk[..count]);
            assert!(request.len() < 65536);
        }
        let _ = socket.write_all(&response);
        String::from_utf8(request).unwrap()
    });
    (endpoint, worker)
}
fn client(endpoint: &str) -> S3BlobStore {
    S3BlobStore::new(endpoint, "us-east-1", "bucket", "key", "secret")
}
fn response(status: &str, headers: &str, body: &[u8]) -> Vec<u8> {
    let mut out = format!("HTTP/1.1 {status}\r\nConnection: close\r\n{headers}\r\n").into_bytes();
    out.extend_from_slice(body);
    out
}
#[tokio::test]
async fn exact_range_validates_status_metadata_and_body_without_fallback() {
    let cases = [
        (
            "206 Partial Content",
            "Content-Length: 3\r\nContent-Range: bytes 2-4/6\r\n",
            b"cde".as_slice(),
            true,
        ),
        (
            "200 OK",
            "Content-Length: 6\r\n",
            b"abcdef".as_slice(),
            false,
        ),
        (
            "206 Partial Content",
            "Content-Length: 3\r\nContent-Range: bytes 1-3/6\r\n",
            b"bcd".as_slice(),
            false,
        ),
        (
            "206 Partial Content",
            "Content-Length: 3\r\nContent-Range: bytes 2-4/3\r\n",
            b"cde".as_slice(),
            false,
        ),
        (
            "206 Partial Content",
            "Content-Length: 6\r\nContent-Range: bytes 2-4/6\r\n",
            b"abcdef".as_slice(),
            false,
        ),
        (
            "206 Partial Content",
            "Content-Length: 3\r\nContent-Range: bytes 2-4/6\r\n",
            b"cd".as_slice(),
            false,
        ),
    ];
    for (status, headers, body, valid) in cases {
        let (endpoint, worker) = server(response(status, headers, body));
        let result = client(&endpoint).get_range_exact("object", 2..5).await;
        if valid {
            assert_eq!(result.unwrap().unwrap().as_ref(), b"cde");
        } else {
            assert!(result.is_err());
        }
        let request = worker.join().unwrap().to_ascii_lowercase();
        assert!(request.starts_with("get "));
        assert!(request.contains("range: bytes=2-4\r\n"));
    }
}
#[tokio::test]
async fn empty_range_uses_head_and_checks_eof() {
    for (range, valid) in [(6..6, true), (7..7, false)] {
        let (endpoint, worker) = server(response("200 OK", "Content-Length: 6\r\n", b""));
        let result = client(&endpoint).get_range_exact("object", range).await;
        if valid {
            assert!(result.unwrap().unwrap().is_empty());
        } else {
            assert!(result.is_err());
        }
        assert!(worker.join().unwrap().starts_with("HEAD "));
    }
}

#[tokio::test]
async fn rejected_and_missing_responses_do_not_wait_for_or_collect_large_bodies() {
    for (status, missing) in [("200 OK", false), ("404 Not Found", true)] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = vec![];
            let mut chunk = [0; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = socket.read(&mut chunk).unwrap();
                assert!(count > 0);
                request.extend_from_slice(&chunk[..count]);
            }
            socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 1073741824\r\nConnection: close\r\n\r\n").as_bytes()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            client(&endpoint).get_range_exact("object", 2..5),
        )
        .await;
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        let result = result.expect("strict metadata rejection must precede body read");
        if missing {
            assert!(result.unwrap().is_none());
        } else {
            assert!(result.is_err());
        }
    }
}
