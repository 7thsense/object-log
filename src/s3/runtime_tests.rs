use crate::{BoundedMetadataStore, MetadataLimits, MetadataOwners, S3BlobStore, S3ClientOptions};
use std::{
    io::{Read, Write},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
fn runtime_probe_owners() -> MetadataOwners {
    let owner = Arc::new(());
    MetadataOwners {
        retained: owner.clone(),
        physical: owner,
    }
}
fn runtime_probe_store(endpoint: &str, owned: bool) -> S3BlobStore {
    if owned {
        S3BlobStore::new_owned_with_options(
            endpoint,
            "us-east-1",
            "bucket",
            "key",
            "secret",
            S3ClientOptions {
                connect_timeout: Duration::from_secs(1),
                read_timeout: Duration::from_secs(2),
                operation_timeout: Duration::from_secs(3),
                ..Default::default()
            },
        )
        .unwrap()
    } else {
        S3BlobStore::new(endpoint, "us-east-1", "bucket", "key", "secret")
    }
}
fn independent_store_runtime_teardown(owned: bool, destroy: bool) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let server_stop = stop.clone();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let server_gate = gate.clone();
    let (started, entered) = std::sync::mpsc::channel();
    let started = Arc::new(Mutex::new(Some(started)));
    let server = std::thread::spawn(move || {
        let mut workers = Vec::new();
        while !server_stop.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((mut socket, _)) => {
                    let gate = server_gate.clone();
                    let started = started.clone();
                    workers.push(std::thread::spawn(move || {
                        socket
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        loop {
                            let mut request = Vec::new();
                            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                                let mut b = [0; 1];
                                match socket.read(&mut b) {
                                    Ok(0) | Err(_) => return,
                                    Ok(_) => request.push(b[0]),
                                }
                                if request.len() > 16384 {
                                    return;
                                }
                            }
                            let text = String::from_utf8_lossy(&request);
                            let put = text.starts_with("PUT ");
                            let length = text
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .and_then(|n| n.trim().parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            if length > 65536 {
                                return;
                            }
                            let mut body = vec![0; length];
                            if socket.read_exact(&mut body).is_err() {
                                return;
                            }
                            if put {
                                if let Some(sender) = started.lock().unwrap().take() {
                                    sender.send(()).unwrap()
                                }
                                let (lock, notify) = &*gate;
                                let ready = lock.lock().unwrap();
                                let (ready, _) = notify
                                    .wait_timeout_while(ready, Duration::from_secs(5), |ready| {
                                        !*ready
                                    })
                                    .unwrap();
                                if !*ready {
                                    return;
                                }
                            }
                            let response = if put {
                                b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"next\"\r\n\r\n"
                                    .as_slice()
                            } else {
                                b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nETag: \"base\"\r\n\r\nx"
                                    .as_slice()
                            };
                            if socket.write_all(response).is_err() {
                                return;
                            }
                        }
                    }));
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5))
                }
                Err(e) => panic!("accept {e}"),
            }
        }
        for worker in workers {
            worker.join().unwrap()
        }
    });
    let runtime_a = tokio::runtime::Runtime::new().unwrap();
    let a = {
        let _entered = runtime_a.enter();
        runtime_probe_store(&endpoint, owned)
    };
    let limits = MetadataLimits {
        payload_bytes: 8,
        version_bytes: 128,
    };
    runtime_a
        .block_on(a.read_metadata("head", limits, runtime_probe_owners()))
        .unwrap();
    let endpoint_b = endpoint.clone();
    let worker = std::thread::spawn(move || {
        let runtime_b = tokio::runtime::Runtime::new().unwrap();
        let b = {
            let _entered = runtime_b.enter();
            runtime_probe_store(&endpoint_b, owned)
        };
        runtime_b.block_on(async {
            let observed = b
                .read_metadata("head", limits, runtime_probe_owners())
                .await?;
            b.swap_metadata(
                "head",
                observed,
                bytes::Bytes::from_static(b"b"),
                limits,
                runtime_probe_owners(),
            )
            .await
        })
    });
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let runtime_a = if destroy {
        drop(runtime_a);
        None
    } else {
        Some(runtime_a)
    };
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    let result = worker.join().unwrap();
    let stored = matches!(result, Ok(crate::MetadataSwap::Stored));
    drop(runtime_a);
    drop(a);
    stop.store(true, Ordering::Release);
    server.join().unwrap();
    assert!(
        stored,
        "independent store conditional request must complete after runtime teardown"
    );
}

#[test]
fn independent_stores_survive_other_runtime_teardown() {
    for owned in [false, true] {
        for destroy in [false, true] {
            independent_store_runtime_teardown(owned, destroy);
        }
    }
}
