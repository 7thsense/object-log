//! One executor/admission lifecycle per store, independent of caller runtimes.
use super::page_store::ByteBudget;
use crate::ObjectLogError;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    thread,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};

pub(super) struct ExecutionConfig {
    pub workers: usize,
    pub blocking_workers: usize,
    pub operations: usize,
    pub bytes: u32,
}
struct State {
    closed: bool,
    physical: usize,
}
struct Completion {
    done: AtomicBool,
    failed: AtomicBool,
    notify: Notify,
}
struct HostCompletion(Arc<Completion>);
impl Drop for HostCompletion {
    fn drop(&mut self) {
        self.0.failed.store(thread::panicking(), Ordering::Relaxed);
        self.0.done.store(true, Ordering::Release);
        self.0.notify.notify_waiters();
    }
}
struct Executor {
    completion: Arc<Completion>,
    handle: tokio::runtime::Handle,
    stop: Mutex<Option<oneshot::Sender<()>>>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}
impl Drop for Executor {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.get_mut().unwrap().take() {
            let _ = stop.send(());
        }
        // Last physical release can occur on an executor worker. Never join
        // that worker from its own drop; explicit close performs the join.
        drop(self.thread.get_mut().unwrap().take());
    }
}
pub(super) struct Resources {
    pub bytes: Arc<ByteBudget>,
    slots: Arc<Semaphore>,
    state: Mutex<State>,
    drained: Notify,
    executor: Executor,
}
struct OperationCompletion {
    done: AtomicBool,
    notify: Notify,
}
impl OperationCompletion {
    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}
pub(super) struct Physical {
    completion: Arc<OperationCompletion>,
    resources: Arc<Resources>,
    permit: Option<OwnedSemaphorePermit>,
}
impl Physical {
    pub fn validate_budget(&self, budget: &Arc<ByteBudget>) -> Result<(), ObjectLogError> {
        if !Arc::ptr_eq(&self.resources.bytes, budget) {
            return Err(invalid(
                "native operation belongs to another resource owner",
            ));
        }
        Ok(())
    }
}
impl Drop for Physical {
    fn drop(&mut self) {
        drop(self.permit.take());
        let mut state = self.resources.state.lock().unwrap();
        state.physical -= 1;
        if state.physical == 0 {
            self.resources.drained.notify_waiters();
        }
        drop(state);
        self.completion.done.store(true, Ordering::Release);
        self.completion.notify.notify_waiters();
    }
}
struct OwnedTask<F: Future> {
    future: Pin<Box<F>>,
    // Drop factory/future/source inputs before releasing operation ownership.
    _physical: Arc<Physical>,
}
impl<F: Future> Future for OwnedTask<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}
impl Resources {
    pub fn new(config: ExecutionConfig) -> Result<Arc<Self>, ObjectLogError> {
        if config.workers == 0
            || config.blocking_workers == 0
            || config.operations == 0
            || config.operations > Semaphore::MAX_PERMITS
        {
            return Err(ObjectLogError::InvalidConfig(
                "native executor limits".into(),
            ));
        }
        let bytes = ByteBudget::new(config.bytes)?;
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let (stop, stopped) = oneshot::channel();
        let completion = Arc::new(Completion {
            done: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            notify: Notify::new(),
        });
        let finished = completion.clone();
        let thread = thread::Builder::new()
            .name("native-log-host".into())
            .spawn(move || {
                let _completion = HostCompletion(finished);
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(config.workers)
                    .max_blocking_threads(config.blocking_workers)
                    .thread_name("native-log-worker")
                    .enable_all()
                    .build();
                match runtime {
                    Ok(runtime) => {
                        if ready.send(Ok(runtime.handle().clone())).is_err() {
                            return;
                        }
                        runtime.block_on(async {
                            let _ = stopped.await;
                        });
                        drop(runtime);
                    }
                    Err(_) => {
                        let _ = ready.send(Err(invalid("native executor startup failed")));
                    }
                }
            })
            .map_err(|_| invalid("native executor host startup failed"))?;
        let handle = match started.recv() {
            Ok(Ok(handle)) => handle,
            _ => {
                let _ = thread.join();
                return Err(invalid("native executor startup failed"));
            }
        };
        Ok(Arc::new(Self {
            bytes,
            slots: Arc::new(Semaphore::new(config.operations)),
            state: Mutex::new(State {
                closed: false,
                physical: 0,
            }),
            drained: Notify::new(),
            executor: Executor {
                completion,
                handle,
                stop: Mutex::new(Some(stop)),
                thread: Mutex::new(Some(thread)),
            },
        }))
    }

    // Output T must contain no Physical or operation-scoped PageStore: the
    // caller waits for their final release before receiving T. Public carriers
    // retain only byte admission and immutable binding/certificate ownership.
    pub async fn run<T, F, Fut>(self: &Arc<Self>, factory: F) -> Result<T, ObjectLogError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Physical>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ObjectLogError>> + Send + 'static,
    {
        if tokio::runtime::Handle::try_current()
            .is_ok_and(|current| current.id() == self.executor.handle.id())
        {
            return Err(invalid("recursive native operation admission"));
        }
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| invalid("native execution closed"))?;
        {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Err(invalid("native execution closed"));
            }
            state.physical += 1;
        }
        let completion = Arc::new(OperationCompletion {
            done: AtomicBool::new(false),
            notify: Notify::new(),
        });
        let physical = Arc::new(Physical {
            completion: completion.clone(),
            resources: self.clone(),
            permit: Some(permit),
        });
        let owner = physical.clone();
        let (send, reply) = oneshot::channel();
        let future = async move {
            let result = factory(owner).await;
            let _ = send.send(result);
        };
        self.executor.handle.spawn(OwnedTask {
            future: Box::pin(future),
            _physical: physical,
        });
        let result = reply
            .await
            .map_err(|_| invalid("native operation panicked"));
        completion.wait().await;
        result?
    }

    pub async fn close_and_drain(&self) -> Result<(), ObjectLogError> {
        if tokio::runtime::Handle::try_current()
            .is_ok_and(|current| current.id() == self.executor.handle.id())
        {
            return Err(invalid("native executor cannot drain itself"));
        }
        {
            self.state.lock().unwrap().closed = true;
            self.slots.close();
        }
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().unwrap().physical == 0 {
                break;
            }
            notified.await;
        }
        if let Some(stop) = self.executor.stop.lock().unwrap().take() {
            let _ = stop.send(());
        }
        loop {
            let notified = self.executor.completion.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.executor.completion.done.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
        // Every closer observes host completion, including after another closer
        // was cancelled. Joining now cannot wait on live runtime/physical work.
        if let Some(join) = self.executor.thread.lock().unwrap().take() {
            join.join()
                .map_err(|_| invalid("native executor host panicked"))?;
        }
        if self.executor.completion.failed.load(Ordering::Relaxed) {
            return Err(invalid("native executor host panicked"));
        }
        Ok(())
    }
}
fn invalid(reason: &str) -> ObjectLogError {
    ObjectLogError::StorageUnavailable(reason.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AppendOwner, BlobStore, BoundedMetadataStore, LocalBlobStore, MetadataLimits,
        MetadataOwners,
    };

    fn resources() -> Arc<Resources> {
        Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 1,
            bytes: 65536,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn one_slot_capture_closes_with_retained_snapshot_and_rejects_reuse() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalBlobStore::new(directory.path()));
        store
            .put("head", bytes::Bytes::from_static(b"data"))
            .await
            .unwrap();
        let resources = resources();
        let memory = resources.bytes.clone();
        let (snapshot, physical) = resources
            .run(move |physical| async move {
                let weak = Arc::downgrade(&physical);
                let retained = memory.charge(8192)?;
                let snapshot = store
                    .read_metadata(
                        "head",
                        MetadataLimits {
                            payload_bytes: 4,
                            version_bytes: 56,
                        },
                        MetadataOwners {
                            retained: retained as AppendOwner,
                            physical: physical as AppendOwner,
                        },
                    )
                    .await?;
                Ok((snapshot, weak))
            })
            .await
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            resources.close_and_drain(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(physical.upgrade().is_none());
        assert_eq!(snapshot.bytes(), Some(&b"data"[..]));
        assert_eq!(resources.bytes.available(), 65536 - 8192);
        let invoked = Arc::new(AtomicBool::new(false));
        let observed = invoked.clone();
        let alias = snapshot.clone();
        assert!(
            resources
                .run(move |_| async move {
                    observed.store(true, Ordering::SeqCst);
                    Ok(alias.bytes().unwrap().len())
                })
                .await
                .is_err()
        );
        assert!(!invoked.load(Ordering::SeqCst));
        assert_eq!(resources.bytes.available(), 65536 - 8192);
        drop(snapshot);
        assert_eq!(resources.bytes.available(), 65536);
        resources.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn one_slot_cow_append_lookup_and_retained_entry_drain() {
        use super::super::{
            codec::{LeafEntry, PAGE_BYTES, Scope},
            index::Index,
            page_store::PageStore,
        };
        let directory = tempfile::tempdir().unwrap();
        let resources = Resources::new(ExecutionConfig {
            workers: 2,
            blocking_workers: 2,
            operations: 1,
            bytes: 1 << 20,
        })
        .unwrap();
        let pages = PageStore::new(
            Arc::new(LocalBlobStore::new(directory.path())),
            "pages".into(),
            Scope {
                namespace: [1; 32],
                incarnation: [2; 32],
            },
            resources.bytes.clone(),
            MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 56,
            },
        )
        .unwrap();
        let retained = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            resources.run(move |physical| async move {
                let weak = Arc::downgrade(&physical);
                let index = Index::new(pages.in_operation(physical)?);
                let object_id = index.store_object_id("physical/data").await?;
                let entry = LeafEntry {
                    object_id,
                    logical_id: object_id,
                    base: 0,
                    record_count: 1,
                    byte_start: 0,
                    byte_len: 4,
                    payload_hash: [3; 32],
                };
                let root = index.append(None, entry).await?;
                let retained = index.lookup(Some(root), 0).await?.unwrap();
                assert_eq!(
                    index.object_id(retained.entry().object_id).await?.as_str(),
                    "physical/data"
                );
                Ok((retained, weak))
            }),
        )
        .await
        .unwrap()
        .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            resources.close_and_drain(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(retained.1.upgrade().is_none());
        assert_eq!(retained.0.entry().base, 0);
        assert!(resources.bytes.available() < 1 << 20);
        drop(retained);
        assert_eq!(resources.bytes.available(), 1 << 20);
    }

    #[tokio::test]
    async fn panic_releases_slot_and_recursive_admission_rejects_before_factory() {
        let resources = resources();
        assert!(
            resources
                .run::<(), _, _>(|_| async { panic!("injected native task panic") })
                .await
                .is_err()
        );
        assert_eq!(resources.run(|_| async { Ok(17) }).await.unwrap(), 17);
        let nested = resources.clone();
        let invoked = Arc::new(AtomicBool::new(false));
        let observed = invoked.clone();
        resources
            .run(move |_| async move {
                assert!(
                    nested
                        .run(move |_| async move {
                            observed.store(true, Ordering::SeqCst);
                            Ok(())
                        })
                        .await
                        .is_err()
                );
                Ok(())
            })
            .await
            .unwrap();
        assert!(!invoked.load(Ordering::SeqCst));
        resources.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_closer_and_queued_factory_do_not_escape_close() {
        let resources = resources();
        let (entered, started) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let worker = resources.clone();
        let active = tokio::spawn(async move {
            worker
                .run(move |_| async move {
                    entered.send(()).unwrap();
                    wait.await.unwrap();
                    Ok(())
                })
                .await
        });
        started.await.unwrap();
        let invoked = Arc::new(AtomicBool::new(false));
        let observed = invoked.clone();
        let queued = resources.run(move |_| {
            observed.store(true, Ordering::SeqCst);
            async { Ok(()) }
        });
        tokio::pin!(queued);
        std::future::poll_fn(|cx| {
            assert!(queued.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let closer = resources.clone();
        let cancelled = tokio::spawn(async move { closer.close_and_drain().await });
        // Gate closure is observable without timing assumptions and rejects any
        // pending admission before its factory can allocate or issue I/O.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !resources.slots.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        assert!(queued.await.is_err());
        assert!(!invoked.load(Ordering::SeqCst));
        let first = resources.clone();
        let second = resources.clone();
        let closers =
            tokio::spawn(
                async move { tokio::join!(first.close_and_drain(), second.close_and_drain()) },
            );
        release.send(()).unwrap();
        active.await.unwrap().unwrap();
        let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(5), closers)
            .await
            .unwrap()
            .unwrap();
        first.unwrap();
        second.unwrap();
        assert_eq!(resources.state.lock().unwrap().physical, 0);
    }

    #[tokio::test]
    async fn foreign_operation_binding_rejects_before_provider_io() {
        use super::super::{
            codec::{PAGE_BYTES, Scope},
            page_store::PageStore,
        };
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalBlobStore::new(directory.path()));
        let first = resources();
        let second = resources();
        let pages = PageStore::new(
            local.clone(),
            "pages".into(),
            Scope {
                namespace: [1; 32],
                incarnation: [2; 32],
            },
            second.bytes.clone(),
            MetadataLimits {
                payload_bytes: PAGE_BYTES,
                version_bytes: 56,
            },
        )
        .unwrap();
        first
            .run(move |physical| async move {
                assert!(pages.in_operation(physical).is_err());
                Ok(())
            })
            .await
            .unwrap();
        assert!(local.list("pages").await.unwrap().is_empty());
        first.close_and_drain().await.unwrap();
        second.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn construction_panic_and_last_external_owner_drop_release_executor() {
        let resources = resources();
        assert!(
            resources
                .run::<(), _, std::future::Ready<Result<(), ObjectLogError>>>(|_| panic!(
                    "injected factory construction panic"
                ))
                .await
                .is_err()
        );
        assert_eq!(resources.run(|_| async { Ok(7) }).await.unwrap(), 7);
        let completion = resources.executor.completion.clone();
        let weak = Arc::downgrade(&resources);
        drop(resources);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let notified = completion.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if completion.done.load(Ordering::Acquire) {
                    break;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
        assert!(weak.upgrade().is_none());
        assert!(!completion.failed.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn last_resource_release_on_managed_worker_does_not_self_join() {
        let resources = resources();
        let completion = resources.executor.completion.clone();
        let weak = Arc::downgrade(&resources);
        let (entered, started) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let worker = resources.clone();
        let caller = tokio::spawn(async move {
            worker
                .run(move |_| async move {
                    entered.send(()).unwrap();
                    wait.await.unwrap();
                    Ok(())
                })
                .await
        });
        started.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        drop(resources);
        assert!(weak.upgrade().is_some());
        assert!(!completion.done.load(Ordering::Acquire));
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let notified = completion.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if completion.done.load(Ordering::Acquire) {
                    break;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
        assert!(weak.upgrade().is_none());
        assert!(!completion.failed.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn error_and_panic_wait_for_detached_source_owner_terminal() {
        struct Signal(Arc<Notify>);
        impl std::task::Wake for Signal {
            fn wake(self: Arc<Self>) {
                self.0.notify_one();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.notify_one();
            }
        }
        for panic in [false, true] {
            let resources = resources();
            let (entered, started) = oneshot::channel();
            let (release, resume) = oneshot::channel();
            let (reply_release, reply_gate) = oneshot::channel();
            let retained = Arc::new(());
            let weak = Arc::downgrade(&retained);
            let worker = resources.clone();
            let mut caller = Box::pin(async move {
                let result: Result<(), ObjectLogError> = worker
                    .run(move |physical| async move {
                        tokio::spawn(async move {
                            entered.send(()).unwrap();
                            resume.await.unwrap();
                            drop(physical);
                        });
                        reply_gate.await.unwrap();
                        if panic {
                            panic!("injected source reply panic")
                        }
                        Err(invalid("injected source reply error"))
                    })
                    .await;
                drop(retained);
                result
            });
            let signal = Arc::new(Notify::new());
            let waker = std::task::Waker::from(Arc::new(Signal(signal.clone())));
            let mut context = Context::from_waker(&waker);
            assert!(caller.as_mut().poll(&mut context).is_pending());
            started.await.unwrap();
            // No reply exists yet, so this poll installs the controlled Waker.
            assert!(caller.as_mut().poll(&mut context).is_pending());
            reply_release.send(()).unwrap();
            signal.notified().await;
            // The reply/error is ready, but the detached source is not terminal.
            assert!(caller.as_mut().poll(&mut context).is_pending());
            assert_eq!(resources.state.lock().unwrap().physical, 1);
            assert!(weak.upgrade().is_some());
            release.send(()).unwrap();
            assert!(caller.await.is_err());
            assert!(weak.upgrade().is_none());
            assert_eq!(resources.state.lock().unwrap().physical, 0);
            resources.close_and_drain().await.unwrap();
        }
    }

    #[test]
    fn caller_runtime_shutdown_preserves_dispatched_physical_work() {
        let resources = resources();
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (entered, started) = std::sync::mpsc::channel();
        let (release, wait) = oneshot::channel();
        let worker = resources.clone();
        let task = caller.spawn(async move {
            worker
                .run(move |_| async move {
                    entered.send(()).unwrap();
                    let _ = wait.await;
                    Ok(())
                })
                .await
        });
        caller.block_on(async {
            tokio::task::spawn_blocking(move || {
                started
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap()
            })
            .await
            .unwrap();
        });
        drop(caller);
        assert_eq!(resources.state.lock().unwrap().physical, 1);
        release.send(()).unwrap();
        let finish = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        finish.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                resources.close_and_drain(),
            )
            .await
            .unwrap()
            .unwrap();
        });
        assert_eq!(resources.state.lock().unwrap().physical, 0);
        drop(task);
    }
}
