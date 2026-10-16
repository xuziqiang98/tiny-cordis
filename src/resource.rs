use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::task::JoinHandle;

use crate::error::{BoxError, KernelError};
use crate::unwind::catch_future;

pub(crate) type CleanupFuture =
    Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'static>>;
pub(crate) type CleanupFactory = Box<dyn FnOnce() -> CleanupFuture + Send + 'static>;

#[derive(Clone)]
pub struct TaskHandle {
    pub(crate) inner: Arc<TaskInner>,
}

pub(crate) struct TaskInner {
    pub(crate) handle: Mutex<Option<JoinHandle<()>>>,
}

impl TaskHandle {
    pub fn abort(&self) {
        let guard = self.inner.handle.lock().expect("task handle lock poisoned");
        if let Some(handle) = guard.as_ref() {
            handle.abort();
        }
    }

    pub fn is_finished(&self) -> bool {
        self.inner
            .handle
            .lock()
            .expect("task handle lock poisoned")
            .as_ref()
            .is_none_or(JoinHandle::is_finished)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnerStatus {
    Open,
    Closing,
    Closed,
}

pub(crate) struct ResourceOwner {
    status: Mutex<OwnerStatus>,
    tasks: Mutex<std::collections::HashMap<u64, Arc<TaskInner>>>,
    cleanups: Mutex<Vec<CleanupFactory>>,
    child_scopes: Mutex<Vec<u64>>,
    child_fibers: Mutex<Vec<u64>>,
}

impl ResourceOwner {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            status: Mutex::new(OwnerStatus::Open),
            tasks: Mutex::new(std::collections::HashMap::new()),
            cleanups: Mutex::new(Vec::new()),
            child_scopes: Mutex::new(Vec::new()),
            child_fibers: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn with_open_registration<R>(
        &self,
        register: impl FnOnce() -> Result<R, KernelError>,
    ) -> Result<R, KernelError> {
        let status = self.status.lock().expect("resource owner lock poisoned");
        if *status != OwnerStatus::Open {
            return Err(KernelError::ScopeClosed);
        }
        register()
    }

    pub(crate) fn push_child_scope(&self, id: u64) {
        self.child_scopes
            .lock()
            .expect("resource owner lock poisoned")
            .push(id);
    }

    pub(crate) fn push_child_fiber(&self, id: u64) {
        self.child_fibers
            .lock()
            .expect("resource owner lock poisoned")
            .push(id);
    }

    pub(crate) fn remove_child_scope(&self, id: u64) {
        self.child_scopes
            .lock()
            .expect("resource owner lock poisoned")
            .retain(|child| *child != id);
    }

    pub(crate) fn remove_child_fiber(&self, id: u64) {
        self.child_fibers
            .lock()
            .expect("resource owner lock poisoned")
            .retain(|fiber| *fiber != id);
    }

    pub(crate) fn spawn<F>(self: &Arc<Self>, id: u64, future: F) -> Result<TaskHandle, KernelError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| KernelError::Internal("ctx.spawn requires a Tokio runtime".into()))?;
        let owner = Arc::clone(self);
        let task = Arc::new(TaskInner {
            handle: Mutex::new(None),
        });
        let task_for_future = Arc::clone(&task);
        self.with_open_registration(|| {
            let mut tasks = self.tasks.lock().expect("resource owner lock poisoned");
            let mut handle = task.handle.lock().expect("task lock poisoned");
            tasks.insert(id, Arc::clone(&task));
            let join = runtime.spawn(async move {
                future.await;
                owner
                    .tasks
                    .lock()
                    .expect("resource owner lock poisoned")
                    .remove(&id);
                task_for_future
                    .handle
                    .lock()
                    .expect("task lock poisoned")
                    .take();
            });
            *handle = Some(join);
            drop(handle);
            Ok(TaskHandle { inner: task })
        })
    }

    pub(crate) fn defer<F, Fut>(&self, cleanup: F) -> Result<(), KernelError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.with_open_registration(|| {
            self.cleanups
                .lock()
                .expect("resource owner lock poisoned")
                .push(Box::new(move || Box::pin(cleanup())));
            Ok(())
        })
    }

    pub(crate) fn take_children(&self) -> (Vec<u64>, Vec<u64>) {
        let scopes = {
            let mut scopes = self
                .child_scopes
                .lock()
                .expect("resource owner lock poisoned");
            std::mem::take(&mut *scopes)
        };
        let fibers = {
            let mut fibers = self
                .child_fibers
                .lock()
                .expect("resource owner lock poisoned");
            std::mem::take(&mut *fibers)
        };
        (scopes, fibers)
    }

    pub(crate) fn begin_close(&self) {
        let mut status = self.status.lock().expect("resource owner lock poisoned");
        if *status == OwnerStatus::Open {
            *status = OwnerStatus::Closing;
        }
    }

    pub(crate) async fn close_resources(&self) -> Result<(), KernelError> {
        let tasks = {
            let mut tasks = self.tasks.lock().expect("resource owner lock poisoned");
            std::mem::take(&mut *tasks)
        };
        let mut errors = Vec::new();
        for task in tasks.into_values() {
            let join = {
                let mut handle = task.handle.lock().expect("task lock poisoned");
                if let Some(handle) = handle.as_ref() {
                    handle.abort();
                }
                handle.take()
            };
            if let Some(join) = join
                && let Err(error) = join.await
                && !error.is_cancelled()
            {
                errors.push(Box::new(error) as BoxError);
            }
        }

        let cleanups = {
            let mut cleanups = self.cleanups.lock().expect("resource owner lock poisoned");
            std::mem::take(&mut *cleanups)
        };
        for cleanup in cleanups.into_iter().rev() {
            match catch_future(async move { cleanup().await }).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(error),
                Err(message) => {
                    errors.push(Box::new(std::io::Error::other(format!(
                        "cleanup panicked: {message}"
                    ))));
                }
            }
        }

        *self.status.lock().expect("resource owner lock poisoned") = OwnerStatus::Closed;
        KernelError::cleanup(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn poisoned_owner_lock_fails_fast() {
        let owner = ResourceOwner::new();
        let poisoned = Arc::clone(&owner);
        assert!(
            std::thread::spawn(move || {
                let _status = poisoned.status.lock().unwrap();
                panic!("poison resource owner status");
            })
            .join()
            .is_err()
        );

        let result = catch_unwind(AssertUnwindSafe(|| owner.with_open_registration(|| Ok(()))));
        assert!(result.is_err());
    }
}
