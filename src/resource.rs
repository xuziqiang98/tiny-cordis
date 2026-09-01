use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::task::JoinHandle;

use crate::error::{BoxError, KernelError};

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
        if let Ok(guard) = self.inner.handle.lock()
            && let Some(handle) = guard.as_ref()
        {
            handle.abort();
        }
    }

    pub fn is_finished(&self) -> bool {
        self.inner
            .handle
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(JoinHandle::is_finished))
            .unwrap_or(true)
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

    pub(crate) fn status(&self) -> OwnerStatus {
        self.status
            .lock()
            .map(|status| *status)
            .unwrap_or(OwnerStatus::Closed)
    }

    pub(crate) fn ensure_open(&self) -> Result<(), KernelError> {
        if self.status() == OwnerStatus::Open {
            Ok(())
        } else {
            Err(KernelError::ScopeClosed)
        }
    }

    pub(crate) fn add_child_scope(&self, id: u64) -> Result<(), KernelError> {
        self.ensure_open()?;
        self.child_scopes
            .lock()
            .map_err(|_| KernelError::Internal("resource owner lock poisoned".into()))?
            .push(id);
        Ok(())
    }

    pub(crate) fn add_child_fiber(&self, id: u64) -> Result<(), KernelError> {
        self.ensure_open()?;
        self.child_fibers
            .lock()
            .map_err(|_| KernelError::Internal("resource owner lock poisoned".into()))?
            .push(id);
        Ok(())
    }

    pub(crate) fn spawn<F>(self: &Arc<Self>, id: u64, future: F) -> Result<TaskHandle, KernelError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.ensure_open()?;
        let owner = Arc::clone(self);
        let task = Arc::new(TaskInner {
            handle: Mutex::new(None),
        });
        let task_for_future = Arc::clone(&task);
        let join = tokio::runtime::Handle::try_current()
            .map_err(|_| KernelError::Internal("ctx.spawn requires a Tokio runtime".into()))?
            .spawn(async move {
                future.await;
                if let Ok(mut tasks) = owner.tasks.lock() {
                    tasks.remove(&id);
                }
                if let Ok(mut handle) = task_for_future.handle.lock() {
                    handle.take();
                }
            });
        *task
            .handle
            .lock()
            .map_err(|_| KernelError::Internal("task lock poisoned".into()))? = Some(join);
        self.tasks
            .lock()
            .map_err(|_| KernelError::Internal("resource owner lock poisoned".into()))?
            .insert(id, Arc::clone(&task));
        Ok(TaskHandle { inner: task })
    }

    pub(crate) fn defer<F, Fut>(&self, cleanup: F) -> Result<(), KernelError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.ensure_open()?;
        self.cleanups
            .lock()
            .map_err(|_| KernelError::Internal("resource owner lock poisoned".into()))?
            .push(Box::new(move || Box::pin(cleanup())));
        Ok(())
    }

    pub(crate) fn take_children(&self) -> (Vec<u64>, Vec<u64>) {
        let scopes = self
            .child_scopes
            .lock()
            .map(|mut list| std::mem::take(&mut *list))
            .unwrap_or_default();
        let fibers = self
            .child_fibers
            .lock()
            .map(|mut list| std::mem::take(&mut *list))
            .unwrap_or_default();
        (scopes, fibers)
    }

    pub(crate) fn begin_close(&self) -> bool {
        let Ok(mut status) = self.status.lock() else {
            return false;
        };
        if *status != OwnerStatus::Open {
            return false;
        }
        *status = OwnerStatus::Closing;
        true
    }

    pub(crate) async fn close_resources(&self) -> Result<(), KernelError> {
        let tasks = self
            .tasks
            .lock()
            .map(|mut tasks| std::mem::take(&mut *tasks))
            .unwrap_or_default();
        let mut errors = Vec::new();
        for task in tasks.into_values() {
            if let Ok(guard) = task.handle.lock()
                && let Some(handle) = guard.as_ref()
            {
                handle.abort();
            }
            let join = task.handle.lock().ok().and_then(|mut guard| guard.take());
            if let Some(join) = join
                && let Err(error) = join.await
                && !error.is_cancelled()
            {
                errors.push(Box::new(error) as BoxError);
            }
        }

        let cleanups = self
            .cleanups
            .lock()
            .map(|mut list| std::mem::take(&mut *list))
            .unwrap_or_default();
        for cleanup in cleanups.into_iter().rev() {
            match cleanup().await {
                Ok(()) => {}
                Err(error) => errors.push(error),
            }
        }

        if let Ok(mut status) = self.status.lock() {
            *status = OwnerStatus::Closed;
        }
        KernelError::cleanup(errors)
    }
}
