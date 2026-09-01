use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::error::KernelError;
use crate::kernel::{ActivationId, Coordinator, FiberId, ScopeId};
use crate::plugin::PluginSpec;
use crate::resource::ResourceOwner;
use crate::service::ServiceId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FiberState {
    Pending,
    Starting,
    Active,
    Stopping,
    Failed,
    Disposed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DesiredState {
    Running,
    Disposed,
}

pub(crate) struct FiberLife {
    pub(crate) generation: u64,
    pub(crate) activation: Option<Arc<ActivationInner>>,
    pub(crate) snapshot: HashMap<ServiceId, u64>,
    pub(crate) desired: DesiredState,
    pub(crate) waiting_dependents: std::collections::HashSet<FiberId>,
    pub(crate) stop_started: bool,
    pub(crate) failure_pending: bool,
    pub(crate) error: Option<String>,
}

pub(crate) struct FiberInner {
    pub(crate) id: FiberId,
    pub(crate) parent_scope: ScopeId,
    pub(crate) spec: PluginSpec,
    pub(crate) state: Mutex<FiberState>,
    pub(crate) state_tx: watch::Sender<FiberState>,
    pub(crate) life: Mutex<FiberLife>,
}

pub(crate) struct ActivationInner {
    pub(crate) id: ActivationId,
    pub(crate) fiber_id: FiberId,
    pub(crate) owner: Arc<ResourceOwner>,
    pub(crate) staged: Mutex<HashMap<ServiceId, StagedService>>,
    pub(crate) bound_ids: Mutex<Vec<u64>>,
    pub(crate) required_bindings: Mutex<HashMap<ServiceId, u64>>,
    pub(crate) dependency_bindings: Mutex<HashMap<ServiceId, u64>>,
    pub(crate) cleanup_started: Mutex<bool>,
    pub(crate) apply_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

pub(crate) struct StagedService {
    pub(crate) key: crate::service::ServiceKey,
    pub(crate) value: crate::kernel::ErasedService,
}

#[derive(Clone)]
pub struct FiberHandle {
    pub(crate) inner: Arc<FiberInner>,
    pub(crate) coordinator: Arc<Coordinator>,
}

impl FiberHandle {
    pub fn state(&self) -> FiberState {
        self.inner
            .state
            .lock()
            .map(|state| *state)
            .unwrap_or(FiberState::Disposed)
    }

    pub async fn wait_active(&self) -> Result<(), KernelError> {
        let mut receiver = self.inner.state_tx.subscribe();
        loop {
            if self.state() == FiberState::Active {
                return Ok(());
            }
            match *receiver.borrow() {
                FiberState::Active => return Ok(()),
                FiberState::Failed => {
                    let message = self
                        .inner
                        .life
                        .lock()
                        .ok()
                        .and_then(|life| life.error.clone())
                        .unwrap_or_else(|| "unknown plugin error".into());
                    return Err(KernelError::PluginFailed {
                        name: self.inner.spec.name.clone(),
                        source: Box::new(std::io::Error::other(message)),
                    });
                }
                FiberState::Disposed => return Err(KernelError::FiberDisposed),
                _ => {}
            }
            receiver
                .changed()
                .await
                .map_err(|_| KernelError::FiberDisposed)?;
        }
    }

    pub async fn dispose(&self) -> Result<(), KernelError> {
        self.coordinator.dispose_fiber(self.inner.id).await
    }
}
