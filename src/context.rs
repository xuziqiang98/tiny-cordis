use std::any::Any;
use std::sync::Arc;

use crate::error::{BoxError, KernelError};
use crate::fiber::ActivationInner;
use crate::kernel::{Coordinator, OwnerId, ScopeId};
use crate::plugin::Plugin;
use crate::resource::{ResourceOwner, TaskHandle};
use crate::scope::Scope;
use crate::service::{ServiceId, ServiceKey};

#[derive(Clone)]
pub struct Context {
    pub(crate) coordinator: Arc<Coordinator>,
    pub(crate) visible_scope: ScopeId,
    pub(crate) owner: OwnerId,
    pub(crate) activation: Option<Arc<ActivationInner>>,
}

impl Context {
    pub(crate) fn for_scope(coordinator: Arc<Coordinator>, scope: ScopeId) -> Self {
        Self {
            coordinator,
            visible_scope: scope,
            owner: OwnerId::Scope(scope),
            activation: None,
        }
    }

    pub(crate) fn for_activation(
        coordinator: Arc<Coordinator>,
        visible_scope: ScopeId,
        activation: Arc<ActivationInner>,
    ) -> Self {
        Self {
            coordinator,
            visible_scope,
            owner: OwnerId::Activation(activation.id),
            activation: Some(activation),
        }
    }

    pub fn get<T: ?Sized + Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        let key = ServiceKey::of::<T>();
        let value =
            self.coordinator
                .resolve(self.visible_scope, key, self.activation.as_deref())?;
        value.downcast_ref::<Arc<T>>().cloned()
    }

    pub fn require<T: ?Sized + Send + Sync + 'static>(&self) -> Result<Arc<T>, KernelError> {
        self.get::<T>().ok_or_else(|| {
            let key = ServiceKey::of::<T>();
            KernelError::ServiceNotFound {
                service: key.id.type_id(),
                name: key.name,
            }
        })
    }

    pub fn provide<T: ?Sized + Send + Sync + 'static>(
        &self,
        service: Arc<T>,
    ) -> Result<(), KernelError> {
        let key = ServiceKey::of::<T>();
        let erased: Arc<dyn Any + Send + Sync> = Arc::new(service);
        match self.owner {
            OwnerId::Scope(scope) => self.coordinator.provide_scope(scope, key, erased),
            OwnerId::Activation(_) => {
                let activation = self.activation.as_ref().ok_or(KernelError::ScopeClosed)?;
                activation.owner.with_open_registration(|| {
                    let mut staged = activation
                        .staged
                        .lock()
                        .map_err(|_| KernelError::Internal("activation lock poisoned".into()))?;
                    if staged.contains_key(&key.id)
                        || self
                            .coordinator
                            .has_local_binding(self.visible_scope, key.id)
                    {
                        return Err(KernelError::DuplicateService {
                            service: key.id.type_id(),
                            name: key.name,
                        });
                    }
                    staged.insert(key.id, crate::fiber::StagedService { key, value: erased });
                    Ok(())
                })
            }
        }
    }

    pub fn mount<P: Plugin>(&self, plugin: P) -> Result<crate::fiber::FiberHandle, KernelError> {
        let owner = self.owner_resource()?;
        self.coordinator
            .register_fiber(self.visible_scope, owner, plugin)
    }

    pub fn child(&self) -> Result<Scope, KernelError> {
        let owner = self.owner_resource()?;
        self.coordinator.create_scope(self.visible_scope, owner)
    }

    pub fn spawn<F>(&self, future: F) -> Result<TaskHandle, KernelError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.owner_resource()?
            .spawn(self.coordinator.next_task_id(), future)
    }

    pub fn defer<F, Fut>(&self, cleanup: F) -> Result<(), KernelError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.owner_resource()?.defer(cleanup)
    }

    fn owner_resource(&self) -> Result<Arc<ResourceOwner>, KernelError> {
        match self.owner {
            OwnerId::Scope(scope) => self
                .coordinator
                .scope_owner(scope)
                .ok_or(KernelError::ScopeClosed),
            OwnerId::Activation(_) => self
                .activation
                .as_ref()
                .map(|activation| Arc::clone(&activation.owner))
                .ok_or(KernelError::ScopeClosed),
        }
    }

    pub fn visible_scope_id(&self) -> ScopeId {
        self.visible_scope
    }

    pub fn service_id<T: ?Sized + 'static>() -> ServiceId {
        ServiceId::of::<T>()
    }
}
