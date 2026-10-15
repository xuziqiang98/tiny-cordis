use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::mpsc;

use crate::context::Context;
use crate::error::KernelError;
use crate::fiber::{ActivationInner, DesiredState, FiberHandle, FiberInner, FiberLife, FiberState};
use crate::plugin::{Plugin, PluginSpec};
use crate::resource::ResourceOwner;
use crate::scope::{Scope, ScopeRecord};
use crate::service::{ServiceId, ServiceKey};
use crate::unwind::catch_future;

pub(crate) type ScopeId = u64;
pub(crate) type FiberId = u64;
pub(crate) type ActivationId = u64;
pub(crate) type BindingId = u64;
pub(crate) type TaskId = u64;
pub(crate) type ErasedService = Arc<dyn Any + Send + Sync>;

fn close_result(outcome: &Mutex<Option<Vec<String>>>) -> Result<(), KernelError> {
    let errors = outcome
        .lock()
        .map_err(|_| KernelError::Internal("scope close result lock poisoned".into()))?
        .clone()
        .unwrap_or_default();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(KernelError::CleanupFailed {
            errors: errors
                .into_iter()
                .map(|message| Box::new(std::io::Error::other(message)) as crate::error::BoxError)
                .collect(),
        })
    }
}

fn error_messages(error: KernelError) -> Vec<String> {
    match error {
        KernelError::CleanupFailed { errors } => {
            errors.into_iter().map(|error| error.to_string()).collect()
        }
        error => vec![error.to_string()],
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeStatus {
    Open,
    Closing,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BindingState {
    Committed,
    Draining,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum OwnerId {
    Scope(ScopeId),
    Activation(ActivationId),
}

pub(crate) struct Binding {
    pub(crate) key: ServiceKey,
    pub(crate) visible_scope: ScopeId,
    pub(crate) value: ErasedService,
    pub(crate) state: BindingState,
}

pub(crate) struct State {
    pub(crate) root: ScopeId,
    pub(crate) scopes: HashMap<ScopeId, ScopeRecord>,
    pub(crate) fibers: HashMap<FiberId, Arc<FiberInner>>,
    pub(crate) bindings: HashMap<BindingId, Binding>,
    pub(crate) local_bindings: HashMap<ScopeId, HashMap<ServiceId, BindingId>>,
    pub(crate) activation_ids: HashMap<ActivationId, Arc<ActivationInner>>,
}

enum Command {
    Reconcile,
    DisposeFiber(FiberId),
    CloseScope(ScopeId),
    ApplyFinished {
        fiber_id: FiberId,
        generation: u64,
        activation_id: ActivationId,
        result: Result<(), String>,
    },
    StopFinished {
        fiber_id: FiberId,
        generation: u64,
        activation_id: ActivationId,
        errors: Vec<String>,
    },
    ScopeResourcesFinished {
        scope_id: ScopeId,
        errors: Vec<String>,
    },
    Terminate,
}

enum CommitResult {
    Committed,
    Retry,
    Failed(String),
}

pub(crate) struct Coordinator {
    pub(crate) state: RwLock<State>,
    tx: mpsc::UnboundedSender<Command>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<Command>>>,
    started: AtomicBool,
    terminate_requested: AtomicBool,
    next_id: AtomicU64,
}

impl Coordinator {
    pub(crate) fn new() -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let root = 1;
        let mut scopes = HashMap::new();
        scopes.insert(root, ScopeRecord::new(None));
        Arc::new(Self {
            state: RwLock::new(State {
                root,
                scopes,
                fibers: HashMap::new(),
                bindings: HashMap::new(),
                local_bindings: HashMap::new(),
                activation_ids: HashMap::new(),
            }),
            tx,
            rx: Mutex::new(Some(rx)),
            started: AtomicBool::new(false),
            terminate_requested: AtomicBool::new(false),
            next_id: AtomicU64::new(2),
        })
    }

    fn alloc(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn next_task_id(&self) -> TaskId {
        self.alloc()
    }

    pub(crate) fn scope_owner(&self, id: ScopeId) -> Option<Arc<ResourceOwner>> {
        self.state
            .read()
            .ok()
            .and_then(|state| state.scopes.get(&id).map(|scope| Arc::clone(&scope.owner)))
    }

    fn ensure_actor(self: &Arc<Self>) -> Result<(), KernelError> {
        if self.started.load(Ordering::Acquire) {
            return Ok(());
        }
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            KernelError::Internal("lifecycle operations require a Tokio runtime".into())
        })?;
        let Ok(mut receiver) = self.rx.lock() else {
            return Err(KernelError::Internal("coordinator lock poisoned".into()));
        };
        if self.started.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(receiver) = receiver.take() else {
            return Ok(());
        };
        self.started.store(true, Ordering::Release);
        handle.spawn(Self::run_actor(Arc::clone(self), receiver));
        Ok(())
    }

    async fn run_actor(self: Arc<Self>, mut receiver: mpsc::UnboundedReceiver<Command>) {
        while let Some(command) = receiver.recv().await {
            if matches!(command, Command::Terminate) {
                break;
            }
            self.handle(command);
        }
    }

    fn enqueue(self: &Arc<Self>, command: Command) -> Result<(), KernelError> {
        self.ensure_actor()?;
        self.tx.send(command).map_err(|_| KernelError::Shutdown)
    }

    pub(crate) fn create_scope(
        self: &Arc<Self>,
        parent: ScopeId,
        owner: Arc<ResourceOwner>,
    ) -> Result<Scope, KernelError> {
        let id = self.alloc();
        owner.with_open_registration(|| {
            let mut state = self
                .state
                .write()
                .map_err(|_| KernelError::Internal("coordinator lock poisoned".into()))?;
            let parent_record = state
                .scopes
                .get_mut(&parent)
                .ok_or(KernelError::ScopeClosed)?;
            if parent_record.status != ScopeStatus::Open {
                return Err(KernelError::ScopeClosed);
            }
            parent_record.children.insert(id);
            state.scopes.insert(id, ScopeRecord::new(Some(parent)));
            if let Err(error) = owner.push_child_scope(id) {
                state.scopes.remove(&id);
                if let Some(parent_record) = state.scopes.get_mut(&parent) {
                    parent_record.children.remove(&id);
                }
                return Err(error);
            }
            Ok(Scope::new(Arc::clone(self), id))
        })
    }

    pub(crate) fn register_fiber<P: Plugin>(
        self: &Arc<Self>,
        parent_scope: ScopeId,
        owner: Arc<ResourceOwner>,
        plugin: P,
    ) -> Result<FiberHandle, KernelError> {
        self.ensure_actor()?;
        let spec = PluginSpec::from(plugin);
        let id = self.alloc();
        let (state_tx, _) = tokio::sync::watch::channel(FiberState::Pending);
        let fiber = Arc::new(FiberInner {
            id,
            parent_scope,
            spec,
            state: Mutex::new(FiberState::Pending),
            state_tx,
            life: Mutex::new(FiberLife {
                generation: 0,
                activation: None,
                snapshot: HashMap::new(),
                desired: DesiredState::Running,
                waiting_dependents: HashSet::new(),
                stop_started: false,
                failure_pending: false,
                error: None,
                cleanup_errors: Vec::new(),
            }),
        });
        owner.with_open_registration(|| {
            let mut state = self
                .state
                .write()
                .map_err(|_| KernelError::Internal("coordinator lock poisoned".into()))?;
            let scope = state
                .scopes
                .get_mut(&parent_scope)
                .ok_or(KernelError::ScopeClosed)?;
            if scope.status != ScopeStatus::Open {
                return Err(KernelError::ScopeClosed);
            }
            scope.fibers.insert(id);
            state.fibers.insert(id, Arc::clone(&fiber));
            if let Err(error) = owner.push_child_fiber(id) {
                state.fibers.remove(&id);
                if let Some(scope) = state.scopes.get_mut(&parent_scope) {
                    scope.fibers.remove(&id);
                }
                return Err(error);
            }
            Ok(())
        })?;
        self.enqueue(Command::Reconcile)?;
        Ok(FiberHandle {
            inner: fiber,
            coordinator: Arc::clone(self),
        })
    }

    pub(crate) fn provide_scope(
        self: &Arc<Self>,
        scope_id: ScopeId,
        key: ServiceKey,
        value: ErasedService,
    ) -> Result<(), KernelError> {
        let owner = self.scope_owner(scope_id).ok_or(KernelError::ScopeClosed)?;
        let binding_id = self.alloc();
        owner.with_open_registration(|| {
            self.ensure_actor()?;
            let mut state = self
                .state
                .write()
                .map_err(|_| KernelError::Internal("coordinator lock poisoned".into()))?;
            let scope = state
                .scopes
                .get(&scope_id)
                .ok_or(KernelError::ScopeClosed)?;
            if scope.status != ScopeStatus::Open {
                return Err(KernelError::ScopeClosed);
            }
            let local = state.local_bindings.entry(scope_id).or_default();
            if local.contains_key(&key.id) {
                return Err(KernelError::DuplicateService {
                    service: key.id.type_id(),
                    name: key.name,
                });
            }
            local.insert(key.id, binding_id);
            state.bindings.insert(
                binding_id,
                Binding {
                    key,
                    visible_scope: scope_id,
                    value,
                    state: BindingState::Committed,
                },
            );
            drop(state);
            self.enqueue(Command::Reconcile)
        })
    }

    pub(crate) fn resolve(
        &self,
        visible_scope: ScopeId,
        key: ServiceKey,
        activation: Option<&ActivationInner>,
    ) -> Option<ErasedService> {
        let (staged_value, snapshot) = if let Some(activation) = activation {
            let staged_value = activation
                .staged
                .lock()
                .ok()?
                .get(&key.id)
                .map(|service| Arc::clone(&service.value));
            let dependency_snapshot = activation.dependency_bindings.lock().ok()?.clone();
            let snapshot = if dependency_snapshot.is_empty() {
                activation.required_bindings.lock().ok()?.clone()
            } else {
                dependency_snapshot
            };
            (staged_value, snapshot)
        } else {
            (None, HashMap::new())
        };
        if staged_value.is_some() {
            return staged_value;
        }

        let state = self.state.read().ok()?;
        for binding_id in snapshot.values() {
            let Some(binding) = state.bindings.get(binding_id) else {
                continue;
            };
            if binding.key.id == key.id {
                return Some(Arc::clone(&binding.value));
            }
        }
        let mut current = Some(visible_scope);
        while let Some(scope_id) = current {
            let Some(scope) = state.scopes.get(&scope_id) else {
                break;
            };
            if let Some(local) = state.local_bindings.get(&scope_id)
                && let Some(binding_id) = local.get(&key.id)
            {
                let binding = state.bindings.get(binding_id)?;
                if binding.state == BindingState::Committed {
                    return Some(Arc::clone(&binding.value));
                }
                return None;
            }
            current = scope.parent;
        }
        None
    }

    pub(crate) fn has_local_binding(&self, scope: ScopeId, service: ServiceId) -> bool {
        self.state
            .read()
            .ok()
            .and_then(|state| {
                state
                    .local_bindings
                    .get(&scope)
                    .map(|local| local.contains_key(&service))
            })
            .unwrap_or(false)
    }

    pub(crate) fn close_scope(
        self: &Arc<Self>,
        id: ScopeId,
    ) -> impl std::future::Future<Output = Result<(), KernelError>> + Send + 'static {
        let coordinator = Arc::clone(self);
        async move {
            let (already_closed, notify, owner, outcome) = {
                let state = coordinator
                    .state
                    .read()
                    .map_err(|_| KernelError::Internal("coordinator lock poisoned".into()))?;
                let scope = state.scopes.get(&id).ok_or(KernelError::ScopeClosed)?;
                (
                    scope.status == ScopeStatus::Closed,
                    Arc::clone(&scope.close_notify),
                    Arc::clone(&scope.owner),
                    Arc::clone(&scope.close_outcome),
                )
            };
            if already_closed {
                return close_result(&outcome);
            }
            owner.begin_close();
            {
                let mut state = coordinator
                    .state
                    .write()
                    .map_err(|_| KernelError::Internal("coordinator lock poisoned".into()))?;
                let Some(scope) = state.scopes.get_mut(&id) else {
                    return close_result(&outcome);
                };
                if scope.status == ScopeStatus::Open {
                    scope.status = ScopeStatus::Closing;
                }
            }
            coordinator.enqueue(Command::CloseScope(id))?;
            loop {
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let status = {
                    let state = coordinator
                        .state
                        .read()
                        .map_err(|_| KernelError::Internal("coordinator lock poisoned".into()))?;
                    let Some(scope) = state.scopes.get(&id) else {
                        return close_result(&outcome);
                    };
                    scope.status
                };
                if status == ScopeStatus::Closed {
                    return close_result(&outcome);
                }
                notified.await;
            }
        }
    }

    pub(crate) fn dispose_fiber(
        self: &Arc<Self>,
        id: FiberId,
    ) -> impl std::future::Future<Output = Result<(), KernelError>> + Send + 'static {
        let coordinator = Arc::clone(self);
        async move {
            let fiber = {
                let state = coordinator
                    .state
                    .read()
                    .map_err(|_| KernelError::Internal("coordinator lock poisoned".into()))?;
                state
                    .fibers
                    .get(&id)
                    .cloned()
                    .ok_or(KernelError::FiberDisposed)?
            };
            coordinator.enqueue(Command::DisposeFiber(id))?;
            let mut receiver = fiber.state_tx.subscribe();
            loop {
                if *receiver.borrow() == FiberState::Disposed {
                    let (error, cleanup_errors) = fiber
                        .life
                        .lock()
                        .map(|life| (life.error.clone(), life.cleanup_errors.clone()))
                        .unwrap_or_default();
                    if !cleanup_errors.is_empty() {
                        return Err(KernelError::CleanupFailed {
                            errors: cleanup_errors
                                .into_iter()
                                .map(|message| {
                                    Box::new(std::io::Error::other(message))
                                        as crate::error::BoxError
                                })
                                .collect(),
                        });
                    }
                    return error.map_or(Ok(()), |message| Err(KernelError::Internal(message)));
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| KernelError::FiberDisposed)?;
            }
        }
    }

    fn handle(self: &Arc<Self>, command: Command) {
        match command {
            Command::Reconcile => self.reconcile(),
            Command::DisposeFiber(id) => self.request_dispose(id),
            Command::CloseScope(id) => self.request_close_scope(id),
            Command::ApplyFinished {
                fiber_id,
                generation,
                activation_id,
                result,
            } => self.apply_finished(fiber_id, generation, activation_id, result),
            Command::StopFinished {
                fiber_id,
                generation,
                activation_id,
                errors,
            } => self.stop_finished(fiber_id, generation, activation_id, errors),
            Command::ScopeResourcesFinished { scope_id, errors } => {
                self.scope_resources_finished(scope_id, errors)
            }
            Command::Terminate => {}
        }
    }

    fn reconcile(self: &Arc<Self>) {
        let fibers = self
            .state
            .read()
            .map(|state| state.fibers.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for fiber in fibers {
            let state = fiber
                .state
                .lock()
                .map(|state| *state)
                .unwrap_or(FiberState::Disposed);
            let desired = fiber
                .life
                .lock()
                .map(|life| life.desired)
                .unwrap_or(DesiredState::Disposed);
            match (state, desired) {
                (FiberState::Pending, DesiredState::Running)
                    if self.dependencies_ready(&fiber, None) =>
                {
                    self.start_fiber(fiber)
                }
                (FiberState::Active, DesiredState::Running)
                    if !self.active_snapshot_matches(&fiber) =>
                {
                    self.request_stop(fiber.id, false)
                }
                (FiberState::Starting, DesiredState::Running)
                    if !self.dependencies_ready(&fiber, None) =>
                {
                    self.request_stop(fiber.id, false)
                }
                (_, DesiredState::Disposed)
                    if state != FiberState::Disposed && state != FiberState::Stopping =>
                {
                    self.request_stop(fiber.id, true)
                }
                _ => {}
            }
        }
    }

    fn dependencies_ready(
        &self,
        fiber: &Arc<FiberInner>,
        activation: Option<&ActivationInner>,
    ) -> bool {
        let staged = activation
            .and_then(|activation| activation.staged.lock().ok())
            .map(|staged| staged.keys().copied().collect::<HashSet<_>>())
            .unwrap_or_default();
        let Ok(state) = self.state.read() else {
            return false;
        };
        fiber.spec.dependencies.ids().all(|service| {
            resolve_id_locked(&state, fiber.parent_scope, service).is_some()
                || staged.contains(&service)
        })
    }

    fn active_snapshot_matches(&self, fiber: &Arc<FiberInner>) -> bool {
        let Ok(life) = fiber.life.lock() else {
            return false;
        };
        let Ok(state) = self.state.read() else {
            return false;
        };
        fiber.spec.dependencies.ids().all(|service| {
            let current = resolve_id_locked(&state, fiber.parent_scope, service);
            current == life.snapshot.get(&service).copied()
        })
    }

    fn current_dependency_snapshot(
        &self,
        fiber: &Arc<FiberInner>,
    ) -> Option<HashMap<ServiceId, BindingId>> {
        let state = self.state.read().ok()?;
        let mut snapshot = HashMap::new();
        for service in fiber.spec.dependencies.ids() {
            snapshot.insert(
                service,
                resolve_id_locked(&state, fiber.parent_scope, service)?,
            );
        }
        Some(snapshot)
    }

    fn start_fiber(self: &Arc<Self>, fiber: Arc<FiberInner>) {
        let Some(initial_snapshot) = self.current_dependency_snapshot(&fiber) else {
            return;
        };
        let (activation, generation) = {
            let Ok(mut life) = fiber.life.lock() else {
                return;
            };
            if life.desired != DesiredState::Running || life.activation.is_some() {
                return;
            }
            let generation = life.generation.saturating_add(1);
            life.generation = generation;
            let activation = Arc::new(ActivationInner {
                id: self.alloc(),
                fiber_id: fiber.id,
                owner: ResourceOwner::new(),
                staged: Mutex::new(HashMap::new()),
                bound_ids: Mutex::new(Vec::new()),
                required_bindings: Mutex::new(initial_snapshot.clone()),
                dependency_bindings: Mutex::new(HashMap::new()),
                cleanup_started: Mutex::new(false),
                sealed: AtomicBool::new(false),
                apply_handle: Mutex::new(None),
            });
            life.activation = Some(Arc::clone(&activation));
            life.snapshot.clear();
            life.error = None;
            life.failure_pending = false;
            if let Ok(mut state) = fiber.state.lock() {
                *state = FiberState::Starting;
            }
            fiber.state_tx.send_replace(FiberState::Starting);
            (activation, generation)
        };

        if let Ok(mut state) = self.state.write() {
            state
                .activation_ids
                .insert(activation.id, Arc::clone(&activation));
        }
        let coordinator = Arc::clone(self);
        let apply = Arc::clone(&fiber.spec.apply);
        let plugin_name = fiber.spec.name.clone();
        let context = Context::for_activation(
            Arc::clone(self),
            fiber.parent_scope,
            Arc::clone(&activation),
        );
        let activation_for_task = Arc::clone(&activation);
        let handle = tokio::spawn(async move {
            let result = match catch_future(async move { (apply)(context).await }).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(format!("{plugin_name}: {error}")),
                Err(message) => Err(format!("{plugin_name} panicked: {message}")),
            };
            activation_for_task.seal_services();
            let _ = coordinator.enqueue(Command::ApplyFinished {
                fiber_id: activation_for_task.fiber_id,
                generation,
                activation_id: activation_for_task.id,
                result,
            });
        });
        if let Ok(mut slot) = activation.apply_handle.lock() {
            *slot = Some(handle);
        }
    }

    fn apply_finished(
        self: &Arc<Self>,
        fiber_id: FiberId,
        generation: u64,
        activation_id: ActivationId,
        result: Result<(), String>,
    ) {
        let Some(fiber) = self
            .state
            .read()
            .ok()
            .and_then(|state| state.fibers.get(&fiber_id).cloned())
        else {
            return;
        };
        let Some(activation) = fiber.life.lock().ok().and_then(|life| {
            (life.generation == generation
                && life.activation.as_ref().map(|a| a.id) == Some(activation_id))
            .then(|| life.activation.clone())
            .flatten()
        }) else {
            return;
        };
        if let Ok(mut handle) = activation.apply_handle.lock() {
            handle.take();
        }
        let can_commit = result.is_ok()
            && fiber
                .state
                .lock()
                .map(|state| *state == FiberState::Starting)
                .unwrap_or(false)
            && fiber
                .life
                .lock()
                .map(|life| life.desired == DesiredState::Running)
                .unwrap_or(false)
            && self.dependencies_ready(&fiber, Some(&activation));
        if can_commit {
            match self.commit_activation(&fiber, &activation) {
                CommitResult::Committed => {
                    let _ = self.enqueue(Command::Reconcile);
                    return;
                }
                CommitResult::Retry => {
                    self.finish_activation(fiber, generation, None);
                    return;
                }
                CommitResult::Failed(message) => {
                    self.finish_activation(fiber, generation, Some(message));
                    return;
                }
            }
        }

        if let Err(message) = result {
            self.finish_activation(fiber, generation, Some(message));
        } else {
            self.finish_activation(fiber, generation, None);
        }
    }

    fn finish_activation(
        self: &Arc<Self>,
        fiber: Arc<FiberInner>,
        generation: u64,
        failure: Option<String>,
    ) {
        let activation = {
            let Ok(mut life) = fiber.life.lock() else {
                return;
            };
            if life.stop_started {
                None
            } else {
                if let Some(message) = failure {
                    life.error = Some(message);
                    life.failure_pending = true;
                }
                life.stop_started = true;
                if let Ok(mut state) = fiber.state.lock() {
                    *state = FiberState::Stopping;
                }
                fiber.state_tx.send_replace(FiberState::Stopping);
                life.activation.clone()
            }
        };
        if let Some(activation) = activation {
            activation.owner.begin_close();
            self.launch_cleanup(fiber, generation);
        }
    }

    fn commit_activation(
        &self,
        fiber: &Arc<FiberInner>,
        activation: &Arc<ActivationInner>,
    ) -> CommitResult {
        let staged = match activation.staged.lock().map(|staged| {
            staged
                .values()
                .map(|service| (service.key, Arc::clone(&service.value)))
                .collect::<Vec<_>>()
        }) {
            Ok(staged) => staged,
            Err(_) => return CommitResult::Failed("activation lock poisoned".into()),
        };
        let required = match activation
            .required_bindings
            .lock()
            .map(|snapshot| snapshot.clone())
        {
            Ok(required) => required,
            Err(_) => return CommitResult::Failed("activation lock poisoned".into()),
        };
        let mut state = match self.state.write() {
            Ok(state) => state,
            Err(_) => return CommitResult::Failed("coordinator lock poisoned".into()),
        };
        let Some(scope) = state.scopes.get(&fiber.parent_scope) else {
            return CommitResult::Failed("parent scope is unavailable".into());
        };
        if scope.status != ScopeStatus::Open {
            return CommitResult::Retry;
        }
        if let Some((key, _)) = staged.iter().find(|(key, _)| {
            state
                .local_bindings
                .get(&fiber.parent_scope)
                .is_some_and(|local| local.contains_key(&key.id))
        }) {
            return CommitResult::Failed(
                KernelError::DuplicateService {
                    service: key.id.type_id(),
                    name: key.name,
                }
                .to_string(),
            );
        }
        for service in fiber.spec.dependencies.ids() {
            if resolve_id_locked(&state, fiber.parent_scope, service)
                != required.get(&service).copied()
            {
                return CommitResult::Retry;
            }
        }
        let mut ids = Vec::with_capacity(staged.len());
        for (key, value) in staged {
            let id = self.alloc();
            state
                .local_bindings
                .entry(fiber.parent_scope)
                .or_default()
                .insert(key.id, id);
            state.bindings.insert(
                id,
                Binding {
                    key,
                    visible_scope: fiber.parent_scope,
                    value,
                    state: BindingState::Committed,
                },
            );
            ids.push(id);
        }
        drop(state);
        if let Ok(mut bound_ids) = activation.bound_ids.lock() {
            *bound_ids = ids.clone();
        }
        if let Ok(mut life) = fiber.life.lock() {
            life.snapshot = required.clone();
        }
        if let Ok(mut snapshot) = activation.dependency_bindings.lock() {
            *snapshot = required;
        }
        if let Ok(mut current) = fiber.state.lock() {
            *current = FiberState::Active;
        }
        fiber.state_tx.send_replace(FiberState::Active);
        CommitResult::Committed
    }

    fn request_dispose(self: &Arc<Self>, id: FiberId) {
        let Some(fiber) = self
            .state
            .read()
            .ok()
            .and_then(|state| state.fibers.get(&id).cloned())
        else {
            return;
        };
        if let Ok(mut life) = fiber.life.lock() {
            life.desired = DesiredState::Disposed;
        }
        self.request_stop(id, true);
    }

    fn request_stop(self: &Arc<Self>, id: FiberId, dispose: bool) {
        let Some(fiber) = self
            .state
            .read()
            .ok()
            .and_then(|state| state.fibers.get(&id).cloned())
        else {
            return;
        };
        let current = fiber
            .state
            .lock()
            .map(|state| *state)
            .unwrap_or(FiberState::Disposed);
        let (activation, generation) = {
            let Ok(mut life) = fiber.life.lock() else {
                return;
            };
            if dispose {
                life.desired = DesiredState::Disposed;
            }
            if current == FiberState::Disposed
                || current == FiberState::Pending
                || current == FiberState::Failed
            {
                if life.desired == DesiredState::Disposed {
                    if let Ok(mut state) = fiber.state.lock() {
                        *state = FiberState::Disposed;
                    }
                    fiber.state_tx.send_replace(FiberState::Disposed);
                    let owner = self.state.read().ok().and_then(|state| {
                        state
                            .scopes
                            .get(&fiber.parent_scope)
                            .map(|scope| Arc::clone(&scope.owner))
                    });
                    if let Ok(mut state) = self.state.write() {
                        if let Some(scope) = state.scopes.get_mut(&fiber.parent_scope) {
                            scope.fibers.remove(&fiber.id);
                        }
                        state.fibers.remove(&fiber.id);
                    }
                    if let Some(owner) = owner {
                        owner.remove_child_fiber(fiber.id);
                    }
                }
                return;
            }
            if life.stop_started {
                return;
            }
            life.stop_started = true;
            life.generation = life.generation.saturating_add(1);
            (life.activation.clone(), life.generation)
        };
        if let Some(activation) = &activation {
            activation.owner.begin_close();
        }

        let mut dependents = Vec::new();
        if let Some(activation) = &activation {
            let bound_ids = activation
                .bound_ids
                .lock()
                .map(|ids| ids.clone())
                .unwrap_or_default();
            let others = self
                .state
                .read()
                .map(|state| state.fibers.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            if let Ok(mut state) = self.state.write() {
                for binding_id in &bound_ids {
                    if let Some(binding) = state.bindings.get_mut(binding_id) {
                        binding.state = BindingState::Draining;
                    }
                }
            }
            for other in others {
                if other.id == fiber.id {
                    continue;
                }
                let Ok(other_life) = other.life.lock() else {
                    continue;
                };
                let depends_on_binding = other_life
                    .snapshot
                    .values()
                    .any(|id| bound_ids.contains(id))
                    || other_life
                        .activation
                        .as_ref()
                        .and_then(|current| current.required_bindings.lock().ok())
                        .map(|snapshot| snapshot.values().any(|id| bound_ids.contains(id)))
                        .unwrap_or(false);
                if depends_on_binding {
                    dependents.push(other.id);
                }
            }
        }
        if let Ok(mut life) = fiber.life.lock() {
            life.waiting_dependents = dependents.iter().copied().collect();
        }
        if let Ok(mut state) = fiber.state.lock() {
            *state = FiberState::Stopping;
        }
        fiber.state_tx.send_replace(FiberState::Stopping);
        let immediate = dependents.is_empty();
        for dependent in dependents {
            self.request_stop(dependent, false);
        }
        if immediate {
            if activation.is_some() {
                self.launch_cleanup(fiber, generation);
            } else {
                self.finish_without_activation(fiber, generation);
            }
        }
    }

    fn launch_cleanup(self: &Arc<Self>, fiber: Arc<FiberInner>, generation: u64) {
        let Some(activation) = fiber
            .life
            .lock()
            .ok()
            .and_then(|life| life.activation.clone())
        else {
            return;
        };
        let Ok(mut started) = activation.cleanup_started.lock() else {
            return;
        };
        if *started {
            return;
        }
        *started = true;
        drop(started);
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            let apply_handle = activation
                .apply_handle
                .lock()
                .ok()
                .and_then(|mut handle| handle.take());
            if let Some(handle) = apply_handle {
                handle.abort();
                let _ = handle.await;
            }
            let (child_scopes, child_fibers) = activation.owner.take_children();
            let mut errors = Vec::new();
            for child in child_scopes {
                if let Err(error) = coordinator.close_scope(child).await {
                    errors.extend(error_messages(error));
                }
            }
            for child in child_fibers {
                if let Err(error) = coordinator.dispose_fiber(child).await {
                    errors.extend(error_messages(error));
                }
            }
            if let Err(error) = activation.owner.close_resources().await {
                errors.extend(error_messages(error));
            }
            let _ = coordinator.enqueue(Command::StopFinished {
                fiber_id: fiber.id,
                generation,
                activation_id: activation.id,
                errors,
            });
        });
    }

    fn finish_without_activation(self: &Arc<Self>, fiber: Arc<FiberInner>, generation: u64) {
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            let _ = coordinator.enqueue(Command::StopFinished {
                fiber_id: fiber.id,
                generation,
                activation_id: 0,
                errors: Vec::new(),
            });
        });
    }

    fn stop_finished(
        self: &Arc<Self>,
        fiber_id: FiberId,
        generation: u64,
        activation_id: ActivationId,
        errors: Vec<String>,
    ) {
        let Some(fiber) = self
            .state
            .read()
            .ok()
            .and_then(|state| state.fibers.get(&fiber_id).cloned())
        else {
            return;
        };
        let (desired, failed) = {
            let Ok(mut life) = fiber.life.lock() else {
                return;
            };
            if life.generation != generation || !life.stop_started {
                return;
            }
            if !errors.is_empty() {
                let cleanup_message = errors.join("; ");
                life.error = Some(match life.error.take() {
                    Some(message) => format!("{message}; cleanup failed: {cleanup_message}"),
                    None => cleanup_message,
                });
                life.cleanup_errors.extend(errors.iter().cloned());
            }
            let desired = life.desired;
            let failed = life.failure_pending;
            life.activation = None;
            life.snapshot.clear();
            life.stop_started = false;
            life.failure_pending = false;
            (desired, failed)
        };
        if desired == DesiredState::Disposed
            && !errors.is_empty()
            && let Ok(mut state) = self.state.write()
            && let Some(scope) = state.scopes.get_mut(&fiber.parent_scope)
            && scope.status == ScopeStatus::Closing
        {
            scope.close_errors.extend(errors.iter().cloned());
        }
        let activation = if activation_id != 0 {
            self.state
                .write()
                .ok()
                .and_then(|mut state| state.activation_ids.remove(&activation_id))
        } else {
            None
        };
        if let Some(activation) = activation {
            let ids = activation
                .bound_ids
                .lock()
                .map(|ids| ids.clone())
                .unwrap_or_default();
            if let Ok(mut state) = self.state.write() {
                for binding_id in ids {
                    if let Some(binding) = state.bindings.remove(&binding_id)
                        && let Some(local) = state.local_bindings.get_mut(&binding.visible_scope)
                    {
                        local.remove(&binding.key.id);
                    }
                }
            }
        }
        if let Ok(mut state) = fiber.state.lock() {
            *state = if desired == DesiredState::Disposed {
                FiberState::Disposed
            } else if failed {
                FiberState::Failed
            } else {
                FiberState::Pending
            };
        }
        fiber
            .state_tx
            .send_replace(if desired == DesiredState::Disposed {
                FiberState::Disposed
            } else if failed {
                FiberState::Failed
            } else {
                FiberState::Pending
            });
        if desired == DesiredState::Disposed {
            let owner = if let Ok(mut state) = self.state.write() {
                let owner = state
                    .scopes
                    .get(&fiber.parent_scope)
                    .map(|scope| Arc::clone(&scope.owner));
                if let Some(scope) = state.scopes.get_mut(&fiber.parent_scope) {
                    scope.fibers.remove(&fiber.id);
                }
                state.fibers.remove(&fiber.id);
                owner
            } else {
                None
            };
            if let Some(owner) = owner {
                owner.remove_child_fiber(fiber.id);
            }
        }
        let mut providers_to_cleanup = Vec::new();
        let others = self
            .state
            .read()
            .map(|state| state.fibers.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for other in others {
            let Ok(mut life) = other.life.lock() else {
                continue;
            };
            if life.waiting_dependents.remove(&fiber.id)
                && life.waiting_dependents.is_empty()
                && life.stop_started
                && let Some(activation) = life.activation.clone()
            {
                providers_to_cleanup.push((Arc::clone(&other), activation, life.generation));
            }
        }
        for (provider, activation, generation) in providers_to_cleanup {
            self.launch_cleanup(provider, generation);
            drop(activation);
        }
        let _ = self.enqueue(Command::Reconcile);
        let scopes = self
            .state
            .read()
            .map(|state| state.scopes.keys().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        for scope in scopes {
            self.maybe_finish_scope_close(scope);
        }
    }

    fn request_close_scope(self: &Arc<Self>, id: ScopeId) {
        let (children, fibers, owner) = {
            let Ok(mut state) = self.state.write() else {
                return;
            };
            let Some(scope) = state.scopes.get_mut(&id) else {
                return;
            };
            if scope.status == ScopeStatus::Closed {
                scope.close_notify.notify_waiters();
                return;
            }
            scope.status = ScopeStatus::Closing;
            (
                scope.children.iter().copied().collect::<Vec<_>>(),
                scope.fibers.iter().copied().collect::<Vec<_>>(),
                Arc::clone(&scope.owner),
            )
        };
        owner.begin_close();
        let _ = owner.take_children();
        for child in children {
            self.request_close_scope(child);
        }
        for fiber in fibers {
            self.request_stop(fiber, true);
        }
        self.maybe_finish_scope_close(id);
    }

    fn maybe_finish_scope_close(self: &Arc<Self>, id: ScopeId) {
        let owner = {
            let Ok(mut state) = self.state.write() else {
                return;
            };
            let Some(scope) = state.scopes.get(&id) else {
                return;
            };
            let children = scope.children.iter().copied().collect::<Vec<_>>();
            let fibers = scope.fibers.iter().copied().collect::<Vec<_>>();
            let status = scope.status;
            let resources_started = scope.resources_started;
            let owner = Arc::clone(&scope.owner);
            let children_closed = children.iter().all(|child| {
                state
                    .scopes
                    .get(child)
                    .map(|record| record.status == ScopeStatus::Closed)
                    .unwrap_or(true)
            });
            let fibers_disposed = fibers.iter().all(|fiber| {
                state
                    .fibers
                    .get(fiber)
                    .map(|value| {
                        value
                            .state
                            .lock()
                            .map(|state| *state == FiberState::Disposed)
                            .unwrap_or(true)
                    })
                    .unwrap_or(true)
            });
            if status != ScopeStatus::Closing
                || resources_started
                || !children_closed
                || !fibers_disposed
            {
                return;
            }
            if let Some(scope) = state.scopes.get_mut(&id) {
                scope.resources_started = true;
            }
            owner
        };
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            let errors = owner
                .close_resources()
                .await
                .err()
                .map(error_messages)
                .unwrap_or_default();
            let _ = coordinator.enqueue(Command::ScopeResourcesFinished {
                scope_id: id,
                errors,
            });
        });
    }

    fn scope_resources_finished(self: &Arc<Self>, id: ScopeId, errors: Vec<String>) {
        let (notify, parent, all_errors) = {
            let Ok(mut state) = self.state.write() else {
                return;
            };
            let Some(scope) = state.scopes.get_mut(&id) else {
                return;
            };
            scope.close_errors.extend(errors);
            let all_errors = std::mem::take(&mut scope.close_errors);
            if let Ok(mut outcome) = scope.close_outcome.lock() {
                *outcome = Some(all_errors.clone());
            }
            scope.status = ScopeStatus::Closed;
            (Arc::clone(&scope.close_notify), scope.parent, all_errors)
        };
        let parent_owner = if let Ok(mut state) = self.state.write() {
            for binding_id in state
                .local_bindings
                .remove(&id)
                .unwrap_or_default()
                .into_values()
            {
                state.bindings.remove(&binding_id);
            }
            if let Some(parent_scope) = parent.and_then(|parent| state.scopes.get_mut(&parent)) {
                parent_scope.children.remove(&id);
                if parent_scope.status == ScopeStatus::Closing {
                    parent_scope.close_errors.extend(all_errors);
                }
            }
            if parent.is_some() {
                state.scopes.remove(&id);
            }
            parent.and_then(|parent| {
                state
                    .scopes
                    .get(&parent)
                    .map(|scope| Arc::clone(&scope.owner))
            })
        } else {
            None
        };
        if let Some(parent_owner) = parent_owner {
            parent_owner.remove_child_scope(id);
        }
        notify.notify_waiters();
        if let Some(parent) = parent {
            self.maybe_finish_scope_close(parent);
        }
    }
}

fn resolve_id_locked(
    state: &State,
    visible_scope: ScopeId,
    service: ServiceId,
) -> Option<BindingId> {
    let mut current = Some(visible_scope);
    while let Some(scope_id) = current {
        let scope = state.scopes.get(&scope_id)?;
        if scope.status != ScopeStatus::Open {
            return None;
        }
        if let Some(local) = state.local_bindings.get(&scope_id)
            && let Some(binding_id) = local.get(&service)
        {
            let binding = state.bindings.get(binding_id)?;
            return (binding.state == BindingState::Committed).then_some(*binding_id);
        }
        current = scope.parent;
    }
    None
}

pub struct Kernel {
    pub(crate) coordinator: Arc<Coordinator>,
}

impl Kernel {
    pub fn new() -> Self {
        Self {
            coordinator: Coordinator::new(),
        }
    }

    pub fn root(&self) -> Context {
        Context::for_scope(
            Arc::clone(&self.coordinator),
            self.coordinator
                .state
                .read()
                .map(|state| state.root)
                .unwrap_or(1),
        )
    }

    pub async fn shutdown(&self) -> Result<(), KernelError> {
        let root = self
            .coordinator
            .state
            .read()
            .map(|state| state.root)
            .unwrap_or(1);
        let result = self.coordinator.close_scope(root).await;
        if !self
            .coordinator
            .terminate_requested
            .swap(true, Ordering::AcqRel)
        {
            let _ = self.coordinator.tx.send(Command::Terminate);
        }
        result
    }
}

impl Default for Kernel {
    fn default() -> Self {
        Self::new()
    }
}
