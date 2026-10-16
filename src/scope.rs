use std::sync::{Arc, OnceLock};

use tokio::sync::Notify;

use crate::context::Context;
use crate::error::KernelError;
use crate::kernel::{Coordinator, ScopeId, ScopeStatus};
use crate::resource::ResourceOwner;

#[derive(Clone)]
pub struct Scope {
    pub(crate) coordinator: Arc<Coordinator>,
    pub(crate) id: ScopeId,
}

impl Scope {
    pub(crate) fn new(coordinator: Arc<Coordinator>, id: ScopeId) -> Self {
        Self { coordinator, id }
    }

    pub fn context(&self) -> Context {
        Context::for_scope(Arc::clone(&self.coordinator), self.id)
    }

    pub async fn close(&self) -> Result<(), KernelError> {
        self.coordinator.close_scope(self.id).await
    }
}

pub(crate) struct ScopeRecord {
    pub(crate) parent: Option<ScopeId>,
    pub(crate) status: ScopeStatus,
    pub(crate) children: std::collections::HashSet<ScopeId>,
    pub(crate) fibers: std::collections::HashSet<u64>,
    pub(crate) owner: Arc<ResourceOwner>,
    pub(crate) resources_started: bool,
    pub(crate) close_notify: Arc<Notify>,
    pub(crate) close_errors: Vec<String>,
    pub(crate) close_outcome: Arc<OnceLock<Vec<String>>>,
}

impl ScopeRecord {
    pub(crate) fn new(parent: Option<ScopeId>) -> Self {
        Self {
            parent,
            status: ScopeStatus::Open,
            children: std::collections::HashSet::new(),
            fibers: std::collections::HashSet::new(),
            owner: ResourceOwner::new(),
            resources_started: false,
            close_notify: Arc::new(Notify::new()),
            close_errors: Vec::new(),
            close_outcome: Arc::new(OnceLock::new()),
        }
    }
}
