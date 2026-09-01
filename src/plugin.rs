use std::future::Future;

use crate::context::Context;
use crate::error::KernelError;
use crate::service::{ServiceId, ServiceKey};

#[derive(Clone, Debug, Default)]
pub struct Dependencies {
    pub(crate) entries: Vec<ServiceKey>,
}

impl Dependencies {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn require<T: ?Sized + Send + Sync + 'static>(mut self) -> Self {
        let key = ServiceKey::of::<T>();
        if !self.entries.iter().any(|entry| entry.id == key.id) {
            self.entries.push(key);
        }
        self
    }

    pub(crate) fn ids(&self) -> impl Iterator<Item = ServiceId> + '_ {
        self.entries.iter().map(|entry| entry.id)
    }
}

pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    fn requires(&self) -> Dependencies {
        Dependencies::new()
    }

    fn apply(&self, ctx: Context) -> impl Future<Output = Result<(), KernelError>> + Send;
}

pub(crate) type BoxFuture =
    std::pin::Pin<Box<dyn Future<Output = Result<(), KernelError>> + Send + 'static>>;

pub(crate) struct PluginSpec {
    pub name: String,
    pub dependencies: Dependencies,
    pub apply: std::sync::Arc<dyn Fn(Context) -> BoxFuture + Send + Sync>,
}

impl PluginSpec {
    pub fn from<P: Plugin>(plugin: P) -> Self {
        let plugin = std::sync::Arc::new(plugin);
        let name = plugin.name().to_owned();
        let dependencies = plugin.requires();
        let apply_plugin = std::sync::Arc::clone(&plugin);
        let apply = std::sync::Arc::new(move |ctx: Context| {
            let plugin = std::sync::Arc::clone(&apply_plugin);
            Box::pin(async move { plugin.apply(ctx).await }) as BoxFuture
        });
        Self {
            name,
            dependencies,
            apply,
        }
    }
}
