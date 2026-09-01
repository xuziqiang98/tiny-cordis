#![forbid(unsafe_code)]

mod context;
mod error;
mod fiber;
mod kernel;
mod plugin;
mod resource;
mod scope;
mod service;

pub use context::Context;
pub use error::{BoxError, KernelError};
pub use fiber::{FiberHandle, FiberState};
pub use kernel::Kernel;
pub use plugin::{Dependencies, Plugin};
pub use resource::TaskHandle;
pub use scope::Scope;
pub use service::ServiceId;
