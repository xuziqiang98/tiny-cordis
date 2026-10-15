use std::any::TypeId;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug)]
pub enum KernelError {
    ServiceNotFound { service: TypeId, name: &'static str },
    DuplicateService { service: TypeId, name: &'static str },
    ScopeClosed,
    ActivationSealed,
    FiberDisposed,
    PluginFailed { name: String, source: BoxError },
    CleanupFailed { errors: Vec<BoxError> },
    Shutdown,
    Internal(String),
}

impl KernelError {
    pub(crate) fn cleanup(errors: Vec<BoxError>) -> Result<(), Self> {
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Self::CleanupFailed { errors })
        }
    }
}

impl Display for KernelError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServiceNotFound { name, .. } => write!(f, "service {name} is not available"),
            Self::DuplicateService { name, .. } => {
                write!(f, "service {name} is already provided in this scope")
            }
            Self::ScopeClosed => f.write_str("scope is closed"),
            Self::ActivationSealed => f.write_str("activation is no longer accepting services"),
            Self::FiberDisposed => f.write_str("fiber is disposed"),
            Self::PluginFailed { name, source } => write!(f, "plugin {name} failed: {source}"),
            Self::CleanupFailed { errors } => {
                write!(f, "{} cleanup operation(s) failed", errors.len())
            }
            Self::Shutdown => f.write_str("kernel has shut down"),
            Self::Internal(message) => write!(f, "internal kernel error: {message}"),
        }
    }
}

impl Error for KernelError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PluginFailed { source, .. } => Some(source.as_ref()),
            Self::CleanupFailed { .. } => None,
            _ => None,
        }
    }
}
