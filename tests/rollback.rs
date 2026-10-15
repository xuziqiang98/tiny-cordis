use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use tiny_cordis::{Context, FiberState, Kernel, KernelError, Plugin};

struct Failing;
impl Plugin for Failing {
    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        ctx.provide::<u64>(Arc::new(9))?;
        ctx.defer(|| async { Ok(()) })?;
        Err(KernelError::Internal("boom".into()))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn apply_failure_rolls_back_staged_resources() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let fiber = ctx.mount(Failing)?;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if fiber.state() == FiberState::Failed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| KernelError::Internal("fiber did not fail".into()))?;
    assert!(ctx.get::<u64>().is_none());
    assert!(matches!(
        fiber.wait_active().await,
        Err(KernelError::PluginFailed { .. })
    ));
    kernel.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn task_is_cancelled_with_scope() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let stopped = Arc::new(Mutex::new(false));
    let stopped_task = Arc::clone(&stopped);
    let task = ctx.spawn(async move {
        struct Guard(Arc<Mutex<bool>>);
        impl Drop for Guard {
            fn drop(&mut self) {
                *self.0.lock().unwrap() = true;
            }
        }
        let _guard = Guard(stopped_task);
        std::future::pending::<()>().await;
    })?;
    kernel.shutdown().await?;
    assert!(task.is_finished());
    assert!(*stopped.lock().unwrap());
    Ok(())
}

struct FailingResources {
    started: Arc<Notify>,
    task_dropped: Arc<std::sync::atomic::AtomicUsize>,
    cleanups: Arc<std::sync::atomic::AtomicUsize>,
}

impl Plugin for FailingResources {
    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        let started = Arc::clone(&self.started);
        let task_dropped = Arc::clone(&self.task_dropped);
        ctx.spawn(async move {
            struct Guard(Arc<std::sync::atomic::AtomicUsize>);
            impl Drop for Guard {
                fn drop(&mut self) {
                    self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
            let _guard = Guard(task_dropped);
            started.notify_one();
            std::future::pending::<()>().await;
        })?;
        self.started.notified().await;
        let cleanups = Arc::clone(&self.cleanups);
        ctx.defer(move || async move {
            cleanups.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })?;
        ctx.provide::<u64>(Arc::new(7))?;
        Err(KernelError::Internal("apply failed".into()))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn apply_failure_cancels_task_and_runs_cleanup_once() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let started = Arc::new(Notify::new());
    let task_dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cleanups = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fiber = ctx.mount(FailingResources {
        started,
        task_dropped: Arc::clone(&task_dropped),
        cleanups: Arc::clone(&cleanups),
    })?;
    assert!(matches!(
        fiber.wait_active().await,
        Err(KernelError::PluginFailed { .. })
    ));
    assert_eq!(fiber.state(), FiberState::Failed);
    assert!(ctx.get::<u64>().is_none());
    assert_eq!(task_dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(cleanups.load(std::sync::atomic::Ordering::SeqCst), 1);
    kernel.shutdown().await?;
    assert_eq!(cleanups.load(std::sync::atomic::Ordering::SeqCst), 1);
    Ok(())
}

struct PanickingApply(Arc<std::sync::atomic::AtomicUsize>);

impl Plugin for PanickingApply {
    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        let cleanups = Arc::clone(&self.0);
        ctx.defer(move || async move {
            cleanups.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })?;
        ctx.provide::<u32>(Arc::new(9))?;
        panic!("plugin exploded");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn apply_panic_fails_fiber_and_rolls_back_resources() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let cleanups = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fiber = ctx.mount(PanickingApply(Arc::clone(&cleanups)))?;

    let error = tokio::time::timeout(std::time::Duration::from_secs(1), fiber.wait_active())
        .await
        .map_err(|_| KernelError::Internal("panicking plugin did not settle".into()))?
        .unwrap_err();
    assert!(matches!(error, KernelError::PluginFailed { .. }));
    assert_eq!(fiber.state(), FiberState::Failed);
    assert!(ctx.get::<u32>().is_none());
    assert_eq!(cleanups.load(std::sync::atomic::Ordering::SeqCst), 1);
    kernel.shutdown().await?;
    Ok(())
}

struct CaptureActivationContext(Arc<Mutex<Option<Context>>>);

impl Plugin for CaptureActivationContext {
    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        *self.0.lock().unwrap() = Some(ctx);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn activation_context_rejects_service_registration_after_apply() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let root = kernel.root();
    let captured = Arc::new(Mutex::new(None));
    let fiber = root.mount(CaptureActivationContext(Arc::clone(&captured)))?;
    fiber.wait_active().await?;

    let context = captured.lock().unwrap().clone().unwrap();
    assert!(matches!(
        context.provide::<u64>(Arc::new(5)),
        Err(KernelError::ActivationSealed)
    ));
    assert!(root.get::<u64>().is_none());
    kernel.shutdown().await?;
    Ok(())
}

struct CleanupFailure;

impl Plugin for CleanupFailure {
    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        ctx.defer(|| async { Err(std::io::Error::other("plugin cleanup failed").into()) })?;
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_reports_plugin_cleanup_errors() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let fiber = kernel.root().mount(CleanupFailure)?;
    fiber.wait_active().await?;

    let error = kernel.shutdown().await.unwrap_err();
    assert!(matches!(error, KernelError::CleanupFailed { errors } if errors.len() == 1));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fiber_dispose_returns_cleanup_errors() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let fiber = kernel.root().mount(CleanupFailure)?;
    fiber.wait_active().await?;

    let error = fiber.dispose().await.unwrap_err();
    assert!(matches!(error, KernelError::CleanupFailed { errors } if errors.len() == 1));
    kernel.shutdown().await?;
    Ok(())
}
