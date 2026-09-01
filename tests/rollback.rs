use std::sync::{Arc, Mutex};

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
