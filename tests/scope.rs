use std::sync::{Arc, Mutex};

use tiny_cordis::{Context, Kernel, KernelError};

#[tokio::test(flavor = "current_thread")]
async fn child_scope_inherits_and_overrides_services() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let root = kernel.root();
    root.provide::<u32>(Arc::new(1))?;
    assert_eq!(*root.require::<u32>()?, 1);

    let child = root.child()?;
    let child_ctx = child.context();
    assert_eq!(*child_ctx.require::<u32>()?, 1);
    child_ctx.provide::<u32>(Arc::new(2))?;
    assert_eq!(*child_ctx.require::<u32>()?, 2);
    assert_eq!(*root.require::<u32>()?, 1);
    child.close().await?;
    assert_eq!(*root.require::<u32>()?, 1);
    kernel.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn scope_cleanup_is_lifo_and_idempotent() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let events = Arc::new(Mutex::new(Vec::new()));
    for value in 1..=3 {
        let events = Arc::clone(&events);
        ctx.defer(move || async move {
            events.lock().unwrap().push(value);
            Ok(())
        })?;
    }
    kernel.shutdown().await?;
    kernel.shutdown().await?;
    assert_eq!(*events.lock().unwrap(), vec![3, 2, 1]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_service_and_closed_scope_are_rejected() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    ctx.provide::<u8>(Arc::new(1))?;
    assert!(matches!(
        ctx.provide::<u8>(Arc::new(2)),
        Err(KernelError::DuplicateService { .. })
    ));
    kernel.shutdown().await?;
    assert!(matches!(
        ctx.provide::<u16>(Arc::new(3)),
        Err(KernelError::ScopeClosed)
    ));
    assert!(matches!(ctx.child(), Err(KernelError::ScopeClosed)));
    Ok(())
}

#[allow(dead_code)]
fn _context_is_send_sync(_: Context) {}
