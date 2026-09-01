use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Barrier;

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
    assert!(matches!(
        child_ctx.provide::<u64>(Arc::new(3)),
        Err(KernelError::ScopeClosed)
    ));
    assert!(matches!(
        child_ctx.spawn(async {}),
        Err(KernelError::ScopeClosed)
    ));
    assert!(matches!(child_ctx.child(), Err(KernelError::ScopeClosed)));
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

#[tokio::test(flavor = "current_thread")]
async fn recursive_close_releases_nested_resources_and_preserves_root() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let root = kernel.root();
    root.provide::<u32>(Arc::new(7))?;
    let events = Arc::new(Mutex::new(Vec::new()));
    let task_drops = Arc::new(AtomicUsize::new(0));
    let child = root.child()?;
    let grandchild = child.context().child()?;
    for context in [child.context(), grandchild.context()] {
        let task_drops = Arc::clone(&task_drops);
        context.spawn(async move {
            struct Guard(Arc<AtomicUsize>);
            impl Drop for Guard {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }
            let _guard = Guard(task_drops);
            std::future::pending::<()>().await;
        })?;
    }
    tokio::task::yield_now().await;

    for label in ["child", "grandchild"] {
        let events = Arc::clone(&events);
        let label = label.to_owned();
        grandchild.context().defer(move || async move {
            events.lock().unwrap().push(label);
            Ok(())
        })?;
    }
    child.context().defer({
        let events = Arc::clone(&events);
        move || async move {
            events.lock().unwrap().push("child-owner".to_owned());
            Ok(())
        }
    })?;

    child.close().await?;
    assert_eq!(task_drops.load(Ordering::SeqCst), 2);
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            "grandchild".to_owned(),
            "child".to_owned(),
            "child-owner".to_owned()
        ]
    );
    assert_eq!(*root.require::<u32>()?, 7);
    assert!(matches!(child.close().await, Err(KernelError::ScopeClosed)));
    kernel.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_scope_rejects_concurrent_registrations() -> Result<(), KernelError> {
    let kernel = Arc::new(Kernel::new());
    let root = kernel.root();
    let child = root.child()?;
    let context = child.context();
    let barrier = Arc::new(Barrier::new(2));
    let closer = {
        let child = child.clone();
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            child.close().await
        })
    };
    let registrar = {
        let context = context.clone();
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            loop {
                match context.defer(|| async { Ok(()) }) {
                    Ok(()) => tokio::task::yield_now().await,
                    Err(error) => break error,
                }
            }
        })
    };

    closer.await.unwrap()?;
    assert!(matches!(registrar.await.unwrap(), KernelError::ScopeClosed));
    kernel.shutdown().await?;
    Ok(())
}

#[allow(dead_code)]
fn _context_is_send_sync(_: Context) {}
