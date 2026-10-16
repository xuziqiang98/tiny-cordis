use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Barrier;

use tiny_cordis::{Context, Dependencies, FiberState, Kernel, KernelError, Plugin};

trait Clock: Send + Sync {
    fn now(&self) -> u64;
}

struct TestClock(u64);
impl Clock for TestClock {
    fn now(&self) -> u64 {
        self.0
    }
}

struct ClockPlugin;
impl Plugin for ClockPlugin {
    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        ctx.provide::<dyn Clock>(Arc::new(TestClock(42)))?;
        Ok(())
    }
}

struct OtherClockPlugin;
impl Plugin for OtherClockPlugin {
    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        ctx.provide::<dyn Clock>(Arc::new(TestClock(42)))?;
        Ok(())
    }
}

struct RacingProvider {
    barrier: Arc<Barrier>,
}

impl Plugin for RacingProvider {
    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        self.barrier.wait().await;
        ctx.provide::<u32>(Arc::new(1))?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_activation_commits_one_and_fails_the_other() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let barrier = Arc::new(Barrier::new(2));
    let first = ctx.mount(RacingProvider {
        barrier: Arc::clone(&barrier),
    })?;
    let second = ctx.mount(RacingProvider { barrier })?;

    let first_result = first.wait_active().await;
    let second_result = second.wait_active().await;
    assert_ne!(first.state(), FiberState::Starting);
    assert_ne!(second.state(), FiberState::Starting);
    assert_eq!(
        [first.state(), second.state()]
            .iter()
            .filter(|state| **state == FiberState::Active)
            .count(),
        1
    );
    assert_eq!(
        [first_result.is_ok(), second_result.is_ok()]
            .iter()
            .filter(|active| **active)
            .count(),
        1
    );
    assert_eq!(
        [first.state(), second.state()]
            .iter()
            .filter(|state| **state == FiberState::Failed)
            .count(),
        1
    );

    tokio::time::timeout(std::time::Duration::from_secs(1), kernel.shutdown())
        .await
        .map_err(|_| KernelError::Internal("shutdown timed out".into()))??;
    Ok(())
}

struct Consumer {
    starts: Arc<AtomicUsize>,
}
impl Plugin for Consumer {
    fn requires(&self) -> Dependencies {
        Dependencies::new().require::<dyn Clock>()
    }

    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        let starts = Arc::clone(&self.starts);
        let clock = ctx.require::<dyn Clock>()?;
        assert_eq!(clock.now(), 42);
        starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn missing_dependency_then_provider_and_dispose() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let starts = Arc::new(AtomicUsize::new(0));
    let consumer = ctx.mount(Consumer {
        starts: Arc::clone(&starts),
    })?;
    tokio::task::yield_now().await;
    assert_eq!(consumer.state(), FiberState::Pending);
    let consumer_waiter = {
        let consumer = consumer.clone();
        tokio::spawn(async move { consumer.wait_active().await })
    };
    tokio::task::yield_now().await;

    let provider = ctx.mount(ClockPlugin)?;
    provider.wait_active().await?;
    consumer_waiter
        .await
        .map_err(|error| KernelError::Internal(error.to_string()))??;
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    provider.dispose().await?;
    tokio::task::yield_now().await;
    assert_eq!(consumer.state(), FiberState::Pending);
    assert!(matches!(
        provider.dispose().await,
        Err(KernelError::FiberDisposed)
    ));
    kernel.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn provider_identity_change_reactivates_consumer() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let starts = Arc::new(AtomicUsize::new(0));
    let consumer = ctx.mount(Consumer {
        starts: Arc::clone(&starts),
    })?;
    let first = ctx.mount(ClockPlugin)?;
    first.wait_active().await?;
    consumer.wait_active().await?;
    first.dispose().await?;
    let second = ctx.mount(OtherClockPlugin)?;
    second.wait_active().await?;
    consumer.wait_active().await?;
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    kernel.shutdown().await?;
    Ok(())
}

struct ValueProvider(u32);

impl Plugin for ValueProvider {
    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        ctx.provide::<u32>(Arc::new(self.0))?;
        Ok(())
    }
}

struct ValueConsumer(Arc<Mutex<Vec<u32>>>);

impl Plugin for ValueConsumer {
    fn requires(&self) -> Dependencies {
        Dependencies::new().require::<u32>()
    }

    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        self.0.lock().unwrap().push(*ctx.require::<u32>()?);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn child_provider_disposal_falls_back_to_root_provider() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let root = kernel.root();
    root.mount(ValueProvider(1))?.wait_active().await?;
    let child = root.child()?;
    let child_ctx = child.context();
    let child_provider = child_ctx.mount(ValueProvider(2))?;
    child_provider.wait_active().await?;
    let values = Arc::new(Mutex::new(Vec::new()));
    let consumer = child_ctx.mount(ValueConsumer(Arc::clone(&values)))?;
    consumer.wait_active().await?;
    assert_eq!(*values.lock().unwrap(), vec![2]);

    child_provider.dispose().await?;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if consumer.state() == FiberState::Active
                && values.lock().unwrap().last().copied() == Some(1)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| KernelError::Internal("consumer did not fall back to root".into()))?;
    assert_eq!(*values.lock().unwrap(), vec![2, 1]);
    child.close().await?;
    kernel.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_provider_disposal_reactivates_consumer_without_residue() -> Result<(), KernelError>
{
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let values = Arc::new(Mutex::new(Vec::new()));
    let consumer = ctx.mount(ValueConsumer(Arc::clone(&values)))?;

    for value in 0..100 {
        let provider = ctx.mount(ValueProvider(value))?;
        provider.wait_active().await?;
        consumer.wait_active().await?;
        provider.dispose().await?;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if consumer.state() == FiberState::Pending {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| KernelError::Internal("consumer did not become pending".into()))?;
    }

    assert_eq!(values.lock().unwrap().len(), 100);
    assert!(ctx.get::<u32>().is_none());
    kernel.shutdown().await?;
    Ok(())
}

struct OrderedProvider(Arc<Mutex<Vec<&'static str>>>);
impl Plugin for OrderedProvider {
    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        ctx.provide::<u16>(Arc::new(1))?;
        let events = Arc::clone(&self.0);
        ctx.defer(move || async move {
            events.lock().unwrap().push("provider");
            Ok(())
        })?;
        Ok(())
    }
}

struct OrderedConsumer(Arc<Mutex<Vec<&'static str>>>);
impl Plugin for OrderedConsumer {
    fn requires(&self) -> Dependencies {
        Dependencies::new().require::<u16>()
    }

    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        let events = Arc::clone(&self.0);
        ctx.defer(move || async move {
            events.lock().unwrap().push("consumer");
            Ok(())
        })?;
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dependents_are_cleaned_before_provider() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let events = Arc::new(Mutex::new(Vec::new()));
    let consumer = ctx.mount(OrderedConsumer(Arc::clone(&events)))?;
    let provider = ctx.mount(OrderedProvider(Arc::clone(&events)))?;
    provider.wait_active().await?;
    consumer.wait_active().await?;
    provider.dispose().await?;
    assert_eq!(*events.lock().unwrap(), vec!["consumer", "provider"]);
    kernel.shutdown().await?;
    Ok(())
}
