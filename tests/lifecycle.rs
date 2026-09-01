use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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

    let provider = ctx.mount(ClockPlugin)?;
    provider.wait_active().await?;
    consumer.wait_active().await?;
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    provider.dispose().await?;
    tokio::task::yield_now().await;
    assert_eq!(consumer.state(), FiberState::Pending);
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
