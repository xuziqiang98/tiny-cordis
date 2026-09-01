use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tiny_cordis::{Context, Dependencies, Kernel, Plugin};

struct Provider;
impl Plugin for Provider {
    async fn apply(&self, ctx: Context) -> Result<(), tiny_cordis::KernelError> {
        ctx.provide::<u64>(Arc::new(42))?;
        Ok(())
    }
}

struct Consumer(Arc<AtomicUsize>);
impl Plugin for Consumer {
    fn requires(&self) -> Dependencies {
        Dependencies::new().require::<u64>()
    }
    async fn apply(&self, ctx: Context) -> Result<(), tiny_cordis::KernelError> {
        let starts = Arc::clone(&self.0);
        starts.fetch_add(1, Ordering::SeqCst);
        assert_eq!(*ctx.require::<u64>()?, 42);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), tiny_cordis::KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let starts = Arc::new(AtomicUsize::new(0));
    let consumer = ctx.mount(Consumer(Arc::clone(&starts)))?;
    let provider = ctx.mount(Provider)?;
    provider.wait_active().await?;
    consumer.wait_active().await?;
    println!("activations: {}", starts.load(Ordering::SeqCst));
    kernel.shutdown().await
}
