use std::sync::Arc;

use tiny_cordis::{Context, Kernel, Plugin};

struct Value(u64);
impl Plugin for Value {
    async fn apply(&self, ctx: Context) -> Result<(), tiny_cordis::KernelError> {
        let value = self.0;
        ctx.provide::<u64>(Arc::new(value))?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), tiny_cordis::KernelError> {
    let kernel = Kernel::new();
    let root = kernel.root();
    root.provide::<u64>(Arc::new(1))?;
    let child = root.child()?;
    let child_ctx = child.context();
    let override_fiber = child_ctx.mount(Value(2))?;
    override_fiber.wait_active().await?;
    println!("child: {}", *child_ctx.require::<u64>()?);
    child.close().await?;
    println!("root: {}", *root.require::<u64>()?);
    kernel.shutdown().await
}
