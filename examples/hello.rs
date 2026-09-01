use std::sync::Arc;

use tiny_cordis::{Context, Kernel, Plugin};

struct Greeting;

impl Plugin for Greeting {
    async fn apply(&self, ctx: Context) -> Result<(), tiny_cordis::KernelError> {
        ctx.provide::<String>(Arc::new("hello from tiny-cordis".to_owned()))?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), tiny_cordis::KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();
    let fiber = ctx.mount(Greeting)?;
    fiber.wait_active().await?;
    println!("{}", ctx.require::<String>()?);
    kernel.shutdown().await
}
