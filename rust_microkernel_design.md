# Rust 应用微内核设计文档（Cordis-inspired）

> **状态**：v0.1 设计稿  
> **目标读者**：负责实现该项目的 Coding Agent / Rust 开发者  
> **核心原则**：越小越好；只实现“组合系统”所必需的机制，不引入任何具体业务领域概念。  
> **参考对象**：Cordis 的 Context / Plugin / Fiber / Service / Dependency / Effect 思想。  
> **注意**：本项目不是 Cordis 的 Rust 翻译版，也不是 DSH/Agent Framework。应理解 Cordis 的设计思想后，用 Rust 重新设计一个更小、更 Rust-native 的应用微内核。

---

# 1. 项目定位

本项目实现一个：

> **用于组合长期运行、模块化应用的轻量级 Rust 应用微内核。**

它只解决以下问题：

1. 一个模块如何被挂载和卸载？
2. 模块如何提供能力（Service / Capability）？
3. 模块如何声明自己依赖哪些能力？
4. 依赖尚未出现时，模块如何等待？
5. 依赖出现、消失或被替换时，模块如何自动激活、停用和重新激活？
6. 模块创建的资源、异步任务、子作用域如何随生命周期可靠清理？
7. 不同作用域如何继承或覆盖能力？

除此之外的所有问题，都不属于核心。

项目必须保持**领域无关**。核心代码不得出现以下概念：

- Agent
- LLM
- Tool
- Prompt
- Session
- MCP
- HTTP
- Database
- Config file
- HMR
- Logger
- Event bus
- Process manager
- IPC
- WASM
- Plugin marketplace

这些能力未来都可以作为普通 Service / Plugin 实现。

---

# 2. 设计哲学

## 2.1 微内核而不是框架大全

目标不是提供大量功能，而是提供一个足够稳定的小内核，使其他功能可以在其上组合。

应始终优先：

```text
更少的核心概念
>
更多的内置能力
```

如果一个能力可以作为普通插件实现，就不应进入核心。

---

## 2.2 不复制 Cordis API

实现前需要阅读 Cordis，重点理解：

- Context 是服务解析与作用域入口；
- Plugin 声明依赖并被生命周期管理；
- Fiber 表示一次插件运行实例；
- Service 是插件向其他插件公开的能力；
- `inject` 通过服务依赖决定插件何时可以运行；
- effect 与插件生命周期绑定；
- 依赖消失时消费者会被卸载，并在依赖恢复后重新运行。

但是：

> **禁止按文件、类名、API 逐行移植 Cordis。**

Rust 有 ownership、RAII、`Arc`、`Future`、Tokio、强类型和更明确的资源模型，应重新设计。

---

## 2.3 核心只保留三个主要公开概念

第一版尽量只让用户理解：

```text
Context
Plugin
Scope
```

其他概念尽量作为这三个概念的实现细节。

内部可以存在：

- Fiber
- ServiceId
- Binding
- Activation
- Coordinator

但不要无必要扩大公开 API。

---

# 3. 非目标

v0.1 **明确不做**：

1. 动态 `.so/.dll/.dylib` 插件；
2. WASM 插件；
3. 跨语言插件协议；
4. JSON-RPC / gRPC / IPC；
5. 配置文件加载；
6. YAML / TOML 插件树；
7. HMR；
8. Event Bus；
9. Logger / tracing 集成；
10. Scheduler；
11. Process 管理；
12. Sandbox；
13. 权限系统；
14. 插件版本解析；
15. 插件仓库；
16. 宏系统；
17. derive macro；
18. 分布式运行；
19. 多进程；
20. Agent 相关能力。

如果实现 Agent 认为上述功能“以后可能有用”，也**不得提前加入**。

---

# 4. 核心模型

推荐的心智模型：

```text
                    Kernel
                      │
                 Root Scope
                      │
          ┌───────────┴───────────┐
          │                       │
       Plugin A                 Plugin B
          │                       │
    Activation Scope       Activation Scope
          │                       │
   provide Service X       require Service X
          │                       │
          └──────────┬────────────┘
                     │
              dependency binding
```

核心职责只有两类：

```text
能力关系：
Service provide / resolve / require

生命周期关系：
Scope / Plugin / Activation / cleanup
```

---

# 5. Context

`Context` 是用户最常使用的对象。

它不是“全局 Application 对象”，而是：

> **当前作用域下能力解析与生命周期注册的视图。**

建议：

```rust
#[derive(Clone)]
pub struct Context {
    // 内部字段不暴露
}
```

它至少提供：

```rust
impl Context {
    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: ?Sized + Send + Sync + 'static;

    pub fn require<T>(&self) -> Result<Arc<T>, KernelError>
    where
        T: ?Sized + Send + Sync + 'static;

    pub fn provide<T>(&self, service: Arc<T>) -> Result<(), KernelError>
    where
        T: ?Sized + Send + Sync + 'static;

    pub fn mount<P>(&self, plugin: P) -> FiberHandle
    where
        P: Plugin;

    pub fn child(&self) -> Scope;

    pub fn spawn<F>(&self, future: F) -> TaskHandle
    where
        F: Future<Output = ()> + Send + 'static;

    pub fn defer<F, Fut>(&self, cleanup: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static;
}
```

实际 API 可以根据 Rust 类型系统调整，但语义必须保持简洁。

---

# 6. Service：不是类，不是基类，只是一项 typed capability

不要设计：

```rust
trait Service {
    fn start();
    fn stop();
    ...
}
```

Service 本身没有生命周期接口。

任何满足：

```text
Send + Sync + 'static
```

的对象，都可以作为 Service。

例如：

```rust
trait Clock: Send + Sync {
    fn now(&self) -> u64;
}
```

实现：

```rust
struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> u64 {
        0
    }
}
```

插件提供：

```rust
let clock: Arc<dyn Clock> = Arc::new(SystemClock);
ctx.provide::<dyn Clock>(clock)?;
```

消费者获取：

```rust
let clock = ctx.require::<dyn Clock>()?;
```

因此：

> **Service = 被 Context 注册和解析的一项 typed capability。**

没有额外的 Service 基类。

---

# 7. Service 标识

v0.1 推荐直接使用 Rust `TypeId` 作为 Service identity。

逻辑上：

```text
ServiceId = TypeId
```

例如：

```text
dyn Clock
dyn Storage
dyn Executor
```

分别是不同 Service。

v0.1 不支持：

```text
"database.primary"
"database.replica"
"logger.foo"
```

这样的字符串命名服务。

如果应用需要多个同类型实例，应由上层定义：

```rust
trait DatabaseRegistry {
    fn get(&self, name: &str) -> Arc<dyn Database>;
}
```

而不是把 named-service 复杂度塞进内核。

## 同一作用域规则

同一作用域内：

```text
同一个 ServiceId 最多只能有一个 provider。
```

如果重复提供：

```text
DuplicateService
```

直接报错。

子作用域可以覆盖父作用域中的同类型 Service。

---

# 8. Scope

`Scope` 是资源和生命周期的所有权边界。

逻辑结构：

```text
Root
├── Scope A
│   ├── Scope A1
│   └── Scope A2
└── Scope B
```

子作用域：

- 可以读取父作用域 Service；
- 可以提供自己的 Service；
- 相同 Service 类型时，最近的作用域优先；
- 关闭时，释放自己拥有的一切；
- 不影响父作用域。

建议公开：

```rust
pub struct Scope {
    // ...
}

impl Scope {
    pub fn context(&self) -> Context;
    pub async fn close(&self) -> Result<(), KernelError>;
}
```

创建：

```rust
let child = ctx.child();
let child_ctx = child.context();
```

---

# 9. Scope 关闭语义

关闭 Scope 必须：

1. 将 Scope 标记为 Closing；
2. 禁止继续注册新资源；
3. 停止其拥有的插件 Fiber；
4. 关闭子 Scope；
5. 取消其托管的 async task；
6. 执行 cleanup/defer；
7. 移除其拥有的 Service binding；
8. 标记 Closed。

具体内部顺序可以为保证依赖安全做调整，但必须满足：

> **任何依赖某个 Service 的消费者，都必须在该 Service 真正失效前停止使用它。**

清理必须：

- 可重复调用；
- 第二次 `close()` 不产生副作用；
- 不 panic；
- 尽量完成全部 cleanup，即使其中一个 cleanup 返回错误。

---

# 10. Plugin

Plugin 是：

> **可以被挂载到 Context 上，并根据 Service 依赖自动激活/停用的一段模块逻辑。**

推荐公共接口尽量简单：

```rust
pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    fn requires(&self) -> Dependencies {
        Dependencies::new()
    }

    async fn apply(&self, ctx: Context) -> Result<(), KernelError>;
}
```

如果当前稳定 Rust / object safety 使内部实现不方便：

- `Context::mount<P: Plugin>` 保持泛型；
- mount 时将 Plugin erase 成内部 closure；
- 不要求公共 `dyn Plugin`。

不要为了 object-safe 引入复杂 API。

---

# 11. Plugin 的 apply 语义

`apply()` 表示：

> **完成本次 activation 的初始化。**

它不是永久运行的主循环。

正确：

```rust
async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
    let worker = ...;

    ctx.spawn(async move {
        worker.run().await;
    });

    Ok(())
}
```

错误：

```rust
async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
    loop {
        ...
    }
}
```

如果 `apply()` 永远不返回：

- 插件永远处于 Starting；
- 生命周期无法正常完成；
- 依赖变化时难以可靠停止。

因此长期工作必须通过：

```text
ctx.spawn(...)
```

注册为托管任务。

---

# 12. Dependencies

Plugin 通过 typed service 声明依赖。

例如：

```rust
fn requires(&self) -> Dependencies {
    Dependencies::new()
        .require::<dyn Storage>()
        .require::<dyn Clock>()
}
```

Dependencies 本质只需要保存：

```text
Vec<ServiceId>
```

不要在 v0.1 加：

- optional dependency；
- dependency version；
- dependency expression；
- OR dependency；
- provider priority；
- lazy dependency；
- dependency tags。

只有：

```text
required service exists / does not exist
```

---

# 13. Fiber

Fiber 可以作为公开的轻量 Handle，但复杂内部状态不要暴露。

它代表：

> **一个被 mount 的 Plugin 的持续生命周期控制器。**

需要区分：

```text
Fiber
```

和：

```text
Activation
```

Fiber 可以长期存在。

当依赖变化时：

```text
同一个 Fiber
    │
    ├── Activation #1
    │       stop
    │
    ├── Pending
    │
    └── Activation #2
```

因此不要把一次 activation 等同于整个 Fiber。

---

# 14. Fiber 状态机

建议：

```text
                dependencies ready
        ┌────────────────────────────┐
        │                            ▼
     PENDING ───────────────────> STARTING
        ▲                            │
        │                            │ success
        │                            ▼
        │                         ACTIVE
        │                            │
        │         dependency lost   │
        └────────── STOPPING <───────┘
                       │
                       └──────────> PENDING
```

错误：

```text
STARTING ── apply error ──> FAILED
```

永久销毁：

```text
PENDING / STARTING / ACTIVE / FAILED
                  │
               dispose
                  ▼
              DISPOSED
```

完整枚举可为：

```rust
pub enum FiberState {
    Pending,
    Starting,
    Active,
    Stopping,
    Failed,
    Disposed,
}
```

---

# 15. FAILED 的策略

v0.1：

```text
apply() 返回 Error
    ↓
回滚本次 activation
    ↓
Fiber = FAILED
```

不要自动无限重试。

原因：

- 防止错误插件高速重启；
- 行为更确定；
- 更容易调试。

可以提供：

```rust
fiber.retry().await?;
```

或者先不提供 retry，让用户 dispose + mount。

优先选择更简单的实现。

---

# 16. Activation Scope

这是整个设计最重要的内部机制之一。

每次 Plugin 激活时，Kernel 创建一个独立的：

```text
Activation Scope
```

例如：

```text
Application Scope
      │
      ├── Plugin A Fiber
      │       │
      │       └── Activation Scope #1
      │              ├── service bindings
      │              ├── tasks
      │              ├── defers
      │              └── child scopes
      │
      └── Plugin B Fiber
```

如果依赖消失：

```text
close Activation Scope #1
```

即可统一完成：

- task cancellation；
- cleanup；
- service unregister；
- child scope close。

依赖恢复后：

```text
create Activation Scope #2
```

重新执行 `apply()`。

这样不需要插件实现：

```rust
start()
stop()
restart()
```

生命周期统一由 Scope 表达。

---

# 17. Service 的“可见作用域”和“所有权作用域”

这里必须避免一个常见设计错误。

Plugin A 与 Plugin B 都 mount 在同一个 Context：

```text
Context X
├── Plugin A
└── Plugin B
```

A 提供的 Service 必须能够被 B 看见。

但 A 的 Service 又必须随着 A 的 Activation 被自动清理。

因此内部需要区分：

```text
visibility scope
ownership scope
```

对于 Plugin activation：

```text
Service visibility = Plugin 所 mount 的 Context Scope
Service ownership  = 当前 Activation Scope
```

即：

```text
                 Context Scope X
                  /           \
           Plugin A          Plugin B
              │                 │
       Activation A        Activation B
              │
        owns Service S
              │
              └──── published into Scope X
                         │
                         └──── visible to Plugin B
```

当 Activation A 关闭：

```text
Service S 自动从 Scope X 移除。
```

这个区别应作为内部实现规则，不必暴露给普通用户。

---

# 18. Service 解析规则

查找某个 Service `T` 时：

```text
当前 Scope
   ↓
父 Scope
   ↓
父父 Scope
   ↓
Root
```

返回最近的有效 binding。

例如：

```text
Root:
    Storage = A

Child:
    Storage = B
```

则：

```text
Root context  -> A
Child context -> B
```

删除 Child 的 B 后：

```text
Child context -> A
```

---

# 19. Dependency 不只关心“类型存在”，还关心 provider identity

假设：

```text
Plugin X requires Storage
```

最开始解析为：

```text
Storage provider = A
```

后来子作用域出现覆盖：

```text
Storage provider = B
```

虽然：

```text
Storage 仍然存在
```

但 X 实际依赖对象已经变化。

因此 Fiber 应记录一次 activation 所绑定的：

```text
ServiceId -> BindingId
```

当解析结果从：

```text
Storage -> Binding A
```

变为：

```text
Storage -> Binding B
```

必须视为 dependency changed：

```text
ACTIVE
 ↓
STOPPING
 ↓
PENDING
 ↓
STARTING
 ↓
ACTIVE
```

这可以避免插件继续持有旧 Service。

---

# 20. Service 发布必须是事务性的

`apply()` 过程中可能：

```rust
ctx.provide(service_a)?;
ctx.spawn(task);
ctx.defer(...);

some_init().await?;
```

如果最后：

```text
some_init() -> Error
```

那么绝不能留下：

- 半注册 Service；
- 遗留 task；
- 遗留 cleanup；
- 半激活状态。

因此本次 activation 的所有资源必须先属于一个临时 Activation Scope。

只有：

```text
apply() success
```

之后，Service binding 才对其他 Fiber 正式生效。

逻辑：

```text
STARTING
   │
   ├── stage Service A
   ├── stage task
   ├── stage cleanup
   │
   └── apply success
            │
            ▼
          COMMIT
            │
            ▼
          ACTIVE
```

失败：

```text
STARTING
   │
   └── apply error
          │
       rollback activation scope
          │
          ▼
        FAILED
```

这是 v0.1 必须保证的核心 invariant。

---

# 21. Resource / Effect

不要设计复杂的 Effect 类型系统。

只需要解决：

> 插件申请的资源必须在 activation/scope 结束时释放。

核心提供两个入口即可：

```text
ctx.spawn(...)
ctx.defer(...)
```

---

# 22. ctx.spawn

语义：

```rust
ctx.spawn(async move {
    ...
});
```

Kernel：

- 启动 Tokio task；
- 记录 task 与当前 owner scope 的关系；
- owner scope 关闭时取消 task；
- 等待 task 完成 cancellation；
- task 自然结束时从资源表移除。

第一版不需要：

- task priority；
- task restart；
- cron；
- scheduler；
- worker pool；
- timeout policy。

这些以后都可以作为 Service 实现。

---

# 23. ctx.defer

用于管理无法完全通过 Rust Drop 表达的异步清理。

例如：

```rust
ctx.defer(|| async move {
    connection.close().await;
});
```

清理建议：

```text
LIFO
```

即后注册的 cleanup 先执行。

这符合 stack/resource acquisition 的直觉。

注意：

> Rust RAII 很有价值，但 async cleanup 无法完全依赖 `Drop`。

因此不要尝试在 `Drop` 里隐式执行复杂 async shutdown。

Kernel/Scope 应提供显式：

```rust
close().await
shutdown().await
```

---

# 24. 子 Scope 本身也是 Resource

如果插件创建：

```rust
let child = ctx.child();
```

这个 child scope 应归当前 owner scope 所有。

父 scope 关闭：

```text
child scope 自动关闭
```

从而形成结构化生命周期树：

```text
Scope
├── Plugin Fiber
├── Task
├── Cleanup
└── Child Scope
    ├── Plugin Fiber
    ├── Task
    └── ...
```

这实际上是：

> **structured concurrency + scoped service composition**

---

# 25. Kernel

`Kernel` 应非常薄。

建议：

```rust
pub struct Kernel {
    // inner
}

impl Kernel {
    pub fn new() -> Self;

    pub fn root(&self) -> Context;

    pub async fn shutdown(&self) -> Result<(), KernelError>;
}
```

Kernel 负责：

- root scope；
- 内部生命周期协调；
- Service binding 元数据；
- Fiber 状态；
- shutdown。

不要让 Kernel 变成“万能管理器”。

---

# 26. 生命周期并发策略

v0.1 优先：

> **生命周期操作串行化。**

即：

- mount；
- activation；
- commit；
- dependency change；
- stopping；
- disposal；

通过一个确定性的内部 coordinator 顺序执行。

理由：

1. 生命周期操作不是性能瓶颈；
2. 串行状态机更容易保证 invariant；
3. 可以显著减少锁、竞态和死锁；
4. Plugin 自己的后台工作仍然通过 Tokio 并发运行。

不要为了理论吞吐量在 v0.1 并发执行多个 plugin activation。

以后只有 benchmark 证明有必要时再做。

---

# 27. 推荐内部 Coordinator 模型

可考虑：

```text
Context/FiberHandle API
        │
        ▼
   Kernel Coordinator
        │
        ├── scopes
        ├── fibers
        ├── bindings
        └── lifecycle transitions
```

所有 lifecycle mutation 通过 coordinator 串行处理。

但是注意一个危险：

> 不要让 coordinator 在持有全局状态锁时 `await plugin.apply()`。

否则 plugin 内调用：

```text
ctx.provide()
ctx.mount()
ctx.child()
```

可能造成 re-entrancy / deadlock。

推荐：

1. coordinator 创建 Activation；
2. release 内部锁；
3. 执行 `plugin.apply(ctx).await`；
4. 收到结果；
5. coordinator commit / rollback。

具体实现可以调整，但不得在全局锁中执行用户代码。

---

# 28. Cleanup 顺序与依赖安全

如果：

```text
Plugin B requires Service S
Plugin A provides Service S
```

关闭 A 时不能：

```text
先销毁 S
再通知 B
```

否则 B cleanup 可能还要用 S。

必须保证：

```text
停止依赖者 B
    ↓
B cleanup 完成
    ↓
移除 S
    ↓
停止 provider A
```

更一般地：

> provider 失效时，依赖者必须先完成 deactivation。

v0.1 不一定需要构造一个复杂的静态 DAG。

可以通过 Fiber activation 保存的：

```text
BindingId dependencies
```

反向找到 dependents。

---

# 29. 不要求 Plugin 声明 provides

为了保持简单：

```text
Plugin 只声明 requires
```

Service 在 `apply()` 中通过：

```text
ctx.provide()
```

动态注册。

因此 v0.1 不需要：

```rust
fn provides() -> ...
```

代价是：

- 无法在运行前完整构建依赖 DAG；
- 无法完美静态检测所有 dependency cycle。

这个代价是可以接受的。

对于长期 Pending 的 Fiber，只需要能报告：

```text
Missing services:
- Storage
- Clock
```

不要为 cycle detection 引入大量 metadata。

---

# 30. Event Bus 不属于核心

Cordis 自带 events，但本项目故意不复制。

如果用户需要 Event Bus：

```rust
trait EventBus: Send + Sync {
    ...
}
```

然后：

```text
EventBusPlugin
    ↓
ctx.provide::<dyn EventBus>(...)
```

即可。

原因：

- Event dispatch 不是 plugin lifecycle 的必要条件；
- 不同应用需要完全不同的 event semantics；
- sync/async/broadcast/waterfall/bail 等会迅速膨胀核心 API。

因此 v0.1 **禁止实现 `ctx.emit()` / `ctx.on()`**。

---

# 31. Logger 不属于核心

核心不提供：

```text
ctx.logger
```

也不依赖完整 logging framework。

如果需要：

```text
LoggerPlugin
TracingPlugin
```

作为上层能力。

核心自身只应：

- 返回结构化错误；
- 暴露 Fiber state；
- 在 debug build 中使用最小必要 diagnostic。

不要因为“调试方便”把 logging 变成核心 Service。

---

# 32. Config / Loader / HMR 不属于核心

Cordis 的 config loader、group、include、HMR 很有价值，但这些属于：

```text
distribution / tooling layer
```

不属于 microkernel。

未来可以：

```text
kernel-loader
kernel-config
kernel-hmr
```

但不得进入：

```text
kernel-core
```

v0.1 只支持 Rust 代码静态组合：

```rust
let kernel = Kernel::new();
let ctx = kernel.root();

ctx.mount(StoragePlugin::new());
ctx.mount(AppPlugin::new());

kernel.shutdown().await?;
```

---

# 33. 动态插件不属于核心

v0.1 不支持：

```text
libloading
Rust ABI plugin
.so
.dll
WASM
subprocess plugin
```

未来如果需要，可实现：

```text
DynamicPluginLoader
WasmPluginHost
ProcessPluginHost
```

它们仍然只是普通 Plugin。

尤其不要为了未来动态插件需求，提前污染 Plugin trait。

---

# 34. Tokio 策略

v0.1 可以明确依赖 Tokio。

原因：

- 项目本身就是 async runtime 上的应用微内核；
- `ctx.spawn()` 需要 executor；
- 抽象 executor 会明显增加复杂度。

原则：

> **先选择一个成熟 runtime，而不是为了理论通用性制造 Executor abstraction。**

未来真实需求出现后再考虑 executor abstraction。

Kernel 不负责创建 Tokio runtime。

调用方负责：

```rust
#[tokio::main]
async fn main() {
    ...
}
```

---

# 35. 依赖控制

目标：

```text
runtime dependencies 尽量 <= 2~3 个
```

优先：

```text
tokio
```

`thiserror` 仅在确实减少大量 boilerplate 时考虑。

v0.1 尽量不要依赖：

- async-trait；
- serde；
- serde_json；
- tracing；
- futures 大合集；
- dashmap；
- inventory；
- libloading；
- wasmtime；
- anyhow（公共库错误类型不建议直接 anyhow）。

如果标准库可以简单实现，就用标准库。

---

# 36. unsafe 策略

core crate：

```rust
#![forbid(unsafe_code)]
```

除非未来出现无法绕开的明确需求，否则不使用 unsafe。

当前设计没有任何必须使用 unsafe 的地方。

---

# 37. 错误模型

错误类型保持小。

例如：

```rust
pub enum KernelError {
    ServiceNotFound,
    DuplicateService,
    ScopeClosed,
    FiberDisposed,
    PluginFailed,
    Shutdown,
    Internal,
}
```

实际可以带：

- ServiceId；
- plugin name；
- source error。

不要第一版设计几十个 error variant。

Plugin 自己的业务错误可以：

- Box Error；
- Arc Error；
- 自定义 boxed source；

但核心只负责表示：

```text
Plugin apply failed
```

---

# 38. 公共 API 草案

目标体验：

```rust
use std::sync::Arc;

trait Clock: Send + Sync {
    fn now(&self) -> u64;
}

struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> u64 {
        42
    }
}

struct ClockPlugin;

impl Plugin for ClockPlugin {
    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        ctx.provide::<dyn Clock>(clock)?;
        Ok(())
    }
}

struct Consumer;

impl Plugin for Consumer {
    fn requires(&self) -> Dependencies {
        Dependencies::new()
            .require::<dyn Clock>()
    }

    async fn apply(&self, ctx: Context) -> Result<(), KernelError> {
        let clock = ctx.require::<dyn Clock>()?;

        ctx.spawn(async move {
            println!("{}", clock.now());
        });

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), KernelError> {
    let kernel = Kernel::new();
    let ctx = kernel.root();

    let clock = ctx.mount(ClockPlugin);
    let consumer = ctx.mount(Consumer);

    consumer.wait_active().await?;

    clock.dispose().await?;

    // Consumer automatically leaves ACTIVE because Clock disappeared.

    kernel.shutdown().await?;
    Ok(())
}
```

最终 API 不必逐字一致，但必须维持这种复杂度级别。

---

# 39. Child Scope 覆盖示例

```rust
let kernel = Kernel::new();
let root = kernel.root();

root.mount(DefaultStoragePlugin);

let child = root.child();
let child_ctx = child.context();

child_ctx.mount(TestStoragePlugin);
child_ctx.mount(MyAppPlugin);
```

语义：

```text
Root:
    Storage = Default

Child:
    Storage = Test
```

`MyAppPlugin` 在 Child 中解析：

```text
Storage -> Test
```

Child 关闭后：

```text
Test 自动清理
```

Root 不受影响。

---

# 40. 推荐源码结构

v0.1 **单 crate**。

不要一开始创建 workspace + 十几个 crate。

建议：

```text
src/
├── lib.rs
├── kernel.rs
├── context.rs
├── scope.rs
├── plugin.rs
├── fiber.rs
├── service.rs
├── resource.rs
├── error.rs
└── internal/
    └── coordinator.rs

tests/
├── lifecycle.rs
├── dependency.rs
├── scope.rs
└── rollback.rs

examples/
├── hello.rs
├── dependency.rs
└── override.rs
```

如果实现后发现部分文件只有几十行，可以进一步合并。

---

# 41. 代码规模目标

这是一个“越小越漂亮”的项目。

软目标：

```text
核心 src/：
约 800 ~ 1500 行逻辑代码
```

不包含：

- tests；
- examples；
- comments；
- docs。

如果实现超过约 2000~2500 行，应暂停并检查是否：

- 提前实现了非目标；
- abstraction 过多；
- lifecycle 状态机设计过重；
- public API 太大。

LOC 不是硬指标，但应作为设计复杂度警报。

---

# 42. 必须维持的核心 Invariants

实现过程中优先保证这些 invariant，而不是添加功能。

## I1. 不可见的依赖不能启动消费者

如果 Plugin requires：

```text
A, B
```

则只有 A、B 都可解析时才能进入 Starting。

---

## I2. apply 成功前 Service 不对其他 Fiber 可见

防止观察到半初始化 provider。

---

## I3. apply 失败必须完全 rollback

失败后：

```text
0 leaked service
0 leaked task
0 leaked child scope
0 leaked cleanup
```

---

## I4. Provider 失效前先停 Dependents

消费者 cleanup 期间依赖必须仍然可用。

---

## I5. Provider identity 变化必须触发重新 activation

不仅检查 Service type 是否存在。

---

## I6. Scope close 必须递归释放所有 owner resources

不得留下 orphan task / binding / child scope。

---

## I7. cleanup 至少执行一次，最多执行一次

所有 disposer 必须 exactly-once。

---

## I8. shutdown 幂等

```rust
kernel.shutdown().await?;
kernel.shutdown().await?;
```

第二次不得崩溃。

---

## I9. 用户代码不得在 Kernel 全局状态锁中执行

包括：

- Plugin apply；
- defer cleanup；
- 用户 Future poll 中的逻辑。

---

## I10. 核心不依赖任何业务领域

任何测试 core 的例子都应使用：

```text
Clock
Greeting
Storage
Counter
```

而不是 Agent/LLM。

---

# 43. 必做测试

## T1. 无依赖插件

```text
mount
→ Starting
→ Active
```

---

## T2. 缺失依赖

Consumer requires Service X：

```text
mount Consumer
→ Pending
```

不能调用 apply。

---

## T3. Provider 出现

```text
Consumer Pending
Provider Active
Service X committed
Consumer Starting
Consumer Active
```

---

## T4. Provider 消失

```text
Provider dispose
→ Consumer Stopping
→ Consumer cleanup
→ Service X removed
→ Consumer Pending
```

检查清理顺序。

---

## T5. Provider 再出现

```text
Consumer Pending
→ new Provider
→ Consumer new Activation
→ Active
```

确保 apply 被再次调用。

---

## T6. apply rollback

Plugin：

```text
provide X
spawn task
defer cleanup
return Err
```

结果：

```text
Fiber Failed
X 不存在
task 已取消
cleanup 已执行
```

---

## T7. 重复 Service

同一 scope 两个 provider 同时提供同一个 Service：

```text
DuplicateService
```

不能静默覆盖。

---

## T8. Child override

Root 提供 A。

Child 提供 B。

Child consumer 必须得到 B。

关闭 B 后 consumer 必须重新 activation 并得到 A。

---

## T9. LIFO cleanup

注册：

```text
cleanup 1
cleanup 2
cleanup 3
```

关闭顺序：

```text
3
2
1
```

---

## T10. Managed task cancellation

spawn 一个不会自行结束的 Future。

关闭 scope。

确认 task 被 abort/cancel。

---

## T11. Recursive scope close

```text
Root
└── Child
    └── GrandChild
```

关闭 Child：

```text
GrandChild 资源全部清理
Child 资源全部清理
Root 保持有效
```

---

## T12. shutdown 幂等

重复 shutdown 不 panic、不 double-cleanup。

---

## T13. 高频重激活无泄漏

循环：

```text
mount provider
dispose provider
```

100 次。

Consumer 应反复：

```text
Pending <-> Active
```

最终资源计数回到基线。

---

# 44. Benchmark 不是 v0.1 的首要目标

不要先写 benchmark 再为了 benchmark 复杂化设计。

v0.1 性能只需做到：

- Service lookup 足够轻量；
- 生命周期不会明显阻塞 runtime；
- 无明显 O(N²) 热路径；
- 不产生无界任务/资源泄漏。

Agent/LLM 等系统的端到端性能通常不会被几十微秒级 lifecycle overhead 决定。

Rust 的价值首先是：

- ownership；
- 明确生命周期；
- Send/Sync；
- 资源安全；
- async task 管理；
- 长期运行稳定性。

而不是追求“比 TypeScript 快多少倍”。

---

# 45. 与 Cordis 的关系：应该学什么

实现 Agent 应重点阅读 Cordis 以下部分。

## 45.1 Context

关注：

- Context 如何作为服务容器；
- child/scoped context；
- service lookup；
- scope inheritance。

不要复制：

- JS Proxy；
- declaration merging；
- intercept；
- isolate map 的具体 TS 实现。

Rust 不需要 JS Proxy。

---

## 45.2 Registry / Plugin

关注：

- plugin mount；
- dependency-driven activation；
- 一个 plugin callback 对应多个 runtime fiber 的思想。

不要复制：

- TypeScript 多种 plugin shape；
- decorator；
- Config schema；
- object/function/class plugin 兼容层。

Rust v0.1 只需要一种 Plugin trait。

---

## 45.3 Fiber

重点研究：

- Fiber 生命周期；
- effect ownership；
- dependency snapshot；
- unload/reload。

这是最值得借鉴的部分。

---

## 45.4 Reflect / Service

关注：

- service provider 与 consumer 解耦；
- provider 生命周期影响 service availability；
- service 由 owning fiber 自动移除。

不要复制：

- dynamic property read；
- Proxy reflect；
- 字符串 service key。

Rust 使用 typed service。

---

## 45.5 Effects

关注：

> registration 必须与 owner 生命周期绑定。

这是必须保留的思想。

Rust 实现应使用：

```text
Scope ownership
+ managed task
+ async defer
```

而不是机械复制 callback disposer API。

---

# 46. 与 Cordis 相比，本项目有意删除的能力

Cordis Core / 官方扩展中存在，但 v0.1 不复制：

| Cordis 能力 | 本项目 v0.1 |
|---|---|
| Context | 保留核心思想 |
| Plugin | 保留，简化为一个 trait |
| Fiber | 保留内部核心思想 |
| Service | 保留，但改为 typed capability |
| inject | 保留为 typed Dependencies |
| effect | 保留语义，改为 Scope ownership |
| Events | 删除，未来插件 |
| Logger | 删除，未来插件 |
| Config validation | 删除 |
| Loader | 删除 |
| YAML tree | 删除 |
| HMR | 删除 |
| intercept | 删除 |
| JS Proxy reflect | 删除 |
| function/class/object 多 plugin 形态 | 删除 |
| timer helpers | 删除，未来插件 |
| dynamic module loading | 删除 |

---

# 47. Rust-native 的关键改进

本项目不是“Cordis in Rust”，而应利用 Rust 做出以下改进。

## 47.1 typed Service

Cordis：

```text
ctx["storage"]
```

Rust：

```text
ctx.require::<dyn Storage>()
```

减少字符串 key 和运行时类型错误。

---

## 47.2 Scope ownership

资源天然属于 scope。

```text
owner scope close
→ all owned resources close
```

而不是每个插件自己维护 start/stop。

---

## 47.3 Arc + Send + Sync

Service 天然可跨 Tokio task 安全共享。

---

## 47.4 Structured async lifetime

所有 `ctx.spawn()` task 都必须有 owner。

禁止 detached orphan task 成为默认行为。

---

## 47.5 Explicit async shutdown

不要假装 `Drop` 可以解决所有异步资源关闭问题。

---

# 48. 一个重要原则：不要制造“万能 Resource trait”

不建议：

```rust
trait Resource {
    async fn start();
    async fn stop();
    async fn reload();
    async fn health();
    ...
}
```

这会重新制造一个复杂框架。

正确方向：

```text
任意对象
+
一个 cleanup future
```

即可。

即：

```rust
ctx.defer(|| async move {
    resource.close().await;
});
```

---

# 49. 一个重要原则：不要制造“万能 Component”

不要统一：

```text
Plugin
Service
Task
Scope
Process
EventListener
```

为一个巨大：

```rust
trait Component
```

这些对象的语义不同。

微内核的“统一”来自：

```text
共同的 owner scope
```

而不是共同的 inheritance hierarchy。

---

# 50. 一个重要原则：不要提前抽象 Executor

即便“未来可能支持 async-std/smol”，v0.1 也不要做：

```rust
trait Executor {
    ...
}
```

这会让：

- task cancellation；
- JoinHandle；
- runtime context；
- spawn semantics；

全部复杂化。

直接 Tokio。

---

# 51. 一个重要原则：不要提前设计跨语言

核心不应出现：

```text
protocol
message
serialization
RPC
schema
transport
```

未来需要 Python / TypeScript / WASM 时，应在内核上实现：

```text
ProcessHostPlugin
WasmHostPlugin
RpcBridgePlugin
```

而不是修改核心模型。

---

# 52. 建议实现顺序

Coding Agent 必须按顺序实现，不要并行铺开所有功能。

## Phase 0：阅读与最小设计确认

阅读 Cordis：

```text
vendor/cordis/src/context.ts
vendor/cordis/src/registry.ts
vendor/cordis/src/fiber.ts
vendor/cordis/src/reflect.ts
vendor/cordis/src/service.ts
docs/cordis-primer.md
docs/user/develop/framework/
```

输出一份非常短的内部笔记：

```text
Cordis 哪些思想保留
哪些明确删除
Rust 版生命周期 invariant
```

然后开始编码。

不要先写大篇架构文档。

---

## Phase 1：Scope + Context

实现：

- Kernel；
- Root Scope；
- Child Scope；
- Scope close；
- Context clone；
- owner resource registration。

先不实现 Plugin dependency。

测试：

- child close；
- recursive close；
- cleanup LIFO；
- shutdown idempotent。

---

## Phase 2：Typed Service

实现：

- ServiceId(TypeId)；
- provide；
- get；
- require；
- parent lookup；
- child override；
- duplicate same-scope rejection。

测试全部 Service semantics。

---

## Phase 3：Plugin + Fiber 基础

实现：

- Plugin；
- mount；
- FiberHandle；
- Pending / Starting / Active / Failed / Disposed；
- Activation Scope；
- apply success；
- rollback。

先不做 dependency disappearance。

---

## Phase 4：Dependencies

实现：

- typed Dependencies；
- missing -> Pending；
- provider commit -> wake consumers；
- activation 保存 BindingId snapshot。

---

## Phase 5：Reactive lifecycle

实现：

- provider dispose；
- reverse dependent lookup；
- consumers stop first；
- consumers -> Pending；
- provider reappears -> reactivation；
- provider identity change -> restart。

这是 v0.1 最关键的一步。

---

## Phase 6：Managed async resources

实现：

- `ctx.spawn()`；
- task cancellation；
- `ctx.defer()`；
- cleanup ordering。

如果前面 Scope 已有统一 resource abstraction，这一步应很小。

---

## Phase 7：Polish

只做：

- public API 简化；
- docs；
- examples；
- error messages；
- race tests；
- clippy；
- fmt。

不要增加新 feature。

---

# 53. v0.1 完成标准

只有满足以下条件，才算完成：

- [ ] 单 crate；
- [ ] 核心 API 可在 10 分钟内理解；
- [ ] Context / Plugin / Scope 是主要公开概念；
- [ ] typed Service 可工作；
- [ ] parent/child scope inheritance 可工作；
- [ ] child override 可工作；
- [ ] dependency missing -> Pending；
- [ ] dependency available -> Active；
- [ ] dependency disappears -> consumer 自动停止；
- [ ] dependency returns -> consumer 自动重新激活；
- [ ] provider identity change -> consumer 重启；
- [ ] apply failure 完全 rollback；
- [ ] scope close 无资源泄漏；
- [ ] `ctx.spawn()` task 被 owner 管理；
- [ ] `ctx.defer()` exactly-once；
- [ ] shutdown 幂等；
- [ ] 无 Agent/LLM 领域代码；
- [ ] 无 dynamic plugin；
- [ ] 无 event bus；
- [ ] 无 config loader；
- [ ] 无 HMR；
- [ ] 无 unsafe；
- [ ] tests 覆盖关键 lifecycle；
- [ ] examples 足够说明使用方式。

---

# 54. Agent 实现约束

给 Coding Agent 的硬约束：

## 必须

1. 先阅读 Cordis 源码再实现；
2. 理解思想，不照搬 TypeScript 结构；
3. 优先减少 abstraction；
4. public API 变化必须解释为什么；
5. 每完成一个 Phase 就跑 tests；
6. 生命周期相关 bug 优先于 feature；
7. 所有 user callback 都不能在全局状态锁中执行；
8. 所有长期 async task 必须有明确 owner；
9. 关键 cleanup 行为必须测试顺序；
10. 最终执行 `cargo fmt`、`cargo clippy`、`cargo test`。

## 禁止

1. 未经需要创建 workspace；
2. 创建 proc-macro crate；
3. 使用 unsafe；
4. 引入动态插件；
5. 引入 serde/config；
6. 引入 event bus；
7. 引入 tracing/logging 作为核心能力；
8. 引入 Agent 概念；
9. 引入跨语言协议；
10. 因“以后可能需要”提前抽象。

---

# 55. 设计取舍记录

## 为什么 Event 不进核心？

因为 lifecycle/composition 不依赖 event bus。

---

## 为什么 Service 使用 TypeId？

因为 Rust 本身有强类型，可以去掉 Cordis 字符串 key 的动态性。

---

## 为什么同 scope 同类型只允许一个 Service？

因为这是最简单、确定的解析规则。

多实例需求应由上层 Registry Service 表达。

---

## 为什么 Plugin 只声明 requires，不声明 provides？

减少 metadata 重复。

实际 provider 由 `ctx.provide()` 决定。

---

## 为什么 lifecycle v0.1 串行？

正确性与确定性比 plugin activation throughput 更重要。

---

## 为什么 Tokio 直接进入 core？

因为 executor abstraction 的成本大于 v0.1 收益。

---

## 为什么没有 `stop()`？

因为 activation-owned Scope 就是 stop 机制。

Plugin 只负责：

```text
apply
```

清理由：

```text
Scope
```

统一完成。

---

## 为什么没有 `reload()`？

依赖变化时：

```text
close old activation
+
create new activation
```

就是 reload。

不需要单独抽象。

---

# 56. 最终架构

最终核心应该看起来接近：

```text
┌────────────────────────────────────────────┐
│                   Kernel                   │
│                                            │
│   ┌──────────┐       ┌─────────────────┐   │
│   │  Scope   │──────▶│ Service Bindings│   │
│   └────┬─────┘       └─────────────────┘   │
│        │                                   │
│        │ owns                              │
│        ▼                                   │
│   ┌──────────┐    requires   ┌──────────┐  │
│   │  Fiber   │──────────────▶│ Service  │  │
│   └────┬─────┘               └──────────┘  │
│        │                                   │
│        │ activation                        │
│        ▼                                   │
│   ┌──────────┐                             │
│   │  Scope   │                             │
│   ├──────────┤                             │
│   │ Tasks    │                             │
│   │ Cleanup  │                             │
│   │ Services │                             │
│   └──────────┘                             │
└────────────────────────────────────────────┘
```

用户看到的核心则尽量只有：

```text
Kernel
Context
Scope
Plugin
FiberHandle
```

其中真正需要理解的设计概念只有：

```text
Context
Plugin
Scope
```

---

# 57. 判断一个新功能是否应该进入核心

以后任何 feature 都必须先回答：

> **如果删除这个功能，Context + Plugin + Scope 的基本组合和生命周期是否仍然成立？**

如果答案是：

```text
成立
```

那它大概率不属于核心。

例如：

```text
Event Bus      -> 不属于
Logger         -> 不属于
HTTP           -> 不属于
Scheduler      -> 不属于
Process        -> 不属于
MCP            -> 不属于
WASM           -> 不属于
Config         -> 不属于
HMR            -> 不属于
Agent          -> 不属于
```

核心必须持续抵抗 feature creep。

---

# 58. 一句话定义

如果实现过程中迷失方向，回到这一句话：

> **这是一个用 Rust 实现的、基于 typed capability、scoped ownership 和 dependency-driven lifecycle 的极小应用微内核。**

不是：

> Rust 版 Cordis。

不是：

> Agent Kernel。

不是：

> Plugin Marketplace。

不是：

> Application Framework。

它只是一颗足够小、足够可靠的：

> **composition kernel**。

---

# 59. 参考 Cordis 文件

实现前建议重点阅读 DeepSeek Harness 仓库中的：

```text
vendor/cordis/README.md

vendor/cordis/src/
├── context.ts
├── registry.ts
├── fiber.ts
├── reflect.ts
├── service.ts
└── events.ts

docs/
├── cordis-primer.md
├── cordis-tutorial/
└── user/develop/framework/
```

阅读目的不是复制实现，而是理解：

```text
service availability
        ↓
plugin lifecycle
        ↓
effect ownership
        ↓
reactive composition
```

这四者如何连接。

---

# 60. 最后给实现 Agent 的提醒

**宁可少实现，也不要多实现。**

如果两个方案都正确：

```text
方案 A：300 行
方案 B：900 行
```

优先认真思考 A。

如果一个 abstraction 只有一个实现：

```text
先不要抽象。
```

如果一个 feature 不是完成核心 invariant 所必需：

```text
先不要实现。
```

如果为了支持“未来可能存在的场景”而让当前 API 变复杂：

```text
拒绝。
```

这个项目是否漂亮，不取决于它支持多少能力，而取决于：

> **用多小的核心，建立了多清晰、多可靠的组合与生命周期模型。**
