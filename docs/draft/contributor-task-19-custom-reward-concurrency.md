# 任务 19 设计方案：Custom Reward 并发执行

> 状态：设计草案，尚未实现。本文用于 Relax 贡献者计划 2026 第一期任务 19 的方案对齐、开发记录和 Draft PR 验收。

## 1. 任务信息

| 项目 | 内容 |
| --- | --- |
| 任务编号 | 19 |
| 任务名称 | Custom reward 并发执行 |
| 参与者 | `guozhihao-224` |
| 任务入口 | `relax/engine/rewards/__init__.py` |
| 交付方式 | GitHub Issue + Draft PR |
| 任务目标 | 同步 custom reward 进入 Ray Worker 池执行；异步 custom reward 保持直接 `await`；并发限制、函数缓存和异常定位行为明确且可测试 |

## 官方任务描述（完整要求）

### 背景

同步 custom reward 如果直接在 Rollout event loop 中执行，会阻塞其他异步任务。当前实现也没有让 custom reward 复用已有 `RewardWorker` 的进程隔离能力和全局并发限制。

### 目标

- 同步 custom reward 统一交给 Reward Worker 池执行。
- 异步 custom reward 保持现有异步调用方式，直接在 event loop 中 `await`。
- 两类 custom reward 均遵守框架已有的并发控制和异常处理约定。

### 验收标准

1. 同步 custom reward 必须在独立进程中运行，不能阻塞 Rollout event loop。
2. `reward_max_concurrency` 必须能够限制 custom reward 的最大并发执行数量。
3. `reward_num_workers` 必须能够控制同步 custom reward 使用的 Reward Worker 数量。
4. 异步 custom reward 必须保持直接 `await`，不能错误地发送到同步 Worker 中执行。
5. custom reward function 在对应执行进程中只加载一次，后续请求复用已加载函数。
6. custom reward 发生异常时，错误信息必须能够定位到对应 Sample。
7. 成功、异常和高并发场景均不能产生 semaphore 泄漏、任务永久等待或死锁。

### 交付要求

- 创建并维护对应的开发 Issue，用于固定代码基线、运行环境、实现范围和验收口径。
- 提交 Draft PR，实现同步/异步 custom reward 分流、Worker 执行、函数缓存和异常定位。
- PR 必须包含针对性的 CPU/Ray 单元测试、必要的接入文档、验证命令和原始测试结果。

官方任务来源：Relax 开源贡献者计划 2026 第一期正式任务文档，任务 19「Custom reward 并发执行」。

## 2. 固定基线

### 2.1 代码基线

| 项目 | 当前值 |
| --- | --- |
| 仓库 | `https://github.com/redai-infra/Relax.git` |
| 基线 commit | `039ce876d25540adad847d4223b4de4722d8f425` |
| 当前分支 | `main` |
| 计划开发分支 | `feat/custom-reward-concurrency` |

### 2.2 环境

当前本地环境仅用于静态分析和文档编写，不作为最终验收环境。最终验收沿用本仓库 CI 的 Linux CPU 环境，避免使用本地
macOS/Python 3.14 环境替代可复制的 Ray 测试环境。

| 项目 | 当前值 |
| --- | --- |
| OS | macOS 26.5.1, arm64 |
| Python | 3.14.6 |
| Ray | 未安装 |
| PyTorch | 未安装 |
| CUDA / GPU | 不适用 |
| 最终 CPU 测试环境 | GitHub Actions `ubuntu-latest`，Python 3.10 / 3.11 / 3.12 矩阵，按该 commit 的 `requirements.txt` 安装 Ray；PR 中记录 `python --version`、`ray --version` 和 `pip freeze` 摘要 |

本任务不需要多节点 GPU 集成测试。最终验收以 CPU Ray Actor 测试、异步并发测试和 pre-commit 为主。

## 3. 背景与现状

### 3.1 当前调用链

当前存在 legacy rollout 和 agentic rollout 两条 Reward 调用链，custom reward 在不同入口下的行为并不完全一致。

Legacy rollout：

```text
generate_and_rm / generate_and_rm_group
        |
        v
batched_async_rm / async_rm
        |
        v
RewardExecutor.execute
        |
        +-- built-in async reward --> event loop
        |
        +-- built-in sync reward  --> RewardWorker Ray Actor
        |
        +-- custom single reward  --> RewardExecutor 中直接 load + await
        |
        +-- custom batch/group    --> batched_async_rm 中直接 load + await
```

Agentic rollout：

```text
RewardDomain._run_sample_reward / _run_group_reward
        |
        v
_async_rm / _batched_async_rm
        |
        +-- built-in reward --> relax.engine.rewards
        |
        +-- custom reward   --> event loop 中直接 load + await
```

关键代码：

- `RewardWorker`：`relax/engine/rewards/__init__.py` 的 `RewardWorker`
- `RewardExecutor`：`relax/engine/rewards/__init__.py` 的 `RewardExecutor`
- custom single 入口：`relax/engine/rewards/__init__.py` 的 `RewardExecutor.execute`
- batch 入口：`relax/engine/rewards/__init__.py` 的 `batched_async_rm`
- agentic custom single 入口：`relax/agentic/pipeline/reward.py` 的 `_run_sample_reward`
- agentic custom group 入口：`relax/agentic/pipeline/reward.py` 的 `_run_group_reward`
- agentic 并发限制：`relax/agentic/pipeline/reward.py` 的 `RewardDomain`
- 参数说明：`relax/utils/arguments.py` 的 `custom_rm_path` 参数定义
- 当前公开 custom reward 契约：`docs/zh/guide/configuration.md` 的 custom reward 小节

### 3.2 现存问题

1. 同步 custom reward 在 event loop 中运行会阻塞其他 Rollout 协程。
2. 普通同步函数返回非 awaitable，当前代码执行 `await rm_function(...)` 会直接失败。
3. 多个入口分别调用 `load_function()`，没有统一的加载、分类、缓存和失效边界。
4. Legacy single、legacy batch/group、agentic single、agentic group 的并发行为不一致：agentic 路径已有
   `RewardDomain` semaphore，legacy batch/group custom 路径则绕过 `RewardExecutor` semaphore。
5. 如果只修改 `relax/engine/rewards/__init__.py`，agentic custom reward 仍会直接在 event loop 中执行。
6. `custom_rm` 当前声明为 `ReloadScope.IMMEDIATE`；永久缓存 callable 会破坏“reload 后下一次调用生效”的既有行为。
7. Ray 远端错误缺少 Sample 或 group 标识，不易定位具体失败输入。
8. `batched_async_rm()` 的 custom 路径会传入 `list[Sample]`，但公开文档描述的是单 `Sample` 契约，需要在开发前冻结兼容策略。
9. 现有 `RewardExecutor.execute()` 通过 `ignore_custom` 绕过 custom 分支，但 `batched_async_rm()` 的 custom 分支没有同等优先级规则；迁移时必须明确
   `execute()`、`async_rm()`、`batched_async_rm()` 和四个新入口之间的委托关系。
10. 命名 Actor 的 `get_if_exists=True` 只按名字复用，当前没有校验 `num_workers`、实现版本或配置 fingerprint。

## 4. 设计目标

### 4.1 必须实现

- 同步 custom reward 在独立 Ray Worker 进程执行。
- 异步 custom reward 保持在当前 event loop 中直接 `await`。
- DeepEyes 等 `async def` custom reward 明确属于 Driver/event-loop 路径，不要求出现在 RewardWorker PID 中；本任务的 Worker
  收益对象是同步、CPU-bound custom reward。
- `reward_max_concurrency` 同时约束同步和异步 custom reward。
- `reward_num_workers` 控制同步 custom reward 的 Worker 数量。
- legacy 和 agentic 路径共享同一个 custom reward 执行边界，不复制加载、分类和错误处理逻辑。
- custom function 在每个执行进程、每个 reload generation 中只加载一次并缓存。
- `custom_rm` reload 后，Driver 和 Worker 的下一次调用必须使用新函数。
- 异常信息包含 custom path 和 Sample 标识。
- 成功、异常和取消均不得造成 semaphore 泄漏或死锁。
- 保持输入 Sample 与输出 Reward 的顺序一致。
- single 和 group 调用模式必须显式，不通过 payload 类型或失败重试猜测。
- 兼容现有公开路径：除本文第 5 节明确标注的 legacy batch 兼容例外外，不静默改变 custom reward 的入参形态。

### 4.2 不在范围内

- 不修改 Controller、Service 或 Launcher。
- 不新增依赖。
- 不改变内置 Reward 的数学行为。
- 不实现跨节点 Reward 调度策略。
- 不将 CPU-bound custom reward 改成线程池执行。
- 不修改 `relax/utils/arguments.py` 的现有参数定义。
- 不重构内置 Reward dispatch，不引入通用 RewardBackend 继承体系。

## 5. 兼容性约定

### 5.1 维护者决策：兼容优先

本任务选择**兼容优先**，不是破坏性迁移。原因是当前 `batched_async_rm()` 在设置
`custom_rm_path` 时无条件以 `list[Sample]` 调用外部函数；即使 `group_rm=False`，也可能已有用户依赖该行为。
本任务不把 `group_rm=False` 的 legacy batch 调用静默改成 N 次 single 调用，也不通过“第一次失败后重试”探测签名。

新的显式入口契约和 legacy 兼容行为分别如下：

| 调用入口 | `custom_rm_path` | `ignore_custom` | `group_rm` | 调用形态 | 兼容性说明 |
| --- | --- | --- | --- | --- | --- |
| `async_rm()` / `execute()` | 有 | `False` | `False` | 一个 `Sample`，返回一个 scalar/dict | 新的 scalar 契约 |
| `async_rm()` / `execute()` | 有 | `True` | 任意 | 走内置 Reward，不调用 custom | `ignore_custom` 优先 |
| `execute_custom_sample()` | 有 | 不适用 | `False` | 一个 `Sample`，返回一个 scalar/dict | 显式 single 入口 |
| 显式 `execute_custom_group()` | 有 | 不适用 | 不读取 | 一个 `list[Sample]`，返回等长 list | 显式 group 入口，不根据 `args.group_rm` 拒绝调用 |
| legacy `batched_async_rm()` | 有 | `False` | `True` | 一个 `list[Sample]`，返回等长 list | 正式 group 行为 |
| legacy `batched_async_rm()` | 有 | `False` | `False` | 仍传一个 `list[Sample]` | legacy batch 兼容例外，记录弃用风险 |
| `batched_async_rm()` | 无或 `ignore_custom=True` | 任意 | 任意 | `gather(async_rm(...))`，保持输入顺序 | 不改变现有内置路径 |

规范 custom function 签名为：

```python
def reward_func(args, sample: Sample, **kwargs) -> float | dict:
    ...

async def async_reward_func(args, sample: Sample, **kwargs) -> float | dict:
    ...

def group_reward_func(args, samples: list[Sample], **kwargs) -> list[float | dict]:
    ...
```

`group_rm` 表示 rollout 的分组语义；在 legacy `batched_async_rm()` 兼容路径中，它不再被解释为是否允许
batch 入参。未来若要移除该兼容例外，必须另开 Issue，明确标记为破坏性变更，并提供迁移说明；本任务不引入隐式
fan-out。

只有可被 `inspect.iscoroutinefunction()` 正确识别的 `async def` callable 属于异步 custom reward；普通同步 callable
即使返回 awaitable，也不属于支持的异步契约，必须报错而不是在 Worker 中临时创建 event loop。

### 5.2 Sync Worker args 边界

“兼容优先”只保证 custom reward 的调用形态（scalar `Sample` / batch `list[Sample]`）不被静默改变，**不保证
sync Worker 继续收到完整训练 Namespace**。这是有意的序列化和进程隔离边界变更，必须在迁移文档中明确说明：

- async custom reward 仍在 Driver/event loop 中运行，继续收到原有完整 `args`。
- sync custom reward 在 Worker 中只收到 `RewardWorkerConfig.as_namespace()` 的白名单视图。
- 现有 sync custom 函数依赖白名单外字段时，必须迁移到 `custom_options`，或改为 async/Driver 路径；不能期待 Worker
  隐式携带完整 Namespace。
- `custom_options` 不是 CLI 参数，也不从完整 `args` 自动复制；调用方通过显式 `custom_options={...}` keyword
  传入 `execute_custom_sample()` / `execute_custom_group()`（legacy wrapper 通过 `kwargs` 透传）。没有额外配置时使用
  空 mapping。
- 文档示例必须展示如何从训练配置中挑选 JSON-like、可序列化字段填充 `custom_options`；模型、Tokenizer、Ray handle、
  event loop、锁和文件句柄不得放入其中。

示例（字段由调用方显式选择，不由框架自动拷贝）：

```python
custom_options = {
    "task_name": args.task_name,
    "threshold": float(args.reward_threshold),
}
await executor.execute_custom_sample(args, sample, custom_options=custom_options)
```

因此，sync args 收窄属于明确记录的迁移要求，而不是被“兼容优先”掩盖的隐含兼容承诺。

### 5.3 Issue 确认门槛

Issue 必须由维护者明确回复 `compatibility-first`（或明确改选 breaking-change 并更新本文）后，任务才进入实现阶段。
在确认之前，贡献者不得自行把 `group_rm=False` 的 legacy batch 路径改成 N 次 single 调用。该确认只冻结调用
契约，不代表已经完成实现或测试验收。

## 6. 修订实现方案

### 6.1 设计原则与职责边界

本任务采用组合而非继承，不创建通用 RewardBackend 层次。职责划分如下：

| 组件 | 职责 | 不负责 |
| --- | --- | --- |
| `RewardExecutor` | semaphore、single/group 入口、执行位置选择、Worker 选择、统一异常边界 | 模块 reload 实现、业务 Reward 计算 |
| `_CustomRewardResolver` | load、sync/async 分类、按 generation 缓存、缓存失效 | Ray 调度、semaphore、结果写回 Sample |
| `RewardWorker` | 在独立进程执行同步 custom reward | 判断 single/group 模式、包装业务异常 |
| legacy/agentic 调用方 | 判断何时需要 Reward，并显式选择 single/group 入口 | 直接 load 或调用 custom function |

`RewardExecutor` 不直接维护裸 `dict[str, Callable]`，避免继续扩大其职责。Driver 和每个 Worker 各组合一个私有
`_CustomRewardResolver` 实例。

### 6.2 统一执行入口

legacy 路径调用带 `RewardExecutor` semaphore 的入口：

```python
await executor.execute_custom_sample(args, sample, **kwargs)
await executor.execute_custom_group(args, samples, **kwargs)
```

agentic 路径已由 `RewardDomain` 持有 semaphore，因此调用命名明确、不会再次获取许可的 dispatch：

```python
await executor.dispatch_custom_sample(args, sample, **kwargs)
await executor.dispatch_custom_group(args, samples, **kwargs)
```

不使用 `already_holds_semaphore: bool` 一类 flag argument 切换并发行为。四个入口共享私有 `_invoke_custom()`，但
single/group 分别负责各自的参数和返回值校验。每个顶层入口只获取一次 semaphore：`execute()` / legacy
`execute_custom_*()` 负责许可，内部的 `execute_builtin()`、`_invoke_custom()` 和 resolver 不再重复获取；agentic
`dispatch_custom_*()` 假定调用方已经持有 `RewardDomain` 许可。

入口分支必须按以下顺序实现，`ignore_custom` 优先于所有 custom 分支：

```python
async def execute(self, args, sample, **kwargs):
    async with self._semaphore:
        if args.custom_rm_path and not kwargs.get("ignore_custom", False):
            return await self._execute_custom_sample_unlocked(args, sample, **kwargs)
        return await self._execute_builtin_unlocked(args, sample, **kwargs)


async def batched_async_rm(args, samples, **kwargs):
    if args.custom_rm_path and not kwargs.get("ignore_custom", False):
        # Legacy compatibility: preserve the historical list[Sample] call,
        # even when group_rm=False. Do not fan out implicitly.
        return await executor.execute_custom_group(args, samples, **kwargs)
    return await asyncio.gather(*(async_rm(args, sample, **kwargs) for sample in samples))
```

因此，`execute()` 继续是现有 `async_rm()` 的兼容入口，并直接委托到不再获取 semaphore 的私有实现；
`execute_custom_sample()` / `execute_custom_group()` 是供直接调用的、各自负责一次许可的公开 wrapper。两者不得
互相调用，否则会产生双重 semaphore。`ignore_custom=True` 时，single 和 batch 都必须走内置 Reward 路径。非 custom
的 `batched_async_rm()` 继续 `gather(async_rm(...))`，保持输入顺序和既有 `kwargs` 透传。
legacy batch 兼容例外只存在于该 wrapper；新代码必须使用显式的 `execute_custom_sample()` 或
`execute_custom_group()`。

```text
legacy rollout ──┐
                 ├──> RewardExecutor custom single/group entry
agentic rollout ─┘                 |
                                   +-- resolver: load/classify/cache
                                   |
                                   +-- async --> Driver event loop
                                   |
                                   +-- sync  --> RewardWorker
```

`relax/agentic/pipeline/reward.py` 不再直接 `load_function()`；它保留现有 RewardDomain 调度职责，但将 custom
执行委托给无重复 semaphore 的 dispatch 入口。

### 6.2.1 现有 API 迁移规则

- 保留 `RewardExecutor.execute()`、`async_rm()` 和 `batched_async_rm()`，不删除或改名公开入口。
- `execute()` 委托到不获取 semaphore 的私有 `_execute_custom_sample_unlocked()` / `_execute_builtin_unlocked()`；
  `execute_custom_sample()` 仍是直接调用时的 semaphore-owning wrapper，内置 Reward 分支和 `ignore_custom` 行为保持不变。
- `batched_async_rm()` 的非 custom 分支继续批量创建 `async_rm()` 任务并 `gather`；custom 分支按第 5 节的 legacy
  兼容规则调用 `execute_custom_group()`。
- `ignore_custom` 必须沿调用链显式透传，不能只在 `execute()` 中局部判断。
- 所有新入口保留 `**kwargs`，并在 Driver/Worker 边界前完成可序列化检查；不得因迁移丢弃历史 kwargs。
- agentic single/group 只调用 `dispatch_custom_sample()` / `dispatch_custom_group()`，因为 semaphore 由
  `RewardDomain` 唯一持有；legacy 入口不复用这两个无许可入口。

### 6.3 Callable 解析与分类

增加私有、无继承的 resolver：

```python
@dataclass(frozen=True)
class _ResolvedCustomReward:
    function: Callable
    is_async: bool
    generation: int


class _CustomRewardResolver:
    def ensure_loaded(self, path: str, generation: int) -> _ResolvedCustomReward:
        ...

    def invalidate(self, path: str) -> None:
        ...

    def invalidate_all(self) -> None:
        ...
```

首次 `ensure_loaded()` 时完成加载和分类，后续执行直接使用缓存结果。async 检测规则为：

```python
def _is_async_callable(function: Callable) -> bool:
    function = inspect.unwrap(function)
    if inspect.iscoroutinefunction(function):
        return True
    return inspect.iscoroutinefunction(getattr(function, "__call__", None))
```

不允许通过先调用函数再观察返回值来区分 sync/async，因为同步函数可能产生外部请求、文件写入或计数器副作用。
无法被上述逻辑识别的 decorator 包装函数不属于保证支持范围，接入文档必须说明应使用 `functools.wraps` 保留属性。

### 6.4 缓存与热重载生命周期

generation 的所有权和传播固定如下：

1. `reload_utils.py` 增加实例级 `ReloadGenerationRegistry`，由每个 `ReloadableMixin` 实例持有并保存
   `(module_name, module_path) -> generation`；不使用模块级全局 generation。
2. `ReloadableMixin.reload_function_by_name("custom_rm")` 是 reload 的唯一写入口。只有 `reload_function()` 成功
   后才调用 `registry.bump(module_name, module_path)`，并在返回值中包含 `generation`；失败时 generation 不变。
3. Driver 创建 `RewardExecutor` 时注入同一个 registry（或等价的
   `get_generation(module_name, module_path)` provider）。`RewardExecutor.current_custom_generation()` 只读该
   provider，不自行递增；每次 custom 调用把 `path`、`generation` 和实现版本一并传给 Worker。
4. Worker 内部只维护进程本地 `(path, generation)` cache。收到更高 generation 时，调用显式的
   `ensure_loaded(path, generation)`，先刷新该 Worker 的 `sys.modules`，再重新解析 callable；仅调用普通
   `load_function()` 不算刷新。
5. `reload_function_by_name()` 的返回值是 Driver 传播 generation 的唯一来源；不允许调用方自行猜测或重置 generation。
6. reload 发生时已经提交的任务继续使用提交时的旧 generation；reload 返回成功后新提交的任务使用新 generation。
   Worker 刷新失败必须让对应任务失败并保留 path/generation，不得静默回退到旧 callable。

Provider 的 bind 时序固定如下：

1. `RewardExecutor.__init__()` 默认安装进程内 `StaticGenerationProvider(0)`；因此单测或仅 agentic 路径在没有
   `RolloutManager` 时可以正常执行，初始 generation 为 `0`，但没有 reload 写入口。
2. `RolloutManager.__init__()` 在当前实现不调用 `super().__init__()`，因此必须显式创建
   `self._reload_generations = ReloadGenerationRegistry()`；完成其余字段、reload registry 和服务初始化后、开始接收请求前，
   在函数末尾调用：
   `RewardExecutor.get_or_create(...).bind_generation_provider(self._reload_generations.current)`。
3. `bind_generation_provider()` 对同一 provider 重复调用必须幂等；已经绑定到另一个活动 provider 时拒绝覆盖并报配置错误，
   除非先执行 `RewardExecutor.reset()`。这样不会因多个 RolloutManager 或测试顺序悄悄漂移 generation 来源。
4. 如果 Executor 先于 Manager 创建并已经执行过 custom reward，晚绑定时必须调用 Driver resolver 的
   `invalidate_all()`；后续请求重新读取 Manager 当前 generation。绑定前已提交的任务仍使用 generation `0`，不在执行中途切换。
5. 如果 Manager 在首次 custom 调用后才绑定，必须按同一规则失效旧 Driver cache；如果当前 generation 仍为 `0`，也要完成
   provider 替换，不能仅因数值相同而保留未知来源的 cache。
6. 仅 agentic 且确实需要热重载时，调用方必须显式提供同一类 provider；没有 provider 的路径只保证 generation `0` 的
   初始加载，不宣称支持 `reload_function_by_name("custom_rm")` 后自动传播。

缓存遵守以下不变量：

1. 每个进程、每个 path、每个 generation 最多调用一次 `load_function()`。
2. Driver 和 Worker 是不同进程，分别加载一次属于必要加载。
3. `custom_rm` reload 成功后 generation 必须增加。
4. Driver 下一次 `ensure_loaded()` 时丢弃旧 callable。
5. 所有存活 RewardWorker 必须在下一次执行前收到新 generation，并重新加载对应 path。
6. Actor 重建后从空缓存开始，不能继承旧 Actor 的隐式状态。

`custom_rm` 目前属于 `ReloadScope.IMMEDIATE`；本任务只改变其缓存失效链，不改变其他 IMMEDIATE 函数的语义。
对 `custom_rm` 而言，“IMMEDIATE”定义为 reload 成功后下一次 Driver/Worker 调用使用新 generation，而不是只在
Driver 进程刷新 `sys.modules`。

固定命名 Actor 使用 `get_if_exists=True` 时，还需要校验 Actor generation/实现版本，防止复用旧代码和旧缓存。
创建 Actor 时写入不可变的 `WorkerConfigFingerprint`（至少包含 `num_workers`、实现版本、custom path 和
`generation_protocol_version`）。该字段描述协议版本，不是运行时 reload counter。发现同名 Actor fingerprint 不匹配时必须拒绝复用并报出差异，或使用包含 job/rollout
namespace 的新名称；不能静默接受首次创建参数。

### 6.5 Sync custom reward Worker 接口

Worker 方法只执行同步函数，不承担业务分流和异常格式化：

```python
def compute_custom(
    self,
    *,
    path: str,
    generation: int,
    worker_config: RewardWorkerConfig,
    payload: Sample | list[Sample],
    kwargs: dict,
):
    loaded = self._custom_resolver.ensure_loaded(path, generation)
    if loaded.is_async:
        raise TypeError("Async custom reward must run in the rollout event loop")
    return loaded.function(worker_config.as_namespace(), payload, **kwargs)
```

传递 import path 而不是函数对象，避免 Ray 序列化闭包、局部函数或不可 pickle callable。single/group 模式由
Driver 选择显式入口，Worker 不根据 payload 类型猜测模式；Worker 内部统一只调用 `ensure_loaded()`，不直接调用
`load_function()`。

`RewardWorkerConfig` 是 Worker 专用的、冻结的可序列化 dataclass，不是完整训练 `Namespace` 的别名。它只包含
custom reward 契约允许使用的字段：`custom_rm_path`、`group_rm`、`reward_key`、`rm_type`、`rm_url`、
`implementation_version`、`generation` 以及显式声明的 `custom_options`。Worker 通过 `as_namespace()` 提供
兼容的属性访问视图。

以下对象禁止进入 Worker payload：模型/Tokenizer、Ray Actor handle、事件循环、锁、文件句柄、线程对象、GPU tensor、
未声明的训练组件和任意闭包。Driver 通过 `build_reward_worker_config(args, custom_options)` 按白名单复制字段；
`cloudpickle.dumps()` 只在 WorkerConfig 首次创建或其 fingerprint 变化时执行，并缓存该 fingerprint 的校验结果，
不在每个 reward 请求上重复预检。失败时立即抛出包含 custom path 和字段名的 `TypeError`。大体积且不可变的数据只有在
显式声明为 object reference 时才允许使用 `ray.put()`，不能用它绕过字段契约。

### 6.6 并发语义

- Legacy single custom reward 使用 `RewardExecutor` semaphore。
- Legacy batch/group custom reward 必须进入统一 custom group 入口，不再绕过 semaphore。
- Agentic 路径已经由 `RewardDomain` semaphore 控制提交数量；接入统一入口时必须避免无意引入不一致的双重限制。
- 显式 `execute_custom_sample()` / `dispatch_custom_sample()` 的并发单位为一个 Sample。
- 显式 `execute_custom_group()` 以及 `group_rm=True` 的 legacy batch 路径，并发单位为整个 group/list；一个同步 group
  function 作为一次 Worker 调用执行。
- `group_rm=False` 的 legacy `batched_async_rm()` 是兼容例外：并发单位是“一次 `list[Sample]` 调用”，只占用一个
  group semaphore，不会扇出为 N 个 Sample 许可。
- `reward_num_workers` 是同步 custom reward 的物理并行上限；`reward_max_concurrency` 是允许进入 Reward 执行区域的
  逻辑并发上限，实际同步并行度不超过两者较小值。

semaphore 由调用域唯一持有：legacy 的 `execute_custom_*` 入口使用 `RewardExecutor` semaphore；agentic 的
`RewardDomain` 持有 semaphore 后调用 `dispatch_custom_*`。两类入口共享实际执行实现，但不通过布尔参数改变锁行为。

### 6.7 返回值校验

- single custom reward 接受数值或 dict，保持现有 `reward_key` 行为。
- group custom reward 必须返回 list，且长度与输入 `samples` 完全一致。
- 长度校验在统一 Driver 边界完成，不依赖调用方的 `zip(strict=...)` 行为。
- 结果顺序严格对应输入顺序，不允许 Worker 内部重排。
- 同步函数如果返回 awaitable，抛出明确契约错误，不在 Worker 中创建 event loop 补救。

### 6.8 异常边界

新增私有异常类型：

```python
class CustomRewardError(RuntimeError):
    pass
```

Worker 保留原始业务异常；Driver 捕获本地异常或 `RayTaskError`，只在统一调度边界包装一次，避免多层
`RuntimeError -> RayTaskError -> RuntimeError`。

single 错误消息至少包含：

- `custom_rm_path`
- `sample.index`
- `sample.group_index`
- `sample.session_id`
- 原始异常类型和消息

group 错误消息至少包含 path、group 标识、Sample 数量和各 Sample 的稳定标识摘要，不任意选择单个 Sample 代表整个
group。使用 `raise CustomRewardError(...) from error` 保留本地或 Ray 远端 traceback。

### 6.9 Clean Code 约束

- custom function 的 load、分类、缓存和失效只能通过 `_CustomRewardResolver`，调用方不得直接访问缓存字典。
- legacy 和 agentic 调用方不得直接调用 `load_function()` 或 custom callable。
- single/group 使用显式方法，不使用 payload 类型猜测，不使用失败后重试探测调用模式。
- semaphore 行为通过不同方法名表达，不使用 flag argument 改变资源获取语义。
- 异常只在 Driver 调度边界包装一次，不在 Worker 和调用方重复拼接上下文。
- 不新增 RewardBackend 继承体系，不顺带重构内置 Reward dispatch。
- 新 helper 和方法必须有显式类型注解；已知结构的数据优先使用 dataclass，不使用无约束嵌套 dict。
- 缓存、Actor 和 singleton 必须提供测试可控的清理或失效接口，测试不得直接依赖修改多个内部字段完成复位。
- `RewardExecutor` 提供 `async close()` 释放本实例的 semaphore、Worker 引用和命名 Actor；提供
  `@classmethod async reset()` 作为测试/进程生命周期入口，完成 close 后再清空 singleton。生产代码不得通过直接赋值
  `_instance` 或 `_workers` 清理状态。

## 7. 文件改动计划

| 文件 | 计划改动 |
| --- | --- |
| `relax/engine/rewards/__init__.py` | 统一 single/group custom 执行入口、resolver、Worker 方法、结果校验和异常边界 |
| `relax/agentic/pipeline/reward.py` | 移除 custom function 直接加载，委托统一执行入口并避免重复获取 semaphore |
| `relax/utils/reload_utils.py` | 新增实例级 `ReloadGenerationRegistry`；`reload_function_by_name("custom_rm")` 成功后递增并返回 generation |
| `relax/distributed/ray/rollout.py` | 在 `RolloutManager.__init__()` 完成初始化前 bind Driver generation provider；不改变 reload 写入口 |
| `tests/engine/rewards/test_reward_worker.py` | legacy custom reward 并发、缓存、reload、异常、排序和死锁回归测试 |
| `tests/agentic/pipeline/test_reward.py`（新建目录） | agentic single/group custom reward 分流和并发所有权测试；优先复用 legacy fixture，Ray 不可用时使用 mock dispatch |
| `tests/engine/rewards/custom_reward_fixtures.py` | 可 import 的同步、异步、慢函数和异常 fixture |
| `docs/en/guide/configuration.md` | 明确 custom reward sync/async 与 group 契约 |
| `docs/zh/guide/configuration.md` | 中文对应说明 |
| `docs/en/guide/customize-training.md` | 增加同步和异步 custom reward 示例 |
| `docs/zh/guide/customize-training.md` | 中文对应说明 |

## 8. 测试方案

### 8.1 正确性测试

- legacy single、legacy group、agentic single、agentic group 四条路径均有针对性测试。
- async custom reward 在当前 event loop 执行，Worker 调用次数为 0。
- sync custom reward 在不同进程 PID 中执行。
- async custom reward 收到原有 Driver `args`；sync custom reward 只收到白名单构造的 `RewardWorkerConfig.as_namespace()`，不传完整训练 Namespace。
- `Sample` 和声明过的 `kwargs` 完整传递；未声明或不可序列化字段在 Driver 提交前明确失败。
- dict 和数值返回值保持兼容。
- group 输出顺序与输入 Sample 顺序一致。
- group 返回非 list 时给出明确错误。
- group custom reward 返回长度不匹配时给出明确错误。
- 同步 callable 返回 awaitable 时给出明确契约错误。
- agentic 路径不会再次直接调用 `load_function()`。
- `group_rm=False` 的 legacy `batched_async_rm()` 仍进行一次 list 调用；显式 single/group 入口分别遵守 scalar/list 契约。
- `ignore_custom=True` 在 single 和 batch 入口均优先走内置 Reward。

### 8.2 并发测试

- 预热全部 Worker 和函数缓存后，8 个 `sleep(0.1)` 同步 Reward、4 个 Worker 的执行时间应显著小于串行约
  0.8 秒。
- 运行慢同步 Reward 时，event-loop heartbeat 仍能持续推进。
- 使用协调 Actor 或执行时间区间记录实际重叠，验证 `reward_max_concurrency=N` 时最大逻辑并发不超过 N。
- 验证同步实际并行度不超过 `min(reward_max_concurrency, reward_num_workers)`。
- agentic 请求只获取一次并发许可，不同时受两个相同上限的 semaphore 排队。
- sync custom reward 异常后，后续请求仍能进入 semaphore。
- 覆盖 Executor 先创建、Manager 后 bind；首次 custom 调用前后 bind 都必须使 Driver cache 失效并读取当前 generation。
- 覆盖同一 provider 重复 bind 幂等、不同活动 provider bind 被拒绝，以及无 Manager 时默认 generation `0`。

wall-clock 使用宽松阈值且只作为辅助证据，Actor 冷启动、首次 import 和首次序列化不得计入主要计时区间。

### 8.3 缓存测试

- 同一 Worker 对相同 path 和 generation 重复执行，只调用一次 `load_function()`。
- 不同 path 分别缓存。
- async Driver 路径对相同 generation 重复执行只加载一次。
- `reload_function_by_name("custom_rm")` 只有成功后才递增 generation，并在结果中返回新值。
- `custom_rm` reload 后 Driver 下一次调用使用新函数。
- `custom_rm` reload 后全部存活 Worker 下一次调用使用新函数；每个 Worker 在自己的 `sys.modules` 中刷新。
- Manager late-bind 后 Driver 旧 cache 被清空，in-flight 旧 generation 任务仍完成且新任务使用 provider generation。
- Actor 被销毁并重建后使用空缓存，不复用旧 generation 状态。

### 8.4 异常测试

- 异常包含 Sample index。
- group 异常包含 group 标识、Sample 数量和稳定标识摘要。
- Ray 远端 traceback 被保留。
- 取消等待中的 Reward 不会增加 semaphore 容量。
- 产品语义固定为：Driver 取消只释放本地 semaphore；已经提交的 Ray task 不保证取消，可以继续执行并产生副作用，返回值被丢弃。
- 测试验证取消后 semaphore 可复用、不会重复释放，且已提交 Ray task 的“继续执行”行为被明确记录，而不是依赖未定义的强制取消。
- Worker 异常不会导致 batch 永久等待。

### 8.5 序列化与 Actor 生命周期测试

- 使用接近真实训练配置的 `argparse.Namespace`，包含不可 pickle 的对象，验证白名单 builder 只复制允许字段。
- 对需要传递但不可序列化的字段给出包含 custom path 和字段名的明确错误；被明确排除的训练对象不应进入 Ray payload。
- 使用真实 `RewardWorkerConfig` fingerprint 检查 `num_workers`、实现版本、custom path 和
  `generation_protocol_version` 不匹配时拒绝复用。
- 相同 fingerprint 重复请求不会再次执行 `cloudpickle.dumps()`；只有首次创建或 fingerprint 变化时重新预检。
- `custom_options` 只从显式 keyword 构建；未显式传入时为空，不从完整训练 Namespace 偷渡字段。
- 调用 `await RewardExecutor.reset()` 清理 singleton、Worker 引用和命名 Actor；测试不得直接改 `_instance` / `_workers`。

### 8.6 验证命令

```bash
pytest tests/engine/rewards/test_reward_worker.py -q
pytest tests/engine/rewards/ -q
pytest tests/agentic/pipeline/test_reward.py -q
pre-commit run --all-files
```

CI 验收使用 `.github/workflows/ci.yml` 的 `ubuntu-latest` 和 Python 3.10/3.11/3.12 矩阵；本地 macOS、Python 3.14
和未安装 Ray 的环境只能执行静态检查，不能宣称 CPU Ray 测试通过。多节点 GPU 集成测试跳过，原因是本任务只修改
CPU Reward 调度与 Ray Actor 执行，不涉及 GPU 模型或分布式训练通信。

## 9. 验收指标

| 验收项 | 证据 |
| --- | --- |
| sync custom reward 独立进程执行 | PID 或 Actor 测试 |
| async custom reward 直接 await | mock Worker 调用次数为 0 |
| 四条调用路径行为一致 | legacy/agentic single/group 测试 |
| 并发参数生效 | 协调 Actor 最大并发计数和预热后 wall-clock 测试 |
| 不存在双重 semaphore | agentic 并发许可获取次数断言 |
| 函数按 generation 加载一次 | Driver/Worker cache 与 reload 单测 |
| reload 后新函数生效 | Driver 和全部 Worker reload 回归测试 |
| generation provider 接线完整 | Executor 先/后于 Manager 创建、late-bind cache invalidation 和 provider 重绑测试 |
| legacy batch 兼容性保持 | `group_rm=False` + `batched_async_rm()` list 入参回归测试 |
| `ignore_custom` 优先级保持 | single/batch custom 与 built-in 分支测试 |
| 异常可定位到输入 | single/group 错误消息和 traceback 断言 |
| 无死锁 | timeout 包裹的异常、取消和高并发测试 |
| Actor 生命周期明确 | 命名 Actor 复用、版本校验和重建测试 |
| Worker 参数边界明确 | 真实 `Namespace` 白名单和不可序列化字段测试 |
| 测试生命周期明确 | `RewardExecutor.reset()` 清理测试 |
| 兼容现有内置 Reward | 现有 RewardExecutor 测试全部通过 |

## 10. 风险与回退

### 10.1 风险

- 外部用户依赖的 legacy batch custom reward 行为可能限制未来 API 简化；本任务将其明确记录为兼容例外，不静默破坏。
- 未进入白名单的 `args` 字段不能传入 Worker；如果 custom function 依赖这类字段，必须在 Driver 侧显式报错并迁移。
- 固定命名 Actor 使用 `get_if_exists=True`，如果 fingerprint 校验缺失，可能复用旧代码、旧缓存或不同配置创建的 Actor。
- decorated callable 的 sync/async 属性可能无法仅靠 `inspect` 完全判断。
- Driver reload 与 Worker 独立进程模块缓存之间可能出现 generation 不一致。
- Driver 取消不强制撤销已提交的 Ray Actor 方法；远端函数可能继续执行或产生副作用，调用方必须接受该语义。
- agentic 路径如果同时获取 `RewardDomain` 和 `RewardExecutor` semaphore，可能造成非预期排队和吞吐下降。

### 10.2 缓解措施

- §5 已冻结 scalar/group/legacy batch 契约；实现前只等待 Issue 中的 `compatibility-first` 维护者确认。
- 使用真实 DeepEyes custom reward 做兼容测试。
- 使用实例级 generation registry 驱动 Driver/Worker 缓存失效，并测试 reload 后下一次调用。
- 为命名 Actor 校验 `WorkerConfigFingerprint`；不匹配时拒绝复用或使用 job/rollout namespace。
- 测试 fixture 通过 `await RewardExecutor.reset()` 清理 singleton 和 Ray Actor，不直接修改私有字段。
- 对返回 awaitable 的异常情况提供清晰错误，而不是静默在线程或 Actor 中运行。
- 对真实 `argparse.Namespace` 使用白名单 builder 和 cloudpickle 预检；公开文档明确 Worker 不接收完整 Namespace。
- 明确 semaphore 唯一所有者，并通过许可获取次数测试防止双重限制。

### 10.3 回退方式

- 该改动不增加配置项；回退时同时恢复 legacy 和 agentic custom reward 的旧分支。
- 内置 Reward 路径保持不变，可独立回退 custom 执行逻辑。
- reload generation 逻辑需要与执行入口一起回退，不能保留失去消费者的半套状态。

## 11. Draft PR 模板

### Summary

- Closes `TODO(agent): 任务 19 对应开发 Issue`
- 将同步 custom reward 从 Rollout event loop 移入 Ray RewardWorker 池。
- 保持 async custom reward 直接 `await`，统一 legacy/agentic single/group 执行边界。
- 增加 reload-safe 函数缓存、明确的并发所有权和统一异常定位。

### Changes

- `RewardExecutor` 增加显式 single/group custom reward 执行入口。
- 增加组合式 `_CustomRewardResolver`，集中处理加载、分类、generation 缓存和失效。
- `RewardWorker` 增加同步 custom function 执行接口，不承担业务分流和异常包装。
- agentic RewardDomain 委托统一执行入口，并保持 semaphore 唯一所有权。
- 明确 scalar custom reward 与 group reward 的调用契约。
- 增加 CPU Ray Actor 并发、reload、异常、取消、Actor 生命周期和缓存测试。
- 更新中英文接入文档。

### Verification

- 环境、硬件、commit：GitHub Actions `ubuntu-latest`，Python 3.10/3.11/3.12，CPU-only，基线 `039ce876...`；PR 记录实际 Ray 版本和 `pip freeze` 摘要。
- 可复制命令：见本文第 8.6 节。
- 单元测试结果：`TODO(agent)`
- 并发 before/after：`TODO(agent)`
- 日志或 profile：`TODO(agent)`

### Risk & Rollback

- 已知限制：依赖 importable dotted path；decorator 必须保留 coroutine 属性；不支持不可序列化参数。
- 主要风险：历史 batch custom reward 兼容性、Worker reload generation 和命名 Actor 复用。
- 回退方式：同时恢复 legacy/agentic custom reward 直接调用分支，不影响内置 Reward。

### Checklist

- [ ] Diff 仅包含任务 19 必要改动
- [ ] 新增和现有 Reward 测试全部通过
- [ ] Issue 已获维护者 `compatibility-first` 确认（或已更新为明确的 breaking-change 方案）
- [ ] scalar/group/legacy batch custom reward 契约已与配置文档同步
- [ ] legacy/agentic single/group 四条路径均已覆盖
- [ ] custom_rm reload 后 Driver 和全部 Worker 使用新函数
- [ ] RolloutManager provider bind 的创建顺序、late-bind cache invalidation 和默认 generation `0` 已覆盖
- [ ] semaphore 唯一所有权已通过测试确认
- [ ] 命名 Actor 生命周期和复用规则已明确
- [ ] `RewardWorkerConfig` 白名单和 `RewardExecutor.reset()` 已通过测试确认
- [ ] 中英文文档同步更新
- [ ] `pre-commit run --all-files` 通过
- [ ] 不含密钥、数据集、checkpoint 或机器隐私信息
- [ ] 已逐条回复 review comment
