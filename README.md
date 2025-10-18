# sdax - Structured Declarative Async eXecution

[![PyPI version](https://badge.fury.io/py/sdax.svg)](https://pypi.org/project/sdax/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub](https://img.shields.io/badge/github-sdax-blue.svg)](https://github.com/owebeeone/sdax)

`sdax` is a lightweight, high-performance, in-process micro-orchestrator for Python's `asyncio`. It is designed to manage complex, tiered, parallel asynchronous tasks with a declarative API, guaranteeing a correct and predictable order of execution.

It is ideal for building the internal logic of a single, fast operation, such as a complex API endpoint, where multiple dependent I/O calls (to databases, feature flags, or other services) must be reliably initialized, executed, and torn down.

**Links:**
- 📦 [PyPI Package](https://pypi.org/project/sdax/)
- 💻 [GitHub Repository](https://github.com/owebeeone/sdax)
- 🐛 [Issue Tracker](https://github.com/owebeeone/sdax/issues)

## Key Features

- **Immutable Builder Pattern**: Build processors using a fluent builder API that produces immutable, reusable processor instances.
- **Structured Lifecycle**: Enforces a rigid `pre-execute` -> `execute` -> `post-execute` lifecycle for all tasks.
- **Tiered Parallel Execution**: Tasks are grouped into integer "levels." All tasks at a given level are executed in parallel, and the framework ensures all tasks at level `N` complete successfully before level `N+1` begins.
- **Guaranteed Cleanup**: `post-execute` runs for **any task whose `pre-execute` was started**, regardless of whether it succeeded, failed, or was cancelled. This ensures resources are always released.
- **Concurrent Execution Safe**: Multiple concurrent executions of the same processor instance are fully isolated, perfect for high-throughput API endpoints.
- **Declarative & Flexible**: Define tasks and task functions as frozen dataclasses. Methods for each phase are optional, and each can have its own timeout and retry configuration.
- **Lightweight**: Runs directly inside your application's event loop with minimal dependencies (datatrees, frozendict), with minimal overhead (see Performance section for details).

## Installation

```bash
pip install sdax
```

Or for development:
```bash
git clone https://github.com/owebeeone/sdax.git
cd sdax
pip install -e .
```

## Quick Start

```python
import asyncio
from dataclasses import dataclass
from sdax import AsyncTaskProcessor, AsyncTask, TaskFunction

# 1. Define your context class with typed fields
@dataclass
class TaskContext:
    user_id: int | None = None
    feature_flags: dict | None = None
    db_connection = None

# 2. Define your task functions
async def check_auth(ctx: TaskContext):
    print("Level 1: Checking authentication...")
    await asyncio.sleep(0.1)
    ctx.user_id = 123
    print("Auth successful.")

async def load_feature_flags(ctx: TaskContext):
    print("Level 1: Loading feature flags...")
    await asyncio.sleep(0.2)
    ctx.feature_flags = {"new_api": True}
    print("Flags loaded.")

async def fetch_user_data(ctx: TaskContext):
    print("Level 2: Fetching user data...")
    if not ctx.user_id:
        raise ValueError("Auth failed, cannot fetch user data.")
    await asyncio.sleep(0.1)
    print("User data fetched.")

async def close_db_connection(ctx: TaskContext):
    print("Tearing down db connection...")
    await asyncio.sleep(0.05)
    print("Connection closed.")

async def main():
    # 3. Create your context
    ctx = TaskContext()

    # 4. Build an immutable processor with declarative workflow
    processor = (
        AsyncTaskProcessor.builder()
        .add_task(
            level=1,
            task=AsyncTask(
                name="Authentication",
                pre_execute=TaskFunction(function=check_auth),
                post_execute=TaskFunction(function=close_db_connection)
            )
        )
        .add_task(
            level=1,
            task=AsyncTask(
                name="FeatureFlags",
                pre_execute=TaskFunction(function=load_feature_flags)
            )
        )
        .add_task(
            level=2,
            task=AsyncTask(
                name="UserData",
                execute=TaskFunction(function=fetch_user_data)
            )
        )
        .build()
    )

    # 5. Run the processor (can be reused for multiple concurrent executions)
    try:
        await processor.process_tasks(ctx)
        print("\nWorkflow completed successfully!")
    except* Exception as e:
        print(f"\nWorkflow failed: {e.exceptions[0]}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Important: Cleanup Guarantees & Resource Management

**⚠️ Critical Behavior**: `post_execute` runs for **any task whose `pre_execute` was started**, even if:
- `pre_execute` raised an exception
- `pre_execute` was cancelled (due to a sibling task failure)
- `pre_execute` timed out

This is **by design** for resource management. If your `pre_execute` acquires resources (opens files, database connections, locks), your `post_execute` **must be idempotent** and handle partial initialization.

### Example: Safe Resource Management

```python
@dataclass
class TaskContext:
    lock: asyncio.Lock | None = None
    lock_acquired: bool = False

async def acquire_lock(ctx: TaskContext):
    ctx.lock = await some_lock.acquire()
    # If cancelled here, lock is acquired but flag not set
    ctx.lock_acquired = True

async def release_lock(ctx: TaskContext):
    # ✅ GOOD: Check if we actually acquired the lock
    if ctx.lock_acquired and ctx.lock:
        await ctx.lock.release()
    # ✅ GOOD: Or use try/except for safety
    try:
        if ctx.lock:
            await ctx.lock.release()
    except Exception:
        pass  # Already released or never acquired
```

**Why this matters**: In parallel execution, if one task fails, all other tasks in that level are cancelled. Without guaranteed cleanup, you'd leak resources.

## Execution Model

### Task dependency graph (directed acyclic graph, DAG)

In addition to level-based execution, sdax supports execution driven by a task dependency graph (a directed acyclic graph, DAG), where tasks declare dependencies on other tasks by name. The analyzer groups tasks into waves: a wave is a set of tasks that share the same effective prerequisite tasks and can start together as soon as those prerequisites complete.

- **Waves are start barriers only**: Dependencies remain task-to-task; waves do not depend on waves. A wave becomes ready when all of its prerequisite tasks have completed their `pre_execute` successfully.
- **Phases**:
  - `pre_execute`: scheduled by waves. On the first failure, remaining `pre_execute` tasks are cancelled; any task whose pre was started still gets `post_execute` later.
  - `execute`: runs in a single TaskGroup after all pre phases complete.
  - `post_execute`: runs in reverse dependency order (via reverse graph waves). Cleanup is best-effort; failures are collected without cancelling sibling cleanup.
- **Validation**: The `TaskAnalyzer` validates the graph (cycles, missing deps) at build time.
- **Immutability**: The analyzer output and processors are immutable and safe to reuse across concurrent executions.

Minimal example:

```python
from sdax import AsyncTask, TaskFunction
from sdax.sdax_task_analyser import TaskAnalyzer
from sdax.sdax_core import AsyncDagTaskProcessor

# Define tasks and dependencies by name
analyzer = TaskAnalyzer()
analyzer.add_task(AsyncTask(name="A", pre_execute=TaskFunction(lambda c: ...)), depends_on=())
analyzer.add_task(AsyncTask(name="B", pre_execute=TaskFunction(lambda c: ...)), depends_on=("A",))
analyzer.add_task(AsyncTask(name="C", execute=TaskFunction(lambda c: ...)), depends_on=("A",))
analyzer.add_task(AsyncTask(name="D", post_execute=TaskFunction(lambda c: ...)), depends_on=("B","C"))

# Build immutable analysis and processor
analysis = analyzer.analyze()
processor = AsyncDagTaskProcessor.builder().from_analysis(analysis).build()

# Run with your context object
# await processor.process_tasks(ctx)
```

Key properties:
- Tasks with identical effective prerequisites are grouped into the same wave for `pre_execute` scheduling.
- `execute` runs across all tasks that passed pre, regardless of wave membership.
- `post_execute` uses the reverse dependency graph to order cleanup, running each task's cleanup in isolation and aggregating exceptions.

Failure semantics:
- **If any `pre_execute` fails**: all remaining scheduled `pre_execute` tasks are cancelled; no further pre waves are started; the `execute` phase is skipped; `post_execute` still runs for tasks whose pre was started (and for tasks with no pre) in reverse dependency order; exceptions are aggregated.
- **If any `execute` fails**: other execute tasks continue; `post_execute` runs; exceptions are aggregated.
- **If any `post_execute` fails**: siblings are not cancelled; all eligible cleanup still runs; exceptions are aggregated.
- The final error is an `ExceptionGroup` that may include failures from pre, execute, and post.

### The "Elevator" Pattern

Tasks execute in a strict "elevator up, elevator down" pattern:

```
Level 1: [A-pre, B-pre, C-pre] ─┐
                                 ├─→ (parallel)
Level 2: [D-pre, E-pre] ────────┘
                                 ├─→ (parallel)  
Execute: [A-exec, B-exec, D-exec, E-exec] ─┘
                                 
Teardown: ┌─ [D-post, E-post] (parallel)
          └─ [A-post, B-post, C-post] (parallel)
```

**Key Rules**:
1. Within a level, tasks run **in parallel**
2. Levels execute **sequentially** (level N+1 waits for level N)
3. `execute` phase runs **after all** `pre_execute` phases complete
4. `post_execute` runs in **reverse level order** (LIFO)
5. If **any** task fails, remaining tasks are cancelled but cleanup still runs

### Task Phases

Each task can define up to 3 optional phases:

| Phase | When It Runs | Purpose | Cleanup Guarantee |
|-------|-------------|---------|-------------------|
| `pre_execute` | First, by level | Initialize resources, setup | `post_execute` runs if started |
| `execute` | After all pre_execute | Do main work | `post_execute` runs if pre_execute started |
| `post_execute` | Last, reverse order | Cleanup, release resources | Always runs if pre_execute started |

## Performance

**Benchmarks** (single-threaded, zero-work tasks):

| Python Version | Multi-level | Single Large Level | Framework Overhead |
|----------------|-------------|--------------------|--------------------|
| **Python 3.13** | ~137,000 tasks/sec | ~21,500 tasks/sec | ~7µs per task |
| **Python 3.11** | ~15,000 tasks/sec | ~159 tasks/sec | ~67µs per task |

*Python 3.13 has significantly improved asyncio performance compared to 3.11. Benchmarks show 9x better throughput in many scenarios.*

**Key Observations**:
- **Multi-level execution**: ~79% of raw asyncio performance (Python 3.13)
- **Scalability**: Tested with 1,000+ tasks across 100 levels
- **Real-world performance**: For typical I/O-bound tasks (10ms+), framework overhead is <0.1% and negligible

**When to use**:
- ✅ I/O-bound workflows (database, HTTP, file operations)
- ✅ Complex multi-step operations with dependencies
- ✅ Multiple levels with reasonable task counts (5-50 tasks/level)
- ✅ Tasks where guaranteed cleanup is critical

**When NOT to use**:
- ❌ CPU-bound work (use `ProcessPoolExecutor` instead)
- ❌ Single level with 100+ parallel tasks (use raw `asyncio.TaskGroup`)
- ❌ Simple linear async/await (unnecessary overhead)
- ❌ Ultra high-frequency operations (>100k ops/sec needed)

## Use Cases

### ✅ Perfect For

1. **Complex API Endpoints**
   ```python
   Level 1: [Auth, RateLimit, FeatureFlags]  # Parallel
   Level 2: [FetchUser, FetchPermissions]     # Depends on Level 1
   Level 3: [LoadData, ProcessRequest]        # Depends on Level 2
   ```

2. **Data Pipeline Steps**
   ```python
   Level 1: [OpenDBConnection, OpenFileHandle]
   Level 2: [ReadData, TransformData]
   Level 3: [WriteResults]
   Post: Always close connections/files
   ```

3. **Build/Deploy Systems**
   ```python
   Level 1: [CheckoutCode, ValidateConfig]
   Level 2: [RunTests, BuildArtifacts]
   Level 3: [Deploy, NotifySlack]
   ```

4. **High-Throughput API Server** (Concurrent Execution)
   ```python
   # Build immutable workflow once at startup
   processor = (
       AsyncTaskProcessor.builder()
       .add_task(AsyncTask("Auth", ...), level=1)
       .add_task(AsyncTask("FetchData", ...), level=2)
       .build()
   )
   
   # Reuse processor for thousands of concurrent requests
   @app.post("/api/endpoint")
   async def handle_request(user_id: int):
       ctx = RequestContext(user_id=user_id)
       await processor.process_tasks(ctx)
       return ctx.results
   ```

### ❌ Not Suitable For

- Simple sequential operations (just use `await`)
- Fire-and-forget background tasks (use `asyncio.create_task`)
- Distributed workflows (use Celery, Airflow)
- Event-driven systems (use message queues)

## Error Handling

Tasks can fail at any phase. The framework:
1. **Cancels** remaining tasks at the same level
2. **Runs cleanup** for all tasks that started `pre_execute`
3. **Collects** all exceptions into an `ExceptionGroup`
4. **Raises** the group after cleanup completes

```python
try:
    await processor.process_tasks(ctx)
except* ValueError as eg:
    # Handle specific exception type
    for exc in eg.exceptions:
        print(f"Validation error: {exc}")
except* TimeoutError as eg:
    # Handle timeouts
    for exc in eg.exceptions:
        print(f"Task timed out: {exc}")
except ExceptionGroup as eg:
    # Handle all errors
    print(f"Multiple failures: {eg}")
```

## Advanced Features

### Per-Task Configuration

Each task function can have its own timeout and retry settings:

```python
AsyncTask(
    name="FlakeyAPI",
    execute=TaskFunction(
        function=call_external_api,
        timeout=5.0,         # 5 second timeout (use None for no timeout)
        retries=3,           # Retry 3 times
        initial_delay=1.0,   # Start retries at 1 second (default)
        backoff_factor=2.0   # Exponential backoff: 1s, 2s, 4s
    )
)
```

**Retry Timing Calculation:**
- Each retry delay: `initial_delay * (backoff_factor ** attempt) * uniform(0.5, 1.0)`
- With `initial_delay=1.0`, `backoff_factor=2.0`:
  - First retry: 0.5s to 1.0s (average 0.75s)
  - Second retry: 1.0s to 2.0s (average 1.5s)
  - Third retry: 2.0s to 4.0s (average 3.0s)
- The `uniform(0.5, 1.0)` jitter prevents thundering herd

**Note:** `AsyncTask` and `TaskFunction` are frozen dataclasses, ensuring immutability and thread-safety. Once created, they cannot be modified.

### Shared Context

You define your own context class with typed fields:

```python
@dataclass
class TaskContext:
    user_id: int | None = None
    permissions: list[str] = field(default_factory=list)
    db_connection: Any = None

async def task_a(ctx: TaskContext):
    ctx.user_id = 123  # Set data

async def task_b(ctx: TaskContext):
    user_id = ctx.user_id  # Read data from task_a, with full type hints!
```

**Note**: The context is shared but not thread-safe. Since tasks run in a single asyncio event loop, no locking is needed.

### Concurrent Execution

You can safely run multiple concurrent executions of the same immutable `AsyncTaskProcessor` instance:

```python
# Build immutable processor once at startup
processor = (
    AsyncTaskProcessor.builder()
    .add_task(AsyncTask(...), level=1)
    .build()
)

# Reuse processor for multiple concurrent requests - each with its own context
await asyncio.gather(
    processor.process_tasks(RequestContext(user_id=123)),
    processor.process_tasks(RequestContext(user_id=456)),
    processor.process_tasks(RequestContext(user_id=789)),
)
```

**⚠️ Critical Requirements for Concurrent Execution:**

1. **Context Must Be Self-Contained**
   - Your context must fully contain all request-specific state
   - Do NOT rely on global variables, class attributes, or module-level state
   - Each execution gets its own isolated context instance

2. **Task Functions Must Be Pure (No External Side Effects)**
   - ❌ **BAD**: Writing to shared files, databases, or caches without coordination
   - ❌ **BAD**: Modifying global state or class variables
   - ❌ **BAD**: Using non-isolated external resources
   - ✅ **GOOD**: Reading from the context
   - ✅ **GOOD**: Writing to the context
   - ✅ **GOOD**: Making HTTP requests (each execution independent)
   - ✅ **GOOD**: Database operations with per-execution connections

3. **Example - Safe Concurrent Execution:**

```python
@dataclass
class RequestContext:
    # All request state contained in context
    user_id: int
    db_connection: Any = None
    api_results: dict = field(default_factory=dict)

async def open_db(ctx: RequestContext):
    # Each execution gets its own connection
    ctx.db_connection = await db_pool.acquire()

async def fetch_user_data(ctx: RequestContext):
    # Uses this execution's connection
    ctx.api_results["user"] = await ctx.db_connection.fetch_user(ctx.user_id)

async def close_db(ctx: RequestContext):
    # Cleans up this execution's connection
    if ctx.db_connection:
        await ctx.db_connection.close()

# Safe - each execution isolated
processor.add_task(
    AsyncTask("DB", pre_execute=TaskFunction(open_db), post_execute=TaskFunction(close_db)),
    level=1
)
```

4. **Example - UNSAFE Concurrent Execution:**

```python
# ❌ WRONG - shared state causes race conditions
SHARED_CACHE = {}

async def unsafe_task(ctx: RequestContext):
    # Race condition! Multiple executions writing to same dict
    SHARED_CACHE[ctx.user_id] = await fetch_data(ctx.user_id)  # BAD!
```

**When NOT to use concurrent execution:**
- Your task functions have uncoordinated side effects (file writes, shared caches)
- Your tasks rely on global or class-level state
- Your tasks modify shared resources without proper locking

**When concurrent execution is perfect:**
- Each request has its own isolated resources (DB connections, API clients)
- All state is contained in the context
- Tasks are functionally pure (output depends only on context input)
- High-throughput API endpoints serving independent requests

## Testing

Run the test suite:
```bash
pytest tests/ -v
```

Performance benchmarks:
```bash
python tests/test_performance.py -v
```

Monte Carlo stress testing (runs ~2,750 tasks with random failures):
```bash
python tests/test_monte_carlo.py -v
```

## Comparison to Alternatives

| Feature | sdax | Celery | Airflow | Raw asyncio |
|---------|------|--------|---------|-------------|
| Setup complexity | Minimal | High | Very High | None |
| External dependencies | None | Redis/RabbitMQ | PostgreSQL/MySQL | None |
| Throughput | ~137k tasks/sec | ~500 tasks/sec | ~50 tasks/sec | ~174k ops/sec |
| Overhead | ~7µs/task | Varies | High | Minimal |
| Use case | In-process workflows | Distributed tasks | Complex DAGs | Simple async |
| Guaranteed cleanup | ✅ Yes | ❌ No | ❌ No | Manual |
| Level-based execution | ✅ Yes | ❌ No | ✅ Yes | Manual |

## License

MIT License - see LICENSE file for details.

