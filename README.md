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

- **Structured Lifecycle**: Enforces a rigid `pre-execute` -> `execute` -> `post-execute` lifecycle for all tasks.
- **Tiered Parallel Execution**: Tasks are grouped into integer "levels." All tasks at a given level are executed in parallel, and the framework ensures all tasks at level `N` complete successfully before level `N+1` begins.
- **Guaranteed Cleanup**: `post-execute` runs for **any task whose `pre-execute` was started**, regardless of whether it succeeded, failed, or was cancelled. This ensures resources are always released.
- **Declarative & Flexible**: Define tasks as simple data classes. Methods for each phase are optional, and each can have its own timeout and retry configuration.
- **Lightweight & Dependency-Free**: Runs directly inside your application's event loop with no external dependencies, schedulers, or databases, with minimal overhead (see Performance section for details).

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
from sdax import AsyncTaskProcessor, AsyncTask, TaskFunction, TaskContext

# 1. Define your task functions
async def check_auth(ctx: TaskContext):
    print("Level 1: Checking authentication...")
    await asyncio.sleep(0.1)
    ctx.data["user_id"] = 123
    print("Auth successful.")

async def load_feature_flags(ctx: TaskContext):
    print("Level 1: Loading feature flags...")
    await asyncio.sleep(0.2)
    ctx.data["flags"] = {"new_api": True}
    print("Flags loaded.")

async def fetch_user_data(ctx: TaskContext):
    print("Level 2: Fetching user data...")
    if not ctx.data.get("user_id"):
        raise ValueError("Auth failed, cannot fetch user data.")
    await asyncio.sleep(0.1)
    print("User data fetched.")

async def close_db_connection(ctx: TaskContext):
    print("Tearing down db connection...")
    await asyncio.sleep(0.05)
    print("Connection closed.")

async def main():
    # 2. Create a processor and a context
    processor = AsyncTaskProcessor()
    ctx = TaskContext()

    # 3. Declaratively define your workflow
    processor.add_task(
        level=1,
        task=AsyncTask(
            name="Authentication",
            pre_execute=TaskFunction(function=check_auth),
            post_execute=TaskFunction(function=close_db_connection)
        )
    )
    processor.add_task(
        level=1,
        task=AsyncTask(
            name="FeatureFlags",
            pre_execute=TaskFunction(function=load_feature_flags)
        )
    )
    processor.add_task(
        level=2,
        task=AsyncTask(
            name="UserData",
            execute=TaskFunction(function=fetch_user_data)
        )
    )

    # 4. Run the processor
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
async def acquire_lock(ctx: TaskContext):
    ctx.data["lock"] = await some_lock.acquire()
    # If cancelled here, lock is acquired but flag not set
    ctx.data["lock_acquired"] = True

async def release_lock(ctx: TaskContext):
    # ✅ GOOD: Check if we actually acquired the lock
    if ctx.data.get("lock_acquired"):
        await ctx.data["lock"].release()
    # ✅ GOOD: Or use try/except for safety
    try:
        if "lock" in ctx.data:
            await ctx.data["lock"].release()
    except Exception:
        pass  # Already released or never acquired
```

**Why this matters**: In parallel execution, if one task fails, all other tasks in that level are cancelled. Without guaranteed cleanup, you'd leak resources.

## Execution Model

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
        timeout=5.0,        # 5 second timeout
        retries=3,          # Retry 3 times
        backoff_factor=2.0  # Exponential backoff: 2s, 4s, 8s
    )
)
```

### Shared Context

The `TaskContext` is shared across all tasks and phases:

```python
async def task_a(ctx: TaskContext):
    ctx.data["user_id"] = 123  # Set data

async def task_b(ctx: TaskContext):
    user_id = ctx.data["user_id"]  # Read data from task_a
```

**Note**: The context is shared but not thread-safe. Since tasks run in a single asyncio event loop, no locking is needed.

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

