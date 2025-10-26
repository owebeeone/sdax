# sdax - Structured Declarative Async eXecution

[![PyPI version](https://badge.fury.io/py/sdax.svg)](https://pypi.org/project/sdax/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub](https://img.shields.io/badge/github-sdax-blue.svg)](https://github.com/owebeeone/sdax)

`sdax` is a lightweight, high-performance, in-process micro-orchestrator for Python's `asyncio`. It is designed to manage complex, tiered, parallel asynchronous tasks with a declarative API, guaranteeing a correct and predictable order of execution.

It is ideal for building the internal logic of a single, fast operation, such as a complex API endpoint, where multiple dependent I/O calls (to databases, feature flags, or other services) must be reliably initialized, executed, and torn down.

**Links:**
- [PyPI Package](https://pypi.org/project/sdax/)
- [GitHub Repository](https://github.com/owebeeone/sdax)
- [Issue Tracker](https://github.com/owebeeone/sdax/issues)

## Key Features

- **Graph-based scheduler (DAG)**: Primary execution model is a task dependency graph (directed acyclic graph). Tasks depend on other tasks by name; the analyzer groups tasks into parallelizable waves.
- **Elevator adapter (levels)**: A level-based API is provided as an adapter that builds a graph under the hood to simulate the classic "elevator" model.
- **Structured Lifecycle**: Rigid `pre-execute` -> `execute` -> `post-execute` lifecycle for all tasks.
- **Guaranteed Cleanup**: `post-execute` runs for any task whose `pre-execute` started (even if failed/cancelled) to ensure resources are released.
- **Immutable Builder Pattern**: Build processors via fluent APIs producing immutable, reusable instances.
- **Concurrent Execution Safe**: Multiple concurrent runs are fully isolated.
- **Declarative & Flexible**: Task functions are frozen dataclasses with optional timeouts/retries and independent per-phase configuration.
- **Lightweight**: Minimal dependencies, minimal overhead (see Performance).

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

## TaskFunction Helper Functions

SDAX provides convenient helper functions to simplify TaskFunction creation:

### `task_func()` - Standard Async Functions
```python
from sdax import task_func

# Simple usage
task = task_func(my_async_function)

# With custom configuration
task = task_func(my_function, timeout=30.0, retries=3, 
                 retryable_exceptions=(ValueError, RuntimeError))
```

### `task_group_task()` - Functions with TaskGroup Access
```python
from sdax import task_group_task

# For functions that need to create subtasks
async def parent_task(ctx, task_group):
    subtask = task_group.create_task(subtask_func(), name="subtask")
    return await subtask

task = task_group_task(parent_task, retries=2)
```

### `task_sync_func()` - Synchronous Function Wrapper
```python
from sdax import task_sync_func

# Wrap synchronous functions for async compatibility
def sync_function(ctx):
    # Quick synchronous work (validation, simple calculations)
    ctx["result"] = ctx.get("input", 0) * 2
    return ctx["result"]

task = task_sync_func(sync_function, retries=1)

# ⚠️ WARNING: Sync functions block the event loop!
# Use only for quick operations, not CPU-intensive work
```

### `join()` - Graph Join Nodes
```python
from sdax import join, AsyncTask, task_func

# Creates an empty "join" node in the graph
# Acts as a synchronization point for multiple dependencies
# A join node has no pre_execute, execute, or post_execute functions

processor = (
    AsyncDagTaskProcessor.builder()
    .add_task(AsyncTask("TaskA", execute=task_func(func_a)), depends_on=())
    .add_task(AsyncTask("TaskB", execute=task_func(func_b)), depends_on=())
    .add_task(join("WaitForBoth"), depends_on=("TaskA", "TaskB"))  # Synchronizes TaskA and TaskB
    .add_task(AsyncTask("TaskC", execute=task_func(func_c)), depends_on=("WaitForBoth",))
    .build()
)
```

### Default Configuration
All helpers provide sensible defaults:
- `timeout=None` (infinite timeout)
- `retries=0` (no retries)
- `initial_delay=1.0` seconds
- `backoff_factor=2.0` (exponential backoff)
- `retryable_exceptions=(TimeoutError, ConnectionError, RetryableException)`

## Quick Start

Graph-based (task dependency graph):

```python
import asyncio
from dataclasses import dataclass
from sdax import AsyncTask, task_func
from sdax.sdax_core import AsyncDagTaskProcessor

@dataclass
class TaskContext:
    db_connection: Any = None
    user_id: int | None = None
    feature_flags: dict | None = None

async def open_db(ctx: TaskContext):
    ctx.db_connection = await create_database_connection()
    print("Database opened")

async def close_db(ctx: TaskContext):
    if ctx.db_connection:
        await ctx.db_connection.close()
        print("Database closed")

async def check_auth(ctx: TaskContext):
    await asyncio.sleep(0.1)
    ctx.user_id = 123

async def load_feature_flags(ctx: TaskContext):
    await asyncio.sleep(0.2)
    ctx.feature_flags = {"new_api": True}

async def fetch_user_data(ctx: TaskContext):
    if not ctx.user_id:
        raise ValueError("Auth failed")
    await asyncio.sleep(0.1)

# Fluent builder pattern with helper functions
processor = (
    AsyncDagTaskProcessor[TaskContext]
    .builder()
    .add_task(
        AsyncTask(
            name="Database", 
            pre_execute=task_func(open_db), 
            post_execute=task_func(close_db)
        ), 
        depends_on=()
    )
    .add_task(
        AsyncTask(name="Auth", pre_execute=task_func(check_auth)), 
        depends_on=("Database",)
    )
    .add_task(
        AsyncTask(name="Flags", pre_execute=task_func(load_feature_flags)), 
        depends_on=("Database",)
    )
    .add_task(
        AsyncTask(name="Fetch", execute=task_func(fetch_user_data)), 
        depends_on=("Auth",)
    )
    .build()
)

# await processor.process_tasks(TaskContext())
```

**Note**: Task names use string keys by default, but SDAX supports generic key types for more complex scenarios (see Advanced Features).

Elevator adapter (level-based API; builds a graph under the hood):

```python
from sdax import AsyncTaskProcessor, AsyncTask, task_func

processor = (
    AsyncTaskProcessor.builder()
    .add_task(AsyncTask("Database", pre_execute=task_func(open_db), post_execute=task_func(close_db)), level=0)
    .add_task(AsyncTask("Auth", pre_execute=task_func(check_auth)), level=1)
    .add_task(AsyncTask("Flags", pre_execute=task_func(load_feature_flags)), level=1)
    .add_task(AsyncTask("Fetch", execute=task_func(fetch_user_data)), level=2)
    .build()
)
# await processor.process_tasks(TaskContext())
```

## Important: Cleanup Guarantees & Resource Management

**Critical Behavior (warning):** `post_execute` runs for **any task whose `pre_execute` was started**, even if:
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
    # GOOD: Check if we actually acquired the lock
    if ctx.lock_acquired and ctx.lock:
        await ctx.lock.release()
    # GOOD: Or use try/except for safety
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

Advanced example with complex dependencies:

```python
from sdax import AsyncTask, task_func, join, RetryableException
from sdax.sdax_core import AsyncDagTaskProcessor

@dataclass
class DatabaseContext:
    connection: Any = None
    user_data: dict = field(default_factory=dict)
    cache: dict = field(default_factory=dict)

async def connect_db(ctx: DatabaseContext):
    ctx.connection = await create_connection()

async def load_user(ctx: DatabaseContext):
    ctx.user_data = await ctx.connection.fetch_user()

async def load_cache(ctx: DatabaseContext):
    ctx.cache = await redis_client.get_cache()

async def process_data(ctx: DatabaseContext):
    # Process user data with cache
    result = process_user_data(ctx.user_data, ctx.cache)
    return result

async def cleanup_db(ctx: DatabaseContext):
    if ctx.connection:
        await ctx.connection.close()

async def save_results(ctx: DatabaseContext):
    # Save processed results
    await ctx.connection.save(ctx.user_data)

async def notify_user(ctx: DatabaseContext):
    # Send notification to user
    await notification_service.send(ctx.user_data["user_id"])

# Complex dependency graph with helper functions
processor = (
    AsyncDagTaskProcessor[DatabaseContext]
    .builder()
    .add_task(
        AsyncTask(
            name="ConnectDB", 
            pre_execute=task_func(connect_db, timeout=5.0, retries=2),
            post_execute=task_func(cleanup_db)
        ), 
        depends_on=()
    )
    .add_task(
        AsyncTask(
            name="LoadUser", 
            execute=task_func(load_user, retryable_exceptions=(ConnectionError,))
        ), 
        depends_on=("ConnectDB",)
    )
    .add_task(
        AsyncTask(
            name="LoadCache", 
            execute=task_func(load_cache, timeout=3.0)
        ), 
        depends_on=("ConnectDB",)
    )
    .add_task(
        AsyncTask(
            name="ProcessData", 
            execute=task_func(process_data)
        ), 
        depends_on=("LoadUser", "LoadCache")
    )
    .add_task(
        join("SyncPoint"),
        depends_on=("LoadUser", "LoadCache", "ProcessData")
    )
    .add_task(
        AsyncTask(
            name="SaveResults", 
            execute=task_func(save_results)
        ), 
        depends_on=("SyncPoint",)
    )
    .add_task(
        AsyncTask(
            name="SendNotification", 
            execute=task_func(notify_user)
        ), 
        depends_on=("SyncPoint",)
    )
    .build()
)

# await processor.process_tasks(DatabaseContext())
```

Key properties:
- Tasks with identical effective prerequisites are grouped into the same wave for `pre_execute` scheduling.
- `execute` runs across all tasks that passed pre, regardless of wave membership.
- `post_execute` uses the reverse dependency graph to order cleanup, running each task's cleanup in isolation and aggregating exceptions.

Failure semantics:
- **If any `pre_execute` fails**: all remaining scheduled `pre_execute` tasks are cancelled; no further pre waves are started; the `execute` phase is skipped; `post_execute` still runs for tasks whose pre was started (plus post-only tasks whose dependency chain completed successfully) in reverse dependency order; exceptions are aggregated.
- **If any `execute` fails**: other execute tasks continue; `post_execute` runs; exceptions are aggregated.
- **If any `post_execute` fails**: siblings are not cancelled; all eligible cleanup still runs; exceptions are aggregated.
- The final error is an `SdaxExecutionError` (an `ExceptionGroup` subclass) that may include failures from pre, execute, and post.

### The "Elevator" Pattern (level adapter)

Tasks execute in a strict "elevator up, elevator down" pattern:

```
Level 1: [A-pre, B-pre, C-pre] --> (parallel)
Level 2: [D-pre, E-pre]        --> (parallel)
Execute: [A-exec, B-exec, D-exec, E-exec]

Teardown:
    [D-post, E-post] (parallel)
    [A-post, B-post, C-post] (parallel)
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

**Benchmarks** (1,000 zero-work tasks, best of 10 runs):

| Python | Raw asyncio (ms) | Single level (ms) | Multi level (ms) | Three phases (ms) |
|--------|-------------------|-------------------|------------------|-------------------|
| 3.14rc | 4.60 | 6.43 | 6.49 | 35.07 |
| 3.13.1 | 4.39 | 6.53 | 6.09 | 36.49 |
| 3.12   | 6.11 | 7.32 | 6.90 | 42.10 |
| 3.11   | 5.11 | 13.38 | 13.04 | 53.37 |

Notes:
- Absolute numbers vary by machine; relative ordering is consistent across runs.
- 3.13+ shows substantial asyncio improvements vs 3.11.
- Overhead remains small vs realistic I/O-bound tasks (10ms+ per op).

**When to use**:
- I/O-bound workflows (database, HTTP, file operations)
- Complex multi-step operations with dependencies
- Multiple levels with a reasonable number of tasks (three or more per level)
- Scenarios where guaranteed cleanup is critical

## Use Cases

### Perfect For

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

### Mixing Synchronous and Asynchronous Functions

You can mix synchronous and asynchronous functions in the same processor, but **be aware that synchronous functions block the event loop** and prevent parallelism:

```python
from sdax import task_func, task_sync_func
import time

def sync_data_processor(ctx):
    # Synchronous CPU-intensive work - BLOCKS the event loop!
    ctx["processed_data"] = []
    for item in ctx.get("raw_data", []):
        # Simulate CPU work (sorting, calculations, etc.)
        time.sleep(0.1)  # This blocks ALL other tasks!
        ctx["processed_data"].append(item * 2)
    ctx["processing_complete"] = True

async def async_api_call(ctx):
    # Asynchronous I/O - allows other tasks to run concurrently
    response = await httpx.get("https://api.example.com/data")
    ctx["api_data"] = response.json()

processor = (
    AsyncTaskProcessor.builder()
    .add_task(AsyncTask("ProcessData", execute=task_sync_func(sync_data_processor)), level=1)
    .add_task(AsyncTask("FetchAPI", execute=task_func(async_api_call)), level=1)
    .build()
)
```

**⚠️ Important Limitations:**
- **Synchronous functions block the entire event loop** - no other tasks can run while they execute
- **Use sync functions only for quick operations** (data validation, simple calculations)
- **For CPU-intensive work**, consider using `asyncio.to_thread()` or separate processes
- **I/O operations should always be async** to maintain parallelism

**✅ Good Use Cases for Sync Functions:**
```python
def validate_user_input(ctx):
    # Quick validation - doesn't block for long
    user_id = ctx.get("user_id")
    if not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("Invalid user ID")
    ctx["validated"] = True

def format_response_data(ctx):
    # Simple data transformation
    ctx["formatted_data"] = {
        "user_id": ctx["user_id"],
        "timestamp": ctx.get("timestamp", "unknown")
    }
```

**❌ Bad Use Cases for Sync Functions:**
```python
def bad_cpu_intensive_work(ctx):
    # DON'T DO THIS - blocks everything!
    for i in range(1000000):
        complex_calculation()  # Blocks all other tasks
    ctx["result"] = "done"

def bad_file_io(ctx):
    # DON'T DO THIS - use async I/O instead
    with open("large_file.txt") as f:
        ctx["data"] = f.read()  # Blocks the event loop
```

### High-Throughput API Server (Concurrent Execution)
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

## Error Handling

Tasks can fail at any phase. The framework:
1. **Cancels** remaining tasks at the same level
2. **Runs cleanup** for all tasks that started `pre_execute`
3. **Collects** all exceptions into an `SdaxExecutionError`
4. **Raises** the group after cleanup completes

```python
from sdax import SdaxExecutionError, flatten_exceptions

try:
    await processor.process_tasks(ctx)
except SdaxExecutionError as exc:
    # Inspect individual failures (pre, execute, or post)
    for leaf in exc.leaf_exceptions():
        if isinstance(leaf, ValueError):
            handle_validation_error(leaf)
        elif isinstance(leaf, asyncio.TimeoutError):
            handle_timeout(leaf)
        else:
            log_exception(leaf)

    # Or use the helper to work with aggregated errors
    failures = flatten_exceptions(exc)
    if any(isinstance(f, CriticalError) for f in failures):
        raise  # escalate
```

`SdaxExecutionError` still subclasses `ExceptionGroup`, so `except* SomeError` clauses remain valid when you prefer PEP 654-style handling. The helper utilities above provide a concise way to drill into nested failures without rewriting flattening logic in every consumer.

## Advanced Features

### Generic Key Types for Task Names

SDAX supports flexible key types for task names through the generic parameter `K`. This allows you to use structured keys instead of just strings, providing better type safety and more expressive task naming.

#### **Default Behavior (String Keys)**
```python
from sdax import AsyncTask, task_func

# Standard string-based task names (backward compatible)
processor = (
    AsyncDagTaskProcessor[TaskContext]
    .builder()
    .add_task(AsyncTask(name="Database", execute=task_func(open_db)), depends_on=())
    .add_task(AsyncTask(name="Auth", execute=task_func(check_auth)), depends_on=("Database",))
    .build()
)
```

#### **Custom Key Types**
```python
from typing import Tuple, Literal
from sdax import AsyncTask, task_func

# Define custom key types
ServiceKey = Tuple[str, str]  # (service_name, operation)
PriorityKey = Tuple[int, str]  # (priority_level, task_name)

# Use structured keys for better organization
processor = (
    AsyncDagTaskProcessor[TaskContext]
    .builder()
    .add_task(AsyncTask(name=("database", "connect"), execute=task_func(open_db)), depends_on=())
    .add_task(AsyncTask(name=("auth", "validate"), execute=task_func(check_auth)), depends_on=(("database", "connect"),))
    .add_task(AsyncTask(name=(1, "high_priority"), execute=task_func(important_task)), depends_on=())
    .build()
)
```

#### **Level Adapter with Structured Keys**
The level adapter uses a special `LevelKey` type internally:
```python
from sdax.sdax_core import LevelKey

# LevelKey = Tuple[int, Literal["below"]] | Tuple[int, Literal["above"]] | str
# This allows the level adapter to create structured level nodes:
# - (0, "below") - entry point for level 0
# - (0, "above") - exit point for level 0  
# - "MyTask" - your actual task names
```

#### **Benefits of Generic Key Types**
- **Type Safety**: Compile-time checking of key types
- **Better Organization**: Structured keys for complex task hierarchies
- **IDE Support**: Better autocompletion and refactoring
- **Expressiveness**: Keys can carry semantic meaning beyond just names
- **Backward Compatibility**: String keys continue to work unchanged

#### **Key Type Constraints**
```python
# K must be Hashable | str
K = TypeVar("K", bound=Hashable | str)

# Valid key types:
valid_keys = [
    "string_key",                    # str
    ("service", "operation"),        # Tuple[str, str] (Hashable)
    (1, "priority"),                 # Tuple[int, str] (Hashable)
    frozenset(["a", "b"]),           # frozenset (Hashable)
]

# Invalid key types:
invalid_keys = [
    ["list", "not_hashable"],        # List (not Hashable)
    {"dict": "not_hashable"},        # Dict (not Hashable)
    set(["set", "not_hashable"]),    # Set (not Hashable)
]
```

### Per-Task Configuration

Each task function can have its own timeout and retry settings:

```python
from sdax import task_func

AsyncTask(
    name="FlakeyAPI",
    execute=task_func(
        call_external_api,
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

### Task Group Integration

Tasks can access the underlying `SdaxTaskGroup` for creating subtasks:

```python
from sdax import task_group_task

async def parent_task(ctx: TaskContext, tg: SdaxTaskGroup):
    # Create subtasks using the task group
    subtask1 = tg.create_task(subtask_a(), name="subtask_a")
    subtask2 = tg.create_task(subtask_b(), name="subtask_b")
    
    # Wait for both subtasks to complete
    result1, result2 = await asyncio.gather(subtask1, subtask2)
    return result1 + result2

AsyncTask(
    name="ParentTask",
    execute=task_group_task(parent_task)  # Helper automatically enables tg parameter
)
```

**⚠️ Important: TaskGroup Behavior**
- **If the parent task returns early** (without awaiting subtasks), the TaskGroup will **wait for all subtasks to complete** before the parent task is considered finished
- **This ensures proper cleanup** and prevents orphaned subtasks
- **Subtasks run concurrently** with each other, but the parent waits for all of them
- **If any subtask fails**, the entire TaskGroup fails (following `asyncio.TaskGroup` semantics)

**Example - Early Return Still Waits:**
```python
async def parent_task(ctx: TaskContext, tg: SdaxTaskGroup):
    # Create long-running subtasks
    subtask1 = tg.create_task(asyncio.sleep(5), name="slow_task")
    subtask2 = tg.create_task(asyncio.sleep(3), name="faster_task")
    
    # Return immediately - but TaskGroup waits for both subtasks!
    return "parent_done"  # This task won't complete until both subtasks finish

# The parent task will take ~5 seconds (waiting for the slowest subtask)
```

### Phase-by-Phase Execution

For advanced scenarios you may want to observe or interleave work between the `pre_execute`, `execute`, and `post_execute`
phases. The task processor exposes an async context manager that yields an `AsyncPhaseRunner`:

```python
from sdax import AsyncTaskProcessor, SdaxExecutionError, flatten_exceptions
from sdax.sdax_core import PhaseId  # Phase identifiers: PRE_EXEC, EXECUTE, POST_EXEC

processor = AsyncTaskProcessor.builder() ... .build()

async with processor.open(ctx) as runner:
    while await runner.run_next():
        match runner.last_phase_id():
            case PhaseId.PRE_EXEC:
                audit_pre_phase(ctx)
            case PhaseId.EXECUTE:
                emit_metrics(ctx)
            case PhaseId.POST_EXEC:
                pass  # cleanup phase complete

    if runner.has_failures():
        try:
            runner.raise_failures()
        except SdaxExecutionError as exc:
            for failure in flatten_exceptions(exc):
                log_failure(failure)  # application-specific handling
```

When the context exits, the runner resumes only the phases that are required for consistency: if no phase has started it
is a no-op, if only `pre_execute` has run it skips `execute` but still performs `post_execute` cleanup, otherwise it
continues through the remaining phases. You can inspect `next_phase_id()`, `last_phase_id()`, `has_failures()`, or call
`failures()` / `raise_failures()` to integrate SDAX orchestration with frameworks that need explicit checkpoints (UI
progress reporting, long-running batch systems, etc.).

### Retryable Exceptions

By default, tasks will retry on these exceptions:
- `TimeoutError`
- `ConnectionError` 
- `RetryableException` (custom base class)

You can customize which exceptions trigger retries:

```python
from sdax import RetryableException, task_func

class CustomRetryableError(RetryableException):
    pass

task_func(
    my_function,
    retries=3,
    retryable_exceptions=(TimeoutError, CustomRetryableError)  # Custom retry logic
)
```

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

**Critical requirements for concurrent execution:**

1. **Context Must Be Self-Contained**
   - Your context must fully contain all request-specific state
   - Do NOT rely on global variables, class attributes, or module-level state
   - Each execution gets its own isolated context instance

2. **Task Functions Must Be Pure (No External Side Effects)**
   - BAD: Writing to shared files, databases, or caches without coordination
   - BAD: Modifying global state or class variables
   - BAD: Using non-isolated external resources
   - GOOD: Reading from the context
   - GOOD: Writing to the context
   - GOOD: Making HTTP requests (each execution independent)
   - GOOD: Database operations with per-execution connections

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
    AsyncTask("DB", pre_execute=task_func(open_db), post_execute=task_func(close_db)),
    level=1
)
```

4. **Example - UNSAFE Concurrent Execution:**

```python
# BAD - shared state causes race conditions
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
pytest sdax/tests -v
```

Performance benchmarks:
```bash
python sdax/tests/test_performance.py -v
```

Monte Carlo stress testing (runs ~2,750 tasks with random failures):
```bash
python sdax/tests/test_monte_carlo.py -v
```

## Comparison to Alternatives

| Feature | sdax | Celery | Airflow | Raw asyncio |
|---------|------|--------|---------|-------------|
| Setup complexity | Minimal | High | Very High | None |
| External dependencies | None | Redis/RabbitMQ | PostgreSQL/MySQL | None |
| Throughput | ~137k tasks/sec | ~500 tasks/sec | ~50 tasks/sec | ~174k ops/sec |
| Overhead | ~7us/task | Varies | High | Minimal |
| Use case | In-process workflows | Distributed tasks | Complex DAGs | Simple async |
| Guaranteed cleanup | Yes | No | No | Manual |
| Level-based execution | Yes | No | Yes | Manual |

## License

MIT License - see LICENSE file for details.

