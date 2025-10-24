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

## Quick Start

Graph-based (task dependency graph):

```python
import asyncio
from dataclasses import dataclass
from sdax import AsyncTask, TaskFunction
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

# Fluent builder pattern with generic type parameter
processor = (
    AsyncDagTaskProcessor[TaskContext]
    .builder()
    .add_task(
        AsyncTask(
            name="Database", 
            pre_execute=TaskFunction(open_db), 
            post_execute=TaskFunction(close_db)
        ), 
        depends_on=()
    )
    .add_task(
        AsyncTask(name="Auth", pre_execute=TaskFunction(check_auth)), 
        depends_on=("Database",)
    )
    .add_task(
        AsyncTask(name="Flags", pre_execute=TaskFunction(load_feature_flags)), 
        depends_on=("Database",)
    )
    .add_task(
        AsyncTask(name="Fetch", execute=TaskFunction(fetch_user_data)), 
        depends_on=("Auth",)
    )
    .build()
)

# await processor.process_tasks(TaskContext())
```

Elevator adapter (level-based API; builds a graph under the hood):

```python
from sdax import AsyncTaskProcessor, AsyncTask, TaskFunction

processor = (
    AsyncTaskProcessor.builder()
    .add_task(AsyncTask("Database", pre_execute=TaskFunction(open_db), post_execute=TaskFunction(close_db)), level=0)
    .add_task(AsyncTask("Auth", pre_execute=TaskFunction(check_auth)), level=1)
    .add_task(AsyncTask("Flags", pre_execute=TaskFunction(load_feature_flags)), level=1)
    .add_task(AsyncTask("Fetch", execute=TaskFunction(fetch_user_data)), level=2)
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
from sdax import AsyncTask, TaskFunction, RetryableException
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

# Complex dependency graph with fluent builder
processor = (
    AsyncDagTaskProcessor[DatabaseContext]
    .builder()
    .add_task(
        AsyncTask(
            name="ConnectDB", 
            pre_execute=TaskFunction(connect_db, timeout=5.0, retries=2)
        ), 
        depends_on=()
    )
    .add_task(
        AsyncTask(
            name="LoadUser", 
            execute=TaskFunction(load_user, retryable_exceptions=(ConnectionError,))
        ), 
        depends_on=("ConnectDB",)
    )
    .add_task(
        AsyncTask(
            name="LoadCache", 
            execute=TaskFunction(load_cache, timeout=3.0)
        ), 
        depends_on=("ConnectDB",)
    )
    .add_task(
        AsyncTask(
            name="ProcessData", 
            execute=TaskFunction(process_data)
        ), 
        depends_on=("LoadUser", "LoadCache")
    )
    .add_task(
        AsyncTask(
            name="CleanupDB", 
            post_execute=TaskFunction(cleanup_db)
        ), 
        depends_on=("ConnectDB",)
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
- **If any `pre_execute` fails**: all remaining scheduled `pre_execute` tasks are cancelled; no further pre waves are started; the `execute` phase is skipped; `post_execute` still runs for tasks whose pre was started (and for tasks with no pre) in reverse dependency order; exceptions are aggregated.
- **If any `execute` fails**: other execute tasks continue; `post_execute` runs; exceptions are aggregated.
- **If any `post_execute` fails**: siblings are not cancelled; all eligible cleanup still runs; exceptions are aggregated.
- The final error is an `ExceptionGroup` that may include failures from pre, execute, and post.

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

### Task Group Integration

Tasks can access the underlying `SdaxTaskGroup` for creating subtasks:

```python
from sdax import SdaxTaskGroup

async def parent_task(ctx: TaskContext, tg: SdaxTaskGroup):
    # Create subtasks using the task group
    subtask1 = tg.create_task(subtask_a(), name="subtask_a")
    subtask2 = tg.create_task(subtask_b(), name="subtask_b")
    
    # Wait for both subtasks to complete
    result1, result2 = await asyncio.gather(subtask1, subtask2)
    return result1 + result2

AsyncTask(
    name="ParentTask",
    execute=TaskFunction(
        function=parent_task,
        has_task_group_argument=True  # Enable tg parameter
    )
)
```

### Retryable Exceptions

By default, tasks will retry on these exceptions:
- `TimeoutError`
- `ConnectionError` 
- `RetryableException` (custom base class)

You can customize which exceptions trigger retries:

```python
from sdax import RetryableException

class CustomRetryableError(RetryableException):
    pass

TaskFunction(
    function=my_function,
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
    AsyncTask("DB", pre_execute=TaskFunction(open_db), post_execute=TaskFunction(close_db)),
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

