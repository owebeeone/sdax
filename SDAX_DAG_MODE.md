# SDAX DAG Mode Specification

## Overview

### Wave model (current analyzer implementation)

The current `TaskAnalyzer` builds pre/post execution graphs as an ordered list of waves. Important clarifications for the present implementation:

- A "wave" is a scheduling unit. The analyzer now groups tasks that share identical effective dependencies into the same wave by default (e.g., multiple roots with no deps share a wave). Parallelism is unchanged; grouping only affects graph shape.
- Tasks without a given phase function (e.g., no `pre_execute`) do not form barriers for that phase and are treated as pass-through nodes when computing effective dependencies.
- Grouping multiple tasks with identical effective dependencies into the same wave is a valid optimization, but is not performed by default in the analyzer. The tests rely on the non-coalesced representation (one task per wave) while still permitting parallel execution across waves.
- For node semantics today, a "node" for the pre-execute graph is any task without `pre_execute`. In practice (due to `AsyncTask` validation), such a node may still define `execute` only; it still does not create a pre-execute barrier.

The runtime engine can execute multiple independent waves concurrently and runs all tasks within a wave in parallel as well. Whether tasks are coalesced into the same wave or kept separate does not change correctness or achievable parallelism; it only changes the shape of the analysis graph.

- Each task appears in exactly one wave.
- A wave acts as a barrier over its `depends_on` waves: it can start only after all those predecessor waves complete. Multiple waves can depend on the same predecessor set and become ready concurrently.

This document specifies a dependency-graph-based variant of sdax that allows tasks to declare explicit dependencies on other tasks by name, rather than being organized into integer levels.

**Current sdax (level-based):**
```python
builder.add_task(task=taskA, level=1)
builder.add_task(task=taskB, level=2)  # Runs after all level 1 tasks
builder.add_task(task=taskC, level=2)  # Runs in parallel with taskB
```

**Proposed sdax DAG mode (dependency-based):**
```python
builder.add_task(task=taskA, depends_on=())
builder.add_task(task=taskB, depends_on=('taskA',))  # Runs after taskA
builder.add_task(task=taskC, depends_on=('taskA', 'taskD'))  # If taskD finishes, run in parallel with taskB 
builder.add_task(task=taskD, depends_on=())
```

**Node shortcuts** - For organizational purposes, you can add nodes that have no actual work:
```python
builder.add_task(task=taskA, depends_on=())
builder.add_task(task=taskB, depends_on=('taskA',))  # Runs after taskA
builder.add_task(task=taskC, depends_on=('taskD',))  # Runs in parallel with taskB 
builder.add_node('taskD', depends_on=('taskA',))    # Node: no actual work, just a dependency point
```

A node is literally just a `DagAsyncTask` with all functions set to `None`. It completes immediately after its dependencies complete, and is useful for:
- **Dependency grouping**: Instead of listing many dependencies, depend on a single node
- **Logical milestones**: Mark phases like "all_data_ready" or "validation_complete"
- **Future extension**: Add nodes now, implement later without changing dependents

## Nodes: Dependency Grouping and Milestones

A **node** is a task with no actual work (all functions are `None`). Nodes are powerful organizational tools in complex DAGs.

### Use Cases for Nodes

#### 1. Dependency Grouping (Fan-in Pattern)

Instead of every dependent task listing all its dependencies:

```python
# Without nodes - repetitive
builder.add_task(fetch_users, depends_on=())
builder.add_task(fetch_orders, depends_on=())
builder.add_task(fetch_products, depends_on=())
builder.add_task(validate_data, depends_on=('fetch_users', 'fetch_orders', 'fetch_products'))
builder.add_task(analyze_data, depends_on=('fetch_users', 'fetch_orders', 'fetch_products'))
builder.add_task(export_data, depends_on=('fetch_users', 'fetch_orders', 'fetch_products'))

# With nodes - clean
builder.add_task(fetch_users, depends_on=())
builder.add_task(fetch_orders, depends_on=())
builder.add_task(fetch_products, depends_on=())
builder.add_node('all_data_ready', depends_on=('fetch_users', 'fetch_orders', 'fetch_products'))
builder.add_task(validate_data, depends_on=('all_data_ready',))
builder.add_task(analyze_data, depends_on=('all_data_ready',))
builder.add_task(export_data, depends_on=('all_data_ready',))
```

#### 2. Logical Milestones

Mark important phases in your workflow:

```python
builder.add_task(authenticate, depends_on=())
builder.add_task(load_config, depends_on=())
builder.add_node('initialization_complete', depends_on=('authenticate', 'load_config'))

builder.add_task(fetch_data, depends_on=('initialization_complete',))
builder.add_task(validate_input, depends_on=('initialization_complete',))
builder.add_node('data_ready', depends_on=('fetch_data', 'validate_input'))

builder.add_task(process, depends_on=('data_ready',))
builder.add_node('processing_complete', depends_on=('process',))

builder.add_task(commit_db, depends_on=('processing_complete',))
builder.add_task(send_notifications, depends_on=('processing_complete',))
```

#### 3. Future Extension Points

Add nodes now, implement later without changing dependents:

```python
# Initial version - no caching
builder.add_task(fetch_from_api, depends_on=())
builder.add_node('data_acquired', depends_on=('fetch_from_api',))  # Extension point
builder.add_task(process_data, depends_on=('data_acquired',))

# Later - add caching without changing process_data
builder.add_task(fetch_from_api, depends_on=())
builder.add_task(check_cache, depends_on=())
builder.add_node('data_acquired', depends_on=('fetch_from_api', 'check_cache'))
builder.add_task(process_data, depends_on=('data_acquired',))  # Unchanged!
```

#### 4. Complex Coordination

Model complex business logic:

```python
# E-commerce order processing
builder.add_task(check_inventory, depends_on=())
builder.add_task(validate_payment, depends_on=())
builder.add_task(verify_address, depends_on=())
builder.add_node('order_validated', depends_on=('check_inventory', 'validate_payment', 'verify_address'))

builder.add_task(reserve_inventory, depends_on=('order_validated',))
builder.add_task(charge_payment, depends_on=('order_validated',))
builder.add_node('order_committed', depends_on=('reserve_inventory', 'charge_payment'))

builder.add_task(create_shipment, depends_on=('order_committed',))
builder.add_task(send_confirmation, depends_on=('order_committed',))
builder.add_task(update_analytics, depends_on=('order_committed',))
```

### Performance Characteristics

Nodes have **zero overhead** at runtime:
- They don't run any functions (pre/execute/post are all `None`)
- They're skipped during execution phases
- They only affect the dependency graph structure (computed once at build time)
- Level computation treats them like any other task

The only cost is a small amount of memory for the node object itself.

## Motivation

### Advantages of DAG Mode

1. **Finer-Grained Parallelism**
   - Level-based: All tasks at level N must complete before ANY task at level N+1 starts
   - DAG-based: Each task starts as soon as ITS dependencies complete
   - Example: If taskA takes 10s and taskB takes 0.1s, taskC (depending only on taskB) waits 10s in level mode but only 0.1s in DAG mode

2. **Explicit Dependencies**
   - Makes task relationships clear and self-documenting
   - Easier to reason about complex workflows
   - IDE can visualize the dependency graph

3. **Dynamic Workflows**
   - Tasks can be added/removed without renumbering levels
   - Easier to compose from multiple sources
   - Better for code generation scenarios

4. **Natural Modeling**
   - Many real-world workflows are naturally DAGs (build systems, data pipelines)
   - Level-based is an approximation of DAG-based

### When to Use Each Mode

**Use Level-Based (current sdax) when:**
- You have clear, coarse-grained phases (e.g., AUTH -> FETCH -> PROCESS -> COMMIT)
- Simplicity is more important than maximum parallelism
- You want to minimize coordination overhead
- All tasks in a phase have similar duration

**Use DAG-Based when:**
- Tasks have fine-grained dependencies
- Maximum parallelism is critical
- Task durations vary significantly
- You're modeling a natural DAG (e.g., build system, data pipeline)
- Dependencies are computed dynamically

## API Design

### Core Classes

```python
from sdax import DagAsyncTask, DagAsyncTaskProcessor

# TaskFunction remains unchanged (timeout, retries, initial_delay, backoff_factor)
from sdax import TaskFunction

@dataclass(frozen=True)
class DagAsyncTask:
    """A task in a dependency graph.
    
    Identical to AsyncTask except it uses a name-based dependency system
    instead of integer levels.
    """
    name: str  # Unique identifier for this task
    pre_execute: TaskFunction | None = None
    execute: TaskFunction | None = None
    post_execute: TaskFunction | None = None
    
    def __post_init__(self):
        # Note: In DAG mode, tasks with all None functions (nodes) are valid
        # They serve as dependency grouping points and logical milestones
        pass


@dataclass
class DagAsyncTaskProcessorBuilder:
    """Builder for DAG-based task processor."""
    
    tasks: Dict[str, DagAsyncTask] = field(default_factory=dict)
    dependencies: Dict[str, tuple[str, ...]] = field(default_factory=dict)
    
    def add_task(
        self,
        task: DagAsyncTask,
        depends_on: tuple[str, ...] = ()
    ) -> 'DagAsyncTaskProcessorBuilder':
        """Add a task with its dependencies.
        
        Args:
            task: The task to add
            depends_on: Tuple of task names this task depends on
        
        Returns:
            Self for fluent chaining
        
        Raises:
            ValueError: If task name already exists
            ValueError: If depends_on references non-existent tasks (checked at build())
        """
        if task.name in self.tasks:
            raise ValueError(f"Task '{task.name}' already exists")
        
        self.tasks[task.name] = task
        self.dependencies[task.name] = depends_on
        return self
    
    def add_node(
        self,
        name: str,
        depends_on: tuple[str, ...] = ()
    ) -> 'DagAsyncTaskProcessorBuilder':
        """Add a node (task with no functions) for dependency grouping.
        
        A node is just a DagAsyncTask with all functions set to None.
        It completes immediately after its dependencies and is useful for:
        - Grouping multiple dependencies under a single name
        - Marking logical milestones in the workflow
        - Creating extension points for future implementation
        
        Args:
            name: Unique name for this node
            depends_on: Tuple of task names this node depends on
        
        Returns:
            Self for fluent chaining
        
        Example:
            # Create a milestone node
            builder.add_task(fetch_users, depends_on=())
            builder.add_task(fetch_orders, depends_on=())
            builder.add_task(fetch_products, depends_on=())
            builder.add_node('all_data_ready', depends_on=('fetch_users', 'fetch_orders', 'fetch_products'))
            
            # Now other tasks can depend on the milestone
            builder.add_task(process_data, depends_on=('all_data_ready',))
        """
        node = DagAsyncTask(
            name=name,
            pre_execute=None,
            execute=None,
            post_execute=None
        )
        return self.add_task(node, depends_on)
    
    def build(self) -> 'DagAsyncTaskProcessor':
        """Build the immutable processor.
        
        Validates:
        1. All dependencies reference existing tasks
        2. No cycles in the dependency graph
        3. Graph is a valid DAG
        
        Raises:
            ValueError: If validation fails
        """
        # Validation
        _validate_dependencies(self.tasks, self.dependencies)
        _detect_cycles(self.dependencies)
        
        # Compute dependency metadata for real-time scheduling
        dependents, in_degree = _compute_dependency_metadata(self.dependencies)
        
        # Optional: Optimize by eliding nodes (if enabled)
        # optimized_deps = _elide_nodes(self.tasks, self.dependencies)
        
        # Build execution graphs
        pre_exec_graph = _build_pre_execute_graph(self.tasks, self.dependencies)
        post_exec_graph = _build_post_execute_graph(self.tasks, self.dependencies)
        
        return DagAsyncTaskProcessor(
            tasks=frozendict(self.tasks),
            dependencies=frozendict(self.dependencies),
            pre_execute_graph=pre_exec_graph,
            post_execute_graph=post_exec_graph,
        )


@datatree(frozen=True)
class DagAsyncTaskProcessor:
    """Immutable DAG-based task processor.
    
    Executes tasks using pre-computed execution graphs for maximum parallelism.
    """
    
    tasks: frozendict[str, DagAsyncTask]
    dependencies: frozendict[str, tuple[str, ...]]
    pre_execute_graph: ExecutionGraph  # Pre-computed waves for pre_execute
    post_execute_graph: ExecutionGraph  # Pre-computed waves for post_execute (reverse)
    
    @staticmethod
    def builder() -> DagAsyncTaskProcessorBuilder:
        """Create a new builder."""
        return DagAsyncTaskProcessorBuilder()
    
    async def process_tasks(self, ctx: T):
        """Execute the DAG using pre-computed execution graphs.
        
        Execution model:
        1. Execute pre_execute waves sequentially (tasks within wave run parallel)
        2. Execute all successful tasks in parallel (execute phase)
        3. Execute post_execute waves sequentially in reverse order
        4. All phases exploit maximum parallelism via wave grouping
        
        No runtime coordination needed - graphs pre-computed at build time!
        """
        # Implementation: See Phase 2, 3, 4 sections below
```

### Usage Examples

```python
from sdax import DagAsyncTask, DagAsyncTaskProcessor, TaskFunction

# Define tasks
auth_task = DagAsyncTask(
    name="auth",
    pre_execute=TaskFunction(function=check_auth)
)

db_task = DagAsyncTask(
    name="db",
    pre_execute=TaskFunction(function=connect_db),
    post_execute=TaskFunction(function=close_db)
)

user_task = DagAsyncTask(
    name="user",
    execute=TaskFunction(function=fetch_user)
)

order_task = DagAsyncTask(
    name="order",
    execute=TaskFunction(function=fetch_orders)
)

# Build dependency graph
#
#       auth ──┬──> user
#              │
#       db ────┴──> order
#
# Real-time execution:
#   - Start auth and db (parallel)
#   - When BOTH auth and db complete → start user and order (parallel)
#
# This is the same as level-based for this simple graph, but differs for complex ones!
#
processor = (
    DagAsyncTaskProcessor.builder()
    .add_task(auth_task, depends_on=())
    .add_task(db_task, depends_on=())
    .add_task(user_task, depends_on=('auth', 'db'))
    .add_task(order_task, depends_on=('auth', 'db'))
    .build()
)

# Execute (same as current sdax)
await processor.process_tasks(ctx)

# Concurrent execution (same as current sdax)
await asyncio.gather(
    processor.process_tasks(ctx1),
    processor.process_tasks(ctx2),
    processor.process_tasks(ctx3),
)
```

## Execution Model: Real-Time Dependency Scheduling

Unlike the level-based approach, DAG mode uses **real-time dependency tracking**: tasks start as soon as their dependencies complete, without waiting for an entire "level" to finish.

### Why Not Levels?

Consider this graph:
```
A ─┬─> B ─> E ─┐
   └─> C ─> D ─┴─> F
```

**Level-based execution** (suboptimal):
- Level 0: A
- Level 1: B, C (parallel, but E must wait for C to finish even though B finished first!)
- Level 2: E, D (parallel)
- Level 3: F

If B completes in 1ms but C takes 100ms, E waits 100ms unnecessarily.

**Real-time DAG scheduling** (optimal):
- Start A
- When A completes → start B and C (parallel)
- When B completes → start E immediately (don't wait for C!)
- When C completes → start D immediately (don't wait for B/E!)
- When both E and D complete → start F

This maximizes parallelism by exploiting **independent chains**.

### Phase 1: Build Execution Graphs at Build Time

Instead of runtime coordination with `asyncio.Event`, we **pre-compute execution graphs** during `.build()`:

1. **Validation**:
   - All task names are unique
   - All dependencies reference existing tasks
   - No cycles (use DFS)

2. **Build Pre-Execute Graph**:
   - Group tasks by their **transitive dependency closure**
   - Only include tasks that have `pre_execute` functions
   - Tasks with no `pre_execute` are "instant completion" nodes
   - Result: Graph of **execution waves** (collections of tasks that run together)

3. **Build Execute Graph**:
   - Simple: just collect all tasks with `execute` functions
   - All run in parallel after pre-execute completes

4. **Build Post-Execute Graph (Reverse)**:
   - Only include tasks that have `post_execute` functions
   - Build reverse dependency graph
   - Group by reverse transitive dependency closure
   - Result: Graph of **cleanup waves** in reverse order

**Key insight**: Tasks without a phase function don't create synchronization barriers!

### Example: Build-Time Graph Construction

Consider these tasks and dependencies:

```python
# Tasks with (pre_execute, post_execute) functions
A = (F1, None)    # pre_execute only
B = (F2, F3)      # both
C = (None, F4)    # post_execute only (milestone node with cleanup)
D = (F5, F6)      # both
E = (F7, None)    # pre_execute only

# Dependencies
A -> B
A -> C
C -> E
B -> D
```

**Pre-Execute Graph** (using effective dependencies through nodes):

The algorithm computes **effective dependencies** by following through nodes:
- A: no dependencies → wave 0
- B: depends on A (wave 0) → wave 1
- E: depends on C, C depends on A → E effectively depends on A (wave 0) → wave 1
- D: depends on B (wave 1) → wave 2

Result:
```
Wave 0: [A]           # depends_on=()
Wave 1: [B, E]        # depends_on=(0,), (0,)
                      # B directly depends on A
                      # E depends on C (node), C depends on A → E effectively depends on A
Wave 2: [D]           # depends_on=(1,) - depends on B
```

Note: The current analyzer typically emits one task per wave. Conceptually, this example shows that B and E become available after wave 0; in analyzer output they would appear as two independent waves whose dependencies both include wave 0, and they can run in parallel.

With precise wave dependencies tracked, parallel execution is:
1. Execute wave 0: [A]
2. When wave 0 completes → execute wave 1: [B, E] **in parallel**
3. When wave 1 completes → execute wave 2: [D]

Key: C (the node) doesn't create a synchronization barrier between waves!

**Execute Graph**:
```
All tasks with execute functions run in parallel (none in this example)
```

**Post-Execute Graph** (reverse, only tasks with `post_execute`):
```
Wave 0: [D]           # depends_on=() - leaf (no dependents with post_execute)
Wave 1: [B, C]        # depends_on=(0,), ()
                      # B depends on wave 0 (D must cleanup first)
                      # C is independent (E has no post_execute)
```

**At runtime**:
- Each wave uses `asyncio.TaskGroup` to run tasks in parallel
- If a task fails, cancel current wave and propagate "skip" to future waves
- Track which tasks started (for post_execute)
- Post-execute waves run in reverse, only for started tasks

**Data structure**:
```python
@dataclass
class ExecutionWave:
    """A collection of tasks that can execute together."""
    tasks: tuple[str, ...]  # Task names
    depends_on: tuple[int, ...]  # Wave indices this wave depends on
    
@dataclass
class ExecutionGraph:
    """Pre-computed execution graph."""
    waves: tuple[ExecutionWave, ...]  # Ordered waves
    # Wave 0 has no dependencies, wave N depends on earlier waves
```

### Phase 2: Pre-Execute (Parallel Wave Execution)

Execute waves in parallel when they have independent dependencies:

```python
# Pseudo-code for parallel wave execution

started = set()     # Tasks whose pre_execute was started (need cleanup)
completed = set()   # Tasks whose pre_execute completed successfully
failed = set()      # Tasks whose pre_execute failed
completed_waves = set()  # Wave indices that completed
should_cancel = False    # Set to True when a task fails

# Helper to execute a single wave
async def execute_wave(wave_idx: int):
    if should_cancel:
        return  # Early exit
    
    wave = pre_execute_graph.waves[wave_idx]
    wave_tasks = []
    
    # Check each task in the wave
    for task_name in wave.tasks:
        task = tasks[task_name]
        
        # Check if all task dependencies completed successfully
        deps_ok = all(dep in completed for dep in dependencies[task_name])
        
        if not deps_ok:
            continue  # Skip this task (dependency failed)
        
        # This task can run
        wave_tasks.append(task)
        started.add(task_name)
    
    # Run all tasks in this wave in parallel
    if wave_tasks:
        try:
            async with asyncio.TaskGroup() as tg:
                for task in wave_tasks:
                    tg.create_task(_execute_phase(task, 'pre_execute', ctx))
            
            # All tasks in wave completed successfully
            for task in wave_tasks:
                completed.add(task.name)
        
        except* Exception as eg:
            # Some tasks failed - mark them
            nonlocal should_cancel
            should_cancel = True
            
            for task in wave_tasks:
                if task_failed(task):
                    failed.add(task.name)
                else:
                    completed.add(task.name)
    
    completed_waves.add(wave_idx)

# Execute waves in parallel as dependencies allow
while len(completed_waves) < len(pre_execute_graph.waves) and not should_cancel:
    # Find all waves whose dependencies are satisfied
    ready_waves = [
        idx for idx in range(len(pre_execute_graph.waves))
        if idx not in completed_waves
        and all(dep_idx in completed_waves 
                for dep_idx in pre_execute_graph.waves[idx].depends_on)
    ]
    
    if not ready_waves:
        break  # No progress possible (shouldn't happen with valid DAG)
    
    # Execute all ready waves IN PARALLEL
    async with asyncio.TaskGroup() as tg:
        for wave_idx in ready_waves:
            tg.create_task(execute_wave(wave_idx))
```

**Key properties**:
1. **Independent waves execute in parallel** (maximum parallelism)
2. **Tasks within a wave execute in parallel** via `TaskGroup`
3. **Wave dependencies** are explicitly tracked (not just sequential order)
4. **Failure propagation**: `should_cancel` flag stops future waves
5. All started tasks are tracked for cleanup

**Example**: Consider this graph:
```
A -> B -> D
C -> E
```

Waves:
- Wave 0: [A] (no dependencies)
- Wave 1: [C] (no dependencies)  # Separate wave, but can run in parallel with wave 0
- Wave 1: [B] (depends on wave 0 - specifically A)
- Wave 2: [E] (depends on wave 0 - specifically C)
- Wave 3: [D] (depends on wave 1)

**Execution order**:
1. Start waves 0 and 1 in parallel (A and C run concurrently)
2. When wave 0 completes → wave 2 (B) may be ready; when wave 1 completes → wave 3 (E) may be ready. B and E can run in parallel when their prerequisites complete.
3. When wave 1 completes → execute D

This exploits true parallelism: B starts as soon as A finishes, without waiting for C/E!

**Key invariant**: A task's pre_execute only runs if ALL its dependencies' pre_execute completed successfully.

### Phase 3: Execute (Parallel Across All Tasks)

```
# All tasks that completed pre_execute successfully can run execute in parallel
# Nodes (no execute function) are skipped automatically

completed_tasks = [task for task in started if task in completed and task.execute]

async with asyncio.TaskGroup() as tg:
    for task in completed_tasks:
        tg.create_task(_execute_phase(task, 'execute', ctx))
```

This is the same as current sdax - once setup is done, all execution happens in parallel.

### Phase 4: Post-Execute (Parallel Wave Cleanup)

Execute cleanup waves in parallel when they have independent dependencies:

```python
# Pseudo-code for parallel wave cleanup

cleanup_exceptions = []
completed_waves = set()  # Wave indices that completed

# Helper to execute cleanup for a single wave
async def cleanup_wave(wave_idx: int):
    wave = post_execute_graph.waves[wave_idx]
    wave_tasks = []
    
    # Check each task in the wave
    for task_name in wave.tasks:
        task = tasks[task_name]
        
        # Only cleanup tasks that were started
        if task_name not in started:
            continue
        
        # This task needs cleanup
        wave_tasks.append(task)
    
    # Run all cleanups in this wave in parallel
    # Use isolated TaskGroups per task for exception isolation
    if wave_tasks:
        async def cleanup_isolated(task):
            """Run post_execute in isolated TaskGroup."""
            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(_execute_phase(task, 'post_execute', ctx))
            except* Exception as eg:
                return eg
            return None
        
        # Use gather (not TaskGroup) to isolate exceptions
        results = await asyncio.gather(
            *[cleanup_isolated(task) for task in wave_tasks],
            return_exceptions=True
        )
        
        # Collect exceptions but continue cleanup
        for result in results:
            if result is not None and isinstance(result, BaseException):
                cleanup_exceptions.append(result)
    
    completed_waves.add(wave_idx)

# Execute cleanup waves in parallel as dependencies allow
while len(completed_waves) < len(post_execute_graph.waves):
    # Find all waves whose dependencies are satisfied
    ready_waves = [
        idx for idx in range(len(post_execute_graph.waves))
        if idx not in completed_waves
        and all(dep_idx in completed_waves 
                for dep_idx in post_execute_graph.waves[idx].depends_on)
    ]
    
    if not ready_waves:
        break  # No progress possible
    
    # Execute all ready cleanup waves IN PARALLEL
    async with asyncio.TaskGroup() as tg:
        for wave_idx in ready_waves:
            tg.create_task(cleanup_wave(wave_idx))
```

**Key properties**:
1. **Independent cleanup waves execute in parallel** (maximum parallelism)
2. **Tasks within a wave execute in parallel** via `gather` (exception isolation)
3. **Wave dependencies** ensure correct cleanup order
4. **Exceptions are isolated** (one failure doesn't cancel others)
5. Only cleans up tasks that actually started

**Example**: Consider this cleanup graph:
```
Forward: A -> B -> D
         C -> E

# Suppose all started, B and D have post_execute
```

Post-execute waves (reverse):
- Wave 0: [D] (leaf, no dependents with post_execute)
- Wave 1: [B] (depends on wave 0 - D must cleanup first)

But if E also had post_execute:
- Wave 0: [D, E] (both leaves)  ← **These clean up IN PARALLEL**
- Wave 1: [B] (depends on wave 0)

This exploits true parallelism: independent cleanup branches don't block each other!

### Phase 5: Exception Aggregation

```
Collect all exceptions from:
    - Pre-execute failures
    - Execute failures
    - Post-execute failures

Raise as ExceptionGroup if multiple, or single exception if only one
```

## Algorithm Details

### Cycle Detection

Use DFS with three states per node:
- WHITE: Not visited
- GRAY: Currently visiting (in DFS stack)
- BLACK: Fully visited

If we encounter a GRAY node, we have a cycle.

```python
def _detect_cycles(dependencies: Dict[str, tuple[str, ...]]) -> None:
    """Detect cycles in dependency graph.
    
    Raises:
        ValueError: If a cycle is detected, with the cycle path
    """
    color = {task: 'WHITE' for task in dependencies}
    parent = {task: None for task in dependencies}
    
    def dfs(node: str, path: list[str]) -> None:
        color[node] = 'GRAY'
        path.append(node)
        
        for dep in dependencies.get(node, ()):
            if color[dep] == 'GRAY':
                # Cycle detected
                cycle_start = path.index(dep)
                cycle = path[cycle_start:] + [dep]
                raise ValueError(f"Cycle detected: {' -> '.join(cycle)}")
            elif color[dep] == 'WHITE':
                parent[dep] = node
                dfs(dep, path)
        
        color[node] = 'BLACK'
        path.pop()
    
    for task in dependencies:
        if color[task] == 'WHITE':
            dfs(task, [])
```

### Topological Sort

Use Kahn's algorithm (BFS-based):

```python
def _topological_sort(dependencies: Dict[str, tuple[str, ...]]) -> list[str]:
    """Compute topological ordering of tasks.
    
    Returns:
        List of task names in valid execution order
    """
    # Build reverse dependency graph (dependents)
    dependents = defaultdict(list)
    in_degree = {task: 0 for task in dependencies}
    
    for task, deps in dependencies.items():
        in_degree[task] = len(deps)
        for dep in deps:
            dependents[dep].append(task)
    
    # Start with tasks that have no dependencies
    queue = deque([task for task, deg in in_degree.items() if deg == 0])
    result = []
    
    while queue:
        task = queue.popleft()
        result.append(task)
        
        for dependent in dependents[task]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
    
    return result
```

### Graph Building Algorithms

**Build Pre-Execute Graph**:

Group tasks by their transitive dependency closure, only including tasks with `pre_execute`:

```python
def _build_pre_execute_graph(
    tasks: Dict[str, DagAsyncTask],
    dependencies: Dict[str, tuple[str, ...]]
) -> ExecutionGraph:
    """Build execution waves for pre_execute phase.
    
    Tasks in the same wave have:
    1. All dependencies satisfied by earlier waves
    2. pre_execute functions defined
    
    Tasks without pre_execute are treated as "instant completion"
    and don't create synchronization barriers.
    """
    # Filter tasks with pre_execute
    pre_exec_tasks = {
        name: task for name, task in tasks.items() 
        if task.pre_execute is not None
    }
    
    if not pre_exec_tasks:
        return ExecutionGraph(waves=())
    
    # Compute effective dependencies: transitive deps through nodes
    # For each task, find all dependencies with pre_execute (following through nodes)
    def get_effective_deps(task_name: str, visited: set = None) -> set[str]:
        """Get all transitive dependencies that have pre_execute."""
        if visited is None:
            visited = set()
        if task_name in visited:
            return set()
        visited.add(task_name)
        
        effective = set()
        for dep in dependencies.get(task_name, ()):
            if dep in pre_exec_tasks:
                # This dependency has pre_execute
                effective.add(dep)
            else:
                # This dependency is a node - follow through it
                effective.update(get_effective_deps(dep, visited))
        return effective
    
    # Compute wave assignments
    # Wave number = 1 + max(wave of effective dependencies with pre_execute)
    task_wave = {}
    waves_dict = defaultdict(list)
    
    # Use topological sort to process in order
    topo_order = _topological_sort(dependencies)
    
    for task_name in topo_order:
        if task_name not in pre_exec_tasks:
            # No pre_execute - skip
            continue
        
        # Find max wave of effective dependencies
        effective_deps = get_effective_deps(task_name)
        dep_waves = [task_wave[dep] for dep in effective_deps if dep in task_wave]
        
        # This task's wave
        wave = 0 if not dep_waves else (max(dep_waves) + 1)
        task_wave[task_name] = wave
        waves_dict[wave].append(task_name)
    
    # Compute precise wave dependencies (not just "all earlier waves")
    # For each wave, find which specific waves it depends on
    wave_dependencies = {}
    for wave_num in sorted(waves_dict.keys()):
        deps = set()
        for task_name in waves_dict[wave_num]:
            # Find waves that contain this task's dependencies (with pre_execute)
            for dep in dependencies.get(task_name, ()):
                if dep in task_wave:  # Dependency has pre_execute
                    dep_wave = task_wave[dep]
                    if dep_wave < wave_num:  # Must be earlier wave
                        deps.add(dep_wave)
        wave_dependencies[wave_num] = tuple(sorted(deps))
    
    # Convert to ExecutionWave objects
    waves = []
    for wave_num in sorted(waves_dict.keys()):
        wave = ExecutionWave(
            tasks=tuple(waves_dict[wave_num]),
            depends_on=wave_dependencies.get(wave_num, ())
        )
        waves.append(wave)
    
    return ExecutionGraph(waves=tuple(waves))
```

**Build Post-Execute Graph**:

Same approach but in reverse, only including tasks with `post_execute`:

```python
def _build_post_execute_graph(
    tasks: Dict[str, DagAsyncTask],
    dependencies: Dict[str, tuple[str, ...]]
) -> ExecutionGraph:
    """Build cleanup waves for post_execute phase.
    
    Cleanup order is reverse of execution order:
    - Dependents clean up before dependencies
    - Only includes tasks with post_execute
    """
    # Filter tasks with post_execute
    post_exec_tasks = {
        name: task for name, task in tasks.items() 
        if task.post_execute is not None
    }
    
    if not post_exec_tasks:
        return ExecutionGraph(waves=())
    
    # Build reverse dependency graph (dependents)
    dependents = defaultdict(list)
    for task, deps in dependencies.items():
        for dep in deps:
            dependents[dep].append(task)
    
    # Compute reverse wave assignments
    # Reverse wave = 1 + max(reverse wave of dependents with post_execute)
    task_wave = {}
    waves_dict = defaultdict(list)
    
    # Use reverse topological sort
    reverse_topo = _topological_sort(dependencies)[::-1]
    
    for task_name in reverse_topo:
        if task_name not in post_exec_tasks:
            # No post_execute - skip
            continue
        
        # Find max wave of dependents that have post_execute
        dependent_waves = []
        for dependent in dependents.get(task_name, []):
            if dependent in task_wave:  # Dependent has post_execute
                dependent_waves.append(task_wave[dependent])
        
        # This task's reverse wave (0 = leaves)
        wave = 0 if not dependent_waves else (max(dependent_waves) + 1)
        task_wave[task_name] = wave
        waves_dict[wave].append(task_name)
    
    # Compute precise wave dependencies (not just "all earlier waves")
    # For each wave, find which specific waves it depends on
    wave_dependencies = {}
    for wave_num in sorted(waves_dict.keys()):
        deps = set()
        for task_name in waves_dict[wave_num]:
            # Find waves that contain this task's dependents (with post_execute)
            for dependent in dependents.get(task_name, []):
                if dependent in task_wave:  # Dependent has post_execute
                    dep_wave = task_wave[dependent]
                    if dep_wave < wave_num:  # Must be earlier wave (in reverse order)
                        deps.add(dep_wave)
        wave_dependencies[wave_num] = tuple(sorted(deps))
    
    # Convert to ExecutionWave objects
    waves = []
    for wave_num in sorted(waves_dict.keys()):
        wave = ExecutionWave(
            tasks=tuple(waves_dict[wave_num]),
            depends_on=wave_dependencies.get(wave_num, ())
        )
        waves.append(wave)
    
    return ExecutionGraph(waves=tuple(waves))
```

### Optional: Node Elision Optimization

For performance, elide nodes (tasks with no functions) from the execution graph:

```python
def _elide_nodes(
    tasks: Dict[str, DagAsyncTask],
    dependencies: Dict[str, tuple[str, ...]]
) -> Dict[str, tuple[str, ...]]:
    """Remove nodes from execution graph by transitive dependency closure.
    
    For each node N with dependencies {A, B} and dependents {X, Y}:
    - Remove N from graph
    - Add edges: X depends on {A, B}, Y depends on {A, B}
    
    This preserves semantics while reducing overhead.
    
    Example:
        A -> N -> X    becomes    A -> X
        B -> N -> Y               B -> Y
                                  A -> Y
                                  B -> X
    """
    nodes = {name for name, task in tasks.items() 
             if not any([task.pre_execute, task.execute, task.post_execute])}
    
    if not nodes:
        return dependencies  # No optimization needed
    
    new_dependencies = {}
    
    for task, deps in dependencies.items():
        if task in nodes:
            continue  # Skip nodes
        
        # Expand dependencies through nodes
        expanded_deps = set()
        for dep in deps:
            if dep in nodes:
                # Transitively depend on node's dependencies
                expanded_deps.update(_expand_through_nodes(dep, dependencies, nodes))
            else:
                expanded_deps.add(dep)
        
        new_dependencies[task] = tuple(expanded_deps)
    
    return new_dependencies

def _expand_through_nodes(
    node: str,
    dependencies: Dict[str, tuple[str, ...]],
    nodes: set[str]
) -> set[str]:
    """Recursively expand dependencies through nodes."""
    expanded = set()
    for dep in dependencies[node]:
        if dep in nodes:
            expanded.update(_expand_through_nodes(dep, dependencies, nodes))
        else:
            expanded.add(dep)
    return expanded
```

**Note**: Node elision is optional. It reduces overhead but adds complexity. For most use cases, the overhead of skipping nodes at runtime is negligible.

Implementation note: In the current analyzer, nodes for pre-execute are simply tasks that do not define `pre_execute` (they may still define `execute` to satisfy `AsyncTask` validation). These do not create pre-execute barriers and are followed through when computing effective dependencies.

## Error Handling

### Dependency Failure Propagation

When a task's pre_execute fails:
1. The task is marked as "failed"
2. All dependent tasks are marked as "skipped"
3. Skipped tasks don't run pre_execute or execute
4. Skipped tasks don't run post_execute (no cleanup needed, they never started)

Example:
```
A (fails) ──> B ──> D
 \               /
  ───> C ───────

If A.pre_execute fails:
- B and C are skipped (their pre_execute never runs)
- D is skipped (its pre_execute never runs)
- A.post_execute still runs (cleanup for partial pre_execute work)
```

### Partial Execution

Tasks that started pre_execute (even if it failed or was cancelled) MUST have post_execute run:

```python
@dataclass
class _TaskExecutionState:
    """Track execution state per task."""
    task: DagAsyncTask
    pre_started: bool = False      # pre_execute was started
    pre_succeeded: bool = False    # pre_execute completed successfully
    execute_ran: bool = False      # execute ran
    should_cleanup: bool = False   # post_execute should run
```

## Performance Considerations

### Overhead vs Level-Based

**DAG mode overhead:**
- Cycle detection: O(V + E) where V = tasks, E = dependencies
- Topological sort: O(V + E)
- Level computation: O(V + E)
- Reverse level computation: O(V + E)
- Total build time: O(V + E)

**Runtime overhead:**
- Similar to level-based (same parallelism mechanism)
- Slightly more bookkeeping for dependency tracking
- More granular parallelism may offset overhead

### Optimization: Level Caching

Since the processor is immutable, levels are computed once at build time and cached.

### Optimization: Skip Propagation

Use BFS to propagate "skip" status to dependents, avoiding unnecessary state checks.

## Comparison: Level-Based vs DAG-Based

| Aspect | Level-Based (current sdax) | DAG-Based (proposed) |
|--------|----------------------------|---------------------|
| Conceptual model | Coarse-grained phases | Fine-grained dependencies |
| Parallelism | All tasks in level N complete before level N+1 starts | Tasks start immediately when dependencies complete |
| Maximum parallelism | Good for uniform tasks | Optimal - exploits independent chains |
| Build complexity | O(T) where T = tasks | O(V + E) where V = tasks, E = edges |
| Runtime overhead | Minimal (direct level iteration) | Slightly higher (event coordination) |
| API simplicity | Simpler (just level int) | Slightly more complex (dependency names) |
| Error handling | Same guarantees | Same guarantees |
| Cleanup order | Reverse level order | Real-time reverse dependency order |
| Node support | N/A | Yes - zero runtime overhead |
| Use case | **Phased workflows** (AUTH → FETCH → PROCESS) | **Complex workflows** (build systems, data pipelines) |
| Code clarity | Phases are obvious | Dependencies are explicit |
| Visualization | Simple level diagram | Full dependency graph |

### When to Use Each

**Use Level-Based (current sdax) when:**
- Your workflow has natural phases (e.g., auth → fetch → process → commit)
- Tasks within a phase have similar duration
- You want the simplest possible API
- Overhead is critical (minimal bookkeeping)

**Use DAG-Based when:**
- Tasks have fine-grained, complex dependencies
- Task durations vary significantly
- You want maximum parallelism
- You're modeling a natural DAG (build system, data pipeline, workflow engine)
- You need milestone nodes for organization

## Migration Path

### Level-Based → DAG-Based

Level-based can be expressed in DAG mode:

```python
# Level-based
builder.add_task(taskA, level=1)
builder.add_task(taskB, level=1)
builder.add_task(taskC, level=2)

# Equivalent DAG mode
# Create a synthetic dependency on ALL tasks at the previous level
builder.add_task(taskA, depends_on=())
builder.add_task(taskB, depends_on=())
builder.add_task(taskC, depends_on=('taskA', 'taskB'))  # Depends on all level 1
```

### Automatic Conversion

Could provide a utility to convert level-based to DAG-based:

```python
def level_to_dag_processor(level_processor: AsyncTaskProcessor) -> DagAsyncTaskProcessor:
    """Convert level-based processor to DAG-based.
    
    Creates synthetic dependencies: each task depends on ALL tasks at the previous level.
    """
    # Implementation...
```

## Open Questions

1. **Task naming**: Should we auto-generate names if not provided? Or require explicit names?
   - **Decision**: Require explicit names (better for debugging, visualization)
   - Nodes make this even more important - explicit names create self-documenting milestones

2. **Nodes**: Should we support tasks with no functions?
   - **Decision**: Yes! Nodes (tasks with all `None` functions) are a powerful organizational tool
   - See "Nodes: Dependency Grouping and Milestones" section above
   - `add_node()` convenience method provided

3. **Partial dependency satisfaction**: Should we support OR dependencies (task runs if ANY dependency succeeds)?
   - Proposal: No, keep it simple (AND semantics only). Use separate tasks for OR logic.

4. **Dynamic dependencies**: Should we support dependencies determined at runtime?
   - Proposal: No, graph must be static at build time. Use conditional execution in task functions.

5. **Visualization**: Should we provide a `to_graphviz()` method?
   - Proposal: Yes, extremely useful for debugging and documentation.
   - Nodes will show up as distinctive shapes (e.g., diamonds) in the graph

6. **Task retries**: Should failed tasks be retried before propagating skip to dependents?
   - Proposal: No, retries are per-phase (controlled by TaskFunction), not per-task.

7. **Priority/hints**: Should tasks have priority hints for scheduler?
   - Proposal: Not in v1. Topological order is deterministic and sufficient.

## Implementation Plan

### Phase 1: Core Algorithm (No Integration)
- Implement cycle detection
- Implement topological sort
- Implement level computation
- Implement reverse level computation
- Unit tests for all algorithms

### Phase 2: Builder & Processor Classes
- Implement `DagAsyncTask` (copy from `AsyncTask`)
- Implement `DagAsyncTaskProcessorBuilder`
- Implement `DagAsyncTaskProcessor`
- Integration with existing `TaskFunction`, `_ExecutionContext`

### Phase 3: Execution Engine
- Implement pre_execute with dependency checking
- Implement execute phase
- Implement post_execute with reverse levels
- Reuse `_execute_phase` from current sdax

### Phase 4: Error Handling
- Implement skip propagation
- Implement exception aggregation
- Match current sdax guarantees (post_execute always runs)

### Phase 5: Testing
- Port all existing sdax tests to DAG mode
- Add DAG-specific tests (cycles, complex graphs, skip propagation)
- Performance benchmarks (compare to level-based)

### Phase 6: Documentation & Examples
- Update README with DAG mode
- Add visualization examples
- Migration guide from level-based

## Example: Complex DAG

### Example 1: Build System

```python
# Build system example
#
#   ┌─> compile_a ─┐
#   │              ├─> link_exe ─> test_exe
#   ├─> compile_b ─┤              
#   │              └─> link_lib ─> package
#   └─> compile_c ───────────────────┘
#

builder = DagAsyncTaskProcessor.builder()

# Independent tasks (level 0)
builder.add_task(compile_a, depends_on=())
builder.add_task(compile_b, depends_on=())
builder.add_task(compile_c, depends_on=())

# Depends on compile results (level 1)
builder.add_task(link_exe, depends_on=('compile_a', 'compile_b'))
builder.add_task(link_lib, depends_on=('compile_b',))

# Depends on link results (level 2)
builder.add_task(test_exe, depends_on=('link_exe',))
builder.add_task(package, depends_on=('link_lib', 'compile_c'))

processor = builder.build()

# Real-time execution order (showing parallelism):
# Wave 1: compile_a, compile_b, compile_c (all parallel)
# Wave 2: When compile_a and compile_b finish → link_exe starts
#         When compile_b finishes → link_lib starts (might overlap with link_exe!)
# Wave 3: When link_exe finishes → test_exe starts
#         When link_lib and compile_c finish → package starts
#
# Key advantage: link_lib can start as soon as compile_b finishes,
# even if compile_a is still running!
#
# Cleanup order (reverse real-time):
# - test_exe and package cleanup first (parallel, no dependents)
# - When they finish → link_exe and link_lib cleanup (parallel)
# - When they finish → compile_a, compile_b, compile_c cleanup (parallel)
```

### Example 2: Data Pipeline with Nodes

```python
# Data processing pipeline with milestone nodes
#
#   ┌─> fetch_users ────┐
#   ├─> fetch_orders ───┼─> [all_data_ready] ─┬─> validate ────┐
#   └─> fetch_products ─┘                      └─> transform ───┼─> [ready_to_load] ─┬─> load_db
#                                                                                      ├─> load_cache
#                                                                                      └─> notify
#

builder = DagAsyncTaskProcessor.builder()

# Fetch phase (level 0)
builder.add_task(fetch_users, depends_on=())
builder.add_task(fetch_orders, depends_on=())
builder.add_task(fetch_products, depends_on=())

# Milestone: All data fetched (level 1)
builder.add_node('all_data_ready', depends_on=('fetch_users', 'fetch_orders', 'fetch_products'))

# Process phase (level 2)
builder.add_task(validate, depends_on=('all_data_ready',))
builder.add_task(transform, depends_on=('all_data_ready',))

# Milestone: Ready to load (level 3)
builder.add_node('ready_to_load', depends_on=('validate', 'transform'))

# Load phase (level 4)
builder.add_task(load_db, depends_on=('ready_to_load',))
builder.add_task(load_cache, depends_on=('ready_to_load',))
builder.add_task(notify, depends_on=('ready_to_load',))

processor = builder.build()

# Real-time execution with nodes:
# Wave 1: fetch_users, fetch_orders, fetch_products (all parallel)
# Wave 2: When ALL fetches complete → 'all_data_ready' node completes instantly
#         → validate and transform start (parallel)
# Wave 3: When BOTH validate and transform complete → 'ready_to_load' node completes
#         → load_db, load_cache, notify start (all parallel)
#
# Benefits of nodes:
# 1. Clear milestones: 'all_data_ready', 'ready_to_load'
# 2. Easy to add new tasks at any phase (just depend on the milestone)
# 3. Self-documenting workflow structure
# 4. Can add monitoring/logging at milestone nodes later
# 5. Nodes have ZERO runtime overhead (complete instantly when deps are ready)
```

## Key Design Decisions

### Build-Time Graph Construction vs Runtime Coordination

**Initial proposal**: Runtime coordination with `asyncio.Event` per task

**Final design**: Pre-compute execution graphs (waves) at build time

**Why this approach?**

1. **No runtime coordination overhead**: No `asyncio.Event` objects, no event signaling
2. **Simpler implementation**: Just iterate through waves sequentially
3. **Still optimal parallelism**: Wave grouping exploits independent chains
4. **Easier to debug**: Graph structure is visible at build time
5. **Efficient cancellation**: Just break out of wave loop

**How it achieves optimal parallelism**:

Consider: `A → B → E` and `A → C → D` with `E → F` and `D → F`

If tasks have these functions:
- A: (pre_execute, None)
- B: (pre_execute, None)
- C: (None, None)  # Node - no actual work!
- D: (pre_execute, None)
- E: (pre_execute, None)
- F: (pre_execute, None)

Pre-execute waves:
- Wave 0: [A]
- Wave 1: [B] (depends on A; C has no pre_execute so doesn't create barrier!)
- Wave 2: [D, E] (D depends on B, E depends on C which auto-completed)
- Wave 3: [F] (depends on both D and E)

**The benefit**: C doesn't create a synchronization barrier because it has no `pre_execute`. So E can be in the same wave as D, even though their dependency chains have different lengths!

### Node Elision Optimization

Nodes (tasks with all `None` functions) can be **elided** from the execution graph via transitive dependency closure:

```
A → N → X    becomes    A → X
B → N → Y               B → Y, A → Y, B → X
```

This is **optional** - the overhead of skipping nodes at runtime is negligible for most use cases. But for very large graphs with many organizational nodes, elision can improve performance.

### Cleanup Strategy

Cleanup uses the same real-time scheduling approach but in reverse:
- Each task waits for all its **dependents** to finish cleanup
- Then it can clean up
- Independent branches clean up in parallel

This ensures:
1. Dependents always clean up before dependencies (correct order)
2. Maximum parallelism (independent branches don't block each other)
3. Exception isolation (one cleanup failure doesn't cancel others)

## Conclusion

DAG mode provides a natural and powerful way to express task dependencies with **optimal parallelism**. It maintains all the guarantees of level-based sdax (reliable cleanup, structured concurrency, exception isolation) while offering finer-grained control over task execution order.

### Key Innovation: Build-Time Graph Construction

Instead of runtime coordination with events, we **pre-compute execution graphs** during `.build()`:
- Group tasks into **waves** by their transitive dependency closure
- Only include tasks with actual functions (nodes don't create barriers)
- Build separate graphs for pre-execute and post-execute phases
- At runtime, just iterate through waves sequentially

This gives us:
- ✅ **Optimal parallelism** (independent chains don't block each other)
- ✅ **Zero runtime coordination overhead** (no events, no signaling)
- ✅ **Simple implementation** (wave iteration vs complex event logic)
- ✅ **Easy debugging** (graph structure visible at build time)

### Key Advantages Over Level-Based

- ✅ **Exploits independent chains**: E starts as soon as B finishes, doesn't wait for C
- ✅ **Nodes for organization**: Zero runtime overhead (don't create synchronization barriers)
- ✅ **Explicit dependencies**: Self-documenting, easier to reason about
- ✅ **Natural for complex workflows**: Build systems, data pipelines, workflow engines
- ✅ **Efficient failure handling**: Cancel remaining waves immediately

### Trade-Offs

- ⚠️ **More complex build phase**: Graph construction algorithms (O(V+E) vs O(V))
- ⚠️ **Slightly more complex API**: Dependency names vs level integers
- ⚠️ **More memory**: Store execution graphs (but minimal - just wave lists)

For most use cases, the improved parallelism and clearer semantics outweigh the added build-time complexity.

### Relationship to Level-Based

DAG mode is a **strict generalization** of level mode:
- Every level-based workflow can be expressed as a DAG
- Many DAG workflows cannot be expressed with levels (independent chains)
- Level-based is optimal when all tasks in a phase have similar duration
- DAG-based is optimal when task durations vary or chains are independent

