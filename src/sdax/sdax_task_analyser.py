"""Task dependency graph analyzer for sdax.

Builds immutable "waves" (start barriers) from a named‑task dependency graph:
- Pre‑execute: group tasks that share the same effective predecessors. Waves are
  start barriers only and do not introduce wave→wave dependencies.
- Execute: not wave‑driven; runs across all tasks whose pre succeeded.
- Post‑execute: construct reverse waves using nearest post dependents so
  dependents clean up before their prerequisites.

Design goals:
- Correctness and ergonomics first (uniform lifecycle, deterministic cleanup),
  not micro‑optimizations.
- Analyzer validates missing dependencies and detects cycles at build time.
- Output is immutable and safe to reuse across concurrent executions.
"""

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, Generic, List, Set, Tuple, TypeVar

from sdax.tasks import AsyncTask


@dataclass(frozen=True)
class ExecutionWave:
    """A wave of tasks that can execute together.

    Start barrier only: all tasks share the same predecessor task set.
    """

    wave_num: int
    tasks: Tuple[str, ...]
    depends_on_tasks: Tuple[str, ...] = ()  # Predecessor task names (effective deps)

    def __repr__(self):
        deps_str = (
            f"depends_on_tasks={self.depends_on_tasks}"
            if self.depends_on_tasks
            else "no dependencies"
        )
        return f"Wave {self.wave_num}: {list(self.tasks)} ({deps_str})"


@dataclass(frozen=True)
class ExecutionGraph:
    """Complete execution graph with waves (start barriers).

    Waves group tasks by identical effective predecessors for a given phase.
    """

    waves: Tuple[ExecutionWave, ...]

    def wave_containing(self, task_name: str) -> ExecutionWave | None:
        """Find the wave that contains the given task.

        Args:
            task_name: Name of the task to find

        Returns:
            The ExecutionWave containing the task, or None if not found
        """
        for wave in self.waves:
            if task_name in wave.tasks:
                return wave
        return None

    def __repr__(self):
        lines = [f"ExecutionGraph with {len(self.waves)} waves:"]
        for wave in self.waves:
            lines.append(f"  {wave}")
        return "\n".join(lines)

T = TypeVar("T")

@dataclass(frozen=True)
class TaskAnalysis(Generic[T]):
    """Complete analysis of a task dependency graph."""

    tasks: Dict[str, AsyncTask]
    dependencies: Dict[str, Tuple[str, ...]]
    pre_execute_graph: ExecutionGraph
    post_execute_graph: ExecutionGraph
    pre_root_wave_id: int | None
    post_root_wave_id: int | None

    # Optional runtime convenience metadata (derived, provided at init by analyzer)
    wave_index_by_task: Dict[str, int]
    task_to_consumer_waves: Dict[str, Tuple[int, ...]]
    wave_dep_count: Tuple[int, ...]
    # Post-exec convenience metadata (reverse)
    post_wave_index_by_task: Dict[str, int]
    post_task_to_consumer_waves: Dict[str, Tuple[int, ...]]
    post_wave_dep_count: Tuple[int, ...]
    # Precomputed task name lists per phase
    pre_task_names: Tuple[str, ...]
    execute_task_names: Tuple[str, ...]
    post_task_names: Tuple[str, ...]
    # Additional metadata for runtime scheduling
    post_only_task_names: Tuple[str, ...]
    topological_order: Tuple[str, ...]

    # Statistics
    total_tasks: int
    tasks_with_pre_execute: int
    tasks_with_execute: int
    tasks_with_post_execute: int
    nodes: int  # Tasks with no functions
    max_pre_wave_num: int
    max_post_wave_num: int

    def pre_wave_containing(self, task_name: str) -> ExecutionWave | None:
        """Find the pre-execute wave that contains the given task.

        Args:
            task_name: Name of the task to find

        Returns:
            The ExecutionWave containing the task in pre-execute, or None if not found
        """
        return self.pre_execute_graph.wave_containing(task_name)

    def post_wave_containing(self, task_name: str) -> ExecutionWave | None:
        """Find the post-execute wave that contains the given task.

        Args:
            task_name: Name of the task to find

        Returns:
            The ExecutionWave containing the task in post-execute, or None if not found
        """
        return self.post_execute_graph.wave_containing(task_name)

    def __repr__(self):
        lines = [
            "Task Dependency Analysis",
            "=" * 70,
            f"Total tasks: {self.total_tasks}",
            f"  - With pre_execute: {self.tasks_with_pre_execute}",
            f"  - With execute: {self.tasks_with_execute}",
            f"  - With post_execute: {self.tasks_with_post_execute}",
            f"  - Nodes (no functions): {self.nodes}",
            "",
            "Pre-Execute Graph:",
            str(self.pre_execute_graph),
            "",
            "Post-Execute Graph:",
            str(self.post_execute_graph),
        ]
        return "\n".join(lines)


@dataclass
class TaskAnalyzer(Generic[T]):
    """Builds wave schedules from a task dependency graph.

    Responsibilities:
    - Validate references and detect cycles.
    - Pre phase: group tasks by identical effective predecessors (through nodes).
    - Post phase: order cleanup by nearest post dependents (dependents before deps).
    - Provide immutable analysis + convenience metadata for the runtime.
    """

    tasks: Dict[str, AsyncTask[T]] = field(default_factory=dict)
    dependencies: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    # Pre-exec metadata storage for analyze() output
    _pre_wave_index_by_task: Dict[str, int] = field(default_factory=dict)
    _pre_task_to_consumer_waves: Dict[str, Tuple[int, ...]] = field(default_factory=dict)
    _pre_wave_dep_count: Dict[int, int] = field(default_factory=dict)

    def add_task(
        self,
        task: AsyncTask[T],
        depends_on: Tuple[str, ...] = (),
    ) -> "TaskAnalyzer[T]":
        """Add a task to the analyzer.

        Args:
            task: The AsyncTask to add
            depends_on: Tuple of task names this task depends on

        Returns:
            Self for fluent chaining

        Raises:
            ValueError: If task name already exists
        """
        if task.name in self.tasks:
            raise ValueError(f"Task '{task.name}' already exists")

        self.tasks[task.name] = task
        self.dependencies[task.name] = depends_on
        return self

    def analyze(self) -> TaskAnalysis[T]:
        """Analyze the task graph and build execution waves.

        Returns:
            Complete analysis with pre/post execution graphs

        Raises:
            ValueError: If dependencies reference non-existent tasks or cycles exist
        """
        # Validate
        self._validate_dependencies()
        self._detect_cycles()

        # Pre-compute topological order for reuse across build steps
        self._topological_order = self._topological_sort()

        # Build execution graphs and derived metadata
        pre_exec_graph = self._build_pre_execute_graph()
        post_exec_graph = self._build_post_execute_graph()

        # Compute statistics
        stats = self._compute_statistics()

        # Precompute task name lists per phase
        pre_task_names = tuple(sorted(n for n, t in self.tasks.items() if t.pre_execute is not None))
        execute_task_names = tuple(sorted(n for n, t in self.tasks.items() if t.execute is not None))
        post_task_names = tuple(sorted(n for n, t in self.tasks.items() if t.post_execute is not None))
        post_only_task_names = tuple(
            sorted(
                n
                for n, t in self.tasks.items()
                if t.post_execute is not None and t.pre_execute is None
            )
        )

        return TaskAnalysis(
            tasks=dict(self.tasks),
            dependencies=dict(self.dependencies),
            pre_execute_graph=pre_exec_graph,
            post_execute_graph=post_exec_graph,
            pre_root_wave_id=getattr(self, "_pre_root_wave_id", None),
            post_root_wave_id=getattr(self, "_post_root_wave_id", None),
            wave_index_by_task=dict(self._pre_wave_index_by_task),
            task_to_consumer_waves=dict(self._pre_task_to_consumer_waves),
            wave_dep_count=self._pre_wave_dep_count,
            post_wave_index_by_task=getattr(self, "_post_wave_index_by_task", {}),
            post_task_to_consumer_waves=getattr(self, "_post_task_to_consumer_waves", {}),
            post_wave_dep_count=getattr(self, "_post_wave_dep_count", ()),
            pre_task_names=pre_task_names,
            execute_task_names=execute_task_names,
            post_task_names=post_task_names,
            post_only_task_names=post_only_task_names,
            topological_order=tuple(self._topological_order),
            **stats,
            max_pre_wave_num=len(pre_exec_graph.waves),
            max_post_wave_num=len(post_exec_graph.waves),
        )

    def _validate_dependencies(self):
        """Validate that all dependencies reference existing tasks."""
        for task, deps in self.dependencies.items():
            for dep in deps:
                if dep not in self.tasks:
                    raise ValueError(f"Task '{task}' depends on non-existent task '{dep}'")

    def _detect_cycles(self):
        """Detect cycles in the dependency graph using DFS."""
        color = {task: "WHITE" for task in self.tasks}

        def dfs(node: str, path: List[str]):
            color[node] = "GRAY"
            path.append(node)

            for dep in self.dependencies.get(node, ()):
                if color[dep] == "GRAY":
                    # Cycle detected
                    cycle_start = path.index(dep)
                    cycle = path[cycle_start:] + [dep]
                    raise ValueError(f"Cycle detected: {' -> '.join(cycle)}")
                elif color[dep] == "WHITE":
                    dfs(dep, path)

            color[node] = "BLACK"
            path.pop()

        for task in self.tasks:
            if color[task] == "WHITE":
                dfs(task, [])

    def _topological_sort(self) -> List[str]:
        """Compute topological ordering using Kahn's algorithm."""
        in_degree = {task: 0 for task in self.tasks}
        dependents = defaultdict(list)

        for task, deps in self.dependencies.items():
            in_degree[task] = len(deps)
            for dep in deps:
                dependents[dep].append(task)

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

    def _get_effective_deps(
        self,
        task_name: str,
        tasks_with_phase: Set[str],
        visited: Set[str] = None,
        cache: Dict[str, Set[str]] | None = None,
    ) -> Set[str]:
        """Get all transitive dependencies that have a specific phase.

        This follows through nodes (tasks without the phase) to find
        effective dependencies.

        Args:
            task_name: Task to analyze
            tasks_with_phase: Set of tasks that have the phase we care about
            visited: Set of already visited tasks (for cycle prevention)

        Returns:
            Set of task names that are effective dependencies
        """
        if cache is not None and task_name in cache:
            return cache[task_name]

        if visited is None:
            visited = set()
        if task_name in visited:
            return set()
        visited.add(task_name)

        effective = set()
        for dep in self.dependencies.get(task_name, ()):
            if dep in tasks_with_phase:
                # This dependency has the phase
                effective.add(dep)
            else:
                # This dependency is a node for this phase - follow through
                effective.update(self._get_effective_deps(dep, tasks_with_phase, visited, cache))
        if cache is not None:
            cache[task_name] = effective
        return effective

    def _build_pre_execute_graph(self) -> ExecutionGraph:
        """Build execution waves for pre_execute phase.

        Only includes tasks with pre_execute functions.
        Tasks without pre_execute don't create synchronization barriers.
        """
        # Filter tasks with pre_execute
        pre_exec_tasks = {name for name, task in self.tasks.items() if task.pre_execute is not None}

        if not pre_exec_tasks:
            return ExecutionGraph(waves=())

        # Compute wave assignments
        task_wave = {}
        waves_dict = defaultdict(list)
        wave_depends_on_tasks: Dict[int, Tuple[str, ...]] = {}
        # Grouping maps: signature of effective dependencies -> assigned wave
        signature_to_wave: Dict[Tuple[str, ...], int] = {}
        wave_signature: Dict[int, Tuple[str, ...]] = {}
        eff_cache: Dict[str, Set[str]] = {}

        # Use topological sort to process in order
        topo_order = getattr(self, "_topological_order", None)
        if topo_order is None:
            topo_order = self._topological_sort()
            self._topological_order = topo_order

        for task_name in topo_order:
            if task_name not in pre_exec_tasks:
                continue  # Skip tasks without pre_execute

            # Find effective dependencies (through nodes)
            effective_deps = self._get_effective_deps(task_name, pre_exec_tasks, cache=eff_cache)
            dep_waves = [task_wave[dep] for dep in effective_deps if dep in task_wave]

            # Find minimum required wave based on dependencies
            min_wave = 0 if not dep_waves else (max(dep_waves) + 1)

            # Build signature by names of effective dependencies (order-insensitive)
            signature = tuple(sorted(effective_deps))

            # Reuse existing wave for identical signature, otherwise allocate
            if signature in signature_to_wave:
                wave = signature_to_wave[signature]
            else:
                wave = min_wave
                # Ensure we don't collide with an existing different signature
                while wave in wave_signature:
                    wave += 1
                signature_to_wave[signature] = wave
                wave_signature[wave] = signature

            task_wave[task_name] = wave
            waves_dict[wave].append(task_name)
            wave_depends_on_tasks[wave] = signature

        # Convert to ExecutionWave objects
        waves = []
        for wave_num in sorted(waves_dict.keys()):
            wave = ExecutionWave(
                wave_num=wave_num,
                tasks=tuple(sorted(waves_dict[wave_num])),
                depends_on_tasks=wave_depends_on_tasks.get(wave_num, ()),
            )
            waves.append(wave)

        pre_graph = ExecutionGraph(waves=tuple(waves))

        root_waves = [w.wave_num for w in pre_graph.waves if not w.depends_on_tasks]
        if len(root_waves) > 1:
            raise ValueError(
                "TaskAnalyzer detected multiple root pre-execute waves; exotic graphs are not supported."
            )
        self._pre_root_wave_id = root_waves[0] if root_waves else None

        # Build runtime convenience metadata
        wave_index_by_task: Dict[str, int] = {}
        for w in pre_graph.waves:
            for t in w.tasks:
                wave_index_by_task[t] = w.wave_num
        task_to_consumer_waves_mut: Dict[str, Set[int]] = defaultdict(set)
        for w in pre_graph.waves:
            for dep_task in w.depends_on_tasks:
                task_to_consumer_waves_mut[dep_task].add(w.wave_num)
        task_to_consumer_waves = {
            k: tuple(sorted(v)) for k, v in task_to_consumer_waves_mut.items()
        }
        # Build tuple indexed by wave_num (consecutive)
        wave_dep_count = tuple(len(w.depends_on_tasks) for w in pre_graph.waves)

        self._pre_wave_index_by_task = wave_index_by_task
        self._pre_task_to_consumer_waves = task_to_consumer_waves
        self._pre_wave_dep_count = wave_dep_count

        return pre_graph

    def _build_post_execute_graph(self) -> ExecutionGraph:
        """Build cleanup waves for post_execute phase.

        Cleanup order is reverse of execution order:
        - Dependents clean up before dependencies
        - Only includes tasks with post_execute
        """
        # Filter tasks with post_execute
        post_exec_tasks = {
            name for name, task in self.tasks.items() if task.post_execute is not None
        }

        if not post_exec_tasks:
            return ExecutionGraph(waves=())

        # Build reverse dependency graph (dependents)
        dependents = defaultdict(list)
        for task, deps in self.dependencies.items():
            for dep in deps:
                dependents[dep].append(task)

        # Helper: effective post dependents (follow through non-post nodes)
        eff_post_dep_cache: Dict[str, Set[str]] = {}

        def get_effective_post_dependents(task_name: str, visited: Set[str] | None = None) -> Set[str]:
            if visited is None:
                visited = set()
            if task_name in eff_post_dep_cache:
                return eff_post_dep_cache[task_name]
            if task_name in visited:
                return set()
            visited.add(task_name)

            effective: Set[str] = set()
            for d in dependents.get(task_name, []):
                if d in post_exec_tasks:
                    effective.add(d)
                # Always traverse further to catch post tasks beyond non-post nodes
                effective |= get_effective_post_dependents(d, visited)

            eff_post_dep_cache[task_name] = effective
            return effective

        # Helper: nearest post dependents (first post tasks reachable via any path)
        nearest_post_dep_cache: Dict[str, Set[str]] = {}

        def get_nearest_post_dependents(task_name: str, visited: Set[str] | None = None) -> Set[str]:
            if visited is None:
                visited = set()
            if task_name in nearest_post_dep_cache:
                return nearest_post_dep_cache[task_name]
            if task_name in visited:
                return set()
            visited.add(task_name)

            nearest: Set[str] = set()
            for d in dependents.get(task_name, []):
                if d in post_exec_tasks:
                    # First post task on this path → include and do not traverse past it
                    nearest.add(d)
                else:
                    # Not a post task → continue searching
                    nearest |= get_nearest_post_dependents(d, visited)

            nearest_post_dep_cache[task_name] = nearest
            return nearest

        # Compute reverse wave assignments (by reverse depth over effective post dependents)
        task_wave = {}
        waves_dict = defaultdict(list)
        wave_depends_on_tasks: Dict[int, Tuple[str, ...]] = {}

        # Reverse topo ensures we place dependents first
        topo_order = getattr(self, "_topological_order", None)
        if topo_order is None:
            topo_order = self._topological_sort()
            self._topological_order = topo_order
        reverse_topo = topo_order[::-1]

        for task_name in reverse_topo:
            if task_name not in post_exec_tasks:
                continue  # Skip tasks without post_execute

            # Effective dependents with post_execute (follow through non-post nodes)
            effective_deps = tuple(sorted(get_effective_post_dependents(task_name)))
            # Nearest post dependents (stop at first post along each path)
            nearest_post_deps = tuple(sorted(get_nearest_post_dependents(task_name)))

            # Compute reverse wave number based on already-assigned dependents
            dependent_waves = [task_wave[d] for d in effective_deps if d in task_wave]
            min_wave = 0 if not dependent_waves else (max(dependent_waves) + 1)

            wave = min_wave
            task_wave[task_name] = wave
            waves_dict[wave].append(task_name)
            # Union depends_on_tasks across tasks assigned to the same wave (nearest-only)
            if wave in wave_depends_on_tasks:
                prev = set(wave_depends_on_tasks[wave])
                merged = tuple(sorted(prev.union(nearest_post_deps)))
                wave_depends_on_tasks[wave] = merged
            else:
                wave_depends_on_tasks[wave] = nearest_post_deps

        # Convert to ExecutionWave objects
        waves = []
        for wave_num in sorted(waves_dict.keys()):
            wave = ExecutionWave(
                wave_num=wave_num,
                tasks=tuple(sorted(waves_dict[wave_num])),
                depends_on_tasks=wave_depends_on_tasks.get(wave_num, ()),
            )
            waves.append(wave)

        post_graph = ExecutionGraph(waves=tuple(waves))

        post_root_waves = [w.wave_num for w in post_graph.waves if not w.depends_on_tasks]
        if len(post_root_waves) > 1:
            raise ValueError(
                "TaskAnalyzer detected multiple root post-execute waves; exotic graphs are not supported."
            )
        self._post_root_wave_id = post_root_waves[0] if post_root_waves else None

        # Build post convenience metadata (reverse)
        post_wave_index_by_task: Dict[str, int] = {}
        for w in post_graph.waves:
            for t in w.tasks:
                post_wave_index_by_task[t] = w.wave_num
        post_task_to_consumer_waves_mut: Dict[str, Set[int]] = defaultdict(set)
        for w in post_graph.waves:
            for dep_task in w.depends_on_tasks:
                post_task_to_consumer_waves_mut[dep_task].add(w.wave_num)
        post_task_to_consumer_waves = {
            k: tuple(sorted(v)) for k, v in post_task_to_consumer_waves_mut.items()
        }
        post_wave_dep_count = tuple(len(w.depends_on_tasks) for w in post_graph.waves)

        self._post_wave_index_by_task = post_wave_index_by_task
        self._post_task_to_consumer_waves = post_task_to_consumer_waves
        self._post_wave_dep_count = post_wave_dep_count

        return post_graph

    # Legacy function removed: wave dependencies by index no longer computed

    def _compute_statistics(self) -> Dict:
        """Compute statistics about the task graph."""
        stats = {
            "total_tasks": len(self.tasks),
            "tasks_with_pre_execute": sum(
                1 for t in self.tasks.values() if t.pre_execute is not None
            ),
            "tasks_with_execute": sum(1 for t in self.tasks.values() if t.execute is not None),
            "tasks_with_post_execute": sum(
                1 for t in self.tasks.values() if t.post_execute is not None
            ),
            # Nodes: strictly tasks with no functions at all (conceptual nodes)
            # With AsyncTask validation, this typically evaluates to 0
            "nodes": sum(
                1
                for t in self.tasks.values()
                if not any([t.pre_execute, t.execute, t.post_execute])
            ),
        }
        return stats
