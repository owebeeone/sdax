"""Task Dependency Analyzer for SDAX DAG Mode.

This module analyzes task dependency graphs and constructs execution waves
for optimal parallel execution.

Wave construction exploits:
1. Independent chains (parallel execution when possible)
2. Nodes (tasks with no functions don't create sync barriers)
3. Precise wave dependencies (not just "all earlier waves")
"""

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

from sdax.sdax_core import AsyncTask


@dataclass(frozen=True)
class ExecutionWave:
    """A wave of tasks that can execute together.

    All tasks in a wave can run in parallel. The wave can start executing
    once all task dependencies (from earlier waves specified in depends_on)
    have completed.
    
    Note: depends_on specifies which waves must complete before this wave
    can start, based on the actual task dependencies within this wave.
    This is derived from task dependencies, not an independent relationship.
    """

    wave_num: int
    tasks: Tuple[str, ...]
    depends_on: Tuple[int, ...]  # Wave indices whose completion unblocks this wave

    def __repr__(self):
        deps_str = f"depends_on={self.depends_on}" if self.depends_on else "no dependencies"
        return f"Wave {self.wave_num}: {list(self.tasks)} ({deps_str})"


@dataclass(frozen=True)
class ExecutionGraph:
    """Complete execution graph with waves."""

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


@dataclass(frozen=True)
class TaskAnalysis:
    """Complete analysis of a task dependency graph."""

    tasks: Dict[str, AsyncTask]
    dependencies: Dict[str, Tuple[str, ...]]
    pre_execute_graph: ExecutionGraph
    post_execute_graph: ExecutionGraph

    # Statistics
    total_tasks: int
    tasks_with_pre_execute: int
    tasks_with_execute: int
    tasks_with_post_execute: int
    nodes: int  # Tasks with no functions

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


class TaskAnalyzer:
    """Analyzes task dependency graphs and constructs execution waves."""

    def __init__(self):
        self.tasks: Dict[str, AsyncTask] = {}
        self.dependencies: Dict[str, Tuple[str, ...]] = {}

    def add_task(
        self,
        task: AsyncTask,
        depends_on: Tuple[str, ...] = (),
    ) -> "TaskAnalyzer":
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

    def analyze(self) -> TaskAnalysis:
        """Analyze the task graph and build execution waves.

        Returns:
            Complete analysis with pre/post execution graphs

        Raises:
            ValueError: If dependencies reference non-existent tasks or cycles exist
        """
        # Validate
        self._validate_dependencies()
        self._detect_cycles()

        # Build execution graphs
        pre_exec_graph = self._build_pre_execute_graph()
        post_exec_graph = self._build_post_execute_graph()

        # Compute statistics
        stats = self._compute_statistics()

        return TaskAnalysis(
            tasks=dict(self.tasks),
            dependencies=dict(self.dependencies),
            pre_execute_graph=pre_exec_graph,
            post_execute_graph=post_exec_graph,
            **stats,
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
        self, task_name: str, tasks_with_phase: Set[str], visited: Set[str] = None
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
                effective.update(self._get_effective_deps(dep, tasks_with_phase, visited))
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

        # Use topological sort to process in order
        topo_order = self._topological_sort()

        for task_name in topo_order:
            if task_name not in pre_exec_tasks:
                continue  # Skip tasks without pre_execute

            # Find effective dependencies (through nodes)
            effective_deps = self._get_effective_deps(task_name, pre_exec_tasks)
            dep_waves = [task_wave[dep] for dep in effective_deps if dep in task_wave]

            # Find minimum required wave based on dependencies
            min_wave = 0 if not dep_waves else (max(dep_waves) + 1)

            # Find next unused wave starting from min_wave
            # This keeps independent chains in separate waves for parallel execution
            wave = min_wave
            while wave in waves_dict and waves_dict[wave]:
                wave += 1

            task_wave[task_name] = wave
            waves_dict[wave].append(task_name)

        # Compute precise wave dependencies
        wave_dependencies = self._compute_wave_dependencies(waves_dict, task_wave, pre_exec_tasks)

        # Convert to ExecutionWave objects
        waves = []
        for wave_num in sorted(waves_dict.keys()):
            wave = ExecutionWave(
                wave_num=wave_num,
                tasks=tuple(waves_dict[wave_num]),
                depends_on=wave_dependencies.get(wave_num, ()),
            )
            waves.append(wave)

        return ExecutionGraph(waves=tuple(waves))

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

        # Compute reverse wave assignments
        task_wave = {}
        waves_dict = defaultdict(list)

        # Use reverse topological sort
        reverse_topo = self._topological_sort()[::-1]

        for task_name in reverse_topo:
            if task_name not in post_exec_tasks:
                continue  # Skip tasks without post_execute

            # Find dependents that have post_execute
            dependent_waves = []
            for dependent in dependents.get(task_name, []):
                if dependent in task_wave:  # Dependent has post_execute
                    dependent_waves.append(task_wave[dependent])

            # This task's reverse wave (0 = leaves)
            wave = 0 if not dependent_waves else (max(dependent_waves) + 1)
            task_wave[task_name] = wave
            waves_dict[wave].append(task_name)

        # Compute precise wave dependencies for cleanup
        wave_dependencies = {}
        for wave_num in sorted(waves_dict.keys()):
            deps = set()
            for task_name in waves_dict[wave_num]:
                # Find waves that contain this task's dependents (with post_execute)
                for dependent in dependents.get(task_name, []):
                    if dependent in task_wave:
                        dep_wave = task_wave[dependent]
                        if dep_wave < wave_num:
                            deps.add(dep_wave)
            wave_dependencies[wave_num] = tuple(sorted(deps))

        # Convert to ExecutionWave objects
        waves = []
        for wave_num in sorted(waves_dict.keys()):
            wave = ExecutionWave(
                wave_num=wave_num,
                tasks=tuple(waves_dict[wave_num]),
                depends_on=wave_dependencies.get(wave_num, ()),
            )
            waves.append(wave)

        return ExecutionGraph(waves=tuple(waves))

    def _compute_wave_dependencies(
        self,
        waves_dict: Dict[int, List[str]],
        task_wave: Dict[str, int],
        tasks_with_phase: Set[str],
    ) -> Dict[int, Tuple[int, ...]]:
        """Compute precise wave dependencies.

        For each wave, find which specific earlier waves it depends on
        (not just "all earlier waves").
        """
        wave_dependencies = {}

        for wave_num in sorted(waves_dict.keys()):
            deps = set()
            for task_name in waves_dict[wave_num]:
                # Find effective dependencies
                effective_deps = self._get_effective_deps(task_name, tasks_with_phase)
                # Map to waves
                for dep in effective_deps:
                    if dep in task_wave:
                        dep_wave = task_wave[dep]
                        if dep_wave < wave_num:
                            deps.add(dep_wave)

            wave_dependencies[wave_num] = tuple(sorted(deps))

        return wave_dependencies

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
            "nodes": sum(
                1
                for t in self.tasks.values()
                if not any([t.pre_execute, t.post_execute])
            ),
        }
        return stats
