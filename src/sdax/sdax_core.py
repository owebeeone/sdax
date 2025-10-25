"""
SDAX core engine classes.

This module contains the core engine classes for the SDAX framework.
"""

import asyncio
import random
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Dict, Generic, List, Mapping, Sequence, Tuple, TypeVar

from sdax.sdax_task_analyser import ExecutionWave, TaskAnalysis, TaskAnalyzer
from sdax.tasks import AsyncTask, SdaxTaskGroup, TaskFunction

T = TypeVar("T")


class SdaxExecutionError(ExceptionGroup):
    """Raised when one or more SDAX task phases fail."""


def _is_retryable_exception(exc: BaseException, retryable: tuple[type[BaseException], ...]) -> bool:
    """Return True if the exception (or its aggregated children) are retryable."""
    if not retryable:
        return False
    if isinstance(exc, ExceptionGroup):
        return all(_is_retryable_exception(child, retryable) for child in exc.exceptions)
    return isinstance(exc, retryable)


@dataclass
class _ExecutionContext(Generic[T]):
    """Runtime state for a single execution of the processor.

    This allows multiple concurrent executions of the same processor
    without race conditions, as each execution gets its own isolated context.
    """

    user_context: T


@dataclass(frozen=True)
class _TaskGroupWrapper(SdaxTaskGroup):
    """Wrapper for asyncio.TaskGroup to provide a SdaxTaskGroup interface."""
    task_group: asyncio.TaskGroup

    def create_task(
        self, coro: Awaitable[Any], *, name: str | None = None, context: Any | None = None):
        return self.task_group.create_task(coro, name=name, context=context)


class PhaseId(Enum):
    PRE_EXEC = auto()
    EXECUTE = auto()
    POST_EXEC = auto()


@dataclass(slots=True)
class Phase(Generic[T]):
    phase_id: PhaseId
    run: Callable[["PhaseContext[T]"], Awaitable[PhaseId | None]]


@dataclass(slots=True)
class PhaseContext(Generic[T]):
    exec_ctx: _ExecutionContext[T]
    analysis: TaskAnalysis[T]
    tasks: Dict[str, AsyncTask[T]]
    pre_started: set[str] = field(default_factory=set)
    pre_succeeded: set[str] = field(default_factory=set)
    pre_exception: BaseException | None = None
    exec_exception: BaseException | None = None
    post_exceptions: list[BaseException] = field(default_factory=list)
    synthetic_pre_started: set[str] = field(default_factory=set)

    @dataclass(slots=True)
    class _WavePhaseRunner:
        wave_lookup: Dict[int, ExecutionWave]
        targets: list[int]
        completed: list[int]
        task_to_consumer_waves: Mapping[str, Sequence[int]]
        should_run: Callable[[str], bool]
        run_task: Callable[[str, _TaskGroupWrapper], Awaitable[None]]
        propagate_exceptions: bool
        complete_on_error: bool
        scheduled: set[int] = field(default_factory=set)
        exceptions: list[BaseException] = field(default_factory=list)
        tg_wrapper: _TaskGroupWrapper | None = None

        def _require_task_group(self) -> _TaskGroupWrapper:
            if self.tg_wrapper is None:
                raise RuntimeError("Task group wrapper has not been initialized.")
            return self.tg_wrapper

        async def execute(self, root_wave_id: int) -> list[BaseException]:
            async with asyncio.TaskGroup() as tg:
                self.tg_wrapper = _TaskGroupWrapper(task_group=tg)
                await self.schedule_wave(root_wave_id)
            return self.exceptions

        async def schedule_wave(self, idx: int) -> None:
            if idx in self.scheduled:
                return
            self.scheduled.add(idx)
            wave = self.wave_lookup.get(idx)
            if wave is None:
                return

            has_runnable = False
            tg_wrapper = self._require_task_group()
            for task_name in wave.tasks:
                if not self.should_run(task_name):
                    await self.on_task_complete(task_name)
                    continue
                has_runnable = True
                tg_wrapper.create_task(self.run_wrapper(task_name))
            if not has_runnable:
                return

        async def on_task_complete(self, task_name: str) -> None:
            for consumer_idx in self.task_to_consumer_waves.get(task_name, ()):
                if consumer_idx >= len(self.completed):
                    continue
                self.completed[consumer_idx] += 1
                if self.completed[consumer_idx] >= self.targets[consumer_idx]:
                    await self.schedule_wave(consumer_idx)

        async def run_wrapper(self, task_name: str) -> None:
            tg_wrapper = self._require_task_group()
            try:
                await self.run_task(task_name, tg_wrapper)
            except BaseException as exc:
                if not self.propagate_exceptions:
                    self.exceptions.append(exc)
                    if self.complete_on_error:
                        await self.on_task_complete(task_name)
                    return
                if self.complete_on_error:
                    await self.on_task_complete(task_name)
                raise
            else:
                await self.on_task_complete(task_name)

    def create_phase(self, phase_id: PhaseId) -> Phase[T]:
        runner = self._phase_runner_map().get(phase_id)
        if runner is None:
            raise ValueError(f"Unsupported phase id {phase_id}")
        return Phase(phase_id=phase_id, run=runner)

    def _phase_runner_map(self) -> Dict[PhaseId, Callable[["PhaseContext[T]"], Awaitable[PhaseId | None]]]:
        return {
            PhaseId.PRE_EXEC: PhaseContext._run_pre_phase,
            PhaseId.EXECUTE: PhaseContext._run_execute_phase,
            PhaseId.POST_EXEC: PhaseContext._run_post_phase,
        }

    async def _run_pre_phase(self) -> PhaseId | None:
        pre_waves = self.analysis.pre_execute_graph.waves
        if pre_waves:
            try:
                await self._run_wave_phase(
                    root_wave_id=self.analysis.pre_root_wave_id,
                    waves=pre_waves,
                    wave_dep_count=self.analysis.wave_dep_count,
                    task_to_consumer_waves=self.analysis.task_to_consumer_waves,
                    should_run=self._pre_should_run,
                    run_task=self._run_pre_task,
                    propagate_exceptions=True,
                    complete_on_error=False,
                )
            except* Exception as eg:
                self.pre_exception = eg
        return self._phase_after_pre()

    def _phase_after_pre(self) -> PhaseId:
        if self.pre_exception is not None:
            return PhaseId.POST_EXEC
        if self.analysis.execute_task_names:
            return PhaseId.EXECUTE
        return PhaseId.POST_EXEC

    async def _run_execute_phase(self) -> PhaseId | None:
        exec_names = self.analysis.execute_task_names
        exec_to_run: list[str] = []
        for name in exec_names:
            task = self.tasks.get(name)
            if not task or task.execute is None:
                continue
            if task.pre_execute is not None and name not in self.pre_succeeded:
                continue
            exec_to_run.append(name)

        if exec_to_run:
            try:
                async with asyncio.TaskGroup() as tg:
                    tg_wrapper = _TaskGroupWrapper(task_group=tg)
                    for name in exec_to_run:
                        tg.create_task(
                            self._execute_with_retry(
                                self.tasks[name].execute, self.exec_ctx, tg_wrapper
                            )
                        )
            except* Exception as eg:
                self.exec_exception = eg
        return PhaseId.POST_EXEC

    async def _run_post_phase(self) -> PhaseId | None:
        post_waves = self.analysis.post_execute_graph.waves
        if post_waves:
            self.synthetic_pre_started = self._compute_synthetic_pre_started(self.pre_succeeded)
            self.post_exceptions.extend(
                await self._run_wave_phase(
                    root_wave_id=self.analysis.post_root_wave_id,
                    waves=post_waves,
                    wave_dep_count=self.analysis.post_wave_dep_count,
                    task_to_consumer_waves=self.analysis.post_task_to_consumer_waves,
                    should_run=self._post_should_run,
                    run_task=self._run_post_task,
                    propagate_exceptions=False,
                    complete_on_error=True,
                )
            )
        return None

    def raise_failures(self) -> None:
        exceptions: list[BaseException] = []
        if self.pre_exception:
            exceptions.append(self.pre_exception)
        if self.exec_exception:
            exceptions.append(self.exec_exception)
        exceptions.extend(self.post_exceptions)
        if not exceptions:
            return
        raise SdaxExecutionError("SDAX task execution failed", exceptions)

    def _pre_should_run(self, name: str) -> bool:
        task = self.tasks[name]
        return task.pre_execute is not None

    async def _run_pre_task(self, task_name: str, tg_wrapper: _TaskGroupWrapper):
        task = self.tasks[task_name]
        self.pre_started.add(task_name)
        await self._execute_with_retry(task.pre_execute, self.exec_ctx, tg_wrapper)
        self.pre_succeeded.add(task_name)

    def _post_should_run(self, name: str) -> bool:
        task = self.tasks[name]
        if task.post_execute is None:
            return False
        if task.pre_execute is None:
            return name in self.synthetic_pre_started
        return name in self.pre_started

    async def _run_post_task(self, name: str, _tg_wrapper: _TaskGroupWrapper):
        task = self.tasks[name]
        result = await self._run_post_isolated(task, self.exec_ctx)
        if isinstance(result, BaseException):
            raise result

    async def _run_post_isolated(
        self, task: AsyncTask[T], exec_ctx: _ExecutionContext[T]
    ) -> BaseException | None:
        if not task.post_execute:
            return None
        try:
            async with asyncio.TaskGroup() as tg:
                tg_wrapper = _TaskGroupWrapper(task_group=tg)
                tg.create_task(self._execute_with_retry(task.post_execute, exec_ctx, tg_wrapper))
        except ExceptionGroup as eg:
            return eg
        return None

    async def _run_wave_phase(
        self,
        *,
        root_wave_id: int | None,
        waves: Sequence[ExecutionWave],
        wave_dep_count: Sequence[int],
        task_to_consumer_waves: Mapping[str, Sequence[int]],
        should_run: Callable[[str], bool],
        run_task: Callable[[str, _TaskGroupWrapper], Awaitable[None]],
        propagate_exceptions: bool,
        complete_on_error: bool,
    ) -> list[BaseException]:
        if not waves:
            return []
        if root_wave_id is None:
            raise ValueError("Wave execution requires a root wave id from the analyzer.")

        wave_lookup: Dict[int, ExecutionWave] = {wave.wave_num: wave for wave in waves}
        if root_wave_id not in wave_lookup:
            raise ValueError(f"Root wave id {root_wave_id} not found in wave definitions.")
        max_wave_index = max(wave_lookup) + 1 if wave_lookup else 0

        targets = list(wave_dep_count)
        if len(targets) < max_wave_index:
            targets.extend([0] * (max_wave_index - len(targets)))
        completed = [0] * len(targets)

        runner = PhaseContext._WavePhaseRunner(
            wave_lookup=wave_lookup,
            targets=targets,
            completed=completed,
            task_to_consumer_waves=task_to_consumer_waves,
            should_run=should_run,
            run_task=run_task,
            propagate_exceptions=propagate_exceptions,
            complete_on_error=complete_on_error,
        )

        return await runner.execute(root_wave_id)

    async def _execute_with_retry(
        self,
        task_func_obj: TaskFunction[T],
        exec_ctx: _ExecutionContext[T],
        tg_wrapper: _TaskGroupWrapper,
    ):
        if not task_func_obj:
            return
        retries = task_func_obj.retries
        timeout = task_func_obj.timeout
        initial_delay = task_func_obj.initial_delay
        backoff_factor = task_func_obj.backoff_factor
        retryable_exceptions = task_func_obj.retryable_exceptions
        ctx = exec_ctx.user_context

        for attempt in range(retries + 1):
            try:
                if timeout is None:
                    await task_func_obj.call(ctx, tg_wrapper)
                else:
                    await asyncio.wait_for(task_func_obj.call(ctx, tg_wrapper), timeout=timeout)
                return
            except BaseException as exc:
                if not _is_retryable_exception(exc, retryable_exceptions) or attempt >= retries:
                    raise
                delay = initial_delay * (backoff_factor**attempt) * random.uniform(0.5, 1.0)
                await asyncio.sleep(delay)

    def _compute_synthetic_pre_started(self, pre_succeeded: set[str]) -> set[str]:
        analysis = self.analysis
        tasks = analysis.tasks
        dependencies = analysis.dependencies

        ready: set[str] = set(pre_succeeded)
        topo_order = analysis.topological_order or tuple(self._topological_order())

        for name in topo_order:
            task = tasks[name]
            deps = dependencies.get(name, ())
            if task.pre_execute is not None:
                if name in pre_succeeded:
                    ready.add(name)
                continue
            if all(dep in ready for dep in deps):
                ready.add(name)
        return ready

    def _topological_order(self) -> List[str]:
        dependencies = self.analysis.dependencies
        tasks = self.analysis.tasks

        in_degree = {name: len(dependencies.get(name, ())) for name in tasks}
        dependents: Dict[str, List[str]] = defaultdict(list)
        for task_name, deps in dependencies.items():
            for dep in deps:
                dependents[dep].append(task_name)

        queue: deque[str] = deque(sorted(name for name, deg in in_degree.items() if deg == 0))
        order: List[str] = []

        while queue:
            current = queue.popleft()
            order.append(current)
            for dependent in dependents.get(current, ()):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        return order


class AsyncTaskProcessorBuilder(Generic[T], ABC):
    @abstractmethod
    def add_task(self, task: AsyncTask[T], level: int) -> "AsyncTaskProcessorBuilder[T]":
        pass

    @abstractmethod
    def build(self) -> "AsyncTaskProcessor[T]":
        pass


class AsyncTaskProcessor(Generic[T], ABC):
    """The core engine that processes a collection of async tasks."""
    @abstractmethod
    async def process_tasks(self, ctx: T):
        pass

    @abstractmethod
    def open(self, ctx: T) -> "AsyncPhaseRunner[T]":
        pass

    @staticmethod
    def builder() -> AsyncTaskProcessorBuilder[T]:
        """Create a builder for level-based / Elevator adapter task processor."""
        return AsyncDagLevelAdapterBuilder[T]()


@dataclass
class AsyncDagTaskProcessorBuilder(Generic[T]):
    """Builder for DAG-based task processor.
    This uses a TaskAnalyzer to build a TaskAnalysis graph, which is then used to 
    create an AsyncDagTaskProcessor.
    """

    taskAnalyzer: TaskAnalyzer[T] = field(default_factory=TaskAnalyzer)

    def add_task(
        self,
        task: AsyncTask[T],
        depends_on: Tuple[str, ...] = (),
    ) -> "AsyncDagTaskProcessorBuilder[T]":
        """Add a task to the analyzer.

        Args:
            task: The AsyncTask to add
            depends_on: Tuple of task names this task depends on

        Returns:
            Self for fluent chaining

        Raises:
            ValueError: If task name already exists
        """
        self.taskAnalyzer.add_task(task, depends_on=depends_on)
        return self

    def build(self) -> "AsyncDagTaskProcessor[T]":
        analysis = self.taskAnalyzer.analyze()
        return AsyncDagTaskProcessor(analysis=analysis)


@dataclass(slots=True)
class AsyncPhaseRunner(Generic[T]):
    """Async context manager that drives phase execution for a processor run."""

    processor: "AsyncDagTaskProcessor[T]"
    ctx: T
    _phase_ctx: PhaseContext[T] = field(init=False)
    _current_phase: Phase[T] | None = field(init=False, default=None)
    _next_phase_id: PhaseId | None = field(init=False, default=None)
    _last_phase_id: PhaseId | None = field(init=False, default=None)
    _closed: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        analysis = self.processor.analysis
        self._phase_ctx = PhaseContext[T](
            exec_ctx=_ExecutionContext(user_context=self.ctx),
            analysis=analysis,
            tasks=dict(analysis.tasks),
        )
        self._current_phase = self._phase_ctx.create_phase(PhaseId.PRE_EXEC)
        self._next_phase_id = (
            self._current_phase.phase_id if self._current_phase is not None else None
        )

    async def __aenter__(self) -> "AsyncPhaseRunner[T]":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb,
    ) -> bool:
        try:
            await self.aclose()
            self.raise_failures()
        finally:
            self._closed = True
        return False

    async def run_next(self) -> bool:
        """Execute the current phase and prepare the next one.

        Returns:
            bool: True if another phase remains to be executed, False otherwise.

        Raises:
            RuntimeError: If called after the runner has been closed.
        """
        if self._closed:
            raise RuntimeError("Attempted to run phases on a closed processor.")
        if self._current_phase is None:
            return False

        current = self._current_phase
        self._last_phase_id = current.phase_id
        next_phase_id = await current.run(self._phase_ctx)
        self._current_phase = (
            self._phase_ctx.create_phase(next_phase_id) if next_phase_id is not None else None
        )
        self._next_phase_id = (
            self._current_phase.phase_id if self._current_phase is not None else None
        )
        return self._current_phase is not None

    async def aclose(self) -> None:
        """Ensure all remaining phases are executed."""
        if self._closed:
            return
        while self._current_phase is not None:
            if not await self.run_next():
                break
        self._closed = True

    def next_phase_id(self) -> PhaseId | None:
        """Return the identifier of the next phase scheduled to run."""
        return self._next_phase_id

    def last_phase_id(self) -> PhaseId | None:
        """Return the identifier of the most recently executed phase."""
        return self._last_phase_id

    def has_failures(self) -> bool:
        """Return True if any exceptions occurred during execution."""
        ctx = self._phase_ctx
        return bool(
            ctx.pre_exception or ctx.exec_exception or ctx.post_exceptions
        )

    def failures(self) -> list[BaseException]:
        """Return a list of all exceptions that occurred during execution."""
        ctx = self._phase_ctx
        failures: list[BaseException] = []
        if ctx.pre_exception:
            failures.append(ctx.pre_exception)
        if ctx.exec_exception:
            failures.append(ctx.exec_exception)
        failures.extend(ctx.post_exceptions)
        return failures

    def raise_failures(self) -> None:
        """Raise all exceptions that occurred during execution as an SdaxExecutionError."""
        self._phase_ctx.raise_failures()


@dataclass(frozen=True)
class AsyncDagTaskProcessor(Generic[T]):
    """Immutable DAG executor that consumes TaskAnalysis graphs.

    Execution policy:
      - Pre: single TaskGroup, staged by wave completed_count against depends_on_tasks.
      - Execute: single TaskGroup for tasks whose pre succeeded.
      - Post: per-task isolated TaskGroups driven by post graph and started set.
    """

    analysis: TaskAnalysis[T]

    @staticmethod
    def builder() -> AsyncDagTaskProcessorBuilder[T]:
        """Create a builder for a new AsyncDagTaskProcessor."""
        return AsyncDagTaskProcessorBuilder[T]()

    async def process_tasks(self, ctx: T):
        """Process all phases of a new execution of the processor."""
        async with self.open(ctx) as runner:
            while await runner.run_next():
                pass

    def open(self, ctx: T) -> AsyncPhaseRunner[T]:
        """Create a phase runner for advanced control over execution."""
        return AsyncPhaseRunner(self, ctx)


@dataclass
class AsyncDagLevelAdapterBuilder(AsyncTaskProcessorBuilder[T]):
    """Level-compatible builder that adapts to DAG by inserting level nodes.

    API matches AsyncTaskProcessorBuilder: add_task(task, level) -> self;
    build() -> AsyncDagTaskProcessor.
    """

    _levels: Dict[int, List[AsyncTask[T]]] = field(default_factory=lambda: defaultdict(list))

    def add_task(self, task: AsyncTask[T], level: int) -> "AsyncDagLevelAdapterBuilder[T]":
        self._levels[level].append(task)
        return self

    def build(self) -> AsyncDagTaskProcessor:
        builder = AsyncDagTaskProcessor[T].builder()
        if not self._levels:
            return builder.build()

        sorted_levels = sorted(self._levels.keys())

        def below_name(lvl: int) -> str:
            return f"__level_{lvl}_below__"

        def above_name(lvl: int) -> str:
            return f"__level_{lvl}_above__"

        # Create level nodes and tasks with appropriate dependencies
        prev_level: int | None = None
        for lvl in sorted_levels:
            # Ensure below node for this level; link to previous level's above node if exists
            deps_for_below = () if prev_level is None else (above_name(prev_level),)
            builder.add_task(AsyncTask(name=below_name(lvl)), depends_on=deps_for_below)

            # Add real tasks at this level depending on below node
            for task in self._levels[lvl]:
                builder.add_task(task, depends_on=(below_name(lvl),))

            # Add an above node that depends on all tasks at this level (if none, depends on below)
            level_tasks = self._levels[lvl]
            if level_tasks:
                builder.add_task(
                    AsyncTask(name=above_name(lvl)),
                    depends_on=tuple(t.name for t in level_tasks),
                )
            else:
                builder.add_task(AsyncTask(name=above_name(lvl)), depends_on=(below_name(lvl),))

            prev_level = lvl

        return builder.build()
