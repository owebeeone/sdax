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


@dataclass
class _ExecutionContext(Generic[T]):
    """Runtime state for a single execution of the processor.

    This allows multiple concurrent executions of the same processor
    without race conditions, as each execution gets its own isolated context.
    """

    user_context: T


class PhaseId(Enum):
    PRE_EXEC = auto()
    EXECUTE = auto()
    POST_EXEC = auto()


@dataclass(slots=True)
class Phase(Generic[T]):
    phase_id: PhaseId
    run: Callable[["PhaseContext[T]"], Awaitable["Phase[T] | None"]]


@dataclass(slots=True)
class PhaseContext(Generic[T]):
    processor: "AsyncDagTaskProcessor[T]"
    exec_ctx: _ExecutionContext[T]
    analysis: TaskAnalysis[T]
    tasks: Dict[str, AsyncTask[T]]
    pre_started: set[str] = field(default_factory=set)
    pre_succeeded: set[str] = field(default_factory=set)
    pre_exception: BaseException | None = None
    exec_exception: BaseException | None = None
    post_exceptions: list[BaseException] = field(default_factory=list)
    synthetic_pre_started: set[str] = field(default_factory=set)


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

    @staticmethod
    def builder() -> AsyncTaskProcessorBuilder[T]:
        return AsyncDagLevelAdapterBuilder[T]()


@dataclass
class AsyncDagTaskProcessorBuilder(Generic[T]):
    """Builder for DAG-based task processor using precomputed TaskAnalysis."""

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


@dataclass(frozen=True)
class _TaskGroupWrapper(SdaxTaskGroup):
    """Wrapper for asyncio.TaskGroup to provide a SdaxTaskGroup interface."""
    task_group: asyncio.TaskGroup

    def create_task(
        self, coro: Awaitable[Any], *, name: str | None = None, context: Any | None = None):
        return self.task_group.create_task(coro, name=name, context=context)


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
        return AsyncDagTaskProcessorBuilder[T]()

    async def process_tasks(self, ctx: T):
        phase_ctx = PhaseContext(
            processor=self,
            exec_ctx=_ExecutionContext(user_context=ctx),
            analysis=self.analysis,
            tasks=dict(self.analysis.tasks),
        )

        phase = self._make_phase(PhaseId.PRE_EXEC)
        while phase is not None:
            phase = await phase.run(phase_ctx)

        self._raise_if_failures(phase_ctx)

    def _make_phase(self, phase_id: PhaseId | None) -> Phase[T] | None:
        if phase_id is None:
            return None
        runner_map: dict[PhaseId, Callable[[PhaseContext[T]], Awaitable[Phase[T] | None]]] = {
            PhaseId.PRE_EXEC: self._run_pre_phase,
            PhaseId.EXECUTE: self._run_execute_phase,
            PhaseId.POST_EXEC: self._run_post_phase,
        }
        runner = runner_map.get(phase_id)
        if runner is None:
            return None
        return Phase(phase_id=phase_id, run=runner)

    async def _run_pre_phase(self, phase_ctx: PhaseContext[T]) -> Phase[T] | None:
        analysis = phase_ctx.analysis
        pre_waves = analysis.pre_execute_graph.waves
        if pre_waves:
            try:
                await self._run_wave_phase(
                    phase_ctx=phase_ctx,
                    root_wave_id=analysis.pre_root_wave_id,
                    waves=pre_waves,
                    wave_dep_count=analysis.wave_dep_count,
                    task_to_consumer_waves=analysis.task_to_consumer_waves,
                    should_run=self._pre_should_run,
                    run_task=self._run_pre_task,
                    propagate_exceptions=True,
                    complete_on_error=False,
                )
            except* Exception as eg:
                phase_ctx.pre_exception = eg
        return self._make_phase(self._phase_after_pre(phase_ctx))

    def _phase_after_pre(self, phase_ctx: PhaseContext[T]) -> PhaseId:
        if phase_ctx.pre_exception is not None:
            return PhaseId.POST_EXEC
        if phase_ctx.analysis.execute_task_names:
            return PhaseId.EXECUTE
        return PhaseId.POST_EXEC

    async def _run_execute_phase(self, phase_ctx: PhaseContext[T]) -> Phase[T] | None:
        analysis = phase_ctx.analysis
        tasks_by_name = phase_ctx.tasks
        exec_names = analysis.execute_task_names
        exec_to_run: list[str] = []
        for name in exec_names:
            task = tasks_by_name.get(name)
            if not task or task.execute is None:
                continue
            if task.pre_execute is not None and name not in phase_ctx.pre_succeeded:
                continue
            exec_to_run.append(name)

        if exec_to_run:
            try:
                async with asyncio.TaskGroup() as tg:
                    tg_wrapper = _TaskGroupWrapper(task_group=tg)
                    for name in exec_to_run:
                        tg.create_task(
                            self._execute_with_retry(
                                tasks_by_name[name].execute, phase_ctx.exec_ctx, tg_wrapper
                            )
                        )
            except* Exception as eg:
                phase_ctx.exec_exception = eg

        return self._make_phase(PhaseId.POST_EXEC)

    async def _run_post_phase(self, phase_ctx: PhaseContext[T]) -> Phase[T] | None:
        analysis = phase_ctx.analysis
        post_waves = analysis.post_execute_graph.waves
        if post_waves:
            phase_ctx.synthetic_pre_started = self._compute_synthetic_pre_started(
                phase_ctx.pre_succeeded
            )
            phase_ctx.post_exceptions.extend(
                await self._run_wave_phase(
                    phase_ctx=phase_ctx,
                    root_wave_id=analysis.post_root_wave_id,
                    waves=post_waves,
                    wave_dep_count=analysis.post_wave_dep_count,
                    task_to_consumer_waves=analysis.post_task_to_consumer_waves,
                    should_run=self._post_should_run,
                    run_task=self._run_post_task,
                    propagate_exceptions=False,
                    complete_on_error=True,
                )
            )
        return None

    def _pre_should_run(self, phase_ctx: PhaseContext[T], name: str) -> bool:
        task = phase_ctx.tasks[name]
        return task.pre_execute is not None

    async def _run_pre_task(
        self, phase_ctx: PhaseContext[T], task_name: str, tg_wrapper: _TaskGroupWrapper
    ):
        task = phase_ctx.tasks[task_name]
        phase_ctx.pre_started.add(task_name)
        await self._execute_with_retry(task.pre_execute, phase_ctx.exec_ctx, tg_wrapper)
        phase_ctx.pre_succeeded.add(task_name)

    def _post_should_run(self, phase_ctx: PhaseContext[T], name: str) -> bool:
        task = phase_ctx.tasks[name]
        if task.post_execute is None:
            return False
        if task.pre_execute is None:
            return name in phase_ctx.synthetic_pre_started
        return name in phase_ctx.pre_started

    async def _run_post_task(
        self, phase_ctx: PhaseContext[T], name: str, _tg_wrapper: _TaskGroupWrapper
    ):
        task = phase_ctx.tasks[name]
        result = await self._run_post_isolated(task, phase_ctx.exec_ctx)
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

    def _raise_if_failures(self, phase_ctx: PhaseContext[T]) -> None:
        exceptions: list[BaseException] = []
        if phase_ctx.pre_exception:
            exceptions.append(phase_ctx.pre_exception)
        if phase_ctx.exec_exception:
            exceptions.append(phase_ctx.exec_exception)
        exceptions.extend(phase_ctx.post_exceptions)
        if not exceptions:
            return
        if len(exceptions) == 1:
            raise exceptions[0]
        raise ExceptionGroup("Multiple failures during DAG execution", exceptions)

    async def _execute_with_retry(
        self,
        task_func_obj: TaskFunction[T],
        exec_ctx: _ExecutionContext[T],
        tg_wrapper: _TaskGroupWrapper):
        if not task_func_obj:
            return
        retries = task_func_obj.retries
        timeout = task_func_obj.timeout
        initial_delay = task_func_obj.initial_delay
        backoff_factor = task_func_obj.backoff_factor
        retryable_exceptions = task_func_obj.retryable_exceptions
        ctx = exec_ctx.user_context

        # If retryable_exceptions is empty, no retries should occur
        if not retryable_exceptions or retries <= 0:
            if timeout is None:
                await task_func_obj.call(ctx, tg_wrapper)
            else:
                await asyncio.wait_for(task_func_obj.call(ctx, tg_wrapper), timeout=timeout)
            return
        for attempt in range(retries + 1):
            try:
                if timeout is None:
                    await task_func_obj.call(ctx, tg_wrapper)
                else:
                    await asyncio.wait_for(task_func_obj.call(ctx, tg_wrapper), timeout=timeout)
                return
            except retryable_exceptions as _:
                if attempt >= retries:
                    raise
                delay = initial_delay * (backoff_factor**attempt) * random.uniform(0.5, 1.0)
                await asyncio.sleep(delay)

    def _compute_synthetic_pre_started(self, pre_succeeded: set[str]) -> set[str]:
        """Compute tasks that can be treated as having completed pre-execute.

        Tasks with real pre-execute must have succeeded. Tasks without pre-execute
        are considered started only if all their dependencies have already been
        deemed started (recursively).
        """
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

    async def _run_wave_phase(
        self,
        phase_ctx: PhaseContext[T],
        *,
        root_wave_id: int | None,
        waves: Sequence[ExecutionWave],
        wave_dep_count: Sequence[int],
        task_to_consumer_waves: Mapping[str, Sequence[int]],
        should_run: Callable[[PhaseContext[T], str], bool],
        run_task: Callable[[PhaseContext[T], str, _TaskGroupWrapper], Awaitable[None]],
        propagate_exceptions: bool,
        complete_on_error: bool,
    ) -> list[BaseException]:
        """Generic wave executor used for both pre and post phases."""
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

        scheduled: set[int] = set()
        exceptions: list[BaseException] = []
        tg_ref: Dict[str, _TaskGroupWrapper] = {}

        async def schedule_wave(idx: int):
            if idx in scheduled:
                return
            scheduled.add(idx)
            wave = wave_lookup.get(idx)
            if wave is None:
                return

            has_runnable = False
            for task_name in wave.tasks:
                if not should_run(phase_ctx, task_name):
                    await on_task_complete(task_name)
                    continue
                has_runnable = True
                tg_ref["tg"].create_task(run_wrapper(task_name))
            if not has_runnable:
                # Wave contributes to dependents even if no runnable tasks
                return

        async def on_task_complete(task_name: str):
            for consumer_idx in task_to_consumer_waves.get(task_name, ()):
                if consumer_idx >= len(completed):
                    continue
                completed[consumer_idx] += 1
                if completed[consumer_idx] >= targets[consumer_idx]:
                    await schedule_wave(consumer_idx)

        async def run_wrapper(task_name: str):
            try:
                await run_task(phase_ctx, task_name, tg_ref["tg"])
            except BaseException as exc:
                if not propagate_exceptions:
                    exceptions.append(exc)
                    if complete_on_error:
                        await on_task_complete(task_name)
                    return
                if complete_on_error:
                    await on_task_complete(task_name)
                raise
            else:
                await on_task_complete(task_name)

        async with asyncio.TaskGroup() as tg:
            tg_ref["tg"] = _TaskGroupWrapper(task_group=tg)
            await schedule_wave(root_wave_id)

        return exceptions


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
