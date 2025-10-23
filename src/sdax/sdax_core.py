"""
SDAX core engine classes.

This module contains the core engine classes for the SDAX framework.
"""

import asyncio
import random
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Generic, List, Mapping, Sequence, TypeVar

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
    def builder(use_dag: bool = True) -> AsyncTaskProcessorBuilder[T]:
        return AsyncDagLevelAdapterBuilder[T]()


@dataclass
class AsyncDagTaskProcessorBuilder(Generic[T]):
    """Builder for DAG-based task processor using precomputed TaskAnalysis."""

    analysis: TaskAnalysis[T] | None = None

    def from_analysis(self, analysis: TaskAnalysis[T]) -> "AsyncDagTaskProcessorBuilder[T]":
        self.analysis = analysis
        return self

    def build(self) -> "AsyncDagTaskProcessor[T]":
        if self.analysis is None:
            raise ValueError("TaskAnalysis must be provided via from_analysis()")
        return AsyncDagTaskProcessor(analysis=self.analysis)


@dataclass(frozen=True)
class _TaskGroupWrapper(SdaxTaskGroup):
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
        analysis = self.analysis
        tasks_by_name: Dict[str, AsyncTask[T]] = analysis.tasks

        # Create execution context for this run
        exec_ctx: _ExecutionContext[T] = _ExecutionContext[T](user_context=ctx)

        # -------- Pre-execute (single TaskGroup, staged by readiness) --------
        pre_started: set[str] = set()
        pre_succeeded: set[str] = set()
        pre_exception: BaseException | None = None

        pre_waves = analysis.pre_execute_graph.waves

        def pre_should_run(name: str) -> bool:
            return tasks_by_name[name].pre_execute is not None

        async def run_pre(task_name: str, tg_wrapper: _TaskGroupWrapper):
            task = tasks_by_name[task_name]
            pre_started.add(task_name)
            await self._execute_with_retry(task.pre_execute, exec_ctx, tg_wrapper)
            pre_succeeded.add(task_name)

        if pre_waves:
            try:
                await self._run_wave_phase(
                    waves=pre_waves,
                    wave_dep_count=analysis.wave_dep_count,
                    task_to_consumer_waves=analysis.task_to_consumer_waves,
                    should_run=pre_should_run,
                    run_task=run_pre,
                    propagate_exceptions=True,
                    complete_on_error=False,
                )
            except* Exception as eg:
                pre_exception = eg

        # -------- Execute (single TaskGroup across eligible tasks) --------
        exec_exception: BaseException | None = None
        exec_names = analysis.execute_task_names
        exec_to_run: list[str] = []
        for name in exec_names:
            t = tasks_by_name[name]
            if t.pre_execute is not None and name not in pre_succeeded:
                continue
            exec_to_run.append(name)

        if exec_to_run:
            try:
                async with asyncio.TaskGroup() as tg:
                    tg_wrapper = _TaskGroupWrapper(task_group=tg)
                    for name in exec_to_run:
                        tg.create_task(
                            self._execute_with_retry(
                                tasks_by_name[name].execute, exec_ctx, tg_wrapper))
            except* Exception as eg:
                exec_exception = eg

        # -------- Post-execute (best-effort cleanup, per wave, isolated) --------
        post_exceptions: list[BaseException] = []
        post_waves = analysis.post_execute_graph.waves

        async def run_post_isolated(task: AsyncTask[T]):
            if not task.post_execute:
                return None
            try:
                async with asyncio.TaskGroup() as tg:
                    tg_wrapper = _TaskGroupWrapper(task_group=tg)
                    tg.create_task(self._execute_with_retry(task.post_execute, exec_ctx, tg_wrapper))
            except ExceptionGroup as eg:
                return eg
            return None

        def post_should_run(name: str) -> bool:
            task = tasks_by_name[name]
            if task.post_execute is None:
                return False
            if task.pre_execute is None:
                return True
            return name in pre_started

        async def run_post(name: str, tg_wrapper: _TaskGroupWrapper):
            task = tasks_by_name[name]
            result = await run_post_isolated(task)
            if isinstance(result, BaseException):
                raise result

        if post_waves:
            post_exceptions.extend(
                await self._run_wave_phase(
                    waves=post_waves,
                    wave_dep_count=analysis.post_wave_dep_count,
                    task_to_consumer_waves=analysis.post_task_to_consumer_waves,
                    should_run=post_should_run,
                    run_task=run_post,
                    propagate_exceptions=False,
                    complete_on_error=True,
                )
            )

        # -------- Aggregate exceptions --------
        exceptions: list[BaseException] = []
        if pre_exception:
            exceptions.append(pre_exception)
        if exec_exception:
            exceptions.append(exec_exception)
        exceptions.extend(post_exceptions)
        if exceptions:
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
        ctx = exec_ctx.user_context
        for attempt in range(retries + 1):
            try:
                if timeout is None:
                    await task_func_obj.call(ctx, tg_wrapper)
                else:
                    await asyncio.wait_for(task_func_obj.call(ctx, tg_wrapper), timeout=timeout)
                return
            except (asyncio.TimeoutError, ConnectionError) as _:
                if attempt >= retries:
                    raise
                delay = initial_delay * (backoff_factor**attempt) * random.uniform(0.5, 1.0)
                await asyncio.sleep(delay)

    async def _run_wave_phase(
        self,
        *,
        waves: Sequence[ExecutionWave],
        wave_dep_count: Sequence[int],
        task_to_consumer_waves: Mapping[str, Sequence[int]],
        should_run: Callable[[str], bool],
        run_task: Callable[[str, _TaskGroupWrapper], Awaitable[None]],
        propagate_exceptions: bool,
        complete_on_error: bool,
    ) -> list[BaseException]:
        """Generic wave executor used for both pre and post phases."""
        if not waves:
            return []

        wave_lookup: Dict[int, ExecutionWave] = {wave.wave_num: wave for wave in waves}
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
                if not should_run(task_name):
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
                await run_task(task_name, tg_ref["tg"])
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
            for idx in sorted(wave_lookup):
                if targets[idx] == 0:
                    await schedule_wave(idx)

        return exceptions


@dataclass
class AsyncDagLevelAdapterBuilder(AsyncTaskProcessorBuilder[T]):
    """Level-compatible builder that adapts to DAG by inserting level nodes.

    API matches AsyncTaskProcessorBuilder: add_task(task, level) -> self; build() -> AsyncDagTaskProcessor.
    """

    _levels: Dict[int, List[AsyncTask[T]]] = field(default_factory=lambda: defaultdict(list))

    def add_task(self, task: AsyncTask[T], level: int) -> "AsyncDagLevelAdapterBuilder[T]":
        self._levels[level].append(task)
        return self

    def build(self) -> AsyncDagTaskProcessor:
        analyzer = TaskAnalyzer()
        if not self._levels:
            analysis = analyzer.analyze()
            return AsyncDagTaskProcessor.builder().from_analysis(analysis).build()

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
            analyzer.add_task(AsyncTask(name=below_name(lvl)), depends_on=deps_for_below)

            # Add real tasks at this level depending on below node
            for task in self._levels[lvl]:
                analyzer.add_task(task, depends_on=(below_name(lvl),))

            # Add an above node that depends on all tasks at this level (if none, depends on below)
            level_tasks = self._levels[lvl]
            if level_tasks:
                analyzer.add_task(
                    AsyncTask(name=above_name(lvl)),
                    depends_on=tuple(t.name for t in level_tasks),
                )
            else:
                analyzer.add_task(AsyncTask(name=above_name(lvl)), depends_on=(below_name(lvl),))

            prev_level = lvl

        analysis = analyzer.analyze()
        return AsyncDagTaskProcessor.builder().from_analysis(analysis).build()
