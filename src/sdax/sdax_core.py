"""
SDAX core engine classes.

This module contains the core engine classes for the SDAX framework.
"""
from abc import ABC, abstractmethod
import asyncio
import random
from collections import defaultdict
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Dict, Generic, List, TypeVar

from datatrees import datatree, dtfield
from frozendict import frozendict
from sdax.sdax_task_analyser import TaskAnalysis, TaskAnalyzer
from sdax.tasks import AsyncTask, TaskFunction

T = TypeVar("T")

@dataclass
class _ExecutionContext(Generic[T]):
    """Runtime state for a single execution of the processor.

    This allows multiple concurrent executions of the same processor
    without race conditions, as each execution gets its own isolated context.
    """

    user_context: T


class _LevelManager:
    """An internal context manager to handle the parallel execution of all
    tasks within a single level for both setup and teardown."""

    def __init__(
        self,
        level: int,
        tasks: List[AsyncTask],
        exec_ctx: _ExecutionContext,
        processor: "AsyncTaskProcessor",
    ):
        self.level = level
        self.tasks = tasks
        self.exec_ctx = exec_ctx
        self.processor = processor
        self.active_tasks: List[AsyncTask] = []
        self.started_tasks: List[AsyncTask] = []  # Tasks that started pre_execute
        self.pre_execute_exception: BaseException | None = None
        self.post_execute_exceptions: List[BaseException] = []  # Exceptions from post_execute

    async def __aenter__(self) -> List[AsyncTask]:
        """Runs pre_execute for tasks that have it, and considers tasks
        without it as implicitly successful."""
        successful_tasks: List[AsyncTask] = []
        tasks_to_run: List[AsyncTask] = []

        for task in self.tasks:
            if task.pre_execute:
                tasks_to_run.append(task)
            else:
                successful_tasks.append(task)

        if tasks_to_run:
            pre_exec_map = {}
            try:
                async with asyncio.TaskGroup() as tg:
                    pre_exec_map = {
                        tg.create_task(
                            self.processor._execute_phase(task, "pre_execute", self.exec_ctx)
                        ): task
                        for task in tasks_to_run
                    }
            except* Exception as eg:
                # Some tasks failed, store the exception to raise later
                # (after __aexit__ has chance to run post_execute for tasks)
                self.pre_execute_exception = eg

            # Track ALL tasks that started pre_execute (for cleanup in __aexit__)
            self.started_tasks.extend(pre_exec_map.values())

            # Add only the tasks whose pre_execute completed successfully
            for async_task, task in pre_exec_map.items():
                if not async_task.cancelled() and async_task.exception() is None:
                    successful_tasks.append(task)

        # Track active tasks (successful pre_execute) for execute phase
        self.active_tasks = successful_tasks

        return self.active_tasks

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Runs post_execute for all tasks whose pre_execute was started.

        This ensures cleanup happens even if pre_execute was cancelled or failed,
        which is critical for resource management (releasing locks, closing files, etc).

        Uses isolated TaskGroups per task to ensure one post_execute exception
        doesn't cancel other cleanup tasks (preventing resource leaks).
        """
        # Run post_execute for ALL tasks that started pre_execute
        tasks_to_cleanup = self.started_tasks

        # Also include tasks without pre_execute that are in active_tasks
        for task in self.active_tasks:
            if task not in tasks_to_cleanup:
                tasks_to_cleanup.append(task)

        if not tasks_to_cleanup:
            return

        # Helper to run post_execute with exception isolation
        async def _run_post_isolated(task: AsyncTask):
            """Run post_execute in its own TaskGroup for structured concurrency.

            This ensures child tasks are properly managed while preventing
            exceptions from cancelling sibling post_execute tasks.
            """
            if not task.post_execute:
                return None

            exception_caught = None
            try:
                # Each post_execute gets its own TaskGroup for child task management
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(
                        self.processor._execute_phase(task, "post_execute", self.exec_ctx)
                    )
            except* Exception as eg:
                # Capture exception but don't propagate
                exception_caught = eg

            return exception_caught

        # Run all post_execute in parallel using gather (no cancellation on exception)
        post_tasks = [_run_post_isolated(task) for task in tasks_to_cleanup]
        results = await asyncio.gather(*post_tasks, return_exceptions=True)

        # Collect exceptions from post_execute
        for result in results:
            if result is not None and isinstance(result, BaseException):
                self.post_execute_exceptions.append(result)


class AsyncTaskProcessorBuilder(Generic[T], ABC):
    @abstractmethod
    def add_task(self, task: AsyncTask[T], level: int) -> "AsyncTaskProcessorBuilder[T]":
        pass

    @abstractmethod
    def build(self) -> "AsyncTaskProcessor[T]":
        pass

@dataclass
class AsyncLevelTaskProcessorBuilder(AsyncTaskProcessorBuilder[T]):
    """The builder for the core engine that processes a collection of tiered async tasks."""

    tasks: Dict[int, List[AsyncTask[T]]] = field(default_factory=lambda: defaultdict(list))

    def add_task(self, task: AsyncTask[T], level: int) -> "AsyncTaskProcessorBuilder[T]":
        """Add a task at the specified level. Returns self for fluent chaining."""
        self.tasks[level].append(task)
        return self

    def build(self) -> "AsyncTaskProcessor[T]":
        """Build an immutable AsyncTaskProcessor from the accumulated tasks."""
        # Convert defaultdict to regular dict and freeze task lists
        frozen_tasks = {level: tuple(tasks) for level, tasks in self.tasks.items()}
        return AsyncLevelTaskProcessor[T](tasks=frozendict(frozen_tasks))

class AsyncTaskProcessor(Generic[T], ABC):

    @abstractmethod
    async def process_tasks(self, ctx: T):
        pass

    @staticmethod
    def builder(use_dag: bool = True) -> AsyncTaskProcessorBuilder[T]:
        if use_dag:
            return AsyncDagLevelAdapterBuilder[T]()
        else:
            return AsyncLevelTaskProcessorBuilder[T]()


@datatree(frozen=True)
class AsyncLevelTaskProcessor(Generic[T]):
    """Immutable core engine that processes a collection of tiered async tasks.

    This class is frozen and can be safely shared across multiple concurrent
    executions. Use AsyncTaskProcessorBuilder to construct instances.
    """

    tasks: frozendict[int, tuple[AsyncTask[T], ...]]

    # Calculated field: sorted levels for iteration
    sorted_levels: tuple[int, ...] = dtfield(
        self_default=lambda self: tuple(sorted(self.tasks.keys()))
    )

    @staticmethod
    def builder(use_dag: bool = False) -> AsyncTaskProcessorBuilder[T]:
        """Create a new builder for constructing an immutable processor."""
        if use_dag:
            return AsyncDagLevelAdapterBuilder[T]()
        else:
            return AsyncLevelTaskProcessorBuilder[T]()

    async def _execute_phase(self, task: AsyncTask[T], phase: str, exec_ctx: _ExecutionContext[T]):
        """A helper method to wrap the execution of a single task phase
        with its configured timeout and retry logic."""
        task_func_obj = getattr(task, phase)
        if not task_func_obj:
            return

        func = task_func_obj.function
        retries = task_func_obj.retries
        timeout = task_func_obj.timeout
        initial_delay = task_func_obj.initial_delay
        backoff_factor = task_func_obj.backoff_factor

        # All tasks in this execution share the same user context
        ctx = exec_ctx.user_context

        for attempt in range(retries + 1):
            try:
                if timeout is None:
                    await func(ctx)
                else:
                    await asyncio.wait_for(func(ctx), timeout=timeout)
                return  # Success
            except (asyncio.TimeoutError, ConnectionError) as _:
                if attempt >= retries:
                    raise

                # Calculate delay with exponential backoff and multiplicative jitter
                # delay = initial_delay * (backoff_factor ** attempt) * uniform(0.5, 1.0)
                # This gives min delay of initial_delay * 0.5 and max of initial_delay
                # on first retry
                delay = initial_delay * (backoff_factor**attempt) * random.uniform(0.5, 1.0)
                await asyncio.sleep(delay)

    async def process_tasks(self, ctx: T):
        """The main entry point to run the entire tiered workflow.

        Creates an isolated execution context for this run, enabling
        safe concurrent executions of the same processor instance.
        """
        # Create execution context for this run
        exec_ctx = _ExecutionContext(user_context=ctx)

        active_tasks: List[AsyncTask] = []
        level_managers: List[_LevelManager] = []

        async with AsyncExitStack() as stack:
            for level in self.sorted_levels:
                level_manager = _LevelManager(level, self.tasks[level], exec_ctx, self)
                level_managers.append(level_manager)
                tasks_from_level = await stack.enter_async_context(level_manager)
                active_tasks.extend(tasks_from_level)

            execute_exception = None
            try:
                async with asyncio.TaskGroup() as tg:
                    for task in active_tasks:
                        if task.execute:
                            tg.create_task(self._execute_phase(task, "execute", exec_ctx))
            except* Exception as eg:
                execute_exception = eg

        # Collect all exceptions from pre_execute, execute, and post_execute phases
        exceptions = [lm.pre_execute_exception for lm in level_managers if lm.pre_execute_exception]
        if execute_exception:
            exceptions.append(execute_exception)

        # Collect all post_execute exceptions from all levels
        for lm in level_managers:
            exceptions.extend(lm.post_execute_exceptions)

        if exceptions:
            # Raise all collected exceptions as a group
            if len(exceptions) == 1:
                raise exceptions[0]
            else:
                msg = "Multiple failures during task execution"
                raise ExceptionGroup(msg, exceptions)


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


@datatree(frozen=True)
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
        # completed_count indexed by wave_num
        pre_completed_count: Dict[int, int] = {w.wave_num: 0 for w in pre_waves}
        pre_dep_target = analysis.wave_dep_count
        scheduled_waves: set[int] = set()

        async def run_pre(task_name: str):
            task = tasks_by_name[task_name]
            pre_started.add(task_name)
            await self._execute_phase(task, "pre_execute", exec_ctx)
            pre_succeeded.add(task_name)
            # Increment consumer waves and schedule newly ready waves
            for widx in analysis.task_to_consumer_waves.get(task_name, ()):  # type: ignore[attr-defined]
                pre_completed_count[widx] = pre_completed_count.get(widx, 0) + 1
                if pre_completed_count[widx] >= pre_dep_target[widx]:
                    await schedule_wave(widx)

        # schedule_wave needs access to tg; define placeholder and bind later
        tg_ref: Dict[str, asyncio.TaskGroup] = {}

        async def schedule_wave(wave_idx: int):
            if wave_idx in scheduled_waves:
                return
            scheduled_waves.add(wave_idx)
            wave = pre_waves[wave_idx]
            for name in wave.tasks:
                tg_ref["tg"].create_task(run_pre(name))

        if pre_waves:
            try:
                async with asyncio.TaskGroup() as tg:
                    tg_ref["tg"] = tg
                    # Seed root waves (dep count == 0)
                    for w in pre_waves:
                        if analysis.wave_dep_count[w.wave_num] == 0:
                            await schedule_wave(w.wave_num)
            except* Exception as eg:
                pre_exception = eg

        # -------- Execute (single TaskGroup across eligible tasks) --------
        exec_exception: BaseException | None = None
        exec_names = getattr(analysis, "execute_task_names", ())
        exec_to_run: list[str] = []
        for name in exec_names:
            t = tasks_by_name[name]
            if t.pre_execute is not None and name not in pre_succeeded:
                continue
            exec_to_run.append(name)

        if exec_to_run:
            try:
                async with asyncio.TaskGroup() as tg:
                    for name in exec_to_run:
                        tg.create_task(self._execute_phase(tasks_by_name[name], "execute", exec_ctx))
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
                    tg.create_task(self._execute_phase(task, "post_execute", exec_ctx))
            except ExceptionGroup as eg:
                return eg
            return None

        if post_waves:
            for wave in post_waves:
                # Determine eligibility: started pre or no pre
                eligible: list[AsyncTask[T]] = []
                for name in wave.tasks:
                    t = tasks_by_name[name]
                    if t.post_execute is None:
                        continue
                    if t.pre_execute is None or name in pre_started:
                        eligible.append(t)
                if not eligible:
                    continue
                results = await asyncio.gather(
                    *[run_post_isolated(t) for t in eligible], return_exceptions=True
                )
                for res in results:
                    if isinstance(res, BaseException):
                        post_exceptions.append(res)

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

    async def _execute_phase(self, task: AsyncTask[T], phase: str, exec_ctx: _ExecutionContext[T]):
        task_func_obj = getattr(task, phase)
        if not task_func_obj:
            return
        func = task_func_obj.function
        retries = task_func_obj.retries
        timeout = task_func_obj.timeout
        initial_delay = task_func_obj.initial_delay
        backoff_factor = task_func_obj.backoff_factor
        ctx = exec_ctx.user_context
        for attempt in range(retries + 1):
            try:
                if timeout is None:
                    await func(ctx)
                else:
                    await asyncio.wait_for(func(ctx), timeout=timeout)
                return
            except (asyncio.TimeoutError, ConnectionError) as _:
                if attempt >= retries:
                    raise
                delay = initial_delay * (backoff_factor**attempt) * random.uniform(0.5, 1.0)
                await asyncio.sleep(delay)


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
