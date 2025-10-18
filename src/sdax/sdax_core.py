"""
SDAX core engine classes.

This module contains the core engine classes for the SDAX framework.
"""
import asyncio
import random
from collections import defaultdict
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Dict, Generic, List, TypeVar

from datatrees import datatree, dtfield
from frozendict import frozendict
from sdax.sdax_task_analyser import TaskAnalysis
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


@dataclass
class AsyncTaskProcessorBuilder(Generic[T]):
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
        return AsyncTaskProcessor(tasks=frozendict(frozen_tasks))


@datatree(frozen=True)
class AsyncTaskProcessor(Generic[T]):
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
    def builder() -> AsyncTaskProcessorBuilder[T]:
        """Create a new builder for constructing an immutable processor."""
        return AsyncTaskProcessorBuilder[T]()

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
class AsyncDagTaskProcessorBuilder:
    """Builder for DAG-based task processor using precomputed TaskAnalysis."""

    analysis: TaskAnalysis | None = None

    def from_analysis(self, analysis: TaskAnalysis) -> "AsyncDagTaskProcessorBuilder":
        self.analysis = analysis
        return self

    def build(self) -> "AsyncDagTaskProcessor":
        if self.analysis is None:
            raise ValueError("TaskAnalysis must be provided via from_analysis()")
        return AsyncDagTaskProcessor(analysis=self.analysis)


@datatree(frozen=True)
class AsyncDagTaskProcessor:
    """Immutable DAG executor that consumes TaskAnalysis graphs.

    Execution policy:
      - Pre: single TaskGroup, staged by wave completed_count against depends_on_tasks.
      - Execute: single TaskGroup for tasks whose pre succeeded.
      - Post: per-task isolated TaskGroups driven by post graph and started set.
    """

    analysis: TaskAnalysis

    @staticmethod
    def builder() -> AsyncDagTaskProcessorBuilder:
        return AsyncDagTaskProcessorBuilder()

    async def process_tasks(self, ctx: T):
        # Placeholder: integrate runtime engine per design doc
        # For now, raise NotImplementedError to signal future work
        raise NotImplementedError("AsyncDagTaskProcessor.process_tasks not implemented yet")
