import asyncio
import random
from collections import defaultdict
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, List, TypeVar

T = TypeVar("T")


@dataclass
class TaskFunction:
    """Encapsulates a callable with its own execution parameters."""

    function: Callable[[T], Awaitable[None]]
    timeout: float = 2.0
    retries: int = 0
    backoff_factor: float = 2.0


@dataclass
class AsyncTask:
    """A declarative definition of a task with optional pre-execute, execute,
    and post-execute phases, each with its own configuration."""

    name: str
    pre_execute: TaskFunction | None = None
    execute: TaskFunction | None = None
    post_execute: TaskFunction | None = None


class _LevelManager:
    """An internal context manager to handle the parallel execution of all
    tasks within a single level for both setup and teardown."""

    def __init__(
        self, level: int, tasks: List[AsyncTask], ctx: T, processor: "AsyncTaskProcessor"
    ):
        self.level = level
        self.tasks = tasks
        self.ctx = ctx
        self.processor = processor
        self.active_tasks: List[AsyncTask] = []
        self.started_tasks: List[AsyncTask] = []  # Tasks that started pre_execute
        self.pre_execute_exception: BaseException | None = None

    async def __aenter__(self) -> List[AsyncTask]:
        """Runs pre_execute for tasks that have it, and considers tasks
        without it as implicitly successful."""
        # Attach context to all tasks in this level
        for task in self.tasks:
            setattr(task, "_ctx", self.ctx)

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
                        tg.create_task(self.processor._execute_phase(task, "pre_execute")): task
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
        """
        # Run post_execute for ALL tasks that started pre_execute
        tasks_to_cleanup = self.started_tasks

        # Also include tasks without pre_execute that are in active_tasks
        for task in self.active_tasks:
            if task not in tasks_to_cleanup:
                tasks_to_cleanup.append(task)

        if not tasks_to_cleanup:
            return

        async with asyncio.TaskGroup() as tg:
            for task in tasks_to_cleanup:
                if task.post_execute:
                    phase_name = "post_execute"
                    tg.create_task(self.processor._execute_phase(task, phase_name))


@dataclass
class AsyncTaskProcessor:
    """The core engine that processes a collection of tiered async tasks."""

    tasks: Dict[int, List[AsyncTask]] = field(default_factory=lambda: defaultdict(list))

    def add_task(self, task: AsyncTask, level: int):
        self.tasks[level].append(task)

    async def _execute_phase(self, task: AsyncTask, phase: str):
        """A helper method to wrap the execution of a single task phase
        with its configured timeout and retry logic."""
        task_func_obj = getattr(task, phase)
        if not task_func_obj:
            return

        func = task_func_obj.function
        retries = task_func_obj.retries
        timeout = task_func_obj.timeout
        backoff_factor = task_func_obj.backoff_factor

        # We need a context attached to the task for the function call
        ctx = getattr(task, "_ctx", None)
        if not ctx:
            msg = f"TaskContext not found on task '{task.name}' during phase '{phase}'"
            raise RuntimeError(msg)

        for attempt in range(retries + 1):
            try:
                await asyncio.wait_for(func(ctx), timeout=timeout)
                return  # Success
            except (asyncio.TimeoutError, ConnectionError) as e:
                # This print is for retry attempts, useful for debugging.
                # Consider replacing with logging.
                attempt_info = f"{attempt + 1}/{retries + 1}"
                print(
                    f"  - [{task.name}/{phase}] Attempt {attempt_info} "
                    f"failed: {e.__class__.__name__}"
                )
                if attempt >= retries:
                    raise

                delay = (backoff_factor**attempt) + random.uniform(0, 0.5)
                await asyncio.sleep(delay)

    async def process_tasks(self, ctx: T):
        """The main entry point to run the entire tiered workflow."""
        active_tasks: List[AsyncTask] = []
        level_managers: List[_LevelManager] = []

        async with AsyncExitStack() as stack:
            sorted_levels = sorted(self.tasks.keys())
            for level in sorted_levels:
                level_manager = _LevelManager(level, self.tasks[level], ctx, self)
                level_managers.append(level_manager)
                tasks_from_level = await stack.enter_async_context(level_manager)
                active_tasks.extend(tasks_from_level)

            execute_exception = None
            try:
                async with asyncio.TaskGroup() as tg:
                    for task in active_tasks:
                        if task.execute:
                            tg.create_task(self._execute_phase(task, "execute"))
            except* Exception as eg:
                execute_exception = eg

        # Collect all exceptions from pre_execute and execute phases
        exceptions = [lm.pre_execute_exception for lm in level_managers if lm.pre_execute_exception]
        if execute_exception:
            exceptions.append(execute_exception)

        if exceptions:
            # Raise all collected exceptions as a group
            if len(exceptions) == 1:
                raise exceptions[0]
            else:
                msg = "Multiple failures during task execution"
                raise ExceptionGroup(msg, exceptions)
