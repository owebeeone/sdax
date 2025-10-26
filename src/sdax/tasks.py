"""
Task group classes.
"""

from abc import ABC, abstractmethod
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Callable, Generic, Hashable, TypeVar

T = TypeVar("T")
K = TypeVar("K", bound=Hashable | str)


class RetryableException(BaseException):
    """An exception that can be retried."""


class SdaxTaskGroup(ABC):
    """A group of tasks that can be created and executed together."""
    @abstractmethod
    def create_task(
        self,
        coro: Awaitable[Any], *,
        name: str | None = None,
        context: Any | None = None
    ):
        pass


@dataclass(frozen=True, slots=True)
class TaskFunction(Generic[T]):
    """Encapsulates a callable with its own execution parameters.

    Retry timing:
    - First retry: initial_delay * uniform(0.5, 1.0)
    - Subsequent retries: initial_delay * (backoff_factor ** attempt) * uniform(0.5, 1.0)
    """
    function: Callable[[T], Awaitable[Any]] \
            | Callable[[T, SdaxTaskGroup],  Awaitable[Any]]
    timeout: float | None = None  # None means no timeout
    retries: int = 0
    initial_delay: float = 1.0  # Initial retry delay in seconds
    backoff_factor: float = 2.0
    retryable_exceptions: tuple[type[BaseException], ...] = \
        (TimeoutError, ConnectionError, RetryableException)
    has_task_group_argument: bool = False

    def __post_init__(self):
        if self.timeout is not None and self.timeout < 0.0:
            raise ValueError(f"timeout must be > 0.0, got {self.timeout}")
        if self.retries < 0:
            raise ValueError(f"retries must be >= 0, got {self.retries}")
        if self.initial_delay < 0:
            raise ValueError(f"initial_delay must be >= 0, got {self.initial_delay}")
        if self.backoff_factor <= 0.0:
            raise ValueError(f"backoff_factor must be > 1.0, got {self.backoff_factor}")
        if self.retries > 0 and not self.retryable_exceptions:
            raise ValueError("retryable_exceptions must be a non-empty tuple")

    def call(self, arg: T, task_group: SdaxTaskGroup) -> Awaitable[Any]:
        """Call the function with the given argument and task group."""
        if self.has_task_group_argument:
            return self.function(arg, task_group)
        return self.function(arg)


@dataclass(frozen=True, eq=False, order=False, slots=True)
class AsyncTask(Generic[T, K]):
    """A declarative definition of a task with optional pre-execute, execute,
    and post-execute phases, each with its own configuration."""

    name: K
    pre_execute: TaskFunction[T] | None = None
    execute: TaskFunction[T] | None = None
    post_execute: TaskFunction[T] | None = None

# Helper functions.

def task_func(
    function: Callable[[T], Awaitable[Any]], 
    *,
    timeout: float | None = None,  # None means no timeout
    retries: int = 0,
    initial_delay: float = 1.0,  # Initial retry delay in seconds
    backoff_factor: float = 2.0,
    retryable_exceptions: tuple[type[BaseException], ...] = \
        (TimeoutError, ConnectionError, RetryableException)
) -> TaskFunction[T]:
    """
    Creates a standard TaskFunction for a task.
    """

    return TaskFunction(
        function=function,
        timeout=timeout,
        retries=retries,
        initial_delay=initial_delay,
        backoff_factor=backoff_factor,
        retryable_exceptions=retryable_exceptions,
        has_task_group_argument=False
    )

def task_group_task(
    function: Callable[[T, SdaxTaskGroup], Awaitable[Any]], 
    *,
    timeout: float | None = None,  # None means no timeout
    retries: int = 0,
    initial_delay: float = 1.0,  # Initial retry delay in seconds
    backoff_factor: float = 2.0,
    retryable_exceptions: tuple[type[BaseException], ...] = \
        (TimeoutError, ConnectionError, RetryableException)
) -> TaskFunction[T]:
    """
    Creates a TaskFunction that takes the task group as its second argument.
    """

    return TaskFunction(
        function=function,
        timeout=timeout,
        retries=retries,
        initial_delay=initial_delay,
        backoff_factor=backoff_factor,
        retryable_exceptions=retryable_exceptions,
        has_task_group_argument=True
    )

def task_sync_func(
    function: Callable[[T], Any],
    *,
    retries: int = 0,
    initial_delay: float = 1.0,  # Initial retry delay in seconds
    backoff_factor: float = 2.0,
    retryable_exceptions: tuple[type[BaseException], ...] = \
        (TimeoutError, ConnectionError, RetryableException)
) -> TaskFunction[T]:
    """
    Creates a TaskFunction that wraps a synchronous function.
    """

    async def wrapper(arg: T) -> Any:
        return function(arg)

    return TaskFunction(
        function=wrapper,
        timeout=None,
        retries=retries,
        initial_delay=initial_delay,
        backoff_factor=backoff_factor,
        retryable_exceptions=retryable_exceptions,
        has_task_group_argument=False
    )

