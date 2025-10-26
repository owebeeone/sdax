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

