from dataclasses import dataclass
from typing import Awaitable, Callable, Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class TaskFunction(Generic[T]):
    """Encapsulates a callable with its own execution parameters.

    Retry timing:
    - First retry: initial_delay * uniform(0.5, 1.0)
    - Subsequent retries: initial_delay * (backoff_factor ** attempt) * uniform(0.5, 1.0)
    """
    function: Callable[[T], Awaitable[None]]
    timeout: float | None = 2.0  # None means no timeout
    retries: int = 0
    initial_delay: float = 1.0  # Initial retry delay in seconds
    backoff_factor: float = 2.0


@dataclass(frozen=True)
class AsyncTask(Generic[T]):
    """A declarative definition of a task with optional pre-execute, execute,
    and post-execute phases, each with its own configuration."""

    name: str
    pre_execute: TaskFunction[T] | None = None
    execute: TaskFunction[T] | None = None
    post_execute: TaskFunction[T] | None = None

