"""
sdax - Structured Declarative Async eXecution

A lightweight, high-performance, in-process micro-orchestrator for structured,
declarative, and parallel asynchronous tasks in Python.
"""

from .sdax_core import (
    AsyncDagLevelAdapterBuilder,
    AsyncDagTaskProcessor,
    AsyncDagTaskProcessorBuilder,
    AsyncTaskProcessor,
    SdaxExecutionError,
    flatten_exceptions,
)
from .tasks import AsyncTask, RetryableException, SdaxTaskGroup, TaskFunction, task_func, task_group_task, task_sync_func

__version__ = "0.6.1"

__all__ = [
    "AsyncTask",
    "RetryableException",
    "SdaxTaskGroup",
    "TaskFunction",
    "task_func",
    "task_group_task",
    "task_sync_func",
    "AsyncTaskProcessor",
    "AsyncDagTaskProcessor",
    "AsyncDagTaskProcessorBuilder",
    "AsyncDagLevelAdapterBuilder",
    "SdaxExecutionError",
    "flatten_exceptions",
]
