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
from .tasks import AsyncTask, RetryableException, SdaxTaskGroup, TaskFunction

__version__ = "0.5.2"

__all__ = [
    "AsyncTask",
    "RetryableException",
    "SdaxTaskGroup",
    "TaskFunction",
    "AsyncTaskProcessor",
    "AsyncDagTaskProcessor",
    "AsyncDagTaskProcessorBuilder",
    "AsyncDagLevelAdapterBuilder",
    "SdaxExecutionError",
    "flatten_exceptions",
]
