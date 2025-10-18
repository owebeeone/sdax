"""
sdax - Structured Declarative Async eXecution

A lightweight, high-performance, in-process micro-orchestrator for structured,
declarative, and parallel asynchronous tasks in Python.
"""

from .sdax_core import (
    AsyncTaskProcessor,
    AsyncDagTaskProcessor,
    AsyncDagTaskProcessorBuilder,
)
from .tasks import AsyncTask, TaskFunction

__version__ = "0.1.0"

__all__ = [
    "AsyncTask",
    "TaskFunction",
    "AsyncTaskProcessor",
    "AsyncDagTaskProcessor",
    "AsyncDagTaskProcessorBuilder",
]
