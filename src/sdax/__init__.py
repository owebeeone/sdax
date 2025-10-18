"""
sdax - Structured Declarative Async eXecution

A lightweight, high-performance, in-process micro-orchestrator for structured,
declarative, and parallel asynchronous tasks in Python.
"""
__version__ = "0.1.0"

from .sdax_core import (
    AsyncTask,
    AsyncTaskProcessor,
    TaskFunction,
)

__all__ = [
    "AsyncTask",
    "AsyncTaskProcessor",
    "TaskFunction",
]
