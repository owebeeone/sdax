"""
Tests for task helper functions: task_func and task_group_task.
"""

import asyncio
import pytest
import unittest
from unittest.mock import AsyncMock, MagicMock

from sdax import (
    AsyncTask,
    AsyncTaskProcessor,
    RetryableException,
    SdaxTaskGroup,
    TaskFunction,
    task_func,
    task_group_task,
    task_sync_func,
)


class TestTaskHelperFunctions(unittest.IsolatedAsyncioTestCase):
    """Test the task_func and task_group_task helper functions."""

    async def test_task_func_basic_usage(self):
        """Test basic usage of task_func helper."""
        async def simple_task(ctx):
            return "Hello, World!"

        # Create TaskFunction using helper
        task_function = task_func(simple_task)
        
        # Verify it's a TaskFunction instance
        assert isinstance(task_function, TaskFunction)
        assert task_function.function == simple_task
        assert task_function.timeout is None
        assert task_function.retries == 0
        assert task_function.initial_delay == 1.0
        assert task_function.backoff_factor == 2.0
        assert task_function.has_task_group_argument is False

    async def test_task_func_with_custom_config(self):
        """Test task_func with custom configuration."""
        async def custom_task(ctx):
            return ctx.get("value", "default")

        # Create TaskFunction with custom settings
        task_function = task_func(
            custom_task,
            timeout=30.0,
            retries=3,
            initial_delay=0.5,
            backoff_factor=1.5,
            retryable_exceptions=(ValueError, RuntimeError)
        )
        
        assert task_function.timeout == 30.0
        assert task_function.retries == 3
        assert task_function.initial_delay == 0.5
        assert task_function.backoff_factor == 1.5
        assert task_function.retryable_exceptions == (ValueError, RuntimeError)

    async def test_task_func_in_processor(self):
        """Test task_func in actual processor execution."""
        async def add_one(ctx):
            result = ctx.get("count", 0) + 1
            ctx["add_one"] = result
            return result

        async def multiply_by_two(ctx):
            result = ctx.get("count", 0) * 2
            ctx["multiply_by_two"] = result
            return result

        # Create tasks using helper functions
        task1 = AsyncTask("add_one", execute=task_func(add_one))
        task2 = AsyncTask("multiply_by_two", execute=task_func(multiply_by_two))

        # Build processor
        processor = (
            AsyncTaskProcessor.builder()
            .add_task(task1, level=1)
            .add_task(task2, level=2)
            .build()
        )

        # Execute
        ctx = {"count": 5}
        await processor.process_tasks(ctx)

        # Verify results
        assert ctx["add_one"] == 6
        assert ctx["multiply_by_two"] == 10

    async def test_task_group_task_basic_usage(self):
        """Test basic usage of task_group_task helper."""
        async def group_task(ctx, task_group: SdaxTaskGroup):
            return f"Group has create_task method: {hasattr(task_group, 'create_task')}"

        # Create TaskFunction using helper
        task_function = task_group_task(group_task)
        
        assert isinstance(task_function, TaskFunction)
        assert task_function.function == group_task
        assert task_function.has_task_group_argument is True

    async def test_task_group_task_with_custom_config(self):
        """Test task_group_task with custom configuration."""
        async def group_task(ctx, task_group: SdaxTaskGroup):
            return "custom_config_test"

        # Create TaskFunction with custom settings
        task_function = task_group_task(
            group_task,
            timeout=60.0,
            retries=5,
            initial_delay=2.0,
            backoff_factor=3.0
        )
        
        assert task_function.timeout == 60.0
        assert task_function.retries == 5
        assert task_function.initial_delay == 2.0
        assert task_function.backoff_factor == 3.0
        assert task_function.has_task_group_argument is True

    async def test_task_group_task_in_processor(self):
        """Test task_group_task in actual processor execution."""
        async def count_tasks(ctx, task_group: SdaxTaskGroup):
            # Just verify we can call create_task
            task = task_group.create_task(asyncio.sleep(0.001), name="test_task")
            await task
            ctx["count_tasks"] = "success"

        async def get_task_names(ctx, task_group: SdaxTaskGroup):
            # Just verify we can call create_task
            task = task_group.create_task(asyncio.sleep(0.001), name="test_task2")
            await task
            ctx["get_task_names"] = "success"

        # Create tasks using helper functions
        task1 = AsyncTask("count_tasks", execute=task_group_task(count_tasks))
        task2 = AsyncTask("get_task_names", execute=task_group_task(get_task_names))

        # Build processor
        processor = (
            AsyncTaskProcessor.builder()
            .add_task(task1, level=1)
            .add_task(task2, level=1)
            .build()
        )

        # Execute
        ctx = {}
        await processor.process_tasks(ctx)

        # Verify results
        assert ctx["count_tasks"] == "success"
        assert ctx["get_task_names"] == "success"

    async def test_helper_functions_vs_direct_construction(self):
        """Test that helper functions produce equivalent TaskFunction instances."""
        async def test_function(ctx):
            return "test"

        # Create using helper
        helper_task = task_func(test_function, timeout=10.0, retries=2)
        
        # Create directly
        direct_task = TaskFunction(
            function=test_function,
            timeout=10.0,
            retries=2,
            initial_delay=1.0,
            backoff_factor=2.0,
            retryable_exceptions=(TimeoutError, ConnectionError, RetryableException),
            has_task_group_argument=False
        )
        
        # They should be equivalent
        assert helper_task.function == direct_task.function
        assert helper_task.timeout == direct_task.timeout
        assert helper_task.retries == direct_task.retries
        assert helper_task.initial_delay == direct_task.initial_delay
        assert helper_task.backoff_factor == direct_task.backoff_factor
        assert helper_task.retryable_exceptions == direct_task.retryable_exceptions
        assert helper_task.has_task_group_argument == direct_task.has_task_group_argument

    async def test_helper_functions_with_retryable_exceptions(self):
        """Test helper functions with custom retryable exceptions."""
        async def failing_task(ctx):
            raise ValueError("Test error")

        # Create task with custom retryable exceptions
        task_function = task_func(
            failing_task,
            retries=2,
            retryable_exceptions=(ValueError,)
        )
        
        assert task_function.retryable_exceptions == (ValueError,)

    async def test_keyword_only_parameters(self):
        """Test that helper functions enforce keyword-only parameters."""
        async def test_func(ctx):
            return "test"

        # This should work (all keyword arguments)
        task_func(test_func, timeout=5.0, retries=1)
        
        # This should fail (positional arguments)
        with pytest.raises(TypeError):
            task_func(test_func, 5.0, 1)  # Should raise TypeError

    async def test_default_values(self):
        """Test that helper functions use correct default values."""
        async def test_func(ctx):
            return "test"

        task_function = task_func(test_func)
        
        # Check defaults match documentation
        assert task_function.timeout is None
        assert task_function.retries == 0
        assert task_function.initial_delay == 1.0
        assert task_function.backoff_factor == 2.0
        assert task_function.retryable_exceptions == (TimeoutError, ConnectionError, RetryableException)
        assert task_function.has_task_group_argument is False

    async def test_task_group_task_defaults(self):
        """Test that task_group_task uses correct default values."""
        async def test_func(ctx, task_group: SdaxTaskGroup):
            return "test"

        task_function = task_group_task(test_func)
        
        # Check defaults
        assert task_function.timeout is None
        assert task_function.retries == 0
        assert task_function.initial_delay == 1.0
        assert task_function.backoff_factor == 2.0
        assert task_function.retryable_exceptions == (TimeoutError, ConnectionError, RetryableException)
        assert task_function.has_task_group_argument is True

    async def test_task_sync_func_basic_usage(self):
        """Test basic usage of task_sync_func helper."""
        def simple_sync_task(ctx):
            return f"Hello from sync: {ctx.get('name', 'World')}!"

        # Create TaskFunction using helper
        task_function = task_sync_func(simple_sync_task)
        
        # Verify it's a TaskFunction instance
        assert isinstance(task_function, TaskFunction)
        assert task_function.timeout is None
        assert task_function.retries == 0
        assert task_function.initial_delay == 1.0
        assert task_function.backoff_factor == 2.0
        assert task_function.has_task_group_argument is False

        # Test that the wrapper function works
        result = await task_function.function({"name": "Test"})
        assert result == "Hello from sync: Test!"

    async def test_task_sync_func_with_custom_config(self):
        """Test task_sync_func with custom configuration."""
        def custom_sync_task(ctx):
            return ctx.get("value", 0) * 2

        # Create TaskFunction with custom settings
        task_function = task_sync_func(
            custom_sync_task,
            retries=3,
            initial_delay=0.5,
            backoff_factor=1.5,
            retryable_exceptions=(ValueError, RuntimeError)
        )
        
        assert task_function.timeout is None  # Always None for sync functions
        assert task_function.retries == 3
        assert task_function.initial_delay == 0.5
        assert task_function.backoff_factor == 1.5
        assert task_function.retryable_exceptions == (ValueError, RuntimeError)
        assert task_function.has_task_group_argument is False

        # Test execution
        result = await task_function.function({"value": 5})
        assert result == 10

    async def test_task_sync_func_in_processor(self):
        """Test task_sync_func in actual processor execution."""
        def add_one_sync(ctx):
            result = ctx.get("count", 0) + 1
            ctx["add_one_sync"] = result
            return result

        def multiply_by_two_sync(ctx):
            result = ctx.get("count", 0) * 2
            ctx["multiply_by_two_sync"] = result
            return result

        # Create tasks using helper functions
        task1 = AsyncTask("add_one_sync", execute=task_sync_func(add_one_sync))
        task2 = AsyncTask("multiply_by_two_sync", execute=task_sync_func(multiply_by_two_sync))

        # Build processor
        processor = (
            AsyncTaskProcessor.builder()
            .add_task(task1, level=1)
            .add_task(task2, level=2)
            .build()
        )

        # Execute
        ctx = {"count": 5}
        await processor.process_tasks(ctx)

        # Verify results
        assert ctx["add_one_sync"] == 6
        assert ctx["multiply_by_two_sync"] == 10

    async def test_task_sync_func_vs_task_func(self):
        """Test that task_sync_func produces equivalent TaskFunction to task_func with sync wrapper."""
        def sync_function(ctx):
            return ctx.get("value", 0) + 1

        # Create using task_sync_func
        sync_task = task_sync_func(sync_function, retries=2)
        
        # Create equivalent using task_func with async wrapper
        async def async_wrapper(ctx):
            return sync_function(ctx)
        
        async_task = task_func(async_wrapper, retries=2)
        
        # They should have equivalent configuration
        assert sync_task.timeout == async_task.timeout
        assert sync_task.retries == async_task.retries
        assert sync_task.initial_delay == async_task.initial_delay
        assert sync_task.backoff_factor == async_task.backoff_factor
        assert sync_task.retryable_exceptions == async_task.retryable_exceptions
        assert sync_task.has_task_group_argument == async_task.has_task_group_argument

        # Both should produce the same result
        test_ctx = {"value": 5}
        sync_result = await sync_task.function(test_ctx)
        async_result = await async_task.function(test_ctx)
        assert sync_result == async_result == 6

    async def test_task_sync_func_exception_handling(self):
        """Test that task_sync_func properly handles exceptions."""
        def failing_sync_task(ctx):
            raise ValueError("Sync function failed")

        task_function = task_sync_func(failing_sync_task)
        
        # Test that exceptions are properly propagated
        with pytest.raises(ValueError, match="Sync function failed"):
            await task_function.function({})

    async def test_task_sync_func_keyword_only_parameters(self):
        """Test that task_sync_func enforces keyword-only parameters."""
        def test_func(ctx):
            return "test"

        # This should work (all keyword arguments)
        task_sync_func(test_func, retries=1)
        
        # This should fail (positional arguments)
        with pytest.raises(TypeError):
            task_sync_func(test_func, 1)  # Should raise TypeError

    async def test_task_sync_func_default_values(self):
        """Test that task_sync_func uses correct default values."""
        def test_func(ctx):
            return "test"

        task_function = task_sync_func(test_func)
        
        # Check defaults match documentation
        assert task_function.timeout is None  # Always None for sync functions
        assert task_function.retries == 0
        assert task_function.initial_delay == 1.0
        assert task_function.backoff_factor == 2.0
        assert task_function.retryable_exceptions == (TimeoutError, ConnectionError, RetryableException)
        assert task_function.has_task_group_argument is False

    async def test_task_sync_func_mixed_with_async_tasks(self):
        """Test mixing sync and async tasks in the same processor."""
        def sync_task(ctx):
            ctx["sync_result"] = "sync"
            return "sync"

        async def async_task(ctx):
            ctx["async_result"] = "async"
            return "async"

        # Create mixed tasks
        sync_task_func = AsyncTask("sync_task", execute=task_sync_func(sync_task))
        async_task_func = AsyncTask("async_task", execute=task_func(async_task))

        # Build processor
        processor = (
            AsyncTaskProcessor.builder()
            .add_task(sync_task_func, level=1)
            .add_task(async_task_func, level=1)
            .build()
        )

        # Execute
        ctx = {}
        await processor.process_tasks(ctx)

        # Verify both tasks executed
        assert ctx["sync_result"] == "sync"
        assert ctx["async_result"] == "async"
