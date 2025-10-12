"""Test concurrent execution of the same AsyncTaskProcessor instance.

Verifies that multiple concurrent executions maintain isolated contexts
without race conditions.
"""
import asyncio
import unittest
from dataclasses import dataclass, field
from typing import Dict

from sdax import AsyncTask, AsyncTaskProcessor, TaskFunction


@dataclass
class TaskContext:
    """Test context with execution ID for tracking."""
    execution_id: int
    data: Dict = field(default_factory=dict)


async def set_execution_id(ctx: TaskContext):
    """Task that records its execution ID."""
    await asyncio.sleep(0.01)
    ctx.data["recorded_id"] = ctx.execution_id


async def increment_counter(ctx: TaskContext):
    """Task that increments a counter in the context."""
    await asyncio.sleep(0.005)
    count = ctx.data.get("count", 0)
    ctx.data["count"] = count + 1


async def verify_execution_id(ctx: TaskContext):
    """Task that verifies the execution ID hasn't changed."""
    await asyncio.sleep(0.01)
    recorded = ctx.data.get("recorded_id")
    if recorded != ctx.execution_id:
        raise ValueError(
            f"Context corruption! Expected ID {ctx.execution_id}, got {recorded}"
        )


class TestConcurrentExecution(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_executions_isolated_contexts(self):
        """
        Test that multiple concurrent executions of the same processor
        maintain isolated contexts without interference.
        
        This would have failed before the _ExecutionContext refactor.
        """
        # Create a single processor with tasks
        processor = AsyncTaskProcessor()
        
        processor.add_task(
            AsyncTask(
                name="SetID",
                pre_execute=TaskFunction(set_execution_id),
            ),
            level=1,
        )
        
        processor.add_task(
            AsyncTask(
                name="Increment1",
                execute=TaskFunction(increment_counter),
            ),
            level=1,
        )
        
        processor.add_task(
            AsyncTask(
                name="Increment2",
                execute=TaskFunction(increment_counter),
            ),
            level=1,
        )
        
        processor.add_task(
            AsyncTask(
                name="Verify",
                post_execute=TaskFunction(verify_execution_id),
            ),
            level=2,
        )
        
        # Create multiple contexts with different execution IDs
        contexts = [TaskContext(execution_id=i) for i in range(10)]
        
        # Run all executions concurrently
        results = await asyncio.gather(
            *[processor.process_tasks(ctx) for ctx in contexts],
            return_exceptions=True
        )
        
        # Verify no exceptions occurred
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.fail(f"Execution {i} failed: {result}")
        
        # Verify each context maintained its isolation
        for ctx in contexts:
            self.assertEqual(
                ctx.data["recorded_id"],
                ctx.execution_id,
                f"Execution {ctx.execution_id} had context corruption",
            )
            self.assertEqual(
                ctx.data["count"],
                2,
                f"Execution {ctx.execution_id} didn't complete all increments",
            )

    async def test_concurrent_executions_with_failures(self):
        """
        Test concurrent executions where some fail, ensuring failures
        don't interfere with successful executions.
        """
        processor = AsyncTaskProcessor()
        
        async def maybe_fail(ctx: TaskContext):
            """Fail if execution_id is even."""
            await asyncio.sleep(0.01)
            ctx.data["executed"] = True
            if ctx.execution_id % 2 == 0:
                raise ValueError(f"Execution {ctx.execution_id} failed intentionally")
        
        processor.add_task(
            AsyncTask(name="MaybeFail", execute=TaskFunction(maybe_fail)),
            level=1,
        )
        
        # Run 6 concurrent executions (IDs 0-5, evens will fail)
        contexts = [TaskContext(execution_id=i) for i in range(6)]
        
        results = await asyncio.gather(
            *[processor.process_tasks(ctx) for ctx in contexts],
            return_exceptions=True
        )
        
        # Verify even-numbered executions failed
        for i, result in enumerate(results):
            if i % 2 == 0:
                self.assertIsInstance(
                    result,
                    (ExceptionGroup, ValueError),
                    f"Execution {i} should have failed",
                )
            else:
                self.assertIsNone(
                    result,
                    f"Execution {i} should have succeeded but got: {result}",
                )
        
        # Verify all contexts executed (even the ones that failed)
        for ctx in contexts:
            self.assertTrue(
                ctx.data.get("executed"),
                f"Execution {ctx.execution_id} didn't execute",
            )

    async def test_high_concurrency_stress_test(self):
        """
        Stress test with many concurrent executions to catch race conditions.
        """
        processor = AsyncTaskProcessor()
        
        # Add multiple tasks across multiple levels
        for level in range(1, 4):
            for task_num in range(5):
                async def task_func(ctx: TaskContext, lvl=level, num=task_num):
                    await asyncio.sleep(0.001)
                    key = f"L{lvl}T{num}"
                    ctx.data[key] = ctx.execution_id
                
                processor.add_task(
                    AsyncTask(
                        name=f"L{level}T{task_num}",
                        execute=TaskFunction(task_func),
                    ),
                    level=level,
                )
        
        # Run 50 concurrent executions
        NUM_EXECUTIONS = 50
        contexts = [TaskContext(execution_id=i) for i in range(NUM_EXECUTIONS)]
        
        results = await asyncio.gather(
            *[processor.process_tasks(ctx) for ctx in contexts],
            return_exceptions=True
        )
        
        # Verify all succeeded
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.fail(f"Execution {i} failed: {result}")
        
        # Verify each context has correct data
        for ctx in contexts:
            # Should have data from all tasks (3 levels * 5 tasks = 15 entries)
            self.assertEqual(
                len(ctx.data),
                15,
                f"Execution {ctx.execution_id} missing data",
            )
            
            # All values should be this execution's ID
            for key, value in ctx.data.items():
                self.assertEqual(
                    value,
                    ctx.execution_id,
                    f"Execution {ctx.execution_id}: {key} has wrong value {value}",
                )


if __name__ == "__main__":
    unittest.main()

