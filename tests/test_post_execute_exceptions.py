"""Test to verify post_execute exception handling guarantees.

This tests the critical invariant: if one post_execute raises an exception,
ALL other post_execute tasks must still complete (no cancellation).
"""
import asyncio
import unittest
from dataclasses import dataclass, field
from typing import Dict, List

from sdax import AsyncTask, AsyncTaskProcessor, TaskFunction


@dataclass
class TaskContext:
    data: Dict = field(default_factory=dict)


CALL_LOG: List[str] = []


async def noop_pre(ctx: TaskContext):
    """Successful pre_execute."""
    CALL_LOG.append("pre")


async def failing_post_A(ctx: TaskContext):
    """A post_execute that raises an exception."""
    CALL_LOG.append("failing_post_A_started")
    await asyncio.sleep(0.01)
    CALL_LOG.append("failing_post_A_raising")
    raise ValueError("Post-execute A failed!")


async def cleanup_post_B(ctx: TaskContext):
    """A post_execute that should clean up resources."""
    CALL_LOG.append("cleanup_post_B_started")
    await asyncio.sleep(0.02)  # Takes slightly longer
    CALL_LOG.append("cleanup_post_B_completed")


async def cleanup_post_C(ctx: TaskContext):
    """Another post_execute that should clean up resources."""
    CALL_LOG.append("cleanup_post_C_started")
    await asyncio.sleep(0.015)
    CALL_LOG.append("cleanup_post_C_completed")


class TestPostExecuteExceptions(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        """Reset call log before each test."""
        global CALL_LOG
        CALL_LOG = []

    async def test_post_execute_exception_does_not_cancel_siblings(self):
        """
        CRITICAL TEST: If one post_execute raises an exception, other post_execute
        tasks must still complete. This is the core guarantee for resource cleanup.
        
        Scenario:
        - Level 1 has 3 tasks, all with pre_execute and post_execute
        - Task A's post_execute raises an exception
        - Tasks B and C's post_execute must still complete (not be cancelled)
        
        Expected behavior:
        - All 3 pre_execute run successfully
        - All 3 post_execute start
        - Task A's post_execute raises exception
        - Tasks B and C's post_execute complete despite A's exception
        """
        processor = AsyncTaskProcessor()
        ctx = TaskContext()

        # Three tasks at level 1, all with pre_execute and post_execute
        processor.add_task(
            AsyncTask(
                name="FailingTask",
                pre_execute=TaskFunction(noop_pre),
                post_execute=TaskFunction(failing_post_A),
            ),
            level=1,
        )

        processor.add_task(
            AsyncTask(
                name="CleanupTaskB",
                pre_execute=TaskFunction(noop_pre),
                post_execute=TaskFunction(cleanup_post_B),
            ),
            level=1,
        )

        processor.add_task(
            AsyncTask(
                name="CleanupTaskC",
                pre_execute=TaskFunction(noop_pre),
                post_execute=TaskFunction(cleanup_post_C),
            ),
            level=1,
        )

        # Process should raise exception, but all cleanup should complete
        with self.assertRaises(ExceptionGroup):
            await processor.process_tasks(ctx)

        # Verify all pre_execute ran
        pre_count = CALL_LOG.count("pre")
        self.assertEqual(pre_count, 3, "All 3 pre_execute should run")

        # Verify all post_execute started
        self.assertIn("failing_post_A_started", CALL_LOG)
        self.assertIn("cleanup_post_B_started", CALL_LOG)
        self.assertIn("cleanup_post_C_started", CALL_LOG)

        # CRITICAL: Verify all post_execute completed (even though A raised exception)
        self.assertIn("failing_post_A_raising", CALL_LOG)
        self.assertIn(
            "cleanup_post_B_completed",
            CALL_LOG,
            "Task B's post_execute must complete even though A raised exception",
        )
        self.assertIn(
            "cleanup_post_C_completed",
            CALL_LOG,
            "Task C's post_execute must complete even though A raised exception",
        )

    async def test_multiple_post_execute_exceptions(self):
        """
        Test that if multiple post_execute raise exceptions, all still run to completion.
        """
        global CALL_LOG
        CALL_LOG = []

        async def failing_post_1(ctx: TaskContext):
            CALL_LOG.append("post_1_started")
            await asyncio.sleep(0.01)
            CALL_LOG.append("post_1_raising")
            raise ValueError("Post 1 failed")

        async def failing_post_2(ctx: TaskContext):
            CALL_LOG.append("post_2_started")
            await asyncio.sleep(0.02)
            CALL_LOG.append("post_2_raising")
            raise RuntimeError("Post 2 failed")

        async def cleanup_post_3(ctx: TaskContext):
            CALL_LOG.append("post_3_started")
            await asyncio.sleep(0.015)
            CALL_LOG.append("post_3_completed")

        processor = AsyncTaskProcessor()
        ctx = TaskContext()

        processor.add_task(
            AsyncTask(
                name="Fail1",
                pre_execute=TaskFunction(noop_pre),
                post_execute=TaskFunction(failing_post_1),
            ),
            level=1,
        )

        processor.add_task(
            AsyncTask(
                name="Fail2",
                pre_execute=TaskFunction(noop_pre),
                post_execute=TaskFunction(failing_post_2),
            ),
            level=1,
        )

        processor.add_task(
            AsyncTask(
                name="Cleanup3",
                pre_execute=TaskFunction(noop_pre),
                post_execute=TaskFunction(cleanup_post_3),
            ),
            level=1,
        )

        with self.assertRaises(ExceptionGroup):
            await processor.process_tasks(ctx)

        # All post_execute must complete
        self.assertIn("post_1_raising", CALL_LOG)
        self.assertIn("post_2_raising", CALL_LOG)
        self.assertIn("post_3_completed", CALL_LOG, "Cleanup must complete despite other failures")


if __name__ == "__main__":
    unittest.main()

