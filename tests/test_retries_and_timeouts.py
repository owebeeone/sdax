import asyncio
import unittest
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict

from sdax import AsyncTask, AsyncTaskProcessor, TaskFunction


@dataclass
class TaskContext:
    """A simple data-passing object for tasks to share state."""
    data: Dict = field(default_factory=dict)


ATTEMPTS = defaultdict(int)


class TestSdaxRetriesAndTimeouts(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        """Reset attempts counter before each test."""
        global ATTEMPTS
        ATTEMPTS = defaultdict(int)

    async def test_retry_logic_succeeds(self):
        """Verify that a task succeeds after a number of retries."""
        ctx = TaskContext()

        async def fail_then_succeed(context: TaskContext):
            global ATTEMPTS
            ATTEMPTS["retry_task"] += 1
            if ATTEMPTS["retry_task"] < 3:
                raise ConnectionError("Simulated temporary failure")
            # Success on the 3rd attempt

        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="RetryTask",
                    execute=TaskFunction(
                        function=fail_then_succeed,
                        retries=3,  # Allow enough retries to succeed
                        backoff_factor=0.1,  # Keep test fast
                    ),
                ),
                1,
            )
            .build()
        )

        await processor.process_tasks(ctx)
        self.assertEqual(ATTEMPTS["retry_task"], 3)

    async def test_retry_logic_fails_after_exhaustion(self):
        """Verify that a task fails permanently if it exceeds its retries."""
        ctx = TaskContext()

        async def always_fail(context: TaskContext):
            global ATTEMPTS
            ATTEMPTS["fail_task"] += 1
            raise ConnectionError("Simulated permanent failure")

        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="FailTask",
                    execute=TaskFunction(function=always_fail, retries=2, backoff_factor=0.1),
                ),
                1,
            )
            .build()
        )

        with self.assertRaises(ExceptionGroup):
            await processor.process_tasks(ctx)

        # It should try once, then retry twice, for a total of 3 attempts
        self.assertEqual(ATTEMPTS["fail_task"], 3)

    async def test_timeout_is_enforced(self):
        """Verify that a task that takes too long is correctly timed out."""
        ctx = TaskContext()

        async def slow_task(context: TaskContext):
            await asyncio.sleep(1)  # This will take too long

        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="SlowTask",
                    pre_execute=TaskFunction(
                        function=slow_task,
                        timeout=0.1,  # Set a very short timeout
                    ),
                ),
                1,
            )
            .build()
        )

        with self.assertRaises(ExceptionGroup) as cm:
            await processor.process_tasks(ctx)

        # Check that the underlying exception is indeed a TimeoutError
        self.assertIsInstance(cm.exception.exceptions[0], asyncio.TimeoutError)

    async def test_no_timeout_with_none(self):
        """Verify that timeout=None allows tasks to run indefinitely."""
        ctx = TaskContext()

        async def long_running_task(context: TaskContext):
            await asyncio.sleep(0.5)  # Takes a while
            context.data["completed"] = True

        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="LongTask",
                    execute=TaskFunction(
                        function=long_running_task,
                        timeout=None,  # No timeout - should complete
                    ),
                ),
                1,
            )
            .build()
        )

        # Should complete successfully without timing out
        await processor.process_tasks(ctx)
        self.assertTrue(ctx.data.get("completed"))


if __name__ == "__main__":
    unittest.main()
