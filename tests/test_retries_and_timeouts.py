import asyncio
import unittest
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict

from sdax import (
    AsyncTask,
    AsyncTaskProcessor,
    SdaxExecutionError,
    TaskFunction,
    flatten_exceptions,
)


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

        with self.assertRaises(SdaxExecutionError):
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

        with self.assertRaises(SdaxExecutionError) as cm:
            await processor.process_tasks(ctx)

        # Check that the underlying exception is indeed a TimeoutError
        leaves = flatten_exceptions(cm.exception)
        self.assertEqual(len(leaves), 1)
        self.assertIsInstance(leaves[0], asyncio.TimeoutError)

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

    async def test_empty_retryable_exceptions_no_retry(self):
        """Verify that empty retryable_exceptions prevents retries."""
        ctx = TaskContext()

        async def always_fails(context: TaskContext):
            global ATTEMPTS
            ATTEMPTS["always_fails"] += 1
            raise ValueError("This always fails")

        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="NoRetryTask",
                    execute=TaskFunction(
                        function=always_fails,
                        retries=3,  # Would normally retry 3 times
                        retryable_exceptions=(),  # Empty tuple = no retries
                    ),
                ),
                1,
            )
            .build()
        )

        # Should fail immediately without retries
        try:
            await processor.process_tasks(ctx)
            self.fail("Expected an exception to be raised")
        except SdaxExecutionError as eg:
            # Check that the underlying exception is ValueError
            leaves = flatten_exceptions(eg)
            self.assertEqual(len(leaves), 1)
            self.assertIsInstance(leaves[0], ValueError)
            self.assertEqual(str(leaves[0]), "This always fails")

        # Should only have been called once (no retries)
        self.assertEqual(ATTEMPTS["always_fails"], 1)

    async def test_custom_retryable_exceptions_success(self):
        """Verify that custom retryable_exceptions work correctly for retryable errors."""
        ctx = TaskContext()

        class CustomRetryableError(Exception):
            pass

        async def fails_with_retryable(context: TaskContext):
            global ATTEMPTS
            ATTEMPTS["fails_with_retryable"] += 1
            if ATTEMPTS["fails_with_retryable"] < 3:
                raise CustomRetryableError("Retryable error")
            context.data["succeeded"] = True

        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="RetryableTask",
                    execute=TaskFunction(
                        function=fails_with_retryable,
                        retries=3,
                        retryable_exceptions=(CustomRetryableError,),
                    ),
                ),
                1,
            )
            .build()
        )

        # Should succeed after retries
        await processor.process_tasks(ctx)
        self.assertTrue(ctx.data.get("succeeded"))
        self.assertEqual(ATTEMPTS["fails_with_retryable"], 3)

    async def test_custom_retryable_exceptions_failure(self):
        """Verify that custom retryable_exceptions work correctly for non-retryable errors."""
        ctx = TaskContext()

        class CustomRetryableError(Exception):
            pass

        class CustomNonRetryableError(Exception):
            pass

        async def fails_with_non_retryable(context: TaskContext):
            global ATTEMPTS
            ATTEMPTS["fails_with_non_retryable"] += 1
            raise CustomNonRetryableError("Non-retryable error")

        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="NonRetryableTask",
                    execute=TaskFunction(
                        function=fails_with_non_retryable,
                        retries=3,
                        retryable_exceptions=(CustomRetryableError,),  # Different exception type
                    ),
                ),
                1,
            )
            .build()
        )

        # Should fail immediately without retries
        try:
            await processor.process_tasks(ctx)
            self.fail("Expected an exception to be raised")
        except SdaxExecutionError as eg:
            # Check that the underlying exception is CustomNonRetryableError
            leaves = flatten_exceptions(eg)
            self.assertEqual(len(leaves), 1)
            self.assertIsInstance(leaves[0], CustomNonRetryableError)
            self.assertEqual(str(leaves[0]), "Non-retryable error")

        # Should only have been called once (no retries)
        self.assertEqual(ATTEMPTS["fails_with_non_retryable"], 1)

    async def test_exception_group_retryable_exceptions(self):
        """Ensure ExceptionGroups containing only retryable exceptions are retried."""
        ctx = TaskContext()

        class CustomRetryableError(Exception):
            pass

        async def fails_with_group(context: TaskContext):
            global ATTEMPTS
            ATTEMPTS["fails_with_group"] += 1
            if ATTEMPTS["fails_with_group"] < 3:
                raise ExceptionGroup("grouped failure", [CustomRetryableError("retry me")])
            context.data["succeeded"] = True

        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="GroupRetryTask",
                    execute=TaskFunction(
                        function=fails_with_group,
                        retries=3,
                        retryable_exceptions=(CustomRetryableError,),
                    ),
                ),
                1,
            )
            .build()
        )

        # Should succeed after retries even though ExceptionGroup is raised
        await processor.process_tasks(ctx)
        self.assertTrue(ctx.data.get("succeeded"))
        self.assertEqual(ATTEMPTS["fails_with_group"], 3)

    async def test_exception_group_with_non_retryable_fails(self):
        """Ensure mixed ExceptionGroups do not trigger retries."""
        ctx = TaskContext()

        class CustomRetryableError(Exception):
            pass

        class CustomNonRetryableError(Exception):
            pass

        async def fails_with_mixed_group(context: TaskContext):
            global ATTEMPTS
            ATTEMPTS["fails_with_mixed_group"] += 1
            raise ExceptionGroup(
                "mixed failure",
                [CustomRetryableError("retryable"), CustomNonRetryableError("non-retryable")],
            )

        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="MixedGroupTask",
                    execute=TaskFunction(
                        function=fails_with_mixed_group,
                        retries=3,
                        retryable_exceptions=(CustomRetryableError,),
                    ),
                ),
                1,
            )
            .build()
        )

        with self.assertRaises(SdaxExecutionError) as cm:
            await processor.process_tasks(ctx)

        leaves = flatten_exceptions(cm.exception)
        self.assertTrue(any(isinstance(exc, CustomNonRetryableError) for exc in leaves))
        self.assertEqual(ATTEMPTS["fails_with_mixed_group"], 1)


if __name__ == "__main__":
    unittest.main()
