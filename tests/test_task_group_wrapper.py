import asyncio
import unittest
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Dict

from sdax import AsyncTask, AsyncTaskProcessor, SdaxTaskGroup, TaskFunction


@dataclass
class TaskContext:
    """A simple data-passing object for tasks to share state."""
    data: Dict = field(default_factory=dict)


class MockTaskGroup(SdaxTaskGroup):
    """Mock implementation of SdaxTaskGroup for testing."""

    def __init__(self):
        self.created_tasks = []
        self.task_group = asyncio.TaskGroup()

    def create_task(
        self,
        coro: Awaitable[Any],
        *,
        name: str | None = None,
        context: Any | None = None
    ):
        """Track created tasks and return a mock task."""
        self.created_tasks.append({
            'coro': coro,
            'name': name,
            'context': context
        })
        # Return a mock task that can be awaited
        return asyncio.create_task(coro, name=name)


# Global counters for tracking task execution
TASK_COUNTERS = defaultdict(int)
SUBTASK_RESULTS = []


class TestTaskGroupWrapper(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        """Reset counters before each test."""
        global TASK_COUNTERS, SUBTASK_RESULTS
        TASK_COUNTERS = defaultdict(int)
        SUBTASK_RESULTS = []

    async def test_task_group_wrapper_parameter_usage(self):
        """Test that tg_wrapper parameter is successfully passed and used."""
        ctx = TaskContext()

        async def subtask_1(context: TaskContext):
            """A simple subtask that increments a counter."""
            TASK_COUNTERS["subtask_1"] += 1
            SUBTASK_RESULTS.append("subtask_1_completed")
            return "subtask_1_result"

        async def subtask_2(context: TaskContext):
            """Another subtask that increments a counter."""
            TASK_COUNTERS["subtask_2"] += 1
            SUBTASK_RESULTS.append("subtask_2_completed")
            return "subtask_2_result"

        async def main_task_with_task_group(
            context: TaskContext, 
            task_group: SdaxTaskGroup
        ):
            """Main task that uses the task group to create subtasks."""
            TASK_COUNTERS["main_task"] += 1

            # Use the task group to create subtasks
            task1 = task_group.create_task(
                subtask_1(context), 
                name="subtask_1"
            )
            task2 = task_group.create_task(
                subtask_2(context), 
                name="subtask_2"
            )
            # Wait for both subtasks to complete
            result1 = await task1
            result2 = await task2
            # Store the results
            context.data["subtask_results"] = [result1, result2]
            SUBTASK_RESULTS.append("main_task_completed")
            return f"main_task_completed_with_results_{result1}_{result2}"

        # Create the processor with a task that uses the task group
        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="MainTaskWithTaskGroup",
                    execute=TaskFunction(
                        function=main_task_with_task_group,
                        has_task_group_argument=True,  # Important: indicates this task uses tg_wrapper
                        timeout=5.0,
                    ),
                ),
                1,
            )
            .build()
        )

        # Execute the processor
        await processor.process_tasks(ctx)

        # Verify the main task was executed
        self.assertEqual(TASK_COUNTERS["main_task"], 1)
        
        # Verify subtasks were created and executed
        self.assertEqual(TASK_COUNTERS["subtask_1"], 1)
        self.assertEqual(TASK_COUNTERS["subtask_2"], 1)
        
        # Verify all tasks completed
        self.assertIn("subtask_1_completed", SUBTASK_RESULTS)
        self.assertIn("subtask_2_completed", SUBTASK_RESULTS)
        self.assertIn("main_task_completed", SUBTASK_RESULTS)
        
        # Verify the context data was updated
        self.assertIn("subtask_results", ctx.data)
        self.assertEqual(ctx.data["subtask_results"], ["subtask_1_result", "subtask_2_result"])

    async def test_task_group_wrapper_with_async_subtasks(self):
        """Test that tg_wrapper can handle async subtasks with delays."""
        ctx = TaskContext()

        async def delayed_subtask(context: TaskContext, delay: float, name: str):
            """A subtask that takes some time to complete."""
            await asyncio.sleep(delay)
            TASK_COUNTERS[name] += 1
            SUBTASK_RESULTS.append(f"{name}_completed_after_{delay}s")
            return f"{name}_result"

        async def coordinator_task(
            context: TaskContext, 
            task_group: SdaxTaskGroup
        ):
            """Coordinator task that creates multiple async subtasks."""
            TASK_COUNTERS["coordinator"] += 1
            
            # Create multiple subtasks with different delays
            tasks = []
            for i in range(3):
                task = task_group.create_task(
                    delayed_subtask(context, 0.1 * (i + 1), f"subtask_{i}"),
                    name=f"delayed_subtask_{i}"
                )
                tasks.append(task)
            
            # Wait for all subtasks to complete
            results = await asyncio.gather(*tasks)
            
            # Store results in context
            context.data["delayed_results"] = results
            SUBTASK_RESULTS.append("coordinator_completed")
            
            return f"coordinator_completed_with_{len(results)}_results"

        # Create the processor
        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="CoordinatorTask",
                    execute=TaskFunction(
                        function=coordinator_task,
                        has_task_group_argument=True,
                        timeout=10.0,
                    ),
                ),
                1,
            )
            .build()
        )

        # Execute the processor
        await processor.process_tasks(ctx)

        # Verify coordinator was executed
        self.assertEqual(TASK_COUNTERS["coordinator"], 1)
        
        # Verify all subtasks were executed
        for i in range(3):
            self.assertEqual(TASK_COUNTERS[f"subtask_{i}"], 1)
        
        # Verify all completion markers are present
        for i in range(3):
            self.assertIn(f"subtask_{i}_completed_after_{0.1 * (i + 1)}s", SUBTASK_RESULTS)
        self.assertIn("coordinator_completed", SUBTASK_RESULTS)
        
        # Verify context data
        self.assertIn("delayed_results", ctx.data)
        self.assertEqual(len(ctx.data["delayed_results"]), 3)

    async def test_task_group_wrapper_error_handling(self):
        """Test that tg_wrapper handles errors in subtasks properly."""
        ctx = TaskContext()

        async def failing_subtask(context: TaskContext):
            """A subtask that always fails."""
            TASK_COUNTERS["failing_subtask"] += 1
            raise ValueError("Subtask failed intentionally")

        async def successful_subtask(context: TaskContext):
            """A subtask that succeeds."""
            TASK_COUNTERS["successful_subtask"] += 1
            SUBTASK_RESULTS.append("successful_subtask_completed")
            return "successful_subtask_result"

        async def error_handling_task(
            context: TaskContext, 
            task_group: SdaxTaskGroup
        ):
            """Task that handles errors from subtasks."""
            TASK_COUNTERS["error_handler"] += 1
            
            try:
                # Create both successful and failing subtasks
                success_task = task_group.create_task(
                    successful_subtask(context),
                    name="successful_subtask"
                )
                fail_task = task_group.create_task(
                    failing_subtask(context),
                    name="failing_subtask"
                )
                
                # Wait for both tasks
                results = await asyncio.gather(success_task, fail_task, return_exceptions=True)
                
                # Store results (including exceptions)
                context.data["subtask_results"] = results
                SUBTASK_RESULTS.append("error_handler_completed")
                
                return f"error_handler_completed_with_{len(results)}_results"
                
            except Exception as e:
                SUBTASK_RESULTS.append(f"error_handler_failed_{type(e).__name__}")
                raise

        # Create the processor
        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="ErrorHandlingTask",
                    execute=TaskFunction(
                        function=error_handling_task,
                        has_task_group_argument=True,
                        timeout=5.0,
                    ),
                ),
                1,
            )
            .build()
        )

        # Execute the processor - expect it to raise an exception due to the failing subtask
        with self.assertRaises(ExceptionGroup):
            await processor.process_tasks(ctx)

        # Verify error handler was executed
        self.assertEqual(TASK_COUNTERS["error_handler"], 1)
        
        # Verify both subtasks were executed
        self.assertEqual(TASK_COUNTERS["successful_subtask"], 1)
        self.assertEqual(TASK_COUNTERS["failing_subtask"], 1)
        
        # Verify completion markers
        self.assertIn("successful_subtask_completed", SUBTASK_RESULTS)
        # Note: error_handler_completed might not be reached due to the exception
        
        # Verify context data contains both success and error (if the error handler completed)
        if "subtask_results" in ctx.data:
            results = ctx.data["subtask_results"]
            self.assertEqual(len(results), 2)
            
            # One should be successful, one should be an exception
            success_results = [r for r in results if not isinstance(r, Exception)]
            error_results = [r for r in results if isinstance(r, Exception)]
            
            self.assertEqual(len(success_results), 1)
            self.assertEqual(len(error_results), 1)
            self.assertIsInstance(error_results[0], ValueError)
            self.assertEqual(str(error_results[0]), "Subtask failed intentionally")


if __name__ == "__main__":
    unittest.main()
