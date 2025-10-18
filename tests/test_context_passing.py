import asyncio
import unittest
from dataclasses import dataclass, field
from typing import Dict

from sdax import AsyncTask, AsyncTaskProcessor, TaskFunction


@dataclass
class TaskContext:
    """A simple data-passing object for tasks to share state."""
    data: Dict = field(default_factory=dict)


class TestSdaxContextPassing(unittest.IsolatedAsyncioTestCase):
    async def test_context_is_shared_and_mutable(self):
        """Verify that all tasks receive the same context and can modify it."""
        ctx = TaskContext()

        async def set_value(context: TaskContext):
            await asyncio.sleep(0.01)
            context.data["level1_seen"] = True

        async def check_value(context: TaskContext):
            await asyncio.sleep(0.01)
            self.assertTrue(context.data.get("level1_seen"))

        processor = (
            AsyncTaskProcessor.builder()
            .add_task(AsyncTask("Setter", pre_execute=TaskFunction(set_value)), 1)
            .add_task(AsyncTask("Checker", execute=TaskFunction(check_value)), 2)
            .build()
        )

        await processor.process_tasks(ctx)

        # Final check on the original context object
        self.assertTrue(ctx.data.get("level1_seen"))

    # Add more test cases for context management here...


if __name__ == "__main__":
    unittest.main()
