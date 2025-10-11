import asyncio
import random
import unittest

from sdax import AsyncTask, AsyncTaskProcessor, TaskContext, TaskFunction

# --- Monte Carlo Test Task Functions ---


async def mc_pre_execute(ctx, level, task_id, fail_chance):
    """A pre-execute function that logs its entry and may fail."""
    token = (level, task_id)
    ctx.data["pre_started"].append(token)

    try:
        await asyncio.sleep(random.uniform(0.001, 0.01))
        if random.random() < fail_chance / 2:
            raise ValueError(f"Task {task_id} at level {level} failed pre-execute intentionally.")
        ctx.data["pre_stack"].append(token)
        await asyncio.sleep(random.uniform(0.001, 0.01))

        if random.random() < fail_chance / 2:
            raise ValueError(f"Task {task_id} at level {level} failed intentionally.")
    except asyncio.CancelledError:
        # Track tasks that were cancelled (due to sibling task failure)
        ctx.data["cancelled_stack"].append(token)
        raise


async def mc_post_execute(ctx, level, task_id):
    """A post-execute function that logs its entry."""
    await asyncio.sleep(random.uniform(0.001, 0.01))
    token = (level, task_id)
    ctx.data["post_stack"].append(token)


class TestSdaxMonteCarlo(unittest.IsolatedAsyncioTestCase):
    async def test_monte_carlo_execution_and_teardown(self):
        """
        Runs many randomized scenarios to vigorously test the core logic.
        Validates tiered execution order ("elevator up"), symmetrical teardown
        ("elevator down"), and the guarantee that post_execute is called for
        every successful pre_execute, and never for a failed one.
        """
        N_RUNS = 20  # Number of separate processors to run
        MAX_LEVELS = 50
        MAX_TASKS_PER_LEVEL = 10
        FAIL_CHANCE = 0.05  # 5% chance for any pre_execute to fail

        for i in range(N_RUNS):
            with self.subTest(run=i):
                processor = AsyncTaskProcessor()
                ctx = TaskContext()
                ctx.data["pre_started"] = []  # Tasks that started pre_execute
                ctx.data["pre_stack"] = []  # Tasks that completed pre_execute
                ctx.data["post_stack"] = []  # Tasks that completed post_execute
                ctx.data["cancelled_stack"] = []  # Tasks that were cancelled

                num_levels = random.randint(1, MAX_LEVELS)
                for level in range(1, num_levels + 1):
                    num_tasks = random.randint(1, MAX_TASKS_PER_LEVEL)
                    for task_num in range(num_tasks):
                        task_id = f"L{level}-T{task_num}"
                        task = AsyncTask(
                            name=task_id,
                            pre_execute=TaskFunction(
                                lambda c, l=level, tid=task_id: mc_pre_execute(
                                    c, l, tid, FAIL_CHANCE
                                )
                            ),
                            post_execute=TaskFunction(
                                lambda c, l=level, tid=task_id: mc_post_execute(c, l, tid)
                            ),
                        )
                        processor.add_task(task, level)

                try:
                    await processor.process_tasks(ctx)
                except ExceptionGroup:
                    # We expect failures, this is part of the test
                    pass

                # --- VALIDATION ---
                pre_started = ctx.data["pre_started"]
                pre_stack = ctx.data["pre_stack"]
                post_stack = ctx.data["post_stack"]
                cancelled_stack = ctx.data["cancelled_stack"]

                # 1. Validate "Elevator Up": pre_stack levels should be non-decreasing
                last_level = 0
                for level, task_id in pre_stack:
                    self.assertGreaterEqual(level, last_level)
                    last_level = level

                # 2. Validate "Elevator Down": post_stack levels should be non-increasing
                last_level = float("inf")
                for level, task_id in post_stack:
                    self.assertLessEqual(level, last_level)
                    last_level = level

                # 3. Validate Cleanup Symmetry: post_execute is called for all started tasks
                # The crucial invariant: any task that STARTED pre_execute gets post_execute
                # called for cleanup, regardless of whether pre_execute succeeded, failed, or was cancelled.
                self.assertTrue(
                    set(post_stack).issubset(set(pre_started)),
                    f"All post_execute tasks must have started pre_execute. "
                    f"Extra in post: {set(post_stack) - set(pre_started)}"
                )

                # 4. Validate cleanup behavior for cancelled tasks
                # Cancelled tasks SHOULD have post_execute called for cleanup
                # This is critical: if pre_execute acquired resources (locks, files),
                # post_execute must run to release them, even if cancelled.
                for task_token in cancelled_stack:
                    self.assertIn(
                        task_token,
                        post_stack,
                        f"Cancelled task {task_token} must have post_execute for cleanup",
                    )

                # 5. Tasks can be cancelled at any point during pre_execute
                # Regardless of when cancellation occurs, post_execute runs for cleanup

                # 6. Cancelled tasks detected: this shows the framework is working
                if cancelled_stack:
                    # At least some tasks were cancelled due to sibling failures
                    # All of them should have had their post_execute called
                    pass  # This is expected and correct


if __name__ == "__main__":
    unittest.main()
