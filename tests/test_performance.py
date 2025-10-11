import asyncio
import time
import unittest

from sdax import AsyncTask, AsyncTaskProcessor, TaskContext, TaskFunction

# Minimal sleep to yield thread (0 on modern Python)
YIELD_TIME = 0


async def perf_pre_execute(ctx, level, task_id):
    """A minimal pre-execute function that just yields."""
    del level, task_id  # Unused, but kept for signature consistency
    if YIELD_TIME > 0:
        await asyncio.sleep(YIELD_TIME)
    ctx.data["pre_count"] += 1


async def perf_execute(ctx, level, task_id):
    """A minimal execute function that just yields."""
    del level, task_id  # Unused, but kept for signature consistency
    if YIELD_TIME > 0:
        await asyncio.sleep(YIELD_TIME)
    ctx.data["exec_count"] += 1


async def perf_post_execute(ctx, level, task_id):
    """A minimal post-execute function that just yields."""
    del level, task_id  # Unused, but kept for signature consistency
    if YIELD_TIME > 0:
        await asyncio.sleep(YIELD_TIME)
    ctx.data["post_count"] += 1


class TestSdaxPerformance(unittest.IsolatedAsyncioTestCase):
    async def test_high_throughput_many_levels(self):
        """
        Performance test: Many levels with few tasks each.
        Tests the overhead of level management and sequential level processing.
        """
        NUM_LEVELS = 100
        TASKS_PER_LEVEL = 5
        TOTAL_TASKS = NUM_LEVELS * TASKS_PER_LEVEL

        processor = AsyncTaskProcessor()
        ctx = TaskContext()
        ctx.data["pre_count"] = 0
        ctx.data["exec_count"] = 0
        ctx.data["post_count"] = 0

        for level in range(1, NUM_LEVELS + 1):
            for task_num in range(TASKS_PER_LEVEL):
                task_id = f"L{level}-T{task_num}"
                task = AsyncTask(
                    name=task_id,
                    pre_execute=TaskFunction(
                        lambda c, lvl=level, tid=task_id: perf_pre_execute(c, lvl, tid)
                    ),
                    execute=TaskFunction(
                        lambda c, lvl=level, tid=task_id: perf_execute(c, lvl, tid)
                    ),
                    post_execute=TaskFunction(
                        lambda c, lvl=level, tid=task_id: perf_post_execute(c, lvl, tid)
                    ),
                )
                processor.add_task(task, level)

        start_time = time.perf_counter()
        await processor.process_tasks(ctx)
        elapsed = time.perf_counter() - start_time

        # Verify all tasks ran
        self.assertEqual(ctx.data["pre_count"], TOTAL_TASKS)
        self.assertEqual(ctx.data["exec_count"], TOTAL_TASKS)
        self.assertEqual(ctx.data["post_count"], TOTAL_TASKS)

        # Performance metrics
        tasks_per_second = TOTAL_TASKS / elapsed
        ms_per_task = (elapsed * 1000) / TOTAL_TASKS

        print(f"\n{'=' * 70}")
        print("Performance Test: Many Levels (Sequential)")
        print(f"{'=' * 70}")
        print(f"  Levels:              {NUM_LEVELS}")
        print(f"  Tasks per level:     {TASKS_PER_LEVEL}")
        print(f"  Total tasks:         {TOTAL_TASKS}")
        print(f"  Total phases:        {TOTAL_TASKS * 3} (pre + exec + post)")
        print(f"  Elapsed time:        {elapsed:.3f}s")
        print(f"  Throughput:          {tasks_per_second:.0f} tasks/sec")
        print(f"  Time per task:       {ms_per_task:.3f}ms")
        print(f"{'=' * 70}\n")

        # Basic performance assertion (very generous to account for CI/slow machines)
        # Should be able to process at least 1000 tasks/sec on modern hardware
        self.assertGreater(
            tasks_per_second, 100, f"Performance too slow: {tasks_per_second:.0f} tasks/sec"
        )

    async def test_high_throughput_parallel_heavy(self):
        """
        Performance test: Few levels with many tasks each.
        Tests the overhead of parallel task execution within levels.
        """
        NUM_LEVELS = 10
        TASKS_PER_LEVEL = 50
        TOTAL_TASKS = NUM_LEVELS * TASKS_PER_LEVEL

        processor = AsyncTaskProcessor()
        ctx = TaskContext()
        ctx.data["pre_count"] = 0
        ctx.data["exec_count"] = 0
        ctx.data["post_count"] = 0

        for level in range(1, NUM_LEVELS + 1):
            for task_num in range(TASKS_PER_LEVEL):
                task_id = f"L{level}-T{task_num}"
                task = AsyncTask(
                    name=task_id,
                    pre_execute=TaskFunction(
                        lambda c, lvl=level, tid=task_id: perf_pre_execute(c, lvl, tid)
                    ),
                    execute=TaskFunction(
                        lambda c, lvl=level, tid=task_id: perf_execute(c, lvl, tid)
                    ),
                    post_execute=TaskFunction(
                        lambda c, lvl=level, tid=task_id: perf_post_execute(c, lvl, tid)
                    ),
                )
                processor.add_task(task, level)

        start_time = time.perf_counter()
        await processor.process_tasks(ctx)
        elapsed = time.perf_counter() - start_time

        # Verify all tasks ran
        self.assertEqual(ctx.data["pre_count"], TOTAL_TASKS)
        self.assertEqual(ctx.data["exec_count"], TOTAL_TASKS)
        self.assertEqual(ctx.data["post_count"], TOTAL_TASKS)

        # Performance metrics
        tasks_per_second = TOTAL_TASKS / elapsed
        ms_per_task = (elapsed * 1000) / TOTAL_TASKS

        print(f"\n{'=' * 70}")
        print("Performance Test: Parallel Heavy")
        print(f"{'=' * 70}")
        print(f"  Levels:              {NUM_LEVELS}")
        print(f"  Tasks per level:     {TASKS_PER_LEVEL}")
        print(f"  Total tasks:         {TOTAL_TASKS}")
        print(f"  Total phases:        {TOTAL_TASKS * 3} (pre + exec + post)")
        print(f"  Elapsed time:        {elapsed:.3f}s")
        print(f"  Throughput:          {tasks_per_second:.0f} tasks/sec")
        print(f"  Time per task:       {ms_per_task:.3f}ms")
        print(f"  Parallel efficiency: {TASKS_PER_LEVEL * NUM_LEVELS * 3 / elapsed:.0f} phases/sec")
        print(f"{'=' * 70}\n")

        # Basic performance assertion
        self.assertGreater(
            tasks_per_second, 100, f"Performance too slow: {tasks_per_second:.0f} tasks/sec"
        )

    async def test_high_throughput_massive_scale(self):
        """
        Performance test: Massive scale with balanced levels and tasks.
        Tests overall system performance at scale.
        """
        NUM_LEVELS = 50
        TASKS_PER_LEVEL = 20
        TOTAL_TASKS = NUM_LEVELS * TASKS_PER_LEVEL

        processor = AsyncTaskProcessor()
        ctx = TaskContext()
        ctx.data["pre_count"] = 0
        ctx.data["exec_count"] = 0
        ctx.data["post_count"] = 0

        for level in range(1, NUM_LEVELS + 1):
            for task_num in range(TASKS_PER_LEVEL):
                task_id = f"L{level}-T{task_num}"
                task = AsyncTask(
                    name=task_id,
                    pre_execute=TaskFunction(
                        lambda c, lvl=level, tid=task_id: perf_pre_execute(c, lvl, tid)
                    ),
                    execute=TaskFunction(
                        lambda c, lvl=level, tid=task_id: perf_execute(c, lvl, tid)
                    ),
                    post_execute=TaskFunction(
                        lambda c, lvl=level, tid=task_id: perf_post_execute(c, lvl, tid)
                    ),
                )
                processor.add_task(task, level)

        start_time = time.perf_counter()
        await processor.process_tasks(ctx)
        elapsed = time.perf_counter() - start_time

        # Verify all tasks ran
        self.assertEqual(ctx.data["pre_count"], TOTAL_TASKS)
        self.assertEqual(ctx.data["exec_count"], TOTAL_TASKS)
        self.assertEqual(ctx.data["post_count"], TOTAL_TASKS)

        # Performance metrics
        tasks_per_second = TOTAL_TASKS / elapsed
        ms_per_task = (elapsed * 1000) / TOTAL_TASKS
        total_phases = TOTAL_TASKS * 3

        print(f"\n{'=' * 70}")
        print("Performance Test: Massive Scale")
        print(f"{'=' * 70}")
        print(f"  Levels:              {NUM_LEVELS}")
        print(f"  Tasks per level:     {TASKS_PER_LEVEL}")
        print(f"  Total tasks:         {TOTAL_TASKS}")
        print(f"  Total phases:        {total_phases} (pre + exec + post)")
        print(f"  Elapsed time:        {elapsed:.3f}s")
        print(f"  Throughput:          {tasks_per_second:.0f} tasks/sec")
        print(f"  Phase throughput:    {total_phases / elapsed:.0f} phases/sec")
        print(f"  Time per task:       {ms_per_task:.3f}ms")
        print(f"  Overhead per task:   {ms_per_task:.4f}ms (minimal work)")
        print(f"{'=' * 70}\n")

        # Basic performance assertion
        self.assertGreater(
            tasks_per_second, 100, f"Performance too slow: {tasks_per_second:.0f} tasks/sec"
        )

    async def test_minimal_overhead_single_level(self):
        """
        Performance test: All tasks in a single level (maximum parallelism).
        Tests the minimal overhead when everything runs in parallel.
        """
        TOTAL_TASKS = 1000

        processor = AsyncTaskProcessor()
        ctx = TaskContext()
        ctx.data["pre_count"] = 0
        ctx.data["exec_count"] = 0
        ctx.data["post_count"] = 0

        for task_num in range(TOTAL_TASKS):
            task_id = f"T{task_num}"
            task = AsyncTask(
                name=task_id,
                pre_execute=TaskFunction(lambda c, tid=task_id: perf_pre_execute(c, 1, tid)),
                execute=TaskFunction(lambda c, tid=task_id: perf_execute(c, 1, tid)),
                post_execute=TaskFunction(lambda c, tid=task_id: perf_post_execute(c, 1, tid)),
            )
            processor.add_task(task, 1)

        start_time = time.perf_counter()
        await processor.process_tasks(ctx)
        elapsed = time.perf_counter() - start_time

        # Verify all tasks ran
        self.assertEqual(ctx.data["pre_count"], TOTAL_TASKS)
        self.assertEqual(ctx.data["exec_count"], TOTAL_TASKS)
        self.assertEqual(ctx.data["post_count"], TOTAL_TASKS)

        # Performance metrics
        tasks_per_second = TOTAL_TASKS / elapsed
        us_per_task = (elapsed * 1_000_000) / TOTAL_TASKS

        print(f"\n{'=' * 70}")
        print("Performance Test: Minimal Overhead (Single Level)")
        print(f"{'=' * 70}")
        print(f"  Total tasks:         {TOTAL_TASKS}")
        print("  Level:               1 (all parallel)")
        print(f"  Total phases:        {TOTAL_TASKS * 3} (pre + exec + post)")
        print(f"  Elapsed time:        {elapsed:.3f}s")
        print(f"  Throughput:          {tasks_per_second:.0f} tasks/sec")
        print(f"  Time per task:       {us_per_task:.1f}µs")
        print(f"  Framework overhead:  ~{us_per_task:.1f}µs per task")
        print(f"{'=' * 70}\n")

        # Basic performance assertion
        self.assertGreater(
            tasks_per_second, 500, f"Performance too slow: {tasks_per_second:.0f} tasks/sec"
        )


if __name__ == "__main__":
    unittest.main()
