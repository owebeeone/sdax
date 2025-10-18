"""
Performance analysis to identify bottlenecks in the sdax framework.
"""

import asyncio
import cProfile
import pstats
import time
from dataclasses import dataclass, field
from io import StringIO
from typing import Dict

from sdax import AsyncTask, AsyncTaskProcessor, TaskFunction


@dataclass
class TaskContext:
    """A simple data-passing object for tasks to share state."""
    data: Dict = field(default_factory=dict)


# Minimal async functions
async def noop_task(ctx: TaskContext):
    """Does absolutely nothing - just to measure framework overhead."""
    pass


async def yield_task(ctx: TaskContext):
    """Just yields control - minimal async overhead."""
    await asyncio.sleep(0)


class PerformanceAnalyzer:
    """Analyzes performance of different framework components."""

    @staticmethod
    async def test_raw_asyncio_baseline():
        """Baseline: raw asyncio with TaskGroup."""

        async def task():
            await asyncio.sleep(0)

        start = time.perf_counter()
        async with asyncio.TaskGroup() as tg:
            for _ in range(1000):
                tg.create_task(task())
        elapsed = time.perf_counter() - start

        print("\n" + "=" * 70)
        print("BASELINE: Raw asyncio.TaskGroup")
        print("=" * 70)
        print(f"  Tasks:           1,000")
        print(f"  Elapsed:         {elapsed:.3f}s")
        print(f"  Throughput:      {1000 / elapsed:.0f} tasks/sec")
        print(f"  Time per task:   {elapsed * 1000:.3f}ms")
        print("=" * 70)

        return elapsed

    @staticmethod
    async def test_single_level_overhead():
        """Measure overhead of single level with parallel tasks."""
        ctx = TaskContext()

        # Build processor with 1000 tasks in a single level
        builder = AsyncTaskProcessor.builder()
        for i in range(1000):
            builder.add_task(
                AsyncTask(name=f"T{i}", execute=TaskFunction(lambda c: noop_task(c))), level=1
            )
        
        processor = builder.build()

        start = time.perf_counter()
        await processor.process_tasks(ctx)
        elapsed = time.perf_counter() - start

        print("\n" + "=" * 70)
        print("SINGLE LEVEL: All tasks in one level")
        print("=" * 70)
        print(f"  Tasks:           1,000")
        print(f"  Levels:          1")
        print(f"  Elapsed:         {elapsed:.3f}s")
        print(f"  Throughput:      {1000 / elapsed:.0f} tasks/sec")
        print(f"  Time per task:   {elapsed * 1000:.3f}ms")
        print(f"  Overhead:        {(elapsed * 1000):.3f}ms total")
        print("=" * 70)

        return elapsed

    @staticmethod
    async def test_multi_level_overhead():
        """Measure overhead of multiple sequential levels."""
        ctx = TaskContext()

        # Build processor with 10 tasks per level across 100 levels
        builder = AsyncTaskProcessor.builder()
        for level in range(1, 101):
            for i in range(10):
                builder.add_task(
                    AsyncTask(name=f"L{level}-T{i}", execute=TaskFunction(lambda c: noop_task(c))),
                    level=level,
                )
        
        processor = builder.build()

        start = time.perf_counter()
        await processor.process_tasks(ctx)
        elapsed = time.perf_counter() - start

        print("\n" + "=" * 70)
        print("MULTI LEVEL: Sequential level processing")
        print("=" * 70)
        print(f"  Tasks:           1,000")
        print(f"  Levels:          100")
        print(f"  Tasks/level:     10")
        print(f"  Elapsed:         {elapsed:.3f}s")
        print(f"  Throughput:      {1000 / elapsed:.0f} tasks/sec")
        print(f"  Time per task:   {elapsed * 1000:.3f}ms")
        print(f"  Time per level:  {elapsed * 1000 / 100:.3f}ms")
        print("=" * 70)

        return elapsed

    @staticmethod
    async def test_three_phase_overhead():
        """Measure overhead when all three phases are used."""
        ctx = TaskContext()

        # Build processor with 1000 tasks with pre, exec, and post
        builder = AsyncTaskProcessor.builder()
        for i in range(1000):
            builder.add_task(
                AsyncTask(
                    name=f"T{i}",
                    pre_execute=TaskFunction(lambda c: noop_task(c)),
                    execute=TaskFunction(lambda c: noop_task(c)),
                    post_execute=TaskFunction(lambda c: noop_task(c)),
                ),
                level=1,
            )
        
        processor = builder.build()

        start = time.perf_counter()
        await processor.process_tasks(ctx)
        elapsed = time.perf_counter() - start

        print("\n" + "=" * 70)
        print("THREE PHASES: Pre + Execute + Post")
        print("=" * 70)
        print(f"  Tasks:           1,000")
        print(f"  Total phases:    3,000")
        print(f"  Elapsed:         {elapsed:.3f}s")
        print(f"  Throughput:      {1000 / elapsed:.0f} tasks/sec")
        print(f"  Phase rate:      {3000 / elapsed:.0f} phases/sec")
        print(f"  Time per task:   {elapsed * 1000:.3f}ms")
        print(f"  Time per phase:  {elapsed * 1000 / 3:.3f}ms")
        print("=" * 70)

        return elapsed

    @staticmethod
    async def test_context_manager_overhead():
        """Measure overhead of AsyncExitStack context managers."""
        from contextlib import AsyncExitStack

        class DummyManager:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        start = time.perf_counter()
        async with AsyncExitStack() as stack:
            for _ in range(100):
                await stack.enter_async_context(DummyManager())
        elapsed = time.perf_counter() - start

        print("\n" + "=" * 70)
        print("CONTEXT MANAGER: AsyncExitStack overhead")
        print("=" * 70)
        print(f"  Managers:        100")
        print(f"  Elapsed:         {elapsed * 1000:.3f}ms")
        print(f"  Per manager:     {elapsed * 1000 / 100:.3f}ms")
        print("=" * 70)

        return elapsed

    @staticmethod
    async def test_taskgroup_overhead():
        """Measure overhead of creating TaskGroups."""

        async def dummy():
            pass

        # Single large TaskGroup
        start = time.perf_counter()
        async with asyncio.TaskGroup() as tg:
            for _ in range(1000):
                tg.create_task(dummy())
        elapsed_single = time.perf_counter() - start

        # Many small TaskGroups
        start = time.perf_counter()
        for _ in range(100):
            async with asyncio.TaskGroup() as tg:
                for _ in range(10):
                    tg.create_task(dummy())
        elapsed_many = time.perf_counter() - start

        print("\n" + "=" * 70)
        print("TASKGROUP: Creation overhead")
        print("=" * 70)
        print(f"  Single large (1000 tasks):  {elapsed_single * 1000:.3f}ms")
        print(f"  Many small (100x10 tasks):  {elapsed_many * 1000:.3f}ms")
        print(f"  Overhead per TaskGroup:     {(elapsed_many - elapsed_single) * 1000 / 100:.3f}ms")
        print("=" * 70)

        return elapsed_single, elapsed_many


async def run_profiled_test():
    """Run a test with cProfile to see where time is spent."""
    ctx = TaskContext()

    # Build processor with 100 levels, 5 tasks each = 500 tasks
    builder = AsyncTaskProcessor.builder()
    for level in range(1, 101):
        for i in range(5):
            builder.add_task(
                AsyncTask(
                    name=f"L{level}-T{i}",
                    pre_execute=TaskFunction(lambda c: noop_task(c)),
                    execute=TaskFunction(lambda c: noop_task(c)),
                    post_execute=TaskFunction(lambda c: noop_task(c)),
                ),
                level=level,
            )
    
    processor = builder.build()

    await processor.process_tasks(ctx)


async def main():
    """Run all performance analysis tests."""
    print("\n" + "=" * 70)
    print("SDAX PERFORMANCE ANALYSIS")
    print("=" * 70)

    analyzer = PerformanceAnalyzer()

    # Run baseline tests
    baseline = await analyzer.test_raw_asyncio_baseline()
    single = await analyzer.test_single_level_overhead()
    multi = await analyzer.test_multi_level_overhead()
    three_phase = await analyzer.test_three_phase_overhead()
    cm_overhead = await analyzer.test_context_manager_overhead()
    tg_single, tg_many = await analyzer.test_taskgroup_overhead()

    # Summary
    print("\n" + "=" * 70)
    print("ANALYSIS SUMMARY")
    print("=" * 70)
    print(f"  Raw asyncio baseline:       {baseline * 1000:.3f}ms for 1000 tasks")
    print(f"  Single level framework:     {single * 1000:.3f}ms for 1000 tasks")
    print(f"  Multi level framework:      {multi * 1000:.3f}ms for 1000 tasks")
    print(f"  Three phase framework:      {three_phase * 1000:.3f}ms for 1000 tasks")
    print()
    print(f"  Framework overhead (single):  {(single - baseline) * 1000:.3f}ms")
    print(f"  Framework overhead (multi):   {(multi - baseline) * 1000:.3f}ms")
    print(f"  Per-level overhead:          {(multi - single) / 100 * 1000:.3f}ms")
    print(f"  Per-phase overhead:          {(three_phase / 3000 - baseline / 1000) * 1000:.3f}ms")
    print()
    print(f"  AsyncExitStack (100 mgrs):   {cm_overhead * 1000:.3f}ms")
    print(f"  TaskGroup overhead (each):   {(tg_many - tg_single) * 1000 / 100:.3f}ms")
    print("=" * 70)

    # Run cProfile
    print("\n" + "=" * 70)
    print("DETAILED PROFILING (500 tasks, 100 levels)")
    print("=" * 70)

    profiler = cProfile.Profile()
    profiler.enable()
    await run_profiled_test()
    profiler.disable()

    # Print stats
    s = StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats("cumulative")
    ps.print_stats(30)  # Top 30 functions

    print(s.getvalue())

    # Also print by time
    print("\n" + "=" * 70)
    print("TOP FUNCTIONS BY TOTAL TIME")
    print("=" * 70)
    s = StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats("tottime")
    ps.print_stats(20)  # Top 20 functions
    print(s.getvalue())


if __name__ == "__main__":
    asyncio.run(main())
