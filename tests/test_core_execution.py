import asyncio
import inspect
import sys
import time
import unittest
from dataclasses import dataclass, field
from typing import Dict, List

from sdax import (
    AsyncTask,
    AsyncTaskProcessor,
    SdaxExecutionError,
    TaskFunction,
    flatten_exceptions,
)
from sdax.sdax_core import AsyncDagTaskProcessor, PhaseId


@dataclass
class TaskContext:
    """A simple data-passing object for tasks to share state."""
    data: Dict = field(default_factory=dict)


# A shared log to track the order of function calls across tests
CALL_LOG = []


# --- Test Task Functions ---
async def log_pre(name: str, ctx: TaskContext):
    await asyncio.sleep(0.01)
    CALL_LOG.append(f"{name}-pre")


async def log_exec(name: str, ctx: TaskContext):
    await asyncio.sleep(0.01)
    CALL_LOG.append(f"{name}-exec")


async def log_post(name: str, ctx: TaskContext):
    await asyncio.sleep(0.01)
    CALL_LOG.append(f"{name}-post")


async def fail_pre(name: str, ctx: TaskContext):
    await asyncio.sleep(0.01)
    CALL_LOG.append(f"{name}-pre-fail")
    raise ValueError(f"{name} failed pre-execute")


async def fail_exec(name: str, ctx: TaskContext):
    await asyncio.sleep(0.01)
    CALL_LOG.append(f"{name}-exec-fail")
    raise ValueError(f"{name} failed execute")


def bind_async(func, *args):
    async def _bound(ctx: TaskContext):
        result = func(*args, ctx)
        if inspect.isawaitable(result):
            return await result
        return result

    return _bound


class TestSdaxCoreExecution(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        """Clear the call log before each test."""
        global CALL_LOG
        CALL_LOG = []

    async def _build_processor_from_levels(self, levels: Dict[int, List[AsyncTask]]) -> AsyncTaskProcessor:
        builder = AsyncTaskProcessor.builder()
        for level in sorted(levels.keys()):
            for task in levels[level]:
                builder = builder.add_task(task, level)
        return builder.build()

    async def test_successful_multi_level_execution_order(self):
        """Verify the correct execution order for a successful run."""
        ctx = TaskContext()

        # Build processor with tasks
        processor = (
            AsyncTaskProcessor.builder()
            # Level 1
            .add_task(
                AsyncTask(
                    "L1A",
                    pre_execute=TaskFunction(bind_async(log_pre, "L1A")),
                    post_execute=TaskFunction(bind_async(log_post, "L1A")),
                ),
                1,
            )
            .add_task(
                AsyncTask(
                    "L1B",
                    pre_execute=TaskFunction(bind_async(log_pre, "L1B")),
                    post_execute=TaskFunction(bind_async(log_post, "L1B")),
                ),
                1,
            )
            # Level 2
            .add_task(
                AsyncTask(
                    "L2A",
                    pre_execute=TaskFunction(bind_async(log_pre, "L2A")),
                    execute=TaskFunction(bind_async(log_exec, "L2A")),
                    post_execute=TaskFunction(bind_async(log_post, "L2A")),
                ),
                2,
            )
            .build()
        )

        await processor.process_tasks(ctx)

        # Pre-executes happen level by level
        self.assertIn("L1A-pre", CALL_LOG[0:2])
        self.assertIn("L1B-pre", CALL_LOG[0:2])
        self.assertEqual(CALL_LOG[2], "L2A-pre")
        # Execute happens after all pre-executes
        self.assertEqual(CALL_LOG[3], "L2A-exec")
        # Post-executes happen in reverse level order
        self.assertEqual(CALL_LOG[4], "L2A-post")
        self.assertIn("L1A-post", CALL_LOG[5:7])
        self.assertIn("L1B-post", CALL_LOG[5:7])

    async def test_pre_execute_failure_teardown_order(self):
        """Verify correct teardown when a pre_execute fails."""
        ctx = TaskContext()

        processor = (
            AsyncTaskProcessor.builder()
            # Level 1
            .add_task(
                AsyncTask(
                    "L1A",
                    pre_execute=TaskFunction(bind_async(log_pre, "L1A")),
                    post_execute=TaskFunction(bind_async(log_post, "L1A")),
                ),
                1,
            )
            # Level 2
            .add_task(
                AsyncTask(
                    "L2A",
                    pre_execute=TaskFunction(bind_async(fail_pre, "L2A")),
                    post_execute=TaskFunction(bind_async(log_post, "L2A")),
                ),
                2,
            )
            .add_task(
                AsyncTask(
                    "L2B",
                    pre_execute=TaskFunction(bind_async(log_pre, "L2B")),
                    post_execute=TaskFunction(bind_async(log_post, "L2B")),
                ),
                2,
            )
            .build()
        )

        with self.assertRaises(ExceptionGroup):
            await processor.process_tasks(ctx)

        # L1A pre-executes successfully
        self.assertIn("L1A-pre", CALL_LOG)
        # L2 tasks run in parallel, one fails
        self.assertIn("L2A-pre-fail", CALL_LOG)
        self.assertIn("L2B-pre", CALL_LOG)
        # Execute phase is never reached
        self.assertNotIn("L2A-exec", CALL_LOG)
        # Post-execute runs for ALL tasks whose pre-execute started (for cleanup)
        # This includes L2A even though it failed - critical for resource cleanup
        self.assertIn("L2A-post", CALL_LOG)
        self.assertIn("L2B-post", CALL_LOG)
        self.assertIn("L1A-post", CALL_LOG)
        # The teardown order is L2 then L1
        self.assertIn("L2A-post", CALL_LOG[-3:])
        self.assertIn("L2B-post", CALL_LOG[-3:])
        self.assertIn("L1A-post", CALL_LOG[-3:])

    async def test_execute_failure_teardown_order(self):
        """Verify correct teardown when an execute fails."""
        ctx = TaskContext()

        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    "L1A",
                    pre_execute=TaskFunction(bind_async(log_pre, "L1A")),
                    post_execute=TaskFunction(bind_async(log_post, "L1A")),
                ),
                1,
            )
            .add_task(
                AsyncTask(
                    "L1B",
                    pre_execute=TaskFunction(bind_async(log_pre, "L1B")),
                    execute=TaskFunction(bind_async(fail_exec, "L1B")),
                    post_execute=TaskFunction(bind_async(log_post, "L1B")),
                ),
                1,
            )
            .build()
        )

        with self.assertRaises(ExceptionGroup):
            await processor.process_tasks(ctx)

        # All pre-executes should succeed
        self.assertIn("L1A-pre", CALL_LOG)
        self.assertIn("L1B-pre", CALL_LOG)
        # One execute fails
        self.assertIn("L1B-exec-fail", CALL_LOG)
        # All successful pre-executes must have their post-executes called
        self.assertIn("L1A-post", CALL_LOG)
        self.assertIn("L1B-post", CALL_LOG)

    async def test_tasks_with_missing_functions(self):
        """Verify tasks with None functions are handled correctly."""
        ctx = TaskContext()

        # Build processor with tasks having various combinations of phases
        processor = (
            AsyncTaskProcessor.builder()
            # No pre_execute, should still run execute and post
            .add_task(
                AsyncTask(
                    "L1-NoPre",
                    execute=TaskFunction(bind_async(log_exec, "L1-NoPre")),
                    post_execute=TaskFunction(bind_async(log_post, "L1-NoPre")),
                ),
                1,
            )
            # No execute, should still run pre and post
            .add_task(
                AsyncTask(
                    "L1-NoExec",
                    pre_execute=TaskFunction(bind_async(log_pre, "L1-NoExec")),
                    post_execute=TaskFunction(bind_async(log_post, "L1-NoExec")),
                ),
                1,
            )
            # Only post_execute, should not run pre or exec
            .add_task(
                AsyncTask(
                    "L2-PostOnly", post_execute=TaskFunction(bind_async(log_post, "L2-PostOnly"))
                ),
                2,
            )
            .build()
        )

        await processor.process_tasks(ctx)

        self.assertEqual(
            set(CALL_LOG),
            {
                "L1-NoExec-pre",
                "L1-NoPre-exec",
                "L2-PostOnly-post",
                "L1-NoPre-post",
                "L1-NoExec-post",
            },
        )
        # Verify L1-NoPre did not run pre-execute
        self.assertNotIn("L1-NoPre-pre", CALL_LOG)
        # Verify L1-NoExec did not run execute
        self.assertNotIn("L1-NoExec-exec", CALL_LOG)

    async def test_post_execute_respects_dependency_order(self):
        """Child cleanup must finish before parent cleanup begins."""
        ctx = TaskContext(data={"child_done": False, "order": []})

        async def pre(name: str, ctx: TaskContext):
            CALL_LOG.append(f"{name}-pre")

        async def post_child(ctx: TaskContext):
            ctx.data["order"].append("child:start")
            await asyncio.sleep(0.1)

            ctx.data["child_done"] = True
            ctx.data["order"].append("child:end")


        async def post_parent(ctx: TaskContext):
            ctx.data["order"].append("parent:start")
            self.assertTrue(
                ctx.data.get("child_done"),
                "Child post_execute must complete before parent starts",
            )
            ctx.data["order"].append("parent:end")

        processor = await self._build_processor_from_levels(
            {
                1: [
                    AsyncTask(
                        "Parent",
                        pre_execute=TaskFunction(bind_async(pre, "Parent")),
                        post_execute=TaskFunction(post_parent),
                    )
                ],
                2: [
                    AsyncTask(
                        "Child",
                        pre_execute=TaskFunction(bind_async(pre, "Child")),
                        post_execute=TaskFunction(post_child),
                    )
                ],
            }
        )

        await processor.process_tasks(ctx)
        self.assertEqual(
            ctx.data["order"],
            ["child:start", "child:end", "parent:start", "parent:end"],
        )

    async def test_post_execute_waits_for_all_dependents(self):
        """Root cleanup must wait for every dependent cleanup to finish."""
        ctx = TaskContext(data={"completed": set()})

        async def pre(name: str, ctx: TaskContext):
            CALL_LOG.append(f"{name}-pre")

        async def post_mid(name: str, ctx: TaskContext):
            await asyncio.sleep(0.05)
            ctx.data["completed"].add(name)

        async def post_root(ctx: TaskContext):
            self.assertSetEqual(
                ctx.data.get("completed"),
                {"MidA", "MidB"},
                "Root post_execute must wait for all dependents to complete",
            )

        processor = await self._build_processor_from_levels(
            {
                1: [
                    AsyncTask(
                        "Root",
                        pre_execute=TaskFunction(bind_async(pre, "Root")),
                        post_execute=TaskFunction(post_root),
                    )
                ],
                2: [
                    AsyncTask(
                        "MidA",
                        pre_execute=TaskFunction(bind_async(pre, "MidA")),
                        post_execute=TaskFunction(bind_async(post_mid, "MidA")),
                    ),
                    AsyncTask(
                        "MidB",
                        pre_execute=TaskFunction(bind_async(pre, "MidB")),
                        post_execute=TaskFunction(bind_async(post_mid, "MidB")),
                    ),
                ],
            }
        )

        await processor.process_tasks(ctx)
        self.assertSetEqual(ctx.data["completed"], {"MidA", "MidB"})

    async def test_post_only_tasks_skipped_when_dependency_fails(self):
        """Post-only cleanup tasks should not run when dependencies never complete."""
        ctx = TaskContext()

        processor = (
            AsyncDagTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="A",
                    pre_execute=TaskFunction(bind_async(log_pre, "A")),
                    post_execute=TaskFunction(bind_async(log_post, "A")),
                ),
                depends_on=(),
            )
            .add_task(
                AsyncTask(
                    name="B",
                    pre_execute=TaskFunction(bind_async(fail_pre, "B")),
                ),
                depends_on=("A",),
            )
            .add_task(
                AsyncTask(
                    name="C",
                    post_execute=TaskFunction(bind_async(log_post, "C")),
                ),
                depends_on=("B",),
            )
            .add_task(
                AsyncTask(
                    name="D",
                    post_execute=TaskFunction(bind_async(log_post, "D")),
                ),
                depends_on=("C", "A"),
            )
            .build()
        )

        with self.assertRaises(ExceptionGroup):
            await processor.process_tasks(ctx)

        self.assertIn("B-pre-fail", CALL_LOG, "Expected upstream failure to occur")
        self.assertIn(
            "A-post", CALL_LOG, "Upstream task with started pre_execute should clean up"
        )
        self.assertNotIn("C-post", CALL_LOG, "Dependent post-only task must not run")
        self.assertNotIn("D-post", CALL_LOG, "Downstream post-only task must not run")

    async def test_post_only_tasks_run_when_dependencies_succeed(self):
        """Post-only cleanup tasks should run when dependencies complete successfully."""
        ctx = TaskContext()

        processor = (
            AsyncDagTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    name="A",
                    pre_execute=TaskFunction(bind_async(log_pre, "A")),
                    post_execute=TaskFunction(bind_async(log_post, "A")),
                ),
                depends_on=(),
            )
            .add_task(
                AsyncTask(
                    name="B",
                    post_execute=TaskFunction(bind_async(log_post, "B")),
                ),
                depends_on=("A",),
            )
            .add_task(
                AsyncTask(
                    name="C",
                    post_execute=TaskFunction(bind_async(log_post, "C")),
                ),
                depends_on=("B",),
            )
            .build()
        )

        await processor.process_tasks(ctx)

        self.assertIn("A-pre", CALL_LOG, "Upstream task should run pre")
        self.assertIn("A-post", CALL_LOG, "Upstream task should run post cleanup")
        self.assertIn("B-post", CALL_LOG, "Post-only dependent task should run cleanup")
        self.assertIn("C-post", CALL_LOG, "Post-only downstream task should run cleanup")
        self.assertLess(
            CALL_LOG.index("C-post"),
            CALL_LOG.index("B-post"),
            "Downstream post-only cleanup must occur before its dependency",
        )

    async def test_phase_runner_loop_matches_readme_example(self):
        """Ensure README-style phase runner loop continues to work."""
        ctx = TaskContext()
        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    "Runner",
                    pre_execute=TaskFunction(bind_async(log_pre, "Runner")),
                    execute=TaskFunction(bind_async(fail_exec, "Runner")),
                    post_execute=TaskFunction(bind_async(log_post, "Runner")),
                ),
                1,
            )
            .build()
        )

        observed_phases: list[PhaseId] = []
        handled_failures: list[BaseException] = []
        final_phase: PhaseId | None = None

        with self.assertRaises(SdaxExecutionError):
            async with processor.open(ctx) as runner:
                while await runner.run_next():
                    phase = runner.last_phase_id()
                    observed_phases.append(phase)
                    match phase:
                        case PhaseId.PRE_EXEC:
                            CALL_LOG.append("loop-pre")
                        case PhaseId.EXECUTE:
                            CALL_LOG.append("loop-exec")
                        case PhaseId.POST_EXEC:
                            pass

                final_phase = runner.last_phase_id()

                if runner.has_failures():
                    try:
                        runner.raise_failures()
                    except SdaxExecutionError as exc:
                        handled_failures.extend(flatten_exceptions(exc))

        self.assertIn(PhaseId.PRE_EXEC, observed_phases)
        self.assertIn(PhaseId.EXECUTE, observed_phases)
        self.assertEqual(final_phase, PhaseId.POST_EXEC)
        self.assertIn("Runner-pre", CALL_LOG)
        self.assertIn("Runner-exec-fail", CALL_LOG)
        self.assertIn("Runner-post", CALL_LOG)
        self.assertIn("loop-pre", CALL_LOG)
        self.assertIn("loop-exec", CALL_LOG)
        self.assertEqual(len(handled_failures), 1)
        self.assertIsInstance(handled_failures[0], ValueError)

    async def test_phase_runner_exit_without_running_phases(self):
        """Leaving the phase runner early should not execute any phase."""
        ctx = TaskContext()
        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    "Runner",
                    pre_execute=TaskFunction(bind_async(log_pre, "Runner")),
                    execute=TaskFunction(bind_async(log_exec, "Runner")),
                    post_execute=TaskFunction(bind_async(log_post, "Runner")),
                ),
                1,
            )
            .build()
        )

        async with processor.open(ctx):
            # No explicit run_next invocation; closing should be a no-op.
            pass

        self.assertEqual(CALL_LOG, [])

    async def test_phase_runner_cleanup_after_pre_only(self):
        """If only pre_execute ran, closing the runner should trigger cleanup but skip execute."""
        ctx = TaskContext()
        processor = (
            AsyncTaskProcessor.builder()
            .add_task(
                AsyncTask(
                    "Runner2",
                    pre_execute=TaskFunction(bind_async(log_pre, "Runner2")),
                    execute=TaskFunction(bind_async(log_exec, "Runner2")),
                    post_execute=TaskFunction(bind_async(log_post, "Runner2")),
                ),
                1,
            )
            .build()
        )

        async with processor.open(ctx) as runner:
            await runner.run_next()  # Run pre phase only

        self.assertIn("Runner2-pre", CALL_LOG)
        self.assertIn("Runner2-post", CALL_LOG)
        self.assertNotIn("Runner2-exec", CALL_LOG)


if __name__ == "__main__":
    unittest.main()
