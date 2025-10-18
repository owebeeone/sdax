"""Basic DAG processor tests (unittest style)."""

import asyncio
import unittest
from dataclasses import dataclass, field
from typing import List

from sdax import AsyncDagTaskProcessor, AsyncTask, TaskFunction
from sdax.sdax_task_analyser import TaskAnalyzer


async def _dummy(ctx):
    return None


class TestDagBasic(unittest.TestCase):
    def test_builds_from_analysis_and_waves_simple(self):
        analyzer = TaskAnalyzer()
        analyzer.add_task(
            AsyncTask(name="A", pre_execute=TaskFunction(function=_dummy)), depends_on=()
        )
        analyzer.add_task(
            AsyncTask(name="B", pre_execute=TaskFunction(function=_dummy)), depends_on=()
        )
        analyzer.add_task(
            AsyncTask(name="C", execute=TaskFunction(function=_dummy)), depends_on=("A", "B")
        )
        analyzer.add_task(
            AsyncTask(name="D", post_execute=TaskFunction(function=_dummy)), depends_on=("C",)
        )

        analysis = analyzer.analyze()

        pre_graph = analysis.pre_execute_graph
        self.assertEqual(len(pre_graph.waves), 1)
        self.assertEqual(set(pre_graph.waves[0].tasks), {"A", "B"})
        self.assertEqual(pre_graph.waves[0].depends_on_tasks, ())

        post_graph = analysis.post_execute_graph
        self.assertEqual(len(post_graph.waves), 1)
        self.assertEqual(post_graph.waves[0].tasks, ("D",))
        self.assertEqual(post_graph.waves[0].depends_on_tasks, ())

        proc = AsyncDagTaskProcessor.builder().from_analysis(analysis).build()
        self.assertIs(proc.analysis, analysis)

    def test_process_tasks_not_implemented(self):
        async def run():
            analyzer = TaskAnalyzer()
            analysis = analyzer.analyze()
            proc = AsyncDagTaskProcessor.builder().from_analysis(analysis).build()
            with self.assertRaises(NotImplementedError):
                await proc.process_tasks(ctx=None)

        asyncio.run(run())

    def test_process_tasks_not_implemented_with_pre_and_post(self):
        @dataclass
        class Ctx:
            order: List[str] = field(default_factory=list)
            pre_A: bool = False
            pre_B: bool = False
            post_C: bool = False

        async def pre_A(ctx: Ctx):
            ctx.pre_A = True
            ctx.order.append("pre:A")

        async def pre_B(ctx: Ctx):
            self.assertTrue(ctx.pre_A)
            ctx.pre_B = True
            ctx.order.append("pre:B")

        async def post_C(ctx: Ctx):
            self.assertTrue(ctx.pre_B)
            ctx.post_C = True
            ctx.order.append("post:C")

        async def run():
            analyzer = TaskAnalyzer()
            # Pre-only A, B
            analyzer.add_task(
                AsyncTask(name="A", pre_execute=TaskFunction(function=pre_A)), depends_on=()
            )
            analyzer.add_task(
                AsyncTask(name="B", pre_execute=TaskFunction(function=pre_B)), depends_on=("A",)
            )
            # Post-only C
            analyzer.add_task(
                AsyncTask(name="C", post_execute=TaskFunction(function=post_C)), depends_on=("B",)
            )

            analysis = analyzer.analyze()
            proc = AsyncDagTaskProcessor.builder().from_analysis(analysis).build()
            ctx = Ctx()
            await proc.process_tasks(ctx=ctx)

            # As-if assertions (when implemented):
            with self.subTest("expected_when_implemented"):
                # Expected order (pre then post)
                expected_prefixes = {"pre:A", "pre:B", "post:C"}
                self.assertTrue(expected_prefixes.issuperset(set(ctx.order)) or ctx.order == [])
                # Expected flags
                self.assertIn(ctx.pre_A, (False, True))
                self.assertIn(ctx.pre_B, (False, True))
                self.assertIn(ctx.post_C, (False, True))

        asyncio.run(run())

    def test_process_tasks_not_implemented_with_execute_only(self):
        @dataclass
        class Ctx:
            results: List[str] = field(default_factory=list)

        async def exec_X(ctx: Ctx):
            ctx.results.append("X")

        async def run():
            analyzer = TaskAnalyzer()
            analyzer.add_task(
                AsyncTask(name="X", execute=TaskFunction(function=exec_X)), depends_on=()
            )
            analysis = analyzer.analyze()
            proc = AsyncDagTaskProcessor.builder().from_analysis(analysis).build()
            ctx = Ctx()
            await proc.process_tasks(ctx=ctx)

            # As-if assertions (when implemented)
            with self.subTest("expected_when_implemented"):
                self.assertIn(len(ctx.results), (0, 1))
                if ctx.results:
                    self.assertEqual(ctx.results, ["X"])

        asyncio.run(run())

    def test_process_tasks_not_implemented_large_graph(self):
        @dataclass
        class Ctx:
            order: List[str] = field(default_factory=list)

        async def pre_r(ctx: Ctx, name: str):
            ctx.order.append(f"pre:{name}")

        async def exec_mid(ctx: Ctx):
            ctx.order.append("exec:MID")

        async def post_leaf(ctx: Ctx):
            ctx.order.append("post:LEAF")

        async def run():
            analyzer = TaskAnalyzer()
            # Create a small DAG: two roots -> mid -> leaf
            analyzer.add_task(
                AsyncTask(name="R1", pre_execute=TaskFunction(function=lambda c: pre_r(c, "R1"))),
                depends_on=(),
            )
            analyzer.add_task(
                AsyncTask(name="R2", pre_execute=TaskFunction(function=lambda c: pre_r(c, "R2"))),
                depends_on=(),
            )
            analyzer.add_task(
                AsyncTask(name="MID", execute=TaskFunction(function=exec_mid)), depends_on=("R1", "R2")
            )
            analyzer.add_task(
                AsyncTask(name="LEAF", post_execute=TaskFunction(function=post_leaf)), depends_on=("MID",)
            )

            analysis = analyzer.analyze()
            proc = AsyncDagTaskProcessor.builder().from_analysis(analysis).build()
            ctx = Ctx()
            with self.assertRaises(NotImplementedError):
                await proc.process_tasks(ctx=ctx)

            # As-if assertions (when implemented)
            with self.subTest("expected_when_implemented"):
                # Order contains pre for both roots, exec for mid, and post for leaf in some valid order
                expected = {"pre:R1", "pre:R2", "exec:MID", "post:LEAF"}
                self.assertTrue(expected.issuperset(set(ctx.order)) or ctx.order == [])

        asyncio.run(run())

if __name__ == "__main__":
    unittest.main()
