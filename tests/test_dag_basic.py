"""Basic DAG processor tests (unittest style)."""

import asyncio
import unittest

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

if __name__ == "__main__":
    unittest.main()
