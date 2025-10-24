import asyncio
import random
from collections import deque
from dataclasses import dataclass, field
import sys
import time
from typing import Dict, List, Tuple, Set

import unittest

from sdax import AsyncTask, TaskFunction
from sdax.sdax_core import AsyncDagTaskProcessor
from sdax.sdax_task_analyser import TaskAnalyzer
from .dag_system import generate_random_dag, validate_forward, validate_reverse


@dataclass
class Ctxt:
    forw: List[str] = field(default_factory=list)
    executed: List[str] = field(default_factory=list)
    back: List[str] = field(default_factory=list)


async def _maybe_sleep():
    delay = random.uniform(1.0, 1.0001) - 1.0
    if delay > 0:
        await asyncio.sleep(delay)


def function_maker(name: str):
    async def forward(ctx: Ctxt):
        await _maybe_sleep()
        ctx.forw.append(name)
        await _maybe_sleep()

    async def backward(ctx: Ctxt):
        await _maybe_sleep()
        ctx.back.append(name)
        await _maybe_sleep()

    async def execute(ctx: Ctxt):
        await _maybe_sleep()
        ctx.executed.append(name)
        await _maybe_sleep()

    return forward, backward, execute


def _topological_order(graph) -> List[str]:
    in_degree: Dict[str, int] = {name: len(node.dependencies) for name, node in graph.nodes.items()}
    queue = deque(sorted(name for name, deg in in_degree.items() if deg == 0))
    order: List[str] = []

    while queue:
        current = queue.popleft()
        order.append(current)
        for dependent in graph.nodes[current].dependents:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(order) != len(graph.nodes):
        raise ValueError("Graph contains a cycle or disconnected components")
    return order


NULL_TASK_PROBABILITY = 0.1


class TestConcurrentExecution(unittest.IsolatedAsyncioTestCase):
    async def test_monte_dag(self):
        seed = int(time.time())
        try:
            graph = generate_random_dag(
                total_nodes_count=200,
                origin_nodes_count=10,
                end_nodes_count=10,
                min_deps_per_node=1,
                max_deps_per_node=5,
                random_seed=seed,
            )

            pre_empty: set[str] = set()
            execute_empty: set[str] = set()
            post_empty: set[str] = set()

            builder = AsyncDagTaskProcessor.builder()
            # Ensure deterministic task insertion order
            for name in _topological_order(graph):
                node = graph.nodes[name]
                forward, backward, execute = function_maker(name)

                pre_fn = (
                    None
                    if random.random() < NULL_TASK_PROBABILITY
                    else TaskFunction(function=forward)
                )
                post_fn = (
                    None
                    if random.random() < NULL_TASK_PROBABILITY
                    else TaskFunction(function=backward)
                )
                execute_fn = (
                    None
                    if random.random() < NULL_TASK_PROBABILITY
                    else TaskFunction(function=execute)
                )

                if pre_fn is None:
                    pre_empty.add(name)
                if post_fn is None:
                    post_empty.add(name)
                if execute_fn is None:
                    execute_empty.add(name)

                builder.add_task(
                    AsyncTask(
                        name=name, pre_execute=pre_fn, post_execute=post_fn, execute=execute_fn
                    ),
                    depends_on=node.dependencies,
                )

            processor = builder.build()

            for _ in range(4):
                ctx = Ctxt()
                await processor.process_tasks(ctx)

                forward_trace: Tuple[str, ...] = tuple(ctx.forw)
                reverse_trace: Tuple[str, ...] = tuple(ctx.back)
                executed_trace: Set[str, ...] = set(ctx.executed)

                assert len(executed_trace) == len(ctx.executed), "Duplicate executed tasks"
                assert executed_trace.issubset(graph.nodes)
                assert len(executed_trace & execute_empty) == 0, \
                    f"Executed tasks contain excluded tasks {(executed_trace & execute_empty)}"

                # ensure the executed trace + the excluded tasks is the full graph
                assert set(graph.nodes) == executed_trace | execute_empty, \
                    "Executed trace + excluded tasks does not match graph size"

                assert validate_forward(graph, forward_trace, pre_empty), (
                    "Forward traversal violated dependency ordering"
                )
                assert validate_reverse(graph, reverse_trace, post_empty), (
                    "Reverse traversal violated dependency ordering"
                )

        except Exception as _:
            # Ensure we print the seed on failure so we can reproduce the failure.
            print(f"Seed: {seed}", file=sys.stderr)
            raise


if __name__ == "__main__":
    unittest.main()
