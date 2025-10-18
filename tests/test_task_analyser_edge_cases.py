"""Edge case tests for task analyzer.

Tests specific scenarios to ensure correctness.
"""

import pytest
from sdax.sdax_core import AsyncTask, TaskFunction
from sdax.sdax_task_analyser import TaskAnalyzer


# Helper functions for creating test tasks
async def dummy_func(ctx):
    """Dummy function for testing."""
    pass


def make_task(
    name: str,
    has_pre: bool = False,
    has_exec: bool = False,
    has_post: bool = False,
    is_node: bool = False,
) -> AsyncTask:
    """Helper to create tasks for testing.

    If is_node=True, creates a task with only execute (minimal function)
    to serve as a milestone/node while satisfying AsyncTask validation.
    """
    if is_node:
        # Nodes need at least one function for AsyncTask validation
        return AsyncTask(
            name=name,
            pre_execute=None,
            execute=TaskFunction(function=dummy_func),
            post_execute=None,
        )

    return AsyncTask(
        name=name,
        pre_execute=TaskFunction(function=dummy_func) if has_pre else None,
        execute=TaskFunction(function=dummy_func) if has_exec else None,
        post_execute=TaskFunction(function=dummy_func) if has_post else None,
    )


def get_task_wave_map(graph):
    """Build a map from task name to wave number."""
    task_to_wave = {}
    for wave in graph.waves:
        for task in wave.tasks:
            task_to_wave[task] = wave.wave_num
    return task_to_wave


def can_run_in_parallel(graph, task1: str, task2: str) -> bool:
    """Check if two tasks can potentially run in parallel using depends_on_tasks."""
    task_to_wave = get_task_wave_map(graph)
    if task1 not in task_to_wave or task2 not in task_to_wave:
        return False

    def effective_deps(t: str, seen: set[str] | None = None) -> set[str]:
        if seen is None:
            seen = set()
        if t in seen:
            return set()
        seen.add(t)
        w = graph.waves[task_to_wave[t]]
        deps = set(w.depends_on_tasks)
        for d in tuple(deps):
            deps |= effective_deps(d, seen)
        return deps

    deps1 = effective_deps(task1)
    deps2 = effective_deps(task2)
    return (task1 not in deps2) and (task2 not in deps1)


def verify_dependency_order(graph, task: str, dependencies: set) -> bool:
    """Verify that a task has all given dependencies (transitively)."""
    task_to_wave = get_task_wave_map(graph)
    if task not in task_to_wave:
        return False

    def effective_deps(t: str, seen: set[str] | None = None) -> set[str]:
        if seen is None:
            seen = set()
        if t in seen:
            return set()
        seen.add(t)
        w = graph.waves[task_to_wave[t]]
        deps = set(w.depends_on_tasks)
        for d in tuple(deps):
            deps |= effective_deps(d, seen)
        return deps

    eff = effective_deps(task)
    return dependencies.issubset(eff)


def format_wave_structure(graph) -> str:
    """Format wave structure as a string for assertion messages.

    Returns a readable representation of the wave structure.
    """
    lines = []
    for wave in graph.waves:
        deps = wave.depends_on_tasks
        deps_str = (
            f"depends_on_tasks={deps}" if deps else "no deps"
        )
        tasks_str = ", ".join(wave.tasks)
        lines.append(f"Wave {wave.wave_num}: [{tasks_str}] ({deps_str})")
    return "\n".join(lines)


class TestPostExecuteGraphEdgeCases:
    """Test edge cases in post-execute graph construction."""

    def test_cleanup_only_after_independent_chains(self):
        """
        Two independent chains where only one has cleanup.

        A -> B (with post)
        C -> D (no post)

        B should cleanup first (wave 0), then A (wave 1).
        D's completion doesn't affect cleanup order.
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True, has_post=True), depends_on=())
        analyzer.add_task(make_task("B", has_pre=True, has_post=True), depends_on=("A",))
        analyzer.add_task(make_task("C", has_pre=True), depends_on=())
        analyzer.add_task(make_task("D", has_pre=True), depends_on=("C",))

        analysis = analyzer.analyze()

        # Verify pre-execute: A, B, C, D all present
        pre_graph = analysis.pre_execute_graph
        pre_map = get_task_wave_map(pre_graph)
        assert set(pre_map.keys()) == {"A", "B", "C", "D"}

        # Verify dependency ordering
        assert verify_dependency_order(pre_graph, "B", {"A"})
        assert verify_dependency_order(pre_graph, "D", {"C"})

        # A and C can run in parallel (independent roots)
        assert can_run_in_parallel(pre_graph, "A", "C")

        # Post-execute: only A and B (C and D have no cleanup)
        post_graph = analysis.post_execute_graph
        post_map = get_task_wave_map(post_graph)
        assert set(post_map.keys()) == {"A", "B"}

        # B cleans up before A
        assert verify_dependency_order(post_graph, "A", {"B"})

    def test_parallel_cleanup_waves(self):
        """
        Cleanup waves that are independent should be identified.

        A -> B (both with post)
        C -> D (both with post)

        Cleanup: B and D (wave 0, parallel), then A and C (wave 1, parallel).
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True, has_post=True), depends_on=())
        analyzer.add_task(make_task("B", has_pre=True, has_post=True), depends_on=("A",))
        analyzer.add_task(make_task("C", has_pre=True, has_post=True), depends_on=())
        analyzer.add_task(make_task("D", has_pre=True, has_post=True), depends_on=("C",))

        analysis = analyzer.analyze()
        post_graph = analysis.post_execute_graph

        # Post-execute: 2 waves
        assert len(post_graph.waves) == 2

        # Find waves for each task
        wave_a = analysis.post_wave_containing("A")
        wave_b = analysis.post_wave_containing("B")
        wave_c = analysis.post_wave_containing("C")
        wave_d = analysis.post_wave_containing("D")

        # B and D are leaves (can cleanup in parallel)
        assert wave_b is not None and wave_d is not None
        assert wave_b.wave_num == wave_d.wave_num, "B and D should be in the same wave"
        assert set(wave_b.tasks) == {"B", "D"}
        assert wave_b.depends_on_tasks == ()

        # A and C depend on B/D's wave (via depends_on_tasks)
        assert wave_a is not None and wave_c is not None
        assert wave_a.wave_num == wave_c.wave_num, "A and C should be in the same wave"
        assert set(wave_a.tasks) == {"A", "C"}
        assert set(wave_a.depends_on_tasks) == {"B", "D"}


class TestNodeEdgeCases:
    """Test edge cases with nodes."""

    def test_all_nodes(self):
        """Graph with only nodes should produce empty execution graphs."""
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", is_node=True), depends_on=())
        analyzer.add_task(make_task("B", is_node=True), depends_on=("A",))
        analyzer.add_task(make_task("C", is_node=True), depends_on=("B",))

        analysis = analyzer.analyze()

        # Note: In our test implementation, nodes have an execute function
        # to satisfy AsyncTask validation, so they appear in execute phase
        # but don't create barriers in pre/post phases
        assert len(analysis.pre_execute_graph.waves) == 0
        assert len(analysis.post_execute_graph.waves) == 0
        # Nodes count represents truly functionless tasks (conceptual nodes)
        # With AsyncTask validation, this should be zero
        assert analysis.nodes == 0
        assert analysis.tasks_with_execute == 3  # But all 3 are execute-only

    def test_node_with_only_post_execute(self):
        """
        Node in execution graph but has cleanup.

        A (pre, post) -> N (only post) -> B (pre, post)

        Pre-execute: A -> B (N doesn't block)
        Post-execute: B -> N -> A (N does participate in cleanup)
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True, has_post=True), depends_on=())
        analyzer.add_task(make_task("N", has_post=True), depends_on=("A",))  # Cleanup-only
        analyzer.add_task(make_task("B", has_pre=True, has_post=True), depends_on=("N",))

        analysis = analyzer.analyze()
        pre_graph = analysis.pre_execute_graph
        post_graph = analysis.post_execute_graph

        # Pre-execute: N doesn't block B
        assert len(pre_graph.waves) == 2

        wave_a = analysis.pre_wave_containing("A")
        wave_b = analysis.pre_wave_containing("B")

        assert wave_a is not None and wave_a.tasks == ("A",)
        assert wave_b is not None and wave_b.tasks == ("B",)

        # Post-execute: N participates in cleanup
        assert len(post_graph.waves) == 3

        post_wave_a = analysis.post_wave_containing("A")
        post_wave_n = analysis.post_wave_containing("N")
        post_wave_b = analysis.post_wave_containing("B")

        assert post_wave_b is not None and post_wave_b.tasks == ("B",)
        assert post_wave_n is not None and post_wave_n.tasks == ("N",)
        assert post_wave_a is not None and post_wave_a.tasks == ("A",)

        # Verify cleanup order: B -> N -> A
        assert post_wave_b.wave_num < post_wave_n.wave_num < post_wave_a.wave_num


class TestComplexDependencies:
    """Test complex dependency patterns."""

    def test_multi_level_diamond(self):
        """
        Complex diamond with multiple levels:

             A
           /   \\
          B     C
         / \\   / \\
        D   E F   G
         \\ / \\ /
          H   I
           \\ /
            J
        """
        analyzer = TaskAnalyzer()

        analyzer.add_task(make_task("A", has_pre=True), depends_on=())
        analyzer.add_task(make_task("B", has_pre=True), depends_on=("A",))
        analyzer.add_task(make_task("C", has_pre=True), depends_on=("A",))
        analyzer.add_task(make_task("D", has_pre=True), depends_on=("B",))
        analyzer.add_task(make_task("E", has_pre=True), depends_on=("B",))
        analyzer.add_task(make_task("F", has_post=True), depends_on=("C",))
        analyzer.add_task(make_task("G", has_pre=True), depends_on=("C",))
        analyzer.add_task(make_task("H", has_pre=True), depends_on=("D", "E"))
        analyzer.add_task(make_task("I", has_pre=True), depends_on=("F", "G"))
        analyzer.add_task(make_task("J", has_pre=True), depends_on=("H", "I"))

        analysis = analyzer.analyze()
        graph = analysis.pre_execute_graph

        # Verify all tasks with pre_execute are present (F only has post_execute)
        task_map = get_task_wave_map(graph)
        assert set(task_map.keys()) == {"A", "B", "C", "D", "E", "G", "H", "I", "J"}, (
            f"Expected 9 tasks with pre_execute (F only has post), got {set(task_map.keys())}\n"
            f"Actual structure:\n{format_wave_structure(graph)}"
        )

        # Assert we have waves (structure visible in failure message)
        assert len(graph.waves) > 0, (
            f"Expected multiple waves for complex diamond, got {len(graph.waves)}\n"
            f"Actual structure:\n{format_wave_structure(graph)}"
        )

        # Verify dependency ordering
        assert verify_dependency_order(graph, "B", {"A"})
        assert verify_dependency_order(graph, "C", {"A"})
        assert verify_dependency_order(graph, "D", {"B"})
        assert verify_dependency_order(graph, "E", {"B"})
        # F only has post_execute, so not in pre-execute graph
        assert verify_dependency_order(graph, "G", {"C"})
        assert verify_dependency_order(graph, "H", {"D", "E"})
        # I depends on F (post-only) and G
        # Since G already depends on C, I only needs to depend on G (gets C transitively)
        assert verify_dependency_order(graph, "I", {"G"})
        assert verify_dependency_order(graph, "J", {"H", "I"})

        # Key parallelism opportunities
        assert can_run_in_parallel(graph, "B", "C")
        assert can_run_in_parallel(graph, "D", "E")
        assert can_run_in_parallel(graph, "D", "G")  # Independent branches
        assert can_run_in_parallel(graph, "H", "I")  # Final convergence

        # Post-execute graph: F should appear here
        post_graph = analysis.post_execute_graph
        assert len(post_graph.waves) > 0, "Post-execute graph should have F"
        post_task_map = get_task_wave_map(post_graph)
        assert "F" in post_task_map, "F (post-only) should be in post-execute graph"

    def test_skip_intermediate_waves(self):
        """
        Test that wave dependencies skip intermediate waves when possible.

        A -> B
        A -> C -> D

        D should depend only on wave with C, not wave with B.
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True), depends_on=())
        analyzer.add_task(make_task("B", has_pre=True), depends_on=("A",))
        analyzer.add_task(make_task("C", has_pre=True), depends_on=("A",))
        analyzer.add_task(make_task("D", has_pre=True), depends_on=("C",))

        analysis = analyzer.analyze()
        graph = analysis.pre_execute_graph

        # Verify all tasks present
        task_map = get_task_wave_map(graph)
        assert set(task_map.keys()) == {"A", "B", "C", "D"}

        # Verify dependency ordering
        assert verify_dependency_order(graph, "B", {"A"})
        assert verify_dependency_order(graph, "C", {"A"})
        assert verify_dependency_order(graph, "D", {"C"})

        # Key properties
        # B and C can run in parallel (both depend on A)
        assert can_run_in_parallel(graph, "B", "C")
        # B and D are in independent chains -> they can run in parallel
        assert can_run_in_parallel(graph, "B", "D")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
