"""Tests for SDAX Task Dependency Analyzer.

Tests complex dependency graphs to validate wave construction assumptions.
"""

from typing import Any

import pytest

from sdax.sdax_core import AsyncTask, TaskFunction
from sdax.sdax_task_analyser import TaskAnalyzer


# Helper functions for creating test tasks
async def dummy_func(ctx: Any):
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
        # Use execute as it's the "default" phase
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
    """Build a map from task name to wave number.

    Returns: Dict[str, int] mapping task name to its wave number
    """
    task_to_wave = {}
    for wave in graph.waves:
        for task in wave.tasks:
            task_to_wave[task] = wave.wave_num
    return task_to_wave


def can_run_in_parallel(graph, task1: str, task2: str) -> bool:
    """Check if two tasks can potentially run in parallel.

    Two tasks can run in parallel if neither depends (transitively) on the other.
    Uses wave.depends_on_tasks recursively to derive effective dependencies.
    """
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
        deps_str = f"depends_on_tasks={deps}" if deps else "no deps"
        tasks_str = ", ".join(wave.tasks)
        lines.append(f"Wave {wave.wave_num}: [{tasks_str}] ({deps_str})")
    return "\n".join(lines)


class TestBasicGraphs:
    """Test basic dependency graph patterns."""

    def test_simple_linear_chain(self):
        """A -> B -> C: Simple linear dependency chain."""
        analyzer = TaskAnalyzer[Any, str]()
        analyzer.add_task(make_task("A", has_pre=True, has_post=True), depends_on=())
        analyzer.add_task(make_task("B", has_pre=True, has_post=True), depends_on=("A",))
        analyzer.add_task(make_task("C", has_pre=True, has_post=True), depends_on=("B",))

        analysis = analyzer.analyze()
        pre_graph = analysis.pre_execute_graph
        post_graph = analysis.post_execute_graph

        # Pre-execute: sequential waves
        assert len(pre_graph.waves) == 3

        wave_a = analysis.pre_wave_containing("A")
        wave_b = analysis.pre_wave_containing("B")
        wave_c = analysis.pre_wave_containing("C")

        assert wave_a is not None and wave_a.tasks == ("A",)
        assert wave_a.depends_on_tasks == ()

        assert wave_b is not None and wave_b.tasks == ("B",)
        assert set(wave_b.depends_on_tasks) == {"A"}

        assert wave_c is not None and wave_c.tasks == ("C",)
        assert set(wave_c.depends_on_tasks) == {"B"}

        # Post-execute: reverse order
        assert len(post_graph.waves) == 3

        post_wave_a = analysis.post_wave_containing("A")
        post_wave_b = analysis.post_wave_containing("B")
        post_wave_c = analysis.post_wave_containing("C")

        assert post_wave_c is not None and post_wave_c.tasks == ("C",)
        assert post_wave_b is not None and post_wave_b.tasks == ("B",)
        assert post_wave_a is not None and post_wave_a.tasks == ("A",)

    def test_independent_chains(self):
        """A -> B  and  C -> D: Two independent chains.

        Independent chains should allow parallel execution.
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True), depends_on=())
        analyzer.add_task(make_task("B", has_pre=True), depends_on=("A",))
        analyzer.add_task(make_task("C", has_pre=True), depends_on=())
        analyzer.add_task(make_task("D", has_pre=True), depends_on=("C",))

        analysis = analyzer.analyze()
        graph = analysis.pre_execute_graph

        # Verify all tasks are present
        task_map = get_task_wave_map(graph)
        assert set(task_map.keys()) == {"A", "B", "C", "D"}

        # Assert wave structure for visibility
        # Roots (A, C) share the same effective dependencies (none) and should be grouped
        assert len(graph.waves) == 3, (
            f"Expected 3 waves with grouped roots, got {len(graph.waves)}\n"
            f"Actual structure:\n{format_wave_structure(graph)}"
        )

        # Verify each task is in exactly one wave
        all_wave_tasks = [task for wave in graph.waves for task in wave.tasks]
        assert len(all_wave_tasks) == 4, (
            f"Each task should appear exactly once, got {len(all_wave_tasks)}\n"
            f"Actual structure:\n{format_wave_structure(graph)}"
        )

        # Verify dependency ordering and grouping
        assert verify_dependency_order(graph, "B", {"A"})
        assert verify_dependency_order(graph, "D", {"C"})
        # Wave 0 must contain both A and C (grouped roots)
        assert set(graph.waves[0].tasks) == {"A", "C"}
        # Wave 1 depends on A; Wave 2 depends on C (independent chains)
        assert set(graph.waves[1].depends_on_tasks) == {"A"}
        assert set(graph.waves[2].depends_on_tasks) == {"C"}

        # Key property: A and C can run in parallel (independent roots)
        assert can_run_in_parallel(graph, "A", "C")

        # Key property: B and D can potentially run in parallel
        # (they're in independent chains)
        assert can_run_in_parallel(graph, "B", "D")

        # Post-execute graph should be empty (no tasks have post_execute)
        post_graph = analysis.post_execute_graph
        assert len(post_graph.waves) == 0, "Post-execute graph should be empty"

    def test_diamond_pattern(self):
        r"""
        Diamond dependency:
            A
           / \
          B   C
           \ /
            D
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True), depends_on=())
        analyzer.add_task(make_task("B", has_pre=True), depends_on=("A",))
        analyzer.add_task(make_task("C", has_pre=True), depends_on=("A",))
        analyzer.add_task(make_task("D", has_pre=True), depends_on=("B", "C"))

        analysis = analyzer.analyze()
        graph = analysis.pre_execute_graph

        # Verify all tasks present
        task_map = get_task_wave_map(graph)
        assert set(task_map.keys()) == {"A", "B", "C", "D"}

        # Verify dependency ordering
        assert verify_dependency_order(graph, "B", {"A"})
        assert verify_dependency_order(graph, "C", {"A"})
        assert verify_dependency_order(graph, "D", {"B", "C"})

        # Key property: B and C can run in parallel
        assert can_run_in_parallel(graph, "B", "C")

        # Post-execute graph should be empty (no tasks have post_execute)
        post_graph = analysis.post_execute_graph
        assert len(post_graph.waves) == 0, "Post-execute graph should be empty"


class TestNodesAsBarriers:
    """Test that nodes (tasks with no functions) don't create sync barriers."""

    def test_node_in_chain(self):
        """A -> N -> B where N is a node (no functions)."""
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True), depends_on=())
        analyzer.add_task(make_task("N", is_node=True), depends_on=("A",))  # Node
        analyzer.add_task(make_task("B", has_pre=True), depends_on=("N",))

        analysis = analyzer.analyze()

        # Pre-execute: B should be in wave 1 (depends on A, not N)
        assert len(analysis.pre_execute_graph.waves) == 2
        assert analysis.pre_execute_graph.waves[0].tasks == ("A",)
        assert analysis.pre_execute_graph.waves[1].tasks == ("B",)
        assert set(analysis.pre_execute_graph.waves[1].depends_on_tasks) == {"A"}

        # Post-execute graph should be empty (no tasks have post_execute)
        assert len(analysis.post_execute_graph.waves) == 0, "Post-execute graph should be empty"

    def test_node_doesnt_block_parallel_execution(self):
        """
        A -> B -> D
        A -> C -> E

        Where C is a node (no pre_execute). E should be able to run in parallel with B.
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True), depends_on=())
        analyzer.add_task(make_task("B", has_pre=True), depends_on=("A",))
        analyzer.add_task(make_task("C", is_node=True), depends_on=("A",))  # Node
        analyzer.add_task(make_task("D", has_pre=True), depends_on=("B",))
        analyzer.add_task(make_task("E", has_pre=True), depends_on=("C",))

        analysis = analyzer.analyze()
        graph = analysis.pre_execute_graph

        # Verify tasks present (C is a node, so not in pre_execute graph)
        task_map = get_task_wave_map(graph)
        assert set(task_map.keys()) == {"A", "B", "D", "E"}

        # Verify dependency ordering
        # E depends on C (node), which depends on A, so E effectively depends on A
        assert verify_dependency_order(graph, "B", {"A"})
        assert verify_dependency_order(graph, "E", {"A"})  # Through node C
        assert verify_dependency_order(graph, "D", {"B"})

        # Key property: B and E can run in parallel (both depend only on A)
        assert can_run_in_parallel(graph, "B", "E")

        # Post-execute graph should be empty (no tasks have post_execute)
        assert len(analysis.post_execute_graph.waves) == 0, "Post-execute graph should be empty"

    def test_multiple_nodes_in_chain(self):
        """A -> N1 -> N2 -> B where N1 and N2 are nodes."""
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True), depends_on=())
        analyzer.add_task(make_task("N1", is_node=True), depends_on=("A",))  # Node
        analyzer.add_task(make_task("N2", is_node=True), depends_on=("N1",))  # Node
        analyzer.add_task(make_task("B", has_pre=True), depends_on=("N2",))

        analysis = analyzer.analyze()

        # B should depend directly on A (through nodes)
        assert len(analysis.pre_execute_graph.waves) == 2
        assert analysis.pre_execute_graph.waves[0].tasks == ("A",)
        assert analysis.pre_execute_graph.waves[1].tasks == ("B",)

        # Post-execute graph should be empty (no tasks have post_execute)
        assert len(analysis.post_execute_graph.waves) == 0, "Post-execute graph should be empty"


class TestComplexGraphs:
    """Test complex real-world-like dependency graphs."""

    def test_build_system_graph(self):
        """
        Simulates a build system:
            compile_a ──┐
            compile_b ──┼─> link_exe -> test
            compile_c ──┼─> link_lib -> package
        """
        analyzer = TaskAnalyzer()

        # Compile phase
        analyzer.add_task(make_task("compile_a", has_pre=True), depends_on=())
        analyzer.add_task(make_task("compile_b", has_pre=True), depends_on=())
        analyzer.add_task(make_task("compile_c", has_pre=True), depends_on=())

        # Link phase
        analyzer.add_task(
            make_task("link_exe", has_pre=True), depends_on=("compile_a", "compile_b")
        )
        analyzer.add_task(
            make_task("link_lib", has_pre=True), depends_on=("compile_b", "compile_c")
        )

        # Final phase
        analyzer.add_task(make_task("test", has_pre=True), depends_on=("link_exe",))
        analyzer.add_task(make_task("package", has_pre=True), depends_on=("link_lib",))

        analysis = analyzer.analyze()
        graph = analysis.pre_execute_graph

        # Verify all tasks present
        task_map = get_task_wave_map(graph)
        assert set(task_map.keys()) == {
            "compile_a",
            "compile_b",
            "compile_c",
            "link_exe",
            "link_lib",
            "test",
            "package",
        }

        # Verify dependency ordering
        assert verify_dependency_order(graph, "link_exe", {"compile_a", "compile_b"})
        assert verify_dependency_order(graph, "link_lib", {"compile_b", "compile_c"})
        assert verify_dependency_order(graph, "test", {"link_exe"})
        assert verify_dependency_order(graph, "package", {"link_lib"})

        # Key properties: parallel execution opportunities
        # All compiles can run in parallel
        assert can_run_in_parallel(graph, "compile_a", "compile_b")
        assert can_run_in_parallel(graph, "compile_a", "compile_c")
        assert can_run_in_parallel(graph, "compile_b", "compile_c")

        # Both links can run in parallel
        assert can_run_in_parallel(graph, "link_exe", "link_lib")

        # Test and package can run in parallel
        assert can_run_in_parallel(graph, "test", "package")

    def test_data_pipeline_with_nodes(self):
        """
        Data pipeline with milestone nodes:
            fetch_users  ─┐
            fetch_orders  ┼─> [all_data] ─┬─> validate ─┐
            fetch_products┘               └─> transform ┼─> [ready] -> load
        """
        analyzer = TaskAnalyzer()

        # Fetch phase
        analyzer.add_task(make_task("fetch_users", has_pre=True), depends_on=())
        analyzer.add_task(make_task("fetch_orders", has_pre=True), depends_on=())
        analyzer.add_task(make_task("fetch_products", has_pre=True), depends_on=())

        # Milestone node
        analyzer.add_task(
            make_task("all_data", is_node=True),
            depends_on=("fetch_users", "fetch_orders", "fetch_products"),
        )  # Node

        # Process phase
        analyzer.add_task(make_task("validate", has_pre=True), depends_on=("all_data",))
        analyzer.add_task(make_task("transform", has_pre=True), depends_on=("all_data",))

        # Milestone node
        analyzer.add_task(
            make_task("ready", is_node=True), depends_on=("validate", "transform")
        )  # Node

        # Load phase
        analyzer.add_task(make_task("load", has_pre=True), depends_on=("ready",))

        analysis = analyzer.analyze()
        graph = analysis.pre_execute_graph

        # Verify all tasks present (nodes not in pre_execute graph)
        task_map = get_task_wave_map(graph)
        assert set(task_map.keys()) == {
            "fetch_users",
            "fetch_orders",
            "fetch_products",
            "validate",
            "transform",
            "load",
        }

        # Verify dependency ordering
        # validate and transform depend on all_data (node), which depends on all fetches
        assert verify_dependency_order(
            graph, "validate", {"fetch_users", "fetch_orders", "fetch_products"}
        )
        assert verify_dependency_order(
            graph, "transform", {"fetch_users", "fetch_orders", "fetch_products"}
        )
        # load depends on ready (node), which depends on validate and transform
        assert verify_dependency_order(graph, "load", {"validate", "transform"})

        # Key properties: parallel execution
        # All fetches can run in parallel
        assert can_run_in_parallel(graph, "fetch_users", "fetch_orders")
        assert can_run_in_parallel(graph, "fetch_users", "fetch_products")
        assert can_run_in_parallel(graph, "fetch_orders", "fetch_products")

        # validate and transform can run in parallel (both depend on fetches through node)
        assert can_run_in_parallel(graph, "validate", "transform")

    def test_mixed_phases_graph(self):
        """
        Test with tasks having different phase combinations.

        A: (pre, execute, post)
        B: (pre, post)
        C: (None, None, post) - cleanup-only node
        D: (pre, None, None)
        E: (None, execute, None)
        """
        analyzer = TaskAnalyzer()

        analyzer.add_task(make_task("A", has_pre=True, has_exec=True, has_post=True), depends_on=())
        analyzer.add_task(make_task("B", has_pre=True, has_post=True), depends_on=("A",))
        analyzer.add_task(
            make_task("C", has_post=True),  # Cleanup-only
            depends_on=("A",),
        )
        analyzer.add_task(
            make_task("D", has_pre=True),  # Setup-only
            depends_on=("B",),
        )
        analyzer.add_task(
            make_task("E", has_exec=True),  # Execute-only
            depends_on=("C",),
        )

        analysis = analyzer.analyze()

        print("\n" + str(analysis))

        # Pre-execute: only A, B, D
        assert len(analysis.pre_execute_graph.waves) == 3
        assert analysis.pre_execute_graph.waves[0].tasks == ("A",)
        assert analysis.pre_execute_graph.waves[1].tasks == ("B",)
        assert analysis.pre_execute_graph.waves[2].tasks == ("D",)

        # Post-execute: only A, B, C
        assert len(analysis.post_execute_graph.waves) == 2
        # C and D are leaves (no dependents with post_execute)
        # But D has no post_execute, so only C in wave 0
        # Actually, C depends on A, so...
        # Let me think: reverse order for cleanup
        # D has no post_execute, E has no post_execute
        # So cleanup: D has no cleanup, B cleans up, C cleans up, A cleans up
        # Reverse dependencies: who depends on me?
        # B depends on A, C depends on A, D depends on B, E depends on C
        # For cleanup: start with leaves (tasks with no dependents that have post_execute)
        # D has dependent E (no post_execute), so D is a leaf for cleanup (but D has no post)
        # B has dependent D (no post_execute), so B is a leaf for cleanup
        # C has dependent E (no post_execute), so C is a leaf for cleanup
        # So wave 0: B and C
        # Then A (wave 1)
        assert set(analysis.post_execute_graph.waves[0].tasks) == {"B", "C"}
        assert analysis.post_execute_graph.waves[1].tasks == ("A",)


class TestWaveDependencies:
    """Test that wave dependencies are precise (not just all earlier waves)."""

    def test_precise_wave_dependencies(self):
        """
        A -> B
        C -> D

        Tests that B and D (from independent chains) can run in parallel.
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True), depends_on=())
        analyzer.add_task(make_task("B", has_pre=True), depends_on=("A",))
        analyzer.add_task(make_task("C", has_pre=True), depends_on=())
        analyzer.add_task(make_task("D", has_pre=True), depends_on=("C",))

        analysis = analyzer.analyze()
        graph = analysis.pre_execute_graph

        # Verify all tasks present
        task_map = get_task_wave_map(graph)
        assert set(task_map.keys()) == {"A", "B", "C", "D"}

        # Verify dependency ordering
        assert verify_dependency_order(graph, "B", {"A"})
        assert verify_dependency_order(graph, "D", {"C"})

        # Key property: B and D can run in parallel (independent chains)
        assert can_run_in_parallel(graph, "B", "D")


class TestPostExecuteGraphs:
    """Additional tests focused on post-execute graph structure."""

    def test_post_linear_chain(self):
        """
        A -> B -> C (all with post)

        Cleanup order: C, then B, then A (three waves, reverse depth).
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_post=True), depends_on=())
        analyzer.add_task(make_task("B", has_post=True), depends_on=("A",))
        analyzer.add_task(make_task("C", has_post=True), depends_on=("B",))

        analysis = analyzer.analyze()
        post_graph = analysis.post_execute_graph
        # No pre-execute functions → no pre waves
        assert len(analysis.pre_execute_graph.waves) == 0

        assert len(post_graph.waves) == 3

        wA = analysis.post_wave_containing("A")
        wB = analysis.post_wave_containing("B")
        wC = analysis.post_wave_containing("C")

        assert wC is not None and wC.wave_num == 0 and wC.tasks == ("C",)
        assert wB is not None and wB.wave_num == 1 and wB.tasks == ("B",)
        assert set(wB.depends_on_tasks) == {"C"}
        assert wA is not None and wA.wave_num == 2 and wA.tasks == ("A",)
        assert set(wA.depends_on_tasks) == {"B"}

    def test_post_two_independent_chains_three_levels(self):
        """
        A -> B -> D   and   C -> E -> F (all with post)

        Waves:
          0: D, F
          1: B, E   (depends_on_tasks = {D, F})
          2: A, C   (depends_on_tasks = {B, E})
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_post=True), depends_on=())
        analyzer.add_task(make_task("B", has_post=True), depends_on=("A",))
        analyzer.add_task(make_task("D", has_post=True), depends_on=("B",))

        analyzer.add_task(make_task("C", has_post=True), depends_on=())
        analyzer.add_task(make_task("E", has_post=True), depends_on=("C",))
        analyzer.add_task(make_task("F", has_post=True), depends_on=("E",))

        analysis = analyzer.analyze()
        post_graph = analysis.post_execute_graph
        # No pre-execute functions → no pre waves
        assert len(analysis.pre_execute_graph.waves) == 0

        assert len(post_graph.waves) == 3
        w0 = post_graph.waves[0]
        w1 = post_graph.waves[1]
        w2 = post_graph.waves[2]

        assert set(w0.tasks) == {"D", "F"}
        assert w0.depends_on_tasks == ()
        assert set(w1.tasks) == {"B", "E"}
        assert set(w1.depends_on_tasks) == {"D", "F"}
        assert set(w2.tasks) == {"A", "C"}
        assert set(w2.depends_on_tasks) == {"B", "E"}

    def test_post_ignores_non_post_tasks(self):
        """
        A (post) -> B (post) -> X (no post)

        Post graph should include only A and B:
          0: B
          1: A (depends_on_tasks = {B})
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_post=True), depends_on=())
        analyzer.add_task(make_task("B", has_post=True), depends_on=("A",))
        analyzer.add_task(make_task("X", has_pre=True), depends_on=("B",))  # no post

        analysis = analyzer.analyze()
        post_graph = analysis.post_execute_graph
        # Only X has pre in this test → exactly one pre wave containing X
        assert len(analysis.pre_execute_graph.waves) == 1
        assert analysis.pre_execute_graph.waves[0].tasks == ("X",)

        assert len(post_graph.waves) == 2
        w0 = post_graph.waves[0]
        w1 = post_graph.waves[1]

        assert w0.tasks == ("B",)
        assert w0.depends_on_tasks == ()
        assert w1.tasks == ("A",)
        assert set(w1.depends_on_tasks) == {"B"}

    def test_mixed_chain_post_follow_through_nodes(self):
        """
        Chain: A -> B -> C -> D -> E

        Phases:
          A: pre only
          B: post only
          C: pre only
          D: post only
          E: pre only

        Expectations:
          - Pre waves: only tasks with pre -> A, C, E (grouped by deps).
          - Post waves: only tasks with post -> D then B (D wave 0, B wave 1),
            because B depends (transitively) on D via C.
        """
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True), depends_on=())
        analyzer.add_task(make_task("B", has_post=True), depends_on=("A",))
        analyzer.add_task(make_task("C", has_pre=True), depends_on=("B",))
        analyzer.add_task(make_task("D", has_post=True), depends_on=("C",))
        analyzer.add_task(make_task("E", has_pre=True), depends_on=("D",))

        analysis = analyzer.analyze()

        # Pre graph contains only A, C, E
        pre_graph = analysis.pre_execute_graph
        pre_tasks = {t for w in pre_graph.waves for t in w.tasks}
        assert pre_tasks == {"A", "C", "E"}

        # Post graph contains only D then B (reverse order, follow-through non-post nodes)
        post_graph = analysis.post_execute_graph
        assert len(post_graph.waves) == 2
        w0 = post_graph.waves[0]
        w1 = post_graph.waves[1]
        assert w0.tasks == ("D",)
        assert w0.depends_on_tasks == ()
        assert w1.tasks == ("B",)
        assert set(w1.depends_on_tasks) == {"D"}


class TestErrorCases:
    """Test error handling."""

    def test_missing_dependency(self):
        """Reference to non-existent task should fail."""
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True), depends_on=("NonExistent",))

        with pytest.raises(ValueError, match="non-existent task"):
            analyzer.analyze()

    def test_cycle_detection(self):
        """Circular dependency should be detected."""
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True), depends_on=("C",))
        analyzer.add_task(make_task("B", has_pre=True), depends_on=("A",))
        analyzer.add_task(make_task("C", has_pre=True), depends_on=("B",))

        with pytest.raises(ValueError, match="Cycle detected"):
            analyzer.analyze()

    def test_self_cycle(self):
        """Task depending on itself should be detected."""
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True), depends_on=("A",))

        with pytest.raises(ValueError, match="Cycle detected"):
            analyzer.analyze()


class TestStatistics:
    """Test that statistics are computed correctly."""

    def test_statistics(self):
        """Verify statistics calculation."""
        analyzer = TaskAnalyzer()
        analyzer.add_task(make_task("A", has_pre=True, has_post=True), depends_on=())
        analyzer.add_task(make_task("B", has_exec=True), depends_on=())
        analyzer.add_task(make_task("C", is_node=True), depends_on=())  # Node
        analyzer.add_task(make_task("D", has_pre=True, has_exec=True), depends_on=())

        analysis = analyzer.analyze()

        assert analysis.total_tasks == 4
        assert analysis.tasks_with_pre_execute == 2  # A, D
        assert analysis.tasks_with_execute == 3  # B, D, C (node has execute for validation)
        assert analysis.tasks_with_post_execute == 1  # A


class TestRandomGraph:
    """Test with randomly generated graphs."""

    def test_random_graph_20_tasks(self):
        """
        Random graph with 20 tasks:
        - 4 root tasks (no dependencies)
        - Average 2 dependencies per non-root task
        - 60% have pre_execute
        - 40% have post_execute
        - 20% have execute
        - Tasks with no functions become nodes
        """
        import random

        # random.seed(42)  # For reproducibility

        analyzer = TaskAnalyzer()
        task_names = [f"T{i:02d}" for i in range(20)]

        # Track which tasks exist so we can refer to them
        existing_tasks = []

        # Create 4 root tasks first
        for i in range(4):
            name = task_names[i]

            # Randomly assign functions
            has_pre = random.random() < 0.6
            has_post = random.random() < 0.3
            has_exec = random.random() < 0.2

            # If no functions, make it a node (will have minimal execute for validation)
            if not has_pre and not has_post and not has_exec:
                task = make_task(name, is_node=True)
            else:
                task = make_task(name, has_pre=has_pre, has_post=has_post, has_exec=has_exec)

            analyzer.add_task(task, depends_on=())
            existing_tasks.append(name)

        # Create remaining 16 tasks with dependencies
        for i in range(4, 20):
            name = task_names[i]

            # Randomly assign functions
            has_pre = random.random() < 0.6
            has_post = random.random() < 0.3
            has_exec = random.random() < 0.2

            # If no functions, make it a node
            if not has_pre and not has_post and not has_exec:
                task = make_task(name, is_node=True)
            else:
                task = make_task(name, has_pre=has_pre, has_post=has_post, has_exec=has_exec)

            # Average 2 dependencies: randomly pick 1-3 dependencies from existing tasks
            num_deps = random.randint(1, min(3, len(existing_tasks)))
            dependencies = tuple(random.sample(existing_tasks, num_deps))

            analyzer.add_task(task, depends_on=dependencies)
            existing_tasks.append(name)

        # Analyze the graph
        analysis = analyzer.analyze()

        # Verify basic properties
        assert analysis.total_tasks == 20, f"Expected 20 tasks, got {analysis.total_tasks}"

        # Verify no cycles (analyze() would raise if there were cycles)
        # Verify all tasks are accounted for
        pre_tasks = set()
        for wave in analysis.pre_execute_graph.waves:
            pre_tasks.update(wave.tasks)

        post_tasks = set()
        for wave in analysis.post_execute_graph.waves:
            post_tasks.update(wave.tasks)

        # Count tasks with each phase
        all_tasks_dict = analysis.tasks
        tasks_with_pre = sum(1 for t in all_tasks_dict.values() if t.pre_execute is not None)
        tasks_with_post = sum(1 for t in all_tasks_dict.values() if t.post_execute is not None)

        # Verify graph structure
        assert len(pre_tasks) == tasks_with_pre, (
            f"Pre-execute graph should have {tasks_with_pre} tasks, got {len(pre_tasks)}"
        )
        assert len(post_tasks) == tasks_with_post, (
            f"Post-execute graph should have {tasks_with_post} tasks, got {len(post_tasks)}"
        )

        # Verify statistics
        assert analysis.tasks_with_pre_execute == tasks_with_pre
        assert analysis.tasks_with_post_execute == tasks_with_post

        # Verify dependency ordering in pre-execute graph
        task_to_wave = get_task_wave_map(analysis.pre_execute_graph)
        for task_name, deps in analysis.dependencies.items():
            if task_name not in task_to_wave:
                continue  # Task has no pre_execute

            # Get effective dependencies (only those with pre_execute)
            effective_deps = set()
            for dep in deps:
                if dep in task_to_wave:
                    effective_deps.add(dep)

            # Verify this task comes after its dependencies
            if effective_deps:
                assert verify_dependency_order(
                    analysis.pre_execute_graph, task_name, effective_deps
                ), f"Task {task_name} dependency order violation"

        # Print summary for visibility
        print("\nRandom Graph Summary:")
        print(f"  Total tasks: {analysis.total_tasks}")
        print(f"  Tasks with pre_execute: {analysis.tasks_with_pre_execute}")
        print(f"  Tasks with execute: {analysis.tasks_with_execute}")
        print(f"  Tasks with post_execute: {analysis.tasks_with_post_execute}")
        print(f"  Nodes (no functions): {analysis.nodes}")
        print(f"  Pre-execute waves: {len(analysis.pre_execute_graph.waves)}")
        print(f"  Post-execute waves: {len(analysis.post_execute_graph.waves)}")


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
