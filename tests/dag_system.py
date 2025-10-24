import random
from dataclasses import dataclass, field
from typing import Dict, Set, Tuple, Optional, Literal

# --- 1. Data Structures ---

@dataclass
class Node:
    """
    Represents a single node in the graph.
    
    Attributes:
        name: The unique string identifier for the node.
        dependencies: A tuple of node names that this node depends on (incoming edges).
        dependents: A tuple of node names that depend on this node (outgoing edges).
    """
    name: str
    dependencies: Tuple[str, ...]
    dependents: Tuple[str, ...]

    def __repr__(self) -> str:
        return (
            f"Node(name='{self.name}', "
            f"deps={list(self.dependencies)}, "
            f"dependents={list(self.dependents)})"
        )

@dataclass
class Graph:
    """
    Represents the entire graph as a collection of nodes.

    Attributes:
        nodes: A dictionary mapping node names (str) to their Node objects.
    """
    nodes: Dict[str, Node] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.nodes)

# --- 2. DAG Generation ---

def generate_random_dag(
    total_nodes_count: int,
    origin_nodes_count: int,
    end_nodes_count: int,
    min_deps_per_node: int,
    max_deps_per_node: int,
    random_seed: int
) -> Graph:
    """
    Generates a random Directed Acyclic Graph (DAG) based on the given parameters.

    Args:
        total_nodes_count: The total number of nodes in the graph.
        origin_nodes_count: Number of nodes with 0 incoming edges.
        end_nodes_count: Number of nodes with 0 outgoing edges.
        min_deps_per_node: Target minimum dependencies for non-origin nodes.
        max_deps_per_node: Target maximum dependencies for non-origin nodes.
        random_seed: Seed for the random number generator.

    Returns:
        A Graph object.
        
    Raises:
        ValueError: If (origin + end) > total nodes, or if parameters are invalid.
    """
    if total_nodes_count <= 0:
        raise ValueError("total_nodes_count must be positive.")
    if origin_nodes_count < 0 or end_nodes_count < 0:
        raise ValueError("Node counts cannot be negative.")
    if min_deps_per_node < 0 or max_deps_per_node < min_deps_per_node:
        raise ValueError("Invalid dependency counts (N..P).")
    if origin_nodes_count + end_nodes_count > total_nodes_count:
        raise ValueError(
            "origin_nodes_count + end_nodes_count cannot exceed total_nodes_count."
        )

    random.seed(random_seed)

    # 1. Create all nodes
    #    Initialize dependencies and dependents as empty tuples
    graph_nodes: Dict[str, Node] = {
        str(i): Node(name=str(i), dependencies=(), dependents=()) 
        for i in range(1, total_nodes_count + 1)
    }

    # 2. Create shuffled topological order
    node_names = list(graph_nodes.keys())
    random.shuffle(node_names)

    # 3. Designate origin and end sets
    origin_names = set(node_names[:origin_nodes_count])
    end_names = set(node_names[total_nodes_count - end_nodes_count:])

    # 4. Edge Creation
    for i, current_node_name in enumerate(node_names):
        
        # 4a. Skip origin nodes
        if current_node_name in origin_names:
            continue

        # 4b. Build available dependency pool
        # Pool = nodes before this one, EXCLUDING any designated end nodes
        available_deps_pool = [
            name for name in node_names[:i] if name not in end_names
        ]
        
        num_available = len(available_deps_pool)
        if num_available == 0:
            # This node cannot have dependencies, becomes a de-facto origin
            continue

        # 4c. Determine dependency count
        # Constrained by N, P, and number of available nodes
        min_deps = min(min_deps_per_node, num_available)
        max_deps = min(max_deps_per_node, num_available)
        
        num_to_add = random.randint(min_deps, max_deps)
        
        if num_to_add == 0:
            continue

        # 4d. Add edges
        dependency_names = random.sample(available_deps_pool, k=num_to_add)
        
        current_node = graph_nodes[current_node_name]
        current_node.dependencies = tuple(dependency_names)
        
        for dep_name in dependency_names:
            dep_node = graph_nodes[dep_name]
            # Add to the tuple (tuples are immutable, so we create a new one)
            dep_node.dependents += (current_node_name,)

    return Graph(nodes=graph_nodes)

# --- 3. Traversal Validation ---

def _get_effective_deps(
    graph: Graph,
    node_name: str,
    empty_nodes: Set[str],
    memo: Dict[str, Set[str]],
    direction: Literal["forward", "reverse"]
) -> Set[str]:
    """
    Internal helper to recursively find all non-empty dependencies.
    """
    if node_name in memo:
        return memo[node_name]

    if direction == "forward":
        neighbors = graph.nodes[node_name].dependencies
    else:
        neighbors = graph.nodes[node_name].dependents

    effective_deps: Set[str] = set()
    for name in neighbors:
        if name in empty_nodes:
            # This neighbor is "empty", so we need its dependencies instead
            effective_deps.update(
                _get_effective_deps(graph, name, empty_nodes, memo, direction)
            )
        else:
            # This is a real, non-empty dependency
            effective_deps.add(name)

    memo[node_name] = effective_deps
    return effective_deps


def _validate_traversal(
    graph: Graph,
    trace: Tuple[str, ...],
    empty_nodes: Set[str],
    direction: Literal["forward", "reverse"]
) -> bool:
    """
    Internal helper to perform the core validation logic.
    """

    if len(graph) != len(trace) + len(empty_nodes):
        print("Validation failed: Trace does not match graph size.")
        return False

    # 1. Uniqueness Check
    if len(set(trace)) != len(trace):
        print("Validation failed: Trace contains duplicate nodes.")
        return False

    # 2. State
    visited: Set[str] = set()
    memo: Dict[str, Set[str]] = {}

    # 3. Iteration
    for node_name in trace:
        if node_name not in graph.nodes:
            print(f"Validation failed: Node '{node_name}' not in graph.")
            return False

        # 4. Dependency Check
        effective_deps = _get_effective_deps(
            graph, node_name, empty_nodes, memo, direction
        )

        # 5. Validation
        if not effective_deps.issubset(visited):
            print(
                f"Validation failed for node '{node_name}':\n"
                f"  Effective Reqs: {effective_deps}\n"
                f"  Visited:        {visited}\n"
                f"  Missing:        {effective_deps - visited}"
            )
            return False
        
        # 6. Update State
        visited.add(node_name)

    # 7. Return
    return True


def validate_forward(
    graph: Graph,
    trace: Tuple[str, ...],
    empty_nodes: Set[str]
) -> bool:
    """
    Validates a forward traversal trace (e.g., "9", "3", "1").
    A node is valid to visit if all its *dependencies* have been visited.
    """
    return _validate_traversal(graph, trace, empty_nodes, direction="forward")


def validate_reverse(
    graph: Graph,
    trace: Tuple[str, ...],
    empty_nodes: Set[str]
) -> bool:
    """
    Validates a reverse traversal trace (e.g., "1", "3", "9").
    A node is valid to visit if all its *dependents* have been visited.
    """
    return _validate_traversal(graph, trace, empty_nodes, direction="reverse")

