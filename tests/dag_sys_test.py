"""
"""

from pprint import pprint
from dag_system import (
    Node, 
    Graph, 
    generate_random_dag, 
    validate_forward, 
    validate_reverse
)

def run_tests():
    print("--- Running Validation Tests on Manual Graph ---")
    
    # 1. Create your example graph: 1<-3, 3<-9, 1<-4, 4<-9
    #    - "9" is origin
    #    - "1" is end
    #    - "1" depends on "3", "4"
    #    - "3" depends on "9"
    #    - "4" depends on "9"
    g_test = Graph(nodes={
        "1": Node(name="1", dependencies=("3", "4"), dependents=()),
        "3": Node(name="3", dependencies=("9",), dependents=("1",)),
        "4": Node(name="4", dependencies=("9",), dependents=("1",)),
        "9": Node(name="9", dependencies=(), dependents=("3", "4")),
    })
    
    print("Test Graph:")
    pprint(g_test.nodes)
    print("-" * 20)

    # --- Forward Validation Tests ---
    print("\n[Forward Validation]")
    trace_f_correct_1 = ("9", "3", "4", "1")
    trace_f_correct_2 = ("9", "4", "3", "1")
    trace_f_wrong_1 = ("1", "3", "4", "9") # Your example of a "wrong" one
    trace_f_wrong_2 = ("9", "1", "3", "4") # "1" visited before "3", "4"

    print(f"Trace {trace_f_correct_1}: {validate_forward(g_test, trace_f_correct_1, set())}")
    print(f"Trace {trace_f_correct_2}: {validate_forward(g_test, trace_f_correct_2, set())}")
    print(f"Trace {trace_f_wrong_1}: {validate_forward(g_test, trace_f_wrong_1, set())}")
    print(f"Trace {trace_f_wrong_2}: {validate_forward(g_test, trace_f_wrong_2, set())}")

    # --- Reverse Validation Tests ---
    print("\n[Reverse Validation]")
    trace_r_correct_1 = ("1", "3", "4", "9")
    trace_r_correct_2 = ("1", "4", "3", "9")
    trace_r_wrong_1 = ("9", "4", "3", "1") # "9" visited before "3", "4"

    print(f"Trace {trace_r_correct_1}: {validate_reverse(g_test, trace_r_correct_1, set())}")
    print(f"Trace {trace_r_correct_2}: {validate_reverse(g_test, trace_r_correct_2, set())}")
    print(f"Trace {trace_r_wrong_1}: {validate_reverse(g_test, trace_r_wrong_1, set())}")

    # --- Empty Node Tests ---
    print("\n[Empty Node Validation]")
    
    # Forward: 4 is empty. Graph -> 1<-3, 3<-9, 1<-9
    empty_fwd = {"4"}
    trace_f_empty = ("9", "3", "1") # "1" now only needs "9" and "3"
    print(f"Forward empty={empty_fwd}, trace={trace_f_empty}:")
    print(f"  Result: {validate_forward(g_test, trace_f_empty, empty_fwd)}")

    # Reverse: 3 is empty. Reverse Graph -> 1->4, 4->9, 1->9
    # This means "9" now effectively depends on "1" (via "3") and "4".
    empty_rev = {"3"}
    trace_r_empty = ("1", "4", "9") # "9" now needs "1" (via "3") and "4"
    print(f"\nReverse empty={empty_rev}, trace={trace_r_empty}:")
    print(f"  Result: {validate_reverse(g_test, trace_r_empty, empty_rev)}")
    
    print("\n--- Running DAG Generation ---")
    
    try:
        # Generate a random graph
        g_random = generate_random_dag(
            total_nodes_count=20,
            origin_nodes_count=3,
            end_nodes_count=3,
            min_deps_per_node=2, # Your N
            max_deps_per_node=8, # Your P
            random_seed=43
        )
        
        print(f"Generated a {len(g_random.nodes)}-node graph.")
        # Pretty print a few nodes
        print("Sample nodes:")
        for i in range(21):
            if str(i) in g_random.nodes:
                print(f"  {g_random.nodes[str(i)]}")
                
        # Find and print origins and ends
        origins = [n.name for n in g_random.nodes.values() if not n.dependencies]
        ends = [n.name for n in g_random.nodes.values() if not n.dependents]
        print(f"\nActual Origins: {origins} (Count: {len(origins)})")
        print(f"Actual Ends: {ends} (Count: {len(ends)})")
        
    except ValueError as e:
        print(f"Error generating graph: {e}")

if __name__ == "__main__":
    run_tests()
