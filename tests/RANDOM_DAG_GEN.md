# **Specification: Random DAG Generation and Traversal Verification**

This document outlines the requirements for a Python module that generates a random Directed Acyclic Graph (DAG) and provides functions to validate forward and reverse traversals of that DAG, accounting for "empty" nodes.

## **1\. Parameters**

The generation and verification process will be controlled by the following parameters:

* **random\_seed**: (int) The seed for the random number generator to ensure reproducible results.  
* **total\_nodes\_count**: (int) The total number of nodes in the graph.  
* **origin\_nodes\_count**: (int) The number of nodes that will be "origin" nodes (guaranteed to have 0 dependencies/incoming edges).  
* **end\_nodes\_count**: (int) The number of nodes that will be "end" nodes (guaranteed to have 0 dependents/outgoing edges).  
* **min\_deps\_per\_node (N)**: (int) The *target minimum* number of dependencies for any non-origin node.  
* **max\_deps\_per\_node (P)**: (int) The *target maximum* number of dependencies for any non-origin node.  
* **graph**: (Graph) The graph object to be validated.  
* **trace**: (tuple\[str, ...\]) An ordered tuple of node name strings representing the visitation sequence.  
* **forward\_empty\_nodes**: (set\[str\]) A set of node names to be treated as "empty" during a *forward* traversal.  
* **reverse\_empty\_nodes**: (set\[str\]) A set of node names to be treated as "empty" during a *reverse* traversal.

## **2\. Data Structures**

The graph will be represented by two dataclass objects.

### **2.1. Node**

A dataclass representing a single node in the graph.

* **name**: (str) The unique string identifier for the node (e.g., "1", "10").  
* **dependencies**: (tuple\[str, ...\]) A tuple of node names that this node *depends on* (incoming edges). This corresponds to your "back tuple".  
* **dependents**: (tuple\[str, ...\]) A tuple of node names that *depend on this node* (outgoing edges). This corresponds to your "forward tuple".

### **2.2. Graph**

A dataclass representing the entire graph.

* **nodes**: (dict\[str, Node\]) A dictionary mapping node names (str) to their corresponding Node objects.

## **3\. Function: generate\_random\_dag**

### **3.1. Signature**

def generate\_random\_dag(  
    total\_nodes\_count: int,  
    origin\_nodes\_count: int,  
    end\_nodes\_count: int,  
    min\_deps\_per\_node: int,  
    max\_deps\_per\_node: int,  
    random\_seed: int  
) \-\> Graph:

### **3.2. Generation Algorithm**

The function will guarantee an acyclic graph by establishing a random topological order *before* creating edges.

1. **Validation**: A ValueError will be raised if origin\_nodes\_count \+ end\_nodes\_count \> total\_nodes\_count, as a node cannot be both an origin and an end.  
2. **Seeding**: Set random.seed(random\_seed).  
3. **Node Creation**: Create all total\_nodes\_count Node objects, named "1" through total\_nodes\_count, and store them in the graph.nodes dictionary.  
4. **Topological Shuffle**: Create a list of all node names and shuffle it. This shuffled list shuffled\_names defines the *only* valid direction for dependencies. An edge (u, v) can only be created if u appears *before* v in shuffled\_names.  
5. **Designate Sets**:  
   * origin\_names: The first origin\_nodes\_count names in shuffled\_names.  
   * end\_names: The last end\_nodes\_count names in shuffled\_names.  
6. **Edge Creation**: Iterate through shuffled\_names from index i \= 0 to total\_nodes\_count \- 1\.  
   * Let current\_node\_name \= shuffled\_names\[i\].  
   * **Origin Node Check**: If current\_node\_name is in origin\_names, skip it (it receives 0 dependencies).  
   * **Dependency Pool**: Identify the pool of *available* dependency nodes:  
     * available\_pool \= { u | u \= shuffled\_names\[j\], j \< i, u NOT IN end\_names }  
     * This rule ensures two things:  
       1. Acyclicity (dependency u comes before current\_node v).  
       2. "End Node" respect (a node in end\_names will never be selected as a dependency, so it will never have a dependent added to its list).  
   * **Dependency Count**:  
     * num\_available \= len(available\_pool).  
     * min\_deps \= min(min\_deps\_per\_node, num\_available)  
     * max\_deps \= min(max\_deps\_per\_node, num\_available)  
     * num\_to\_add \= random.randint(min\_deps, max\_deps) (if num\_available \> 0, else 0).  
   * **Add Edges**:  
     * Randomly sample num\_to\_add nodes from available\_pool.  
     * For each dep\_name sampled:  
       * Add dep\_name to graph.nodes\[current\_node\_name\].dependencies.  
       * Add current\_node\_name to graph.nodes\[dep\_name\].dependents.  
7. **Return**: Return the populated Graph object.

**Note on N..P Constraint**: The min/max\_deps\_per\_node parameters are *targets*. The actual number of dependencies will be constrained by the number of available, non-end nodes that appear earlier in the random topological sort.

## **4\. Function: validate\_forward**

### **4.1. Signature**

def validate\_forward(  
    graph: Graph,  
    trace: tuple\[str, ...\],  
    empty\_nodes: set\[str\]  
) \-\> bool:

### **4.2. Validation Logic**

This function validates a "visit log" trace against the graph's dependency rules.

1. **Uniqueness Check**: The trace must not contain duplicate nodes. If len(set(trace)) \!= len(trace), return False.  
2. **State**: Initialize visited \= set() and memo \= {} (for memoization).  
3. **Iteration**: Iterate through each node\_name in the trace.  
4. **Dependency Check**: For the current\_node\_name, find its *effective dependencies* by recursively resolving "empty" nodes.  
   * effective\_deps \= \_get\_effective\_dependencies(graph, node\_name, empty\_nodes, memo, direction="forward")  
5. **Validation**: Check if all effective\_deps are in the visited set.  
   * If not effective\_deps.issubset(visited), return False (a dependency was not met).  
6. **Update State**: Add node\_name to visited.  
7. **Return**: If the loop completes, return True.

## **5\. Function: validate\_reverse**

### **5.1. Signature**

def validate\_reverse(  
    graph: Graph,  
    trace: tuple\[str, ...\],  
    empty\_nodes: set\[str\]  
) \-\> bool:

### **5.2. Validation Logic**

This function validates a trace assuming a *reverse* traversal (i.e., a node can be "cleared" if all nodes that *depend on it* have been cleared).

The logic is **identical** to validate\_forward, but it operates on the node.dependents list instead of node.dependencies.

1. **Uniqueness Check**: (Same as forward).  
2. **State**: Initialize visited \= set() and memo \= {}.  
3. **Iteration**: Iterate through each node\_name in the trace.  
4. **Dependency Check**:  
   * effective\_deps \= \_get\_effective\_dependencies(graph, node\_name, empty\_nodes, memo, direction="reverse")  
5. **Validation**: (Same as forward).  
6. **Update State**: (Same as forward).  
7. **Return**: (Same as forward).

## **6\. Helper Function: \_get\_effective\_dependencies (Internal)**

This memoized, recursive helper function is the core of the validation logic.

1. **Input**: graph, node\_name, empty\_nodes, memo, direction ("forward" or "reverse").  
2. **Memoization**: If node\_name in memo, return memo\[node\_name\].  
3. **Get Neighbors**:  
   * If direction \== "forward", neighbors \= graph.nodes\[node\_name\].dependencies.  
   * If direction \== "reverse", neighbors \= graph.nodes\[node\_name\].dependents.  
4. **Resolve Empties**:  
   * Initialize effective\_deps \= set().  
   * For name in neighbors:  
     * If name in empty\_nodes: Recursively call \_get\_effective\_dependencies for name and effective\_deps.update() the result.  
     * If name not in empty\_nodes: Add name directly to effective\_deps.add(name).  
5. **Store & Return**: Store effective\_deps in memo\[node\_name\] and return it.