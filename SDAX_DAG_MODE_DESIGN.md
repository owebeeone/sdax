## SDAX DAG Mode – Concise Runtime Design

### Waves (start barriers only)
- A wave groups tasks that share the same predecessor task set (effective dependencies).
- Waves have no intrinsic ordering; there are no wave→wave dependencies.
- Each wave carries depends_on_tasks: the set of prerequisite task names.
- A wave is only a barrier to starting its tasks. Tasks within a wave finish independently; there is no intra‑wave completion barrier.

### Pre‑execute phase
- Group tasks by identical effective_deps into waves. Keep a deterministic order in each wave.
- Maintain per‑wave completed_count (initially 0) and depends_on_tasks (fixed set).
- Maintain task_to_consumer_waves: for each predecessor task, which waves it unlocks.
- Use a single global TaskGroup for the entire pre phase; stage submissions by readiness:
  - Seed: schedule the single root wave with len(depends_on_tasks) == 0 (grouping by empty deps ensures exactly one seed wave containing all roots).
  - When a predecessor task completes, increment completed_count for its consumer waves; when completed_count == len(depends_on_tasks), schedule that wave’s tasks into the same TaskGroup.
  - Mark tasks “started” when scheduled so their post‑execute will run even if cancelled mid‑call.
- Failure policy: on first pre error or timeout, let the TaskGroup cancel all running pre tasks; stop scheduling new waves. CancelledError should not be retried.

### Execute phase
- Run one TaskGroup over all tasks whose pre succeeded. Nodes without execute are skipped.
- Aggregate exceptions from the TaskGroup after it completes.

### Post‑execute phase (best‑effort cleanup)
- Only run for tasks whose pre was started (started set), regardless of success or cancellation.
- Order by the reverse task dependency graph (dependents clean up before dependencies). No waves required.
- Run each post in its own small TaskGroup (or isolated context) and gather results to avoid one failure cancelling siblings.
- Aggregate post exceptions without cancelling other cleanups. Callers decide policy; post should be idempotent and robust to partial init.

### Analyzer outputs (recommended)
- ExecutionWave:
  - tasks: tuple[str, ...]
  - depends_on_tasks: tuple[str, ...] (effective predecessor task names)
- Convenience metadata:
  - wave_index_by_task: dict[str, int]
  - task_to_consumer_waves: dict[str, tuple[int, ...]]
  - wave_dep_count (total dependency count): dict[int, int]
- Grouping rule: group by identical effective_deps (no need to include direct_dependents for this model).

### Validation (build-time)
- Analyzer must validate the graph before building a processor:
  - All dependencies reference existing tasks.
  - No cycles (raise ValueError with cycle path).
- Fail fast: cycle/missing-dependency errors are raised during analysis/build (when creating the immutable DagTaskProcessor), not deferred to execution time.

### Immutability & concurrent executions
- TaskAnalyzer is immutable; its analysis output can be shared across runs.
- DagTaskProcessor built from the analysis is also immutable and safe to share.
- Each call to process_tasks(ctx) creates per‑execution runtime state:
  - per‑wave completed_count map and ready queue
  - global pre TaskGroup scope and failure flag
  - started task set for post‑execute eligibility
  - per‑execution exception aggregation

### Optional runtime features
- Concurrency limits: optional global or per‑wave semaphore to cap fan‑out.
- Timeouts/retries: per‑phase settings; CancelledError should short‑circuit retries.
- Telemetry: per‑task timings, retries, cancellations, exception taxonomy for observability.


