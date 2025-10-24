"""Example: Analyzing a complex task dependency graph.

This demonstrates how the task analyzer builds execution waves
for optimal parallel execution using the fluent builder API.
"""

from sdax.sdax_core import AsyncTask, TaskFunction, AsyncDagTaskProcessor


# Helper for creating test tasks
async def dummy_func(ctx):
    """Dummy function for testing."""
    pass


def make_task(
    name: str,
    has_pre: bool = False,
    has_exec: bool = False,
    has_post: bool = False,
    is_node: bool = False
) -> AsyncTask:
    """Helper to create tasks for examples.
    
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


def example_e_commerce_workflow():
    """Analyze an e-commerce order processing workflow."""
    print("=" * 70)
    print("E-Commerce Order Processing Workflow")
    print("=" * 70)
    print()
    
    # Build processor using fluent builder pattern
    processor = (
        AsyncDagTaskProcessor.builder()
        # Phase 1: Validation (independent)
        .add_task(make_task('check_inventory', has_pre=True, has_post=True), depends_on=())
        .add_task(make_task('validate_payment', has_pre=True, has_post=True), depends_on=())
        .add_task(make_task('verify_address', has_pre=True), depends_on=())
        
        # Milestone node: all validations complete
        .add_task(make_task('order_validated', is_node=True), depends_on=('check_inventory', 'validate_payment', 'verify_address'))
        
        # Phase 2: Commitment (after validation)
        .add_task(make_task('reserve_inventory', has_pre=True, has_post=True), depends_on=('order_validated',))
        .add_task(make_task('charge_payment', has_pre=True, has_post=True), depends_on=('order_validated',))
        
        # Milestone node: order committed
        .add_task(make_task('order_committed', is_node=True), depends_on=('reserve_inventory', 'charge_payment'))
        
        # Phase 3: Fulfillment (after commitment)
        .add_task(make_task('create_shipment', has_pre=True), depends_on=('order_committed',))
        .add_task(make_task('send_confirmation', has_pre=True), depends_on=('order_committed',))
        .add_task(make_task('update_analytics', has_pre=True), depends_on=('order_committed',))
        .build()
    )
    
    # Access the analysis from the processor
    analysis = processor.analysis
    
    print(analysis)
    print()
    
    print("Key Insights:")
    print("-" * 70)
    print("1. Milestone nodes (order_validated, order_committed) have zero overhead")
    print("2. Three validation tasks run in parallel (wave 0)")
    print("3. reserve_inventory and charge_payment run in parallel (wave 1)")
    print("4. All fulfillment tasks run in parallel (wave 2)")
    print("5. Cleanup runs in reverse order, ensuring proper rollback")
    print()


def example_with_independent_chains():
    """Example showing how independent chains exploit parallelism."""
    print("=" * 70)
    print("Independent Processing Chains")
    print("=" * 70)
    print()
    
    # Build processor using fluent builder pattern
    processor = (
        AsyncDagTaskProcessor.builder()
        # Chain 1: User processing
        .add_task(make_task('fetch_user', has_pre=True), depends_on=())
        .add_task(make_task('enrich_user', has_pre=True), depends_on=('fetch_user',))
        .add_task(make_task('cache_user', has_pre=True), depends_on=('enrich_user',))
        
        # Chain 2: Order processing (completely independent)
        .add_task(make_task('fetch_orders', has_pre=True), depends_on=())
        .add_task(make_task('aggregate_orders', has_pre=True), depends_on=('fetch_orders',))
        .add_task(make_task('cache_orders', has_pre=True), depends_on=('aggregate_orders',))
        
        # Final merge
        .add_task(make_task('generate_report', has_pre=True), depends_on=('cache_user', 'cache_orders'))
        .build()
    )
    
    # Access the analysis from the processor
    analysis = processor.analysis
    
    print(analysis)
    print()
    
    print("Key Insights:")
    print("-" * 70)
    print("Wave 0: fetch_user and fetch_orders (parallel)")
    print("Wave 1: enrich_user and aggregate_orders (parallel)")
    print("Wave 2: cache_user and cache_orders (parallel)")
    print("Wave 3: generate_report (waits for both chains)")
    print()
    print("Without DAG mode, this would require 7 sequential steps!")
    print("With DAG mode, it's only 4 waves with maximum parallelism.")
    print()


if __name__ == '__main__':
    example_e_commerce_workflow()
    example_with_independent_chains()

