# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.1] - 2025-10-27

### Added
- **Task Creation Helper**: Added `task()` function for creating AsyncTask instances
  - Simplified syntax for creating tasks with name and phase functions
  - More ergonomic than direct AsyncTask instantiation
  - Validates that at least one phase function is provided
  - **Usage Example**:
    ```python
    from sdax import task, task_func
    
    # Simple task creation
    my_task = task("MyTask", execute=task_func(my_function))
    
    # Task with multiple phases
    my_task = task("SetupAndCleanup", 
                   pre_execute=task_func(setup),
                   post_execute=task_func(cleanup))
    ```
- **Join Node Helper**: Added `join()` function for creating empty graph synchronization points
  - Creates empty graph nodes that act as synchronization points in task dependencies
  - More idiomatic than creating empty AsyncTask instances
  - Useful for coordinating multiple parallel tasks before downstream processing
  - **Usage Example**:
    ```python
    from sdax import join
    
    # Create a synchronization point that waits for TaskA and TaskB
    .add_task(join("SyncPoint"), depends_on=("TaskA", "TaskB"))
    
    # Multiple tasks can depend on the join node
    .add_task(AsyncTask("Process"), depends_on=("SyncPoint",))
    .add_task(AsyncTask("Notify"), depends_on=("SyncPoint",))
    ```


### Added
- **Task Creation Helper**: Added `task()` function for creating AsyncTask instances
  - Simplified syntax for creating tasks with name and phase functions
  - More ergonomic than direct AsyncTask instantiation
  - Validates that at least one phase function is provided
  - **Usage Example**:
    ```python
    from sdax import task, task_func
    
    # Simple task creation
    my_task = task("MyTask", execute=task_func(my_function))
    
    # Task with multiple phases
    my_task = task("SetupAndCleanup", 
                   pre_execute=task_func(setup),
                   post_execute=task_func(cleanup))
    ```
- **Join Node Helper**: Added `join()` function for creating empty graph synchronization points
  - Creates empty graph nodes that act as synchronization points in task dependencies
  - More idiomatic than creating empty AsyncTask instances
  - Useful for coordinating multiple parallel tasks before downstream processing
  - **Usage Example**:
    ```python
    from sdax import join
    
    # Create a synchronization point that waits for TaskA and TaskB
    .add_task(join("SyncPoint"), depends_on=("TaskA", "TaskB"))
    
    # Multiple tasks can depend on the join node
    .add_task(AsyncTask("Process"), depends_on=("SyncPoint",))
    .add_task(AsyncTask("Notify"), depends_on=("SyncPoint",))
    ```

## [0.7.0] - 2025-10-26

### Added
- **Generic Key Type Support**: Added support for custom key types in task names
  - `K` type parameter with `Hashable | str` bound for flexible key types
  - `LevelKey` type alias for level-based task naming: `Tuple[int, Literal["below"]] | Tuple[int, Literal["above"]] | str`
  - Backward compatible with existing string-based task names
- **Task Function Helper Utilities**: Added convenience functions for creating TaskFunction instances
  - `task_func()`: Creates standard TaskFunction with configurable timeout, retries, and exception handling
  - `task_group_func()`: Creates TaskFunction that receives SdaxTaskGroup as second argument
  - `task_sync_func()`: Creates TaskFunction that wraps synchronous functions with async compatibility
  - All helpers provide sensible defaults for retry behavior and exception handling
  - Simplified TaskFunction creation with keyword-only parameters for better API ergonomics
  - **Usage Examples**:
    ```python
    # Simple usage
    task = task_func(my_async_function)
    
    # With custom configuration
    task = task_func(my_function, timeout=30.0, retries=3, 
                     retryable_exceptions=(ValueError, RuntimeError))
    
    # Task group variant
    tg_task = task_group_func(my_group_function, retries=2)
    
    # Synchronous function wrapper
    sync_task = task_sync_func(my_sync_function, retries=1)
    ```
  - **Default Configuration**:
    - `timeout=None` (infinite timeout)
    - `retries=0` (no retries)
    - `initial_delay=1.0` seconds
    - `backoff_factor=2.0` (exponential backoff)
    - `retryable_exceptions=(TimeoutError, ConnectionError, RetryableException)`

### Changed
- **Enhanced Level Adapter**: Improved level-based task naming system
  - Level nodes now use structured keys `(level, "below")` and `(level, "above")` instead of string concatenation
  - More type-safe and efficient level node management
  - Better integration with generic key type system

### Removed
- **Dependency Cleanup**: Removed unused `frozendict` dependency
  - Simplified project dependencies
  - Reduced external dependency footprint

### Improved
- **Developer Experience**: Enhanced API ergonomics with helper functions
  - Reduced boilerplate code for TaskFunction creation
  - Keyword-only parameters prevent common configuration errors
  - Clear separation between standard and task-group variants
- **Code Maintainability**: Simplified internal phase management
  - Replaced complex dictionary mapping with direct method dispatch
  - More explicit and easier to understand phase creation logic
  - Reduced cognitive load for developers working with the codebase
- **Type Safety**: Enhanced generic type support
  - Better type inference for custom key types
  - Structured level keys provide compile-time safety
  - Improved IDE support and autocompletion


### Added
- **Generic Key Type Support**: Added support for custom key types in task names
  - `K` type parameter with `Hashable | str` bound for flexible key types
  - `LevelKey` type alias for level-based task naming: `Tuple[int, Literal["below"]] | Tuple[int, Literal["above"]] | str`
  - Backward compatible with existing string-based task names
- **Task Function Helper Utilities**: Added convenience functions for creating TaskFunction instances
  - `task_func()`: Creates standard TaskFunction with configurable timeout, retries, and exception handling
  - `task_group_func()`: Creates TaskFunction that receives SdaxTaskGroup as second argument
  - `task_sync_func()`: Creates TaskFunction that wraps synchronous functions with async compatibility
  - All helpers provide sensible defaults for retry behavior and exception handling
  - Simplified TaskFunction creation with keyword-only parameters for better API ergonomics
  - **Usage Examples**:
    ```python
    # Simple usage
    task = task_func(my_async_function)
    
    # With custom configuration
    task = task_func(my_function, timeout=30.0, retries=3, 
                     retryable_exceptions=(ValueError, RuntimeError))
    
    # Task group variant
    tg_task = task_group_func(my_group_function, retries=2)
    
    # Synchronous function wrapper
    sync_task = task_sync_func(my_sync_function, retries=1)
    ```
  - **Default Configuration**:
    - `timeout=None` (infinite timeout)
    - `retries=0` (no retries)
    - `initial_delay=1.0` seconds
    - `backoff_factor=2.0` (exponential backoff)
    - `retryable_exceptions=(TimeoutError, ConnectionError, RetryableException)`

### Changed
- **Enhanced Level Adapter**: Improved level-based task naming system
  - Level nodes now use structured keys `(level, "below")` and `(level, "above")` instead of string concatenation
  - More type-safe and efficient level node management
  - Better integration with generic key type system

### Removed
- **Dependency Cleanup**: Removed unused `frozendict` dependency
  - Simplified project dependencies
  - Reduced external dependency footprint

### Improved
- **Developer Experience**: Enhanced API ergonomics with helper functions
  - Reduced boilerplate code for TaskFunction creation
  - Keyword-only parameters prevent common configuration errors
  - Clear separation between standard and task-group variants
- **Code Maintainability**: Simplified internal phase management
  - Replaced complex dictionary mapping with direct method dispatch
  - More explicit and easier to understand phase creation logic
  - Reduced cognitive load for developers working with the codebase
- **Type Safety**: Enhanced generic type support
  - Better type inference for custom key types
  - Structured level keys provide compile-time safety
  - Improved IDE support and autocompletion

## [0.6.1] - 2025-10-25

### Added
- **Phase-by-Phase Execution API**: New advanced execution model for fine-grained control over task phases
  - `AsyncPhaseRunner[T]` class for step-by-step execution control
  - `PhaseId` enum with `PRE_EXEC`, `EXECUTE`, and `POST_EXEC` identifiers
  - `processor.open(ctx)` async context manager for phased execution
  - `runner.run_next()` method to advance through phases incrementally
  - `runner.next_phase_id()` and `runner.last_phase_id()` for phase inspection
  - `runner.has_failures()`, `runner.failures()`, and `runner.raise_failures()` for error handling
- **Enhanced Error Handling**:
  - `SdaxExecutionError` exception class for structured error reporting
  - `flatten_exceptions()` utility function for exception processing
- **Improved Documentation**: Comprehensive examples and use cases for phased execution

### Changed
- **Default Timeout Behavior**: Changed default timeout/deadline from a fixed value to `None` (infinite timeout)
  - Tasks now run indefinitely by default unless explicitly configured with a timeout
  - This provides more predictable behavior for long-running operations
- **Version Management**: Updated version handling to accommodate documentation changes
- Enhanced `AsyncTaskProcessor` with new `open()` method for phased execution
- Updated public API exports to include new error handling utilities

### Migration Guide
- **Timeout Behavior**: If your code was relying on the previous default timeout behavior, you may need to explicitly set timeouts on `TaskFunction` instances
  - Old behavior: Tasks had a default timeout
  - New behavior: Tasks run indefinitely unless `timeout` parameter is specified
  - Example: `TaskFunction(my_func, timeout=30.0)` for 30-second timeout
- **TaskFunction Creation**: Consider using the new helper functions for cleaner code
  - **Before**: `TaskFunction(function=my_func, timeout=30.0, retries=2, initial_delay=1.0, backoff_factor=2.0, retryable_exceptions=(TimeoutError, ConnectionError, Exception), has_task_group_argument=False)`
  - **After**: `task_func(my_func, timeout=30.0, retries=2)`
  - **Task Group**: `task_group_func(my_func, timeout=30.0, retries=2)` instead of manually setting `has_task_group_argument=True`
  - **Sync Functions**: `task_sync_func(my_sync_func, retries=2)` instead of manually wrapping with async wrapper
- **Generic Key Types**: Existing string-based task names continue to work unchanged
  - New generic key support is opt-in and backward compatible
  - Level adapter now uses structured tuple keys internally but maintains string compatibility
- New phased execution API is completely optional
- All other existing APIs remain fully compatible

## [0.5.2] - 2025-10-24

### Added
- Comprehensive test suite with 62 tests covering all major functionality
- Performance benchmarks and Monte Carlo stress testing
- Extensive documentation with examples and use cases
- GitHub Actions workflow for automated PyPI publishing

### Features
- **Graph-based scheduler (DAG)**: Primary execution model with task dependency graphs
- **Elevator adapter (levels)**: Level-based API that builds graphs under the hood
- **Structured Lifecycle**: Rigid `pre_execute` -> `execute` -> `post_execute` phases
- **Guaranteed Cleanup**: `post_execute` runs for any task whose `pre_execute` started
- **Immutable Builder Pattern**: Fluent APIs producing immutable, reusable instances
- **Concurrent Execution Safe**: Multiple concurrent runs are fully isolated
- **Declarative & Flexible**: Task functions with optional timeouts/retries
- **Lightweight**: Zero external dependencies, minimal overhead

### Performance
- High throughput: ~137k tasks/sec
- Minimal overhead: ~7μs per task
- Optimized for I/O-bound workflows

## [0.5.1] - 2025-10-22

### Added
- Initial public release
- Core DAG and level-based execution models
- Task retry and timeout mechanisms
- Resource cleanup guarantees
- Type-safe context passing

## [0.5.0] - 2025-10-22

### Added
- Initial development release
- Basic task orchestration framework
- Async/await support
- Task dependency management

---

## Version History Summary

- **0.6.0**: Added phased execution API for advanced use cases
- **0.5.2**: Stable release with comprehensive testing and documentation
- **0.5.1**: First public release with core features
- **0.5.0**: Initial development release

## Contributing

When adding new features or making changes, please update this changelog following the format above:
- Group changes by type: Added, Changed, Deprecated, Removed, Fixed, Security
- Use present tense ("Add feature" not "Added feature")
- Include migration notes for breaking changes
- Reference issues and pull requests when applicable
