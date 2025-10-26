# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.1] - 2024-12-19

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
- New phased execution API is completely optional
- All other existing APIs remain fully compatible

## [0.5.2] - 2024-12-19

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

## [0.5.1] - 2024-12-19

### Added
- Initial public release
- Core DAG and level-based execution models
- Task retry and timeout mechanisms
- Resource cleanup guarantees
- Type-safe context passing

## [0.5.0] - 2024-12-19

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
