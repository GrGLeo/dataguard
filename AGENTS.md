# Agent Guidelines for DataGuard

## Build & Test Commands
- **Build**: `cargo build` (development) or `cargo build --release` (optimized)
- **Python binding**: `maturin develop` (installs locally for testing)
- **Rust tests**: `cargo test` (all tests) or `cargo test test_name` (single test)
- **Python tests**: `pytest tests/` (all) or `pytest tests/test_file.py::test_name` (single test)
- **Lint Rust**: `cargo clippy -- -D warnings` (enforced in CI)
- **Format Rust**: `cargo fmt --check` (check) or `cargo fmt` (apply)
- **Lint Python**: `ruff check` (check) or `ruff format` (format)
- **Benchmarks**: `cargo bench`

## Code Style
- **Rust**: Use `thiserror` for errors, derive `Debug`/`PartialEq` for testable types, allow `clippy::too_many_arguments`
- **Error handling**: Custom `RuleError` enum with descriptive variants, convert to `PyErr` for Python bindings
- **Imports**: Group stdlib, external crates, then internal modules; use `#[cfg(feature = "python")]` for PyO3 code
- **Naming**: snake_case for functions/variables, PascalCase for types/enums, builder pattern for column constructors
- **Types**: Explicit types for public APIs, use Arrow types for columnar data, `Option<T>` for nullable parameters
- **Python API**: Use `#[pyclass]` and `#[pymethods]`, builder methods return `PyResult<Self>` for chaining
- **Tests**: Use `tempfile` for test data, assert exact error counts, test both valid and invalid cases
