# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

This project uses `just` as a task runner. Common commands:

- `just test` - Run all tests with test-utils feature
- `just test-one <test_name>` - Run a specific test (e.g., `just test-one test_simple_query`)
- `just test-e2e [test_name]` - Run end-to-end tests without federation
- `just test-federation [test_name]` - Run federation-specific tests
- `just test-integration [test_name]` - Run both e2e and federation tests
- `just coverage` - Generate test coverage report using cargo-tarpaulin

Environment variables for debugging:
- `RUST_LOG=debug` - Enable debug logging
- `DISABLE_CLEANUP=true` - Keep test containers running after tests
- `DISABLE_CLEANUP_ON_ERROR=true` - Keep containers only on test failure

## Architecture Overview

This is a Rust library that integrates ClickHouse with Apache DataFusion, allowing ClickHouse tables to be queried through DataFusion's SQL engine.

### Core Components

1. **ClickHouseBuilder** (src/builders.rs) - Main entry point for configuring connections
   - Creates catalog providers and table providers
   - Handles connection pooling via clickhouse-arrow

2. **Custom Context Support** (src/context.rs)
   - `ClickHouseSessionContext` extends DataFusion's SessionContext
   - Custom QueryPlanner prevents DataFusion from optimizing away ClickHouse functions
   - Enables ClickHouse UDF pushdown through special `clickhouse()` wrapper function

3. **Table Provider** (src/table_provider.rs)
   - Implements DataFusion's TableProvider trait
   - Handles schema inference and filter pushdown
   - Manages execution of queries against ClickHouse

4. **Federation** (src/federation.rs) - Optional feature for cross-database queries
   - Allows joining ClickHouse tables with other DataFusion sources
   - Automatic query pushdown optimization
   - Enabled by default in Cargo.toml

5. **UDF System** (src/udfs/)
   - Special handling for ClickHouse-specific functions
   - `clickhouse()` wrapper prevents DataFusion optimization
   - Analyzer rules for function pushdown

### Key Design Decisions

- Uses Arrow format for efficient data transfer between ClickHouse and DataFusion
- Connection pooling for performance (via clickhouse-arrow)
- Custom context required for full ClickHouse function support
- String encoding defaults to UTF-8 for DataFusion compatibility
- Currently uses forked datafusion-federation pending upstream PR

### Testing Strategy

Tests use testcontainers to spin up isolated ClickHouse instances. Test modules:
- `tests/e2e.rs` - Basic functionality tests
- `tests/common/` - Shared test utilities and ClickHouse container setup
- Integration tests demonstrate real usage patterns

When writing tests, use the `init_clickhouse_context_*` helpers from tests/common/mod.rs.

## ClickHouse Function Pushdown Analyzer

### Current Status (as of 2025-07-09)
- ✅ Basic analyzer test passes (non-federation)
- ✅ Federation test passes  
- ✅ Proper TreeNodeRecursion::Jump short-circuiting implemented
- ✅ Infinite recursion bug fixed
- 🔄 Ready to test complex join queries

### Key Implementation Details:
- Uses two-phase design: ClickHouseFunctionAnalyzer + ClickHousePlanTransformer
- Proper idempotency with federation-aware guards
- File: `src/udfs/pushdown_analyzer.rs`
- Test commands: `just test-analyzer` and `just test-analyzer-federation`

### Technical Architecture:
1. **Phase 1 (Analysis)**: ClickHouseFunctionAnalyzer discovers clickhouse() functions and maps them to target tables
2. **Phase 2 (Transformation)**: ClickHousePlanTransformer transforms TableScan nodes and replaces function calls with column references
3. **Federation Support**: Compile-time feature flags (`#[cfg(feature = "federation")]`) for federation vs non-federation paths
4. **Efficiency**: Uses `TreeNodeRecursion::Jump` to short-circuit unnecessary tree traversals

### Key Fixes Applied:
- Fixed infinite recursion by removing recursive `transform_plan` calls
- Added proper `Transformed::new(node, false, TreeNodeRecursion::Jump)` for short-circuiting
- Implemented federation-aware idempotency guards using `ch_func_*` pattern detection
- Made analyzer follow DataFusion patterns from push_down_filter.rs

### Next Steps:
1. Uncomment complex join test in tests/analyzer.rs
2. Test and fix any issues with multi-table joins
3. Enable all commented tests