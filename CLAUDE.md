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

This is a Rust library that integrates ClickHouse with Apache DataFusion, allowing `ClickHouse` tables to be queried through DataFusion's SQL engine.

### Core Components

1. **ClickHouseBuilder** (src/builders.rs) - Main entry point for configuring connections
   - Creates catalog providers and table providers
   - Handles connection pooling via clickhouse-arrow

2. **Custom Context Support** (src/context.rs)
   - `ClickHouseSessionContext` extends DataFusion's SessionContext
   - Custom QueryPlanner prevents DataFusion from optimizing away `ClickHouse` functions
   - Enables `ClickHouse` UDF pushdown through special `clickhouse()` wrapper function

3. **Table Provider** (src/table_provider.rs)
   - Implements DataFusion's TableProvider trait
   - Handles schema inference and filter pushdown
   - Manages execution of queries against ClickHouse

4. **Federation** (src/federation.rs) - Optional feature for cross-database queries
   - Allows joining `ClickHouse` tables with other `DataFusion` sources
   - Automatic query pushdown optimization
   - Enabled by default in Cargo.toml

5. **Column Lineage System** (src/column_lineage.rs)
   - Tracks column dependencies through complex query plans
   - Resolves column references to their table sources
   - Supports JOIN aliases, subqueries, and computed columns

6. **ClickHouse Function Pushdown** (src/clickhouse_function_pushdown.rs)
   - Pushes `clickhouse()` functions down to appropriate levels
   - Uses column lineage for intelligent blocking decisions
   - Handles federation vs non-federation scenarios

### Key Design Decisions

- Uses Arrow format for efficient data transfer between `ClickHouse` and `DataFusion`
- Connection pooling for performance (via clickhouse-arrow)
- Custom context required for full `ClickHouse` function support
- String encoding defaults to UTF-8 for DataFusion compatibility
- Column lineage tracking for sophisticated function pushdown optimization

### Testing Strategy

Tests use testcontainers to spin up isolated `ClickHouse` instances. Test modules:
- `tests/e2e.rs` - Basic functionality tests
- `tests/common/` - Shared test utilities and `ClickHouse` container setup
- Integration tests demonstrate real usage patterns

When writing tests, use the `init_clickhouse_context_*` helpers from tests/common/mod.rs.

## `ClickHouse` Function Pushdown System

### Current Status (as of 2025-07-19)
- ✅ Clean analyzer implementation completed
- ✅ Column lineage system with ResolvedSource tracking
- ✅ Proper TreeNode API usage and column dependency resolution
- 🔄 Ready for testing and refinement

### Key Implementation Details:
- **Single-phase design**: Simplified analyzer using column lineage for decision making
- **Proper column comparison**: Uses `resolve_to_source()` and `disjoin_tables()` instead of naive Column equality
- **Efficient caching**: Tracks function resolved sources to avoid recomputation
- **File**: `src/clickhouse_function_pushdown.rs`

### Technical Architecture:
1. **Column Lineage Building**: Creates dependency map for all columns in the plan
2. **Function Discovery**: Identifies `clickhouse()` functions during traversal
3. **Intelligent Blocking**: Uses resolved sources to determine if filters/aggregates block pushdown
4. **Smart Placement**: Pushes functions as low as possible while respecting blocking operations

### Coding Guidelines Applied:
- No ad-hoc functions - logic kept inline where analyzable
- Functional style leveraging Rust's capabilities
- Direct column lineage integration instead of complex pre-computation
- TreeNode API used correctly with proper visitor patterns

### Next Steps:
1. **Test comprehensive scenarios**: Single table, multi-table, edge cases
2. **Implement federation-aware wrapping**: Extension nodes vs Projections
3. **Add join bypass logic**: Use `disjoin_tables()` for compound function analysis
4. **Performance optimization**: Profile and optimize for large plans
