# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

clickhouse-datafusion is a Rust library that integrates ClickHouse with Apache DataFusion, enabling ClickHouse tables to be queried through DataFusion's SQL engine. It builds on clickhouse-arrow for high-performance data access and provides advanced features like federation and ClickHouse UDF pushdown.

## Build and Test Commands

This project uses `just` as a task runner. Common commands:

- `just test` - Run all tests with different feature combinations (unit + integration)
- `just test-unit` - Run only unit tests with all feature combinations
- `just test-one <test_name>` - Run a specific test across all feature combinations
- `just test-e2e [test_name]` - Run end-to-end tests without federation
- `just test-federation [test_name]` - Run federation-specific tests
- `just test-integration [test_name]` - Run both e2e and federation tests
- `just coverage` - Generate HTML test coverage report and open in browser
- `just coverage-lcov` - Generate lcov.info coverage report for CI
- `just checks` - Run same checks as CI (format, clippy, tests)

Environment variables for debugging:
- `RUST_LOG=debug` - Enable debug logging
- `DISABLE_CLEANUP=true` - Keep test containers running after tests
- `DISABLE_CLEANUP_ON_ERROR=true` - Keep containers only on test failure

## Architecture Overview

clickhouse-datafusion bridges ClickHouse and DataFusion, allowing complex SQL queries that span ClickHouse tables and other DataFusion sources.

### Core Components

1. **ClickHouseBuilder** (src/builders.rs) - Main entry point for configuration
   - Creates catalog providers and table providers
   - Handles connection pooling via clickhouse-arrow
   - Configures Arrow options and schema coercion
   - Provides `build_catalog()` to create ClickHouseCatalogBuilder

2. **ClickHouseSessionContext** (src/context.rs) - Enhanced DataFusion context
   - Extends DataFusion's SessionContext with ClickHouse-specific features
   - Required for ClickHouse UDF pushdown functionality
   - Uses custom ClickHouseQueryPlanner to prevent UDF optimization
   - Registers ClickHouse-specific UDFs and analyzer rules

3. **Table Providers** (src/providers/) - DataFusion integration layer
   - `ClickHouseTableProvider` - Implements DataFusion's TableProvider trait
   - `ClickHouseTableProviderFactory` - Creates table providers from SQL DDL
   - `ClickHouseCatalogProvider` - Manages ClickHouse schemas in DataFusion
   - Handles schema inference, filter pushdown, and execution

4. **Federation** (src/federation.rs) - Cross-database query support
   - Optional feature for joining ClickHouse with other DataFusion sources
   - Uses datafusion-federation for automatic query pushdown optimization
   - FederatedContext trait to enable federation on SessionContext

5. **UDF System** (src/udfs/) - ClickHouse function integration
   - `clickhouse()` UDF for direct ClickHouse function calls
   - `clickhouse_apply()` for lambda functions with parameter binding
   - `clickhouse_eval()` for string-based function evaluation (federation only)
   - Schema coercion support for type flexibility

6. **Function Pushdown Analyzer** (src/analyzer/) - Advanced optimization
   - ClickHouseFunctionPushdown analyzer rule for intelligent UDF placement
   - Column lineage tracking to determine safe pushdown locations
   - Handles complex scenarios with JOINs, subqueries, and aggregations

### Key Features

- **High Performance**: Built on clickhouse-arrow for optimal data transfer
- **Arrow Integration**: Native Apache Arrow support for efficient data processing
- **Connection Pooling**: bb8-based connection pooling for scalability
- **Federation Support**: Join ClickHouse tables with other data sources
- **ClickHouse UDFs**: Direct access to ClickHouse functions in SQL queries
- **Schema Flexibility**: Optional schema coercion for type compatibility
- **Advanced Analytics**: Support for window functions, CTEs, and complex queries

### Testing Strategy

Tests use testcontainers to spin up isolated ClickHouse instances:
- **Unit Tests**: Test individual components with mocked connections
- **Integration Tests**: Full end-to-end testing with real ClickHouse containers
- **Federation Tests**: Cross-database query testing
- **UDF Tests**: Comprehensive ClickHouse function pushdown scenarios

Test helpers in `tests/common/mod.rs` provide:
- Container setup and teardown
- Test data insertion
- Context configuration utilities

### Usage Patterns

#### Basic Setup
```rust
use clickhouse_datafusion::{ClickHouseBuilder, ClickHouseSessionContext};
use datafusion::prelude::SessionContext;

// Create basic DataFusion context
let ctx = SessionContext::new();

// Enable federation if needed
#[cfg(feature = "federation")]
let ctx = ctx.federate();

// Enable ClickHouse UDF support
let ctx = ClickHouseSessionContext::from(ctx);

// Build ClickHouse integration
let builder = ClickHouseBuilder::new("clickhouse://localhost:9000")
    .configure_arrow_options(|opts| opts.with_strings_as_strings(true))
    .build_catalog(&ctx, Some("clickhouse"))
    .await?;
```

#### ClickHouse UDFs
```rust
// Direct function calls
"SELECT clickhouse(exp(id), 'Float64') FROM clickhouse.db.table"

// Lambda functions
"SELECT clickhouse(arrayMap($x, upper($x), names), 'List(Utf8)') FROM table"

// String evaluation (federation only)
"SELECT clickhouse_eval('exp(id)', 'Float64') FROM clickhouse.db.table"
```

### Development Notes

- Feature flags: `federation` (enabled by default), `cloud`, `test-utils`, `mocks`
- Requires Rust 2024 edition
- Uses DataFusion 49+ for latest query planning features
- Extensive clippy configuration in clippy.toml
- Test containers automatically managed via testcontainers-rs
