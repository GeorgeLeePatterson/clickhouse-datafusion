# clickhouse-datafusion Examples

This directory contains examples demonstrating various features of clickhouse-datafusion.

## Running Examples

All examples require the `test-utils` feature to set up test containers:

```bash
# Run the basic example
cargo run --example basic --features test-utils

# With debug logging
RUST_LOG=debug cargo run --example basic --features test-utils
```

## Available Examples

### `basic.rs`
A comprehensive example showcasing:

- **Basic Setup**: Creating ClickHouse context and connecting to ClickHouse
- **Schema Management**: Creating databases, tables, and inserting data
- **Basic Queries**: Simple SELECT queries against ClickHouse tables
- **ClickHouse UDFs**: Using ClickHouse functions in DataFusion SQL
  - String functions (`upper`, `length`)
  - Array functions (`arrayJoin`)
  - Lambda functions (`arrayMap`, `arrayFilter`)
- **Federation**: Joining ClickHouse tables with in-memory DataFusion tables
- **Complex Analytics**: Window functions combined with ClickHouse UDFs

**Key Features Demonstrated:**
- ✅ ClickHouse container setup (automatic via testcontainers)
- ✅ Database and table creation with Arrow schemas
- ✅ Data insertion using RecordBatch
- ✅ ClickHouse UDF integration
- ✅ Cross-database federation
- ✅ Advanced SQL analytics

## Prerequisites

- Docker (required for testcontainers to spin up ClickHouse)
- Rust 1.75+ with Cargo

## Features

Examples automatically enable the required features:
- `test-utils`: Enables testcontainer support for ClickHouse
- `federation`: Enables cross-database query capabilities

## Output

The basic example produces output showing:
1. Container setup logs
2. Table creation and data insertion confirmations
3. Query results formatted as ASCII tables
4. Summary of demonstrated features

## Development

To add new examples:
1. Create a new `.rs` file in this directory
2. Use the same testcontainer setup pattern as `basic.rs`
3. Focus on specific features or use cases
4. Add documentation explaining what the example demonstrates
5. Update this README with the new example
