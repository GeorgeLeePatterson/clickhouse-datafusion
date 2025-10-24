# clickhouse-datafusion Examples

This directory contains examples demonstrating various features of clickhouse-datafusion.

## Running Examples

All examples require the `test-utils` feature to set up test containers:

```bash
# Run any example (use --release for better performance)
cargo run --example 00_summary --features test-utils --release

# Federation examples need the federation feature
cargo run --example 05_federation --features "test-utils federation" --release

# With debug logging
RUST_LOG=debug cargo run --example 01_basic_filtering --features test-utils --release

# Run all examples
bash examples/run_all.sh
```

## Available Examples

### Core Examples

#### `00_summary.rs` - Comprehensive Overview
A comprehensive example showcasing all major features:
- Basic setup and connection
- Schema management (CREATE DATABASE, CREATE TABLE)
- Data insertion using RecordBatch
- ClickHouse UDFs (string, array, lambda functions)
- Federation with in-memory DataFusion tables
- Window functions and advanced analytics

**Start here** to understand the full capabilities of the library.

#### `01_basic_filtering.rs` - Simple Queries
Demonstrates basic query patterns:
- WHERE clauses with filter pushdown
- Column projection
- LIMIT and ORDER BY

#### `02_aggregations.rs` - Aggregation Functions
Shows aggregation capabilities:
- COUNT, SUM, AVG, MIN, MAX
- GROUP BY operations
- HAVING clauses

#### `03_joins.rs` - Join Operations
Demonstrates various join types:
- INNER JOIN
- LEFT/RIGHT JOIN
- FULL OUTER JOIN
- Self joins

#### `04_window_functions.rs` - Window Analytics
Advanced window function examples:
- ROW_NUMBER, RANK, DENSE_RANK
- Partitioning and ordering
- Moving averages and cumulative sums

### Federation Examples

#### `05_federation.rs` - Cross-Database Queries
**Requires:** `--features "test-utils federation"`

Demonstrates querying across data sources:
- Joining ClickHouse tables with DataFusion in-memory tables
- Automatic query pushdown optimization
- Mixing different data sources in a single query

#### `08_parquet_federation.rs` - Parquet + ClickHouse
**Requires:** `--features "test-utils federation"`

Advanced federation with Parquet files:
- Reading from Parquet files
- Joining Parquet data with ClickHouse tables
- Cross-format analytics

### Data Management Examples

#### `06_drop_tables.rs` - DROP TABLE Support
Demonstrates table lifecycle management:
- Creating temporary tables
- Dropping tables programmatically
- Verifying table existence

#### `07_parquet_to_clickhouse.rs` - Parquet to ClickHouse
ETL pipeline example:
- Reading Parquet files
- Creating ClickHouse tables from Parquet schema
- Bulk loading data into ClickHouse

### Performance Examples

#### `09_write_concurrency.rs` - Parallel Writes
**Showcases the parallel writes feature:**
- Configurable write concurrency
- Metrics tracking (rows/sec, MB/sec)
- Performance comparison across different concurrency levels
- Demonstrates optimal configuration for high-throughput inserts

#### `10_large_scale.rs` - Performance Benchmarking
**Advanced benchmarking tool** for testing write performance:
- Tests various row sizes, worker counts, and batch sizes
- Supports different compression methods (LZ4, ZSTD, None)
- TMPFS support for zero-disk-I/O testing
- Generates comprehensive performance reports
- Derives optimal configuration based on data characteristics

**Environment variables:**
- `ROW_COUNTS`: e.g., `5MM,10MM` (comma-separated)
- `WORKERS`: e.g., `4,8,16` (comma-separated)
- `BATCH_SIZES`: e.g., `100K,200K,500K` (comma-separated)
- `ITERS`: Number of iterations per test (default: 3)
- `COMP`: Compression method - `NONE`, `LZ4`, or `ZSTD` (default: LZ4)
- `USE_TMPFS`: `true` to enable zero-disk-I/O testing
- `INT32`, `UINT64`, `FLOAT64`, `UTF8`: Column counts per type

**Example usage:**
```bash
# Test 5M rows with different worker counts and compression
COMP=ZSTD INT32=5 UINT64=3 FLOAT64=2 UTF8=5 \
  ROW_COUNTS=5MM WORKERS=8,16 BATCH_SIZES=100K,200K \
  ITERS=3 USE_TMPFS=true \
  cargo run --example 10_large_scale --features test-utils --release
```

### Comprehensive Example

#### `99_Agent_All_in_one.rs` - Everything Combined
The kitchen sink example demonstrating all features in one place:
- All query types
- All ClickHouse UDF capabilities
- Federation scenarios
- Performance patterns
- Error handling

## Prerequisites

- Docker (required for testcontainers to spin up ClickHouse)
- Rust 1.75+ with Cargo
- For TMPFS testing: Docker with tmpfs support

## Features

Examples automatically enable required features:
- `test-utils`: Enables testcontainer support for ClickHouse
- `federation`: Enables cross-database query capabilities (required for examples 05, 08)

## Output

Examples produce formatted output showing:
1. Container setup progress
2. Table creation and data insertion confirmations
3. Query results formatted as ASCII tables (via comfy-table)
4. Performance metrics (where applicable)
5. Summary of demonstrated features

## Development

To add new examples:
1. Create a new `.rs` file following the numbering pattern
2. Use the testcontainer setup pattern from existing examples
3. Focus on specific features or use cases
4. Add comprehensive documentation explaining what the example demonstrates
5. Update this README with the new example description
6. Add the example to `run_all.sh` if applicable
