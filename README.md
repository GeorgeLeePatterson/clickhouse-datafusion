# 🚇 `ClickHouse` `DataFusion` Integration

A high-performance Rust library that integrates `ClickHouse` with Apache `DataFusion`, enabling seamless querying across `ClickHouse` and other data sources.

[![Crates.io](https://img.shields.io/crates/v/clickhouse-datafusion.svg)](https://crates.io/crates/clickhouse-datafusion)
[![Documentation](https://docs.rs/clickhouse-datafusion/badge.svg)](https://docs.rs/clickhouse-datafusion)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://img.shields.io/github/actions/workflow/status/GeorgeLeePatterson/clickhouse-datafusion/ci.yml?branch=main)](https://github.com/GeorgeLeePatterson/clickhouse-datafusion/actions)
[![Coverage](https://codecov.io/gh/GeorgeLeePatterson/clickhouse-datafusion/branch/main/graph/badge.svg)](https://codecov.io/gh/GeorgeLeePatterson/clickhouse-datafusion)

Built on [clickhouse-arrow](https://github.com/GeorgeLeePatterson/clickhouse-arrow) for optimal performance and [DataFusion](https://datafusion.apache.org/) for advanced SQL analytics.

## Why clickhouse-datafusion?

- **🚀 High Performance**: Built on [clickhouse-arrow](https://github.com/GeorgeLeePatterson/clickhouse-arrow) for optimal data transfer and Arrow format efficiency
- **⚡ Connection Pooling**: clickhouse-arrow provides connection pooling for scalability
- **🔗 Federation Support**: Join `ClickHouse` tables with other `DataFusion` sources seamlessly
- **⛓️ Two-tier Execution**: Delegate complexity to `ClickHouse` leveraging additional optimizations at the edge
- **🛠️ `ClickHouse` UDFs**: Direct access to `ClickHouse` functions in `DataFusion` SQL queries
- **📊 Advanced Analytics**: Support for window functions, CTEs, subqueries, and complex JOINs
- **🎯 Arrow Native**: Native Apache Arrow integration for zero-copy data processing
- **🔄 Schema Flexibility**: Optional schema coercion after fetching data for automatic type compatibility

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
clickhouse-datafusion = "0.1.3"
```

### Basic Usage

```rust,ignore
use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_datafusion::{ClickHouseBuilder, ClickHouseSessionContext};
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create DataFusion context with ClickHouse UDF support
    let ctx = ClickHouseSessionContext::from(SessionContext::new());

    // Build ClickHouse integration
    let clickhouse = ClickHouseBuilder::new("http://localhost:9000")
        .configure_client(|c| c.with_username("clickhouse"))
        .configure_arrow_options(|opts| opts.with_strings_as_strings(true))
        .build_catalog(&ctx, Some("clickhouse"))
        .await?;

    // Define schema for test table
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    // Set schema (database name)
    let clickhouse = clickhouse.with_schema("my_database").await?;

    // Register existing table
    let clickhouse = clickhouse.register_existing_table("my_table").await?;

    // Create a new table on the remote server and get the catalog builder back
    let clickhouse = clickhouse
        .with_new_table("new_table", ClickHouseEngine::MergeTree, schema)
        .create(ctx)
        .await?;

    // Finally build the catalog so the changes take effect (in DataFusion)
    let _catalog = clickhouse.build(&ctx).await?;

    // Query ClickHouse tables
    let df = ctx.sql("SELECT * FROM clickhouse.my_database.my_table LIMIT 10").await?;
    datafusion::arrow::util::pretty::print_batches(&df.collect().await?)?;

    Ok(())
}
```

### With Federation (Cross-DBMS Queries)

```rust,ignore
use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_datafusion::{ClickHouseBuilder, ClickHouseSessionContext};
use clickhouse_datafusion::federation::FederatedContext;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Enable federation for cross-database queries
    let ctx = SessionContext::new().federate();

    // Optionally enable the full ClickHouseSessionContext with ClickHouseQueryPlanner
    //
    // NOTE: Not required, provides a custom QueryPlanner and Analyzer with optional federation.
    let ctx = ClickHouseSessionContext::from(ctx);

    // Configure the underlying connection and initialize the ClickHouse catalog
    let clickhouse = ClickHouseBuilder::new("http://localhost:9000")
        // The default database set on the client is used initially by the returned catalog builder.
        .configure_client(|c| c.with_username("clickhouse").with_default_database("other_db"))
        .build_catalog(&ctx, Some("clickhouse")) // default catalog name
        .await?;

    // Create a table on the remote server and build the catalog (so changes are registered in
    // DataFusion).
    //
    // The 'clickhouse' catalog returned from the above builder can continue to be used to create as
    // many tables as needed. But remember to always `build` the catalog when you want to interact
    // with it via DataFusion queries, using either 'build' or 'build_schema'.
    let _clickhouse_catalog_provider = clickhouse
        // Change the default database from 'other_db' to 'analytics'
        .with_schema("analytics")
        .await?
        // Create users table on the database 'analytics'
        .with_new_table(
            // Table name
            "user_events",
            // Engine - ClickHouseEngine::default() could be used as well.
            ClickHouseEngine::MergeTree,
            // Define the schema for user_events table
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Int32, false),
                Field::new("event_count", DataType::UInt32, false),
            ])),
        )
        // Set `clickhouse_arrow::CreateOptions` for `user_events` table
        .update_create_options(|opts| opts.with_order_by(&["id".into()]))
        // Finally create the table
        .create(ctx)
        .await?
        // And then "build" the catalog, synchronizing the remote schema with DataFusion
        .build(&ctx)
        .await?;

    // Register other data sources (Parquet, CSV, etc.)
    ctx.register_parquet("local_data", "data.parquet", ParquetReadOptions::default()).await?;

    // Join across different data sources
    let df = ctx.sql("
        SELECT ch.user_id,
               ch.event_count,
               local.user_name
        FROM clickhouse.analytics.user_events ch
        JOIN local_data local
          ON ch.user_id = local.user_id
        WHERE ch.event_count > 100
    ").await?;
    datafusion::arrow::util::pretty::print_batches(&df.collect().await?)?;

    Ok(())
}
```

## `ClickHouse` Functions

Access `ClickHouse`'s powerful functions directly in `DataFusion` SQL:

### Direct Function Calls

```sql
-- Mathematical functions
SELECT clickhouse(sigmoid(price), 'Float64') as price_sigmoid FROM products;

-- String functions
SELECT clickhouse(`base64Encode`(name), 'Utf8') as b64_name FROM users;

-- Array functions
SELECT clickhouse(`arrayJoin`(tags), 'Utf8') as individual_tags FROM articles;
```

### Lambda Functions

```sql
-- Transform arrays with lambda functions
SELECT
    names,
    clickhouse(`arrayMap`($x, concat($x, '_processed'), names), 'List(Utf8)') as processed_names
FROM user_data;

-- Filter arrays
SELECT clickhouse(`arrayFilter`($x, length($x) > 3, tags), 'List(Utf8)') as long_tags
FROM content;
```

### Complex Analytics

```sql
-- Window functions with ClickHouse functions
SELECT
    user_id,
    clickhouse(exp(revenue), 'Float64') as exp_revenue,
    SUM(revenue) OVER (PARTITION BY user_id ORDER BY date) as running_total
FROM sales_data;

-- CTEs with ClickHouse functions
WITH processed_data AS (
    SELECT
        user_id,
        clickhouse(`arrayJoin`(event_types), 'Utf8') as event_type
    FROM user_events
)
SELECT event_type, COUNT(*) as event_count
FROM processed_data
GROUP BY event_type;
```

## Architecture

### Core Components

- **`ClickHouseBuilder`**: Main configuration entry point
- **`ClickHouseSessionContext`**: Enhanced `DataFusion` context with `ClickHouse` UDF support
- **Table Providers**: `DataFusion` integration layer for `ClickHouse` tables
- **Federation**: Cross-database query support via datafusion-federation
- **UDF System**: `ClickHouse` function integration with intelligent pushdown
- **Function Analyzer**: Advanced optimization for UDF placement

### Key Features

#### Connection Pooling
```rust,ignore
let clickhouse = ClickHouseBuilder::new("http://localhost:9000")
    .configure_pool(|pool| pool.max_size(10))
    .configure_client(|client| client.with_compression(CompressionMethod::LZ4))
    .build_catalog(&ctx, None)
    .await?;
```

#### Schema Coercion
```rust
let builder = ClickHouseBuilder::new("http://localhost:9000")
    .with_schema_coercion(true) // Enable automatic type coercion
    .build_catalog(&ctx, None)
    .await?;
```

#### Schema Management
```rust,ignore
// Create tables from Arrow schema
let schema = Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
]);

clickhouse.with_new_table("users", "MergeTree".into(), schema).create(&ctx).await?;
```

## Federation Support

When the `federation` feature is enabled, clickhouse-datafusion can join `ClickHouse` tables with other `DataFusion` sources:

> **Note**: The current release uses `datafusion-federation` v0.4.7 from crates.io for publishing compatibility. This version has a known issue with `UNNEST` operations due to an upstream DataFusion bug in expression handling. If you need `UNNEST` support in federated queries, please track [PR #135](https://github.com/datafusion-contrib/datafusion-federation/pull/135) for the fix.

```sql
-- Join ClickHouse with Parquet files
SELECT
    ch.user_id,
    ch.total_purchases,
    parquet.user_segment
FROM clickhouse.analytics.user_stats ch
JOIN local_parquet.user_segments parquet
    ON ch.user_id = parquet.user_id
WHERE ch.total_purchases > 1000;

-- Federated aggregations
SELECT
    segment,
    AVG(clickhouse(log(total_purchases), 'Float64')) as avg_log_purchases
FROM (
    SELECT
        ch.user_id,
        ch.total_purchases,
        csv.segment
    FROM clickhouse.sales.users ch
    JOIN local_csv.segments csv ON ch.user_id = csv.user_id
)
GROUP BY segment;
```

## Features

- **default**: Core functionality with `ClickHouse` integration
- **federation**: Enable cross-database queries
- **cloud**: `ClickHouse` Cloud support
- **test-utils**: Testing utilities for development

## Development

### Running Tests

```bash
# All tests with multiple feature combinations
just test

# Specific test types
just test-e2e          # End-to-end tests
just test-federation   # Federation tests
just test-unit         # Unit tests only

# Coverage reports
just coverage          # HTML report
just coverage-lcov     # LCOV for CI
```

### Environment Variables

- `RUST_LOG=debug` - Enable debug logging
- `DISABLE_CLEANUP=true` - Keep test containers running
- `DISABLE_CLEANUP_ON_ERROR=true` - Keep containers on test failure

## Examples

See the [examples](examples/) directory for a complete working example:

- **Basic Integration**: Simple `ClickHouse` querying
- **Federation**: Cross-database joins
- **UDF Usage**: `ClickHouse` function examples
- **Schema Management**: Table creation and management

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](https://github.com/GeorgeLeePatterson/clickhouse-datafusion/blob/main/CONTRIBUTING.md) for guidelines.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/GeorgeLeePatterson/clickhouse-datafusion/blob/main/LICENSE) for details.
