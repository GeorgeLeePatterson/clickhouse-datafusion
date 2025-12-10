#![allow(unused_crate_dependencies)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::too_many_lines)]
//! # Agent-Focused Comprehensive Example for clickhouse-datafusion
//!
//! ## PURPOSE
//! This example is designed for AI agents to understand the complete functionality
//! of the clickhouse-datafusion library. It demonstrates ALL features with detailed
//! explanations of concepts, patterns, and best practices.
//!
//! ## AUDIENCE
//! AI agents that need to understand how to use clickhouse-datafusion in user code.
//! This is NOT optimized for human readability - it's optimized for agent learning.
//!
//! ## STRUCTURE
//! Each section is clearly marked and explains:
//! - WHAT: What functionality is being demonstrated
//! - WHY: Why you would use this pattern
//! - HOW: How to implement it correctly
//! - VARIATIONS: Alternative approaches
//!
//! ## RUN THIS EXAMPLE
//! ```bash
//! cargo run --example 99_Agent_All_in_one --features "test-utils federation"
//! ```

use std::fs;
use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::test_utils::get_or_create_container;
use clickhouse_datafusion::federation::FederatedContext;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty;
use datafusion::datasource::MemTable;
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::*;

// =============================================================================
// SECTION 1: CONFIGURATION CONSTANTS
// =============================================================================
// WHAT: Catalog and schema names for ClickHouse integration
// WHY: Using constants makes code maintainable and easy to change
// CONCEPT: In ClickHouse/DataFusion hierarchy:
//   - CATALOG: Top-level namespace (e.g., "ch_df_examples")
//   - SCHEMA: Database name (e.g., "example_db") - schema ≡ database in ClickHouse
//   - TABLE: Actual table name (e.g., "users")
// USAGE: Reference tables as "{CATALOG}.{SCHEMA}.{table_name}" in SQL

const CATALOG: &str = "ch_df_examples";
const SCHEMA: &str = "example_db"; // Note: In ClickHouse, schema is synonymous with database

// =============================================================================
// SECTION 2: SETUP AND INITIALIZATION
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🤖 Agent-Focused Comprehensive Example for clickhouse-datafusion\n");
    println!("{}", "=".repeat(80));

    // -------------------------------------------------------------------------
    // 2.1: ClickHouse Container Setup
    // -------------------------------------------------------------------------
    // WHAT: Automatic ClickHouse Docker container management
    // WHY: No manual Docker setup required - testcontainers handles it
    // HOW: get_or_create_container() creates/reuses a ClickHouse instance
    // RETURNS: ClickHouseContainer with connection details (host, port, credentials)
    //
    // IMPORTANT - EXAMPLES vs PRODUCTION USAGE:
    //   This example uses testcontainers for automatic setup (test-utils feature).
    //   In your own applications, you must:
    //     1. Set up your own ClickHouse instance (Docker, bare metal, or cloud)
    //     2. Replace ch.get_native_url() with your actual ClickHouse URL
    //     3. Provide your own credentials via configure_client()
    //
    //   Example production setup:
    //     let clickhouse = ClickHouseBuilder::new("your-clickhouse-server:9000")
    //         .configure_client(|c| c
    //             .with_username("your_user")
    //             .with_password("your_password")
    //             .with_default_database("your_db"))
    //         .build_catalog(&ctx, Some(CATALOG))
    //         .await?;
    println!("\n📦 SECTION 2.1: Container Setup");
    println!("{}", "-".repeat(80));
    let ch = get_or_create_container(None).await;
    println!("✅ ClickHouse container ready at: {}", ch.get_native_url());

    // -------------------------------------------------------------------------
    // 2.2: DataFusion Context Creation
    // -------------------------------------------------------------------------
    // WHAT: Create DataFusion execution context with federation support
    // WHY: Federation allows joining ClickHouse with other data sources
    // PATTERN: SessionContext::new().federate() enables cross-source queries
    println!("\n🔧 SECTION 2.2: DataFusion Context");
    println!("{}", "-".repeat(80));
    let ctx = SessionContext::new().federate();
    println!("✅ Federation-enabled DataFusion context created");

    // -------------------------------------------------------------------------
    // 2.3: ClickHouse Catalog Builder
    // -------------------------------------------------------------------------
    // WHAT: Configure ClickHouse connection and create catalog
    // WHY: Catalog builder pattern allows flexible table/schema management
    // CONFIGURATION:
    //   - configure_client(): Set username, password, compression, etc.
    //   - build_catalog(): Create catalog with optional name (defaults to "clickhouse")
    // RETURNS: ClickHouseCatalogBuilder for further table creation
    //
    // IMPORTANT - URL FORMATS AND PORTS:
    //   Native Protocol (port 9000):
    //     - Format: "localhost:9000" or "192.168.1.100:9000"
    //     - NO "http://" prefix on native URLs
    //     - Used by: clickhouse-arrow, native ClickHouse clients
    //     - Performance: Faster binary protocol
    //
    //   HTTP Protocol (port 8123):
    //     - Format: "http://localhost:8123" or "https://clickhouse.example.com:8443"
    //     - REQUIRES "http://" or "https://" prefix
    //     - Used by: HTTP-based ClickHouse clients, REST APIs
    //     - Performance: Slower text-based protocol
    //
    //   This library uses the NATIVE protocol via clickhouse-arrow for optimal performance.
    //
    // IMPORTANT - DEFAULT SCHEMA (DATABASE):
    //   The schema (database) set as the default on the ClickHouse client will be set
    //   as the initial schema after calling build_catalog(). This can be changed on the
    //   fly by calling with_schema() on the catalog builder returned.
    //
    //   Example with default database:
    //     .configure_client(|c| c.with_default_database("analytics"))
    //     // Initial schema will be "analytics"
    //
    // IMPORTANT - DEFAULT ARROW OPTIONS:
    //   By default, this library configures Arrow options for DataFusion compatibility:
    //     - strings_as_strings(true)     - Use string encoding vs binary
    //     - strict_schema(false)         - Allow schema flexibility
    //     - disable_strict_schema_ddl(true)
    //     - nullable_array_default_empty(true)
    //
    //   Override via: .configure_arrow_options(|opts| opts.with_strings_as_strings(false))
    //
    // IMPORTANT - SCHEMA COERCION:
    //   Schema coercion allows automatic type conversion during query execution.
    //   Enable via: ClickHouseBuilder::new(url).with_coercion(true)
    //
    //   WHEN TO USE:
    //     - Convenience over performance (non-latency-sensitive use cases)
    //     - Working with ClickHouse higher-order functions with complex return types
    //     - Prototyping where exact type matching is tedious
    //
    //   PERFORMANCE COST:
    //     - Non-zero cost incurred PER RecordBatch during streaming
    //     - Avoid if possible by providing exact return types to clickhouse() UDF
    //     - Cost accumulates on large result sets
    println!("\n🔗 SECTION 2.3: ClickHouse Catalog Builder");
    println!("{}", "-".repeat(80));
    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| c.with_username(&ch.user).with_password(&ch.password))
        .build_catalog(&ctx, Some(CATALOG))
        .await?;
    println!("✅ ClickHouse catalog '{CATALOG}' initialized");

    // =============================================================================
    // SECTION 3: SCHEMA AND TABLE CREATION
    // =============================================================================
    println!("\n📋 SECTION 3: Schema and Table Creation");
    println!("{}", "=".repeat(80));

    // -------------------------------------------------------------------------
    // 3.1: Define Arrow Schemas
    // -------------------------------------------------------------------------
    // WHAT: Arrow schemas define table structure with types and nullability
    // WHY: Type-safe schema definition prevents runtime errors
    // SUPPORTED TYPES:
    //   - Numeric: Int8, Int16, Int32, Int64, UInt8-64, Float32, Float64
    //   - String: Utf8, LargeUtf8
    //   - Boolean: Boolean
    //   - Temporal: Date32, Date64, Timestamp
    //   - Decimal: Decimal128, Decimal256
    //   - Complex: List, Struct, Map
    println!("\n🔨 SECTION 3.1: Arrow Schema Definition");
    println!("{}", "-".repeat(80));

    let users_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false), // NOT NULL
        Field::new("name", DataType::Utf8, false),     // NOT NULL
        Field::new("email", DataType::Utf8, false),    // NOT NULL
        Field::new("age", DataType::Int32, true),      // NULLABLE
        Field::new("salary", DataType::Float64, true), // NULLABLE
    ]));

    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int32, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("total", DataType::Float64, false),
    ]));

    let events_schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int32, false),
        Field::new("event_type", DataType::Utf8, false),
        // List type for array columns
        Field::new_list("tags", Field::new_list_field(DataType::Utf8, false), false),
    ]));

    println!("✅ Defined 3 Arrow schemas: users, orders, events");

    // -------------------------------------------------------------------------
    // 3.2: Create ClickHouse Tables
    // -------------------------------------------------------------------------
    // WHAT: Use catalog builder to create tables on ClickHouse server
    // PATTERN:
    //   1. with_schema(db_name) - Set database/schema
    //   2. with_new_table(name, engine, schema) - Define table
    //   3. update_create_options() - Set engine options (ORDER BY, PARTITION BY, etc.)
    //   4. create(ctx) - Execute CREATE TABLE
    //   5. build(ctx) - Register in DataFusion catalog
    //
    // CRITICAL - MUST CALL .build() OR .build_schema():
    //   After creating tables, you MUST call one of the build variations to ensure
    //   the catalog provider is up to date with the remote ClickHouse database.
    //   If you forget to do this, DataFusion queries targeting these tables will FAIL.
    //
    //   The catalog builder pattern allows chaining multiple table creations, then
    //   calling .build(&ctx) once at the end to synchronize everything.
    //
    // PATTERN - FLUENT BUILDER CHAINING:
    //   The .create() method returns back a ClickHouseCatalogBuilder, enabling chaining:
    //     clickhouse
    //       .with_new_table("users", ...).create(&ctx).await?
    //       .with_new_table("orders", ...).create(&ctx).await?
    //       .with_new_table("events", ...).create(&ctx).await?
    //       .build(&ctx).await?;
    //   This allows creating multiple tables before synchronizing with DataFusion once.
    //
    // IMPORTANT - FEDERATION REQUIREMENT:
    //   If the "federation" feature is enabled (as in this example), the SessionContext
    //   MUST be federated via .federate() before calling .build(), or build() will error.
    //
    // ENGINE OPTIONS:
    //   - MergeTree: Default, high-performance
    //   - ReplacingMergeTree: Deduplication
    //   - SummingMergeTree: Automatic aggregation
    //   - AggregatingMergeTree: Aggregation functions in table engine
    println!("\n🏗️  SECTION 3.2: Create ClickHouse Tables");
    println!("{}", "-".repeat(80));

    let _clickhouse = clickhouse
        .with_schema(SCHEMA)
        .await?
        // Create users table
        .with_new_table("users", ClickHouseEngine::MergeTree, Arc::clone(&users_schema))
        .update_create_options(|opts| opts.with_order_by(&["user_id".to_string()]))
        .create(&ctx)
        .await?
        // Create orders table
        .with_new_table("orders", ClickHouseEngine::MergeTree, Arc::clone(&orders_schema))
        .update_create_options(|opts| opts.with_order_by(&["order_id".to_string()]))
        .create(&ctx)
        .await?
        // Create events table
        .with_new_table("events", ClickHouseEngine::MergeTree, Arc::clone(&events_schema))
        .update_create_options(|opts| opts.with_order_by(&["event_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("✅ Created 3 ClickHouse tables: users, orders, events");

    // =============================================================================
    // SECTION 4: DATA INSERTION
    // =============================================================================
    println!("\n💾 SECTION 4: Data Insertion");
    println!("{}", "=".repeat(80));

    // -------------------------------------------------------------------------
    // 4.1: SQL INSERT Statements
    // -------------------------------------------------------------------------
    // WHAT: Insert data using SQL INSERT INTO ... VALUES
    // WHY: SQL is familiar and supports bulk inserts
    // FORMAT:
    //   - INSERT INTO table (cols) VALUES (vals), (vals)...
    //   - Use format!() to interpolate catalog/schema constants
    //   - collect().await to execute and wait for completion
    //
    // IMPORTANT - INSERT RETURN VALUE:
    //   INSERT returns a Vec<RecordBatch> with a single "count" column (UInt64)
    //   containing the number of rows inserted.
    //
    //   You can either:
    //     1. Capture and use it: let result = .collect().await?;
    //     2. Ignore it: .collect().await.map(|_| ())?;
    //
    // PATTERN:
    //   ctx.sql(&format!("INSERT..."))
    //      .await? - Parse SQL
    //      .collect().await? - Execute and return row count
    println!("\n📝 SECTION 4.1: SQL INSERT Statements");
    println!("{}", "-".repeat(80));

    let result = ctx
        .sql(&format!(
            "INSERT INTO {CATALOG}.{SCHEMA}.users
            (user_id, name, email, age, salary)
        VALUES
            (1, 'Alice', 'alice@example.com', 30, 75000.0),
            (2, 'Bob', 'bob@example.com', 25, 65000.0),
            (3, 'Carol', 'carol@example.com', 35, 85000.0),
            (4, 'Dave', 'dave@example.com', 28, 72000.0),
            (5, 'Eve', 'eve@example.com', 40, 95000.0)"
        ))
        .await?
        .collect()
        .await?;
    println!("Inserted into users:");
    pretty::print_batches(&result)?;

    let result = ctx
        .sql(&format!(
            "INSERT INTO {CATALOG}.{SCHEMA}.orders
            (order_id, user_id, product, quantity, total)
        VALUES
            (101, 1, 'Laptop', 1, 1500.00),
            (102, 1, 'Mouse', 2, 40.00),
            (103, 2, 'Keyboard', 1, 100.00),
            (104, 3, 'Monitor', 2, 600.00),
            (105, 4, 'Headphones', 1, 150.00),
            (106, 5, 'Webcam', 3, 300.00)"
        ))
        .await?
        .collect()
        .await?;
    println!("Inserted into orders:");
    pretty::print_batches(&result)?;

    let result = ctx
        .sql(&format!(
            "INSERT INTO {CATALOG}.{SCHEMA}.events
            (event_id, user_id, event_type, tags)
        VALUES
            (1, 1, 'login', make_array('web', 'auth')),
            (2, 1, 'purchase', make_array('web', 'order')),
            (3, 2, 'login', make_array('mobile', 'auth')),
            (4, 3, 'browse', make_array('web', 'catalog')),
            (5, 4, 'purchase', make_array('mobile', 'order'))"
        ))
        .await?
        .collect()
        .await?;
    println!("Inserted into events:");
    pretty::print_batches(&result)?;

    println!("\n✅ Inserted data into 3 tables");

    // Add delay for ClickHouse to process writes
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // -------------------------------------------------------------------------
    // 4.2: Parallel Writes / Write Concurrency Configuration
    // -------------------------------------------------------------------------
    // WHAT: Configure concurrent INSERT operations for improved throughput
    // WHY: Bulk inserts (100k+ rows) benefit from parallel processing
    //      Default concurrency=4 is optimal for most workloads
    //      Higher concurrency improves throughput for large-scale data loads
    //
    // HOW TO CONFIGURE:
    //   ClickHouseBuilder::new(url)
    //     .configure_pool(|p| p.max_size(16))        // Connection pool size
    //     .with_write_concurrency(8)                  // Parallel write workers
    //     .build_catalog(&ctx, Some(CATALOG))
    //     .await?
    //
    // KEY CONCEPTS:
    //   1. Write Concurrency: Number of batches processed in parallel
    //   2. Connection Pool: Must be >= write_concurrency for optimal performance
    //   3. Batch Processing: DataFusion splits INSERT into batches, each processed by a separate
    //      worker using buffer_unordered()
    //
    // CONFIGURATION GUIDELINES:
    //   - DEFAULT (concurrency=4):    General-purpose workloads
    //   - MEDIUM  (concurrency=8):    Bulk inserts 100k-1M rows
    //   - HIGH    (concurrency=12-16): Large-scale data loads (1M+ rows)
    //
    //   IMPORTANT: Always set pool.max_size >= write_concurrency
    //   Example: concurrency=8 requires pool.max_size >= 8
    //
    // PERFORMANCE MONITORING:
    //   Use EXPLAIN ANALYZE to view metrics:
    //     ctx.sql("EXPLAIN ANALYZE INSERT INTO...").await?.collect().await?
    //
    //   Metrics shown:
    //     - output_rows: Total rows inserted
    //     - elapsed_compute: Time spent on INSERT operation
    //
    // EXAMPLE THROUGHPUT IMPROVEMENTS (5M rows, 13 columns):
    //   concurrency=4:  ~400k rows/sec  (baseline)
    //   concurrency=8:  ~600k rows/sec  (+50% improvement)
    //   concurrency=16: ~800k rows/sec  (+100% improvement)
    //
    // WHEN TO INCREASE CONCURRENCY:
    //   ✅ Bulk data loads (100k+ rows)
    //   ✅ ETL pipelines with high throughput requirements
    //   ✅ Data migration scenarios
    //   ✅ When network/ClickHouse server can handle higher load
    //
    // WHEN TO USE DEFAULT (concurrency=4):
    //   ✅ Transactional inserts (<10k rows)
    //   ✅ Real-time streaming inserts
    //   ✅ Limited ClickHouse server resources
    //   ✅ Network bandwidth constraints
    //
    // ADVANCED: Compression Impact
    //   Different compression methods affect throughput:
    //     - NONE: Highest throughput, largest network usage
    //     - LZ4:  Balanced (recommended default)
    //     - ZSTD: Best compression, slightly lower throughput
    //
    //   Configure via:
    //     .configure_client(|c| c.with_compression(CompressionMethod::LZ4))
    //
    // BEST PRACTICES:
    //   1. Start with default (concurrency=4) and benchmark
    //   2. Use EXPLAIN ANALYZE to measure actual performance
    //   3. Increase concurrency gradually (4→8→12→16)
    //   4. Monitor ClickHouse server CPU/memory during inserts
    //   5. Match pool size to concurrency for optimal resource usage
    //   6. Test with representative data sizes and schemas
    //
    // SEE ALSO:
    //   - examples/09_write_concurrency.rs: Working example with benchmarks
    //   - examples/10_large_scale.rs: Advanced benchmarking tool
    println!("\n⚡ SECTION 4.2: Parallel Writes Configuration");
    println!("{}", "-".repeat(80));
    println!("💡 Parallel writes improve INSERT throughput for bulk data loads");
    println!("   Default concurrency=4 is optimal for most use cases");
    println!("   Increase for bulk inserts: use .with_write_concurrency(8-16)");
    println!("   Always ensure connection pool size >= write_concurrency");
    println!("   Monitor performance using: EXPLAIN ANALYZE INSERT INTO...");
    println!("\n📚 For detailed examples and benchmarks:");
    println!("   - cargo run --example 09_write_concurrency --features test-utils");
    println!("   - cargo run --example 10_large_scale --features test-utils");

    // =============================================================================
    // SECTION 5: BASIC QUERIES
    // =============================================================================
    println!("\n🔍 SECTION 5: Basic Queries");
    println!("{}", "=".repeat(80));

    // -------------------------------------------------------------------------
    // 5.1: Simple SELECT Query
    // -------------------------------------------------------------------------
    // WHAT: Basic SELECT with filtering
    // PATTERN:
    //   ctx.sql("SELECT...").await?.collect().await?
    // RETURNS: Vec<RecordBatch> containing query results
    println!("\n📊 SECTION 5.1: Simple SELECT Query");
    println!("{}", "-".repeat(80));

    let result = ctx
        .sql(&format!(
            "SELECT
                user_id,
                name,
                email,
                salary
            FROM {CATALOG}.{SCHEMA}.users
            WHERE salary > 70000
            ORDER BY salary DESC"
        ))
        .await?
        .collect()
        .await?;

    println!("Users with salary > 70000:");
    pretty::print_batches(&result)?;

    // -------------------------------------------------------------------------
    // 5.2: DataFrame API Query
    // -------------------------------------------------------------------------
    // WHAT: Use DataFrame API instead of SQL
    // WHY: More type-safe, composable, programmatic query building
    // PATTERN:
    //   1. ctx.table("name") - Get table reference
    //   2. .filter(condition) - WHERE clause
    //   3. .select(columns) - SELECT columns
    //   4. .sort(order) - ORDER BY
    //   5. .limit(n) - LIMIT
    println!("\n🛠️  SECTION 5.2: DataFrame API Query");
    println!("{}", "-".repeat(80));

    let df = ctx
        .table(&format!("{CATALOG}.{SCHEMA}.users"))
        .await?
        .filter(col("age").gt(lit(28)))?
        .select(vec![col("name"), col("age"), col("salary")])?
        .sort(vec![col("age").sort(false, false)])?;

    let result = df.collect().await?;
    println!("Users with age > 28 (DataFrame API):");
    pretty::print_batches(&result)?;

    // =============================================================================
    // SECTION 6: AGGREGATIONS
    // =============================================================================
    println!("\n📈 SECTION 6: Aggregations");
    println!("{}", "=".repeat(80));

    // -------------------------------------------------------------------------
    // 6.1: SQL Aggregations with GROUP BY
    // -------------------------------------------------------------------------
    // WHAT: Aggregate functions with grouping
    // FUNCTIONS: COUNT, SUM, AVG, MIN, MAX
    // PATTERN: SELECT agg_func(col) ... GROUP BY col
    println!("\n🧮 SECTION 6.1: SQL Aggregations with GROUP BY");
    println!("{}", "-".repeat(80));

    let result = ctx
        .sql(&format!(
            "SELECT
                COUNT(*) as total_orders,
                SUM(total) as revenue,
                AVG(total) as avg_order,
                MIN(total) as min_order,
                MAX(total) as max_order
            FROM {CATALOG}.{SCHEMA}.orders"
        ))
        .await?
        .collect()
        .await?;

    println!("Order statistics:");
    pretty::print_batches(&result)?;

    // -------------------------------------------------------------------------
    // 6.2: DataFrame API Aggregations
    // -------------------------------------------------------------------------
    // WHAT: Use DataFrame aggregate() method
    // PATTERN: .aggregate(group_cols, agg_exprs)
    println!("\n🔢 SECTION 6.2: DataFrame API Aggregations");
    println!("{}", "-".repeat(80));

    let df = ctx.table(&format!("{CATALOG}.{SCHEMA}.orders")).await?.aggregate(vec![], vec![
        count(col("order_id")).alias("order_count"),
        sum(col("total")).alias("total_revenue"),
        avg(col("total")).alias("avg_order_value"),
    ])?;

    let result = df.collect().await?;
    println!("Aggregations via DataFrame API:");
    pretty::print_batches(&result)?;

    // =============================================================================
    // SECTION 7: JOINS
    // =============================================================================
    println!("\n🔗 SECTION 7: Joins");
    println!("{}", "=".repeat(80));

    // -------------------------------------------------------------------------
    // 7.1: SQL INNER JOIN
    // -------------------------------------------------------------------------
    // WHAT: Join two tables on common column
    // TYPES: INNER, LEFT, RIGHT, FULL OUTER, CROSS
    // PATTERN: FROM table1 JOIN table2 ON condition
    println!("\n🤝 SECTION 7.1: SQL INNER JOIN");
    println!("{}", "-".repeat(80));

    let result = ctx
        .sql(&format!(
            "SELECT
                u.name,
                u.email,
                o.product,
                o.quantity,
                o.total
            FROM {CATALOG}.{SCHEMA}.orders o
            INNER JOIN {CATALOG}.{SCHEMA}.users u ON o.user_id = u.user_id
            ORDER BY o.order_id"
        ))
        .await?
        .collect()
        .await?;

    println!("Orders with user details:");
    pretty::print_batches(&result)?;

    // -------------------------------------------------------------------------
    // 7.2: DataFrame API JOIN
    // -------------------------------------------------------------------------
    // WHAT: Join using DataFrame API
    // PATTERN: .join(other_df, join_type, left_cols, right_cols, filter)
    println!("\n🔀 SECTION 7.2: DataFrame API JOIN");
    println!("{}", "-".repeat(80));

    let users_df = ctx.table(&format!("{CATALOG}.{SCHEMA}.users")).await?;
    let orders_df = ctx.table(&format!("{CATALOG}.{SCHEMA}.orders")).await?;

    let joined = orders_df
        .join(users_df, JoinType::Inner, &["user_id"], &["user_id"], None)?
        .select(vec![col("name"), col("product"), col("quantity"), col("total")])?
        .sort(vec![col("total").sort(false, false)])?;

    let result = joined.collect().await?;
    println!("Joined data via DataFrame API:");
    pretty::print_batches(&result)?;

    // -------------------------------------------------------------------------
    // 7.3: Aggregated JOIN
    // -------------------------------------------------------------------------
    // WHAT: Join with aggregation (total spending per user)
    // PATTERN: GROUP BY after JOIN
    println!("\n💰 SECTION 7.3: Aggregated JOIN");
    println!("{}", "-".repeat(80));

    let result = ctx
        .sql(&format!(
            "SELECT
                u.name,
                COUNT(o.order_id) as order_count,
                SUM(o.total) as total_spent
            FROM {CATALOG}.{SCHEMA}.users u
            LEFT JOIN {CATALOG}.{SCHEMA}.orders o ON u.user_id = o.user_id
            GROUP BY u.name
            ORDER BY total_spent DESC"
        ))
        .await?
        .collect()
        .await?;

    println!("Total spending per user:");
    pretty::print_batches(&result)?;

    // =============================================================================
    // SECTION 8: WINDOW FUNCTIONS
    // =============================================================================
    println!("\n🪟 SECTION 8: Window Functions");
    println!("{}", "=".repeat(80));

    // -------------------------------------------------------------------------
    // 8.1: ROW_NUMBER Window Function
    // -------------------------------------------------------------------------
    // WHAT: Assign row numbers within partitions
    // SYNTAX: ROW_NUMBER() OVER (PARTITION BY col ORDER BY col)
    // FUNCTIONS: ROW_NUMBER, RANK, DENSE_RANK, NTILE
    println!("\n🔢 SECTION 8.1: ROW_NUMBER Window Function");
    println!("{}", "-".repeat(80));

    let result = ctx
        .sql(&format!(
            "SELECT
                name,
                product,
                total,
                ROW_NUMBER() OVER (PARTITION BY product ORDER BY total DESC) as rank_in_product
            FROM {CATALOG}.{SCHEMA}.users u
            JOIN {CATALOG}.{SCHEMA}.orders o ON u.user_id = o.user_id
            ORDER BY product, rank_in_product"
        ))
        .await?
        .collect()
        .await?;

    println!("Ranked orders by product:");
    pretty::print_batches(&result)?;

    // -------------------------------------------------------------------------
    // 8.2: Aggregate Window Functions
    // -------------------------------------------------------------------------
    // WHAT: Running totals and averages
    // FUNCTIONS: SUM() OVER, AVG() OVER, MIN() OVER, MAX() OVER
    println!("\n📊 SECTION 8.2: Aggregate Window Functions");
    println!("{}", "-".repeat(80));

    let result = ctx
        .sql(&format!(
            "SELECT
                order_id,
                total,
                SUM(total) OVER (ORDER BY order_id) as running_total,
                AVG(total) OVER (ORDER BY order_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as \
             moving_avg
            FROM {CATALOG}.{SCHEMA}.orders
            ORDER BY order_id"
        ))
        .await?
        .collect()
        .await?;

    println!("Running totals and moving averages:");
    pretty::print_batches(&result)?;

    // =============================================================================
    // SECTION 9: FEDERATION (Cross-Source Queries)
    // =============================================================================
    println!("\n🌐 SECTION 9: Federation");
    println!("{}", "=".repeat(80));

    // -------------------------------------------------------------------------
    // 9.1: Register In-Memory Table
    // -------------------------------------------------------------------------
    // WHAT: Create local DataFusion table in memory
    // WHY: Demonstrate joining ClickHouse with other data sources
    // PATTERN: MemTable::try_new(schema, batches)
    println!("\n💾 SECTION 9.1: Register In-Memory Table");
    println!("{}", "-".repeat(80));

    let local_schema = Arc::new(Schema::new(vec![
        Field::new("product", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("warehouse", DataType::Utf8, false),
    ]));

    let local_batch = RecordBatch::try_new(Arc::clone(&local_schema), vec![
        Arc::new(StringArray::from(vec![
            "Laptop",
            "Mouse",
            "Keyboard",
            "Monitor",
            "Headphones",
            "Webcam",
        ])),
        Arc::new(StringArray::from(vec![
            "Electronics",
            "Accessories",
            "Accessories",
            "Electronics",
            "Audio",
            "Video",
        ])),
        Arc::new(StringArray::from(vec![
            "Warehouse A",
            "Warehouse B",
            "Warehouse B",
            "Warehouse A",
            "Warehouse C",
            "Warehouse C",
        ])),
    ])?;

    let mem_table = MemTable::try_new(local_schema, vec![vec![local_batch]])?;
    let _prev_table = ctx.register_table("product_info", Arc::new(mem_table))?;

    println!("✅ Registered in-memory table: product_info");

    // -------------------------------------------------------------------------
    // 9.2: Federated Query (ClickHouse + In-Memory)
    // -------------------------------------------------------------------------
    // WHAT: Join ClickHouse table with local in-memory table
    // WHY: Combine remote and local data without moving everything to one system
    // PATTERN: Reference tables from different catalogs in same query
    println!("\n🔄 SECTION 9.2: Federated Query");
    println!("{}", "-".repeat(80));

    let result = ctx
        .sql(&format!(
            "SELECT
                o.product,
                p.category,
                p.warehouse,
                SUM(o.total) as total_sales
            FROM {CATALOG}.{SCHEMA}.orders o
            JOIN product_info p ON o.product = p.product
            GROUP BY o.product, p.category, p.warehouse
            ORDER BY total_sales DESC"
        ))
        .await?
        .collect()
        .await?;

    println!("Federated query - ClickHouse + in-memory:");
    pretty::print_batches(&result)?;

    // -------------------------------------------------------------------------
    // 9.3: Parquet Federation
    // -------------------------------------------------------------------------
    // WHAT: Join ClickHouse with Parquet files
    // WHY: Common pattern for data lake integration
    // PATTERN: register_parquet() then query like any other table
    println!("\n📄 SECTION 9.3: Parquet Federation");
    println!("{}", "-".repeat(80));

    // Create a Parquet file
    let parquet_path = "/tmp/agent_example_data.parquet";
    let parquet_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("signup_source", DataType::Utf8, false),
    ]));

    let parquet_batch = RecordBatch::try_new(Arc::clone(&parquet_schema), vec![
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
        Arc::new(StringArray::from(vec!["North", "South", "East", "West", "North"])),
        Arc::new(StringArray::from(vec!["web", "mobile", "web", "mobile", "referral"])),
    ])?;

    let file = fs::File::create(parquet_path)?;
    let mut writer = ArrowWriter::try_new(file, parquet_schema, None)?;
    writer.write(&parquet_batch)?;
    let _metadata = writer.close()?;

    ctx.register_parquet("user_regions", parquet_path, ParquetReadOptions::default()).await?;
    println!("✅ Registered Parquet file: user_regions");

    // Query joining ClickHouse + Parquet
    let result = ctx
        .sql(&format!(
            "SELECT
                u.name,
                u.email,
                r.region,
                r.signup_source,
                COUNT(o.order_id) as order_count
            FROM {CATALOG}.{SCHEMA}.users u
            JOIN user_regions r ON u.user_id = r.user_id
            LEFT JOIN {CATALOG}.{SCHEMA}.orders o ON u.user_id = o.user_id
            GROUP BY u.name, u.email, r.region, r.signup_source
            ORDER BY order_count DESC"
        ))
        .await?
        .collect()
        .await?;

    println!("Federated query - ClickHouse + Parquet:");
    pretty::print_batches(&result)?;

    // Cleanup
    fs::remove_file(parquet_path)?;

    // =============================================================================
    // SECTION 10: COMMON TABLE EXPRESSIONS (CTEs)
    // =============================================================================
    println!("\n📝 SECTION 10: Common Table Expressions (CTEs)");
    println!("{}", "=".repeat(80));

    // -------------------------------------------------------------------------
    // 10.1: Simple CTE
    // -------------------------------------------------------------------------
    // WHAT: Named subquery for reuse
    // SYNTAX: WITH cte_name AS (query) SELECT ... FROM cte_name
    // WHY: Improves readability, allows query reuse
    println!("\n🔖 SECTION 10.1: Simple CTE");
    println!("{}", "-".repeat(80));

    let result = ctx
        .sql(&format!(
            "WITH high_value_orders AS (
                SELECT
                    user_id,
                    product,
                    total
                FROM {CATALOG}.{SCHEMA}.orders
                WHERE total > 200
            )
            SELECT
                u.name,
                h.product,
                h.total
            FROM high_value_orders h
            JOIN {CATALOG}.{SCHEMA}.users u ON h.user_id = u.user_id
            ORDER BY h.total DESC"
        ))
        .await?
        .collect()
        .await?;

    println!("High-value orders with user details:");
    pretty::print_batches(&result)?;

    // -------------------------------------------------------------------------
    // 10.2: Multiple CTEs
    // -------------------------------------------------------------------------
    // WHAT: Multiple named subqueries
    // PATTERN: WITH cte1 AS (...), cte2 AS (...) SELECT ...
    println!("\n📚 SECTION 10.2: Multiple CTEs");
    println!("{}", "-".repeat(80));

    let result = ctx
        .sql(&format!(
            "WITH user_totals AS (
                SELECT
                    user_id,
                    SUM(total) as total_spent
                FROM {CATALOG}.{SCHEMA}.orders
                GROUP BY user_id
            ),
            user_stats AS (
                SELECT
                    u.name,
                    u.age,
                    ut.total_spent,
                    CASE
                        WHEN ut.total_spent > 1000 THEN 'VIP'
                        WHEN ut.total_spent > 500 THEN 'Premium'
                        ELSE 'Regular'
                    END as customer_tier
                FROM {CATALOG}.{SCHEMA}.users u
                JOIN user_totals ut ON u.user_id = ut.user_id
            )
            SELECT
                customer_tier,
                COUNT(*) as customer_count,
                AVG(total_spent) as avg_spending,
                AVG(age) as avg_age
            FROM user_stats
            GROUP BY customer_tier
            ORDER BY avg_spending DESC"
        ))
        .await?
        .collect()
        .await?;

    println!("Customer tier analysis:");
    pretty::print_batches(&result)?;

    // =============================================================================
    // SECTION 11: ERROR HANDLING AND BEST PRACTICES
    // =============================================================================
    println!("\n⚠️  SECTION 11: Error Handling and Best Practices");
    println!("{}", "=".repeat(80));

    // -------------------------------------------------------------------------
    // 11.1: Query Error Handling
    // -------------------------------------------------------------------------
    // WHAT: Proper error handling for queries
    // PATTERN: Use Result<T, E> and ? operator
    println!("\n🛡️  SECTION 11.1: Query Error Handling");
    println!("{}", "-".repeat(80));

    match ctx.sql(&format!("SELECT * FROM {CATALOG}.{SCHEMA}.nonexistent_table")).await {
        Ok(df) => {
            drop(df.collect().await);
            println!("⚠️  Unexpected: Query succeeded on nonexistent table");
        }
        Err(e) => {
            println!("✅ Expected error caught: {e}");
            println!("   ERROR HANDLING TIP: Always handle query errors gracefully");
        }
    }

    // -------------------------------------------------------------------------
    // 11.2: Best Practices Summary
    // -------------------------------------------------------------------------
    println!("\n📌 SECTION 11.2: Best Practices Summary");
    println!("{}", "-".repeat(80));
    println!("BEST PRACTICES FOR AGENTS:");
    println!("1. Tables follow a consistent catalog.schema.table naming convention");
    println!("2. Use format!() to interpolate constants in SQL");
    println!("3. Always await async operations with ? for error handling");
    println!("4. Use Arc::clone() for shared schema references");
    println!("5. Call .collect().await to execute queries");
    println!("6. Use .map(|_| ())? to discard INSERT/UPDATE results");
    println!(
        "7. Add tokio::time::sleep() after writes if reading immediately, or check for data \
         availability"
    );
    println!("8. Use vertical SQL formatting for readability");
    println!("9. Prefer DataFrame API for type-safe query building, SQL for ad-hoc queries");
    println!("10. Use federation (.federate()) for cross-source queries");

    // =============================================================================
    // SECTION 12: SUMMARY
    // =============================================================================
    println!("\n✅ SECTION 12: Example Complete");
    println!("{}", "=".repeat(80));
    println!("\n🎯 CONCEPTS COVERED:");
    println!("   ✓ Container setup with testcontainers");
    println!("   ✓ DataFusion context with federation");
    println!("   ✓ ClickHouse catalog and schema creation");
    println!("   ✓ Table creation with Arrow schemas");
    println!("   ✓ Data insertion (SQL and bulk)");
    println!("   ✓ Basic SELECT queries (SQL and DataFrame API)");
    println!("   ✓ Aggregations (COUNT, SUM, AVG, MIN, MAX)");
    println!("   ✓ Joins (INNER, LEFT, with aggregations)");
    println!("   ✓ Window functions (ROW_NUMBER, running totals)");
    println!("   ✓ Federation (ClickHouse + in-memory + Parquet)");
    println!("   ✓ Common Table Expressions (CTEs)");
    println!("   ✓ Error handling and best practices");
    println!("\n🤖 AGENT LEARNING COMPLETE!");

    let _ = ch.shutdown().await.ok();
    Ok(())
}
