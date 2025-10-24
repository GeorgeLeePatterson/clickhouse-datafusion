#![allow(unused_crate_dependencies)]
// Basic DataFrame Filtering Example
//
// This example shows how to filter data from ClickHouse using DataFusion's DataFrame API.
//
// See examples/README.md for Docker setup instructions.
//
// Run this example:
// ```bash
// cargo run --example 01_basic_filtering --features test-utils
// ```

use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::test_utils::get_or_create_container;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::prelude::*;

// Catalog and schema (database) configuration
const CATALOG: &str = "ch_df_examples";
const SCHEMA: &str = "example_db"; // Note: In ClickHouse, schema is synonymous with database

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ClickHouse-DataFusion: Basic Filtering Example\n");

    // Setup ClickHouse container
    let ch = get_or_create_container(None).await;

    // Create DataFusion context
    let ctx = SessionContext::new();

    // Connect to ClickHouse
    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| c.with_username(&ch.user).with_password(&ch.password))
        .build_catalog(&ctx, Some(CATALOG))
        .await?;

    // Define schema for users table
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("department", DataType::Utf8, true),
        Field::new("salary", DataType::Float64, true),
    ]));

    // Create database and table
    let _clickhouse = clickhouse
        .with_schema(SCHEMA)
        .await?
        .with_new_table("users", ClickHouseEngine::MergeTree, schema)
        .update_create_options(|opts| opts.with_order_by(&["user_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("✓ Created database and table");

    // Insert sample data
    // Note: INSERT returns a Vec<RecordBatch> with a single "count" column (UInt64)
    // containing the number of rows inserted. You can use .map(|_| ())? to ignore it.
    let insert_result = ctx
        .sql(&format!(
            "INSERT INTO {CATALOG}.{SCHEMA}.users
            (user_id, name, age, department, salary)
        VALUES
            (1, 'Alice', 30, 'Engineering', 75000.0),
            (2, 'Bob', 25, 'Engineering', 65000.0),
            (3, 'Carol', 35, 'Sales', 80000.0),
            (4, 'Dave', 28, 'Sales', 70000.0),
            (5, 'Eve', 40, 'Engineering', 95000.0)"
        ))
        .await?
        .collect()
        .await?;

    println!("✓ Inserted sample data");
    print_batches(&insert_result)?;
    println!();

    // Example 1: Simple filter (age > 25)
    println!("Example 1: Filter users where age > 25\n");
    let df =
        ctx.table(&format!("{CATALOG}.{SCHEMA}.users")).await?.filter(col("age").gt(lit(25)))?;

    let results = df.collect().await?;
    print_batches(&results)?;

    // Example 2: Multiple conditions
    println!("\nExample 2: Filter Engineering department with salary > 70000\n");
    let df = ctx
        .table(&format!("{CATALOG}.{SCHEMA}.users"))
        .await?
        .filter(col("department").eq(lit("Engineering")).and(col("salary").gt(lit(70000))))?;

    let results = df.collect().await?;
    print_batches(&results)?;

    // Example 3: Using SQL instead
    println!("\nExample 3: Same query using SQL\n");
    let df = ctx
        .sql(&format!(
            "SELECT *
            FROM {CATALOG}.{SCHEMA}.users
            WHERE age BETWEEN 30 AND 40"
        ))
        .await?;

    let results = df.collect().await?;
    print_batches(&results)?;

    println!("\n✅ Example completed successfully!");

    let _ = ch.shutdown().await.ok();
    Ok(())
}
