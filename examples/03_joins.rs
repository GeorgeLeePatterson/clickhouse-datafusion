#![allow(unused_crate_dependencies)]
// Table Joins Example
//
// This example shows how to join multiple ClickHouse tables using DataFusion.
//
// See examples/README.md for Docker setup instructions.
//
// Run this example:
// ```bash
// cargo run --example 03_joins --features test-utils
// ```

use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::test_utils::get_or_create_container;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::logical_expr::JoinType;
use datafusion::prelude::*;

// Catalog and schema (database) configuration
const CATALOG: &str = "ch_df_examples";
const SCHEMA: &str = "example_db"; // Note: In ClickHouse, schema is synonymous with database

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ClickHouse-DataFusion: Joins Example\n");

    // Setup ClickHouse container
    let ch = get_or_create_container(None).await;

    let ctx = SessionContext::new();

    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| c.with_username(&ch.user).with_password(&ch.password))
        .build_catalog(&ctx, Some(CATALOG))
        .await?;

    // Clean up any existing tables from previous runs
    drop(ctx.sql(&format!("DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.users")).await);
    drop(ctx.sql(&format!("DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.orders")).await);

    // Create users table
    let users_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, true),
    ]));

    let clickhouse = clickhouse
        .with_schema(SCHEMA)
        .await?
        .with_new_table("users", ClickHouseEngine::MergeTree, users_schema)
        .update_create_options(|opts| opts.with_order_by(&["user_id".to_string()]))
        .create(&ctx)
        .await?;

    // Create orders table
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("total", DataType::Float64, false),
    ]));

    let _clickhouse = clickhouse
        .with_new_table("orders", ClickHouseEngine::MergeTree, orders_schema)
        .update_create_options(|opts| opts.with_order_by(&["order_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("✓ Created tables");

    // Insert users
    // Note: INSERT returns a Vec<RecordBatch> with a single "count" column (UInt64)
    // containing the number of rows inserted. You can use .map(|_| ())? to ignore it.
    let result = ctx
        .sql(&format!(
            "INSERT INTO {CATALOG}.{SCHEMA}.users
            (user_id, name, department)
        VALUES
            (1, 'Alice', 'Engineering'),
            (2, 'Bob', 'Sales'),
            (3, 'Carol', 'Marketing'),
            (4, 'Dave', 'Sales')"
        ))
        .await?
        .collect()
        .await?;
    println!("✓ Inserted into users:");
    print_batches(&result)?;

    // Insert orders
    let result = ctx
        .sql(&format!(
            "INSERT INTO {CATALOG}.{SCHEMA}.orders
            (order_id, user_id, total)
        VALUES
            (101, 1, 250.50),
            (102, 1, 150.00),
            (103, 2, 500.75),
            (104, 3, 99.99),
            (105, 1, 75.25)"
        ))
        .await?
        .collect()
        .await?;
    println!("✓ Inserted into orders:");
    print_batches(&result)?;

    println!();
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Example 1: INNER JOIN using DataFrame API
    println!("Example 1: INNER JOIN - Users with their orders\n");
    let users = ctx.table(&format!("{CATALOG}.{SCHEMA}.users")).await?;
    let orders = ctx.table(&format!("{CATALOG}.{SCHEMA}.orders")).await?;

    let df = users
        .join(orders, JoinType::Inner, &["user_id"], &["user_id"], None)?
        .select(vec![col("name"), col("department"), col("order_id"), col("total")])?;

    print_batches(&df.collect().await?)?;

    // Example 2: Aggregated JOIN using SQL
    println!("\nExample 2: Total spending per user (using SQL)\n");
    let df = ctx
        .sql(&format!(
            "SELECT
                u.name,
                u.department,
                COUNT(o.order_id) as order_count,
                SUM(o.total) as total_spent
            FROM {CATALOG}.{SCHEMA}.users u
            LEFT JOIN {CATALOG}.{SCHEMA}.orders o ON u.user_id = o.user_id
            GROUP BY u.name, u.department
            ORDER BY total_spent DESC"
        ))
        .await?;

    print_batches(&df.collect().await?)?;

    // Example 3: Filter after JOIN
    println!("\nExample 3: Users with total spending > 200\n");
    let df = ctx
        .sql(&format!(
            "SELECT
                u.name,
                SUM(o.total) as total_spent
            FROM {CATALOG}.{SCHEMA}.users u
            INNER JOIN {CATALOG}.{SCHEMA}.orders o ON u.user_id = o.user_id
            GROUP BY u.name
            HAVING SUM(o.total) > 200
            ORDER BY total_spent DESC"
        ))
        .await?;

    print_batches(&df.collect().await?)?;

    println!("\n✅ Example completed successfully!");

    let _ = ch.shutdown().await.ok();
    Ok(())
}
