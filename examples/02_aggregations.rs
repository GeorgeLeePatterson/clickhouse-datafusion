#![allow(unused_crate_dependencies)]
// Aggregations and GROUP BY Example
//
// This example demonstrates grouping and aggregating data with ClickHouse and DataFusion.
//
// See examples/README.md for Docker setup instructions.
//
// Run this example:
// ```bash
// cargo run --example 02_aggregations --features test-utils
// ```

use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::test_utils::get_or_create_container;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::functions_aggregate::expr_fn::{avg, count, max};
use datafusion::prelude::*;

// Catalog and schema (database) configuration
const CATALOG: &str = "ch_df_examples";
const SCHEMA: &str = "example_db"; // Note: In ClickHouse, schema is synonymous with database

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ClickHouse-DataFusion: Aggregations Example\n");

    // Setup ClickHouse container
    let ch = get_or_create_container(None).await;

    let ctx = SessionContext::new();

    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| c.with_username(&ch.user).with_password(&ch.password))
        .build_catalog(&ctx, Some(CATALOG))
        .await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, true),
        Field::new("salary", DataType::Float64, true),
    ]));

    let _clickhouse = clickhouse
        .with_schema(SCHEMA)
        .await?
        .with_new_table("employees", ClickHouseEngine::MergeTree, schema)
        .update_create_options(|opts| opts.with_order_by(&["user_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("✓ Created database and table");

    // Note: INSERT returns a Vec<RecordBatch> with a single "count" column (UInt64)
    // containing the number of rows inserted. You can use .map(|_| ())? to ignore it.
    let result = ctx
        .sql(&format!(
            "INSERT INTO {CATALOG}.{SCHEMA}.employees
            (user_id, name, department, salary)
        VALUES
            (1, 'Alice', 'Engineering', 75000.0),
            (2, 'Bob', 'Engineering', 65000.0),
            (3, 'Carol', 'Sales', 80000.0),
            (4, 'Dave', 'Sales', 70000.0),
            (5, 'Eve', 'Engineering', 95000.0),
            (6, 'Frank', 'Marketing', 60000.0),
            (7, 'Grace', 'Marketing', 72000.0)"
        ))
        .await?
        .collect()
        .await?;

    println!("✓ Inserted sample data");
    print_batches(&result)?;
    println!();
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Example 1: COUNT all rows
    println!("Example 1: Total number of employees\n");
    let df = ctx
        .table(&format!("{CATALOG}.{SCHEMA}.employees"))
        .await?
        .aggregate(vec![], vec![count(lit(1)).alias("total_employees")])?;

    print_batches(&df.collect().await?)?;

    // Example 2: GROUP BY with multiple aggregates
    println!("\nExample 2: Department statistics (count, avg salary, max salary)\n");
    let df = ctx.table(&format!("{CATALOG}.{SCHEMA}.employees")).await?.aggregate(
        vec![col("department")],
        vec![
            count(lit(1)).alias("employee_count"),
            avg(col("salary")).alias("avg_salary"),
            max(col("salary")).alias("max_salary"),
        ],
    )?;

    print_batches(&df.collect().await?)?;

    // Example 3: Using SQL for aggregation
    println!("\nExample 3: Find departments with average salary > 70000 (using SQL)\n");
    let df = ctx
        .sql(&format!(
            "SELECT
                department,
                AVG(salary) as avg_salary,
                COUNT(*) as count
            FROM {CATALOG}.{SCHEMA}.employees
            GROUP BY department
            HAVING AVG(salary) > 70000
            ORDER BY avg_salary DESC"
        ))
        .await?;

    print_batches(&df.collect().await?)?;

    println!("\n✅ Example completed successfully!");

    let _ = ch.shutdown().await.ok();
    Ok(())
}
