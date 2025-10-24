#![allow(unused_crate_dependencies)]
#![allow(clippy::doc_markdown)]
//! Example demonstrating DROP TABLE operations with ClickHouse
//!
//! This example demonstrates:
//! - Creating tables using the catalog builder
//! - Dropping tables via SQL (fully supported!)
//! - DROP TABLE IF EXISTS for safer operations
//! - Verifying tables are removed from both DataFusion and ClickHouse
//!
//! See examples/README.md for Docker setup instructions.
//!
//! Run this example:
//! ```bash
//! cargo run --example 06_drop_tables --features test-utils
//! ```

use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::test_utils::get_or_create_container;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::prelude::*;

// Catalog and schema (database) configuration
const CATALOG: &str = "ch_df_examples";
const SCHEMA: &str = "example_db"; // Note: In ClickHouse, schema is synonymous with database

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🗑️  ClickHouse DROP TABLE Example\n");

    // Setup ClickHouse container
    let ch = get_or_create_container(None).await;

    // Create DataFusion context
    let ctx = SessionContext::new();

    // Connect to ClickHouse
    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| c.with_username(&ch.user).with_password(&ch.password))
        .build_catalog(&ctx, Some(CATALOG))
        .await?;

    // ============================================================
    // Example 1: Create and drop a simple table
    // ============================================================
    println!("1️⃣  Creating and dropping a simple table...\n");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let _clickhouse = clickhouse
        .with_schema(SCHEMA)
        .await?
        .with_new_table("users", ClickHouseEngine::MergeTree, Arc::clone(&schema))
        .update_create_options(|opts| opts.with_order_by(&["id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("   ✅ Created table 'users'");

    // Insert some data using SQL
    // Note: INSERT returns a Vec<RecordBatch> with a single "count" column (UInt64)
    // containing the number of rows inserted. You can use .map(|_| ())? to ignore it.
    let result = ctx
        .sql(&format!(
            "INSERT INTO {CATALOG}.{SCHEMA}.users
            (id, name)
        VALUES
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie')"
        ))
        .await?
        .collect()
        .await?;
    println!("   ✅ Inserted 3 rows");
    print_batches(&result)?;

    // Verify table exists and has data
    let count = ctx
        .sql(&format!("SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.users"))
        .await?
        .collect()
        .await?;
    println!("   ✅ Table has {} rows", count[0].column(0).as_primitive::<Int64Type>().value(0));

    // Drop the table using SQL
    println!("\n   🗑️  Dropping table 'users' via SQL...");
    ctx.sql(&format!("DROP TABLE {CATALOG}.{SCHEMA}.users"))
        .await?
        .collect()
        .await
        .map(|_| ())?;
    println!("   ✅ Table dropped successfully");

    // Verify table no longer exists
    println!("\n   🔍 Verifying table is gone...");
    let verify_result = ctx.sql(&format!("SELECT * FROM {CATALOG}.{SCHEMA}.users")).await;
    match verify_result {
        Ok(_) => println!("   ❌ Unexpected: Table still exists!"),
        Err(e) => {
            if e.to_string().contains("not found") || e.to_string().contains("doesn't exist") {
                println!("   ✅ Confirmed: Table no longer exists in DataFusion");
            } else {
                println!("   ⚠️  Unexpected error: {e}");
            }
        }
    }

    // ============================================================
    // Example 2: DROP TABLE IF EXISTS (safe dropping)
    // ============================================================
    println!("\n2️⃣  Testing DROP TABLE IF EXISTS...\n");

    // Create a table
    let _clickhouse = ctx
        .catalog(CATALOG)
        .ok_or("Catalog not found")?
        .schema(SCHEMA)
        .ok_or("Schema not found")?;

    let temp_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    // We need to get the catalog builder again to create a new table
    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| c.with_username(&ch.user).with_password(&ch.password))
        .build_catalog(&ctx, Some(CATALOG))
        .await?;

    let _clickhouse = clickhouse
        .with_schema(SCHEMA)
        .await?
        .with_new_table("temp_table", ClickHouseEngine::MergeTree, temp_schema)
        .update_create_options(|opts| opts.with_order_by(&["id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("   ✅ Created table 'temp_table'");

    // Drop it with IF EXISTS
    println!("   🗑️  Dropping with IF EXISTS...");
    ctx.sql(&format!("DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.temp_table"))
        .await?
        .collect()
        .await
        .map(|_| ())?;
    println!("   ✅ Table dropped");

    // Try to drop it again (should not error)
    println!("   🗑️  Attempting to drop again (should be safe)...");
    ctx.sql(&format!("DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.temp_table"))
        .await?
        .collect()
        .await
        .map(|_| ())?;
    println!("   ✅ IF EXISTS prevented error on non-existent table");

    // Try without IF EXISTS on non-existent table (should error)
    println!("\n   🗑️  Attempting DROP without IF EXISTS on non-existent table...");
    let result = ctx.sql(&format!("DROP TABLE {CATALOG}.{SCHEMA}.nonexistent_table")).await;
    match result {
        Ok(_) => println!("   ❌ Unexpected: DROP succeeded on non-existent table"),
        Err(e) => {
            println!("   ✅ Expected error: {e}");
            println!("   💡 Use IF EXISTS to avoid errors");
        }
    }

    // ============================================================
    // Example 3: Multiple table operations
    // ============================================================
    println!("\n3️⃣  Multiple table operations...\n");

    // Recreate catalog builder for multiple tables
    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| c.with_username(&ch.user).with_password(&ch.password))
        .build_catalog(&ctx, Some(CATALOG))
        .await?;

    let simple_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    // Create multiple tables
    let _clickhouse = clickhouse
        .with_schema(SCHEMA)
        .await?
        .with_new_table("table1", ClickHouseEngine::MergeTree, Arc::clone(&simple_schema))
        .update_create_options(|opts| opts.with_order_by(&["id".to_string()]))
        .create(&ctx)
        .await?
        .with_new_table("table2", ClickHouseEngine::MergeTree, Arc::clone(&simple_schema))
        .update_create_options(|opts| opts.with_order_by(&["id".to_string()]))
        .create(&ctx)
        .await?
        .with_new_table("table3", ClickHouseEngine::MergeTree, Arc::clone(&simple_schema))
        .update_create_options(|opts| opts.with_order_by(&["id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("   ✅ Created 3 tables: table1, table2, table3");

    // Drop them all
    println!("\n   🗑️  Dropping all 3 tables...");
    for table in &["table1", "table2", "table3"] {
        ctx.sql(&format!("DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{table}"))
            .await?
            .collect()
            .await
            .map(|_| ())?;
        println!("   ✅ Dropped {table}");
    }

    // ============================================================
    // Summary
    // ============================================================
    println!("\n4️⃣  Summary...\n");

    println!("   📋 What we demonstrated:");
    println!("      ✅ DROP TABLE removes tables from both DataFusion and ClickHouse");
    println!("      ✅ DROP TABLE IF EXISTS provides safe deletion");
    println!("      ✅ Multiple tables can be dropped independently");
    println!("      ✅ Attempting to drop non-existent tables errors (without IF EXISTS)");
    println!();
    println!("   💡 Best practices:");
    println!("      • Use DROP TABLE IF EXISTS for safer operations");
    println!("      • Verify table removal with catalog queries if needed");
    println!("      • DROP TABLE works via ctx.sql() just like other DDL");
    println!();
    println!("   ✅ Supported DDL operations in clickhouse-datafusion:");
    println!("      ✅ CREATE TABLE (via catalog builder)");
    println!("      ✅ DROP TABLE (via SQL)");
    println!("      ✅ DROP TABLE IF EXISTS (via SQL)");
    println!("      ✅ INSERT INTO (via SQL)");
    println!("      ✅ SELECT queries (via SQL)");

    println!("\n✅ All DROP TABLE operations completed successfully!");

    let _ = ch.shutdown().await.ok();
    Ok(())
}
