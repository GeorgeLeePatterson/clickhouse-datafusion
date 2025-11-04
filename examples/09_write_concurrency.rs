#![allow(unused_crate_dependencies)]
#![allow(clippy::doc_markdown)]
//! Parallel Write Concurrency for High-Throughput Inserts
//!
//! Demonstrates configuring write concurrency for bulk INSERT operations.
//!
//! Features:
//! - Default concurrency (4 concurrent writes)
//! - Custom concurrency configuration (8 concurrent writes)
//! - Performance measurement and comparison
//! - Best practices for different workload sizes
//!
//! When to use higher concurrency:
//! - Bulk inserts with large datasets (100k+ rows)
//! - Multiple batches being inserted
//! - Available connection pool capacity
//!
//! See examples/README.md for Docker setup instructions.
//!
//! Run this example:
//! ```bash
//! cargo run --example 09_write_concurrency --features test-utils --release
//! ```

use std::sync::Arc;
use std::time::Instant;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::test_utils::get_or_create_container;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::prelude::*;

// Catalog and schema (database) configuration
const CATALOG: &str = "ch_df_examples";
const SCHEMA: &str = "concurrency_demo";

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Write Concurrency Example\n");
    println!("This example demonstrates parallel write performance for bulk inserts.\n");

    // Setup ClickHouse container
    let ch = get_or_create_container(None).await;

    // Test parameters - reduced for faster example execution
    let num_batches = 10;
    let rows_per_batch = 2_000;
    let total_rows = num_batches * rows_per_batch;

    println!("📊 Test Configuration:");
    println!("   - Total rows: {total_rows}");
    println!("   - Batches: {num_batches}");
    println!("   - Rows per batch: {rows_per_batch}\n");

    // ========================================
    // Example 1: Default Concurrency (4)
    // ========================================
    println!("1️⃣  Testing with DEFAULT concurrency (4 concurrent writes)...\n");

    let ctx1 = SessionContext::new();

    // Build ClickHouse catalog with default settings
    // Default write_concurrency = 4
    let builder1 = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| {
            c.with_username(&ch.user).with_password(&ch.password).with_ipv4_only(true)
        })
        .configure_pool(|p| p.max_size(8))
        .build_catalog(&ctx1, Some(CATALOG))
        .await?;

    // Create schema and table
    let clickhouse1 = builder1
        .with_schema(SCHEMA)
        .await?
        .with_new_table(
            "metrics_default",
            ClickHouseEngine::MergeTree,
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("timestamp", DataType::Int64, false),
                Field::new("value", DataType::Float64, false),
                Field::new("category", DataType::Utf8, false),
            ])),
        )
        .update_create_options(|opts| opts.with_order_by(&["id".into()]))
        .create(&ctx1)
        .await?
        .build_schema(None, &ctx1)
        .await?;

    drop(clickhouse1.build(&ctx1).await?);

    // Insert data and measure time
    let start1 = Instant::now();
    for i in 0..num_batches {
        let offset = i * rows_per_batch;
        let batch = create_test_batch(offset, rows_per_batch)?;
        let temp_table = format!("temp_batch_{i}");
        drop(ctx1.register_batch(&temp_table, batch)?);

        let _result = ctx1
            .sql(&format!(
                "INSERT INTO {CATALOG}.{SCHEMA}.metrics_default SELECT * FROM {temp_table}"
            ))
            .await?
            .collect()
            .await?;
    }
    let duration1 = start1.elapsed();
    let throughput1 = f64::from(total_rows) / duration1.as_secs_f64();

    println!("   ✅ Completed in {duration1:?}");
    println!("   📈 Throughput: {throughput1:.0} rows/sec\n");

    // ========================================
    // Example 2: Custom High Concurrency (8)
    // ========================================
    println!("2️⃣  Testing with HIGH concurrency (8 concurrent writes)...\n");

    let ctx2 = SessionContext::new();

    // Build ClickHouse catalog with custom write concurrency
    let builder2 = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| {
            c.with_username(&ch.user).with_password(&ch.password).with_ipv4_only(true)
        })
        .configure_pool(|p| p.max_size(16)) // Larger pool for higher concurrency
        .with_write_concurrency(8) // ⭐ Configure parallel writes
        .build_catalog(&ctx2, Some(CATALOG))
        .await?;

    // Create schema and table
    let clickhouse2 = builder2
        .with_schema(SCHEMA)
        .await?
        .with_new_table(
            "metrics_high_concurrency",
            ClickHouseEngine::MergeTree,
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("timestamp", DataType::Int64, false),
                Field::new("value", DataType::Float64, false),
                Field::new("category", DataType::Utf8, false),
            ])),
        )
        .update_create_options(|opts| opts.with_order_by(&["id".into()]))
        .create(&ctx2)
        .await?
        .build_schema(None, &ctx2)
        .await?;

    drop(clickhouse2.build(&ctx2).await?);

    // Insert data and measure time
    let start2 = Instant::now();
    for i in 0..num_batches {
        let offset = i * rows_per_batch;
        let batch = create_test_batch(offset, rows_per_batch)?;
        let temp_table = format!("temp_batch_{i}");
        drop(ctx2.register_batch(&temp_table, batch)?);

        let _result = ctx2
            .sql(&format!(
                "INSERT INTO {CATALOG}.{SCHEMA}.metrics_high_concurrency SELECT * FROM \
                 {temp_table}"
            ))
            .await?
            .collect()
            .await?;
    }
    let duration2 = start2.elapsed();
    let throughput2 = f64::from(total_rows) / duration2.as_secs_f64();

    println!("   ✅ Completed in {duration2:?}");
    println!("   📈 Throughput: {throughput2:.0} rows/sec\n");

    // ========================================
    // Example 3: EXPLAIN ANALYZE (Metrics)
    // ========================================
    println!("3️⃣  Demonstrating EXPLAIN ANALYZE metrics visibility...\n");

    // Create a single batch for demonstration
    let demo_batch = create_test_batch(0, 1000)?;
    drop(ctx2.register_batch("demo_data", demo_batch)?);

    // Use EXPLAIN ANALYZE to see metrics
    let explain_result = ctx2
        .sql(&format!(
            "EXPLAIN ANALYZE INSERT INTO {CATALOG}.{SCHEMA}.metrics_high_concurrency SELECT * \
             FROM demo_data"
        ))
        .await?
        .collect()
        .await?;

    println!("   EXPLAIN ANALYZE output:");
    print_batches(&explain_result)?;
    println!();

    // ========================================
    // Performance Comparison
    // ========================================
    println!("📊 Performance Comparison:\n");
    println!("┌─────────────────────┬──────────────┬──────────────────┬────────────┐");
    println!("│ Configuration       │ Duration     │ Throughput       │ vs Default │");
    println!("├─────────────────────┼──────────────┼──────────────────┼────────────┤");
    println!("│ Default (conc=4)    │ {duration1:>10.2?} │ {throughput1:>13.0} r/s │   baseline │");
    println!(
        "│ High (conc=8)       │ {:>10.2?} │ {:>13.0} r/s │ {:>8.2}x │",
        duration2,
        throughput2,
        throughput2 / throughput1
    );
    println!("└─────────────────────┴──────────────┴──────────────────┴────────────┘");
    println!();

    // ========================================
    // Verify Data Integrity
    // ========================================
    println!("4️⃣  Verifying data integrity...\n");

    let verify_ctx = SessionContext::new();
    let builder_verify = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| {
            c.with_username(&ch.user).with_password(&ch.password).with_ipv4_only(true)
        })
        .build_catalog(&verify_ctx, Some(CATALOG))
        .await?;

    drop(
        builder_verify
            .with_schema(SCHEMA)
            .await?
            .build_schema(None, &verify_ctx)
            .await?
            .build(&verify_ctx)
            .await?,
    );

    // Check row counts
    let count1 = verify_ctx
        .sql(&format!("SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.metrics_default"))
        .await?
        .collect()
        .await?;

    let count2 = verify_ctx
        .sql(&format!("SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.metrics_high_concurrency"))
        .await?
        .collect()
        .await?;

    println!("   Default concurrency table rows:");
    print_batches(&count1)?;
    println!();

    println!("   High concurrency table rows:");
    print_batches(&count2)?;
    println!();

    // ========================================
    // Best Practices
    // ========================================
    println!("💡 Best Practices:\n");
    println!("   ✅ Use default (concurrency=4) for most workloads");
    println!("   ✅ Increase concurrency for bulk inserts (100k+ rows)");
    println!("   ✅ Match concurrency to connection pool size");
    println!("   ✅ Monitor with EXPLAIN ANALYZE for optimization");
    println!("   ✅ Test different concurrency levels for your workload\n");

    println!("✅ Example completed successfully!");

    let _ = ch.shutdown().await.ok();
    Ok(())
}

/// Helper function to create test data batches
fn create_test_batch(
    offset: i32,
    num_rows: i32,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let ids: Vec<i32> = (offset..offset + num_rows).collect();
    let timestamps: Vec<i64> = ids.iter().map(|&i| i64::from(i) * 1000).collect();
    let values: Vec<f64> = ids.iter().map(|&i| f64::from(i) * 1.5 + 10.0).collect();
    let categories: Vec<String> = ids.iter().map(|&i| format!("category_{}", i % 10)).collect();

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("category", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(categories)),
        ],
    )?)
}
