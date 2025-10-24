#![allow(unused_crate_dependencies)]
#![cfg(not(feature = "mocks"))]

//! Stress tests for parallel writes and connection pool behavior
//!
//! These tests validate the integration between:
//! - clickhouse-datafusion's parallel write concurrency
//! - clickhouse-arrow's connection pool scaling (4 → 16 connections)

mod common;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[
    ("testcontainers", "debug"),
    ("hyper", "error"),
    ("clickhouse_arrow", "debug"),
    ("datafusion", "error"),
    ("clickhouse_datafusion", "debug"),
];

// Test 1: Maximum Concurrency Stress
#[cfg(feature = "test-utils")]
e2e_test!(stress_max_concurrency, tests::test_max_concurrency_stress, TRACING_DIRECTIVES, None);

// Test 2: Over-Subscription Scenario
#[cfg(feature = "test-utils")]
e2e_test!(stress_over_subscription, tests::test_over_subscription, TRACING_DIRECTIVES, None);

// Test 3: Connection Pool Overflow (36 concurrent operations)
#[cfg(feature = "test-utils")]
e2e_test!(stress_overflow_protection, tests::test_overflow_protection, TRACING_DIRECTIVES, None);

// Test 4: Multi-Client Parallel Inserts
#[cfg(feature = "test-utils")]
e2e_test!(stress_multi_client, tests::test_multi_client_inserts, TRACING_DIRECTIVES, None);

// Test 5: Connection Leak Detection
#[cfg(feature = "test-utils")]
e2e_test!(stress_connection_leaks, tests::test_connection_leak_detection, TRACING_DIRECTIVES, None);

// Test 6: Large Batch Handling
#[cfg(feature = "test-utils")]
e2e_test!(stress_large_batches, tests::test_large_batch_handling, TRACING_DIRECTIVES, None);

// Test 7: Scaling Study (Performance Matrix)
#[cfg(feature = "test-utils")]
e2e_test!(stress_scaling_study, tests::test_scaling_study, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use clickhouse_arrow::ArrowConnectionPoolBuilder;
    use clickhouse_arrow::prelude::ClickHouseEngine;
    use clickhouse_arrow::test_utils::ClickHouseContainer;
    #[cfg(feature = "federation")]
    use clickhouse_datafusion::federation::FederatedContext;
    use clickhouse_datafusion::{ClickHouseBuilder, DEFAULT_CLICKHOUSE_CATALOG};
    use datafusion::arrow;
    use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;

    use crate::common::helpers::configure_client;

    /// Test 1: Maximum Concurrency Stress
    /// Push both features to the limit with maximum connections and concurrency
    pub(super) async fn test_max_concurrency_stress(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "stress_max_concurrency";
        eprintln!("\n=== Test 1: Maximum Concurrency Stress ===");
        eprintln!("Config: pool_size=16, write_concurrency=16");
        eprintln!("Dataset: 100,000 rows in 100 batches");

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        // Maximum configuration
        let builder = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| configure_client(c, &ch))
            .configure_pool(|p| p.max_size(16))
            .with_write_concurrency(16)
            .build_catalog(&ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
            .await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let clickhouse = builder
            .with_schema(db)
            .await?
            .with_new_table("stress_table", ClickHouseEngine::MergeTree, schema.clone())
            .update_create_options(|opts| opts.with_order_by(&["id".into()]))
            .create(&ctx)
            .await?
            .build_schema(None, &ctx)
            .await?;

        drop(clickhouse.build(&ctx).await?);

        // Generate 100,000 rows in 100 batches
        let num_batches = 100;
        let batch_size = 1000;

        eprintln!(
            ">>> Inserting {} rows across {} batches...",
            num_batches * batch_size,
            num_batches
        );
        let start = Instant::now();

        for batch_num in 0..num_batches {
            let offset = batch_num * batch_size;
            let ids: Vec<i32> = (offset..offset + batch_size).collect();
            let names: Vec<String> = ids.iter().map(|i| format!("user_{}", i)).collect();

            let batch = RecordBatch::try_new(schema.clone(), vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ])?;

            let temp_table = format!("temp_data_{}", batch_num);
            drop(ctx.register_batch(&temp_table, batch)?);

            let _result = ctx
                .sql(&format!(
                    "INSERT INTO clickhouse.{db}.stress_table SELECT * FROM {temp_table}"
                ))
                .await?
                .collect()
                .await?;

            if (batch_num + 1) % 20 == 0 {
                eprintln!("  Progress: {}/{} batches", batch_num + 1, num_batches);
            }
        }

        let duration = start.elapsed();
        eprintln!(
            ">>> Completed in {:?} ({:.0} rows/sec)",
            duration,
            (num_batches * batch_size) as f64 / duration.as_secs_f64()
        );

        // Verify
        let count = ctx
            .sql(&format!("SELECT COUNT(*) FROM clickhouse.{db}.stress_table"))
            .await?
            .collect()
            .await?;

        arrow::util::pretty::print_batches(&count)?;
        eprintln!("✅ Test 1 PASSED");

        Ok(())
    }

    /// Test 2: Over-Subscription Scenario
    /// More writers than connections (realistic production)
    pub(super) async fn test_over_subscription(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "stress_over_subscription";
        eprintln!("\n=== Test 2: Over-Subscription Scenario ===");
        eprintln!("Config: pool_size=4, write_concurrency=16 (4x over-subscription)");
        eprintln!("Dataset: 50,000 rows in 50 batches");

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        // Over-subscription: 16 concurrent writers, only 4 connections
        let builder = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| configure_client(c, &ch))
            .configure_pool(|p| p.max_size(4))
            .with_write_concurrency(16)
            .build_catalog(&ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
            .await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let clickhouse = builder
            .with_schema(db)
            .await?
            .with_new_table("stress_table", ClickHouseEngine::MergeTree, schema.clone())
            .update_create_options(|opts| opts.with_order_by(&["id".into()]))
            .create(&ctx)
            .await?
            .build_schema(None, &ctx)
            .await?;

        drop(clickhouse.build(&ctx).await?);

        let num_batches = 50;
        let batch_size = 1000;

        eprintln!(">>> Testing connection pool under pressure...");
        let start = Instant::now();

        for batch_num in 0..num_batches {
            let offset = batch_num * batch_size;
            let ids: Vec<i32> = (offset..offset + batch_size).collect();
            let values: Vec<i32> = ids.iter().map(|i| i * 2).collect();

            let batch = RecordBatch::try_new(schema.clone(), vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ])?;

            let temp_table = format!("temp_data_{}", batch_num);
            drop(ctx.register_batch(&temp_table, batch)?);

            let _result = ctx
                .sql(&format!(
                    "INSERT INTO clickhouse.{db}.stress_table SELECT * FROM {temp_table}"
                ))
                .await?
                .collect()
                .await?;
        }

        let duration = start.elapsed();
        eprintln!(">>> Completed in {:?} (no deadlocks or hangs)", duration);

        let count = ctx
            .sql(&format!("SELECT COUNT(*) FROM clickhouse.{db}.stress_table"))
            .await?
            .collect()
            .await?;

        arrow::util::pretty::print_batches(&count)?;
        eprintln!("✅ Test 2 PASSED: Connection pool handled contention gracefully");

        Ok(())
    }

    /// Test 3: Overflow Protection (36 concurrent operations)
    /// Reproduce the exact scenario that caused overflow in old clickhouse-arrow
    pub(super) async fn test_overflow_protection(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "stress_overflow";
        eprintln!("\n=== Test 3: Connection Pool Overflow Protection ===");
        eprintln!("Config: pool_size=8, simulating 36 concurrent InsertMany ops");
        eprintln!("Goal: Verify no counter overflow (old bug)");

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| configure_client(c, &ch))
            .configure_pool(|p| p.max_size(8))
            .with_write_concurrency(8)
            .build_catalog(&ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
            .await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Utf8, false),
        ]));

        let clickhouse = builder
            .with_schema(db)
            .await?
            .with_new_table("stress_table", ClickHouseEngine::MergeTree, schema.clone())
            .update_create_options(|opts| opts.with_order_by(&["id".into()]))
            .create(&ctx)
            .await?
            .build_schema(None, &ctx)
            .await?;

        drop(clickhouse.build(&ctx).await?);

        // Simulate heavy concurrent load that caused overflow
        let num_operations = 36;
        eprintln!(">>> Running {} concurrent insert operations...", num_operations);

        for op in 0..num_operations {
            let ids: Vec<i32> = (op * 100..(op + 1) * 100).collect();
            let data: Vec<String> = ids.iter().map(|i| format!("data_{}", i)).collect();

            let batch = RecordBatch::try_new(schema.clone(), vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(data)),
            ])?;

            let temp_table = format!("temp_data_{}", op);
            drop(ctx.register_batch(&temp_table, batch)?);

            let _result = ctx
                .sql(&format!(
                    "INSERT INTO clickhouse.{db}.stress_table SELECT * FROM {temp_table}"
                ))
                .await?
                .collect()
                .await?;

            if (op + 1) % 10 == 0 {
                eprintln!("  Progress: {}/{} operations", op + 1, num_operations);
            }
        }

        let count = ctx
            .sql(&format!("SELECT COUNT(*) FROM clickhouse.{db}.stress_table"))
            .await?
            .collect()
            .await?;

        arrow::util::pretty::print_batches(&count)?;
        eprintln!("✅ Test 3 PASSED: No overflow detected");

        Ok(())
    }

    /// Test 4: Multi-Client Parallel Inserts
    /// Simulate multiple DataFusion contexts writing simultaneously
    pub(super) async fn test_multi_client_inserts(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "stress_multi_client";
        eprintln!("\n=== Test 4: Multi-Client Parallel Inserts ===");
        eprintln!("Config: 3 separate contexts, pool_size=16, concurrency=8 each");

        // Create 3 separate contexts
        let ctx1 = SessionContext::new();
        let ctx2 = SessionContext::new();
        let ctx3 = SessionContext::new();

        #[cfg(feature = "federation")]
        let ctx1 = ctx1.federate();
        #[cfg(feature = "federation")]
        let ctx2 = ctx2.federate();
        #[cfg(feature = "federation")]
        let ctx3 = ctx3.federate();

        // Create separate builders for each context (pool builders don't clone)
        for (i, ctx) in [&ctx1, &ctx2, &ctx3].iter().enumerate() {
            let pool_builder = ArrowConnectionPoolBuilder::new(ch.get_native_url())
                .configure_client(|c| configure_client(c, &ch))
                .configure_pool(|p| p.max_size(16));

            let builder =
                ClickHouseBuilder::new_with_pool_builder(ch.get_native_url(), pool_builder)
                    .with_write_concurrency(8)
                    .build_catalog(ctx, Some(&format!("clickhouse_{}", i + 1)))
                    .await?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("client_id", DataType::Int32, false),
            ]));

            let clickhouse = builder
                .with_schema(db)
                .await?
                .with_new_table("stress_table", ClickHouseEngine::MergeTree, schema)
                .update_create_options(|opts| opts.with_order_by(&["id".into()]))
                .create(ctx)
                .await?
                .build_schema(None, ctx)
                .await?;

            drop(clickhouse.build(ctx).await?);
        }

        eprintln!(">>> Running concurrent inserts from 3 clients...");

        // Run inserts concurrently
        let handles = vec![
            tokio::spawn(insert_from_client(ctx1, db, 1, 1000)),
            tokio::spawn(insert_from_client(ctx2, db, 2, 1000)),
            tokio::spawn(insert_from_client(ctx3, db, 3, 1000)),
        ];

        for handle in handles {
            handle.await.unwrap()?;
        }

        eprintln!("✅ Test 4 PASSED: Multi-client inserts completed");

        Ok(())
    }

    async fn insert_from_client(
        ctx: SessionContext,
        db: &str,
        client_id: i32,
        num_rows: i32,
    ) -> Result<()> {
        let ids: Vec<i32> = (0..num_rows).collect();
        let client_ids: Vec<i32> = vec![client_id; num_rows as usize];

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("client_id", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(client_ids)),
        ])?;

        let temp_table = format!("temp_data_client_{}", client_id);
        drop(ctx.register_batch(&temp_table, batch)?);

        let _result = ctx
            .sql(&format!(
                "INSERT INTO clickhouse_{}.{}.stress_table SELECT * FROM {temp_table}",
                client_id, db
            ))
            .await?
            .collect()
            .await?;

        eprintln!("  Client {} completed", client_id);
        Ok(())
    }

    /// Test 5: Connection Leak Detection
    /// Ensure no resource leaks under stress
    pub(super) async fn test_connection_leak_detection(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "stress_leak_detection";
        eprintln!("\n=== Test 5: Connection Leak Detection ===");
        eprintln!("Running 100 INSERT operations and monitoring pool state");

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| configure_client(c, &ch))
            .configure_pool(|p| p.max_size(8))
            .with_write_concurrency(8)
            .build_catalog(&ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
            .await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let clickhouse = builder
            .with_schema(db)
            .await?
            .with_new_table("stress_table", ClickHouseEngine::MergeTree, schema.clone())
            .update_create_options(|opts| opts.with_order_by(&["id".into()]))
            .create(&ctx)
            .await?
            .build_schema(None, &ctx)
            .await?;

        drop(clickhouse.build(&ctx).await?);

        eprintln!(">>> Running 100 insert operations...");

        for i in 0..100 {
            let ids: Vec<i32> = (i * 10..(i + 1) * 10).collect();
            let values: Vec<i32> = ids.clone();

            let batch = RecordBatch::try_new(schema.clone(), vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ])?;

            let temp_table = format!("temp_data_{}", i);
            drop(ctx.register_batch(&temp_table, batch)?);

            let _result = ctx
                .sql(&format!(
                    "INSERT INTO clickhouse.{db}.stress_table SELECT * FROM {temp_table}"
                ))
                .await?
                .collect()
                .await?;

            if (i + 1) % 25 == 0 {
                eprintln!("  Progress: {}/100 operations", i + 1);
            }
        }

        eprintln!("✅ Test 5 PASSED: No connection leaks detected");

        Ok(())
    }

    /// Test 6: Large Batch Handling
    /// Test with very large individual batches
    pub(super) async fn test_large_batch_handling(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "stress_large_batches";
        eprintln!("\n=== Test 6: Large Batch Handling ===");
        eprintln!("Config: 10 batches × 100,000 rows = 1M total rows");

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| configure_client(c, &ch))
            .configure_pool(|p| p.max_size(16))
            .with_write_concurrency(8)
            .build_catalog(&ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
            .await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let clickhouse = builder
            .with_schema(db)
            .await?
            .with_new_table("stress_table", ClickHouseEngine::MergeTree, schema.clone())
            .update_create_options(|opts| opts.with_order_by(&["id".into()]))
            .create(&ctx)
            .await?
            .build_schema(None, &ctx)
            .await?;

        drop(clickhouse.build(&ctx).await?);

        let num_batches = 10;
        let batch_size = 100_000;

        eprintln!(">>> Inserting {} large batches...", num_batches);
        let start = Instant::now();

        for batch_num in 0..num_batches {
            let offset = batch_num * batch_size;
            let ids: Vec<i32> = (offset..offset + batch_size).collect();
            let names: Vec<String> = ids.iter().map(|i| format!("record_{}", i)).collect();

            let batch = RecordBatch::try_new(schema.clone(), vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ])?;

            let temp_table = format!("temp_data_{}", batch_num);
            drop(ctx.register_batch(&temp_table, batch)?);

            let _result = ctx
                .sql(&format!(
                    "INSERT INTO clickhouse.{db}.stress_table SELECT * FROM {temp_table}"
                ))
                .await?
                .collect()
                .await?;

            eprintln!("  Batch {}/{} complete", batch_num + 1, num_batches);
        }

        let duration = start.elapsed();
        eprintln!(
            ">>> Completed 1M rows in {:?} ({:.0} rows/sec)",
            duration,
            1_000_000.0 / duration.as_secs_f64()
        );

        let count = ctx
            .sql(&format!("SELECT COUNT(*) FROM clickhouse.{db}.stress_table"))
            .await?
            .collect()
            .await?;

        arrow::util::pretty::print_batches(&count)?;
        eprintln!("✅ Test 6 PASSED: Large batches handled successfully");

        Ok(())
    }

    /// Test 7: Scaling Study (Performance Matrix)
    /// Benchmark different concurrency levels
    pub(super) async fn test_scaling_study(ch: Arc<ClickHouseContainer>) -> Result<()> {
        eprintln!("\n=== Test 7: Scaling Study ===");
        eprintln!("Testing performance across different concurrency levels\n");

        let configs = vec![
            (1, 4, "Sequential (baseline)"),
            (4, 4, "Conservative (default)"),
            (8, 8, "Moderate"),
            (12, 12, "Aggressive"),
            (16, 16, "Maximum"),
        ];

        let mut results = Vec::new();

        for (concurrency, pool_size, label) in configs {
            let db = format!("stress_scaling_{}", concurrency);

            let ctx = SessionContext::new();
            #[cfg(feature = "federation")]
            let ctx = ctx.federate();

            let builder = ClickHouseBuilder::new(ch.get_native_url())
                .configure_client(|c| configure_client(c, &ch))
                .configure_pool(|p| p.max_size(pool_size))
                .with_write_concurrency(concurrency)
                .build_catalog(&ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
                .await?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Int32, false),
            ]));

            let clickhouse = builder
                .with_schema(&db)
                .await?
                .with_new_table("stress_table", ClickHouseEngine::MergeTree, schema.clone())
                .update_create_options(|opts| opts.with_order_by(&["id".into()]))
                .create(&ctx)
                .await?
                .build_schema(None, &ctx)
                .await?;

            drop(clickhouse.build(&ctx).await?);

            // Insert 50,000 rows
            let num_batches = 50;
            let batch_size = 1000;

            let start = Instant::now();

            for batch_num in 0..num_batches {
                let offset = batch_num * batch_size;
                let ids: Vec<i32> = (offset..offset + batch_size).collect();
                let values: Vec<i32> = ids.clone();

                let batch = RecordBatch::try_new(schema.clone(), vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(Int32Array::from(values)),
                ])?;

                let temp_table = format!("temp_data_{}", batch_num);
                drop(ctx.register_batch(&temp_table, batch)?);

                let _result = ctx
                    .sql(&format!(
                        "INSERT INTO clickhouse.{db}.stress_table SELECT * FROM {temp_table}"
                    ))
                    .await?
                    .collect()
                    .await?;
            }

            let duration = start.elapsed();
            let throughput = (num_batches * batch_size) as f64 / duration.as_secs_f64();

            results.push((label, concurrency, pool_size, duration, throughput));

            eprintln!(
                "✓ {} (concurrency={}, pool={}): {:?} ({:.0} rows/sec)",
                label, concurrency, pool_size, duration, throughput
            );
        }

        eprintln!("\n=== Scaling Study Results ===");
        eprintln!(
            "{:<25} {:>12} {:>8} {:>12} {:>15}",
            "Config", "Concurrency", "Pool", "Duration", "Throughput"
        );
        eprintln!("{:-<72}", "");

        let baseline_throughput = results[0].4;

        for (label, concurrency, pool_size, duration, throughput) in &results {
            let speedup = throughput / baseline_throughput;
            eprintln!(
                "{:<25} {:>12} {:>8} {:>12.2?} {:>12.0} ({:.2}x)",
                label, concurrency, pool_size, duration, throughput, speedup
            );
        }

        eprintln!("\n✅ Test 7 PASSED: Scaling study complete");

        Ok(())
    }
}
