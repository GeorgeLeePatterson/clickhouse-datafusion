#![allow(unused_crate_dependencies)]
//! Large Scale `DataFusion` Insert Benchmark
//!
//! Comprehensive benchmarking tool for testing high-throughput INSERT operations
//! through `DataFusion`'s SQL interface with `ClickHouse` backend.
//!
//! Features:
//! - Configurable row counts (supports M/MM/K notation)
//! - Multiple worker (concurrency) levels
//! - Variable batch sizes
//! - Outlier-stripped averaging across iterations
//! - Detailed performance metrics and comparison tables
//!
//! Environment Variables:
//! - `ROW_COUNTS`: Comma-separated list (e.g., "1MM,5MM,10MM" or "1000000")
//! - WORKERS: Comma-separated worker counts (e.g., "8,16,32")
//! - `BATCH_SIZES`: Comma-separated batch sizes (e.g., "5K,10K,20K")
//! - ITERS: Number of iterations per test (default: 3, minimum: 3)
//! - `USE_TMPFS`: Set to "true" or "1" to enable tmpfs for zero disk I/O (default: false)
//!
//! Schema Configuration (via clickhouse-arrow's BatchConfig):
//! - Use environment variables from `clickhouse_arrow::test_utils::arrow_tests::BatchConfig`
//! - Example: UINT64=5 UTF8=2 FLOAT64=3
//!
//! Run this example:
//! ```bash
//! # Basic run with defaults (1M rows, 16 workers, 10K batch)
//! cargo run --example 10_large_scale --features test-utils --release
//!
//! # Custom configuration with tmpfs for maximum performance
//! USE_TMPFS=true ROW_COUNTS=5MM,10MM WORKERS=8,16,32 BATCH_SIZES=10K,20K cargo run --example 10_large_scale --features test-utils --release
//! ```

use std::sync::Arc;
use std::time::Instant;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::test_utils::{
    ClickHouseContainer, arrow_tests, get_or_create_benchmark_container,
};
use clickhouse_datafusion::prelude::*;
use comfy_table::Table;
use comfy_table::presets::UTF8_FULL;
use datafusion::prelude::*;

// Catalog and schema (database) configuration
const CATALOG: &str = "ch_df_bench";
const SCHEMA: &str = "large_scale";

#[derive(Debug, Clone)]
struct TestResult {
    workers:            usize,
    batch_size:         usize,
    rows:               usize,
    avg_duration_secs:  f64, // Outlier-stripped average
    best_duration_secs: f64, // Best (minimum) time
    avg_rows_per_sec:   f64, // Average throughput
    best_rows_per_sec:  f64, // Best (maximum) throughput
    count_time_secs:    f64,
    drop_time_secs:     f64,
}

#[allow(clippy::cast_precision_loss)]
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let total_start = Instant::now();

    // Parse comma-separated row counts (supports M/MM/K notation)
    let row_counts: Vec<usize> = std::env::var("ROW_COUNTS")
        .unwrap_or_else(|_| "1MM".to_string())
        .split(',')
        .filter_map(parse_number)
        .collect();

    // Parse comma-separated worker counts (concurrency levels)
    let worker_counts: Vec<usize> = std::env::var("WORKERS")
        .unwrap_or_else(|_| "16".to_string())
        .split(',')
        .filter_map(parse_number)
        .collect();

    // Parse comma-separated batch sizes
    let batch_sizes: Vec<usize> = std::env::var("BATCH_SIZES")
        .or_else(|_| std::env::var("BATCH_SIZE"))
        .unwrap_or_else(|_| "10K".to_string())
        .split(',')
        .filter_map(parse_number)
        .collect();

    let iters: usize = std::env::var("ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(3).max(3); // Minimum 3 iterations for outlier stripping

    // Parse compression method (default: LZ4)
    let compression = std::env::var("COMP")
        .map(|s| parse_compression(&s))
        .unwrap_or(clickhouse_arrow::CompressionMethod::LZ4);

    // Get schema configuration from environment
    let config = arrow_tests::BatchConfig::from_env();

    print_banner("Large Scale DataFusion Insert Benchmark", Some(80));
    eprintln!();

    // Check if tmpfs is enabled
    let use_tmpfs =
        std::env::var("USE_TMPFS").is_ok_and(|v| v.eq_ignore_ascii_case("true") || v == "1");
    if use_tmpfs {
        eprintln!("⚡ TMPFS ENABLED - Running with zero disk I/O for maximum performance");
        eprintln!();
    }

    // Display schema configuration
    print_schema_config(&config);
    eprintln!();

    // Measure actual RecordBatch size (using 100K rows as representative sample)
    let bytes_per_row = calculate_bytes_per_row(&config);

    eprintln!("RecordBatch Memory Analysis (100K row sample):");
    eprintln!("  Bytes per row: {bytes_per_row:.2} bytes/row");
    eprintln!();

    eprintln!("Benchmark Parameters:");
    eprintln!("  Row counts:    {row_counts:?}");
    eprintln!("  Worker counts: {worker_counts:?}");
    eprintln!("  Batch sizes:   {batch_sizes:?}");
    eprintln!("  Iterations:    {iters} (outlier-stripped average)");
    eprintln!("  Compression:   {compression:?}");
    eprintln!();

    // Setup ClickHouse container with optional tmpfs support
    // Set USE_TMPFS=true env var for zero disk I/O (data stored in RAM)
    let ch = get_or_create_benchmark_container(None).await;

    let mut all_results = Vec::new();

    // Get test schema
    let test_schema = arrow_tests::create_test_batch_generic(1).schema();

    // Run tests for each row count (outer loop)
    for total_rows in &row_counts {
        let header = format!("Testing {} rows", format_number(*total_rows));
        print_banner(&header, Some(80));
        eprintln!();

        // Run tests for each batch size
        for batch_size in &batch_sizes {
            eprintln!("  >> Batch size: {} <<", format_number(*batch_size));
            eprintln!();

            // Run tests for each worker count
            for workers in &worker_counts {
                let num_batches = (*total_rows).div_ceil(*batch_size);

                eprintln!(
                    "    --- {} workers ({} batches, {:.2} batches/worker) ---",
                    workers,
                    format_number(num_batches),
                    num_batches as f64 / *workers as f64
                );

                // Setup DataFusion context with ClickHouse catalog
                let ctx = SessionContext::new();

                let builder = ClickHouseBuilder::new(ch.get_native_url())
                    .configure_client(|c| configure_client(c, ch, compression))
                    .configure_pool(|p| p.max_size((*workers as u32).max(16))) // Ensure pool >= workers
                    .with_write_concurrency(*workers) // ⭐ Configure parallel writes
                    .build_catalog(&ctx, Some(CATALOG))
                    .await?;

                // Create schema and table for this test
                let table_name = format!("bench_{batch_size}_{workers}");
                let first_field_name = test_schema.field(0).name().clone();
                let clickhouse = builder
                    .with_schema(SCHEMA)
                    .await?
                    .with_new_table(
                        &table_name,
                        ClickHouseEngine::MergeTree,
                        Arc::clone(&test_schema),
                    )
                    .update_create_options(|opts| {
                        opts.with_order_by(std::slice::from_ref(&first_field_name))
                    })
                    .create(&ctx)
                    .await?
                    .build_schema(None, &ctx)
                    .await?;

                drop(clickhouse.build(&ctx).await?);

                let mut durations = Vec::with_capacity(iters);

                for iter in 1..=iters {
                    eprintln!("      Iteration {iter}/{iters}");

                    // Time the insert only
                    let start = Instant::now();
                    insert_via_datafusion(
                        &ctx,
                        &table_name,
                        *total_rows,
                        *batch_size,
                        &config,
                        iter, // Pass iteration number for unique temp table names
                    )
                    .await?;
                    let duration = start.elapsed();
                    durations.push(duration);

                    eprintln!("        Duration: {:.3}s", duration.as_secs_f64());

                    // Drop and recreate table for next iteration (not timed)
                    if iter < iters {
                        drop(
                            ctx.sql(&format!("DROP TABLE {CATALOG}.{SCHEMA}.{table_name}"))
                                .await?
                                .collect()
                                .await?,
                        );

                        // Recreate table with fresh builder
                        let rebuild_builder = ClickHouseBuilder::new(ch.get_native_url())
                            .configure_client(|c| configure_client(c, ch, compression))
                            .configure_pool(|p| p.max_size((*workers as u32).max(16)))
                            .with_write_concurrency(*workers)
                            .build_catalog(&ctx, Some(CATALOG))
                            .await?;

                        let clickhouse_rebuild = rebuild_builder
                            .with_schema(SCHEMA)
                            .await?
                            .with_new_table(
                                &table_name,
                                ClickHouseEngine::MergeTree,
                                Arc::clone(&test_schema),
                            )
                            .update_create_options(|opts| {
                                opts.with_order_by(std::slice::from_ref(&first_field_name))
                            })
                            .create(&ctx)
                            .await?
                            .build_schema(None, &ctx)
                            .await?;
                        drop(clickhouse_rebuild.build(&ctx).await?);
                    }
                }

                // Calculate stats - strip min/max and average the rest
                let mut sorted_durations = durations.clone();
                sorted_durations.sort();

                // Strip outliers if we have 3+ iterations
                let trimmed_durations: Vec<std::time::Duration> = if sorted_durations.len() >= 3 {
                    sorted_durations[1..sorted_durations.len() - 1].to_vec()
                } else {
                    sorted_durations.clone()
                };

                let avg_duration = trimmed_durations.iter().sum::<std::time::Duration>()
                    / trimmed_durations.len() as u32;
                let best_duration = sorted_durations[0]; // Minimum time

                let avg_rows_per_sec = *total_rows as f64 / avg_duration.as_secs_f64();
                let best_rows_per_sec = *total_rows as f64 / best_duration.as_secs_f64();

                eprintln!(
                    "      Avg: {:.3}s | {} rows/sec | Best: {:.3}s | {} rows/sec",
                    avg_duration.as_secs_f64(),
                    format_number(avg_rows_per_sec as usize),
                    best_duration.as_secs_f64(),
                    format_number(best_rows_per_sec as usize)
                );

                // Time the count(*) verification
                let count_start = Instant::now();
                let count_result = ctx
                    .sql(&format!("SELECT count(*) FROM {CATALOG}.{SCHEMA}.{table_name}"))
                    .await?
                    .collect()
                    .await?;
                let count_time = count_start.elapsed();

                let count: u64 = {
                    let col = count_result[0].column(0);
                    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                        arr.value(0)
                    } else if let Some(arr) =
                        col.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        arr.value(0) as u64
                    } else {
                        panic!("Unexpected count type: {:?}", col.data_type());
                    }
                };

                assert_eq!(count, *total_rows as u64, "Row count mismatch!");

                // Time the drop operation
                let drop_start = Instant::now();
                drop(
                    ctx.sql(&format!("DROP TABLE {CATALOG}.{SCHEMA}.{table_name}"))
                        .await?
                        .collect()
                        .await?,
                );
                let drop_time = drop_start.elapsed();
                eprintln!();

                // Store result
                all_results.push(TestResult {
                    workers: *workers,
                    batch_size: *batch_size,
                    rows: *total_rows,
                    avg_duration_secs: avg_duration.as_secs_f64(),
                    best_duration_secs: best_duration.as_secs_f64(),
                    avg_rows_per_sec,
                    best_rows_per_sec,
                    count_time_secs: count_time.as_secs_f64(),
                    drop_time_secs: drop_time.as_secs_f64(),
                });
            }
        }
    }

    // Sort results by avg throughput (highest first)
    all_results.sort_by(|a, b| b.avg_rows_per_sec.partial_cmp(&a.avg_rows_per_sec).unwrap());

    // Print summary table
    eprintln!();
    eprintln!("SUMMARY RESULTS (sorted by avg throughput)");
    eprintln!();
    let mut table = Table::new();
    let _ = table.load_preset(UTF8_FULL).set_header(vec![
        "Workers",
        "Batch Size",
        "Rows",
        "Avg Time (s)",
        "Best Time(s)",
        "Avg rows/sec",
        "Best rows/sec",
        "Best MB/sec",
        "Count(s)",
        "Drop (s)",
    ]);

    for result in &all_results {
        let _ = table.add_row(vec![
            result.workers.to_string(),
            format_number(result.batch_size),
            format_number(result.rows),
            format!("{:.3}", result.avg_duration_secs),
            format!("{:.3}", result.best_duration_secs),
            format_number(result.avg_rows_per_sec as usize),
            format_number(result.best_rows_per_sec as usize),
            format!("{:.2}", result.best_rows_per_sec * bytes_per_row / 1_000_000.0),
            format!("{:.3}", result.count_time_secs),
            format!("{:.3}", result.drop_time_secs),
        ]);
    }

    eprintln!("{table}");

    // Calculate and show average count and drop times
    let avg_count_time =
        all_results.iter().map(|r| r.count_time_secs).sum::<f64>() / all_results.len() as f64;
    let avg_drop_time =
        all_results.iter().map(|r| r.drop_time_secs).sum::<f64>() / all_results.len() as f64;
    eprintln!();
    eprintln!("Average count(*) time: {avg_count_time:.3}s");
    eprintln!("Average drop time:     {avg_drop_time:.3}s");

    // Show best result
    let best = all_results
        .iter()
        .max_by(|a, b| a.best_rows_per_sec.partial_cmp(&b.best_rows_per_sec).unwrap())
        .unwrap();
    eprintln!();
    eprintln!("🏆 BEST RESULT:");
    eprintln!(
        "   Configuration: {} workers, {} batch size, {} rows",
        best.workers,
        format_number(best.batch_size),
        format_number(best.rows)
    );
    eprintln!(
        "   Throughput:    {:.2}M rows/sec ({:.2} MB/sec)",
        best.best_rows_per_sec / 1_000_000.0,
        best.best_rows_per_sec * bytes_per_row / 1_000_000.0
    );
    eprintln!("   Duration:      {:.3}s", best.best_duration_secs);

    // Display test schema
    eprintln!();
    eprintln!("📊 Test Schema:");
    print_schema_summary(&config);
    eprintln!("  RecordBatch: {bytes_per_row:.2} bytes/row");

    // Warning for variable-length types
    if config.utf8 > 0 || config.binary > 0 {
        eprintln!();
        eprintln!(
            "⚠️  Variable-length types detected (UTF8={}, BINARY={})",
            config.utf8, config.binary
        );
        eprintln!("   Bytes/row may vary with batch size due to Arrow overhead.");
    }

    // Show total run time
    let total_elapsed = total_start.elapsed();
    eprintln!("\nTotal run time: {:.3}s", total_elapsed.as_secs_f64());
    let _ = ch.shutdown().await.ok();
    Ok(())
}

// ==================== Helper Functions ====================

/// Configure `ClickHouse` client for test container
fn configure_client(
    client: clickhouse_arrow::ClientBuilder,
    ch: &ClickHouseContainer,
    compression: clickhouse_arrow::CompressionMethod,
) -> clickhouse_arrow::ClientBuilder {
    client
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .with_compression(compression)
}

/// Parse compression method from string
fn parse_compression(s: &str) -> clickhouse_arrow::CompressionMethod {
    match s.to_uppercase().as_str() {
        "NONE" | "UNCOMPRESSED" => clickhouse_arrow::CompressionMethod::None,
        "LZ4" => clickhouse_arrow::CompressionMethod::LZ4,
        "ZSTD" => clickhouse_arrow::CompressionMethod::ZSTD,
        _ => {
            eprintln!("Unknown compression method '{s}', defaulting to LZ4");
            clickhouse_arrow::CompressionMethod::LZ4
        }
    }
}

/// Print a banner with optional custom width
fn print_banner(text: &str, width: Option<usize>) {
    let width = width.unwrap_or(72);
    let border = "=".repeat(width);
    eprintln!("{border}");
    eprintln!("{text:^width$}");
    eprintln!("{border}");
}

/// Parse a number with M/MM/K suffix support
fn parse_number(s: &str) -> Option<usize> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Check for suffix
    let (num_str, multiplier) = if s.ends_with("MM") || s.ends_with("mm") {
        (&s[..s.len() - 2], 1_000_000)
    } else if s.ends_with('M') || s.ends_with('m') || s.ends_with('K') || s.ends_with('k') {
        (&s[..s.len() - 1], 1_000)
    } else {
        (s, 1)
    };

    num_str.trim().parse::<usize>().ok().map(|n| n * multiplier)
}

/// Format a number with commas for readability
fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Calculate bytes per row for a given schema configuration
#[allow(clippy::cast_precision_loss)]
fn calculate_bytes_per_row(config: &arrow_tests::BatchConfig) -> f64 {
    // Create a 100K row sample batch
    let sample_batch = arrow_tests::create_test_batch_with_config_offset(100_000, config, Some(0));

    // Calculate approximate memory usage
    let mut total_bytes = 0;
    for array in sample_batch.columns() {
        total_bytes += array.get_buffer_memory_size();
    }

    total_bytes as f64 / 100_000.0
}

/// Print schema configuration
fn print_schema_config(config: &arrow_tests::BatchConfig) {
    eprintln!("Schema Configuration:");
    if config.int32 > 0 {
        eprintln!("  INT32:   {}", config.int32);
    }
    if config.int64 > 0 {
        eprintln!("  INT64:   {}", config.int64);
    }
    if config.uint64 > 0 {
        eprintln!("  UINT64:  {}", config.uint64);
    }
    if config.float64 > 0 {
        eprintln!("  FLOAT64: {}", config.float64);
    }
    if config.utf8 > 0 {
        eprintln!("  UTF8:    {}", config.utf8);
    }
    if config.binary > 0 {
        eprintln!("  BINARY:  {}", config.binary);
    }
}

/// Print a summary of the schema
fn print_schema_summary(config: &arrow_tests::BatchConfig) {
    let total =
        config.int32 + config.int64 + config.uint64 + config.float64 + config.utf8 + config.binary;
    eprintln!("  Total columns: {total}");
    if config.int32 > 0 {
        eprintln!("  INT32:   {}", config.int32);
    }
    if config.int64 > 0 {
        eprintln!("  INT64:   {}", config.int64);
    }
    if config.uint64 > 0 {
        eprintln!("  UINT64:  {}", config.uint64);
    }
    if config.float64 > 0 {
        eprintln!("  FLOAT64: {}", config.float64);
    }
    if config.utf8 > 0 {
        eprintln!("  UTF8:    {}", config.utf8);
    }
    if config.binary > 0 {
        eprintln!("  BINARY:  {}", config.binary);
    }
}

/// Insert data via `DataFusion` SQL INSERT statements
async fn insert_via_datafusion(
    ctx: &SessionContext,
    table_name: &str,
    total_rows: usize,
    batch_size: usize,
    config: &arrow_tests::BatchConfig,
    iteration: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_batches = total_rows.div_ceil(batch_size);

    for i in 0..num_batches {
        let offset = i * batch_size;
        let current_batch_size = std::cmp::min(batch_size, total_rows - offset);

        // Generate test batch using clickhouse-arrow's batch creator
        let batch = arrow_tests::create_test_batch_with_config_offset(
            current_batch_size,
            config,
            Some(offset),
        );

        // Register as temporary table with unique name per iteration
        let temp_table = format!("temp_iter{iteration}_batch_{i}");
        drop(ctx.register_batch(&temp_table, batch)?);

        // Execute INSERT via SQL
        drop(
            ctx.sql(&format!(
                "INSERT INTO {CATALOG}.{SCHEMA}.{table_name} SELECT * FROM {temp_table}"
            ))
            .await?
            .collect()
            .await?,
        );

        // Deregister temp table to free memory immediately
        drop(ctx.deregister_table(&temp_table)?);
    }

    Ok(())
}
