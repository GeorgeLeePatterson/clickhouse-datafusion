#![allow(unused_crate_dependencies)]
//! Basic example demonstrating clickhouse-datafusion integration
//!
//! This example shows:
//! - Setting up `ClickHouse` with `DataFusion`
//! - Creating tables and inserting data
//! - Basic queries with `ClickHouse` UDFs
//! - Federation with in-memory tables
//!
//! Run with: cargo run --example basic --features test-utils

use std::sync::Arc;

use clickhouse_arrow::test_utils::{ClickHouseContainer, get_or_create_container};
#[cfg(feature = "federation")]
use clickhouse_datafusion::federation::FederatedContext;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use tokio::main;
use tracing::{Level, info};

fn configure_client(
    client: clickhouse_arrow::ClientBuilder,
    ch: &ClickHouseContainer,
) -> clickhouse_arrow::ClientBuilder {
    client
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .with_compression(clickhouse_arrow::CompressionMethod::LZ4)
        // Already the default, useful to locate the default options used
        .with_arrow_options(default_arrow_options())
}

fn create_clickhouse_session_context() -> ClickHouseSessionContext {
    // Create DataFusion context
    let ctx = SessionContext::new();

    // Enable federation if available
    #[cfg(feature = "federation")]
    let ctx = ctx.federate();

    // Enable ClickHouse UDF support
    let ctx = ClickHouseSessionContext::from(ctx);
    info!("✅ Enhanced DataFusion context created");
    ctx
}

async fn build_clickhouse_schema(
    builder: ClickHouseCatalogBuilder,
    db: &str,
    ctx: &ClickHouseSessionContext,
) -> Result<ClickHouseCatalogBuilder> {
    // Define schemas for our tables
    let users_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
    ]));

    let events_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new_list("tags", Field::new_list_field(DataType::Utf8, false), false),
    ]));

    // Create tables using the catalog builder pattern
    let clickhouse = builder
        .with_schema(db)
        .await?
        // Create users table
        .with_new_table(
            "users",
            clickhouse_arrow::prelude::ClickHouseEngine::MergeTree,
            Arc::clone(&users_schema),
        )
        .update_create_options(|opts| opts.with_order_by(&["id".into()]))
        .create(ctx)
        .await?
        // Create events table
        .with_new_table(
            "events",
            clickhouse_arrow::prelude::ClickHouseEngine::MergeTree,
            Arc::clone(&events_schema),
        )
        .update_create_options(|opts| opts.with_order_by(&["user_id".into()]))
        .create(ctx)
        .await?;

    info!("✅ Created tables: users and events");

    // Insert sample data using SQL (the correct approach)
    let _results = ctx
        .sql(&format!(
            "INSERT INTO clickhouse.{db}.users (id, name, email)
                VALUES
                    (1, 'Alice', 'alice@example.com'),
                    (2, 'Bob', 'bob@example.com'),
                    (3, 'Charlie', 'charlie@example.com')"
        ))
        .await?
        .collect()
        .await?;

    let _results = ctx
        .sql(&format!(
            "INSERT INTO clickhouse.{db}.events (user_id, event_type, tags)
                VALUES
                    (1, 'login', make_array('web', 'login')),
                    (2, 'purchase', make_array('mobile', 'purchase')),
                    (3, 'browse', make_array('web', 'browse', 'search'))"
        ))
        .await?
        .collect()
        .await?;

    info!("✅ Inserted sample data");

    // Build the schema to make tables available
    let _clickhouse_catalog = clickhouse.build(ctx).await?;
    info!("✅ Built ClickHouse catalog");

    // Create an in-memory table for federation demonstration
    let segments_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("segment", DataType::Utf8, false),
    ]));

    let segments_data = RecordBatch::try_new(Arc::clone(&segments_schema), vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["premium", "standard", "premium"])),
    ])?;

    let mem_table = Arc::new(MemTable::try_new(segments_schema, vec![vec![segments_data]])?);
    drop(ctx.register_table("user_segments", mem_table)?);
    info!("✅ Registered in-memory table: user_segments");

    Ok(clickhouse)
}

#[main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    info!("🚀 Starting clickhouse-datafusion basic example");

    // Setup ClickHouse container
    let ch = get_or_create_container(None).await;
    info!("✅ ClickHouse container ready at: {}", ch.get_native_url());

    // Create ClickHouse SessionContext
    let ctx = create_clickhouse_session_context();

    // Test database
    let db = "example_db";

    // Build ClickHouse integration using the correct pattern
    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| configure_client(c, ch))
        .build_catalog(&ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
        .await?;

    let _clickhouse = build_clickhouse_schema(clickhouse, db, &ctx).await?;

    // Run example queries
    info!("🔍 Running example queries...\n");

    // 1. Basic query
    println!("1. Basic ClickHouse query:");
    let df = ctx.sql(&format!("SELECT * FROM clickhouse.{db}.users")).await?;
    let results = df.collect().await?;
    print_batches(&results)?;

    // 2. ClickHouse UDF example
    println!("2. ClickHouse UDF - String functions:");
    let df = ctx
        .sql(&format!(
            "SELECT id,
                    name,
                    clickhouse(upper(name), 'Utf8') as upper_name,
                    clickhouse(length(email), 'UInt64') as email_length
            FROM clickhouse.{db}.users"
        ))
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;

    // 3. Array functions with ClickHouse
    println!("3. ClickHouse array functions:");
    let df = ctx
        .sql(&format!(
            "SELECT user_id,
                    event_type,
                    tags,
                    clickhouse(`arrayJoin`(tags), 'Utf8') as individual_tag
            FROM clickhouse.{db}.events"
        ))
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;

    // 4. Lambda functions
    println!("4. ClickHouse lambda functions:");
    let df = ctx
        .sql(&format!(
            "SELECT user_id,
                    tags,
                    clickhouse(
                        `arrayMap`($x, concat($x, '_processed'), tags), 'List(Utf8)'
                    ) as processed_tags,
                    clickhouse(`arrayFilter`($x, length($x) > 3, tags), 'List(Utf8)') as long_tags
            FROM clickhouse.{db}.events"
        ))
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;

    // 5. Federation - Join ClickHouse with in-memory table
    println!("5. Federation - Join ClickHouse with in-memory data:");
    let df = ctx
        .sql(&format!(
            "SELECT u.id,
                    u.name,
                    u.email,
                    s.segment,
                    clickhouse(upper(u.name), 'Utf8') as upper_name
            FROM clickhouse.{db}.users u
            JOIN user_segments s ON u.id = s.user_id
            ORDER BY u.id"
        ))
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;

    // 6. Complex analytics with window functions
    println!("6. Complex analytics - Window functions with ClickHouse UDFs:");
    let df = ctx
        .sql(&format!(
            "SELECT e.user_id,
                    e.event_type,
                    clickhouse(upper(e.event_type), 'Utf8') as upper_event,
                    COUNT(*) OVER (PARTITION BY e.user_id) as user_event_count,
                    ROW_NUMBER() OVER (ORDER BY e.user_id) as row_num
            FROM clickhouse.{db}.events e"
        ))
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;

    info!("🎉 Example completed successfully!");
    info!("💡 This demonstrates:");
    info!("   - ClickHouse integration with DataFusion");
    info!("   - ClickHouse UDF usage (string, array, lambda functions)");
    info!("   - Federation (joining ClickHouse with other data sources)");
    info!("   - Complex analytics (window functions, CTEs)");

    Ok(())
}
