#![allow(unused_crate_dependencies)]
//! Summary example demonstrating clickhouse-datafusion integration
//!
//! This comprehensive example shows:
//! - Setting up `ClickHouse` with `DataFusion` using testcontainers
//! - Creating tables and inserting data
//! - Basic queries with `ClickHouse` UDFs (string, array, lambda functions)
//! - Federation with in-memory tables
//! - Window functions and complex analytics
//!
//! Run with: cargo run --example `00_summary` --features test-utils

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

// Catalog and schema (database) configuration
const CATALOG: &str = "ch_df_examples";
const SCHEMA: &str = "example_db"; // Note: In ClickHouse, schema is synonymous with database

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
        .with_schema(SCHEMA)
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
    // Note: INSERT returns a Vec<RecordBatch> with a single "count" column (UInt64)
    // containing the number of rows inserted. You can use .map(|_| ())? to ignore it.
    let results = ctx
        .sql(&format!(
            "INSERT INTO {CATALOG}.{SCHEMA}.users
                (id, name, email)
            VALUES
                (1, 'Alice', 'alice@example.com'),
                (2, 'Bob', 'bob@example.com'),
                (3, 'Charlie', 'charlie@example.com')"
        ))
        .await?
        .collect()
        .await?;
    info!("Inserted into users:");
    print_batches(&results)?;

    let results = ctx
        .sql(&format!(
            "INSERT INTO {CATALOG}.{SCHEMA}.events
                (user_id, event_type, tags)
            VALUES
                (1, 'login', make_array('web', 'login')),
                (2, 'purchase', make_array('mobile', 'purchase')),
                (3, 'browse', make_array('web', 'browse', 'search'))"
        ))
        .await?
        .collect()
        .await?;
    info!("Inserted into events:");
    print_batches(&results)?;

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

    // Build ClickHouse integration using the correct pattern
    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| configure_client(c, ch))
        .build_catalog(&ctx, Some(CATALOG))
        .await?;

    let _clickhouse = build_clickhouse_schema(clickhouse, &ctx).await?;

    // Run example queries
    info!("🔍 Running example queries...\n");

    // 1. Basic query
    println!("1. Basic ClickHouse query:");
    let df = ctx.sql(&format!("SELECT * FROM {CATALOG}.{SCHEMA}.users")).await?;
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
            FROM {CATALOG}.{SCHEMA}.users"
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
            FROM {CATALOG}.{SCHEMA}.events"
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
            FROM {CATALOG}.{SCHEMA}.events"
        ))
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;

    // 5. Federation - Join ClickHouse with in-memory table
    // note `user_segments` table was created in build_clickhouse_schema()
    println!("5. Federation - Join ClickHouse with in-memory data:");
    let df = ctx
        .sql(&format!(
            "SELECT u.id,
                    u.name,
                    u.email,
                    s.segment,
                    clickhouse(upper(u.name), 'Utf8') as upper_name
            FROM {CATALOG}.{SCHEMA}.users u
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
            FROM {CATALOG}.{SCHEMA}.events e"
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

    let _ = ch.shutdown().await.ok();
    Ok(())
}
