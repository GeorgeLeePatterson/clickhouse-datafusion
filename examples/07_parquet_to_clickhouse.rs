#![allow(unused_crate_dependencies)]
#![allow(clippy::doc_markdown)]
//! Reading Parquet files and writing to ClickHouse
//!
//! Demonstrates two approaches:
//! 1. SQL: INSERT INTO ... SELECT
//! 2. DataFrame API: df.write_table()
//!
//! Works with diverse data types:
//! - Int32, Int64, Float32, Float64
//! - Utf8 (strings), Boolean
//! - Date32, Decimal128
//! - List (arrays)
//!
//! See examples/README.md for Docker setup instructions.
//!
//! Run this example:
//! ```bash
//! cargo run --example 07_parquet_to_clickhouse --features test-utils
//! ```

use std::fs;
use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::test_utils::get_or_create_container;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::*;

// Catalog and schema (database) configuration
const CATALOG: &str = "ch_df_examples";
const SCHEMA: &str = "example_db"; // Note: In ClickHouse, schema is synonymous with database

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("📦 Parquet → ClickHouse Example\n");

    // Setup ClickHouse container
    let ch = get_or_create_container(None).await;

    let parquet_path = "/tmp/sample_data.parquet";

    // Create Parquet file with diverse data types
    println!("1️⃣  Creating Parquet file with varied data types...\n");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("signup_date", DataType::Date32, false),
        Field::new("balance", DataType::Decimal128(18, 2), false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ]));

    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(Float64Array::from(vec![100.50, 250.75, 500.25])),
        Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        Arc::new(BooleanArray::from(vec![true, false, true])),
        Arc::new(Date32Array::from(vec![19000, 19100, 19200])),
        Arc::new(
            Decimal128Array::from(vec![100_050, 250_075, 500_025])
                .with_precision_and_scale(18, 2)?,
        ),
        Arc::new({
            let mut builder = ListBuilder::new(StringBuilder::new());
            builder.values().append_value("premium");
            builder.append(true);
            builder.values().append_value("standard");
            builder.append(true);
            builder.append(true); // empty list for Charlie
            builder.finish()
        }),
    ])?;

    let file = fs::File::create(parquet_path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
    writer.write(&batch)?;
    let _metadata = writer.close()?;
    println!("   ✅ Created Parquet file with {} rows", batch.num_rows());

    // Setup DataFusion and ClickHouse
    println!("\n2️⃣  Connecting to ClickHouse...\n");

    let ctx = SessionContext::new();
    ctx.register_parquet("parquet_data", parquet_path, ParquetReadOptions::default()).await?;

    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| c.with_username(&ch.user).with_password(&ch.password))
        .build_catalog(&ctx, Some(CATALOG))
        .await?;

    // Create ClickHouse table
    let _catalog = clickhouse
        .with_schema(SCHEMA)
        .await?
        .with_new_table("users", ClickHouseEngine::MergeTree, Arc::clone(&schema))
        .update_create_options(|opts| opts.with_order_by(&["id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("   ✅ Created ClickHouse table");

    // Method 1: Copy data using SQL INSERT
    println!("\n3️⃣  Method 1: Copying via SQL INSERT...\n");

    // Note: INSERT returns a Vec<RecordBatch> with a single "count" column (UInt64)
    // containing the number of rows inserted. You can use .map(|_| ())? to ignore it.
    let result = ctx
        .sql(&format!(
            "
            INSERT INTO {CATALOG}.{SCHEMA}.users
            SELECT * FROM parquet_data
            "
        ))
        .await?
        .collect()
        .await?;

    println!("   ✅ Data copied via SQL");
    print_batches(&result)?;

    // Note: DataFrame API approach
    println!("\n4️⃣  DataFrame API approach...\n");
    println!("   💡 DataFrame API df.write_table() also works for Parquet → ClickHouse");
    println!("   💡 May require schema coercion (.with_schema_coercion(true))");
    println!("   💡 See Example 08 for full DataFrame API demonstration with federation");

    // Verify and query
    println!("\n5️⃣  Querying ClickHouse table...\n");

    println!("   All records:");
    let () = ctx
        .sql(&format!(
            "SELECT *
            FROM {CATALOG}.{SCHEMA}.users
            ORDER BY id"
        ))
        .await?
        .show()
        .await?;

    println!("\n   Aggregations:");
    let () = ctx
        .sql(&format!(
            "SELECT
                COUNT(*) as total,
                SUM(amount) as total_amount
            FROM {CATALOG}.{SCHEMA}.users"
        ))
        .await?
        .show()
        .await?;

    // Cleanup
    fs::remove_file(parquet_path)?;
    println!("\n✅ Example completed successfully!");
    println!("\n💡 Key takeaways:");
    println!("   • SQL INSERT INTO ... SELECT is the most reliable way");
    println!("   • DataFrame API df.write_table() works too (see Example 08 for federation demo)");
    println!(
        "   • Parquet supports diverse types: Int, Float, String, Boolean, Date, Decimal, List"
    );
    println!("   • Schema coercion (.with_schema_coercion(true)) helps with type compatibility");

    let _ = ch.shutdown().await.ok();
    Ok(())
}
