#![allow(unused_crate_dependencies)]
#![allow(clippy::doc_markdown)]
//! Federated queries joining Parquet files with ClickHouse tables
//!
//! Demonstrates both SQL and DataFrame API approaches:
//! - Registering Parquet files as DataFusion tables
//! - Creating ClickHouse tables
//! - Joining Parquet + ClickHouse via SQL
//! - Joining Parquet + ClickHouse via DataFrame API
//! - Diverse data types across sources
//!
//! See examples/README.md for Docker setup instructions.
//!
//! Run this example:
//! ```bash
//! cargo run --example 08_parquet_federation --features "test-utils federation"
//! ```

use std::fs;
use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::test_utils::get_or_create_container;
use clickhouse_datafusion::federation::FederatedContext;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::*;

// Catalog and schema (database) configuration
const CATALOG: &str = "ch_df_examples";
const SCHEMA: &str = "example_db"; // Note: In ClickHouse, schema is synonymous with database

fn gen_parq_file(
    schema: Arc<Schema>,
    data: Vec<ArrayRef>,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch = RecordBatch::try_new(Arc::clone(&schema), data)?;

    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    let _metadata = writer.close()?;
    Ok(())
}

fn gen_products_parq_file() -> Result<String, Box<dyn std::error::Error>> {
    let products_schema = Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));

    let products_batch: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![101, 102, 103])),
        Arc::new(StringArray::from(vec!["Laptop", "Mouse", "Keyboard"])),
        Arc::new(Float64Array::from(vec![1499.99, 19.99, 149.99])),
    ];

    let products_path = "/tmp/products.parquet";
    gen_parq_file(products_schema, products_batch, products_path)?;

    Ok(products_path.to_string())
}

fn gen_transactions_parq_file() -> Result<String, Box<dyn std::error::Error>> {
    let transactions_schema = Arc::new(Schema::new(vec![
        Field::new("transaction_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int32, false),
        Field::new("product_id", DataType::Int32, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("total", DataType::Float64, false),
    ]));

    let transactions_batch: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1001, 1002, 1003, 1004])),
        Arc::new(Int32Array::from(vec![1, 2, 1, 3])),
        Arc::new(Int32Array::from(vec![101, 102, 103, 101])),
        Arc::new(Int32Array::from(vec![2, 1, 3, 1])),
        Arc::new(Float64Array::from(vec![2999.98, 19.99, 449.97, 1499.99])),
    ];

    let transactions_path = "/tmp/transactions.parquet";
    gen_parq_file(transactions_schema, transactions_batch, transactions_path)?;

    Ok(transactions_path.to_string())
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔗 Parquet-ClickHouse Federation Example\n");

    // Setup ClickHouse container
    let ch = get_or_create_container(None).await;

    // Create Parquet files
    println!("1️⃣  Creating Parquet files...\n");

    let products_path = gen_products_parq_file()?;
    let products_tbl = "products";
    println!("   ✅ Created products.parquet");

    let transactions_path = gen_transactions_parq_file()?;
    let transactions_tbl = "transactions";
    println!("   ✅ Created transactions.parquet");

    // Setup DataFusion with federation
    println!("\n2️⃣  Setting up federation...\n");

    let ctx = SessionContext::new().federate();

    // Create ClickHouse table with users
    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| c.with_username(&ch.user).with_password(&ch.password))
        .build_catalog(&ctx, Some(CATALOG))
        .await?;

    ctx.register_parquet(products_tbl, &products_path, ParquetReadOptions::default()).await?;
    ctx.register_parquet(transactions_tbl, &transactions_path, ParquetReadOptions::default())
        .await?;
    println!("   ✅ Registered Parquet files");

    let users_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
    ]));

    let _clickhouse = clickhouse
        .with_schema(SCHEMA)
        .await?
        .with_new_table("users", ClickHouseEngine::MergeTree, users_schema)
        .update_create_options(|opts| opts.with_order_by(&["user_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    // Note: INSERT returns a Vec<RecordBatch> with a single "count" column (UInt64)
    // containing the number of rows inserted. You can use .map(|_| ())? to ignore it.
    let result = ctx
        .sql(&format!(
            "INSERT INTO {CATALOG}.{SCHEMA}.users
            (user_id, name, email)
        VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com')"
        ))
        .await?
        .collect()
        .await?;
    println!("   ✅ Created ClickHouse users table");
    print_batches(&result)?;

    // Run federated queries - SQL approach
    println!("\n3️⃣  Running federated queries (SQL)...\n");

    println!("   SQL Query 1: Transactions with user details (Parquet + ClickHouse)");
    let () = ctx
        .sql(&format!(
            "SELECT
                t.transaction_id,
                u.name,
                u.email,
                t.product_id,
                t.total
            FROM {transactions_tbl} t
            JOIN {CATALOG}.{SCHEMA}.users u ON t.user_id = u.user_id
            ORDER BY t.transaction_id"
        ))
        .await?
        .show()
        .await?;

    println!("\n   SQL Query 2: Complete order details (3-way join)");
    let () = ctx
        .sql(&format!(
            "SELECT
                u.name,
                p.name as product,
                t.quantity,
                p.price,
                t.total
            FROM {transactions_tbl} t
            JOIN {CATALOG}.{SCHEMA}.users u ON t.user_id = u.user_id
            JOIN {products_tbl} p ON t.product_id = p.product_id
            ORDER BY t.transaction_id"
        ))
        .await?
        .show()
        .await?;

    // Run federated queries - DataFrame API approach
    println!("\n4️⃣  Running federated queries (DataFrame API)...\n");

    println!("   DataFrame Query 1: Same join using DataFrame API");
    let transactions_df = ctx.table("transactions").await?;
    let users_df = ctx.table(&format!("{CATALOG}.{SCHEMA}.users")).await?;

    let joined = transactions_df
        .clone()
        .join(users_df, JoinType::Inner, &["user_id"], &["user_id"], None)?
        .select(vec![
            col("transaction_id"),
            col("name"),
            col("email"),
            col("product_id"),
            col("total"),
        ])?
        .sort(vec![col("transaction_id").sort(true, false)])?;

    joined.show().await?;

    println!("\n   DataFrame Query 2: Aggregation with GROUP BY");
    let aggregated = transactions_df.aggregate(vec![], vec![
        count(col("transaction_id")).alias("total_orders"),
        sum(col("total")).alias("total_revenue"),
        avg(col("total")).alias("avg_order"),
    ])?;

    aggregated.show().await?;

    // Cleanup
    fs::remove_file(products_path)?;
    fs::remove_file(transactions_path)?;
    println!("\n✅ Example completed successfully!");
    println!("\n💡 Key takeaways:");
    println!("   • Both SQL and DataFrame API support federated queries");
    println!("   • SQL: More concise for complex joins and familiar syntax");
    println!("   • DataFrame API: More type-safe and composable for Rust code");
    println!("   • Federation works seamlessly across Parquet and ClickHouse");
    println!("   • Use .federate() on SessionContext to enable federation");

    let _ = ch.shutdown().await.ok();
    Ok(())
}
