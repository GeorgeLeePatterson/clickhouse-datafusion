#![allow(unused_crate_dependencies)]
// Federation Example - Joining ClickHouse with Local Data
//
// This example demonstrates federating ClickHouse tables with local in-memory tables.
//
// See examples/README.md for Docker setup instructions.
//
// Run this example:
// ```bash
// cargo run --example 05_federation --features "test-utils federation"
// ```

// Gate entire example on federation feature
#[cfg(feature = "federation")]
mod federated_example {
    use std::sync::Arc;

    use clickhouse_arrow::prelude::ClickHouseEngine;
    use clickhouse_arrow::test_utils::get_or_create_container;
    use clickhouse_datafusion::federation::FederatedContext;
    use clickhouse_datafusion::prelude::*;
    use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::print_batches;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::*;

    // Catalog and schema (database) configuration
    const CATALOG: &str = "ch_df_examples";
    const SCHEMA: &str = "example_db"; // Note: In ClickHouse, schema is synonymous with database

    pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 ClickHouse-DataFusion: Federation Example\n");

        // Setup ClickHouse container
        let ch = get_or_create_container(None).await;

        // Enable federation
        let ctx = SessionContext::new().federate();

        // Connect to ClickHouse
        let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| c.with_username(&ch.user).with_password(&ch.password))
            .build_catalog(&ctx, Some(CATALOG))
            .await?;

        // Create ClickHouse table with sales data
        let sales_schema = Arc::new(Schema::new(vec![
            Field::new("product_id", DataType::Int32, false),
            Field::new("quantity", DataType::Int32, false),
            Field::new("revenue", DataType::Float64, false),
        ]));

        let _clickhouse = clickhouse
            .with_schema(SCHEMA)
            .await?
            .with_new_table("sales", ClickHouseEngine::MergeTree, Arc::clone(&sales_schema))
            .update_create_options(|opts| opts.with_order_by(&["product_id".to_string()]))
            .create(&ctx)
            .await?
            .build(&ctx)
            .await?;

        println!("✓ Created ClickHouse table");

        // Insert sales data into ClickHouse
        // Note: INSERT returns a Vec<RecordBatch> with a single "count" column (UInt64)
        // containing the number of rows inserted. You can use .map(|_| ())? to ignore it.
        let result = ctx
            .sql(&format!(
                "INSERT INTO {CATALOG}.{SCHEMA}.sales
            (product_id, quantity, revenue)
        VALUES
            (1, 100, 5000.0),
            (2, 150, 7500.0),
            (3, 80, 4000.0),
            (4, 200, 10000.0)"
            ))
            .await?
            .collect()
            .await?;

        println!("✓ Inserted data into ClickHouse");
        print_batches(&result)?;

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Create local in-memory table with product information
        let product_schema = Arc::new(Schema::new(vec![
            Field::new("product_id", DataType::Int32, false),
            Field::new("product_name", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
        ]));

        let product_batch = RecordBatch::try_new(Arc::clone(&product_schema), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["Widget A", "Widget B", "Gadget X", "Gadget Y"])),
            Arc::new(StringArray::from(vec!["Widgets", "Widgets", "Gadgets", "Gadgets"])),
        ])?;

        let mem_table = MemTable::try_new(product_schema, vec![vec![product_batch]])?;
        let _prev_table = ctx.register_table("products", Arc::new(mem_table))?;

        println!("✓ Created local in-memory table\n");

        // Example 1: Federated JOIN - ClickHouse sales + local product info
        println!("Example 1: Join ClickHouse sales data with local product catalog\n");
        let df = ctx
            .sql(&format!(
                "SELECT
                p.product_name,
                p.category,
                s.quantity,
                s.revenue
            FROM {CATALOG}.{SCHEMA}.sales s
            JOIN products p ON s.product_id = p.product_id
            ORDER BY s.revenue DESC"
            ))
            .await?;

        print_batches(&df.collect().await?)?;

        // Example 2: Aggregated federated query
        println!("\nExample 2: Total revenue by category (federated aggregation)\n");
        let df = ctx
            .sql(&format!(
                "SELECT
                p.category,
                SUM(s.quantity) as total_quantity,
                SUM(s.revenue) as total_revenue,
                AVG(s.revenue) as avg_revenue
            FROM {CATALOG}.{SCHEMA}.sales s
            JOIN products p ON s.product_id = p.product_id
            GROUP BY p.category
            ORDER BY total_revenue DESC"
            ))
            .await?;

        print_batches(&df.collect().await?)?;

        println!("\n✅ Example completed successfully!");
        println!(
            "\n💡 This example showed how to join ClickHouse tables with local in-memory data."
        );
        println!(
            "   You can also federate with Parquet files, CSV files, and other DataFusion sources!"
        );

        let _ = ch.shutdown().await.ok();
        Ok(())
    }
}

#[cfg(feature = "federation")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { federated_example::run().await }

#[cfg(not(feature = "federation"))]
fn main() {
    eprintln!("❌ This example requires the 'federation' feature.");
    eprintln!(
        "   Run with: cargo run --example 05_federation --features \"test-utils federation\""
    );
    std::process::exit(1);
}
