use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use clickhouse_arrow::{ClientBuilder, CompressionMethod, CreateOptions};
use clickhouse_datafusion::{
    ClickHouseBuilder, ClickHouseCatalogBuilder, DEFAULT_CLICKHOUSE_CATALOG, default_arrow_options,
};
use datafusion::arrow;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::kernels::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use tracing::{debug, error, info};

// Configure arrow/clickhouse client
#[allow(unused)]
pub(crate) fn configure_client(client: ClientBuilder, ch: &ClickHouseContainer) -> ClientBuilder {
    client
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .with_compression(CompressionMethod::LZ4)
        .with_arrow_options(default_arrow_options())
}

// Create catalog builder
#[allow(unused)]
pub(crate) async fn create_builder(
    ctx: &SessionContext,
    ch: &ClickHouseContainer,
) -> Result<ClickHouseCatalogBuilder> {
    ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| configure_client(c, ch))
        .build_catalog(ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
        .await
}

// Create catalog builder
#[allow(unused)]
pub(crate) async fn create_builder_with_coercion(
    ctx: &SessionContext,
    ch: &ClickHouseContainer,
) -> Result<ClickHouseCatalogBuilder> {
    ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| configure_client(c, ch))
        .with_coercion(true)
        .build_catalog(ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
        .await
}

#[allow(unused)]
pub(crate) async fn setup_test_tables(
    builder: ClickHouseCatalogBuilder,
    db: &str,
    ctx: &SessionContext,
) -> Result<ClickHouseCatalogBuilder> {
    // -----------------------------
    // Add table
    let schema_people = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // -----------------------------
    // Add second table
    let schema_knicknames = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("other_name", DataType::Utf8, false),
    ]));

    // -----------------------------
    // Add third table
    let schema_people2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new_list("names", Field::new_list_field(DataType::Utf8, false), false),
    ]));

    // Create all tables, registering any additional, then build the final context
    let clickhouse = builder
        // Set schema (database)
        .with_schema(db)
        .await?
        // Create People table
        .with_new_table("people", ClickHouseEngine::MergeTree, schema_people)
        .update_create_options(|opts: CreateOptions| {
            opts.with_order_by(&["id".into(), "name".into()])
                .with_primary_keys(&["id".into()])
                .with_defaults(vec![("name".into(), "'Unknown'".into())].into_iter())
        })
        .create(ctx)
        .await?
        // Create Knicknames table
        .with_new_table_and_options(
            "knicknames",
            schema_knicknames,
            CreateOptions::new(ClickHouseEngine::MergeTree.to_string())
                .with_order_by(&["id".into(), "other_name".into()])
                .with_primary_keys(&["id".into()]),
        )
        .create(ctx)
        .await?
        // Create People2 table
        .with_new_table("people2", ClickHouseEngine::MergeTree, Arc::clone(&schema_people2))
        .update_create_options(|opts| opts.with_order_by(&["id".into()]))
        .create(ctx)
        .await
        .expect("table creation");

    // Build schema
    let clickhouse = clickhouse
        .build_schema(/* Stay with same schema */ None, ctx)
        .await
        .inspect_err(|e| error!("Failed building schema: {e:?}"))?;

    debug!("Builder created");
    Ok(clickhouse)
}

#[allow(unused)]
pub(crate) async fn insert_test_data(
    clickhouse: ClickHouseCatalogBuilder,
    db: &str,
    ctx: &SessionContext,
) -> Result<ClickHouseCatalogBuilder> {
    // -----------------------------
    // Insert data using SQL
    let rows = ctx
        .sql(&format!(
            "INSERT INTO clickhouse.{db}.people (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
        ))
        .await?
        .collect()
        .await?
        .first()
        .map(|b| cast(b.column(0), &DataType::Int32).unwrap().as_primitive::<Int32Type>().value(0))
        .expect("Expected a row count");
    assert_eq!(rows, 2, "Expected 2 rows inserted");
    info!(">>> Inserted data into table `{db}.people`");

    // -----------------------------
    // Insert data using SQL
    let rows = ctx
        .sql(&format!(
            "
                INSERT INTO clickhouse.{db}.people2 (id, name, names)
                VALUES
                    (1, 'Bob', make_array('Buddha', 'Zugus', 'Lulu', 'Kitty', 'Mitty')),
                    (2, 'Alice', make_array('Jazz', 'Kaya', 'Vienna', 'Susie', 'Georgie')),
                    (3, 'Charlie', make_array('Susana', 'Adrienne', 'Blayke'))
                "
        ))
        .await?
        .collect()
        .await?
        .first()
        .map(|b| cast(b.column(0), &DataType::Int32).unwrap().as_primitive::<Int32Type>().value(0))
        .expect("Expected a row count");
    assert_eq!(rows, 3, "Expected 3 rows inserted");
    info!(">>> Inserted data into table `{db}.people2`");

    // Have to wait since the setting `select_sequential_consistency` can't be passed to clickhouse
    debug!("Until datafusion patches SETTINGS in Query structs, we wait: 2s");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(clickhouse)
}

// Add in-memory table
#[allow(unused)]
pub(crate) fn add_memory_table_and_data(ctx: &SessionContext) -> Result<()> {
    let mem_schema = Arc::new(Schema::new(vec![Field::new("event_id", DataType::Int32, false)]));
    let mem_table =
        Arc::new(datafusion::datasource::MemTable::try_new(Arc::clone(&mem_schema), vec![vec![
            arrow::record_batch::RecordBatch::try_new(mem_schema, vec![Arc::new(
                arrow::array::Int32Array::from(vec![1, 2]),
            )])?,
        ]])?);
    let mem_catalog = Arc::new(MemoryCatalogProvider::new());
    let mem_schema = Arc::new(MemorySchemaProvider::new());
    drop(mem_schema.register_table("mem_events".into(), mem_table)?);
    drop(mem_catalog.register_schema("internal", mem_schema)?);
    drop(ctx.register_catalog("memory", mem_catalog));
    Ok(())
}
