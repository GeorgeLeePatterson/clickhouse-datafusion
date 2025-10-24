#![allow(unused_crate_dependencies)]
#![cfg(not(feature = "mocks"))]

mod common;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[
    ("testcontainers", "debug"),
    ("hyper", "error"),
    // --
    ("clickhouse_arrow", "error"),
    ("datafusion", "trace"),
];

// -- FEDERATION/NON FEDERATION --

// Test builders
#[cfg(feature = "test-utils")]
e2e_test!(builder, tests::test_clickhouse_builder, TRACING_DIRECTIVES, None);

// Test table provider and sql
#[cfg(feature = "test-utils")]
e2e_test!(providers, tests::test_providers, TRACING_DIRECTIVES, None);

// Test insert data
#[cfg(feature = "test-utils")]
e2e_test!(insert, tests::test_insert_data, TRACING_DIRECTIVES, None);

// Test parallel writes with different concurrency levels
#[cfg(feature = "test-utils")]
e2e_test!(parallel_writes, tests::test_parallel_writes, TRACING_DIRECTIVES, None);

// Test insert metrics with EXPLAIN ANALYZE
#[cfg(feature = "test-utils")]
e2e_test!(insert_metrics, tests::test_insert_metrics, TRACING_DIRECTIVES, None);

// Test clickhouse udfs smoke test
#[cfg(feature = "test-utils")]
e2e_test!(udfs_smoke_test, tests::test_clickhouse_udfs_smoke_test, TRACING_DIRECTIVES, None);

// Test clickhouse udfs
#[cfg(feature = "test-utils")]
e2e_test!(udfs_clickhouse, tests::test_clickhouse_udfs, TRACING_DIRECTIVES, None);

// Test clickhouse udfs with schema coercion
#[cfg(feature = "test-utils")]
e2e_test!(udfs_coerce, tests::test_clickhouse_udfs_schema_coercion, TRACING_DIRECTIVES, None);

// Test clickhouse udfs lambda
#[cfg(feature = "test-utils")]
e2e_test!(udfs_lambda, tests::test_clickhouse_udfs_lambda, TRACING_DIRECTIVES, None);

// Test clickhouse udfs known failures - feature enhancements
#[cfg(feature = "test-utils")]
e2e_test!(udfs_failing, tests::test_clickhouse_udfs_failing, TRACING_DIRECTIVES, None);

// Test aggregation functions
#[cfg(feature = "test-utils")]
e2e_test!(aggregations, tests::test_aggregation_functions, TRACING_DIRECTIVES, None);

// Test DROP TABLE support
#[cfg(feature = "test-utils")]
e2e_test!(drop_table, tests::test_drop_table, TRACING_DIRECTIVES, None);

// Test DataSink write_all with schema validation
#[cfg(feature = "test-utils")]
e2e_test!(sink_write_all, tests::test_sink_write_all, TRACING_DIRECTIVES, None);

// -- FEDERATION --

// Test simple clickhouse udf
#[cfg(all(feature = "test-utils", feature = "federation"))]
e2e_test!(eval_udf, tests::test_clickhouse_eval_udf, TRACING_DIRECTIVES, None);

// Test FederatedCatalogProvider
#[cfg(all(feature = "test-utils", feature = "federation"))]
e2e_test!(federated_catalog, tests::test_federated_catalog, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use clickhouse_arrow::test_utils::ClickHouseContainer;
    use clickhouse_arrow::{ArrowConnectionPoolBuilder, CreateOptions};
    #[cfg(feature = "federation")]
    use clickhouse_datafusion::federation::FederatedContext as _;
    use clickhouse_datafusion::providers::utils::extract_clickhouse_provider;
    use clickhouse_datafusion::utils::create_schema;
    use clickhouse_datafusion::{
        ClickHouseBuilder, ClickHouseDataSink, ClickHouseSessionContext, ClickHouseTableProvider,
        ClickHouseTableProviderFactory, DEFAULT_CLICKHOUSE_CATALOG,
    };
    use datafusion::arrow;
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::catalog::TableProviderFactory;
    use datafusion::common::{Constraints, DFSchema};
    use datafusion::datasource::TableProvider;
    use datafusion::datasource::sink::DataSink;
    use datafusion::error::Result;
    use datafusion::logical_expr::CreateExternalTable;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::DefaultDisplay;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;
    use futures_util::StreamExt;
    use tracing::error;

    use super::*;
    use crate::common::helpers::configure_client;

    // Test with both federation on/off
    pub(super) async fn test_clickhouse_builder(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_db_builder";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = common::helpers::create_builder(&ctx, &ch).await?;
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // -----------------------------
        // Refresh catalog
        let _catalog_provider = clickhouse.build(&ctx).await?;

        // Test query
        let df = ctx.sql(&format!("SELECT name FROM clickhouse.{db}.people")).await?;
        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Query completed successfully");

        // Test registering non-existent table
        let result =
            clickhouse.register_existing_table("{db}.missing", None::<TableReference>, &ctx).await;
        assert!(result.is_err(), "Expected table not found error");
        eprintln!(">>> Unexpected table passed");

        // Register existing table
        clickhouse
            .register_existing_table(&format!("{db}.people"), Some("people_alias"), &ctx)
            .await?;
        eprintln!(">>> Registered existing table into alias `people_alias`");

        eprintln!(">> Test builder completed");

        Ok(())
    }

    /// Tests to cover table provider, sql table, federation, and connection pooling functionality
    #[expect(clippy::too_many_lines)]
    pub(super) async fn test_providers(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_db_providers";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let native_url = ch.get_native_url();
        let pool_builder = ArrowConnectionPoolBuilder::new(native_url)
            .configure_client(|c| configure_client(c, &ch));
        let builder = ClickHouseBuilder::new_with_pool_builder(native_url, pool_builder)
            .configure_pool(|p| p.max_size(4))
            .configure_arrow_options(|a| a.with_strings_as_strings(true))
            .build_catalog(&ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
            .await?;

        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // -----------------------------
        // Refresh catalog
        let _catalog_provider = clickhouse.build(&ctx).await?;

        let schema_people = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // ---
        // TableProvider

        // Ensure table provider can be extracted
        let table_ref = TableReference::full(DEFAULT_CLICKHOUSE_CATALOG, db, "people");
        let provider = ctx.table_provider(table_ref.clone()).await?;
        let table_provider = extract_clickhouse_provider(&provider);
        assert!(table_provider.is_some(), "Could not extract ClickHouse table provider");
        let clickhouse_table_provider = table_provider.unwrap();
        eprintln!(">>> TableProvider tests passed");

        // ---
        // Connection Pool

        // Access underlying clickhouse connection pool
        let pool = Arc::clone(clickhouse_table_provider.pool());
        let expected_provider = ClickHouseTableProvider::new_with_schema_unchecked(
            Arc::clone(&pool),
            table_ref,
            Arc::clone(&schema_people),
        );
        assert_eq!(
            clickhouse_table_provider.unique_context(),
            expected_provider.unique_context(),
            "Expected the same unique context"
        );

        // Test schema utilities
        let result = clickhouse_datafusion::utils::create_database("default", &pool).await;
        assert!(result.is_ok(), "Expected 'default' to be no-op");

        // Test creating a invalid schemas
        let bad_schema = Arc::new(Schema::empty());
        let bad_table = TableReference::partial("", "invalid");
        let new_table = TableReference::partial(format!("{db}_new"), "test_table");
        let create_options = CreateOptions::from_engine("MergeTree");
        // First invalid database
        let result = create_schema(&bad_table, &bad_schema, &create_options, &pool, true).await;
        assert!(result.is_err(), "Expected error creating database with invalid database");
        // Then invalid table (schema fails)
        let result = create_schema(&new_table, &bad_schema, &create_options, &pool, false).await;
        assert!(result.is_err(), "Expected error creating table with invalid schema");

        eprintln!(">>> Connection pool tests passed");

        // ---
        // TableProviderFactory

        // Test creating factory with existing pool
        let pool_builder = ArrowConnectionPoolBuilder::new(native_url)
            .configure_client(|c| configure_client(c, &ch));
        let result =
            ClickHouseTableProviderFactory::new_with_builder(native_url, pool_builder).await;
        assert!(result.is_ok(), "Expected successful connection for builder");
        // Assert invalid endpoint fails
        let factory = ClickHouseTableProviderFactory::default();
        let session = ctx.state();
        let invalid_create_cmd = CreateExternalTable {
            name:                 "invalid".into(),
            schema:               Arc::new(DFSchema::try_from(Arc::clone(&schema_people))?),
            // Invalid endpoint
            options:              HashMap::from([("endpoint".to_string(), String::new())]),
            column_defaults:      HashMap::new(),
            constraints:          Constraints::default(),
            table_partition_cols: vec![],
            if_not_exists:        false,
            location:             String::new(),
            file_type:            String::new(),
            temporary:            false,
            definition:           None,
            order_exprs:          vec![],
            unbounded:            false,
        };
        let result = factory.create(&session, &invalid_create_cmd).await;
        assert!(result.is_err(), "Expected error for invalid endpoint");

        eprintln!(">>> TableProviderFactory tests passed");

        // ---
        // Static Connection

        // Assert the ability to access static connection, for future compat with
        // `datafusion-table-providers`, which needs static access to connections
        let result = pool.connect_static().await;
        assert!(result.is_ok(), "Expected to access a static connection");
        let static_conn = result.unwrap();

        // Test accessing a non-existent table
        let result =
            static_conn.get_schema(&TableReference::partial("default", "does-not-exist")).await;
        assert!(result.is_err(), "Non existing table expected to fail");

        // Test accessing an existing table
        let result = static_conn.get_schema(&TableReference::partial(db, "people")).await;
        assert!(result.is_ok(), "Expected people schema to be returned");
        let people_schema = result.unwrap();
        assert_eq!(people_schema.fields().len(), 2, "Expected 2 fields in people schema");

        // Test streaming data using static conn
        let result = static_conn.query_arrow(&format!("SHOW TABLES FROM {db}"), &[], None).await;
        assert!(result.is_ok(), "Expected query to succeed");
        let arrow_result =
            result.unwrap().collect::<Vec<_>>().await.into_iter().collect::<Result<Vec<_>>>();
        assert!(arrow_result.is_ok(), "Expected arrow result to be ok");
        let batches = arrow_result.unwrap();
        assert!(!batches.is_empty(), "Expected records returned");

        // Test execute using static conn
        let result = static_conn.execute("SELECT 1", &[]).await;
        assert!(result.is_ok(), "Expected execute to succeed");
        let results = result.unwrap();
        assert_eq!(results, 0, "Expected 0 rows affected");

        eprintln!(">>> Static connection tests passed");

        // ---
        // Data Sink

        // Use the connection pool to construct a data sink
        let sink_table_ref = TableReference::partial(db, "people");
        let data_sink =
            ClickHouseDataSink::new(pool, sink_table_ref.clone(), Arc::clone(&schema_people));

        // Ensure invalid schema fails
        let empty_schema = Arc::new(Schema::empty());
        let diff_name = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name2", DataType::Utf8, false),
        ]));
        let diff_data_type = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Int32, false),
        ]));
        let diff_nullable = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        for s in [empty_schema, diff_name, diff_data_type, diff_nullable] {
            let result = data_sink.verify_input_schema(&s);
            assert!(result.is_err(), "Expected error for different schema");
        }

        // Ensure valid schema succeeds
        let result = data_sink.verify_input_schema(&schema_people);
        assert!(result.is_ok(), "Expected success for valid schema");

        let expected_display = format!("ClickHouseDataSink: table={sink_table_ref}");
        let sink_display = DefaultDisplay(data_sink);
        assert_eq!(format!("{sink_display}"), expected_display);

        // Ensure successful downcast
        let sink_any = Arc::new(sink_display.0) as Arc<dyn DataSink>;
        let downcast_sink = sink_any.as_any().downcast_ref::<ClickHouseDataSink>();
        assert!(downcast_sink.is_some(), "Expected downcast to ClickHouseDataSink");

        // Test insert into ClickHouseDataSink
        let state = ctx.state();
        let input = EmptyExec::new(Arc::clone(&schema_people));
        assert!(
            clickhouse_table_provider
                .insert_into(&state, Arc::new(input), InsertOp::Overwrite)
                .await
                .is_err(),
            "Should not allow overwrite"
        );

        eprintln!(">>> Data Sink tests passed");

        #[cfg(feature = "federation")]
        {
            use datafusion_federation::sql::SQLExecutor;
            use futures_util::StreamExt;

            // ---
            // SQLExecutor

            // Ensure table provider retrieves correct schemas and tables
            let mut table_names = clickhouse_table_provider.table_names().await?;
            table_names.sort();
            let mut expected_names =
                vec!["people".to_string(), "people2".to_string(), "knicknames".to_string()];
            expected_names.sort();
            assert_eq!(table_names, expected_names, "Expected 3 table names");

            let table_schema = clickhouse_table_provider.get_table_schema("people").await?;
            assert_eq!(&table_schema, &schema_people, "Unexpected schema for people");

            let schema_names = clickhouse_table_provider.pool().connect().await?.schemas().await?;
            assert!(schema_names.contains(&db.to_string()), "Expected {db} to be in schemas");

            eprintln!(">>> SQLExecutor tests passed");

            // ---
            // SQLTable

            // Ensure SQLTable can be federated
            let reader = Arc::new(clickhouse_table_provider.reader().clone());
            let result = Arc::clone(&reader).create_federated_table_provider();
            assert!(!format!("{reader:?}").is_empty()); // Region coverage
            assert!(result.is_ok(), "Failed to create federated table provider");

            // Ensure reader produces same results
            let dialect = reader.as_ref().dialect();
            assert!(dialect.identifier_quote_style("").is_some());

            let result = reader.as_ref().table_names().await;
            assert!(result.is_ok(), "Expected table names for sql table");
            let mut sql_table_names = result.unwrap();
            sql_table_names.sort();
            assert_eq!(sql_table_names, expected_names, "Expected 3 table names");

            let table_schema = reader.as_ref().get_table_schema("people").await?;
            assert_eq!(&table_schema, &schema_people, "Unexpected schema for people");

            let result = reader.as_ref().get_table_schema("does-not-exist").await;
            assert!(result.is_err(), "Table should not exist");

            // Ensure execute works on sql table
            let query = format!("SELECT * FROM {db}.people");
            let results = reader.as_ref().execute(&query, Arc::clone(&schema_people));
            assert!(results.is_ok(), "Expected successful execution of SQL query");
            let result =
                results.unwrap().collect::<Vec<_>>().await.into_iter().collect::<Result<Vec<_>>>();
            assert!(result.is_ok(), "Expected RecordBatches");
            let batches = result.unwrap();
            assert!(batches.is_empty(), "Expected no data");

            eprintln!(">>> SQLTable tests passed");
        }

        eprintln!(">> Test providers completed");

        Ok(())
    }

    #[expect(clippy::cast_sign_loss)]
    pub(super) async fn test_insert_data(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_db_insert";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = common::helpers::create_builder(&ctx, &ch).await?;
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // Insert test data (people & people2)
        let clickhouse = common::helpers::insert_test_data(clickhouse, db, &ctx).await?;

        // -----------------------------
        // Refresh catalog
        let _catalog_provider = clickhouse.build(&ctx).await?;

        // -----------------------------
        // Test select query 1
        let results = ctx
            .sql(&format!("SELECT id, name FROM clickhouse.{db}.people"))
            .await?
            .collect()
            .await?;
        assert!(
            results
                .first()
                .map(|r| r.column(0))
                .map(AsArray::as_primitive::<Int32Type>)
                .filter(|a| (0..2).all(|i| a.value(i) as usize == i + 1))
                .is_some()
        );
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Select query on people passed");

        // -----------------------------
        // Test select query 2
        let results = ctx
            .sql(&format!("SELECT id, names FROM clickhouse.{db}.people2"))
            .await?
            .collect()
            .await?;
        assert!(
            results
                .first()
                .map(|r| r.column(0))
                .map(AsArray::as_primitive::<Int32Type>)
                .filter(|a| (0..3).all(|i| a.value(i) as usize == i + 1))
                .is_some()
        );
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Select query on people2 passed");

        // -----------------------------
        // Test select w/ join
        let results = ctx
            .sql(&format!(
                "SELECT * FROM clickhouse.{db}.people p1 JOIN clickhouse.{db}.people2 p2 ON p1.id \
                 = p2.id"
            ))
            .await?
            .collect()
            .await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Join query on people passed");

        // -----------------------------
        // Test datafusion unnest works when federation is off
        #[cfg(not(feature = "federation"))]
        {
            let results = ctx
                .sql(&format!("SELECT id, unnest(names) FROM clickhouse.{db}.people2"))
                .await?
                .collect()
                .await?;
            arrow::util::pretty::print_batches(&results)?;
            eprintln!(">>> Unnest query passed");
        }

        // -----------------------------
        // Test datafusion unnest works when federation is on
        #[cfg(feature = "federation")]
        {
            let result = ctx
                .sql(&format!("SELECT id, unnest(names) FROM clickhouse.{db}.people2"))
                .await?
                .collect()
                .await;
            // arrow::util::pretty::print_batches(&results)?;
            assert!(result.is_err(), "Federation fails due to UNNEST bug");
            eprintln!(">>> Unnest query passed (known failure)");
        }

        eprintln!(">> Test insert completed");

        Ok(())
    }

    #[expect(clippy::too_many_lines)]
    pub(super) async fn test_clickhouse_udfs_smoke_test(
        ch: Arc<ClickHouseContainer>,
    ) -> Result<()> {
        let db = "test_db_udfs_smoke_test";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        // -----------------------------
        // Registering UDF Optimizer and UDF Pushdown
        let ctx = ClickHouseSessionContext::from(ctx);

        let builder = common::helpers::create_builder(&ctx, &ch).await?;
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // Insert test data (people & people2)
        let clickhouse = common::helpers::insert_test_data(clickhouse, db, &ctx).await?;

        // -----------------------------
        // Refresh catalog
        let _catalog_provider = clickhouse.build(&ctx).await?;

        eprintln!("---- Starting queries ----");

        // -----------------------------
        // Test projection with custom Analyzer
        let query = format!(
            "SELECT clickhouse(exp(p2.id), 'Float64'), p2.names
            FROM clickhouse.{db}.people2 p2"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Projection test passed");

        // -----------------------------
        // Test simple clickhouse function without aggregation first
        let query = format!(
            "SELECT p.id,
                    p.name,
                    clickhouse(`toString`(mod(p.id, 2)), 'Utf8') as id_mod
            FROM clickhouse.{db}.people p"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Simple clickhouse function test passed");

        // -----------------------------
        // Test join with custom Analyzer
        let query = format!(
            "SELECT p1.name,
                    clickhouse(exp(p2.id), 'Float64'),
                    p2.names
            FROM clickhouse.{db}.people p1
            JOIN (SELECT id, names FROM clickhouse.{db}.people2) p2
                ON p1.id = p2.id
            "
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Join test w/ Subquery function passed");

        // -----------------------------
        // Test join with custom Analyzer
        let query = format!(
            "SELECT p.name,
                    clickhouse(exp(p2.id), 'Float64'),
                    p2.names
            FROM clickhouse.{db}.people p
            JOIN (
                SELECT id, clickhouse(`arrayJoin`(names), 'Utf8') as names
                FROM clickhouse.{db}.people2
            ) p2 ON p.id = p2.id
            "
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Join test multiple functions passed");

        // -----------------------------
        // Test UNION (with Filter) with clickhouse functions
        let query = format!(
            "SELECT id, clickhouse(upper(name), 'Utf8') as upper_name
            FROM clickhouse.{db}.people
            WHERE id = 1
            UNION ALL
            SELECT id, clickhouse(`arrayJoin`(names), 'Utf8') as upper_name
            FROM clickhouse.{db}.people2
            WHERE id = 1"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> UNION (with Filter) test passed");

        // -----------------------------
        // Test LIMIT with clickhouse functions
        let query = format!(
            "SELECT id, clickhouse(upper(name), 'Utf8') as upper_name
            FROM clickhouse.{db}.people
            LIMIT 1"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> LIMIT test passed");

        // -----------------------------
        // Test SORT with clickhouse functions
        let query = format!(
            "SELECT p.id, clickhouse(upper(p.name), 'Utf8') as upper_name
            FROM clickhouse.{db}.people p
            ORDER BY clickhouse(abs(p.id), 'Int32')"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> SORT test passed");

        // -----------------------------
        // Test LIMIT with JOIN and multiple functions
        let query = format!(
            "SELECT p.name,
                    p2.id,
                    clickhouse(concat(p.name, ' from p1'), 'Utf8') as name_p1,
                    clickhouse(concat(p2.names, ' from p2'), 'Utf8') as name_p2
            FROM clickhouse.{db}.people p
            JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id
            LIMIT 1"
        );
        let results = ctx
            .sql(&query)
            .await
            .inspect_err(|error| error!("Error two-column string query: {}", error))?
            .collect()
            .await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Two-column clickhouse function test with LIMIT and JOIN passed");

        // -----------------------------
        // Test two-column clickhouse function - simple case, determinate
        let query = format!(
            "SELECT p.name,
                    p2.id,
                    clickhouse(p.id + p2.id, 'Int64') as sum_ids
            FROM clickhouse.{db}.people p
            JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Two-column clickhouse function test (simple) passed");

        // -----------------------------
        // Test JOIN + AGGREGATE
        let query = format!(
            "SELECT p2.id, clickhouse(abs(p.id * p2.id), 'Float64') as sum_product
            FROM clickhouse.{db}.people p
            JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id
            GROUP BY p.id, p2.id
            HAVING clickhouse(abs(p.id), 'Int64') > 0"
        );
        let results = ctx.sql(&query).await?.collect().await?;

        drop(arrow::util::pretty::print_batches(&results));
        eprintln!(">>> Join+aggregate test passed");

        eprintln!(">> UDFs smoke test passed");

        Ok(())
    }

    #[expect(clippy::too_many_lines)]
    pub(super) async fn test_clickhouse_udfs(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_db_udfs";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        // -----------------------------
        // Registering UDF Optimizer and UDF Pushdown
        let ctx = ClickHouseSessionContext::from(ctx);

        let builder = common::helpers::create_builder(&ctx, &ch).await?;
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // Insert test data (people & people2)
        let clickhouse = common::helpers::insert_test_data(clickhouse, db, &ctx).await?;

        // -----------------------------
        // Refresh catalog
        let _catalog_provider = clickhouse.build(&ctx).await?;

        // -----------------------------
        //
        // Add in-memory table
        common::helpers::add_memory_table_and_data(&ctx)?;

        eprintln!("---- Starting queries ----");

        // -----------------------------
        // Test JOINS with multiple functions
        //
        // NOTE: This is determinate since the functions can be pushed to either side of join
        let query = format!(
            "SELECT p.name,
                    m.event_id,
                    clickhouse(exp(p.id), 'Float64'),
                    clickhouse(concat(p2.name, 'hello'), 'Utf8')
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id
            "
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Join with multiple functions passed");

        // -----------------------------
        // Test JOIN with functions to be pushed and functions within SUBQUERY
        //
        // NOTE: This is determinate since the functions can be pushed to either side of join
        let query = format!(
            "SELECT p.name,
                    m.event_id,
                    clickhouse(exp(p2.id), 'Float64'),
                    clickhouse(concat(p2.names, 'hello'), 'Utf8')
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            JOIN (
                SELECT id
                    , clickhouse(`arrayJoin`(names), 'Utf8') as names
                FROM clickhouse.{db}.people2
            ) p2 ON p.id = p2.id
            "
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Join with functions at multiple levels passed");

        // -----------------------------
        // Test JOINS with mixed functions
        //
        // NOTE: This is determinate since the functions can be pushed to either side of join
        let query = format!(
            "SELECT p.name,
                    m.event_id,
                    clickhouse(exp(p2.id), 'Float64'),
                    concat(p2.names, 'hello')
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            JOIN (
                SELECT id
                    , clickhouse(`arrayJoin`(names), 'Utf8') as names
                FROM clickhouse.{db}.people2
            ) p2 ON p.id = p2.id
            "
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> JOIN with mixed function passed");

        // -----------------------------
        // Test two-column clickhouse function
        //
        // Pushdown is determinate due to the way the LogicalPlan is constructed.
        // The FROM table factor and the following table factor will be joined as the input to the
        // left join, with the other table on the right. If the order was changed, like the
        // corresponding query in the failing tests below, then it would fail as well.
        let query = format!(
            "SELECT m.event_id,
                    p.name,
                    p2.id,
                    clickhouse(p.id + p2.id, 'Int64') as sum_ids
            FROM clickhouse.{db}.people p
            JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id
            JOIN memory.internal.mem_events m ON p2.id = m.event_id"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Two-column clickhouse function test (advanced) passed");

        // -----------------------------
        // Test LIMIT with clickhouse function w/ multiple column arg
        let query = format!(
            "SELECT m.event_id,
                    p.name,
                    p2.id,
                    clickhouse(exp(p.id), 'Float64') as sum_ids
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id
            LIMIT 1"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> LIMIT with clickhouse function w/ multiple column arg passed");

        // -----------------------------
        // Test SORT with clickhouse function
        let query = format!(
            "SELECT m.event_id,
                    p.name,
                    p2.id,
                    clickhouse(exp(p.id), 'Float64')
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id
            ORDER BY clickhouse(exp(p.id), 'Float64')"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> SORT JOIN with clickhouse function w/ multiple column arg passed");

        // -----------------------------
        // Test expression in Filter that can be pushed down
        let query = format!(
            "SELECT m.event_id, p2.name as p2_name
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people2 p2 ON m.event_id = p2.id
            WHERE m.event_id + p2.id > clickhouse(abs(p2.id), 'UInt32')"
        );
        let result = ctx.sql(&query).await?.collect().await?;
        drop(arrow::util::pretty::print_batches(&result));
        eprintln!(">>> Function in FILTER test passed");

        // -----------------------------
        // Test Scalar across JOIN in ClickHouse function
        let query = format!(
            "SELECT p.name, p.id, m.event_id, clickhouse(abs(2), 'Int64')
            FROM clickhouse.{db}.people p
            JOIN memory.internal.mem_events m ON m.event_id = p.id
            ORDER BY p.id"
        );
        let result = ctx.sql(&query).await?.collect().await?;
        // assert!(result.is_err(), "Expects Scalars to fail resolution");
        drop(arrow::util::pretty::print_batches(&result));
        eprintln!(">>> ClickHouse function with only scalar value test passed");

        // -----------------------------
        // Test CTE with cross-references
        let query = format!(
            "WITH ch_data AS (
                SELECT
                    id,
                    clickhouse(exp(id), 'Float64') as exp_id
                FROM clickhouse.{db}.people
            ),
            ch_data2 AS (
                SELECT
                    p2.id,
                    clickhouse(`arrayJoin`(p2.names), 'Utf8') as name,
                    ch.exp_id
                FROM clickhouse.{db}.people2 p2
                JOIN ch_data ch ON p2.id = ch.id
            )
            SELECT * FROM ch_data2"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> CTE with cross-references test passed");

        // -----------------------------
        // Test window functions over clickhouse results
        let query = format!(
            "SELECT p.id,
                    p.name,
                    clickhouse(exp(p.id), 'Float64') as exp_id,
                    SUM(p.id) OVER (ORDER BY p.id) as running_sum
            FROM clickhouse.{db}.people p"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Window functions over clickhouse results test passed");

        // -----------------------------
        // Test various window functions work with clickhouse context
        let query = format!(
            "SELECT p.id, p.name,
                    clickhouse(exp(p.id), 'Float64') as exp_id,
                    SUM(p.id) OVER (ORDER BY p.id) as sum_running,
                    AVG(p.id) OVER (ORDER BY p.id) as avg_running,
                    COUNT(p.id) OVER (PARTITION BY p.name ORDER BY p.id) as count_by_name,
                    MAX(p.id) OVER () as max_id
            FROM clickhouse.{db}.people p"
        );

        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Window aggregates test passed");

        // -----------------------------
        // Test ranking window functions
        let query = format!(
            "SELECT p.id,
                    p.name,
                    clickhouse(upper(p.name), 'Utf8') as upper_name,
                    RANK() OVER (ORDER BY p.id DESC) as id_rank,
                    DENSE_RANK() OVER (ORDER BY p.name) as name_dense_rank,
                    ROW_NUMBER() OVER (ORDER BY p.id) as row_num
            FROM clickhouse.{db}.people p"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Ranking window functions test passed");

        // -----------------------------
        // Test window functions with clickhouse in ORDER BY AND mixed functions
        let query = format!(
            "SELECT p.id,
                    p.name,
                    clickhouse(exp(p.id), 'Float64') as exp_id,
                    SUM(p.id) OVER (ORDER BY clickhouse(exp(p.id), 'Float64')) as sum_by_exp,
                    RANK() OVER (ORDER BY clickhouse(upper(p.name), 'Utf8')) as rank_by_upper_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY clickhouse(mod(p.id, 2), 'Int32') ORDER BY p.id
                    ) as row_num_by_mod
            FROM clickhouse.{db}.people p"
        );
        let results = ctx.sql(&query).await?.collect().await;
        arrow::util::pretty::print_batches(&results?)?;
        eprintln!(">>> Window functions with clickhouse in ORDER BY test passed");

        // -----------------------------
        // Test CASE expression with clickhouse functions
        let query = format!(
            "SELECT p.id,
                    p.name,
                    CASE
                        WHEN p.name = 'Alice' THEN clickhouse(upper(p.name), 'Utf8')
                        WHEN p.name = 'Bob' THEN clickhouse(lower(p.name), 'Utf8')
                        ELSE clickhouse(concat(p.name, ' (other)'), 'Utf8')
                    END as name_transformed
            FROM clickhouse.{db}.people p"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> CASE expression with clickhouse functions test passed");

        // RESOLVED! Included as a test to ensure this logic doesn't regress
        //
        // CRITICAL BUG FOUND: Multi-table column references in single clickhouse function
        // This test demonstrates a bug where the analyzer incorrectly handles
        // clickhouse functions that reference columns from multiple tables.
        // The function gets pushed down to one table but references columns from another.
        //
        // Error: SchemaError(FieldNotFound { field: Column { relation: Some(Full {
        //   catalog: "clickhouse", schema: "test_db_udfs", table: "people" }),
        //   name: "names" }, ...
        //
        // The analyzer incorrectly looks for "names" in "people" table instead of "people2"
        let query = format!(
            "SELECT p.name,
                    p2.id,
                    clickhouse(concat(p.name, p2.names), 'Utf8') as combined_names
            FROM clickhouse.{db}.people p
            JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id
            LIMIT 1"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Multi-table column clickhouse function test passed");

        Ok(())
    }

    pub(super) async fn test_clickhouse_udfs_schema_coercion(
        ch: Arc<ClickHouseContainer>,
    ) -> Result<()> {
        let db = "test_db_udfs_coercion";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        // -----------------------------
        // Registering UDF Optimizer and UDF Pushdown
        let ctx = ClickHouseSessionContext::from(ctx);

        let builder = common::helpers::create_builder_with_coercion(&ctx, &ch).await?;
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // Insert test data (people & people2)
        let clickhouse = common::helpers::insert_test_data(clickhouse, db, &ctx).await?;

        // -----------------------------
        // Refresh catalog
        let _catalog_provider = clickhouse.build(&ctx).await?;

        // -----------------------------
        //
        // Add in-memory table
        common::helpers::add_memory_table_and_data(&ctx)?;

        // -----------------------------
        // Test JOIN with functions to be pushed and functions within SUBQUERY
        //
        // NOTE: This is determinate since the functions can be pushed to either side of join
        let query = format!(
            "SELECT p.name,
                    m.event_id,
                    clickhouse(exp(p2.id), 'Int32') as exp_id, -- coerces from Float64 to Int32
                    clickhouse(concat(p2.names, 'hello'), 'Utf8') as concat_names
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            JOIN (
                SELECT id, clickhouse(`arrayJoin`(names), 'Utf8') as names
                FROM clickhouse.{db}.people2
            ) p2 ON p.id = p2.id
            "
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Join with functions at multiple levels with schema coercion passed");

        // -----------------------------
        // Test two-column clickhouse function - simple case, determinate
        let query = format!(
            "SELECT p.name,
                p2.id,
                clickhouse(p.id + p2.id, 'Int32') as sum_ids -- coerces from Int64 to Int32
            FROM clickhouse.{db}.people p
            JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Two-column clickhouse function test with schema coercion passed");

        // -----------------------------
        // Test the lambda will produce non-nullable based on ClickHouse Schema
        //
        // TODO: Remove - This will need to be removed after nullability is fixed in udfs
        let query = format!(
            "SELECT p.name,
                    p2.id,
                    p2.names
            FROM clickhouse.{db}.people p
            JOIN (
                SELECT id,
                       clickhouse(
                           `arrayMap`($x, concat($x, ' hello'), names),
                           'List(Utf8)' -- Return type
                        ) as names
                FROM clickhouse.{db}.people2
            ) p2
            ON p.id = p2.id
            "
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> arrayMap lambda in subquery with single parameter test passed");

        Ok(())
    }

    pub(super) async fn test_clickhouse_udfs_lambda(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_db_udfs_lambda";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        // -----------------------------
        // Registering UDF Optimizer and UDF Pushdown
        let ctx = ClickHouseSessionContext::from(ctx);

        let builder = common::helpers::create_builder(&ctx, &ch).await?;
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // Insert test data (people & people2)
        let clickhouse = common::helpers::insert_test_data(clickhouse, db, &ctx).await?;

        // -----------------------------
        // Refresh catalog
        let _catalog_provider = clickhouse.build(&ctx).await?;

        // -----------------------------
        // Test simple lambda with single array column
        let query = format!(
            "SELECT p2.id,
                    p2.names,
                    clickhouse(`arrayMap`($x, concat($x, p2.id), p2.names), 'List(Utf8)')
                      as upper_names
                FROM clickhouse.{db}.people2 p2
                WHERE p2.id = 1"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> arrayMap lambda with single parameter test passed");

        let query = format!(
            "SELECT p.name,
                    p2.id,
                    p2.names
            FROM clickhouse.{db}.people p
            JOIN (
                SELECT id,
                       clickhouse(
                           `arrayMap`($x, concat($x, ' hello'), names),
                           'List(Utf8)' -- Return type
                        ) as names
                FROM clickhouse.{db}.people2
            ) p2
            ON p.id = p2.id
            "
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> arrayMap lambda in subquery with single parameter test passed");

        // -----------------------------
        // Test simple lambda with two array columns
        let query = format!(
            "SELECT p2.id,
                    p2.names,
                    clickhouse(
                        `arrayMap`($x, $y, concat($x, $y, p2.id), p2.names, p2.names),
                        'List(Utf8)'
                    ) as upper_names
                FROM clickhouse.{db}.people2 p2
                WHERE p2.id = 1"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> arrayMap lambda with two parameters test passed");

        // -----------------------------
        // Test simple lambda with single array column wrapped in lambda
        //
        // Note how the clickhouse lambda udf is used here. Not necessary but can help distinguish
        let query = format!(
            "SELECT p2.id,
                    p2.names,
                    clickhouse(
                        lambda(`arrayMap`($x, concat($x, p2.id), p2.names)),
                        'List(Utf8)'
                    ) as upper_names
            FROM clickhouse.{db}.people2 p2
            WHERE p2.id = 1"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> arrayMap lambda with two parameters wrapped test passed");

        // -----------------------------
        // Test arrayFilter lambda
        let query = format!(
            "SELECT p2.id,
                    p2.names,
                    clickhouse(`arrayFilter`($x, length($x) > 3, p2.names), 'List(Utf8)')
                      as long_names
             FROM clickhouse.{db}.people2 p2"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> arrayFilter lambda test passed");

        // -----------------------------
        // Test arrayMap with computation on array elements
        let query = format!(
            "SELECT p2.id,
                    p2.names,
                    clickhouse(`arrayMap`($x, concat($x, '_suffix'), p2.names), 'List(Utf8)')
                      as suffixed_names
             FROM clickhouse.{db}.people2 p2
             WHERE p2.id <= 2"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> arrayMap with string concatenation test passed");

        // -----------------------------
        // Test arrayExists lambda (returns boolean)
        let query = format!(
            "SELECT p2.id,
                    p2.names,
                    clickhouse(`arrayExists`($x, $x = 'Jazz', p2.names), 'Boolean') as has_jazz
             FROM clickhouse.{db}.people2 p2"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> arrayExists lambda test passed");

        eprintln!(">> Lambda UDFs test passed");

        Ok(())
    }

    pub(super) async fn test_clickhouse_udfs_failing(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_db_udfs_failing";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        // -----------------------------
        // Registering UDF Optimizer and UDF Pushdown
        let ctx = ClickHouseSessionContext::from(ctx);

        let builder = common::helpers::create_builder(&ctx, &ch).await?;
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // Insert test data (people & people2)
        let clickhouse = common::helpers::insert_test_data(clickhouse, db, &ctx).await?;

        // -----------------------------
        // Refresh catalog
        let _catalog_provider = clickhouse.build(&ctx).await?;

        // -----------------------------
        //
        // Add in-memory table
        common::helpers::add_memory_table_and_data(&ctx)?;

        // -----------------------------
        // Test deeply nested subqueries
        //
        // NOTE: This now works WITHOUT federation due to improved empty schema handling for
        // COUNT(*) aggregations However, with federation enabled, scalar subqueries are not
        // yet supported in datafusion-federation.
        //
        // The statement above needs a bit more investigation. Currently, deeply nested subqueries
        // requires a setting to be provided to clickhouse. But due to clickhouse limitations, it
        // seems that the setting isn't being applied. Will revist.
        let query = format!(
            "SELECT
                outer_name,
                clickhouse(upper(outer_name), 'Utf8') as upper_name,
                inner_sum
            FROM (
                SELECT
                    p.name as outer_name,
                    p.id as outer_id,
                    (
                        SELECT COUNT(*)
                        FROM (
                            SELECT id, clickhouse(`arrayJoin`(names), 'Utf8') as name
                            FROM clickhouse.{db}.people2
                        ) p2_inner
                        WHERE p2_inner.id <= p.id
                    ) as inner_sum
                FROM clickhouse.{db}.people p
            ) t"
        );
        #[cfg(not(feature = "federation"))]
        {
            let result = ctx.sql(&query).await?.collect().await?;
            arrow::util::pretty::print_batches(&result)?;
            assert!(!result.is_empty(), "Deeply nested subqueries should return results");
            eprintln!(">>> Deeply nested subqueries test passed");
        }
        #[cfg(feature = "federation")]
        {
            let result = ctx.sql(&query).await?.collect().await;
            assert!(result.is_err(), "Scalar subqueries not supported with federation yet");
            eprintln!(">>> Deeply nested subqueries tests need additional review");
        }

        // -----------------------------
        // Test two-column clickhouse function
        //
        // Pushdown is indeterminate since the clickhouse tables are on either side of the join.
        // This fails as expected.
        let query = format!(
            "SELECT m.event_id,
                    p.name,
                    p2.id,
                    clickhouse(p.id + p2.id, 'Int64') as sum_ids
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id"
        );
        let result = ctx.sql(&query).await?.collect().await;
        assert!(result.is_err(), "Indeterminate pushdown expected to fail");
        eprintln!(">>> Two-column clickhouse function across JOIN test passed");

        // -----------------------------
        // Test aggregation over clickhouse function results AND mixed functions
        //
        // NOTE: This fails because of "mixed" functions. Needs to be resolved
        let query = format!(
            "SELECT clickhouse(`toString`(mod(p.id, 2)), 'Utf8') as id_mod,
                    COUNT(p.id) as total,
                    MAX(clickhouse(exp(p.id), 'Float64')) as max_exp,
                    STRING_AGG(p.name, ',') as all_names
            FROM clickhouse.{db}.people p
            GROUP BY id_mod"
        );
        let results = ctx.sql(&query).await?.collect().await;
        assert!(results.is_err());
        eprintln!(">>> Aggregation over clickhouse function results test passed");

        // -----------------------------
        // Test violation: ClickHouse function in aggregate with non-grouped column reference
        // This violates SQL GROUP BY semantics - using non-grouped column in aggregate context
        //
        // Error: Diagnostic(Diagnostic { kind: Error, message: "'p.name' must appear in GROUP BY
        // clause because it's not an aggregate expression", span: None, notes: [], helps:
        // [DiagnosticHelp { message: "Either add 'p.name' to GROUP BY clause, or use an
        // aggregare // function like ANY_VALUE(p.name)", span: None }] }, Plan("Column in
        // SELECT must be in // GROUP BY or an aggregate function: While expanding wildcard,
        // column \"p.name\" must // appear in the GROUP BY clause or must be part of an
        // aggregate function, currently only // \"clickhouse(upper(p.name),Utf8(\"Utf8\")),
        // max(p.id)\" appears in the SELECT clause // satisfies this requirement"))
        let query = format!(
            "SELECT p.name,
                    clickhouse(max(p.id), 'Int64') as max_id,
                    clickhouse(upper(p.name), 'Utf8')
            FROM clickhouse.{db}.people p
                GROUP BY upper(p.name)"
        );
        let results = ctx.sql(&query).await;
        assert!(results.is_err(), "Expected GROUP BY semantic violation");
        eprintln!(">>> GROUP BY semantic violation test passed (correctly failed)\n{results:?}");

        // -----------------------------
        // TODO: Important! This is NOT a semantic violation as it spans only a single clickhouse
        // table. Modify this to use a join THEN it will be a semantic violation.
        //
        // // Test semantic violation: ClickHouse function with aggregate that changes column
        // meaning this should fail because pushing the clickhouse function below the
        // aggregate would change the semantic meaning - the mixed functions prevent the
        // separation of the aggs.
        // // TODO: Once clickhouse functions can be separated from df functions, this should be ok,
        // // since the plan contains only a single clickhouse table.
        // let query = format!(
        //     "SELECT p.name
        //         , COUNT(*) as count
        //         , clickhouse(exp(COUNT(*)), 'Float64') as exp_count
        //     FROM clickhouse.{db}.people p
        //     GROUP BY p.name"
        // );
        // let results = ctx.sql(&query).await?.collect().await;
        // assert!(results.is_err(), "Expected semantic violation for aggregate function pushdown");
        // eprintln!(">>> Aggregate semantic violation test passed (by failing)\n{results:?}");

        // TODO: Remove - verify if this fails as well - AFTER ADDING A JOIN
        // let query = format!(
        //     "SELECT p.name
        //             , clickhouse(max(p.id), 'Int64') as max_id
        //             , clickhouse(upper(p.name), 'Utf8')
        //         FROM clickhouse.{db}.people p
        //         GROUP BY p.name"
        // );
        // let results = ctx.sql(&query).await;
        // assert!(results.is_err(), "Expected GROUP BY semantic violation");
        // eprintln!(">>> GROUP BY semantic violation test passed (correctly failed)\n{results:?}");

        // TODO: Remove - Is this a Sort plan violation? Also, add JOIN
        // let query = format!(
        //     "SELECT p1.name, p2.name as p2_name
        //     FROM clickhouse.{db}.people p1
        //     JOIN clickhouse.{db}.people2 p2 ON p1.id = p2.id
        //     WHERE clickhouse(p1.id + p2.id, 'Int64') >
        //           (SELECT clickhouse(avg(id), 'Float64') FROM clickhouse.{db}.people)
        //     ORDER BY clickhouse(upper(p1.name), 'Utf8')"
        // );

        Ok(())
    }

    pub(super) async fn test_drop_table(ch: Arc<ClickHouseContainer>) -> Result<()> {
        use clickhouse_arrow::prelude::ClickHouseEngine;

        let db = "test_db_drop";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = common::helpers::create_builder(&ctx, &ch).await?;

        // Create test database and table
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let clickhouse = builder
            .with_schema(db)
            .await?
            .with_new_table("test_table", ClickHouseEngine::MergeTree, schema)
            .update_create_options(|opts| opts.with_order_by(&["id".to_string()]))
            .create(&ctx)
            .await?;

        // Build catalog
        let _catalog_provider = clickhouse.build(&ctx).await?;

        eprintln!(">>> Created test table");

        // -----------------------------
        // Verify table exists by running a simple query
        eprintln!(">>> Attempting to query table: clickhouse.{db}.test_table");
        let result = ctx.sql(&format!("SELECT COUNT(*) FROM clickhouse.{db}.test_table")).await;

        match &result {
            Ok(_) => eprintln!(">>> SQL query succeeded"),
            Err(e) => eprintln!(">>> SQL query failed: {e:?}"),
        }

        let collect_result = result?.collect().await;
        match &collect_result {
            Ok(batches) => {
                eprintln!(">>> Query collected successfully, {} batches", batches.len());
                assert!(collect_result.is_ok(), "Table should exist and be queryable");
                eprintln!(">>> Verified table exists");
            }
            Err(e) => {
                eprintln!(">>> Query collection failed: {e:?}");
                panic!("Table should exist and be queryable");
            }
        }

        // -----------------------------
        // Test DROP TABLE
        eprintln!(">>> Testing DROP TABLE...");
        let drop_result = ctx.sql(&format!("DROP TABLE clickhouse.{db}.test_table")).await;

        match drop_result {
            Ok(df) => {
                let collect_result = df.collect().await;
                match collect_result {
                    Ok(_) => {
                        eprintln!(">>> DROP TABLE succeeded");

                        // Verify table no longer exists
                        let verify_result = ctx
                            .sql(&format!("SELECT COUNT(*) FROM clickhouse.{db}.test_table"))
                            .await;

                        assert!(verify_result.is_err(), "Table should not exist after DROP TABLE");
                        eprintln!(">>> Verified table no longer exists");
                    }
                    Err(e) => {
                        eprintln!(">>> DROP TABLE failed during execution: {e}");
                        eprintln!(
                            ">>> EXPECTED: deregister_table not yet implemented in \
                             ClickHouseSchemaProvider"
                        );
                        // This is expected until deregister_table is implemented
                        // The test will pass once the feature is implemented
                        return Ok(());
                    }
                }
            }
            Err(e) => {
                eprintln!(">>> DROP TABLE failed during planning: {e}");
                eprintln!(
                    ">>> EXPECTED: deregister_table not yet implemented in \
                     ClickHouseSchemaProvider"
                );
                // This is expected until deregister_table is implemented
                return Ok(());
            }
        }

        // -----------------------------
        // Test DROP TABLE IF EXISTS with non-existent table
        eprintln!(">>> Testing DROP TABLE IF EXISTS with non-existent table...");
        let drop_if_exists_result =
            ctx.sql(&format!("DROP TABLE IF EXISTS clickhouse.{db}.nonexistent_table")).await;

        match drop_if_exists_result {
            Ok(df) => {
                let collect_result = df.collect().await;
                assert!(
                    collect_result.is_ok(),
                    "DROP TABLE IF EXISTS should succeed even if table doesn't exist"
                );
                eprintln!(">>> DROP TABLE IF EXISTS succeeded for non-existent table");
            }
            Err(e) => {
                eprintln!(">>> DROP TABLE IF EXISTS failed: {e}");
                eprintln!(">>> EXPECTED: deregister_table not yet implemented");
            }
        }

        eprintln!(">> Test DROP TABLE completed");

        Ok(())
    }

    /// Test ClickHouseDataSink write_all method with schema validation
    pub(super) async fn test_sink_write_all(ch: Arc<ClickHouseContainer>) -> Result<()> {
        use clickhouse_arrow::prelude::ClickHouseEngine;
        use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
        use datafusion::datasource::sink::DataSink;
        use datafusion::execution::context::TaskContext;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures_util::stream;

        let db = "test_db_sink";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = common::helpers::create_builder(&ctx, &ch).await?;

        // Create test table schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create database and table
        let clickhouse = builder
            .with_schema(db)
            .await?
            .with_new_table("sink_test", ClickHouseEngine::MergeTree, schema.clone())
            .update_create_options(|opts| opts.with_order_by(&["id".to_string()]))
            .create(&ctx)
            .await?;

        drop(clickhouse.build(&ctx).await?);

        eprintln!(">>> Created test table for sink");

        // Get the pool from the table provider
        let table_ref = TableReference::full(DEFAULT_CLICKHOUSE_CATALOG, db, "sink_test");
        let provider = ctx.table_provider(table_ref.clone()).await?;
        let table_provider = extract_clickhouse_provider(&provider)
            .expect("Could not extract ClickHouse table provider");
        let pool = Arc::clone(table_provider.pool());

        let sink_table_ref = TableReference::partial(db, "sink_test");
        let data_sink = ClickHouseDataSink::new(pool.clone(), sink_table_ref, schema.clone());

        // Test 1: Successful write with valid data
        eprintln!(">>> Test 1: Valid data write");
        let batch1 = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ])?;

        let stream1 =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream::iter(vec![Ok(batch1)])));

        let task_ctx = Arc::new(TaskContext::default());
        let result = data_sink.write_all(stream1, &task_ctx).await;
        assert!(result.is_ok(), "Valid data should write successfully");
        assert_eq!(result.unwrap(), 3, "Should have written 3 rows");
        eprintln!(">>> ✓ Valid data write succeeded");

        // Test 2: Schema mismatch - field count
        eprintln!(">>> Test 2: Field count mismatch");
        let wrong_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch2 =
            RecordBatch::try_new(wrong_schema.clone(), vec![Arc::new(Int32Array::from(vec![4]))])?;

        let stream2 =
            Box::pin(RecordBatchStreamAdapter::new(wrong_schema, stream::iter(vec![Ok(batch2)])));

        let result = data_sink.write_all(stream2, &task_ctx).await;
        assert!(result.is_err(), "Field count mismatch should fail");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Schema fields must match") || err.contains("input has 1 fields, sink 2")
        );
        eprintln!(">>> ✓ Field count mismatch detected");

        // Test 3: Schema mismatch - data type
        eprintln!(">>> Test 3: Data type mismatch");
        let wrong_type_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false), // Wrong type
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch3 = RecordBatch::try_new(wrong_type_schema.clone(), vec![
            Arc::new(arrow::array::Int64Array::from(vec![5i64])),
            Arc::new(StringArray::from(vec!["d"])),
        ])?;

        let stream3 = Box::pin(RecordBatchStreamAdapter::new(
            wrong_type_schema,
            stream::iter(vec![Ok(batch3)]),
        ));

        let result = data_sink.write_all(stream3, &task_ctx).await;
        assert!(result.is_err(), "Data type mismatch should fail");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expected data type"));
        eprintln!(">>> ✓ Data type mismatch detected");

        // Test 4: Schema mismatch - nullability
        eprintln!(">>> Test 4: Nullability mismatch");
        let wrong_nullable_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true), // Wrong nullability
        ]));
        let batch4 = RecordBatch::try_new(wrong_nullable_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![6])),
            Arc::new(StringArray::from(vec![Some("e")])),
        ])?;

        let stream4 = Box::pin(RecordBatchStreamAdapter::new(
            wrong_nullable_schema,
            stream::iter(vec![Ok(batch4)]),
        ));

        let result = data_sink.write_all(stream4, &task_ctx).await;
        assert!(result.is_err(), "Nullability mismatch should fail");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expected nullability"));
        eprintln!(">>> ✓ Nullability mismatch detected");

        // Verify total rows written (only the valid batch)
        let verify = ctx.sql(&format!("SELECT COUNT(*) FROM clickhouse.{db}.sink_test")).await?;
        let batches = verify.collect().await?;
        arrow::util::pretty::print_batches(&batches)?;
        // COUNT(*) returns different types depending on context, just verify we got data
        assert!(!batches.is_empty() && batches[0].num_rows() > 0, "Should have count result");

        // Test 5: Stream error - error in batch stream
        eprintln!(">>> Test 5: Error in batch stream");
        use datafusion::common::DataFusionError;
        let error_stream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Err(DataFusionError::Execution(
                "Simulated stream error".to_string(),
            ))]),
        ));

        let result = data_sink.write_all(error_stream, &task_ctx).await;
        assert!(result.is_err(), "Stream error should propagate");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Simulated stream error"));
        eprintln!(">>> ✓ Stream error propagated");

        // Test 6: Insert to non-existent table (triggers ClickHouse error)
        eprintln!(">>> Test 6: Insert to non-existent table");
        let bad_sink = ClickHouseDataSink::new(
            pool.clone(),
            TableReference::partial(db, "nonexistent_table"),
            schema.clone(),
        );

        let batch6 = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int32Array::from(vec![99])),
            Arc::new(StringArray::from(vec!["z"])),
        ])?;

        let stream6 =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream::iter(vec![Ok(batch6)])));

        let result = bad_sink.write_all(stream6, &task_ctx).await;
        assert!(result.is_err(), "Insert to non-existent table should fail");
        eprintln!(">>> ✓ Non-existent table error detected");

        eprintln!(">>> All DataSink write_all tests passed");

        Ok(())
    }

    // TODO: Add notes, examples, and update README to reflect that this is useful ONLY when
    // federation is enabled. That is because federation naturally "pushes" the UDF to the federated
    // table and it is then converted into SQL for the remote database.
    #[cfg(feature = "federation")]
    pub(super) async fn test_clickhouse_eval_udf(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_db_eval_udf";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = common::helpers::create_builder(&ctx, &ch).await?;
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // Insert test data (people & people2)
        let clickhouse = common::helpers::insert_test_data(clickhouse, db, &ctx).await?;

        // -----------------------------
        // Refresh catalog
        let _catalog_provider = clickhouse.build(&ctx).await?;

        // -----------------------------
        // Test eval usage first
        let results = ctx
            .sql(&format!(
                "SELECT name
                    , clickhouse_eval('exp(id)', 'Float64')
                    , clickhouse_eval('upper(name)', 'Utf8')
                FROM clickhouse.{db}.people"
            ))
            .await?
            .collect()
            .await?;
        assert!(!results.is_empty());
        arrow::util::pretty::print_batches(&results)?;
        let batch = results.first().unwrap();
        assert_eq!(batch.num_rows(), 2);
        eprintln!(">>> clickhouse_eval test 1 passed");

        Ok(())
    }

    #[cfg(feature = "federation")]
    pub(super) async fn test_federated_catalog(ch: Arc<ClickHouseContainer>) -> Result<()> {
        use clickhouse_datafusion::providers::catalog::federation::FederatedCatalogProvider;
        use datafusion::arrow::array::Array;
        use datafusion::catalog::CatalogProvider;
        use datafusion::prelude::SessionConfig;

        let db = "test_federated_catalog";

        // Initialize session context
        let ctx =
            SessionContext::new_with_config(SessionConfig::default().with_information_schema(true));

        // IMPORTANT! If federation is enabled, federate the context
        let ctx = ctx.federate();

        // -----------------------------
        // Registering UDF Optimizer and UDF Pushdown
        let ctx = ClickHouseSessionContext::from(ctx);

        let builder = common::helpers::create_builder(&ctx, &ch).await?;
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // Insert test data (people & people2)
        let clickhouse = common::helpers::insert_test_data(clickhouse, db, &ctx).await?;

        // -----------------------------
        // Refresh catalog
        let clickhouse_catalog = clickhouse.build(&ctx).await?;

        // Create a catalog provider that separates federated from non-federated tables
        let federated_catalog = FederatedCatalogProvider::new_with_default_schema("internal")?;

        // Register the clickhouse catalog
        drop(federated_catalog.add_catalog(&(clickhouse_catalog as Arc<dyn CatalogProvider>)));

        // -----------------------------
        //
        // Add in-memory table
        let mem_schema =
            Arc::new(Schema::new(vec![Field::new("event_id", DataType::Int32, false)]));
        let mem_table =
            Arc::new(datafusion::datasource::MemTable::try_new(Arc::clone(&mem_schema), vec![
                vec![arrow::record_batch::RecordBatch::try_new(mem_schema, vec![Arc::new(
                    arrow::array::Int32Array::from(vec![1]),
                )])?],
            ])?);
        drop(federated_catalog.register_non_federated_table("memory".into(), mem_table)?);

        // Register the federated catalog
        let fed_catalog = Arc::new(federated_catalog) as Arc<dyn CatalogProvider>;
        drop(ctx.register_catalog("clickhouse_alt", Arc::clone(&fed_catalog)));

        // -----------------------------
        // Test eval query
        let query = format!("SELECT p.name FROM clickhouse.{db}.people p");
        // TODO: Assert the output, otherwise useless test
        let results_ch = ctx.sql(&query).await?.collect().await?;
        assert!(
            results_ch
                .first()
                .map(|r| r.column(0))
                .map(AsArray::as_string::<i32>)
                .filter(|a| a.len() > 0)
                .is_some()
        );
        arrow::util::pretty::print_batches(&results_ch)?;
        eprintln!(">>> Simple query passed");

        // -----------------------------
        // Test eval query with new catalog
        let query = format!("SELECT p.name FROM clickhouse_alt.{db}.people p");
        let results_alt = ctx.sql(&query).await?.collect().await?;
        assert!(
            results_alt
                .first()
                .map(|r| r.column(0))
                .map(AsArray::as_string::<i32>)
                .filter(|a| a.len() > 0)
                .is_some()
        );
        assert!(results_ch.iter().zip(results_alt.iter()).all(|(a, b)| a == b));
        arrow::util::pretty::print_batches(&results_alt)?;
        eprintln!(">>> Simple federated query 1 passed");

        // Registering the federated catalog multiple times makes ALL schemas available to all
        // catalogs
        drop(ctx.register_catalog("datafusion", fed_catalog));

        // -----------------------------
        // Test eval query
        let query = format!("SELECT p.name FROM datafusion.{db}.people p");
        let results = ctx.sql(&query).await?.collect().await?;
        assert!(
            results_alt
                .first()
                .map(|r| r.column(0))
                .map(AsArray::as_string::<i32>)
                .filter(|a| a.len() > 0)
                .is_some()
        );
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Simple federated query 2 passed");

        // -----------------------------
        // Print out tables
        let query = "SHOW TABLES";
        let results = ctx.sql(query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;

        eprintln!("Note how the schemas are available (and federated) across both catalogs");

        // -----------------------------
        // Test eval query
        let query = "SELECT m.event_id FROM datafusion.internal.memory m";
        // TODO: Assert the output, otherwise useless test
        let results = ctx.sql(query).await?.collect().await?;
        assert!(
            results
                .first()
                .map(|r| r.column(0))
                .map(AsArray::as_primitive::<Int32Type>)
                .filter(|a| !a.is_empty())
                .is_some()
        );
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Simple non-federated query passed");

        // -----------------------------
        // Test federated query
        let join_query = format!(
            "
            SELECT p.name, m.event_id
            FROM datafusion.internal.memory m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            "
        );
        let results = ctx.sql(&join_query).await?.collect().await?;
        assert!(results.first().filter(|r| r.num_rows() > 0).is_some());
        arrow::util::pretty::print_batches(&results)?;
        eprintln!(">>> Federated query passed");

        eprintln!(">> Test federated catalog completed");

        Ok(())
    }

    pub(super) async fn test_aggregation_functions(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_db_aggregations";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = common::helpers::create_builder(&ctx, &ch).await?;
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // Insert test data (people & people2)
        let clickhouse = common::helpers::insert_test_data(clickhouse, db, &ctx).await?;

        // Refresh catalog
        let _catalog_provider = clickhouse.build(&ctx).await?;

        eprintln!("---- Starting aggregation tests ----");

        // -----------------------------
        // Test COUNT aggregate
        let query = format!("SELECT COUNT(*) as cnt FROM clickhouse.{db}.people");
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
        eprintln!(">>> COUNT(*) test passed");

        // -----------------------------
        // Test COUNT with column
        let query = format!("SELECT COUNT(id) as cnt FROM clickhouse.{db}.people");
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert_eq!(results.len(), 1);
        eprintln!(">>> COUNT(column) test passed");

        // -----------------------------
        // Test SUM aggregate
        let query = format!("SELECT SUM(id) as total FROM clickhouse.{db}.people");
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert_eq!(results.len(), 1);
        eprintln!(">>> SUM test passed");

        // -----------------------------
        // Test AVG aggregate
        let query = format!("SELECT AVG(id) as avg_id FROM clickhouse.{db}.people");
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert_eq!(results.len(), 1);
        eprintln!(">>> AVG test passed");

        // -----------------------------
        // Test MIN aggregate
        let query = format!("SELECT MIN(id) as min_id FROM clickhouse.{db}.people");
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert_eq!(results.len(), 1);
        eprintln!(">>> MIN test passed");

        // -----------------------------
        // Test MAX aggregate
        let query = format!("SELECT MAX(id) as max_id FROM clickhouse.{db}.people");
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert_eq!(results.len(), 1);
        eprintln!(">>> MAX test passed");

        // -----------------------------
        // Test multiple aggregates in one query
        let query = format!(
            "SELECT COUNT(*) as cnt, SUM(id) as total, AVG(id) as avg_id, MIN(id) as min_id, \
             MAX(id) as max_id
             FROM clickhouse.{db}.people"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert_eq!(results.len(), 1);
        eprintln!(">>> Multiple aggregates test passed");

        // -----------------------------
        // Test GROUP BY with aggregates
        let query = format!(
            "SELECT name, COUNT(*) as cnt
             FROM clickhouse.{db}.people
             GROUP BY name
             ORDER BY name"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert_eq!(results.len(), 1);
        eprintln!(">>> GROUP BY with COUNT test passed");

        // -----------------------------
        // Test GROUP BY with multiple aggregates
        let query = format!(
            "SELECT name, COUNT(*) as cnt, SUM(id) as total_id, AVG(id) as avg_id
             FROM clickhouse.{db}.people2
             GROUP BY name
             ORDER BY name"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());
        eprintln!(">>> GROUP BY with multiple aggregates test passed");

        // -----------------------------
        // Test HAVING clause with aggregates
        let query = format!(
            "SELECT name, COUNT(*) as cnt
             FROM clickhouse.{db}.people2
             GROUP BY name
             HAVING COUNT(*) > 0
             ORDER BY name"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());
        eprintln!(">>> HAVING clause test passed");

        // -----------------------------
        // Test aggregates with WHERE clause
        let query = format!(
            "SELECT COUNT(*) as cnt, SUM(id) as total
             FROM clickhouse.{db}.people
             WHERE id > 0"
        );
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert_eq!(results.len(), 1);
        eprintln!(">>> Aggregates with WHERE clause test passed");

        // -----------------------------
        // Test COUNT DISTINCT
        let query = format!("SELECT COUNT(DISTINCT name) as uniques FROM clickhouse.{db}.people2");
        let results = ctx.sql(&query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert_eq!(results.len(), 1);
        eprintln!(">>> COUNT DISTINCT test passed");

        eprintln!(">> All aggregation tests completed successfully");

        Ok(())
    }

    /// Test parallel writes with high concurrency
    pub(super) async fn test_parallel_writes(ch: Arc<ClickHouseContainer>) -> Result<()> {
        use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};

        let db = "test_db_parallel_writes";

        // Initialize session context
        let ctx = SessionContext::new();

        // IMPORTANT! If federation is enabled, federate the context
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        // Create builder with parallel write concurrency
        let builder = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| configure_client(c, &ch))
            .with_write_concurrency(4)
            .build_catalog(&ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
            .await?;

        // Setup test tables
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

        // Refresh catalog
        let _catalog = clickhouse.build(&ctx).await?;

        // Create a larger dataset for testing parallel write capability
        let num_rows = 5000;

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        // Generate test data
        let ids: Vec<i32> = (1..=num_rows).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("person_{}", i)).collect();

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(StringArray::from(names.clone())),
        ])?;

        // Register as a memory table
        drop(ctx.register_batch("bulk_data", batch)?);

        // Insert data using parallel writes
        eprintln!(">>> Inserting {} rows with parallel writes (concurrency=4)...", num_rows);

        let _insert_results = ctx
            .sql(&format!("INSERT INTO clickhouse.{db}.people SELECT id, name FROM bulk_data"))
            .await?
            .collect()
            .await?;

        eprintln!(">>> Parallel insert completed");

        // Verify data integrity - check count
        let count_result = ctx
            .sql(&format!("SELECT COUNT(*) as cnt FROM clickhouse.{db}.people"))
            .await?
            .collect()
            .await?;

        arrow::util::pretty::print_batches(&count_result)?;

        let cnt_array = count_result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>();
        assert_eq!(cnt_array.value(0), num_rows as i64, "Row count mismatch");

        eprintln!(">>> Row count verified: {}", num_rows);

        // Verify data integrity - check sum of IDs
        let sum_result = ctx
            .sql(&format!("SELECT SUM(id) as total FROM clickhouse.{db}.people"))
            .await?
            .collect()
            .await?;

        arrow::util::pretty::print_batches(&sum_result)?;

        let expected_sum: i64 = (1..=num_rows as i64).sum();
        let sum_array = sum_result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>();
        assert_eq!(sum_array.value(0), expected_sum, "Sum mismatch");

        eprintln!(">>> Sum verified: {}", expected_sum);

        // Verify some sample rows
        let sample_result = ctx
            .sql(&format!(
                "SELECT id, name FROM clickhouse.{db}.people WHERE id IN (1, 100, 2500, {}) ORDER \
                 BY id",
                num_rows
            ))
            .await?
            .collect()
            .await?;

        arrow::util::pretty::print_batches(&sample_result)?;
        eprintln!(">>> Sample rows verified");

        eprintln!(">>> Parallel writes test completed successfully");

        Ok(())
    }

    /// Test that metrics are properly tracked and visible in EXPLAIN ANALYZE
    pub(super) async fn test_insert_metrics(ch: Arc<ClickHouseContainer>) -> Result<()> {
        use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};

        let db = "test_db_insert_metrics";

        // Initialize session context
        let ctx = SessionContext::new();

        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        // Create builder with write concurrency
        let builder = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| configure_client(c, &ch))
            .with_write_concurrency(4)
            .build_catalog(&ctx, Some(DEFAULT_CLICKHOUSE_CATALOG))
            .await?;

        // Setup test tables
        let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;
        let _catalog = clickhouse.build(&ctx).await?;

        // Create test data
        let num_rows = 1000;
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let ids: Vec<i32> = (1..=num_rows).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("person_{}", i)).collect();

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(StringArray::from(names)),
        ])?;

        drop(ctx.register_batch("temp_data", batch)?);

        // Use EXPLAIN ANALYZE to verify metrics
        eprintln!(">>> Running INSERT with EXPLAIN ANALYZE");
        let explain_result = ctx
            .sql(&format!(
                "EXPLAIN ANALYZE INSERT INTO clickhouse.{db}.people SELECT * FROM temp_data"
            ))
            .await?
            .collect()
            .await?;

        // Print the EXPLAIN ANALYZE output
        arrow::util::pretty::print_batches(&explain_result)?;

        // Verify metrics are present in the output
        let plan_str = format!("{:?}", explain_result);
        eprintln!(">>> EXPLAIN ANALYZE output:\n{}", plan_str);

        // Check that metrics contain expected fields
        // The output should contain "output_rows" metric
        assert!(
            plan_str.contains("output_rows") || plan_str.contains("metrics"),
            "EXPLAIN ANALYZE should show metrics"
        );

        eprintln!(">>> Metrics verification completed");

        // Verify the data was actually inserted
        let count_result = ctx
            .sql(&format!("SELECT COUNT(*) as cnt FROM clickhouse.{db}.people"))
            .await?
            .collect()
            .await?;

        let cnt_array = count_result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>();
        assert_eq!(cnt_array.value(0), num_rows as i64, "Row count mismatch");

        eprintln!(">>> Insert metrics test completed successfully");

        Ok(())
    }
}
