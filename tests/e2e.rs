#![allow(unused_crate_dependencies)]

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
test_func!(test_builder, tests::test_clickhouse_builder);
#[cfg(feature = "test-utils")]
e2e_test!(builder, test_builder, TRACING_DIRECTIVES, None);

// Test insert data
test_func!(test_insert, tests::test_insert_data);
#[cfg(feature = "test-utils")]
e2e_test!(insert, test_insert, TRACING_DIRECTIVES, None);

// Test clickhouse udf pushdown
test_func!(test_udf_pushdown, tests::test_clickhouse_udf_pushdown);
#[cfg(feature = "test-utils")]
e2e_test!(clickhouse_udf, test_udf_pushdown, TRACING_DIRECTIVES, None);

// -- FEDERATION --

// Test simple clickhouse udf
#[cfg(all(feature = "test-utils", feature = "federation"))]
test_func!(test_simple_udf, tests::test_simple_clickhouse_udf);
#[cfg(all(feature = "test-utils", feature = "federation"))]
e2e_test!(simple_udf, test_simple_udf, TRACING_DIRECTIVES, None);

// Test FederatedCatalogProvider
#[cfg(all(feature = "test-utils", feature = "federation"))]
test_func!(test_federated_catalog, tests::test_federated_catalog);
#[cfg(all(feature = "test-utils", feature = "federation"))]
e2e_test!(federated_catalog, test_federated_catalog, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
mod tests {
    use std::sync::Arc;

    use clickhouse_arrow::test_utils::ClickHouseContainer;
    use clickhouse_datafusion::ClickHouseSessionContext;
    #[cfg(feature = "federation")]
    use clickhouse_datafusion::federation::FederatedContext as _;
    use datafusion::arrow;
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::catalog::{
        CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
    };
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;
    use tracing::{error, info};

    use super::*;

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
        info!("Query completed successfully");

        // Test registering non-existent table
        let result = clickhouse.register_table("{db}.missing", None::<TableReference>, &ctx).await;
        assert!(result.is_err(), "Expected table not found error");
        info!(">>> Unexpected table passed");

        // Register existing table
        clickhouse.register_table(&format!("{db}.people"), Some("people_alias"), &ctx).await?;
        info!(">>> Registered existing table into alias `people_alias`");

        eprintln!(">> Test builder completed");

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
        info!(">>> Select query on people passed");

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
        info!(">>> Select query on people2 passed");

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
        info!(">>> Join query on people passed");

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
            info!(">>> Unnest query passed");
        }

        // -----------------------------
        // Test datafusion unnest works when federation is on
        #[cfg(feature = "federation")]
        {
            let results = ctx
                .sql(&format!("SELECT id, unnest(names) FROM clickhouse.{db}.people2"))
                .await?
                .collect()
                .await?;
            arrow::util::pretty::print_batches(&results)?;
            info!(">>> Unnest query passed");
        }

        eprintln!(">> Test insert completed");

        Ok(())
    }

    #[expect(clippy::too_many_lines)]
    pub(super) async fn test_clickhouse_udf_pushdown(ch: Arc<ClickHouseContainer>) -> Result<()> {
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
        let mem_schema =
            Arc::new(Schema::new(vec![Field::new("event_id", DataType::Int32, false)]));
        let mem_table =
            Arc::new(datafusion::datasource::MemTable::try_new(Arc::clone(&mem_schema), vec![
                vec![arrow::record_batch::RecordBatch::try_new(mem_schema, vec![Arc::new(
                    arrow::array::Int32Array::from(vec![1]),
                )])?],
            ])?);
        let mem_catalog = Arc::new(MemoryCatalogProvider::new());
        let mem_schema = Arc::new(MemorySchemaProvider::new());
        drop(mem_schema.register_table("mem_events".into(), mem_table)?);
        drop(mem_catalog.register_schema("internal", mem_schema)?);
        drop(ctx.register_catalog("memory", mem_catalog));

        // -----------------------------
        // Test projection with custom Analyzer
        let query = format!(
            "SELECT p.name
                , m.event_id
                , clickhouse(exp(p2.id), 'Float64')
                , p2.names
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            JOIN (
                SELECT id, clickhouse(`arrayJoin`(names), 'Utf8') as names
                FROM clickhouse.{db}.people2
            ) p2 ON p.id = p2.id
            "
        );
        let results = ctx
            .sql(&query)
            .await
            .inspect_err(|error| error!("Error exe 1 query: {error}"))?
            .collect()
            .await?;
        arrow::util::pretty::print_batches(&results)?;
        info!(">>> Projection test custom Analyzer 1 passed");

        // -----------------------------
        // Test projection with custom Analyzer
        let query = format!(
            "SELECT p.name
                , m.event_id
                , clickhouse(exp(p2.id), 'Float64')
                , clickhouse(concat(p2.name, 'hello'), 'Utf8')
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id
            "
        );
        let results = ctx
            .sql(&query)
            .await
            .inspect_err(|error| error!("Error exe 2 query: {}", error))?
            .collect()
            .await?;
        arrow::util::pretty::print_batches(&results)?;
        info!(">>> Projection test custom Analyzer 2 passed");

        // -----------------------------
        // Test projection with custom Analyzer
        let query = format!(
            "SELECT p.name
                , m.event_id
                , clickhouse(exp(p2.id), 'Float64')
                , clickhouse(concat(p2.names, 'hello'), 'Utf8')
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            JOIN (
                SELECT id
                    , clickhouse(`arrayJoin`(names), 'Utf8') as names
                FROM clickhouse.{db}.people2
            ) p2 ON p.id = p2.id
            "
        );
        let results = ctx
            .sql(&query)
            .await
            .inspect_err(|error| error!("Error exe 3 query: {}", error))?
            .collect()
            .await?;
        arrow::util::pretty::print_batches(&results)?;
        info!(">>> Projection test custom Analyzer 3 passed");

        // -----------------------------
        // Test projection with custom Analyzer
        let query = format!(
            "SELECT p.name
                , m.event_id
                , clickhouse(exp(p2.id), 'Float64')
                , concat(p2.names, 'hello')
            FROM memory.internal.mem_events m
            JOIN clickhouse.{db}.people p ON p.id = m.event_id
            JOIN (
                SELECT id
                    , clickhouse(`arrayJoin`(names), 'Utf8') as names
                FROM clickhouse.{db}.people2
            ) p2 ON p.id = p2.id
            "
        );
        let results = ctx
            .sql(&query)
            .await
            .inspect_err(|error| error!("Error exe 4 query: {}", error))?
            .collect()
            .await?;
        arrow::util::pretty::print_batches(&results)?;
        info!(">>> Projection test custom Analyzer 4 passed");

        // -----------------------------
        // Test HOF
        // let query = format!(
        //     "SELECT p.name
        //         , m.event_id
        //         , p2.id
        //         , p2.names
        //     FROM memory.internal.mem_events m
        //     JOIN clickhouse.{db}.people p ON p.id = m.event_id
        //     JOIN (
        //         SELECT id, clickhouse(`arrayMap`(x -> concat(x, ' hello'), names), 'Utf8') as
        // names         FROM clickhouse.{db}.people2
        //     ) p2 ON p.id = p2.id
        //     "
        // );
        // TODO: Remove
        // let query = format!(
        //     "SELECT id
        //         , clickhouse(exp(p2.id), 'Float64')
        //         , clickhouse_apply(arrayMap(x => concat(x, ' hello'), names), 'Utf8') as names
        //     FROM clickhouse.{db}.people2 as p2"
        // );
        // let query = format!(
        //     "SELECT id, clickhouse(exp(p2.id), 'Float64')
        //     FROM clickhouse.{db}.people2 as p2"
        // );
        // let query = format!(
        //     "SELECT p.name
        //         , m.event_id
        //         , clickhouse(exp(p2.id), 'Float64')
        //         , concat(p2.names, 'hello')
        //     FROM memory.internal.mem_events m
        //     JOIN clickhouse.{db}.people p ON p.id = m.event_id
        //     JOIN (
        //         SELECT id
        //             , clickhouse(`arrayJoin`(names), 'Utf8') as names
        //         FROM clickhouse.{db}.people2
        //     ) p2 ON p.id = p2.id"
        // );
        // let results = ctx
        //     .sql(&query)
        //     .await
        //     .inspect_err(|error| error!("Error exe 1 query: {error}"))?
        //     .collect()
        //     .await?;
        // arrow::util::pretty::print_batches(&results)?;
        // info!(">>> HOF passed");

        Ok(())

        // TODO: Figure out how to use Higher Order Functions in ClickHouse
        //
        // There are a couple ways to do this, but I believe the best way will involve:
        // 1. Adding AST parsing abilities for `->`
        // 2. Figuring out how to pass in columns if #1 doesn't work.
        // 3. Or use a custom recognized symbol for `->` and replacing in PlaceholderFunction
        //
        //
        // // -----------------------------
        // //
        // // Test projection with custom Analyzer
        // header("Testing projection with custom Analyzer 5", false);

        // let query = "
        //     SELECT p.name
        //         , m.event_id
        //         , clickhouse('arrayMap(x -> concat(x, ''hello''), names)', 'List(Utf8)')
        //     FROM memory.internal.mem_events m
        //     JOIN clickhouse.test_db.people p ON p.id = m.event_id
        //     JOIN clickhouse.test_db.people2 p2 ON p.id = p2.id
        //     ";
        // let df = ctx
        //     .sql(query)
        //     .await
        //     .inspect_err(|error| error!("Error exe 5 query: {}", error))?;
        // let results = df.collect().await?;
        // arrow::util::pretty::print_batches(&results)?;
        // info!(">>> Projection test custom Analyzer 5 passed");
    }

    // TODO: Add notes, examples, and update README to reflect that this is useful ONLY when
    // federation is enabled. That is because federation naturally "pushes" the UDF to the federated
    // table and it is then converted into SQL for the remote database.
    #[cfg(feature = "federation")]
    pub(super) async fn test_simple_clickhouse_udf(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_db_simple_udf";

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
        // Test simple usage first
        let results = ctx
            .sql(&format!(
                "SELECT name
                        , clickhousefunc('exp(id)', 'Float64')
                        , clickhousefunc('upper(name)', 'Utf8')
                    FROM clickhouse.{db}.people"
            ))
            .await?
            .collect()
            .await?;
        assert!(!results.is_empty());
        arrow::util::pretty::print_batches(&results)?;
        let batch = results.first().unwrap();
        assert_eq!(batch.num_rows(), 2);
        info!(">>> clickhousefunc test 1 passed");

        Ok(())
    }

    #[cfg(feature = "federation")]
    pub(super) async fn test_federated_catalog(ch: Arc<ClickHouseContainer>) -> Result<()> {
        use clickhouse_datafusion::federation;
        use datafusion::arrow::array::Array;
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
        let federated_catalog =
            federation::catalog::FederatedCatalogProvider::new_with_default_schema("internal")?;

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
        // Test simple query
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
        info!("Simple query passed");

        // -----------------------------
        // Test simple query with new catalog
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
        info!("Simple federated query 1 passed");

        // Registering the federated catalog multiple times makes ALL schemas available to all
        // catalogs
        drop(ctx.register_catalog("datafusion", fed_catalog));

        // -----------------------------
        // Test simple query
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
        info!("Simple federated query 2 passed");

        // -----------------------------
        // Print out tables
        let query = "SHOW TABLES";
        let results = ctx.sql(query).await?.collect().await?;
        arrow::util::pretty::print_batches(&results)?;

        eprintln!("Note how the schemas are available (and federated) across both catalogs");

        // -----------------------------
        // Test simple query
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
        info!("Simple non-federated query passed");

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
        info!("Federated query passed");

        eprintln!(">> Test federated catalog completed");

        Ok(())
    }
}

// #[cfg(all(feature = "test-utils", feature = "federation"))]
// mod tests_federation {
//     use std::sync::Arc;

//     use clickhouse_arrow::test_utils::ClickHouseContainer;
//     use clickhouse_datafusion::{
//         ClickHouseEngine, ClickHouseSessionContext, DEFAULT_CLICKHOUSE_CATALOG,
//     };
//     use datafusion::arrow;
//     use datafusion::arrow::datatypes::{DataType, Field, Schema};
//     use datafusion::error::Result;
//     use datafusion::prelude::SessionContext;
//     use tracing::{debug, error};

//     use super::*;

//     pub(super) async fn test_clickhouse_federation(ch: Arc<ClickHouseContainer>) -> Result<()> {
//         use clickhouse_datafusion::federation;
//         use datafusion::arrow::array::Int32Array;
//         use datafusion::arrow::record_batch::RecordBatch;

//         let db = "test_db_federation";

//         // Initialize session context
//         let ctx = SessionContext::new();

//         // -----------------------------
//         // Registering UDF Optimizer and UDF Pushdown
//         let ctx = ClickHouseSessionContext::new(ctx, None);

//         let builder = common::helpers::create_builder(&ctx, &ch).await?;
//         let clickhouse = common::helpers::setup_test_tables(builder, db, &ctx).await?;

//         // -----------------------------
//         // Insert data using SQL
//         let insert_sql = format!(
//             "INSERT INTO clickhouse.{db}.people (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
//         );
//         let results = ctx.sql(&insert_sql).await?.collect().await?;
//         datafusion::arrow::util::pretty::print_batches(&results)?;
//         info!(">>> Inserted data into table `{db}.people`");

//         // -----------------------------
//         // Add another table
//         let schema_people2 = Arc::new(Schema::new(vec![
//             Field::new("id", DataType::Int32, false),
//             Field::new("name", DataType::Utf8, false),
//             Field::new_list("names", Field::new_list_field(DataType::Utf8, false), false),
//         ]));

//         let clickhouse = clickhouse
//             .with_table("people2", ClickHouseEngine::MergeTree, schema_people2.clone())
//             .update_create_options(|opts| opts.with_order_by(&["id".into()]))
//             .create(&ctx)
//             .await
//             .expect("table creation");

//         // -----------------------------
//         // Insert data using SQL
//         let insert_sql = format!(
//             "
//             INSERT INTO clickhouse.{db}.people2 (id, name, names)
//             VALUES
//                 (1, 'Bob', make_array('Buddha', 'Zugus', 'Lulu', 'Kitty', 'Mitty')),
//                 (2, 'Alice', make_array('Jazz', 'Kaya', 'Vienna', 'Susie', 'Georgie')),
//                 (3, 'Charlie', make_array('Susana', 'Adrienne', 'Blayke'))
//             "
//         );
//         let insert_df = ctx.sql(&insert_sql).await?.collect().await?;
//         datafusion::arrow::util::pretty::print_batches(&insert_df)?;
//         info!(">>> Inserted data into table `{db}.people2`");

//         // -----------------------------
//         //
//         // Create ch catalog provider and external catalog
//         //
//         // NOTE: Notice build_federated is NOT used. That is to show that federation can be done
//         // manually which occurs after the creation of the in memory table
//         let ctx = clickhouse.build_federated(&ctx).await?;

//         // Create a catalog provider that separates federated from non-federated tables
//         let external_catalog = federation::external::FederatedCatalogProvider::new();

//         // NOTE: The clickhouse catalog can be added here as well. Either way works
//         // external_catalog.add_catalog(CLICKHOUSE_CATALOG_NAME.into(),
//         // Arc::new(clickhouse_catalog));

//         // Add in-memory table
//         let mem_schema =
//             Arc::new(Schema::new(vec![Field::new("event_id", DataType::Int32, false)]));
//         let mem_table =
//             Arc::new(datafusion::datasource::MemTable::try_new(Arc::clone(&mem_schema), vec![
//                 vec![RecordBatch::try_new(mem_schema,
// vec![Arc::new(Int32Array::from(vec![1]))])?],             ])?);
//         let _ = external_catalog.register_non_federated_table("mem_events".into(), mem_table)?;

//         // -----------------------------
//         // Federate session context
//         //
//         // NOTE: Can choose to get a context later or earlier using `build_federated`
//         let ctx = federation::federate_session_context(Some(&ctx));
//         // info!(">>> Session context federated");

//         // -----------------------------
//         // Register federated catalogs
//         let _ = ctx.register_catalog("memory", Arc::new(external_catalog));
//         // let _ = ctx.register_catalog(DEFAULT_CLICKHOUSE_CATALOG, clickhouse_catalog);
//         info!(">>> Federated catalog registered");

//         // -----------------------------
//         // Test select query
//         //
//         // TODO: Assert the output, otherwise useless test
//         let select_query = format!("SELECT name FROM clickhouse.{db}.people");
//         let df = ctx.sql(&select_query).await?;
//         let results = df.collect().await?;
//         arrow::util::pretty::print_batches(&results)?;
//         info!("Select query passed");

//         // -----------------------------
//         // Test select query on mem table
//         //
//         // TODO: Assert the output, otherwise useless test
//         let df = ctx.sql("SELECT m.event_id FROM memory.internal.mem_events m").await?;
//         let results = df.collect().await?;
//         arrow::util::pretty::print_batches(&results)?;
//         info!("Select query passed (memory)");

//         // -----------------------------
//         // Test federated query
//         let join_query = format!(
//             "
//             SELECT p.name, m.event_id
//             FROM memory.internal.mem_events m
//             JOIN clickhouse.{db}.people p ON p.id = m.event_id
//             "
//         );

//         // TODO: Assert the output, otherwise useless test
//         let df = ctx.sql(&join_query).await?;
//         let results = df.collect().await?;
//         arrow::util::pretty::print_batches(&results)?;
//         info!("Federated query passed");

//         // -----------------------------
//         // Test projection with custom Analyzer
//         let query = format!(
//             "
//             SELECT p.name
//                 , m.event_id
//                 , clickhouse(exp(p2.id), 'Float64')
//                 , p2.names
//             FROM memory.internal.mem_events m
//             JOIN clickhouse.{db}.people p ON p.id = m.event_id
//             JOIN (
//                 SELECT id, clickhouse(`arrayJoin`(names), 'Utf8') as names
//                 FROM clickhouse.{db}.people2
//             ) p2 ON p.id = p2.id
//             "
//         );
//         let df = ctx.sql(&query).await.inspect_err(|error| error!("Error exe 1 query:
// {error}"))?;         let results = df.collect().await?;
//         arrow::util::pretty::print_batches(&results)?;
//         info!(">>> Projection test custom Analyzer 1 passed");

//         // -----------------------------
//         // Test projection with custom Analyzer
//         let query = format!(
//             "
//             SELECT p.name
//                 , m.event_id
//                 , clickhouse(exp(p2.id), 'Float64')
//                 , clickhouse(concat(p2.name, 'hello'), 'Utf8')
//             FROM memory.internal.mem_events m
//             JOIN clickhouse.{db}.people p ON p.id = m.event_id
//             JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id
//             "
//         );
//         let df =
//             ctx.sql(&query).await.inspect_err(|error| error!("Error exe 2 query: {}", error))?;
//         let results = df.collect().await?;
//         arrow::util::pretty::print_batches(&results)?;
//         info!(">>> Projection test custom Analyzer 2 passed");

//         // -----------------------------
//         // Test projection with custom Analyzer
//         let query = format!(
//             "
//             SELECT p.name
//                 , m.event_id
//                 , clickhouse(exp(p2.id), 'Float64')
//                 , clickhouse(concat(p2.names, 'hello'), 'Utf8')
//             FROM memory.internal.mem_events m
//             JOIN clickhouse.{db}.people p ON p.id = m.event_id
//             JOIN (
//                 SELECT id
//                     , clickhouse(`arrayJoin`(names), 'Utf8') as names
//                 FROM clickhouse.{db}.people2
//             ) p2 ON p.id = p2.id
//             "
//         );
//         let df =
//             ctx.sql(&query).await.inspect_err(|error| error!("Error exe 3 query: {}", error))?;
//         let results = df.collect().await?;
//         arrow::util::pretty::print_batches(&results)?;
//         info!(">>> Projection test custom Analyzer 3 passed");

//         // -----------------------------
//         // Test projection with custom Analyzer
//         let query = format!(
//             "
//             SELECT p.name
//                 , m.event_id
//                 , clickhouse(exp(p2.id), 'Float64')
//                 , concat(p2.names, 'hello')
//             FROM memory.internal.mem_events m
//             JOIN clickhouse.{db}.people p ON p.id = m.event_id
//             JOIN (
//                 SELECT id
//                     , clickhouse(`arrayJoin`(names), 'Utf8') as names
//                 FROM clickhouse.{db}.people2
//             ) p2 ON p.id = p2.id
//             "
//         );
//         let df =
//             ctx.sql(&query).await.inspect_err(|error| error!("Error exe 4 query: {}", error))?;
//         let results = df.collect().await?;
//         arrow::util::pretty::print_batches(&results)?;
//         info!(">>> Projection test custom Analyzer 4 passed");

//         Ok(())
//     }
// }
