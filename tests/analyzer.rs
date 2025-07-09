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
#[cfg(feature = "test-utils")]
e2e_test!(err => builder, tests::test_clickhouse_udf_pushdown, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
mod tests {
    use std::sync::Arc;

    use clickhouse_arrow::test_utils::ClickHouseContainer;
    use clickhouse_datafusion::ClickHouseSessionContext;
    #[cfg(feature = "federation")]
    use clickhouse_datafusion::federation::FederatedContext as _;
    use datafusion::arrow;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::{
        CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
    };
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use tracing::{error, info};

    use super::*;

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
        let clickhouse = common::helpers::insert_test_data_silent(clickhouse, db, &ctx).await?;

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
            "SELECT id, clickhouse(`arrayJoin`(names), 'Utf8') as names FROM \
             clickhouse.{db}.people2"
        );
        let results = ctx
            .sql(&query)
            .await
            .inspect_err(|error| error!("Error exe 0 query: {error}"))?
            .collect()
            .await?;
        arrow::util::pretty::print_batches(&results)?;
        info!(">>> Projection test custom Analyzer 0 passed");

        // // -----------------------------
        // // Test projection with custom Analyzer
        // let query = format!(
        //     "
        //     SELECT p.name
        //         , m.event_id
        //         , clickhouse(exp(p2.id), 'Float64')
        //         , p2.names
        //     FROM memory.internal.mem_events m
        //     JOIN clickhouse.{db}.people p ON p.id = m.event_id
        //     JOIN (
        //         SELECT id, clickhouse(`arrayJoin`(names), 'Utf8') as names
        //         FROM clickhouse.{db}.people2
        //     ) p2 ON p.id = p2.id
        //     "
        // );
        // let results = ctx
        //     .sql(&query)
        //     .await
        //     .inspect_err(|error| error!("Error exe 1 query: {error}"))?
        //     .collect()
        //     .await?;
        // arrow::util::pretty::print_batches(&results)?;
        // info!(">>> Projection test custom Analyzer 1 passed");

        // // -----------------------------
        // // Test projection with custom Analyzer
        // let query = format!(
        //     "
        //     SELECT p.name
        //         , m.event_id
        //         , clickhouse(exp(p2.id), 'Float64')
        //         , clickhouse(concat(p2.name, 'hello'), 'Utf8')
        //     FROM memory.internal.mem_events m
        //     JOIN clickhouse.{db}.people p ON p.id = m.event_id
        //     JOIN clickhouse.{db}.people2 p2 ON p.id = p2.id
        //     "
        // );
        // let results = ctx
        //     .sql(&query)
        //     .await
        //     .inspect_err(|error| error!("Error exe 2 query: {}", error))?
        //     .collect()
        //     .await?;
        // arrow::util::pretty::print_batches(&results)?;
        // info!(">>> Projection test custom Analyzer 2 passed");

        // // -----------------------------
        // // Test projection with custom Analyzer
        // let query = format!(
        //     "
        //     SELECT p.name
        //         , m.event_id
        //         , clickhouse(exp(p2.id), 'Float64')
        //         , clickhouse(concat(p2.names, 'hello'), 'Utf8')
        //     FROM memory.internal.mem_events m
        //     JOIN clickhouse.{db}.people p ON p.id = m.event_id
        //     JOIN (
        //         SELECT id
        //             , clickhouse(`arrayJoin`(names), 'Utf8') as names
        //         FROM clickhouse.{db}.people2
        //     ) p2 ON p.id = p2.id
        //     "
        // );
        // let results = ctx
        //     .sql(&query)
        //     .await
        //     .inspect_err(|error| error!("Error exe 3 query: {}", error))?
        //     .collect()
        //     .await?;
        // arrow::util::pretty::print_batches(&results)?;
        // info!(">>> Projection test custom Analyzer 3 passed");

        // // -----------------------------
        // // Test projection with custom Analyzer
        // let query = format!(
        //     "
        //     SELECT p.name
        //         , m.event_id
        //         , clickhouse(exp(p2.id), 'Float64')
        //         , concat(p2.names, 'hello')
        //     FROM memory.internal.mem_events m
        //     JOIN clickhouse.{db}.people p ON p.id = m.event_id
        //     JOIN (
        //         SELECT id
        //             , clickhouse(`arrayJoin`(names), 'Utf8') as names
        //         FROM clickhouse.{db}.people2
        //     ) p2 ON p.id = p2.id
        //     "
        // );
        // let results = ctx
        //     .sql(&query)
        //     .await
        //     .inspect_err(|error| error!("Error exe 4 query: {}", error))?
        //     .collect()
        //     .await?;
        // arrow::util::pretty::print_batches(&results)?;
        // info!(">>> Projection test custom Analyzer 4 passed");

        Ok(())
    }
}
