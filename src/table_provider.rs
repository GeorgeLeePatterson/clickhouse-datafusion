use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;

use crate::connection::ClickHouseConnectionPool;
use crate::sink::ClickHouseDataSink;
use crate::sql::SqlTable;

// TODO: Docs
//
/// A custom [`TableProvider`] for `ClickHouse` tables.
#[derive(Debug)]
pub struct ClickHouseTableProvider {
    pub(crate) table:  TableReference,
    pub(crate) reader: SqlTable,
    pub(crate) writer: ClickHouseConnectionPool,
    // TODO: Update SQLExecutor impl to leverage this
    #[expect(unused)]
    pub(crate) exprs:  Option<Vec<Expr>>, // Support pushing down expressions
}

impl ClickHouseTableProvider {
    /// Creates a new `TableProvide`r, fetching the schema from `ClickHouse` if not provided.
    ///
    /// # Errors
    /// - Returns an error if the `SQLTable` creation fails.
    pub async fn try_new(pool: ClickHouseConnectionPool, table: TableReference) -> Result<Self> {
        let writer = pool.clone();
        let inner = SqlTable::new("clickhouse", pool, table.clone()).await?;
        Ok(Self { reader: inner, table, writer, exprs: None })
    }

    /// Creates a new `TableProvider` with a pre-fetched schema.
    pub fn new_with_schema(
        pool: ClickHouseConnectionPool,
        table: TableReference,
        schema: SchemaRef,
    ) -> Self {
        let writer = pool.clone();
        let inner = SqlTable::new_with_schema("clickhouse", pool, schema, table.clone());
        Self { reader: inner, table, writer, exprs: None }
    }

    /// Creates a new `TableProvider` with a pre-fetched schema and logical expressions
    pub fn new_with_schema_and_exprs(
        pool: ClickHouseConnectionPool,
        table: TableReference,
        schema: SchemaRef,
        exprs: Vec<Expr>,
    ) -> Self {
        let writer = pool.clone();
        let inner = SqlTable::new_with_schema("clickhouse", pool, schema, table.clone())
            .with_exprs(exprs.clone());
        Self { reader: inner, table, writer, exprs: Some(exprs) }
    }

    pub fn writer(&self) -> &ClickHouseConnectionPool { &self.writer }
}

#[async_trait]
impl TableProvider for ClickHouseTableProvider {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn schema(&self) -> SchemaRef { self.reader.schema() }

    fn table_type(&self) -> datafusion::datasource::TableType { self.reader.table_type() }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.reader.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.reader.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // ClickHouse doesn't support OVERWRITE natively in the same way as some systems,
        // so we'll treat it as an error for now until something better is implemented.
        //
        // TODO: Impelement `overwrite` but truncating
        if matches!(insert_op, InsertOp::Overwrite) {
            return Err(DataFusionError::NotImplemented(
                "OVERWRITE operation not supported for ClickHouse".to_string(),
            ));
        }

        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(ClickHouseDataSink::new(
                self.writer.clone(),
                self.table.clone(),
                self.reader.schema(),
            )),
            None,
        )))
    }
}
