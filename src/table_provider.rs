use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use futures_util::TryStreamExt as _;

use crate::connection::ClickHouseConnectionPool;
use crate::sink::ClickHouseDataSink;
use crate::sql::{JoinPushDown, SqlTable};

pub const CLICKHOUSE_TABLE_PROVIDER_NAME: &str = "ClickHouseTableProvider";

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
        let inner = SqlTable::new(CLICKHOUSE_TABLE_PROVIDER_NAME, pool, table.clone()).await?;
        Ok(Self { reader: inner, table, writer, exprs: None })
    }

    /// Creates a new `TableProvider` with a pre-fetched schema.
    pub fn new_with_schema(
        pool: ClickHouseConnectionPool,
        table: TableReference,
        schema: SchemaRef,
    ) -> Self {
        let writer = pool.clone();
        let inner =
            SqlTable::new_with_schema(CLICKHOUSE_TABLE_PROVIDER_NAME, pool, schema, table.clone());
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
        let inner =
            SqlTable::new_with_schema(CLICKHOUSE_TABLE_PROVIDER_NAME, pool, schema, table.clone())
                .with_exprs(exprs.clone());
        Self { reader: inner, table, writer, exprs: Some(exprs) }
    }

    pub fn writer(&self) -> &ClickHouseConnectionPool { &self.writer }

    /// Executes a SQL query and returns a stream of record batches.
    ///
    /// This method is exposed so that federation is not required to run sql using this table
    /// provider.
    ///
    /// # Errors
    /// - Returns an error if the connection pool fails to connect.
    /// - Returns an error if the query execution fails.
    pub fn execute_sql(&self, query: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
        let pool = self.writer.clone();
        let query = query.to_string();
        let exec_schema = Arc::clone(&schema);
        let stream = futures_util::stream::once(async move {
            pool.connect().await?.query_arrow(&query, &[], Some(exec_schema)).await
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    /// Provide the unique context for this table provider.
    ///
    /// This method is provided publicly to allow access to whether this provider accesses the same
    /// underlying `ClickHouse` server as another, without relying on federation.
    pub fn unique_context(&self) -> String {
        match self.reader.pool.join_push_down() {
            JoinPushDown::AllowedFor(context) => context,
            // Don't return None here - it will cause incorrect federation with other providers of
            // the same name that also have a compute_context of None. Instead return a
            // random string that will never match any other provider's context.
            JoinPushDown::Disallow => format!("{}", self.reader.unique_id()),
        }
    }
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
