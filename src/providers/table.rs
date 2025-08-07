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
use datafusion::sql::TableReference;

use crate::connection::ClickHouseConnectionPool;
use crate::sink::ClickHouseDataSink;
use crate::sql::{JoinPushDown, SqlTable, execute_sql_query};

pub const CLICKHOUSE_TABLE_PROVIDER_NAME: &str = "ClickHouseTableProvider";

// TODO: Docs
//
/// A custom [`TableProvider`] for remove `ClickHouse` tables.
#[derive(Debug)]
pub struct ClickHouseTableProvider {
    pub(crate) table:  TableReference,
    pub(crate) reader: SqlTable,
}

impl ClickHouseTableProvider {
    /// Creates a new `TableProvider`, fetching the schema from `ClickHouse` if not provided.
    ///
    /// # Errors
    /// - Returns an error if the `SQLTable` creation fails.
    pub async fn try_new(
        pool: Arc<ClickHouseConnectionPool>,
        table: TableReference,
    ) -> Result<Self> {
        let inner = SqlTable::try_new(CLICKHOUSE_TABLE_PROVIDER_NAME, pool, table.clone()).await?;
        Ok(Self { reader: inner, table })
    }

    /// Creates a new `TableProvider` with a pre-fetched schema. The underlying `SqlTable` will not
    /// verify if the table exists nor if the schema is accurate. To catch any errors in these
    /// cases, use [`try_new`](Self::try_new).
    pub fn new_with_schema_unchecked(
        pool: Arc<ClickHouseConnectionPool>,
        table: TableReference,
        schema: SchemaRef,
    ) -> Self {
        let inner = SqlTable::new_with_schema_unchecked(
            CLICKHOUSE_TABLE_PROVIDER_NAME,
            pool,
            schema,
            table.clone(),
        );
        Self { reader: inner, table }
    }

    #[must_use]
    pub fn with_exprs(mut self, exprs: Vec<Expr>) -> Self {
        self.reader = self.reader.with_exprs(exprs);
        self
    }

    /// Whether to coerce the `RecordBatch`es returned by `SQLExecutor::execute` using the generated
    /// `LogicalPlan`'s schema.
    #[must_use]
    pub fn with_coercion(mut self, coerce: bool) -> Self {
        self.reader = self.reader.with_coercion(coerce);
        self
    }

    // TODO: Remove - Docs
    pub fn reader(&self) -> &SqlTable { &self.reader }

    // TODO: Remove - Docs
    pub fn pool(&self) -> &Arc<ClickHouseConnectionPool> { self.reader.pool() }

    // TODO: Remove - Docs
    pub fn coerce_schema(&self) -> bool { self.reader.coerce_schema }

    /// Executes a SQL query and returns a stream of record batches.
    ///
    /// This method is exposed so that federation is not required to run sql using this table
    /// provider.
    ///
    /// # Errors
    /// - Returns an error if the connection pool fails to connect.
    /// - Returns an error if the query execution fails.
    pub fn execute_sql(&self, query: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
        let coerce_schema = self.reader.coerce_schema;
        let pool = Arc::clone(self.pool());
        let query = query.to_string();
        Ok(execute_sql_query(query, pool, schema, coerce_schema))
    }

    /// Provide the unique context for this table provider.
    ///
    /// This method is provided publicly to allow access to whether this provider accesses the same
    /// underlying `ClickHouse` server as another, without relying on federation.
    pub fn unique_context(&self) -> String {
        match self.pool().join_push_down() {
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
                Arc::clone(self.reader.pool()),
                self.table.clone(),
                self.reader.schema(),
            )),
            None,
        )))
    }
}

#[cfg(feature = "federation")]
mod federation {
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::catalog::TableProvider;
    use datafusion::error::{DataFusionError, Result};
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::sql::TableReference;
    use datafusion_federation::sql::{
        RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
    };
    use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
    use tracing::debug;

    use super::ClickHouseTableProvider;
    use crate::dialect::ClickHouseDialect;

    impl ClickHouseTableProvider {
        /// Create a federated table source for this table provider.
        pub fn create_federated_table_source(self: Arc<Self>) -> Arc<dyn FederatedTableSource> {
            let table_name: RemoteTableRef = self.table.clone().into();
            let schema = self.schema();
            debug!(table = %table_name.table_ref(), "Creating federated table source");
            let fed_provider = Arc::new(SQLFederationProvider::new(self));
            Arc::new(SQLTableSource::new_with_schema(fed_provider, table_name, schema))
        }

        /// Create a federated table provider wrapping this table provider.
        pub fn create_federated_table_provider(self: Arc<Self>) -> FederatedTableProviderAdaptor {
            let table_source = Self::create_federated_table_source(Arc::clone(&self));
            FederatedTableProviderAdaptor::new_with_provider(table_source, self)
        }
    }

    #[async_trait]
    impl SQLExecutor for ClickHouseTableProvider {
        fn name(&self) -> &'static str { "clickhouse" }

        fn compute_context(&self) -> Option<String> { self.reader.compute_context() }

        fn dialect(&self) -> Arc<dyn datafusion::sql::unparser::dialect::Dialect> {
            Arc::new(ClickHouseDialect)
        }

        fn ast_analyzer(&self) -> Option<datafusion_federation::sql::AstAnalyzer> {
            // No custom AST rewriting needed for now; arrayJoin and other functions handled by
            // dialect
            None
        }

        fn execute(&self, query: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
            self.reader.execute(query, schema)
        }

        async fn table_names(&self) -> Result<Vec<String>> {
            self.pool()
                .connect()
                .await?
                .tables(self.table.schema().expect("Schema must be present"))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))
        }

        async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
            let table_ref = self
                .table
                .schema()
                .as_ref()
                .map(|s| TableReference::partial(*s, table_name))
                .unwrap_or(TableReference::from(table_name));
            self.pool()
                .connect()
                .await?
                .get_schema(&table_ref)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))
        }
    }
}
