use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_federation::sql::{
    RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use futures_util::TryStreamExt;

use super::{JoinPushDown, SqlTable};

impl SqlTable {
    /// Create a federated table source for this table provider.
    fn create_federated_table_source(self: Arc<Self>) -> Arc<dyn FederatedTableSource> {
        let table_ref = RemoteTableRef::from(self.table_reference.clone());
        let schema = self.schema();
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Arc::new(SQLTableSource::new_with_schema(fed_provider, table_ref, schema))
    }

    /// Create a federated table provider wrapping this table provider.
    ///
    /// # Errors
    /// - Error if `create_federated_table_source` fails.
    pub fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> Result<FederatedTableProviderAdaptor> {
        let table_source = Self::create_federated_table_source(Arc::clone(&self));
        Ok(FederatedTableProviderAdaptor::new_with_provider(table_source, self))
    }
}

#[async_trait]
impl SQLExecutor for SqlTable {
    fn name(&self) -> &str { &self.name }

    fn compute_context(&self) -> Option<String> {
        match self.pool.join_push_down() {
            JoinPushDown::AllowedFor(context) => Some(context),
            // Don't return None here - it will cause incorrect federation with other providers of
            // the same name that also have a compute_context of None. Instead return a
            // random string that will never match any other provider's context.
            JoinPushDown::Disallow => Some(format!("{}", self.unique_id())),
        }
    }

    fn dialect(&self) -> Arc<dyn Dialect> { self.arc_dialect() }

    fn execute(&self, query: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
        let sql = query.to_string();
        let exec_schema = Arc::clone(&schema);
        let pool = self.pool.clone();
        let stream = futures_util::stream::once(async move {
            pool.connect().await?.query_arrow(&sql, &[], Some(exec_schema)).await
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        let Some(schema) = self.table_reference.schema() else {
            return exec_err!("Cannot fetch tables without a schema");
        };
        self.pool.connect().await?.tables(schema).await
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        let table_ref = self
            .table_reference
            .schema()
            .as_ref()
            .map(|s| TableReference::partial(*s, table_name))
            .unwrap_or(TableReference::from(table_name));
        self.pool.connect().await?.get_schema(&table_ref).await
    }
}
