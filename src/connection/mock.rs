use clickhouse_arrow::ClickHouseResponse;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::error::GenericError;
use datafusion::error::Result;
use datafusion::sql::TableReference;
use tracing::debug;

use super::ClickHouseConnection;

impl Default for ClickHouseConnection<'_> {
    fn default() -> Self { ClickHouseConnection { conn: &() } }
}

impl ClickHouseConnection<'_> {
    /// Fetch the names of the tables in a schema (database).
    ///
    /// # Errors
    /// - Returns an error if the tables cannot be fetched.
    #[expect(clippy::unused_async)]
    pub async fn tables(&self, schema: &str) -> Result<Vec<String>> {
        debug!(schema, "Fetching tables");
        Ok(vec![])
    }

    /// Fetch the names of the schemas (databases).
    ///
    /// # Errors
    /// - Returns an error if the schemas cannot be fetched.
    #[expect(clippy::unused_async)]
    pub async fn schemas(&self) -> Result<Vec<String>> {
        debug!("Fetching databases");
        Ok(vec![])
    }

    /// Fetch the schema for a table
    ///
    /// # Errors
    /// - Returns an error if the schema cannot be fetched.
    #[expect(clippy::unused_async)]
    pub async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        debug!(%table_reference, "Fetching schema for table");
        // TODO: Remove
        // let db = table_reference.schema();
        // let table = table_reference.table();
        Ok(std::sync::Arc::new(datafusion::arrow::datatypes::Schema::empty()))
    }

    /// Issues a query against `ClickHouse` and returns the raw `ClickHouseResponse<RecordBatch>`
    ///
    /// # Errors
    /// - Returns an error if the query fails
    #[expect(clippy::unused_async)]
    pub async fn query_arrow_raw(
        &self,
        _sql: &str,
        _params: &[()],
    ) -> Result<ClickHouseResponse<RecordBatch>> {
        Ok(ClickHouseResponse::from_stream(futures_util::stream::once(async move {
            Ok(RecordBatch::new_empty(std::sync::Arc::new(
                datafusion::arrow::datatypes::Schema::empty(),
            )))
        })))
    }

    /// Executes a statement against `ClickHouse` and returns the number of affected rows.
    ///
    /// # Errors
    /// - Returns an error if the query fails.
    #[expect(clippy::unused_async)]
    pub async fn execute(&self, sql: &str, _params: &[()]) -> Result<u64, GenericError> {
        debug!(sql, "Executing query");
        Ok(0)
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use super::*;

    // The following tests are meant to close the coverage gap for mocks
    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_mock_connection() {
        use std::sync::Arc;

        use clickhouse_arrow::ArrowConnectionPoolBuilder;
        use datafusion::arrow::datatypes::Schema;

        use crate::ClickHouseConnectionPool;

        let builder = ArrowConnectionPoolBuilder::new("endpoint");
        let c = ClickHouseConnectionPool::from_pool_builder(builder).await.unwrap();
        let conn = c.connect().await.unwrap();
        let _conn_static = c.connect_static().await.unwrap();
        let _ = ClickHouseConnection::default();
        drop(conn.tables("").await.unwrap());
        drop(conn.schemas().await.unwrap());
        drop(conn.get_schema(&TableReference::bare("")).await.unwrap());
        let _ = conn.execute("", &[]).await.unwrap();
        drop(conn.query_arrow("", &[], Some(Arc::new(Schema::empty()))).await.unwrap());
    }
}
