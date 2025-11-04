#![cfg_attr(feature = "mocks", expect(clippy::unused_async))]
#![cfg_attr(feature = "mocks", expect(dead_code))]

#[cfg(feature = "mocks")]
mod mock;

use clickhouse_arrow::ArrowConnectionPoolBuilder;
#[cfg(not(feature = "mocks"))]
use clickhouse_arrow::{
    ArrowConnectionManager, ArrowFormat, ClickHouseResponse, ConnectionManager,
    Error as ClickhouseNativeError, bb8,
};
#[cfg(not(feature = "mocks"))]
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::common::error::GenericError;
use datafusion::error::Result;
use datafusion::physical_plan::SendableRecordBatchStream;
#[cfg(not(feature = "mocks"))]
use datafusion::sql::TableReference;
use futures_util::TryStreamExt;
use tracing::{debug, error};

use crate::sql::JoinPushDown;
use crate::stream::{RecordBatchStreamWrapper, record_batch_stream_from_stream};

/// Type alias for a pooled connection to a `ClickHouse` database.
#[cfg(not(feature = "mocks"))]
pub type ArrowPoolConnection<'a> = bb8::PooledConnection<'a, ConnectionManager<ArrowFormat>>;
#[cfg(not(feature = "mocks"))]
pub type ArrowPool = bb8::Pool<ArrowConnectionManager>;
/// Type alias for a pooled connection as mocks.
#[cfg(feature = "mocks")]
pub type ArrowPoolConnection<'a> = &'a ();
#[cfg(feature = "mocks")]
pub type ArrowPool = ();

/// A wrapper around a [`clickhouse_arrow::ConnectionPool<ArrowFormat>`]
#[derive(Debug, Clone)]
pub struct ClickHouseConnectionPool {
    // "mocks" feature affects mainly this property and other properties related to connecting.
    pool:              ArrowPool,
    join_push_down:    JoinPushDown,
    /// Number of concurrent write operations allowed when inserting data.
    /// Defaults to 4 (matching clickhouse-arrow's current connection pool limit).
    write_concurrency: usize,
}

impl ClickHouseConnectionPool {
    /// Create a new `ClickHouse` connection pool for use in `DataFusion`. The identifier is used in
    /// the case of federation to determine if queries can be pushed down across two pools
    pub fn new(identifier: impl Into<String>, pool: ArrowPool) -> Self {
        debug!("Creating new ClickHouse connection pool");
        let join_push_down = JoinPushDown::AllowedFor(identifier.into());
        Self { pool, join_push_down, write_concurrency: 4 }
    }

    /// Set the write concurrency level for INSERT operations.
    ///
    /// This controls how many record batches can be written to `ClickHouse` concurrently.
    /// Higher values may improve throughput but increase memory and connection usage.
    ///
    /// Default: 4 (matching clickhouse-arrow's current connection pool limit)
    #[must_use]
    pub fn with_write_concurrency(mut self, concurrency: usize) -> Self {
        self.write_concurrency = concurrency;
        self
    }

    /// Get the configured write concurrency level
    pub fn write_concurrency(&self) -> usize { self.write_concurrency }

    /// Create a new `ClickHouse` connection pool from a builder.
    ///
    /// # Errors
    /// - Returns an error if the connection pool cannot be created.
    pub async fn from_pool_builder(builder: ArrowConnectionPoolBuilder) -> Result<Self> {
        let identifer = builder.connection_identifier();

        // Since this pool will be used for ddl, it's essential it connects to the "default" db
        #[cfg(not(feature = "mocks"))]
        let pool = builder
            .configure_client(|c| c.with_database("default"))
            .build()
            .await
            .inspect_err(|error| error!(?error, "Error building ClickHouse connection pool"))
            .map_err(crate::utils::map_clickhouse_err)?;

        #[cfg(feature = "mocks")]
        let pool = ();

        Ok(Self::new(identifer, pool))
    }

    /// Access the underlying connection pool
    pub fn pool(&self) -> &ArrowPool { &self.pool }

    pub fn join_push_down(&self) -> JoinPushDown { self.join_push_down.clone() }
}

impl ClickHouseConnectionPool {
    /// Get a managed [`ArrowPoolConnection`] wrapped in a [`ClickHouseConnection`]
    ///
    /// # Errors
    /// - Returns an error if the connection cannot be established.
    pub async fn connect(&self) -> Result<ClickHouseConnection<'_>> {
        #[cfg(not(feature = "mocks"))]
        let conn = self
            .pool
            .get()
            .await
            .inspect_err(|error| error!(?error, "Failed getting connection from pool"))
            .map_err(crate::utils::map_external_err)?;
        #[cfg(feature = "mocks")]
        let conn = &();
        Ok(ClickHouseConnection::new(conn))
    }

    /// Get a managed static [`ArrowPoolConnection`] wrapped in a [`ClickHouseConnection`]
    ///
    /// # Errors
    /// - Returns an error if the connection cannot be established.
    pub async fn connect_static(&self) -> Result<ClickHouseConnection<'static>> {
        #[cfg(not(feature = "mocks"))]
        let conn = self
            .pool
            .get_owned()
            .await
            .inspect_err(|error| error!(?error, "Failed getting connection from pool"))
            .map_err(crate::utils::map_external_err)?;
        #[cfg(feature = "mocks")]
        let conn = &();
        Ok(ClickHouseConnection::new_static(conn))
    }
}

/// A wrapper around [`ArrowPoolConnection`] that provides additional functionality relevant for
/// `DataFusion`.
///
/// The methods [`ClickHouseConnection::tables`], [`ClickHouseConnection::get_schema`], and
/// [`ClickHouseConnection::query_arrow`] will all be run against the `ClickHouse` instance.
#[derive(Debug)]
pub struct ClickHouseConnection<'a> {
    conn: ArrowPoolConnection<'a>,
}

impl<'a> ClickHouseConnection<'a> {
    pub fn new(conn: ArrowPoolConnection<'a>) -> Self { ClickHouseConnection { conn } }

    // TODO: Use to provide interop with datafusion-table-providers
    pub fn new_static(conn: ArrowPoolConnection<'static>) -> Self { ClickHouseConnection { conn } }

    /// Issues a query against `ClickHouse` and returns the result as an arrow
    /// [`SendableRecordBatchStream`] using the provided schema.
    ///
    /// The argument `coerce_schema` will be passed to `RecordBatchStream` only if
    /// `projected_schema` is also provided. Otherwise coercion won't be necessary as the streamed
    /// `RecordBatch`es will determine the schema.
    ///
    /// # Errors
    /// - Returns an error if the query fails.
    pub async fn query_arrow_with_schema(
        &self,
        sql: &str,
        params: &[()],
        schema: SchemaRef,
        coerce_schema: bool,
    ) -> Result<RecordBatchStreamWrapper, DataFusionError> {
        debug!(sql, "Running query");
        let batches = Box::pin(
            self.query_arrow_raw(sql, params)
                .await?
                // Map the stream's clickhouse-arrow error to DataFusionError
                .map_err(|e| DataFusionError::External(Box::new(e))),
        );
        Ok(RecordBatchStreamWrapper::new_from_stream(batches, schema).with_coercion(coerce_schema))
    }

    /// Issues a query against `ClickHouse` and returns the result as an arrow
    /// [`SendableRecordBatchStream`] using the provided schema.
    ///
    /// This method allows interop with `datafusion-table-providers` if desired. Otherwise, the
    /// method `Self::query_arrow_raw` can be used to prevent additional wrapping, or
    /// `Self::query_arrow_with_schema` if schema coercion is desired.
    ///
    /// # Errors
    /// - Returns an error if the query fails.
    pub async fn query_arrow(
        &self,
        sql: &str,
        params: &[()],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, GenericError> {
        if let Some(schema) = projected_schema {
            return Ok(Box::pin(self.query_arrow_with_schema(sql, params, schema, false).await?));
        }

        let batches = Box::pin(
            self.query_arrow_raw(sql, params)
                .await?
                // Map the stream's clickhouse-arrow error to DataFusionError
                .map_err(|e| DataFusionError::External(Box::new(e))),
        );

        Ok(Box::pin(
            record_batch_stream_from_stream(batches)
                .await
                .inspect_err(|error| error!(?error, "Failed converting batches to stream"))
                .map_err(Box::new)?,
        ))
    }
}

#[cfg(not(feature = "mocks"))]
impl ClickHouseConnection<'_> {
    /// Fetch the names of the tables in a schema (database).
    ///
    /// # Errors
    /// - Returns an error if the tables cannot be fetched.
    pub async fn tables(&self, schema: &str) -> Result<Vec<String>> {
        debug!(schema, "Fetching tables");
        self.conn
            .fetch_tables(Some(schema), None)
            .await
            .inspect_err(|error| error!(?error, "Fetching tables failed"))
            .map_err(crate::utils::map_clickhouse_err)
    }

    /// Fetch the names of the schemas (databases).
    ///
    /// # Errors
    /// - Returns an error if the schemas cannot be fetched.
    pub async fn schemas(&self) -> Result<Vec<String>> {
        debug!("Fetching databases");
        self.conn
            .fetch_schemas(None)
            .await
            .inspect_err(|error| error!(?error, "Fetching databases failed"))
            .map_err(crate::utils::map_clickhouse_err)
    }

    /// Fetch the schema for a table
    ///
    /// # Errors
    /// - Returns an error if the schema cannot be fetched.
    pub async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        debug!(%table_reference, "Fetching schema for table");
        let db = table_reference.schema();
        let table = table_reference.table();
        let mut schemas =
            self.conn.fetch_schema(db, &[table][..], None).await.map_err(|error| {
                if let ClickhouseNativeError::UndefinedTables { .. } = error {
                    error!(?error, ?db, ?table, "Tables undefined");
                } else {
                    error!(?error, ?db, ?table, "Unknown error occurred while fetching schema");
                }
                crate::utils::map_clickhouse_err(error)
            })?;

        schemas
            .remove(table)
            .ok_or(DataFusionError::External("Schema not found for table".into()))
    }

    /// Issues a query against `ClickHouse` and returns the raw `ClickHouseResponse<RecordBatch>`
    ///
    /// # Errors
    /// - Returns an error if the query fails
    pub async fn query_arrow_raw(
        &self,
        sql: &str,
        _params: &[()],
    ) -> Result<ClickHouseResponse<RecordBatch>> {
        self.conn
            .query(sql, None)
            .await
            .inspect(|_| tracing::trace!("Query executed successfully"))
            .inspect_err(|error| error!(?error, "Failed running query"))
            // Convert the clickhouse-arrow error to a DataFusionError
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// Executes a statement against `ClickHouse` and returns the number of affected rows.
    ///
    /// # Errors
    /// - Returns an error if the query fails.
    pub async fn execute(&self, sql: &str, _params: &[()]) -> Result<u64, GenericError> {
        debug!(sql, "Executing query");
        self.conn
            .execute(sql, None)
            .await
            .inspect_err(|error| error!(?error, "Failed executing query"))
            .map_err(Box::new)?;
        Ok(0)
    }
}

// TODO: Provide compat with datafusion-table-providers DbConnection, AsyncDbConnection

#[cfg(test)]
mod tests {
    use datafusion::sql::TableReference;

    use super::*;

    #[test]
    fn test_table_reference_schema_extraction() {
        // Test the logic used in get_schema method
        let table_ref = TableReference::full("catalog", "schema", "table");
        assert_eq!(table_ref.schema(), Some("schema"));
        assert_eq!(table_ref.table(), "table");

        let partial_ref = TableReference::partial("schema", "table");
        assert_eq!(partial_ref.schema(), Some("schema"));
        assert_eq!(partial_ref.table(), "table");

        let bare_ref = TableReference::bare("table");
        assert_eq!(bare_ref.schema(), None);
        assert_eq!(bare_ref.table(), "table");
    }

    #[test]
    fn test_error_handling_patterns() {
        use clickhouse_arrow::Error as ClickhouseNativeError;

        use crate::utils::map_clickhouse_err;

        // Test the error patterns used in connection methods
        let undefined_tables_error = ClickhouseNativeError::UndefinedTables {
            db:     "test_db".to_string(),
            tables: vec!["test_table".to_string()],
        };

        let mapped_error = map_clickhouse_err(undefined_tables_error);
        match mapped_error {
            DataFusionError::External(boxed_error) => {
                let error_str = boxed_error.to_string();
                assert!(error_str.contains("Tables undefined"));
                assert!(error_str.contains("test_db"));
                assert!(error_str.contains("test_table"));
            }
            _ => panic!("Expected External error"),
        }
    }

    #[test]
    fn test_join_push_down_creation() {
        use crate::sql::JoinPushDown;

        // Test the join push down logic used in connection pool creation
        let identifier = "test_pool";
        let join_push_down = JoinPushDown::AllowedFor(identifier.to_string());

        match join_push_down {
            JoinPushDown::AllowedFor(id) => assert_eq!(id, "test_pool"),
            JoinPushDown::Disallow => panic!("Expected AllowedFor variant"),
        }
    }
}
