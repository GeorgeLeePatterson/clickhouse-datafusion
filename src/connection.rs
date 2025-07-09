use clickhouse_arrow::{
    ArrowConnectionManager, ArrowConnectionPoolBuilder, ArrowFormat, ConnectionManager,
    Error as ClickhouseNativeError, bb8,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::common::error::GenericError;
use datafusion::error::Result;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use futures_util::TryStreamExt;
use tracing::{debug, error};

use crate::sql::JoinPushDown;
use crate::stream::{RecordBatchStream, record_batch_stream_from_stream};
use crate::utils;

/// Type alias for a pooled connection to a `ClickHouse` database using arrow over native protocol.
pub type ArrowPoolConnection<'a> = bb8::PooledConnection<'a, ConnectionManager<ArrowFormat>>;

/// A wrapper around a [`clickhouse_arrow::ConnectionPool<ArrowFormat>`]
#[derive(Debug, Clone)]
pub struct ClickHouseConnectionPool {
    pool:           bb8::Pool<ArrowConnectionManager>,
    join_push_down: JoinPushDown,
}

impl ClickHouseConnectionPool {
    /// Create a new `ClickHouse` connection pool for use in `DataFusion`. The identifier is used in
    /// the case of federation to determine if queries can be pushed down across two pools
    pub fn new(identifier: impl Into<String>, pool: bb8::Pool<ArrowConnectionManager>) -> Self {
        debug!("Creating new ClickHouse connection pool");
        let join_push_down = JoinPushDown::AllowedFor(identifier.into());
        Self { pool, join_push_down }
    }

    /// Create a new `ClickHouse` connection pool from a builder.
    ///
    /// # Errors
    /// - Returns an error if the connection pool cannot be created.
    pub async fn from_pool_builder(builder: ArrowConnectionPoolBuilder) -> Result<Self> {
        let identifer = builder.connection_identifier();

        // Since this pool will be used for ddl, it's essential it connects to the "default" db
        let builder = builder.configure_client(|c| c.with_database("default"));

        let pool = builder
            .build()
            .await
            .inspect_err(|error| error!(?error, "Error building ClickHouse connection pool"))
            .map_err(utils::map_clickhouse_err)?;
        Ok(Self::new(identifer, pool))
    }

    /// Access the underlying connection pool
    pub fn pool(&self) -> &bb8::Pool<ArrowConnectionManager> { &self.pool }

    pub fn join_push_down(&self) -> JoinPushDown { self.join_push_down.clone() }

    /// Get a managed [`ArrowPoolConnection`] wrapped in a [`ClickHouseConnection`]
    ///
    /// # Errors
    /// - Returns an error if the connection cannot be established.
    pub async fn connect(&self) -> Result<ClickHouseConnection<'_>> {
        let conn = self
            .pool
            .get()
            .await
            .inspect_err(|error| error!(?error, "Failed getting connection from pool"))
            .map_err(utils::map_external_err)?;
        Ok(ClickHouseConnection::new(conn))
    }
}

// TODO: Docs - also included links to clickhouse-arrow
/// A wrapper around [`ArrowPoolConnection`] that provides additional functionality relevant for
/// `DataFusion`. The methods [`ClickHouseConnection::tables`],
/// [`ClickHouseConnection::get_schema`], and [`ClickHouseConnection::query_arrow`] will all be run
/// against `ClickHouse` at invocation.
#[derive(Debug)]
pub struct ClickHouseConnection<'a> {
    conn: ArrowPoolConnection<'a>,
}

impl<'a> ClickHouseConnection<'a> {
    pub fn new(conn: bb8::PooledConnection<'a, ConnectionManager<ArrowFormat>>) -> Self {
        ClickHouseConnection { conn }
    }

    // TODO: Use to provide interop with datafusion-table-providers
    pub fn new_static(
        conn: bb8::PooledConnection<'static, ConnectionManager<ArrowFormat>>,
    ) -> Self {
        ClickHouseConnection { conn }
    }

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
            .map_err(utils::map_clickhouse_err)
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
            .map_err(utils::map_clickhouse_err)
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
            self.conn.fetch_schema(db, &[table][..], None).await.map_err(|e| match e {
                e @ ClickhouseNativeError::UndefinedTables { .. } => {
                    error!(error = ?e, ?db, ?table, "Tables undefined");
                    utils::map_clickhouse_err(e)
                }
                e => {
                    error!(error = ?e, ?db, ?table, "Unknown error occurred while fetching schema");
                    utils::map_clickhouse_err(e)
                }
            })?;

        schemas
            .remove(table)
            .ok_or(DataFusionError::External("Schema not found for table".into()))
    }

    /// Issues a query against `ClickHouse` and returns the result as an arrow
    /// [`SendableRecordBatchStream`] using the provided schema.
    ///
    /// # Errors
    /// - Returns an error if the query fails.
    pub async fn query_arrow(
        &self,
        sql: &str,
        _params: &[()],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, GenericError> {
        debug!(sql, "Running query");
        let batches = self
            .conn
            .query(sql, None)
            .await
            .inspect_err(|error| error!(?error, "Failed running query"))
            .map_err(Box::new)?
            .map_err(|e| DataFusionError::External(Box::new(e)));

        if let Some(schema) = projected_schema {
            return Ok(Box::pin(RecordBatchStream::new(schema, Box::pin(batches))));
        }

        Ok(record_batch_stream_from_stream(batches)
            .await
            .inspect_err(|error| error!(?error, "Failed converting batches to stream"))
            .map_err(Box::new)?)
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
