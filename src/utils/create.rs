use clickhouse_arrow::CreateOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::TableReference;
use tracing::{debug, error};

use crate::connection::ClickHouseConnectionPool;
use crate::utils;

/// Create a database in the remote `ClickHouse` instance.
///
/// # Errors
/// - Returns an error if the `ClickHouse` endpoint is unreachable
/// - Returns an error if the `ClickHouse` database fails to be created
pub async fn create_database(database: &str, pool: &ClickHouseConnectionPool) -> Result<()> {
    // Exit early if database is default
    if database == "default" {
        return Ok(());
    }

    let conn = pool.pool().get().await.map_err(utils::map_external_err)?;

    // Ensure db exists
    conn.create_database(Some(database), None).await.map_err(|error| {
        error!(?error, database, "Database creation failed");
        DataFusionError::External(
            format!("Failed to create database for {database}: {error}").into(),
        )
    })?;

    Ok(())
}

/// Create a table in the remote `ClickHouse` instance.
///
/// # Errors
/// - Returns an error if the `ClickHouse` endpoint is unreachable
/// - Returns an error if the `ClickHouse` schema fails to be created
pub async fn create_schema(
    table_ref: &TableReference,
    schema: &SchemaRef,
    table_create_options: &CreateOptions,
    pool: &ClickHouseConnectionPool,
    if_not_exists: bool,
) -> Result<()> {
    let conn = pool.pool().get().await.map_err(utils::map_external_err)?;

    // Ensure db exists
    if if_not_exists {
        debug!(?table_ref, "Creating database schema");
        conn.create_database(table_ref.schema(), None).await.map_err(|error| {
            error!(?error, name = %table_ref, "Database creation failed");
            DataFusionError::External(
                format!("Failed to create database for {table_ref}: {error}").into(),
            )
        })?;
    }

    // TODO: !IMPORTANT! Handle table_partition_cols from cmd
    let table = table_ref.table();
    let schema_name = table_ref.schema();
    debug!(table, ?schema_name, "Creating database table");
    conn.create_table(schema_name, table, schema, table_create_options, None).await.map_err(
        |error| {
            error!(?error, %table_ref, "Table creation failed");
            DataFusionError::External(
                format!("Failed to create table for {table_ref}: {error}").into(),
            )
        },
    )?;

    Ok(())
}
