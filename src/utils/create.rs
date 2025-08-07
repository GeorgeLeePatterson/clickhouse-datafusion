#![cfg_attr(feature = "mocks", expect(unused_imports))]
use clickhouse_arrow::CreateOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::TableReference;
use tracing::{debug, error};

use crate::connection::ClickHouseConnectionPool;
#[cfg(not(feature = "mocks"))]
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

    #[cfg(not(feature = "mocks"))]
    {
        let conn = pool.pool().get().await.map_err(utils::map_external_err)?;

        // Ensure db exists
        conn.create_database(Some(database), None).await.map_err(|error| {
            error!(?error, database, "Database creation failed");
            DataFusionError::External(
                format!("Failed to create database for {database}: {error}").into(),
            )
        })?;
    }

    #[cfg(feature = "mocks")]
    let _ = pool.connect().await?;

    Ok(())
}

/// Create a table in the remote `ClickHouse` instance.
///
/// # Errors
/// - Returns an error if the `ClickHouse` endpoint is unreachable
/// - Returns an error if the `ClickHouse` schema fails to be created
#[cfg(not(feature = "mocks"))]
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

#[cfg(feature = "mocks")]
#[expect(clippy::missing_errors_doc)]
#[expect(clippy::unused_async)]
pub async fn create_schema(
    _table_ref: &TableReference,
    _schema: &SchemaRef,
    _table_create_options: &CreateOptions,
    _pool: &ClickHouseConnectionPool,
    _if_not_exists: bool,
) -> Result<()> {
    Ok(())
}

// The following tests are present to bridge a gap in coverage around mocks
#[cfg(all(test, feature = "test-utils", feature = "mocks"))]
mod tests {
    use std::sync::Arc;

    use clickhouse_arrow::CreateOptions;
    use datafusion::arrow::datatypes::Schema;

    use super::*;
    use crate::connection::ClickHouseConnectionPool;

    #[tokio::test]
    async fn test_create_utils_mocked() {
        let pool = Arc::new(ClickHouseConnectionPool::new("pool".to_string(), ()));

        let result = create_database("test", &pool).await;
        assert!(result.is_ok());

        let result = create_schema(
            &TableReference::from("test"),
            &Arc::new(Schema::empty()),
            &CreateOptions::default(),
            &pool,
            false,
        )
        .await;
        assert!(result.is_ok());
    }
}
