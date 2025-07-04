use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use clickhouse_arrow::{ArrowConnectionPoolBuilder, Destination};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::sql::TableReference;
use parking_lot::Mutex;
use tracing::{debug, error, warn};

use crate::connection::ClickHouseConnectionPool;
use crate::utils::DEFAULT_DATABASE_PARAM;
use crate::{ClickHouseTableProvider, utils};

// TODO: Docs - especially explain the different ways to use this, the fact that it exists since it
// wraps a `ClickHouseConnectionPool`, and how to use it with a `ClickHouseConnectionPoolBuilder`.
//
/// A table factory for creating `ClickHouse` [`TableProvider`]s.
///
/// Returns a federated table provider if the `federation` feature is enabled, otherwise a
/// [`TableProvider`]
#[derive(Debug)]
pub struct ClickHouseTableFactory {
    pool: ClickHouseConnectionPool,
}

impl ClickHouseTableFactory {
    pub fn new(pool: ClickHouseConnectionPool) -> Self { Self { pool } }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>> {
        let pool = self.pool.clone();
        debug!(%table_reference, "Creating ClickHouse table provider");

        let provider = Arc::new(
            ClickHouseTableProvider::try_new(pool, table_reference)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        #[cfg(feature = "federation")]
        let provider = Arc::new(
            provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(provider)
    }

    // TODO: add cfg_attr(not(federated feature)) for non-async usage and useless result
    pub async fn table_provider_from_schema(
        &self,
        table_reference: TableReference,
        schema: SchemaRef,
    ) -> Result<Arc<dyn TableProvider + 'static>> {
        debug!(%table_reference, "Creating ClickHouse table provider");
        let provider = Arc::new(ClickHouseTableProvider::new_with_schema(
            self.pool.clone(),
            table_reference,
            schema,
        ));

        #[cfg(feature = "federation")]
        let provider = Arc::new(
            provider
                .create_federated_table_provider()
                .inspect_err(|error| error!(?error, "Failed creating federated table provider"))
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(provider)
    }
}

/// A `DataFusion` [`TableProviderFactory`] for creating `ClickHouse` tables.
#[derive(Debug)]
pub struct ClickHouseTableProviderFactory {
    pools: Arc<Mutex<HashMap<Destination, ClickHouseConnectionPool>>>,
}

impl ClickHouseTableProviderFactory {
    pub fn new() -> Self { Self { pools: Arc::new(Mutex::new(HashMap::default())) } }

    // TODO: Docs
    pub async fn new_with_builder(
        endpoint: impl Into<Destination>,
        builder: ArrowConnectionPoolBuilder,
    ) -> Result<Self> {
        let this = Self::new();
        drop(this.attach_pool_builder(endpoint, builder).await?);
        Ok(this)
    }

    // TODO: Docs
    /// Attach an existing [`ClickHouseConnectionPool`] to the factory by providing
    /// [`ClickHouseOptions`] which will be built into a [`ClickHouseConnectionPool`]
    pub async fn attach_pool_builder(
        &self,
        endpoint: impl Into<Destination>,
        builder: ArrowConnectionPoolBuilder,
    ) -> Result<ClickHouseConnectionPool> {
        let endpoint = endpoint.into();
        debug!(?endpoint, "Attaching ClickHouse connection pool");

        // Since this pool will be used for ddl, it's essential it connects to the "default" db
        let builder = builder.configure_client(|c| c.with_database("default"));

        // Create connection pool
        let pool = ClickHouseConnectionPool::from_pool_builder(builder).await?;
        debug!(?endpoint, "Connection pool created successfully");

        {
            self.pools.lock().insert(endpoint, pool.clone());
        }

        Ok(pool)
    }

    // TODO: Docs - explain how serialized params are used by `TableProviderFactory` below and how
    // this method enables it
    //
    /// Get or create a connection pool from parameters, returning an existing connection pool if
    /// the endpoint is already connected
    async fn get_or_create_pool_from_params(
        &self,
        endpoint: &str,
        params: &mut HashMap<String, String>,
    ) -> Result<ClickHouseConnectionPool> {
        if endpoint.is_empty() {
            error!("Endpoint is required for ClickHouse, received empty value");
            return exec_err!("Endpoint is required for ClickHouse");
        };

        let destination = Destination::from(endpoint);
        if let Some(pool) = self.pools.lock().get(&destination) {
            debug!("Pool exists for endpoint: {endpoint}");
            return Ok(pool.clone());
        }

        // Parse options (e.g., host, port, database)
        // NOTE: Settings are ignored since this path is for creating tables and settings will be
        // delegated to table creation
        let clickhouse_options = utils::params_to_pool_builder(endpoint, params, true)?;

        // Create connection pool
        self.attach_pool_builder(destination, clickhouse_options).await
    }
}

impl Default for ClickHouseTableProviderFactory {
    fn default() -> Self { Self::new() }
}

#[async_trait]
impl TableProviderFactory for ClickHouseTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        if !cmd.constraints.is_empty() {
            warn!("Constraints not fully supported in ClickHouse; ignoring: {:?}", cmd.constraints);
        }

        let name = cmd.name.clone();
        let mut params = cmd.options.clone();
        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().into());

        // Pull out endpoint
        let endpoint =
            params.get("endpoint").map(ToString::to_string).unwrap_or(cmd.location.clone());

        // Pull out database
        let database = name
            .schema()
            .or(params.get(DEFAULT_DATABASE_PARAM).map(|x| x.as_str()))
            .unwrap_or("default")
            .to_string();

        // Get or create a clickhouse connection pool
        let pool = self
            .get_or_create_pool_from_params(&endpoint, &mut params)
            .await
            .inspect_err(|error| error!(?error, "Failed to create connection pool"))?;

        // Ensure table reference is properly formatted
        let name = match name {
            t @ TableReference::Partial { .. } => t,
            TableReference::Bare { table } => TableReference::partial(database.as_str(), table),
            TableReference::Full { schema, table, .. } => TableReference::partial(schema, table),
        };

        debug!(?name, "Table provider factory creating schema");

        // DDL
        {
            // Get table options
            let column_defaults = &cmd.column_defaults;
            let create_options =
                utils::params::params_to_create_options(&mut params, column_defaults).inspect_err(
                    |error| error!(?error, ?params, "Could not generate table options from params"),
                )?;

            // Create table and optionally database
            utils::create_schema(&name, &schema, &create_options, &pool, cmd.if_not_exists).await?;
        }

        // Create table provider
        let factory = ClickHouseTableFactory::new(pool);
        factory.table_provider_from_schema(name, schema).await
    }
}
