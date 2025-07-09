use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result;
use datafusion::sql::TableReference;
use tracing::{debug, error};

use crate::connection::ClickHouseConnectionPool;
use crate::{ClickHouseTableFactory, utils};

// TODO: Docs
//
/// A custom [`CatalogProvider`] for `ClickHouse` schemas.
#[derive(Debug, Clone)]
pub struct ClickHouseCatalogProvider {
    // TODO: Should we cache schema names?
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl ClickHouseCatalogProvider {
    /// # Errors
    /// - Returns an error if the catalog cannot be retrieved from the database.
    pub async fn try_new(pool: ClickHouseConnectionPool) -> Result<Self> {
        Ok(Self { schemas: DashMap::from_iter(Self::get_catalog(&pool).await?) })
    }

    /// # Errors
    /// - Returns an error if the catalog cannot be retrieved from the database.
    pub async fn refresh_catalog(&self, pool: &ClickHouseConnectionPool) -> Result<()> {
        let current = self.schemas.iter().map(|entry| entry.key().clone()).collect::<Vec<_>>();
        let updated = Self::get_catalog(pool).await?;

        // Remove schemas that are no longer present in the catalog
        for schema in current.into_iter().filter(|s| !updated.iter().any(|(name, _)| name == s)) {
            drop(self.schemas.remove(&schema));
        }

        // Add schemas that are new in the catalog
        for (schema, table) in updated {
            drop(self.schemas.insert(schema, table));
        }

        Ok(())
    }

    async fn get_catalog(
        pool: &ClickHouseConnectionPool,
    ) -> Result<Vec<(String, Arc<dyn SchemaProvider>)>> {
        // Query all databases and tables in one go
        let mut tables = pool
            .pool()
            .get()
            .await
            .map_err(utils::map_external_err)?
            .fetch_all_tables(None)
            .await
            .inspect_err(|error| error!(?error, "Error fetching tables"))
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Add default database if empty
        let _ = tables.entry("default".to_string()).or_insert_with(Vec::new);

        tracing::debug!("Fetched schemas: {:?}", tables.keys());

        Ok(tables
            .into_iter()
            .map(|(schema, tables)| {
                (
                    schema.clone(),
                    Arc::new(ClickHouseSchemaProvider::new(schema, tables, pool.clone()))
                        as Arc<dyn SchemaProvider>,
                )
            })
            .collect::<Vec<_>>())
    }
}

impl CatalogProvider for ClickHouseCatalogProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn schema_names(&self) -> Vec<String> { self.schemas.iter().map(|s| s.key().clone()).collect() }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(s) = self.schemas.get(name) {
            debug!("ClickHouseCatalogProvider found schema: {name}");
            Some(Arc::clone(&s))
        } else {
            debug!("ClickHouseCatalogProvider did not find schema: {name}");
            None
        }
    }
}

/// A custom [`SchemaProvider`] for `ClickHouse` tables.
#[derive(Clone)]
pub struct ClickHouseSchemaProvider {
    name:   String,
    tables: Vec<String>,
    pool:   ClickHouseConnectionPool,
}

impl std::fmt::Debug for ClickHouseSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DatabaseSchemaProvider {{ name: {:?} }}", self.name)
    }
}

impl ClickHouseSchemaProvider {
    pub fn new(name: String, tables: Vec<String>, pool: ClickHouseConnectionPool) -> Self {
        Self { name, tables, pool }
    }
}

#[async_trait]
impl SchemaProvider for ClickHouseSchemaProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn table_names(&self) -> Vec<String> { self.tables.clone() }

    async fn table(&self, table: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(table) {
            debug!(table, "ClickHouseSchemaProvider creating TableProvider");
            let factory = ClickHouseTableFactory::new(self.pool.clone());
            factory
                .table_provider(TableReference::partial(self.name.clone(), table.to_string()))
                .await
                .map(Some)
                .inspect_err(|error| error!(?error, "Error creating table provider"))
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool { self.tables.iter().any(|t| t == name) }
}
