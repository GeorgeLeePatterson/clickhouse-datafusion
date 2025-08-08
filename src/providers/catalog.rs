use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result;
use datafusion::sql::TableReference;
use tracing::{debug, error};

use super::table_factory::ClickHouseTableFactory;
use crate::connection::ClickHouseConnectionPool;
#[cfg(not(feature = "mocks"))]
use crate::utils;

// TODO: Docs
// TODO: Should schema names be cached?
//
/// A custom [`CatalogProvider`] for `ClickHouse` schemas.
#[derive(Debug, Clone)]
pub struct ClickHouseCatalogProvider {
    schemas:        DashMap<String, Arc<dyn SchemaProvider>>,
    coerce_schemas: bool,
}

impl ClickHouseCatalogProvider {
    /// Create a new [`ClickHouseCatalogProvider`] with the given connection pool.
    ///
    /// # Errors
    /// - Returns an error if the catalog cannot be retrieved from the database.
    pub async fn try_new(pool: Arc<ClickHouseConnectionPool>) -> Result<Self> {
        Ok(Self {
            schemas:        DashMap::from_iter(Self::get_catalog(&pool, false).await?),
            coerce_schemas: false,
        })
    }

    /// Create a new [`ClickHouseCatalogProvider`] with the given connection pool, configuring the
    /// dynamic coercion of `RecordBatch` schemas during `TableProvider` execution.
    ///
    /// There is a non-zero cost to this setting, so it should be avoided if possible.
    ///
    /// # Errors
    /// - Returns an error if the catalog cannot be retrieved from the database.
    pub async fn try_new_with_coercion(pool: Arc<ClickHouseConnectionPool>) -> Result<Self> {
        Ok(Self {
            schemas:        DashMap::from_iter(Self::get_catalog(&pool, true).await?),
            coerce_schemas: true,
        })
    }

    /// # Errors
    /// - Returns an error if the catalog cannot be retrieved from the database.
    pub async fn refresh_catalog(&self, pool: &Arc<ClickHouseConnectionPool>) -> Result<()> {
        let current = self.schemas.iter().map(|entry| entry.key().clone()).collect::<Vec<_>>();
        let updated = Self::get_catalog(pool, self.coerce_schemas).await?;

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

    #[cfg(not(feature = "mocks"))]
    async fn get_catalog(
        pool: &Arc<ClickHouseConnectionPool>,
        coerce_schemas: bool,
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
                    Arc::new(
                        ClickHouseSchemaProvider::new(schema, tables, Arc::clone(pool))
                            .with_coercion(coerce_schemas),
                    ) as Arc<dyn SchemaProvider>,
                )
            })
            .collect::<Vec<_>>())
    }

    #[cfg(feature = "mocks")]
    #[expect(clippy::unused_async)]
    async fn get_catalog(
        pool: &Arc<ClickHouseConnectionPool>,
        coerce_schemas: bool,
    ) -> Result<Vec<(String, Arc<dyn SchemaProvider>)>> {
        // Query all databases and tables in one go
        let mut tables = std::collections::HashMap::new();

        // Add default database if empty
        let _ = tables.entry("default".to_string()).or_insert_with(Vec::new);
        tracing::debug!("Fetched schemas: {:?}", tables.keys());

        Ok(tables
            .into_iter()
            .map(|(schema, tables)| {
                (
                    schema.clone(),
                    Arc::new(
                        ClickHouseSchemaProvider::new(schema, tables, Arc::clone(pool))
                            .with_coercion(coerce_schemas),
                    ) as Arc<dyn SchemaProvider>,
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
    name:    String,
    tables:  Vec<String>,
    factory: ClickHouseTableFactory,
}

impl std::fmt::Debug for ClickHouseSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DatabaseSchemaProvider {{ name: {:?} }}", self.name)
    }
}

impl ClickHouseSchemaProvider {
    pub fn new(name: String, tables: Vec<String>, pool: Arc<ClickHouseConnectionPool>) -> Self {
        let factory = ClickHouseTableFactory::new(pool);
        Self { name, tables, factory }
    }

    /// Set whether to coerce schema types.
    #[must_use]
    pub fn with_coercion(mut self, coerce: bool) -> Self {
        self.factory = self.factory.with_coercion(coerce);
        self
    }
}

#[async_trait]
impl SchemaProvider for ClickHouseSchemaProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn table_names(&self) -> Vec<String> { self.tables.clone() }

    async fn table(&self, table: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(table) {
            debug!(table, "ClickHouseSchemaProvider creating TableProvider");
            self.factory
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

#[cfg(feature = "federation")]
pub mod federation {
    use std::any::Any;
    use std::sync::Arc;

    use dashmap::DashMap;
    use datafusion::catalog::{
        CatalogProvider, MemorySchemaProvider, SchemaProvider, TableProvider,
    };
    use datafusion::common::exec_err;
    use datafusion::error::{DataFusionError, Result};
    // Re-export
    pub use datafusion_federation::sql::MultiSchemaProvider;
    use tracing::debug;

    pub const DEFAULT_NON_FEDERATED_SCHEMA: &str = "internal";

    // TODO: Docs - speak about WHY this would be done. For example, to allow federating catalogs
    // across PostgresSQL and ClickHouse.
    //
    /// A "federated" [`CatalogProvider`] that allows providing different catalog providers that can
    /// be used to serve up [`SchemaProvider`]s across multiple catalogs.
    ///
    /// See [`MultiSchemaProvider`] for the equivalent functionality across schemas.
    #[derive(Debug, Clone)]
    pub struct FederatedCatalogProvider {
        default_schema: String,
        schemas:        DashMap<String, Arc<dyn SchemaProvider>>,
    }

    impl Default for FederatedCatalogProvider {
        fn default() -> Self { Self::new() }
    }

    impl FederatedCatalogProvider {
        /// Create a new [`FederatedCatalogProvider`].
        ///
        /// Uses "datafusion" as the 'non-federated' default catalog.
        /// Uses "internal" as the 'non-federated' default schema.
        ///
        /// # Panics
        /// - Should not panic, the trait method returns `Result`, but this call is infallible.
        pub fn new() -> Self {
            Self::new_with_default_schema(DEFAULT_NON_FEDERATED_SCHEMA).unwrap()
        }

        /// Create a new [`FederatedCatalogProvider`] providing the default catalog and schema for
        /// 'non-federated' tables.
        ///
        /// Args:
        /// - `default_catalog`: The default catalog name.
        /// - `default_schema`: The default schema name.
        ///
        /// # Errors
        /// - Returns an error if the schema is `information_schema`
        ///
        /// # Panics
        /// - Should not panic, the method `register_schema` returns `Result`, but the call is
        ///   infallible for `MemoryCatalogProvider`.
        pub fn new_with_default_schema(default_schema: impl Into<String>) -> Result<Self> {
            let default_schema: String = default_schema.into();
            if default_schema == "information_schema" {
                return Err(DataFusionError::Internal(
                    "information_schema is reserved".to_string(),
                ));
            }

            let schemas = DashMap::new();
            drop(schemas.insert(
                default_schema.clone(),
                Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>,
            ));
            Ok(Self { default_schema, schemas })
        }

        /// Add a new catalog to the federated catalog provider.
        ///
        /// # Returns
        /// - Returns a vector of removed (duplicate overwritten) schemas.
        pub fn add_catalog(
            &self,
            catalog: &Arc<dyn CatalogProvider>,
        ) -> Vec<Arc<dyn SchemaProvider>> {
            let schemas = catalog.schema_names();
            let mut removed = Vec::with_capacity(schemas.len());
            for name in schemas {
                debug!("Adding schema: {name}");
                if let Some(schema) = catalog.schema(&name)
                    && let Some(old) = self.schemas.insert(name, schema)
                {
                    removed.push(old);
                }
            }
            removed
        }

        /// Add a new schema to the federated catalog provider.
        ///
        /// # Returns
        /// - Returns a removed schema if it already existed.
        pub fn add_schema(
            &self,
            name: impl Into<String>,
            schema: Arc<dyn SchemaProvider>,
        ) -> Option<Arc<dyn SchemaProvider>> {
            let name = name.into();
            debug!("Adding schema: {name}");
            self.schemas.insert(name, schema)
        }

        // TODO: Docs - explain why should the catalog be registered this way, ie that it enables
        // the federation with this catalog providers schemas
        //
        // TODO: Remove - remove the panics, just return an error, or at least use debug assertions
        /// Register a non-federated table in the "internal" schema of the "datafusion" catalog.
        ///
        /// Arguments:
        /// - `name`: The name of the table to register.
        /// - `table`: The table provider to register.
        ///
        /// # Errors
        /// - Returns an error if table registration fails.
        ///
        /// # Panics
        /// - Should not panic, the catalog and schema are initialized in the constructor.
        pub fn register_non_federated_table(
            &self,
            name: String,
            table: Arc<dyn TableProvider>,
        ) -> Result<Option<Arc<dyn TableProvider>>> {
            debug!("Registering non-federated table: {name}");
            self.schemas
                .get(&self.default_schema)
                .expect("schema registered in constructor")
                .register_table(name, table)
        }
    }

    impl CatalogProvider for FederatedCatalogProvider {
        fn as_any(&self) -> &dyn Any { self }

        fn schema_names(&self) -> Vec<String> {
            self.schemas.iter().map(|c| c.key().clone()).collect()
        }

        fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            if let Some(schema) = self.schemas.get(name) {
                let is_fed = if name == self.default_schema { "NonFederated" } else { "Federated" };
                debug!("FederatedCatalogProvider found schema ({is_fed}): {name}");
                return Some(Arc::clone(schema.value()));
            }
            None
        }

        // TODO: Docs - note how this registers a NON federated table
        fn register_schema(
            &self,
            name: &str,
            schema: Arc<dyn SchemaProvider>,
        ) -> Result<Option<Arc<dyn SchemaProvider>>> {
            Ok(self.schemas.insert(name.into(), schema))
        }

        fn deregister_schema(
            &self,
            name: &str,
            cascade: bool,
        ) -> Result<Option<Arc<dyn SchemaProvider>>> {
            if !self.schemas.contains_key(name) {
                return Ok(None);
            }
            let removed =
                self.schemas.remove_if(name, |_, v| cascade || v.table_names().is_empty());

            // This means attempt to drop non-empty table without cascade, since existence was
            // checked above, and None implies the schema contained tables
            if !cascade && removed.is_none() {
                return exec_err!("Cannot drop schema {} because other tables depend on it", name);
            }
            Ok(removed.map(|r| r.1))
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;

        use datafusion::arrow::array::RecordBatch;
        use datafusion::arrow::datatypes::Schema;
        use datafusion::catalog::{
            CatalogProvider, MemTable, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
        };

        use super::*;

        #[test]
        fn test_federated_catalog_creation() {
            let catalog = FederatedCatalogProvider::default();
            assert_eq!(
                &catalog.default_schema, DEFAULT_NON_FEDERATED_SCHEMA,
                "Unexpected default schema"
            );

            let catalog = FederatedCatalogProvider::new_with_default_schema("information_schema");
            assert!(catalog.is_err(), "Cannot create catalog with invalid default schema");
        }

        #[test]
        fn test_add_catalog() {
            let mem_schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
            let catalog = FederatedCatalogProvider::default();
            drop(catalog.add_schema("memory", Arc::clone(&mem_schema)));

            let mem_catalog = MemoryCatalogProvider::new();
            drop(mem_catalog.register_schema("memory", Arc::clone(&mem_schema)).unwrap());

            let result = catalog.add_catalog(&(Arc::new(mem_catalog) as Arc<dyn CatalogProvider>));
            assert_eq!(result.len(), 1, "Expected MemorySchema returned");

            let result = catalog.register_schema("memory", Arc::clone(&mem_schema));
            assert!(result.is_ok());
            let returned = result.unwrap();
            assert!(returned.is_some(), "Expected a schema returned");
            drop(returned.unwrap());

            let result = catalog.deregister_schema("non-existent", false);
            assert!(result.is_ok());
            let returned = result.unwrap();
            assert!(returned.is_none(), "Expected no schema returned");

            let result = catalog.deregister_schema("memory", false);

            assert!(result.is_ok(), "Expected schema not found");
            let returned = result.unwrap();
            assert!(returned.is_some(), "Expected a schema removed");

            let schema = returned.unwrap();
            let schema_ref = Arc::new(Schema::empty());
            let partition = RecordBatch::new_empty(Arc::clone(&schema_ref));
            let table = Arc::new(MemTable::try_new(schema_ref, vec![vec![partition]]).unwrap());
            drop(schema.register_table("mem_table".into(), table).unwrap());
            drop(catalog.register_schema("memory", schema).unwrap());

            let result = catalog.deregister_schema("memory", false);
            assert!(result.is_err(), "Expected non-cascase to fail");

            let result = catalog.deregister_schema("memory", true);
            assert!(result.is_ok(), "Expected cascase to succeed");
            let returned = result.unwrap();
            assert!(returned.is_some(), "Expected a schema removed");

            // Test register non federated table
            let schema_ref = Arc::new(Schema::empty());
            let partition = RecordBatch::new_empty(Arc::clone(&schema_ref));
            let table = Arc::new(MemTable::try_new(schema_ref, vec![vec![partition]]).unwrap());
            let prev = catalog.register_non_federated_table("non_federated".into(), table).unwrap();
            assert!(prev.is_none());

            // Test downcast
            let any_catalog = Arc::new(catalog.clone()) as Arc<dyn CatalogProvider>;
            let downcasted =
                any_catalog.as_any().downcast_ref::<FederatedCatalogProvider>().unwrap();
            let schema_names = downcasted.schema_names();
            assert_eq!(
                schema_names,
                vec![DEFAULT_NON_FEDERATED_SCHEMA.to_string(),],
                "Expected all registered tables"
            );
            let non_federated = downcasted.schema(DEFAULT_NON_FEDERATED_SCHEMA);
            assert!(non_federated.is_some(), "Expected non_federated schema returned");
        }
    }
}

// The following tests are provided to bridge a gap in coverage over mocked methods
#[cfg(all(test, feature = "test-utils", feature = "mocks"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mocked_methods() {
        // Catalog provider
        let pool = Arc::new(ClickHouseConnectionPool::new("pool".to_string(), ()));
        let catalog = ClickHouseCatalogProvider::try_new(Arc::clone(&pool)).await.unwrap();
        // Test downcast
        let any_catalog = Arc::new(catalog.clone()) as Arc<dyn CatalogProvider>;
        let downcasted = any_catalog.as_any().downcast_ref::<ClickHouseCatalogProvider>().unwrap();
        let schema_names = downcasted.schema_names();
        assert_eq!(schema_names, vec!["default".to_string()], "Only contains default");
        let not_exist = downcasted.schema("not-exists");
        assert!(not_exist.is_none(), "No schemas in a mocked catalog");

        // Schema provider
        let schema_provider =
            ClickHouseSchemaProvider::new("test".into(), vec!["test_table".to_string()], pool);
        tracing::debug!("{schema_provider:?}");
        // Test downcast
        let any_schema = Arc::new(schema_provider.clone()) as Arc<dyn SchemaProvider>;
        let downcasted = any_schema.as_any().downcast_ref::<ClickHouseSchemaProvider>().unwrap();
        let table_names = downcasted.table_names();
        assert_eq!(
            table_names,
            vec!["test_table".to_string()],
            "Should contain the table just added"
        );
        let not_exist = downcasted.table("not-exists").await.unwrap();
        assert!(not_exist.is_none(), "No tables in a mocked schema");
    }
}
