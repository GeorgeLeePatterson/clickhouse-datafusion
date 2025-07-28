//! Implementations for federating `ClickHouse` schemas into a `DataFusion` [`SessionContext`].
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
pub use datafusion_federation; // Re-export
use datafusion_federation::sql::{
    RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use tracing::debug;

use crate::dialect::ClickHouseDialect;
use crate::table_provider::ClickHouseTableProvider;

// TODO: Docs - Need a lot more explaining here. Also, how does this interplay with the structures
// in `context`? Need to consolidate and define this, ensure the order is hard to mess up.
//
/// Use to modify an existing [`SessionContext`] to be used in a federated context, pushing queries
/// and statements down to the sql to be run on remote schemas.
pub trait FederatedContext {
    fn federate(self) -> SessionContext;

    fn is_federated(&self) -> bool;
}

impl FederatedContext for SessionContext {
    fn federate(self) -> SessionContext {
        use datafusion_federation::{FederatedQueryPlanner, default_optimizer_rules};

        let state = self.state();

        if state.optimizer().rules.iter().any(|rule| rule.name() == "federation_optimizer_rule") {
            self
        } else {
            SessionContext::new_with_state(
                self.into_state_builder()
                    .with_optimizer_rules(default_optimizer_rules())
                    .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
                    .build(),
            )
        }
    }

    fn is_federated(&self) -> bool {
        self.state()
            .optimizer()
            .rules
            .iter()
            .any(|rule| rule.name() == "federation_optimizer_rule")
    }
}

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
        // No custom AST rewriting needed for now; arrayJoin and other functions handled by dialect
        None
    }

    fn execute(&self, query: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
        self.execute_sql(query, schema)
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        self.writer
            .connect()
            .await?
            .tables(self.table.schema().expect("Schema must be present"))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        self.writer
            .connect()
            .await?
            .get_schema(&TableReference::from(table_name))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

pub mod catalog {
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
            self.schemas.iter().map(|c| c.key().to_string()).collect()
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
                self.schemas.remove_if(name, |_, v| (cascade || !v.table_names().is_empty()));
            // This means attempt to drop non-empty table without cascade
            if !cascade && removed.is_none() {
                return exec_err!("Cannot drop schema {} because other tables depend on it", name);
            }
            Ok(removed.map(|r| r.1))
        }
    }
}
