//! The various builders to make registering, updating, and synchronizing remote `ClickHouse`
//! databases and `DataFusion` easier.
//!
//! # `ClickHouseBuilder`
//!
//! You begin by creating a `ClickHouseBuilder` instance, which allows you to configure various
//! aspects of the connection, such as the endpoint, authentication, the underlying `ClickHouse`
//! connection pool, `clickhouse-arrow` `Client` options, schema mapping, and more.
//!
//! NOTE: The schema (database) set as the default on the underlying `ClickHouse` client will be set
//! as the initial schema after calling `ClickHouseBuilder::build_catalog`. This can be changed on
//! the fly by calling `with_schema` on the catalog builder returned.
//!
//! ## Schema Coercion
//!
//! You can also specify whether "schema coercion" should occur during streaming of the data
//! returned by query execution. If using the full [`super::context::ClickHouseSessionContext`] and
//! [`super::context::ClickHouseQueryPlanner`], required if you plan on using `ClickHouse`
//! functions, then the return type provided to the second argument of the `clickhouse(...)` scalar
//! UDFs will specify the expected return type.
//!
//! This means you must be exact, otherwise an arrow error will be returned. By configuring schema
//! coercion, anything that can be coerced via arrow will be coerced, based on the expected schema
//! of the `clickhouse` function mentioned above.
//!
//! Generally schema conversion should be avoided, as there is a non-zero cost. The cost is incurred
//! per `RecordBatch` as the results are streamed through `DataFusion`'s execution context, which
//! means it will have a per `RecordBatch` cost that can be avoided by simply providing the correct
//! return type to the clickhouse function. But, for non latency-sensitive use cases, this may
//! provide convenience, especially when dealing with `ClickHouse` higher order functions.
//!
//! # `ClickHouseCatalogBuilder`
//!
//! After configuration of the `ClickHouseBuilder`, a `ClickHouseCatalogBuilder` will be obtained by
//! calling `ClickHouseBuilder::build_catalog`. The `ClickHouseCatalogBuilder` returned can then be
//! used to set the schema being targets, create tables on the remote schema, register existing
//! tables under aliases, and refresh `DataFusion`'s internal catalog to keep the two in sync.
//!
//! Refer to e2e tests in the repository for detailed examples on its usage, especially the tests's
//! associated "helper" functions. The `basic` example shows a basic usage of the
//! `ClickHouseBuilder` that suffices for most use cases.
use std::collections::HashMap;
use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::{
    ArrowConnectionPoolBuilder, ArrowOptions, ArrowPoolBuilder, ClientBuilder, CreateOptions,
    Destination,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{CatalogProvider, TableProviderFactory};
use datafusion::common::{Constraints, DFSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::CreateExternalTable;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use tracing::{debug, error};

use crate::connection::ClickHouseConnectionPool;
use crate::providers::catalog::ClickHouseCatalogProvider;
use crate::providers::table_factory::ClickHouseTableFactory;
use crate::table_factory::ClickHouseTableProviderFactory;
use crate::utils::{self, ENDPOINT_PARAM, default_str_to_expr, register_builtins};

/// The default `DataFusion` catalog name if no other name is provided.
pub const DEFAULT_CLICKHOUSE_CATALOG: &str = "clickhouse";

/// Simple function to provide default [`ArrowOptions`] fit for common use
pub fn default_arrow_options() -> ArrowOptions {
    ArrowOptions::default()
        .with_strings_as_strings(true)
        .with_strict_schema(false)
        .with_disable_strict_schema_ddl(true)
        .with_nullable_array_default_empty(true)
}

/// Entrypoint builder for `ClickHouse` and `DataFusion` integration.
///
/// Ensures `DataFusion` default UDFs as well as `ClickHouse` udfs are registered in
/// [`SessionContext`], the pool is attached to the [`ClickHouseTableProviderFactory`], and
/// `ClickHouse` is reachable by the provided endpoint.
///
/// NOTE: While `clickhouse_arrow` defaults to binary encoding for strings (via
/// [`clickhouse_arrow::ArrowOptions::strings_as_strings`] == false), for `DataFusion` the
/// default is `true`. This can be disabled by modifying the setting via
/// [`ClickHouseBuilder::configure_arrow_options`]
pub struct ClickHouseBuilder {
    endpoint:     Destination,
    pool_builder: ArrowConnectionPoolBuilder,
    factory:      ClickHouseTableProviderFactory,
}

impl ClickHouseBuilder {
    /// Create new `ClickHouseBuilder` that can be used to configure both the
    /// [`clickhouse_arrow::ClientBuilder`], the [`clickhouse_arrow::ArrowPoolBuilder`], and be
    /// "built" into a [`ClickHouseCatalogBuilder`] for interacting with `ClickHouse` databases and
    /// tables.
    pub fn new(endpoint: impl Into<Destination>) -> Self {
        let endpoint = endpoint.into();
        let pool_builder = ArrowConnectionPoolBuilder::new(endpoint.clone())
            .configure_client(|c| c.with_arrow_options(default_arrow_options()));
        Self { endpoint, pool_builder, factory: ClickHouseTableProviderFactory::new() }
    }

    pub fn new_with_pool_builder(
        endpoint: impl Into<Destination>,
        pool_builder: ArrowConnectionPoolBuilder,
    ) -> Self {
        let endpoint = endpoint.into();
        Self { endpoint, pool_builder, factory: ClickHouseTableProviderFactory::new() }
    }

    /// Configure whether to coerce schemas to match expected schemas during query execution.
    /// Remember, there is a non-zero cost incurred per `RecordBatch`, and this is mainly useful for
    /// allowing looser return types when using clickhouse functions.
    #[must_use]
    pub fn with_coercion(mut self, coerce: bool) -> Self {
        self.factory = self.factory.with_coercion(coerce);
        self
    }

    /// Configure the underlying [`clickhouse_arrow::ClientBuilder`].
    #[must_use]
    pub fn configure_client(mut self, f: impl FnOnce(ClientBuilder) -> ClientBuilder) -> Self {
        self.pool_builder = self.pool_builder.configure_client(f);
        self
    }

    /// Configure the underlying [`clickhouse_arrow::ArrowPoolBuilder`].
    #[must_use]
    pub fn configure_pool(mut self, f: impl FnOnce(ArrowPoolBuilder) -> ArrowPoolBuilder) -> Self {
        self.pool_builder = self.pool_builder.configure_pool(f);
        self
    }

    /// `clickhouse_arrow` defaults to binary encoding for
    /// [`datafusion::arrow::datatypes::DataType::Utf8`] columns, the default is inverted for
    /// `DataFusion`. This method allows disabling that change to use binary encoding for strings,
    /// among other configuration options.
    ///
    /// See: [`clickhouse_arrow::ArrowOptions`]
    #[must_use]
    pub fn configure_arrow_options(mut self, f: impl FnOnce(ArrowOptions) -> ArrowOptions) -> Self {
        self.pool_builder = self.pool_builder.configure_client(|c| {
            let options = c.options().ext.arrow.unwrap_or(default_arrow_options());
            c.with_arrow_options(f(options))
        });
        self
    }

    /// Build ensures `ClickHouse` builtins are registered (such as nested functions), the pool is
    /// attached to the factory, the `ClickHouse` endpoint is reachable, and the catalog is created
    /// and registered to the [`SessionContext`].
    ///
    /// # Errors
    /// - Returns an error if the `ClickHouse` endpoint is unreachable
    /// - Returns an error if the `ClickHouse` catalog fails to be created
    pub async fn build_catalog(
        self,
        ctx: &SessionContext,
        catalog: Option<&str>,
    ) -> Result<ClickHouseCatalogBuilder> {
        // Register built in functions and clickhouse udfs
        register_builtins(ctx);

        let catalog = catalog.unwrap_or(DEFAULT_CLICKHOUSE_CATALOG).to_string();
        let database = self.pool_builder.client_options().default_database.clone();
        debug!(catalog, database, "Attaching pool to ClickHouse table factory");

        let endpoint = self.endpoint.to_string();
        let factory = self.factory;
        let pool = Arc::new(factory.attach_pool_builder(self.endpoint, self.pool_builder).await?);

        ClickHouseCatalogBuilder::try_new(ctx, catalog, database, endpoint, pool, factory).await
    }
}

/// [`ClickHouseCatalogBuilder`] can be used to create tables, register existing tables, and finally
/// refresh the `ClickHouse` catalog in `DataFusion`.
///
/// IMPORTANT! After creating tables, one of the build variations, ie `Self::build` or
/// `Self::build_schema`, must be called to ensure the catalog provider is up to date with the
/// remote `ClickHouse` database. If you forget to do this, `DataFusion` queries targeting one of
/// these tables will fail.
#[derive(Clone)]
pub struct ClickHouseCatalogBuilder {
    catalog:  String,
    /// The current schema the builder is targeting.
    schema:   String,
    /// The configured remote endpoint the underlying `ClickHouse` pool is connected.
    endpoint: String,
    /// The `ClickHouse` connection used to communicate with the remote `ClickHouse` database.
    pool:     Arc<ClickHouseConnectionPool>,
    /// This factory is used to create new tables in the remote `ClickHouse` database. This builder
    /// must be built with one of the builder variations, ie `Self::build` or `Self::build_schema`,
    /// so that the provider reflects the most current remote schema.
    factory:  ClickHouseTableProviderFactory,
    /// The catalog provider is a passive catalog provider, meaning it must be "refreshed" after
    /// table creation or schema change of any type to the remove `ClickHouse` database.
    provider: Arc<ClickHouseCatalogProvider>,
}

impl ClickHouseCatalogBuilder {
    // TODO: Docs
    /// Internal constructor
    async fn try_new(
        ctx: &SessionContext,
        catalog: impl Into<String>,
        default_schema: impl Into<String>,
        endpoint: impl Into<String>,
        pool: Arc<ClickHouseConnectionPool>,
        factory: ClickHouseTableProviderFactory,
    ) -> Result<Self> {
        let schema = default_schema.into();
        let catalog = catalog.into();
        let endpoint = endpoint.into();

        // Must have a schema
        let schema = if schema.is_empty() { "default".to_string() } else { schema };

        // Ensure the database exists if not default
        if schema != "default" {
            debug!(schema, "Database not default, attempting create");
            utils::create_database(&schema, &pool).await?;
        }

        // Register catalog or create a new one
        let provider = if factory.coerce_schemas() {
            ClickHouseCatalogProvider::try_new_with_coercion(Arc::clone(&pool)).await
        } else {
            ClickHouseCatalogProvider::try_new(Arc::clone(&pool)).await
        }
        .inspect_err(|error| error!(?error, "Failed to register catalog {catalog}"))?;

        let provider = Arc::new(provider);

        drop(
            ctx.register_catalog(
                catalog.as_str(),
                Arc::clone(&provider) as Arc<dyn CatalogProvider>,
            ),
        );

        Ok(ClickHouseCatalogBuilder { catalog, schema, endpoint, pool, factory, provider })
    }

    /// Return the name of the catalog in `DataFusion`'s context that this builder is configuring.
    pub fn name(&self) -> &str { &self.catalog }

    /// Return the currently set schema (database) being targeted. Can be changed on the fly by
    /// calling `Self::with_schema`.
    pub fn schema(&self) -> &str { &self.schema }

    /// Update the current "schema" (database) that this builder is targeting, and continue
    /// building.
    ///
    /// # Errors
    /// - Returns an error if the schema needs to be created and fails
    pub async fn with_schema(mut self, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        // Don't re-create if schema is already set
        if name == self.schema {
            return Ok(self);
        }
        self.schema = name;

        // Ensure the database exists if not default
        if self.schema != "default" {
            debug!(schema = self.schema, "Database not default, attempting create");
            utils::create_database(&self.schema, &self.pool).await?;
        }

        // Be sure to refresh the catalog before setting the schema
        self.provider.refresh_catalog(&self.pool).await?;

        Ok(self)
    }

    /// Create a new table in the remote `ClickHouse` instance.
    ///
    /// # Arguments
    /// - `name`: The name of the table to create.
    /// - `engine`: The engine to use for the table.
    /// - `schema`: The schema of the table.
    ///
    /// # Returns
    /// - A [`ClickHouseTableCreator`] that can be used to create tables in the remote `ClickHouse`
    ///   instance.
    pub fn with_new_table(
        &self,
        name: impl Into<String>,
        engine: impl Into<ClickHouseEngine>,
        schema: SchemaRef,
    ) -> ClickHouseTableCreator {
        let table = name.into();
        let options = CreateOptions::new(engine.into().to_string());
        debug!(schema = self.schema, table, ?options, "Initializing table creator");
        ClickHouseTableCreator { name: table, builder: self.clone(), options, schema }
    }

    /// Create a new table in the remote `ClickHouse` instance.
    ///
    /// # Arguments
    /// - `name`: The name of the table to create.
    /// - `schema`: The schema of the table.
    /// - `options`: More detailed `CreateOptions` for creating the provided table.
    ///
    /// # Returns
    /// - A [`ClickHouseTableCreator`] that can be used to create tables in the remote `ClickHouse`
    ///   instance.
    pub fn with_new_table_and_options(
        &self,
        name: impl Into<String>,
        schema: SchemaRef,
        options: CreateOptions,
    ) -> ClickHouseTableCreator {
        let table = name.into();
        debug!(schema = self.schema, table, ?options, "Initializing table creator");
        ClickHouseTableCreator { name: table, builder: self.clone(), options, schema }
    }

    /// Register an existing `ClickHouse` table, optionally renaming it in the provided session
    /// state.
    ///
    /// # Errors
    /// - Returns an error if the table does not exist in the remote database
    /// - Returns an error if the table cannot be registered to the context
    pub async fn register_existing_table(
        &self,
        name: impl Into<TableReference>,
        name_as: Option<impl Into<TableReference>>,
        ctx: &SessionContext,
    ) -> Result<()> {
        let name = name.into();
        let database = name.schema().unwrap_or(&self.schema);
        let exists =
            self.pool.connect().await?.tables(database).await?.contains(&name.table().to_string());

        if !exists {
            return Err(DataFusionError::Plan(format!(
                "Table '{name}' does not exist in ClickHouse database '{database}', use \
                 `table_creator` instead"
            )));
        }

        let table = TableReference::full(self.catalog.as_str(), database, name.table());
        let table_as = name_as.map(Into::into).unwrap_or(table.clone());

        let factory = ClickHouseTableFactory::new(Arc::clone(&self.pool));
        let provider = factory.table_provider(table).await?;
        debug!(?table_as, "Registering ClickHouse table provider");
        drop(ctx.register_table(table_as, provider)?);

        Ok(())
    }

    /// Build the current `schema` (database) being managed by this catalog, optionally registering
    /// a new schema to continue building
    ///
    /// Note: For the `SessionContext` to recognize the added tables and updated schema, either this
    /// function or `Self::build` must be called.
    ///
    /// # Errors
    /// - Returnes an error if an error occurs while refreshing the catalog
    pub async fn build_schema(
        mut self,
        new_schema: Option<String>,
        ctx: &SessionContext,
    ) -> Result<Self> {
        let _catalog = self.build_internal(ctx).await?;
        self.schema = new_schema.unwrap_or(self.schema);
        Ok(self)
    }

    /// Re-register the catalog, updating the [`SessionContext`], and return the updated context.
    ///
    /// Note: Important! For the [`SessionContext`] to recognize the added tables and updated
    /// schema, either this function or `Self::build` must be called. For that reason, it is
    /// important to  use the [`SessionContext`] provided back from this function.
    ///
    /// # Errors
    /// - Returns an error if the `SessionContext` has not been federated
    /// - Returnes an error if an error occurs while refreshing the catalog
    /// - Returns an error if the "federation" feature is enabled but the context is not federated
    pub async fn build(&self, ctx: &SessionContext) -> Result<Arc<ClickHouseCatalogProvider>> {
        #[cfg(feature = "federation")]
        {
            use datafusion::common::exec_err;

            use crate::federation::FederatedContext as _;

            if !ctx.is_federated() {
                return exec_err!(
                    "Building this schema with federation enabled but no federated SessionContext \
                     will fail. Call `ctx.federate()` before providing a context to build with."
                );
            }
        }

        self.build_internal(ctx).await
    }

    /// Re-registers the catalog provider, re-configures the [`SessionContext`], and return a
    /// clone of the catalog.
    ///
    /// # Errors
    /// - Returnes an error if an error occurs while refreshing the catalog
    async fn build_internal(&self, ctx: &SessionContext) -> Result<Arc<ClickHouseCatalogProvider>> {
        let catalog = Arc::clone(&self.provider);
        debug!(catalog = self.catalog, "ClickHouse catalog created");

        // Re-register UDFs
        register_builtins(ctx);

        // Re-register catalog
        catalog.refresh_catalog(&self.pool).await?;
        drop(ctx.register_catalog(&self.catalog, Arc::clone(&catalog) as Arc<dyn CatalogProvider>));

        Ok(catalog)
    }
}

/// Builder phase for creating `ClickHouse` tables.
#[derive(Clone)]
pub struct ClickHouseTableCreator {
    name:    String,
    builder: ClickHouseCatalogBuilder,
    schema:  SchemaRef,
    options: CreateOptions,
}

impl ClickHouseTableCreator {
    /// Update the underlying table create options that will be passed to clickhouse.
    ///
    /// See [`CreateOptions`] for more information.
    #[must_use]
    pub fn update_create_options(
        mut self,
        update: impl Fn(CreateOptions) -> CreateOptions,
    ) -> Self {
        self.options = update(self.options);
        self
    }

    /// Create the table, returning back a [`ClickHouseCatalogBuilder`] to create more tables.
    ///
    /// # Errors
    /// - Returns an error if the `TableProviderFactory` fails to create the table
    /// - Returnes an error if an error occurs while refreshing the catalog
    pub async fn create(self, ctx: &SessionContext) -> Result<ClickHouseCatalogBuilder> {
        let schema = self.builder.schema.clone();
        let table = self.name;

        let column_defaults = self
            .options
            .clone()
            .defaults
            .unwrap_or_default()
            .into_iter()
            .map(|(col, value)| (col, default_str_to_expr(&value)))
            .collect::<HashMap<_, _>>();

        let mut options = utils::create_options_to_params(self.options).into_params();
        drop(options.insert(ENDPOINT_PARAM.into(), self.builder.endpoint.clone()));

        let table_ref = TableReference::partial(schema.as_str(), table.as_str());
        let cmd = CreateExternalTable {
            name: table_ref.clone(),
            schema: Arc::new(DFSchema::try_from(Arc::clone(&self.schema))?),
            options,
            column_defaults,
            constraints: Constraints::default(),
            table_partition_cols: vec![],
            if_not_exists: false,
            location: String::new(),
            file_type: String::new(),
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
        };
        let _provider = self
            .builder
            .factory
            .create(&ctx.state(), &cmd)
            .await
            .inspect_err(|error| error!(?error, table, "Factory error creating table"))?;
        debug!(table, "Table created, catalog will be refreshed in `build`");

        // Refresh the catalog after creating the table
        drop(self.builder.build_internal(ctx).await?);

        Ok(self.builder)
    }
}
