//! # `ClickHouse` SQL `DataFusion` `TableProvider`
//!
//! This module implements a SQL [`TableProvider`] for `DataFusion`.
//!
//! This is used as a fallback if the `datafusion-federation` optimizer is not enabled.
use std::any::Any;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::sql::TableReference;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};

use crate::connection::ClickHouseConnectionPool;
use crate::dialect::ClickHouseDialect;
use crate::stream::RecordBatchStream;

/// Helper function to execute a SQL query and return a stream of record batches.
pub fn execute_sql_query(
    sql: impl Into<String>,
    pool: Arc<ClickHouseConnectionPool>,
    schema: SchemaRef,
    coerce_schema: bool,
) -> SendableRecordBatchStream {
    let sql = sql.into();
    tracing::debug!("Running sql: {sql}");
    Box::pin(RecordBatchStream::new_from_query(sql, pool, schema, coerce_schema))
}

/// Controls whether join pushdown is allowed, and under what conditions
///
/// Mainly here to provider future interop with [`datafusion-table-providers`]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum JoinPushDown {
    /// This connection pool should not allow join push down. (i.e. we don't know under what
    /// conditions it is safe to send a join query to the database)
    Disallow,
    /// Allows join push down for other tables that share the same context.
    ///
    /// The context can be part of the connection string that uniquely identifies the server.
    AllowedFor(String),
}

/// A [`TableProvider`] for remote `ClickHouse` sql tables.
#[derive(Clone)]
pub struct SqlTable {
    pub table_reference:      TableReference,
    pub(crate) coerce_schema: bool,
    pool:                     Arc<ClickHouseConnectionPool>,
    name:                     String,
    schema:                   SchemaRef,
    dialect:                  Option<Arc<ClickHouseDialect>>,
    exprs:                    Option<Vec<Expr>>,
}

impl fmt::Debug for SqlTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlTable")
            .field("name", &self.name)
            .field("schema", &self.schema)
            .field("table_reference", &self.table_reference)
            .field("pool", &"ClickHouseConnectionPool")
            .field("dialect", &self.dialect)
            .field("exprs", &self.exprs)
            .field("coerce_schema", &self.coerce_schema)
            .finish()
    }
}

impl SqlTable {
    /// Create a new [`SqlTable`] from a name and a [`ClickHouseConnectionPool`].
    ///
    /// # Errors
    /// - Returns an error if the table does not exist.
    pub async fn try_new(
        name: &str,
        pool: Arc<ClickHouseConnectionPool>,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self> {
        let table_reference = table_reference.into();
        let schema = pool.connect().await?.get_schema(&table_reference).await?;
        Ok(Self::new_with_schema_unchecked(name, pool, schema, table_reference))
    }

    // TODO: Remove - docs
    pub fn new_with_schema_unchecked(
        name: &str,
        pool: Arc<ClickHouseConnectionPool>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<TableReference>,
    ) -> Self {
        Self {
            name: name.to_owned(),
            pool,
            schema: schema.into(),
            table_reference: table_reference.into(),
            dialect: Some(Arc::new(ClickHouseDialect)),
            exprs: None,
            coerce_schema: false,
        }
    }

    #[must_use]
    pub fn with_exprs(mut self, exprs: Vec<Expr>) -> Self {
        self.exprs = Some(exprs);
        self
    }

    /// Whether to coerce the `RecordBatch`es returned by `SQLExecutor::execute` using the provided
    /// `LogicalPlan`'s schema.
    #[must_use]
    pub fn with_coercion(mut self, coerce: bool) -> Self {
        self.coerce_schema = coerce;
        self
    }

    /// Access the underlying connection pool
    pub fn pool(&self) -> &Arc<ClickHouseConnectionPool> { &self.pool }

    // Return the current memory location of the object as a unique identifier
    pub(crate) fn unique_id(&self) -> usize { std::ptr::from_ref(self) as usize }

    fn dialect(&self) -> &(dyn Dialect + Send + Sync) {
        match &self.dialect {
            Some(dialect) => dialect.as_ref(),
            None => &DefaultDialect {},
        }
    }

    #[cfg(feature = "federation")]
    fn arc_dialect(&self) -> Arc<dyn Dialect + Send + Sync> {
        match &self.dialect {
            Some(dialect) => Arc::clone(dialect) as Arc<dyn Dialect>,
            None => Arc::new(DefaultDialect {}),
        }
    }
}

// Methods for generating SQL queries from a LogicalPlan and ExecutionPlans from SQL.
impl SqlTable {
    /// # Errors
    /// - Returns an error if the logical plan creation fails
    fn scan_to_sql(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<String> {
        let logical_plan = self.create_logical_plan(projection, filters, limit)?;
        let sql = Unparser::new(self.dialect()).plan_to_sql(&logical_plan)?.to_string();
        Ok(sql)
    }

    fn create_logical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<LogicalPlan> {
        // Ensure table reference does not include `catalog`
        let table_ref = match self.table_reference.clone() {
            TableReference::Full { schema, table, .. } => TableReference::partial(schema, table),
            table_ref => table_ref,
        };
        let table_source = LogicalTableSource::new(self.schema());
        let mut builder = LogicalPlanBuilder::scan_with_filters(
            table_ref,
            Arc::new(table_source),
            projection.cloned(),
            filters.to_vec(),
        )?;
        if let Some(exprs) = &self.exprs {
            builder = builder.project(exprs.clone())?;
        }
        builder.limit(0, limit)?.build()
    }

    fn create_physical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        sql: String,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            ClickHouseSqlExec::try_new(projection, &self.schema(), Arc::clone(&self.pool), sql)?
                .with_coercion(self.coerce_schema),
        ))
    }
}

#[async_trait]
impl TableProvider for SqlTable {
    fn as_any(&self) -> &dyn Any { self }

    fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }

    fn table_type(&self) -> TableType { TableType::Base }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let filter_push_down: Vec<TableProviderFilterPushDown> = filters
            .iter()
            .map(|f| match Unparser::new(self.dialect()).expr_to_sql(f) {
                Ok(_) => TableProviderFilterPushDown::Exact,
                Err(_) => TableProviderFilterPushDown::Unsupported,
            })
            .collect();

        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sql = self.scan_to_sql(projection, filters, limit)?;
        return self.create_physical_plan(projection, sql);
    }
}

impl Display for SqlTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ClickHouseSqlTable {}", self.name)
    }
}

/// Project a schema safely, taking into account empty columns.
///
/// When DataFusion executes queries like `COUNT(*)`, it passes an empty projection
/// (`Some([])`) indicating no columns are needed. This function returns an empty
/// schema in that case, as DataFusion only needs row counts, not column values.
///
/// The underlying SQL still generates `SELECT 1 FROM table` for ClickHouse, but
/// the RecordBatchStream schema correctly reflects that no column data is accessed.
///
/// # Errors
/// - Returns an error if the schema projection fails
pub fn project_schema_safe(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<SchemaRef> {
    let schema = match projection {
        Some(columns) => {
            if columns.is_empty() {
                // Empty projection = no columns needed (e.g., COUNT(*))
                // Return empty schema to match DataFusion's logical plan expectations
                Arc::new(Schema::empty())
            } else {
                Arc::new(schema.project(columns)?)
            }
        }
        None => Arc::clone(schema),
    };
    Ok(schema)
}

/// A custom [`ExecutionPlan`] for [`SqlTable`]s.
#[derive(Clone)]
pub struct ClickHouseSqlExec {
    projected_schema: SchemaRef,
    pool:             Arc<ClickHouseConnectionPool>,
    sql:              String,
    properties:       PlanProperties,
    coerce_schema:    bool,
}

impl ClickHouseSqlExec {
    /// Create a new [`ClickHouseSqlExec`] instance.
    ///
    /// # Errors
    /// - Returns an error if the schema projection fails
    pub fn try_new(
        projection: Option<&Vec<usize>>,
        schema: &SchemaRef,
        pool: Arc<ClickHouseConnectionPool>,
        sql: String,
    ) -> Result<Self> {
        let projected_schema = project_schema_safe(schema, projection)?;
        Ok(Self {
            projected_schema: Arc::clone(&projected_schema),
            pool,
            sql,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            coerce_schema: false,
        })
    }

    /// Whether the `RecordBatchStream` returned when calling `ExecutionPlan::execute` should
    /// attempt schema coercion on each `RecordBatch`.
    #[must_use]
    pub fn with_coercion(mut self, coerce: bool) -> Self {
        self.coerce_schema = coerce;
        self
    }

    /// Returns the SQL query string used by this execution plan.
    ///
    /// # Errors
    /// - Currently does not return an error. Kept here to allow for validation in the future.
    pub fn sql(&self) -> Result<String> { Ok(self.sql.clone()) }
}

impl fmt::Debug for ClickHouseSqlExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "ClickHouseSqlExec sql={sql}")
    }
}

impl DisplayAs for ClickHouseSqlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "ClickHouseSqlExec sql={sql}")
    }
}

impl ExecutionPlan for ClickHouseSqlExec {
    fn name(&self) -> &'static str { "SqlExec" }

    fn as_any(&self) -> &dyn Any { self }

    fn schema(&self) -> SchemaRef { Arc::clone(&self.projected_schema) }

    fn properties(&self) -> &PlanProperties { &self.properties }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let sql = self.sql()?;
        Ok(execute_sql_query(sql, Arc::clone(&self.pool), self.schema(), self.coerce_schema))
    }
}

#[cfg(feature = "federation")]
pub mod federation {

    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::catalog::TableProvider;
    use datafusion::common::exec_err;
    use datafusion::error::Result;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use datafusion::sql::TableReference;
    use datafusion::sql::unparser::dialect::Dialect;
    use datafusion_federation::sql::{
        RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
    };
    use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};

    use super::{JoinPushDown, SqlTable, execute_sql_query};

    impl SqlTable {
        /// Create a federated table source for this table provider.
        fn create_federated_table_source(self: Arc<Self>) -> Arc<dyn FederatedTableSource> {
            let table_ref = RemoteTableRef::from(self.table_reference.clone());
            let schema = self.schema();
            let fed_provider = Arc::new(SQLFederationProvider::new(self));
            Arc::new(SQLTableSource::new_with_schema(fed_provider, table_ref, schema))
        }

        /// Create a federated table provider wrapping this table provider.
        ///
        /// # Errors
        /// - Error if `create_federated_table_source` fails.
        pub fn create_federated_table_provider(
            self: Arc<Self>,
        ) -> Result<FederatedTableProviderAdaptor> {
            let table_source = Self::create_federated_table_source(Arc::clone(&self));
            Ok(FederatedTableProviderAdaptor::new_with_provider(table_source, self))
        }
    }

    #[async_trait]
    impl SQLExecutor for SqlTable {
        fn name(&self) -> &str { &self.name }

        fn compute_context(&self) -> Option<String> {
            match self.pool.join_push_down() {
                JoinPushDown::AllowedFor(context) => Some(context),
                // Don't return None here - it will cause incorrect federation with other providers
                // of the same name that also have a compute_context of None.
                // Instead return a random string that will never match any other
                // provider's context.
                JoinPushDown::Disallow => Some(format!("{}", self.unique_id())),
            }
        }

        fn dialect(&self) -> Arc<dyn Dialect> { self.arc_dialect() }

        fn execute(&self, sql: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
            let sql = sql.to_string();
            let pool = Arc::clone(&self.pool);
            let coerce_schema = self.coerce_schema;
            Ok(execute_sql_query(sql, pool, schema, coerce_schema))
        }

        async fn table_names(&self) -> Result<Vec<String>> {
            let Some(schema) = self.table_reference.schema() else {
                return exec_err!("Cannot fetch tables without a schema");
            };
            self.pool.connect().await?.tables(schema).await
        }

        async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
            let table_ref = self
                .table_reference
                .schema()
                .as_ref()
                .map(|s| TableReference::partial(*s, table_name))
                .unwrap_or(TableReference::from(table_name));
            self.pool.connect().await?.get_schema(&table_ref).await
        }
    }
}
