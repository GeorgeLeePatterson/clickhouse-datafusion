//! # `ClickHouse` SQL `DataFusion` `TableProvider`
//!
//! This module implements a SQL [`TableProvider`] for `DataFusion`.
//!
//! This is used as a fallback if the `datafusion-federation` optimizer is not enabled.
#[cfg(feature = "federation")]
pub mod federation;

use std::any::Any;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::sql::TableReference;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
use futures_util::TryStreamExt;

use crate::connection::ClickHouseConnectionPool;
use crate::dialect::ClickHouseDialect;

/// Controls whether join pushdown is allowed, and under what conditions
///
/// Mainly here to provider future interop with [`datafusion-table-providers`]
#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub table_reference: TableReference,
    pub(crate) pool:     ClickHouseConnectionPool,
    name:                String,
    schema:              SchemaRef,
    dialect:             Option<Arc<ClickHouseDialect>>,
    exprs:               Option<Vec<Expr>>,
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
            .finish()
    }
}

impl SqlTable {
    /// Create a new [`SqlTable`] from a name and a [`ClickHouseConnectionPool`].
    ///
    /// # Errors
    /// - Returns an error if the table does not exist.
    pub async fn new(
        name: &str,
        pool: ClickHouseConnectionPool,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self> {
        let table_reference = table_reference.into();
        let schema = pool.connect().await?.get_schema(&table_reference).await?;
        Ok(Self::new_with_schema(name, pool, schema, table_reference))
    }

    pub fn new_with_schema(
        name: &str,
        pool: ClickHouseConnectionPool,
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
        }
    }

    #[must_use]
    pub fn with_exprs(mut self, exprs: Vec<Expr>) -> Self {
        self.exprs = Some(exprs);
        self
    }

    /// # Errors
    /// - Returns an error if the logical plan creation fails
    pub fn scan_to_sql(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<String> {
        let logical_plan = self.create_logical_plan(projection, filters, limit)?;
        let sql = Unparser::new(self.dialect()).plan_to_sql(&logical_plan)?.to_string();
        Ok(sql)
    }

    fn create_logical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<LogicalPlan> {
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ClickHouseSqlExec::try_new(
            projection,
            &self.schema(),
            self.pool.clone(),
            sql,
        )?))
    }

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

#[async_trait]
impl TableProvider for SqlTable {
    fn as_any(&self) -> &dyn Any { self }

    fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }

    fn table_type(&self) -> TableType { TableType::Base }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let sql = self.scan_to_sql(projection, filters, limit)?;
        return self.create_physical_plan(projection, sql);
    }
}

impl Display for SqlTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ClickHouseSqlTable {}", self.name)
    }
}

static ONE_COLUMN_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(vec![Field::new("1", DataType::Int64, true)])));

/// Create a new [`SqlTable`] from a name and a [`ClickHouseConnectionPool`].
///
/// # Errors
/// - Returns an error if the projection fails
pub fn project_schema_safe(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<SchemaRef> {
    let schema = match projection {
        Some(columns) => {
            if columns.is_empty() {
                // If the projection is Some([]) then it gets unparsed as `SELECT 1`, so return a
                // schema with a single Int64 column.
                //
                // See: <https://github.com/apache/datafusion/blob/83ce79c39412a4f150167d00e40ea05948c4870f/datafusion/sql/src/unparser/plan.rs#L998>
                ONE_COLUMN_SCHEMA.clone()
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
    pool:             ClickHouseConnectionPool,
    sql:              String,
    properties:       PlanProperties,
}

impl ClickHouseSqlExec {
    /// Create a new [`ClickHouseSqlExec`] instance.
    ///
    /// # Errors
    /// - Returns an error if the schema projection fails
    pub fn try_new(
        projection: Option<&Vec<usize>>,
        schema: &SchemaRef,
        pool: ClickHouseConnectionPool,
        sql: String,
    ) -> DataFusionResult<Self> {
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
        })
    }

    #[must_use]
    pub fn clone_pool(&self) -> ClickHouseConnectionPool { self.pool.clone() }

    /// Returns the SQL query string used by this execution plan.
    ///
    /// # Errors
    /// - Returns an error if the SQL query string cannot be cloned.
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let sql = self.sql().map_err(to_execution_error)?;
        tracing::debug!("ClickHouseSqlExec sql: {sql}");

        let schema = self.schema();
        let pool = self.pool.clone();
        let projected_schema = Arc::clone(&self.projected_schema);
        let fut = async move {
            pool.connect()
                .await
                .map_err(to_execution_error)?
                .query_arrow(&sql, &[], Some(projected_schema))
                .await
        };

        let stream = futures_util::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn to_execution_error(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}
