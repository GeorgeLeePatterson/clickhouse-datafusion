//! TODO: Docs - This module is EXTREMELY important. To fully support `ClickHouse` UDFs, the
//! [`ClickHouseQueryPlanner`] MUST be used since it provides the [`ClickHouseExtensionPlanner`].
//!
//! Additionally note how [`ClickHouseQueryPlanner`] provides `ClickHouseQueryPlanner::with_planner`
//! to allow stacking planners, ensuring the `ClickHouseQueryPlanner` is on top.
//!
//! Equally as important is `ClickHouseSessionContext`. `DataFusion` doesn't support providing a
//! custom `SessionContextProvider` (impl `ContextProvider`). Currently this is the only way to
//! prevent the "optimization" away of UDFs that are meant to be pushed down to `ClickHouse`.
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::cte_worktable::CteWorkTable;
use datafusion::common::file_options::file_type::FileType;
use datafusion::common::plan_datafusion_err;
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::format_as_file_type;
use datafusion::datasource::provider_as_source;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::planner::{ExprPlanner, TypePlanner};
use datafusion::logical_expr::var_provider::is_system_variables;
use datafusion::logical_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF};
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::{DataFrame, Expr, SQLOptions, SessionContext};
use datafusion::sql::parser::Statement;
use datafusion::sql::planner::{ContextProvider, ParserOptions, SqlToRel};
use datafusion::sql::{ResolvedTableReference, TableReference};
use datafusion::variable::VarType;

use crate::udfs::analyzer::ClickHouseFunctionPushdown;
use crate::udfs::placeholder::PlaceholderUDF;
use crate::udfs::planner::ClickHouseExtensionPlanner;
use crate::udfs::pushdown::{CLICKHOUSE_UDF_ALIASES, clickhouse_udf_pushdown_udf};

// TODO: Remove - docs
// Convenience method for preparing a session context both with federation if the feature is enabled
// as well as UDF pushdown support. It is called in `ClickHouseSessionContext::new` or
// `ClickHouseSessionContext::from` as well.
pub fn prepare_session_context(
    ctx: SessionContext,
    extension_planners: Option<Vec<Arc<dyn ExtensionPlanner + Send + Sync>>>,
) -> SessionContext {
    #[cfg(feature = "federation")]
    use crate::federation::FederatedContext as _;

    // If federation is enabled, federate the context first. The planners will be overridden to
    // still include the FederatedQueryPlanner, this just saves a step with the optimizer
    #[cfg(feature = "federation")]
    let ctx = ctx.federate();
    // Pull out state
    let state = ctx.state();
    // Pushdown analyzer rule
    let state_builder = if state
        .analyzer()
        .rules
        .iter()
        .any(|rule| rule.name() == "clickhouse_pushdown_analyzer")
    {
        ctx.into_state_builder()
    } else {
        let mut analyzer_rules = state.analyzer().rules.clone();
        analyzer_rules
            .push(Arc::new(ClickHouseFunctionPushdown) as Arc<dyn AnalyzerRule + Send + Sync>);
        ctx.into_state_builder().with_analyzer_rules(analyzer_rules)
    };
    // Finally, build the context again passing the ClickHouseQueryPlanner
    let ctx = SessionContext::new_with_state(
        state_builder
            .with_query_planner(Arc::new(ClickHouseQueryPlanner::new_with_planners(
                extension_planners.unwrap_or_default(),
            )))
            .build(),
    );
    ctx.register_udf(clickhouse_udf_pushdown_udf());
    // TODO: Remove
    // ctx.register_udf(clickhouse_apply_udf());
    ctx
}

// TODO: Docs - LOTS OF DOCS NEEDED HERE!!!
//
// Create a custom QueryPlanner to include ClickHouseExtensionPlanner
#[derive(Clone)]
pub struct ClickHouseQueryPlanner {
    planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl std::fmt::Debug for ClickHouseQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseQueryPlanner").finish()
    }
}

impl Default for ClickHouseQueryPlanner {
    fn default() -> Self { Self::new() }
}

impl ClickHouseQueryPlanner {
    // TODO: Docs
    pub fn new() -> Self {
        let planners = vec![
            #[cfg(feature = "federation")]
            Arc::new(datafusion_federation::FederatedPlanner::new()),
            Arc::new(ClickHouseExtensionPlanner {}) as Arc<dyn ExtensionPlanner + Send + Sync>,
        ];
        ClickHouseQueryPlanner { planners }
    }

    // TODO: Docs
    pub fn new_with_planners(planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>) -> Self {
        let mut this = Self::new();
        this.planners.extend(planners);
        this
    }

    // TODO: Docs
    #[must_use]
    pub fn with_planner(mut self, planner: Arc<dyn ExtensionPlanner + Send + Sync>) -> Self {
        self.planners.push(planner);
        self
    }
}

#[async_trait]
impl QueryPlanner for ClickHouseQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Delegate to DefaultQueryPlanner with custom extension planners
        let planner = DefaultPhysicalPlanner::with_extension_planners(self.planners.clone());
        planner.create_physical_plan(logical_plan, session_state).await
    }
}

/// Wrapper for [`SessionContext`] which allows running arbitrary `ClickHouse` functions.
#[derive(Clone)]
pub struct ClickHouseSessionContext {
    inner:        SessionContext,
    expr_planner: Option<Arc<dyn ExprPlanner>>,
}

impl ClickHouseSessionContext {
    // TODO: Docs
    pub fn new(
        ctx: SessionContext,
        extension_planners: Option<Vec<Arc<dyn ExtensionPlanner + Send + Sync>>>,
    ) -> Self {
        Self { inner: prepare_session_context(ctx, extension_planners), expr_planner: None }
    }

    #[must_use]
    pub fn with_expr_planner(mut self, expr_planner: Arc<dyn ExprPlanner>) -> Self {
        self.expr_planner = Some(expr_planner);
        self
    }

    // TODO: Docs - especially mention that using the provided context WILL NOT WORK with pushdown
    pub fn into_session_context(self) -> SessionContext { self.inner }

    // TODO: Docs - mention and link the `sql` method on SessionContext
    /// # Errors
    ///
    /// Returns an error if the SQL query is invalid or if the query execution fails.
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        self.sql_with_options(sql, SQLOptions::new()).await
    }

    // TODO: Docs - mention and link the `sql_with_options` method on SessionContext
    /// # Errors
    ///
    /// Returns an error if the SQL query is invalid or if the query execution fails.
    pub async fn sql_with_options(&self, sql: &str, options: SQLOptions) -> Result<DataFrame> {
        let state = self.inner.state();
        let statement = state.sql_to_statement(sql, "ClickHouse")?;
        let plan = self.statement_to_plan(&state, statement).await?;
        options.verify_plan(&plan)?;
        self.execute_logical_plan(plan).await
    }

    // TODO: Docs - mention and link the `statement_to_plan` method on SessionContext
    /// # Errors
    /// - Returns an error if the SQL query is invalid or if the query execution fails.
    pub async fn statement_to_plan(
        &self,
        state: &SessionState,
        statement: Statement,
    ) -> Result<LogicalPlan> {
        let references = state.resolve_table_references(&statement)?;

        // TODO: Remove
        // let lambda_planner = Arc::new(ClickHouseLambdaPlanner);
        let provider =
            ClickHouseContextProvider::new(state.clone(), HashMap::with_capacity(references.len()));
        // TODO: Remove
        // .with_expr_planner(lambda_planner);

        let mut provider = if let Some(planner) = self.expr_planner.as_ref() {
            provider.with_expr_planner(Arc::clone(planner))
        } else {
            provider
        };

        for reference in references {
            // DEV (DataFusion PR): Post PR that makes `resolve_table_ref` pub and access to tables
            // entries let resolved = state.resolve_table_ref(reference);
            let catalog = &state.config_options().catalog;
            let resolved = reference.resolve(&catalog.default_catalog, &catalog.default_schema);
            if let Entry::Vacant(v) = provider.tables.entry(resolved) {
                let resolved = v.key();
                if let Ok(schema) = provider.state.schema_for_ref(resolved.clone()) {
                    if let Some(table) = schema.table(&resolved.table).await? {
                        let _ = v.insert(provider_as_source(table));
                    }
                }
            }
        }

        SqlToRel::new_with_options(&provider, Self::get_parser_options(&self.state()))
            .statement_to_plan(statement)
    }

    fn get_parser_options(state: &SessionState) -> ParserOptions {
        let sql_parser_options = &state.config().options().sql_parser;

        ParserOptions {
            parse_float_as_decimal:             sql_parser_options.parse_float_as_decimal,
            enable_ident_normalization:         sql_parser_options.enable_ident_normalization,
            enable_options_value_normalization: sql_parser_options
                .enable_options_value_normalization,
            support_varchar_with_length:        sql_parser_options.support_varchar_with_length,
            map_varchar_to_utf8view:            sql_parser_options.map_varchar_to_utf8view,
            // TODO: Remove
            // map_string_types_to_utf8view:       sql_parser_options.map_string_types_to_utf8view,
            collect_spans:                      sql_parser_options.collect_spans,
        }
    }
}

impl From<SessionContext> for ClickHouseSessionContext {
    fn from(inner: SessionContext) -> Self { Self::new(inner, None) }
}

impl From<&SessionContext> for ClickHouseSessionContext {
    fn from(inner: &SessionContext) -> Self { Self::new(inner.clone(), None) }
}

impl std::ops::Deref for ClickHouseSessionContext {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target { &self.inner }
}

// TODO: Docs - a LOT more docs here, this is a pretty big needed step
//
/// Custom [`ContextProvider`].
/// Required since `DataFusion` will throw an error on unrecognized functions and the goal is to
/// preserve the Expr structure.
pub struct ClickHouseContextProvider {
    state:         SessionState,
    tables:        HashMap<ResolvedTableReference, Arc<dyn TableSource>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
    type_planner:  Option<Arc<dyn TypePlanner>>,
}

impl ClickHouseContextProvider {
    pub fn new(
        state: SessionState,
        tables: HashMap<ResolvedTableReference, Arc<dyn TableSource>>,
    ) -> Self {
        Self { state, tables, expr_planners: vec![], type_planner: None }
    }

    #[must_use]
    pub fn with_expr_planner(mut self, planner: Arc<dyn ExprPlanner>) -> Self {
        self.expr_planners.push(planner);
        self
    }

    #[must_use]
    pub fn with_type_planner(mut self, type_planner: Arc<dyn TypePlanner>) -> Self {
        self.type_planner = Some(type_planner);
        self
    }

    // NOTE: This method normally resides on `SessionState` but since it's `pub(crate)` it must
    // be reproduced and this is its temporary home until the method is made pub.
    pub fn resolve_table_ref(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> ResolvedTableReference {
        let catalog = &self.state.config_options().catalog;
        table_ref.into().resolve(&catalog.default_catalog, &catalog.default_schema)
    }
}

impl ContextProvider for ClickHouseContextProvider {
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        // Early exit for clickhouse pushdown
        if CLICKHOUSE_UDF_ALIASES.contains(&name) {
            return Some(Arc::new(clickhouse_udf_pushdown_udf()));
        }

        // TODO: Remove
        // // Early exit for clickhouse apply
        // if CLICKHOUSE_APPLY_ALIASES.contains(&name) {
        //     return Some(Arc::new(clickhouse_apply_udf()));
        // }

        // Delegate to inner provider for other UDFs
        if let Some(func) = self.state.scalar_functions().get(name) {
            return Some(Arc::clone(func));
        }

        // Check if this is a known aggregate or window function
        // These should NOT be wrapped as placeholder UDFs
        if self.state.aggregate_functions().contains_key(name) {
            return None;
        }
        if self.state.window_functions().contains_key(name) {
            return None;
        }

        // Allow inner functions to parse as placeholder ScalarUDFs
        Some(Arc::new(ScalarUDF::new_from_impl(PlaceholderUDF::new(name))))
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] { &self.expr_planners }

    fn get_type_planner(&self) -> Option<Arc<dyn TypePlanner>> {
        if let Some(type_planner) = &self.type_planner {
            Some(Arc::clone(type_planner))
        } else {
            None
        }
    }

    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let name = self.resolve_table_ref(name);
        self.tables
            .get(&name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("table '{name}' not found"))
    }

    fn get_table_function_source(
        &self,
        name: &str,
        args: Vec<Expr>,
    ) -> Result<Arc<dyn TableSource>> {
        let tbl_func = self
            .state
            .table_functions()
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("table function '{name}' not found"))?;
        let provider = tbl_func.create_table_provider(&args)?;

        Ok(provider_as_source(provider))
    }

    /// Create a new CTE work table for a recursive CTE logical plan
    /// This table will be used in conjunction with a Worktable physical plan
    /// to read and write each iteration of a recursive CTE
    fn create_cte_work_table(&self, name: &str, schema: SchemaRef) -> Result<Arc<dyn TableSource>> {
        let table = Arc::new(CteWorkTable::new(name, schema));
        Ok(provider_as_source(table))
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        if variable_names.is_empty() {
            return None;
        }

        let provider_type = if is_system_variables(variable_names) {
            VarType::System
        } else {
            VarType::UserDefined
        };

        self.state
            .execution_props()
            .var_providers
            .as_ref()
            .and_then(|provider| provider.get(&provider_type)?.get_type(variable_names))
    }

    fn options(&self) -> &ConfigOptions { self.state.config_options() }

    // TODO: Does this behave well with the logic above in `get_function_meta`?
    fn udf_names(&self) -> Vec<String> { self.state.scalar_functions().keys().cloned().collect() }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> { self.state.window_functions().keys().cloned().collect() }

    fn get_file_type(&self, ext: &str) -> Result<Arc<dyn FileType>> {
        self.state
            .get_file_format_factory(ext)
            .ok_or(plan_datafusion_err!("There is no registered file format with ext {ext}"))
            .map(|file_type| format_as_file_type(file_type))
    }
}
