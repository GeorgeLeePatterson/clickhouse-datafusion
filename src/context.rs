//! TODO: Docs - This module is EXTREMELY important. To fully support `ClickHouse` UDFs, the
//! [`ClickHouseQueryPlanner`] MUST be used since it provides the [`ClickHouseExtensionPlanner`].
//!
//! Additionally note how [`ClickHouseQueryPlanner`] provides `ClickHouseQueryPlanner::with_planner`
//! to allow stacking planners, ensuring the `ClickHouseQueryPlanner` is on top.
//!
//! Equally as important is `ClickHouseSessionContext`. `DataFusion` doesn't support providing a
//! custom `SessionContextProvider` (impl `ContextProvider`). Currently this is the only way to
//! prevent the "optimization" away of UDFs that are meant to be pushed down to `ClickHouse`.
pub mod plan_node;
pub mod planner;

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
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::{DataFrame, Expr, SQLOptions, SessionContext};
use datafusion::sql::parser::Statement;
use datafusion::sql::planner::{ContextProvider, ParserOptions, SqlToRel};
use datafusion::sql::{ResolvedTableReference, TableReference};
use datafusion::variable::VarType;

use self::planner::ClickHouseExtensionPlanner;
use crate::analyzer::function_pushdown::ClickHouseFunctionPushdown;
use crate::udfs::apply::{CLICKHOUSE_APPLY_ALIASES, clickhouse_apply_udf};
use crate::udfs::clickhouse::{CLICKHOUSE_UDF_ALIASES, clickhouse_udf};
use crate::udfs::placeholder::PlaceholderUDF;

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
        .any(|rule| rule.name() == ClickHouseFunctionPushdown.name())
    {
        ctx.into_state_builder()
    } else {
        let analyzer_rules = configure_analyzer_rules(&state);
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
    ctx.register_udf(clickhouse_udf());
    ctx.register_udf(clickhouse_apply_udf());
    ctx
}

pub fn configure_analyzer_rules(state: &SessionState) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    // Pull out analyzer rules
    let mut analyzer_rules = state.analyzer().rules.clone();

    // Insert the ClickHouseFunctionPushdown before type coercion.
    // This datafusion to optimize for the data types expected.
    let type_coercion = TypeCoercion::default();
    let pos = analyzer_rules.iter().position(|x| x.name() == type_coercion.name()).unwrap_or(0);

    let pushdown_rule = Arc::new(ClickHouseFunctionPushdown);
    analyzer_rules.insert(pos, pushdown_rule);
    analyzer_rules
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

        let provider =
            ClickHouseContextProvider::new(state.clone(), HashMap::with_capacity(references.len()));

        let mut provider = if let Some(planner) = self.expr_planner.as_ref() {
            provider.with_expr_planner(Arc::clone(planner))
        } else {
            provider
        };

        for reference in references {
            // DEV (DataFusion PR): Post PR that makes `resolve_table_ref` pub and access to tables
            // entries: let resolved = state.resolve_table_ref(reference);
            let catalog = &state.config_options().catalog;
            let resolved = reference.resolve(&catalog.default_catalog, &catalog.default_schema);
            if let Entry::Vacant(v) = provider.tables.entry(resolved) {
                let resolved = v.key();
                if let Ok(schema) = provider.state.schema_for_ref(resolved.clone())
                    && let Some(table) = schema.table(&resolved.table).await?
                {
                    let _ = v.insert(provider_as_source(table));
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
            map_string_types_to_utf8view:       sql_parser_options.map_string_types_to_utf8view,
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
            return Some(Arc::new(clickhouse_udf()));
        }

        // Early exit for clickhouse apply
        if CLICKHOUSE_APPLY_ALIASES.contains(&name) {
            return Some(Arc::new(clickhouse_apply_udf()));
        }

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

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::planner::{
        ExprPlanner, PlannerResult, RawBinaryExpr, TypePlanner,
    };
    use datafusion::prelude::{SessionContext, lit};
    use datafusion::sql::TableReference;
    use datafusion::sql::sqlparser::ast;

    use super::*;

    // Mock TypePlanner for testing
    #[derive(Debug)]
    struct MockTypePlanner;

    impl TypePlanner for MockTypePlanner {
        fn plan_type(&self, _expr: &ast::DataType) -> Result<Option<DataType>> {
            Ok(Some(DataType::Utf8))
        }
    }

    // Mock ExprPlanner for testing
    #[derive(Debug)]
    struct MockExprPlanner;

    impl ExprPlanner for MockExprPlanner {
        fn plan_binary_op(
            &self,
            expr: RawBinaryExpr,
            _schema: &DFSchema,
        ) -> Result<PlannerResult<RawBinaryExpr>> {
            Ok(PlannerResult::Original(expr))
        }
    }

    fn create_test_context_provider() -> ClickHouseContextProvider {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let tables = HashMap::new();
        ClickHouseContextProvider::new(state, tables)
    }

    #[test]
    fn test_with_expr_planner() {
        let mut provider = create_test_context_provider();
        assert!(provider.expr_planners.is_empty());

        let expr_planner = Arc::new(MockExprPlanner) as Arc<dyn ExprPlanner>;
        provider = provider.with_expr_planner(Arc::clone(&expr_planner));

        assert_eq!(provider.expr_planners.len(), 1);
        assert_eq!(provider.get_expr_planners().len(), 1);
    }

    #[test]
    fn test_with_type_planner() {
        let mut provider = create_test_context_provider();
        assert!(provider.type_planner.is_none());

        let type_planner = Arc::new(MockTypePlanner) as Arc<dyn TypePlanner>;
        provider = provider.with_type_planner(Arc::clone(&type_planner));

        assert!(provider.type_planner.is_some());
    }

    #[test]
    fn test_get_type_planner() {
        let provider = create_test_context_provider();
        assert!(provider.get_type_planner().is_none());

        let type_planner = Arc::new(MockTypePlanner) as Arc<dyn TypePlanner>;
        let provider = provider.with_type_planner(Arc::clone(&type_planner));

        assert!(provider.get_type_planner().is_some());
    }

    #[test]
    fn test_get_table_function_source_not_found() {
        let provider = create_test_context_provider();
        let args = vec![lit("test")];

        let result = provider.get_table_function_source("nonexistent_function", args);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_cte_work_table() {
        let provider = create_test_context_provider();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let result = provider.create_cte_work_table("test_cte", Arc::clone(&schema));
        assert!(result.is_ok());

        let table_source = result.unwrap();
        assert_eq!(table_source.schema(), schema);
    }

    #[test]
    fn test_get_variable_type_empty() {
        let provider = create_test_context_provider();
        let result = provider.get_variable_type(&[]);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_variable_type_system_variables() {
        let provider = create_test_context_provider();
        // System variables start with @@
        let result = provider.get_variable_type(&["@@version".to_string()]);
        // Since no variable providers are set up, this should return None
        assert!(result.is_none());
    }

    #[test]
    fn test_get_variable_type_user_defined() {
        let provider = create_test_context_provider();
        // User-defined variables don't start with @@
        let result = provider.get_variable_type(&["user_var".to_string()]);
        // Since no variable providers are set up, this should return None
        assert!(result.is_none());
    }

    #[test]
    fn test_get_file_type_unknown_extension() {
        let provider = create_test_context_provider();
        let result = provider.get_file_type("unknown_ext");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_file_type_known_extension() {
        let provider = create_test_context_provider();
        // CSV should be a known file type in DataFusion
        let result = provider.get_file_type("csv");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_function_meta_clickhouse_udf() {
        let provider = create_test_context_provider();

        // Test clickhouse UDF alias
        let result = provider.get_function_meta("clickhouse");
        assert!(result.is_some());
        let udf = result.unwrap();
        assert_eq!(udf.name(), "clickhouse");
    }

    #[test]
    fn test_get_function_meta_placeholder_udf() {
        let provider = create_test_context_provider();

        // Test unknown function should return placeholder UDF
        let result = provider.get_function_meta("unknown_function");
        assert!(result.is_some());
        let udf = result.unwrap();
        assert_eq!(udf.name(), "unknown_function");
    }

    #[test]
    fn test_get_function_meta_aggregate_function() {
        let provider = create_test_context_provider();

        // Test known aggregate function should return None (not wrapped as ScalarUDF)
        let result = provider.get_function_meta("sum");
        assert!(result.is_none());
    }

    #[test]
    fn test_get_function_meta_window_function() {
        let provider = create_test_context_provider();

        // Test known window function should return None (not wrapped as ScalarUDF)
        let result = provider.get_function_meta("row_number");
        assert!(result.is_none());
    }

    #[test]
    fn test_get_table_source_not_found() {
        let provider = create_test_context_provider();
        let table_ref = TableReference::bare("nonexistent_table");

        let result = provider.get_table_source(table_ref);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_table_ref() {
        let provider = create_test_context_provider();

        // Test bare table reference
        let table_ref = TableReference::bare("test_table");
        let resolved = provider.resolve_table_ref(table_ref);
        assert_eq!(resolved.table.as_ref(), "test_table");

        // Test partial table reference (schema.table)
        let table_ref = TableReference::partial("test_schema", "test_table");
        let resolved = provider.resolve_table_ref(table_ref);
        assert_eq!(resolved.schema.as_ref(), "test_schema");
        assert_eq!(resolved.table.as_ref(), "test_table");

        // Test full table reference (catalog.schema.table)
        let table_ref = TableReference::full("test_catalog", "test_schema", "test_table");
        let resolved = provider.resolve_table_ref(table_ref);
        assert_eq!(resolved.catalog.as_ref(), "test_catalog");
        assert_eq!(resolved.schema.as_ref(), "test_schema");
        assert_eq!(resolved.table.as_ref(), "test_table");
    }

    #[test]
    fn test_udf_names() {
        let provider = create_test_context_provider();
        let udf_names = provider.udf_names();
        // Should return the names of registered scalar functions
        // The exact contents depend on DataFusion's built-in functions
        assert!(!udf_names.is_empty());
    }

    #[test]
    fn test_udaf_names() {
        let provider = create_test_context_provider();
        let udaf_names = provider.udaf_names();
        // Should return the names of registered aggregate functions
        assert!(!udaf_names.is_empty());
        assert!(udaf_names.contains(&"sum".to_string()));
        assert!(udaf_names.contains(&"count".to_string()));
    }

    #[test]
    fn test_udwf_names() {
        let provider = create_test_context_provider();
        let udwf_names = provider.udwf_names();
        // Should return the names of registered window functions
        assert!(!udwf_names.is_empty());
        assert!(udwf_names.contains(&"row_number".to_string()));
    }

    #[test]
    fn test_options() {
        let provider = create_test_context_provider();
        let options = provider.options();
        // Should return ConfigOptions from the session state
        assert!(!options.catalog.default_catalog.is_empty());
        assert!(!options.catalog.default_schema.is_empty());
    }

    #[test]
    fn test_get_aggregate_meta() {
        let provider = create_test_context_provider();

        // Test known aggregate function
        let result = provider.get_aggregate_meta("sum");
        assert!(result.is_some());
        let udf = result.unwrap();
        assert_eq!(udf.name().to_lowercase().as_str(), "sum");

        // Test unknown aggregate function
        let result = provider.get_aggregate_meta("unknown_aggregate");
        assert!(result.is_none());
    }

    #[test]
    fn test_get_window_meta() {
        let provider = create_test_context_provider();

        // Test known window function
        let result = provider.get_window_meta("row_number");
        assert!(result.is_some());
        let udf = result.unwrap();
        assert_eq!(udf.name().to_lowercase().as_str(), "row_number");

        // Test unknown window function
        let result = provider.get_window_meta("unknown_window");
        assert!(result.is_none());
    }
}
