use std::collections::HashSet;
use std::sync::Arc;

use clickhouse_arrow::rustc_hash::FxHashMap;
use datafusion::arrow::datatypes::Field;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{LogicalPlan, Projection, SubqueryAlias};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;

use super::source_context::ResolvedSource;
use super::source_visitor::{ColumnId, SourceLineageVistor};
use super::utils::{
    extract_function_and_return_type, is_clickhouse_function, use_clickhouse_function_context,
};
use crate::utils::analyze::push_exprs_below_subquery;

/// State for tracking `ClickHouse` functions during pushdown analysis
#[derive(Default, Debug, Clone)]
struct PushdownState {
    /// `ClickHouse` functions being pushed down, organized by resolved sources
    functions:        FxHashMap<ResolvedSource, Vec<Expr>>,
    /// Tracks the resolved sources of all functions collected, for non-branching plans
    function_sources: ResolvedSource,
    /// Tracks the sources of the schemas of the plans visited so far
    plan_sources:     ResolvedSource,
}

impl PushdownState {
    fn take_functions(&mut self) -> Vec<Expr> {
        self.functions.values_mut().flatten().map(std::mem::take).collect::<Vec<_>>()
    }

    fn has_functions(&self) -> bool { self.functions.values().any(|f| !f.is_empty()) }

    /// Using the provided `LogicalPlan`s schema, pull out any functions that can be pushed safely.
    fn take_relevant_functions(
        &mut self,
        schema: &DFSchemaRef,
        visitor: &SourceLineageVistor,
    ) -> FxHashMap<ResolvedSource, Vec<Expr>> {
        if !self.has_functions() {
            return FxHashMap::default();
        }
        let sources = visitor.resolve_schema(schema.as_ref());
        let column_ids = schema
            .columns()
            .into_iter()
            .flat_map(|col| visitor.collect_column_ids(&col))
            .collect::<HashSet<_>>();
        let extracted = self
            .functions
            .iter_mut()
            .filter(|(r, _)| r.resolves_intersects(&sources))
            .map(|(resolved, funcs)| {
                let extracted = funcs
                    .extract_if(.., |f| {
                        f.column_refs()
                            .into_iter()
                            .flat_map(|col| visitor.collect_column_ids(col))
                            .collect::<HashSet<_>>()
                            .is_subset(&column_ids)
                    })
                    .collect::<Vec<_>>();
                (resolved.clone(), extracted)
            })
            .collect::<FxHashMap<_, _>>();
        // Clean up any empty function lists
        self.functions.retain(|_, funcs| !funcs.is_empty());
        extracted
    }
}

/// A `DataFusion` `AnalyzerRule` that identifies largest subtree of a plan to wrap with an
/// extension node, otherwise "pushes down" `ClickHouse` functions when required
#[derive(Debug, Clone, Copy)]
pub struct ClickHouseFunctionPushdown;

impl AnalyzerRule for ClickHouseFunctionPushdown {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        if matches!(plan, LogicalPlan::Ddl(_) | LogicalPlan::DescribeTable(_)) {
            return Ok(plan);
        }

        // Create source lineage visitor and build column -> source lineage for the plan
        #[cfg_attr(feature = "test-utils", expect(unused))]
        let mut lineage_visitor = SourceLineageVistor::new();

        #[cfg(feature = "test-utils")]
        let mut lineage_visitor = lineage_visitor
            .with_source_grouping(HashSet::from(["table1".to_string(), "table2".to_string()]));

        let _ = plan.visit(&mut lineage_visitor)?;

        // Nothing to transform
        if lineage_visitor.clickhouse_function_count == 0 {
            return Ok(plan);
        }

        // Initialize state, setting the plan_sources
        let mut state = PushdownState::default();

        let (new_plan, _) = analyze_and_transform_plan(plan, &mut state, &lineage_visitor)?;
        Ok(new_plan.data)
    }

    fn name(&self) -> &'static str { "clickhouse_function_pushdown" }
}

// TODOS: * Implement error handling for required pushdowns
//        * Check is_volatile and other invariants
//        * Handle Sort in violations checks. Sorting may prevent pushdown in some cases
//        * Refine Subquery in violations checks, might need some tuning
//        * Investigate how to separate mixed functions, ie in aggr_exprs
//        * Initial violations checks allocate, could be improved for cases where no alloc needed
//        * Match more plans to reduce allocations
//
/// Analyze and transform a plan. Functions flow DOWN - recurse if current level cannot handle them.
///
/// The basic flow is:
/// 1. Determine any semantic violations with pushed functions or plan functions
/// 2. Handle some plans specially, ie joins which require function routing
/// 3. Update `PushdownState`'s resolved sources to reflect any functions in plan expressions
/// 4. Check if the resolved sources fully encompass the functions's resolved sources
/// 5. If so, wrap there, highest subtree identified
/// 6. If not, enforce any semantic violations (wrapping prevents this need)
/// 7. Extract any clickhouse functions, replace with alias, and store in state
/// 8. Recurse into inputs
/// 9. Reconstruct plan while unwinding recursion
fn analyze_and_transform_plan(
    plan: LogicalPlan,
    state: &mut PushdownState,
    visitor: &SourceLineageVistor,
) -> Result<(Transformed<LogicalPlan>, Option<ResolvedSource>)> {
    // First determine any semantic violations with pushed functions
    let function_violations = check_state_for_violations(state, &plan, visitor);

    // Then check any plan violations for plan functions
    let plan_violations = check_plan_for_violations(&plan, visitor);

    let plan_sources = visitor.resolve_schema(plan.schema());
    let plan_function_sources = resolve_plan_expr_functions(&plan, visitor);

    // If plan's schema resolves pushed functions, wrap it and return
    if state.has_functions() {
        if state.function_sources.resolves_eq(&plan_sources) {
            let wrapped_plan = wrap_plan_with_functions(plan, state.take_functions(), visitor)?;
            return Ok((Transformed::yes(wrapped_plan), None));
        }

        // All ClickHouse functions need to be resolveable by this point
        if !state.function_sources.resolves_within(&plan_sources) {
            return plan_err!(
                "SQL not supported, could not determine sources of following functions: {:?}",
                state.functions.iter().collect::<Vec<_>>()
            );
        }
    }

    // It may be that the very top level plan is wrapped, need to know if it is the first plan
    let top_level_plan = !state.plan_sources.is_known();
    if top_level_plan {
        state.plan_sources = plan_sources.clone();
    }

    // Wrap higher if function sources resolve into parent plan sources
    if state.plan_sources.resolves_eq(&plan_function_sources) {
        // Top level plan can be wrapped
        if top_level_plan {
            let wrapped_plan = wrap_plan_with_functions(plan, state.take_functions(), visitor)?;
            return Ok((Transformed::yes(wrapped_plan), None));
        }
        // Otherwise send function sources back up
        return Ok((Transformed::no(plan), Some(plan_function_sources)));
    }

    // Merge function sources
    state.function_sources =
        std::mem::take(&mut state.function_sources).merge(plan_function_sources);

    // Finally check if plan can be wrapped itself
    if state.function_sources.resolves_eq(&plan_sources) {
        let wrapped_plan = wrap_plan_with_functions(plan, state.take_functions(), visitor)?;
        return Ok((Transformed::yes(wrapped_plan), None));
    }

    // Either pushdowns or plan functions that need to be pushed down
    if state.function_sources.is_known() {
        // Ensure no semantic violations in the result of pushdown
        semantic_err(
            plan.display(),
            "SQL unsupported, pushed functions violate sql semantics in current plan.",
            &function_violations,
        )?;
        // Ensure no plan violations in the result of pushdown
        semantic_err(
            plan.display(),
            "SQL unsupported, plan violates sql semantics in plan's expressions.",
            &plan_violations,
        )?;
    }

    // Pull out plan sources for comparison
    let parent_sources = std::mem::take(&mut state.plan_sources);

    // SubqueryAlias handling for pushdowns
    if state.has_functions()
        && let LogicalPlan::SubqueryAlias(alias) = &plan
    {
        state.functions = std::mem::take(&mut state.functions)
            .into_iter()
            .map(|(resolv, funcs)| (resolv, push_exprs_below_subquery(funcs, alias)))
            .collect();
    }

    // Update w/ schema and expression changes
    let aliased_exprs = collect_and_transform_exprs(plan.expressions(), visitor, state)?;

    // Track if this plan will be transformed in any way
    let mut was_transformed = state.has_functions();

    // Recurse deeper
    let inputs_transformed = plan
        .inputs()
        .into_iter()
        .cloned()
        .map(|input| {
            let extracted = state.take_relevant_functions(input.schema(), visitor);
            let mut input_state = PushdownState {
                function_sources: extracted
                    .keys()
                    .cloned()
                    .reduce(ResolvedSource::merge)
                    .unwrap_or_default(),
                functions:        extracted,
                plan_sources:     plan_sources.clone(),
            };
            analyze_and_transform_plan(input, &mut input_state, visitor)
        })
        .collect::<Result<Vec<_>>>()?;

    // All ClickHouse functions need to be resolved by this point
    if state.has_functions() {
        return plan_err!(
            "SQL not supported, could not determine sources of following functions: {:?}",
            state.functions.iter().collect::<Vec<_>>()
        );
    }

    // If the inputs determined wrap higher up, merge their functions sources
    let input_resolution = inputs_transformed
        .iter()
        .inspect(|(i, _)| was_transformed |= i.transformed)
        .filter_map(|(_, func_sources)| func_sources.clone())
        .reduce(ResolvedSource::merge)
        .unwrap_or_default();

    let new_inputs = inputs_transformed.into_iter().map(|(i, _)| i.data).collect::<Vec<_>>();
    let new_plan = plan.with_new_exprs(aliased_exprs, new_inputs)?;

    let new_plan =
        // If this node's parent sources resolves, defer to higher plan
        if !top_level_plan && parent_sources.resolves_eq(&input_resolution) {
            return Ok((Transformed::no(plan), Some(input_resolution)));

        // If this plan's schema resolves the function sources, wrap here
        } else if plan_sources.resolves_eq(&input_resolution) {
            let wrapped = wrap_plan_with_functions(new_plan, state.take_functions(), visitor)?;
            Transformed::yes(wrapped)

        // Otherwise, returned plan
        } else if was_transformed {
            Transformed::yes(new_plan)
        } else {
            Transformed::no(new_plan)
        };

    Ok((new_plan, None))
}

fn resolve_plan_expr_functions(
    input: &LogicalPlan,
    visitor: &SourceLineageVistor,
) -> ResolvedSource {
    let mut exprs_resolved = ResolvedSource::default();
    let _ = input
        .apply_expressions(|expr| {
            use_clickhouse_function_context(expr, |e| {
                exprs_resolved = std::mem::take(&mut exprs_resolved).merge(visitor.resolve_expr(e));
                Ok(TreeNodeRecursion::Stop)
            })
            .unwrap();
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
    exprs_resolved
}

/// Add functions to a plan by wrapping with Extension node that persists through optimization
fn wrap_plan_with_functions(
    plan: LogicalPlan,
    functions: Vec<Expr>,
    visitor: &SourceLineageVistor,
) -> Result<LogicalPlan> {
    #[cfg(feature = "federation")]
    #[expect(clippy::unnecessary_wraps)]
    fn return_wrapped_plan(plan: LogicalPlan) -> Result<LogicalPlan> { Ok(plan) }

    #[cfg(not(feature = "federation"))]
    fn return_wrapped_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
        use datafusion::logical_expr::Extension;

        use crate::context::plan_node::ClickHouseFunctionNode;

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ClickHouseFunctionNode::try_new(plan)?),
        }))
    }

    // Collect pushed functions
    let (func_fields, func_cols) = functions_to_field_and_cols(functions, visitor)?;

    // Remove catalog from table scan
    let plan = plan.transform_up_with_subqueries(strip_table_scan_catalog).unwrap().data;
    // Recompute schema
    let plan = plan.recompute_schema()?;

    match plan {
        LogicalPlan::SubqueryAlias(alias) => {
            // Ensure the alias isn't carried down.
            let func_cols = push_exprs_below_subquery(func_cols, &alias);

            let input = Arc::unwrap_or_clone(alias.input);
            let new_input = wrap_plan_in_projection(input, func_fields, func_cols)?.into();
            let new_alias = SubqueryAlias::try_new(new_input, alias.alias)?;
            return_wrapped_plan(LogicalPlan::SubqueryAlias(new_alias))
        }
        _ => return_wrapped_plan(wrap_plan_in_projection(plan, func_fields, func_cols)?),
    }
}

type QualifiedField = (Option<TableReference>, Arc<Field>);

/// Extract inner functions from `ClickHouse` `UDF` wrappers
fn functions_to_field_and_cols(
    functions: Vec<Expr>,
    visitor: &SourceLineageVistor,
) -> Result<(Vec<QualifiedField>, Vec<Expr>)> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for function in functions {
        let is_nullable = visitor.resolve_nullable(&function);
        let alias = function.schema_name().to_string();
        let (inner_function, data_type) = extract_function_and_return_type(function)?;
        fields.push((None, Arc::new(Field::new(&alias, data_type, is_nullable))));
        columns.push(inner_function.alias(alias));
    }
    Ok((fields, columns))
}

fn wrap_plan_in_projection(
    plan: LogicalPlan,
    func_fields: Vec<QualifiedField>,
    func_cols: Vec<Expr>,
) -> Result<LogicalPlan> {
    // No functions, no modification needed
    if func_cols.is_empty() {
        return Ok(plan);
    }

    let metadata = plan.schema().metadata().clone();
    let mut fields =
        plan.schema().iter().map(|(q, f)| (q.cloned(), Arc::clone(f))).collect::<Vec<_>>();
    fields.extend(func_fields);

    // Create new schema accounting for pushed functions
    let new_schema = DFSchema::new_with_metadata(fields, metadata)?;

    // Wrap in a projection only if not already a projection
    let new_plan = if let LogicalPlan::Projection(mut projection) = plan {
        projection.expr.extend(func_cols);
        Projection::try_new_with_schema(projection.expr, projection.input, new_schema.into())?
    } else {
        let mut exprs = plan.schema().columns().into_iter().map(Expr::Column).collect::<Vec<_>>();
        exprs.extend(func_cols);
        Projection::try_new_with_schema(exprs, plan.into(), new_schema.into())?
    };

    Ok(LogicalPlan::Projection(new_plan))
}

/// Helper to both collect clickhouse functions into state as well as transform the original
/// expression into an alias
fn collect_and_transform_exprs(
    exprs: Vec<Expr>,
    visitor: &SourceLineageVistor,
    state: &mut PushdownState,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .map(|expr| collect_and_transform_function(expr, visitor, state).map(|t| t.data))
        .collect::<Result<Vec<_>>>()
}

/// Transform an expression possibly containing a `ClickHouse` function.
///
/// # Errors
/// - Returns an error if the
fn collect_and_transform_function(
    expr: Expr,
    visitor: &SourceLineageVistor,
    state: &mut PushdownState,
) -> Result<Transformed<Expr>> {
    expr.transform_down(|e| {
        if is_clickhouse_function(&e) {
            let func_resolved = visitor.resolve_expr(&e);
            let alias = e.schema_name().to_string();

            // If the only source of the function is a Scalar, unwrap and leave as is.
            if matches!(
                func_resolved,
                ResolvedSource::Scalar(_) | ResolvedSource::Scalars(_) | ResolvedSource::Unknown
            ) {
                let Expr::ScalarFunction(ScalarFunction { mut args, .. }) = e else {
                    unreachable!(); // Guaranteed by `is_clickhouse_function` check
                };
                if args.is_empty() {
                    return plan_err!("`clickhouse` function requires an arg, none found: {alias}");
                }
                return Ok(Transformed::new(args.remove(0), true, TreeNodeRecursion::Jump));
            }

            state.function_sources =
                std::mem::take(&mut state.function_sources).merge(func_resolved.clone());
            let current_funcs = state.functions.entry(func_resolved).or_default();
            // Store the original expression for pushdown
            if !current_funcs.contains(&e) {
                current_funcs.push(e);
            }

            Ok(Transformed::new(
                Expr::Column(Column::from_name(alias)),
                true,
                TreeNodeRecursion::Jump,
            ))
        } else {
            Ok(Transformed::no(e))
        }
    })
}

/// Strip table scan of catalog name before passing to extension node
fn strip_table_scan_catalog(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    plan.transform_up_with_subqueries(|node| {
        if let LogicalPlan::TableScan(mut scan) = node {
            if let TableReference::Full { schema, table, .. } = scan.table_name {
                scan.table_name = TableReference::Partial { schema, table };
                return Ok(Transformed::yes(LogicalPlan::TableScan(scan)));
            }

            Ok(Transformed::no(LogicalPlan::TableScan(scan)))
        } else {
            Ok(Transformed::no(node))
        }
    })
}

/// Analyze state and its functions against current plan for any possible semantic violations
fn check_state_for_violations(
    state: &PushdownState,
    plan: &LogicalPlan,
    visitor: &SourceLineageVistor,
) -> HashSet<Column> {
    let mut function_violations = HashSet::new();
    if !state.functions.is_empty() {
        for func in state.functions.values().flatten() {
            function_violations
                .extend(violates_pushdown_semantics(func, plan, visitor).into_iter().cloned());
        }
    }
    function_violations
}

/// Analyze current plan for any possible semantic violations for found functions
fn check_plan_for_violations(plan: &LogicalPlan, visitor: &SourceLineageVistor) -> HashSet<Column> {
    violates_plan_semantics(plan, visitor).into_iter().cloned().collect()
}

/// Check the provided function expr's column references to ensure the plan being analyzed would
/// violate the plan's semantics if the function needed to be pushed below it.
///
/// NOTE: Important! The function must NOT be from the plan passed in but from a plan higher up,
/// otherwise use `violates_plan_semantics` below.
fn violates_pushdown_semantics<'a>(
    function: &Expr,
    plan: &'a LogicalPlan,
    visitor: &SourceLineageVistor,
) -> HashSet<&'a Column> {
    // Resolve the function's column references to match against plan expressions
    //
    // NOTE: The actual Column IDs are used to provide more granular information.
    let function_column_ids = function
        .column_refs()
        .iter()
        .flat_map(|col| visitor.collect_column_ids(col))
        .collect::<HashSet<_>>();

    // If column ids are empty, nothing to analyze
    if function_column_ids.is_empty() {
        return HashSet::new();
    }

    match plan {
        LogicalPlan::Aggregate(agg) => {
            // Ensure aggr exprs do not change the semantic meaning of function column references
            if let Some(related_cols) = check_function_against_exprs(
                function,
                &agg.aggr_expr,
                &function_column_ids,
                visitor,
                true,
            ) {
                return related_cols;
            }

            // Ensure function column references are exposed in group exprs
            if let Some(related_cols) = check_function_against_exprs(
                function,
                &agg.group_expr,
                &function_column_ids,
                visitor,
                false,
            ) {
                return related_cols;
            }
        }
        LogicalPlan::Window(window) => {
            if let Some(related_cols) = check_function_against_exprs(
                function,
                &window.window_expr,
                &function_column_ids,
                visitor,
                true,
            ) {
                return related_cols;
            }
        }
        LogicalPlan::Subquery(query) => {
            if let Some(related_cols) = check_function_against_exprs(
                function,
                &query.outer_ref_columns,
                &function_column_ids,
                visitor,
                true,
            ) {
                return related_cols;
            }
        }
        _ => {}
    }
    HashSet::new()
}

/// Check the provided function expr's column references to ensure the plan being analyzed does not
/// disallow further pushed down.
///
/// NOTE: Important! The function must NOT be from the plan passed in but from a plan higher up
fn violates_plan_semantics<'a>(
    plan: &'a LogicalPlan,
    _visitor: &SourceLineageVistor,
) -> HashSet<&'a Column> {
    if let LogicalPlan::Aggregate(agg) = plan {
        // Finally, ensure any clickhouse functions WITHIN the aggregate plan do not violate
        // grouping semantics
        for expr in &agg.aggr_expr {
            let mut violations = None;
            drop(use_clickhouse_function_context(expr, |agg_func| {
                // A clickhouse function was found, ensure it exists in the group by
                let found = agg.group_expr.iter().any(|group_e| {
                    let mut found = false;
                    use_clickhouse_function_context(group_e, |group_func| {
                        found |= group_func == agg_func;
                        Ok(TreeNodeRecursion::Stop)
                    })
                    .unwrap();
                    found
                });

                if !found {
                    violations = Some(expr.column_refs());
                    // Exit early
                    return plan_err!("Aggregate functions must be in group by");
                }
                Ok(TreeNodeRecursion::Stop)
            }));

            if let Some(violations) = violations {
                // Return early as invariant is already violated
                return violations;
            }
        }
    }

    HashSet::new()
}

/// Iterate over each plan expression provided and determine whether the function's column IDs must
/// be in the expr's column references or must not be.
fn check_function_against_exprs<'a>(
    func: &Expr,
    exprs: &'a [Expr],
    func_column_ids: &HashSet<ColumnId>,
    visitor: &SourceLineageVistor,
    disjoint_required: bool,
) -> Option<HashSet<&'a Column>> {
    for expr in exprs {
        // Little safeguard to allow replacing entire functions in the plan.
        if expr == func {
            continue;
        }
        let col_refs = expr.column_refs();
        let expr_column_ids =
            col_refs.iter().flat_map(|col| visitor.collect_column_ids(col)).collect::<HashSet<_>>();

        // Not sure when this would occur, scalar maybe?
        if expr_column_ids.is_empty() && !disjoint_required {
            continue;
        }
        // Check if the intersection must be empty or must not be empty
        if expr_column_ids.is_disjoint(func_column_ids) != disjoint_required {
            // Return early as invariant is already violated
            return Some(col_refs);
        }
    }
    None
}

/// Helper to emit an error in the situation where a pushdown would violate query semantics
fn semantic_err(
    name: impl std::fmt::Display,
    msg: &str,
    violations: &HashSet<Column>,
) -> Result<()> {
    if !violations.is_empty() {
        let violations =
            violations.iter().map(Column::quoted_flat_name).collect::<Vec<_>>().join(", ");
        return plan_err!("[{name}] - {msg} Violations: {violations}");
    }
    Ok(())
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::common::{Column, Result};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::functions_aggregate::count::count;
    use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, table_scan};
    use datafusion::prelude::*;
    use datafusion::scalar::ScalarValue;
    use datafusion::sql::TableReference;

    use super::super::source_visitor::SourceLineageVistor;
    use super::*;
    use crate::analyzer::source_context::SourceContext;
    use crate::analyzer::source_visitor::ColumnLineage;
    use crate::udfs::clickhouse::clickhouse_udf;
    #[cfg(feature = "mocks")]
    use crate::{
        ClickHouseConnectionPool, ClickHouseTableProvider,
        analyzer::function_pushdown::ClickHouseFunctionPushdown,
        plan_node::CLICKHOUSE_FUNCTION_NODE_NAME,
    };

    fn create_table_scan(table: TableReference, provider: Arc<dyn TableProvider>) -> LogicalPlan {
        LogicalPlanBuilder::scan(table, provider_as_source(provider), None)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_functions_to_field_and_cols_empty() -> Result<()> {
        let visitor = SourceLineageVistor::default();
        let functions = Vec::new();
        let result = functions_to_field_and_cols(functions, &visitor)?;
        assert!(result.0.is_empty());
        assert!(result.1.is_empty());
        Ok(())
    }

    #[test]
    fn test_functions_to_field_and_cols_single_function() -> Result<()> {
        let visitor = SourceLineageVistor::default();
        let functions = vec![Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_udf()),
            args: vec![lit("count()"), lit("Int64")],
        })];

        let (fields, funcs) = functions_to_field_and_cols(functions, &visitor)?;
        assert_eq!(fields.len(), 1);
        assert_eq!(funcs.len(), 1);

        let (field_ref, field) = &fields[0];
        assert!(field_ref.is_none());
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(!field.is_nullable());

        Ok(())
    }

    #[test]
    fn test_functions_to_field_and_cols_multiple_functions() -> Result<()> {
        let visitor = SourceLineageVistor::default();
        let functions = vec![
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(clickhouse_udf()),
                args: vec![lit("count()"), lit("Int64")],
            }),
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(clickhouse_udf()),
                args: vec![lit("sum(x)"), lit("Float64")],
            }),
        ];

        let (fields, funcs) = functions_to_field_and_cols(functions, &visitor)?;
        assert_eq!(fields.len(), 2);
        assert_eq!(funcs.len(), 2);

        // Check data types
        assert_eq!(fields[0].1.data_type(), &DataType::Int64);
        assert_eq!(fields[1].1.data_type(), &DataType::Float64);

        Ok(())
    }

    #[test]
    fn test_wrap_plan_in_projection_no_functions() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Int32, false)]));
        let provider = Arc::new(EmptyTable::new(schema));
        let plan = create_table_scan(TableReference::bare("test_table"), provider);

        let result = wrap_plan_in_projection(plan.clone(), vec![], vec![])?;

        // Should return original plan unchanged when no functions
        match (&plan, &result) {
            (LogicalPlan::TableScan(original), LogicalPlan::TableScan(result)) => {
                assert_eq!(original.table_name, result.table_name);
            }
            _ => panic!("Expected TableScan plans"),
        }

        Ok(())
    }

    #[test]
    fn test_wrap_plan_in_projection_with_functions() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Int32, false)]));
        let provider = Arc::new(EmptyTable::new(schema));
        let plan = create_table_scan(TableReference::bare("test_table"), provider);

        let func_fields =
            vec![(None, Arc::new(Field::new("func_result", DataType::Float64, true)))];
        let func_cols = vec![lit("test_function").alias("func_result")];

        let result = wrap_plan_in_projection(plan, func_fields, func_cols)?;

        // Should be wrapped in a projection
        match result {
            LogicalPlan::Projection(projection) => {
                assert_eq!(projection.expr.len(), 2); // original column + function
                assert_eq!(projection.schema.fields().len(), 2);
            }
            _ => panic!("Expected Projection plan"),
        }

        Ok(())
    }

    #[test]
    fn test_strip_table_scan_catalog_no_catalog() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Int32, false)]));
        let provider = Arc::new(EmptyTable::new(schema));
        let plan = create_table_scan(TableReference::bare("test_table"), provider);

        let result = strip_table_scan_catalog(plan)?;

        // Should not be transformed since no catalog to strip
        assert!(!result.transformed);

        Ok(())
    }

    #[test]
    fn test_strip_table_scan_catalog_with_catalog() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Int32, false)]));
        let provider = Arc::new(EmptyTable::new(schema));
        let table_ref = TableReference::Full {
            catalog: "catalog".into(),
            schema:  "schema".into(),
            table:   "table".into(),
        };
        let plan = create_table_scan(table_ref, provider);

        let result = strip_table_scan_catalog(plan)?;

        // Should be transformed to remove catalog
        assert!(result.transformed);
        if let LogicalPlan::TableScan(scan) = result.data {
            match scan.table_name {
                TableReference::Partial { schema, table } => {
                    assert_eq!(schema.as_ref(), "schema");
                    assert_eq!(table.as_ref(), "table");
                }
                _ => panic!("Expected Partial table reference after catalog stripping"),
            }
        } else {
            panic!("Expected TableScan after transformation");
        }

        Ok(())
    }

    #[test]
    fn test_check_state_for_violations_empty_state() {
        let state = PushdownState::default();
        let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Int32, false)]));
        let provider = Arc::new(EmptyTable::new(schema));
        let plan = create_table_scan(TableReference::bare("test_table"), provider);
        let visitor = SourceLineageVistor::new();

        let violations = check_state_for_violations(&state, &plan, &visitor);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_check_plan_for_violations_non_aggregate() {
        let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Int32, false)]));
        let provider = Arc::new(EmptyTable::new(schema));
        let plan = create_table_scan(TableReference::bare("test_table"), provider);

        let visitor = SourceLineageVistor::new();
        let violations = check_plan_for_violations(&plan, &visitor);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_semantic_err_no_violations() {
        let violations = HashSet::new();
        let result = semantic_err("TestPlan", "Test message", &violations);
        assert!(result.is_ok());
    }

    #[test]
    fn test_semantic_err_with_violations() {
        let mut violations = HashSet::new();
        let _ = violations.insert(Column::new_unqualified("test_col"));

        let result = semantic_err("TestPlan", "Test message", &violations);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("[TestPlan]"));
        assert!(err_msg.contains("Test message"));
        assert!(err_msg.contains("test_col"));
    }

    #[test]
    fn test_collect_and_transform_exprs_empty() -> Result<()> {
        let exprs = Vec::new();
        let visitor = SourceLineageVistor::new();
        let mut state = PushdownState::default();

        let result = collect_and_transform_exprs(exprs, &visitor, &mut state)?;
        assert!(result.is_empty());
        Ok(())
    }

    #[test]
    fn test_collect_and_transform_exprs_no_clickhouse_functions() -> Result<()> {
        let exprs = vec![col("test_col"), lit(42)];
        let visitor = SourceLineageVistor::new();
        let mut state = PushdownState::default();

        let result = collect_and_transform_exprs(exprs.clone(), &visitor, &mut state)?;
        assert_eq!(result.len(), 2);
        // Original expressions should be preserved when no ClickHouse functions
        assert_eq!(result[0], col("test_col"));
        assert_eq!(result[1], lit(42));
        Ok(())
    }

    #[test]
    fn test_collect_and_transform_function_non_clickhouse() -> Result<()> {
        let expr = col("test_col");
        let visitor = SourceLineageVistor::new();
        let mut state = PushdownState::default();

        let result = collect_and_transform_function(expr.clone(), &visitor, &mut state)?;
        assert!(!result.transformed);
        assert_eq!(result.data, expr);
        Ok(())
    }

    #[test]
    fn test_collect_and_transform_function_with_clickhouse() -> Result<()> {
        let table = TableReference::bare("test");
        let test_col = Column::new(Some(table.clone()), "test_col");

        let schema = Schema::new(vec![Field::new("test_col", DataType::Int32, false)]);

        let clickhouse_expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_udf()),
            args: vec![count(Expr::Column(test_col.clone())), lit("Int32")],
        });

        let mut visitor = SourceLineageVistor::new();

        // Seed the column from a dummy plan
        let dummy_plan =
            table_scan(Some(table.clone()), &schema, None)?.select(vec![0])?.build()?;
        let _ = dummy_plan.visit(&mut visitor)?;
        let mut state = PushdownState::default();
        let result = collect_and_transform_function(clickhouse_expr, &visitor, &mut state)?;
        assert!(result.transformed);

        // Should be replaced with a column alias
        match result.data {
            Expr::Column(_) => {} // Expected
            _ => panic!("Expected Column expression after ClickHouse function transformation"),
        }
        // State should have functions added
        assert!(!state.functions.is_empty());
        Ok(())
    }

    #[test]
    fn test_column_id_resolution() -> Result<()> {
        let table1 = TableReference::bare("test1");
        let table2 = TableReference::bare("test2");
        let test_col1 = Column::new(Some(table1.clone()), "test_col1");
        let test_col_alt1 = Column::new(Some(table1.clone()), "test_col_alt1");
        let test_col2 = Column::new(Some(table2.clone()), "test_col2");

        let schema1 = Schema::new(vec![
            Field::new("test_col1", DataType::Int32, false),
            Field::new("test_col_alt1", DataType::Int32, false),
        ]);

        let schema2 = Schema::new(vec![Field::new("test_col2", DataType::Int32, false)]);

        let mut visitor = SourceLineageVistor::new();

        // Test Columns Ids
        let clickhouse_expr_simple = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_udf()),
            args: vec![
                Expr::Column(test_col1.clone()) + Expr::Column(test_col_alt1.clone()),
                lit("Int64"),
            ],
        })
        .alias("dummy_udf");
        let clickhouse_expr_comp = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_udf()),
            args: vec![
                Expr::Column(test_col1.clone()) + Expr::Column(test_col2.clone()) + lit(2),
                lit("Int64"),
            ],
        })
        .alias("dummy_udf_comp");
        let simple_col = Column::from_name("dummy_udf");
        let comp_col = Column::from_name("dummy_udf_comp");
        let right_plan =
            table_scan(Some(table2.clone()), &schema2, None)?.select(vec![0])?.build()?;

        let dummy_plan = table_scan(Some(table1.clone()), &schema1, None)?
            .select(vec![0, 1])?
            .project(vec![Expr::Column(test_col1.clone()), clickhouse_expr_simple])?
            .filter(Expr::Column(test_col1.clone()).gt(lit(0)))?
            .join_on(right_plan, JoinType::Inner, vec![
                col("table1.test_col1").eq(col("table2.test_col2")),
            ])?
            .project(vec![
                Expr::Column(simple_col.clone()),
                clickhouse_expr_comp,
                lit("hello").alias("scalar_col"),
            ])?
            .build()?;

        let _ = dummy_plan.visit(&mut visitor)?;

        // Simple
        let lineage = visitor.column_lineage.get(&simple_col);
        let resolved = visitor.resolve_column(&simple_col);
        let col_ids = visitor.collect_column_ids(&simple_col);

        let Some(ColumnLineage::Compound(ids)) = lineage else {
            panic!("Derived columns of clickhouse functions should be stored as `Compound`");
        };

        assert_eq!(col_ids.len(), 2, "Expected 2 columns in when resolving context");
        assert_eq!(ids.len(), 3, "Expected 3 columns in Compound context");
        assert!(col_ids.is_subset(ids), "Expected collected columns to be contained in context");

        let ResolvedSource::Compound(sources) = resolved else {
            panic!("Expected Compound source for simple column");
        };

        let mut resolved_sources = sources.to_vec();
        let scalar_source = resolved_sources.pop().unwrap();
        let table_source = resolved_sources.pop().unwrap();

        assert_eq!(
            scalar_source.as_ref(),
            &SourceContext::Scalar(ScalarValue::Utf8(Some("Int64".into())))
        );
        assert_eq!(table_source.as_ref(), &SourceContext::Table(table1.clone()));

        let nullable = visitor.resolve_nullable(&Expr::Column(simple_col.clone()));
        assert!(!nullable);

        // Compound
        let lineage = visitor.column_lineage.get(&comp_col);
        let col_ids = visitor.collect_column_ids(&comp_col);

        let Some(ColumnLineage::Compound(cols)) = lineage else {
            panic!("Derived columns should be stored as `Compound`");
        };

        // NOTE: It does NOT return the ScalarValue!
        assert_eq!(cols.len(), 4, "Expected 4 columns in Compound context, inc Scalars");
        assert_eq!(col_ids.len(), 2, "Expected 2 columns in collected columns, exc Scalars");
        assert!(col_ids.is_subset(cols), "Expected collected columns to be a subset of context");

        Ok(())
    }

    // TODO: Add tests for Unnest plan after upstream bug is addressed
    //
    // Ref: https://github.com/datafusion-contrib/datafusion-federation/pull/135

    // ----
    // All of the following tests require the "mocks" feature to be enabled
    // ----

    /// Helper to check if a plan is a `ClickHouse` Extension node
    #[cfg(feature = "mocks")]
    fn is_clickhouse_extension(plan: &LogicalPlan) -> bool {
        if let LogicalPlan::Extension(ext) = plan {
            ext.node.name() == CLICKHOUSE_FUNCTION_NODE_NAME
        } else {
            false
        }
    }

    /// Helper to find all `ClickHouse` Extension nodes in the tree with their positions
    #[cfg(feature = "mocks")]
    fn find_wrapped_plans(plan: &LogicalPlan) -> Vec<String> {
        fn traverse(plan: &LogicalPlan, wrapped_plans: &mut Vec<String>, path: &str) {
            if is_clickhouse_extension(plan) {
                wrapped_plans.push(format!("{path}: {plan}"));
            }
            for (i, input) in plan.inputs().iter().enumerate() {
                traverse(input, wrapped_plans, &format!("{path}/input[{i}]"));
            }
        }
        let mut wrapped_plans = Vec::new();
        traverse(plan, &mut wrapped_plans, "root");
        wrapped_plans
    }

    #[cfg(all(feature = "federation", feature = "mocks"))]
    fn compare_plan_display(plan: &LogicalPlan, expected: impl Into<String>) {
        let mut plan_display = plan.display_indent().to_string();
        plan_display.retain(|c| !c.is_whitespace());
        let mut expected = expected.into();
        expected.retain(|c| !c.is_whitespace());
        assert_eq!(plan_display, expected, "Expected equal plans");
    }

    /// Create a `SessionContext` with registered tables and analyzer for SQL testing
    #[cfg(feature = "mocks")]
    fn create_test_context() -> Result<SessionContext> {
        let ctx = SessionContext::new();
        // Register the ClickHouse pushdown UDF
        ctx.register_udf(clickhouse_udf());
        // Register the ClickHouse function pushdown analyzer
        ctx.add_analyzer_rule(Arc::new(ClickHouseFunctionPushdown));

        let schema1 = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Int32, false),
            Field::new("col3", DataType::Utf8, false),
        ]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let pool = Arc::new(ClickHouseConnectionPool::new("pool".to_string(), ()));
        let table1 = ClickHouseTableProvider::new_with_schema_unchecked(
            Arc::clone(&pool),
            "table1".into(),
            Arc::clone(&schema1),
        );
        let table2 = ClickHouseTableProvider::new_with_schema_unchecked(
            Arc::clone(&pool),
            "table1".into(),
            schema2,
        );

        // Register table1 (col1: Int32, col2: Int32, col3: Utf8)
        drop(ctx.register_table("table1", Arc::new(table1))?);

        // Register table2 (id: Int32)
        drop(ctx.register_table("table2", Arc::new(table2))?);

        // Register table3 (outside grouping) (col1: Int32, col2: Int32, col3: Utf8)
        let table3 = Arc::new(EmptyTable::new(schema1));
        drop(ctx.register_table("table3", table3)?);

        Ok(ctx)
    }

    /// Run a query and return the analyzed plan
    #[cfg(feature = "mocks")]
    async fn run_query(sql: &str) -> Result<LogicalPlan> {
        let ctx = create_test_context()?;
        let analyzed_plan = ctx.sql(sql).await?.into_optimized_plan()?; // Analyzer runs automatically
        SQLOptions::default().verify_plan(&analyzed_plan)?;
        Ok(analyzed_plan)
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_simple_projection_with_clickhouse_function() -> Result<()> {
        // Test: SELECT clickhouse(exp(col1 + col2), 'Float64'), col2 * 2, UPPER(col3) FROM table1
        // Expected: Entire plan wrapped because all functions and columns from same table
        let sql =
            "SELECT clickhouse(exp(col1 + col2), 'Float64'), col2 * 2, UPPER(col3) FROM table1";
        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        {
            let expected_plan = r#"
            Projection: clickhouse(exp(CAST(table1.col1 + table1.col2 AS Float64)), Utf8("Float64")), CAST(table1.col2 AS Int64) * Int64(2), upper(table1.col3)
              TableScan: table1 projection=[col1, col2, col3]
            "#;
            assert!(wrapped_plans.is_empty(), "Expected no wrapped plans");
            compare_plan_display(&analyzed_plan, expected_plan);
        }
        #[cfg(not(feature = "federation"))]
        {
            assert_eq!(wrapped_plans.len(), 1, "Expected exactly one wrapped plan");
            assert!(wrapped_plans[0].starts_with("root:"), "Expected wrapping at root level");
        }
        Ok(())
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_filter_with_clickhouse_function() -> Result<()> {
        // Test: SELECT col2, col3 FROM table1 WHERE clickhouse(exp(col1), 'Float64') > 10
        // Expected: Filter wrapped because it contains ClickHouse function, outer projection has no
        // functions
        let sql = "SELECT col2, col3 FROM table1 WHERE clickhouse(exp(col1), 'Float64') > 10";
        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        {
            let expected_plan = r#"
               TableScan: table1 projection=[col2, col3], full_filters=[clickhouse(exp(CAST(table1.col1 AS Float64)), Utf8("Float64")) > Float64(10)]
            "#;
            assert!(wrapped_plans.is_empty(), "No wrapping expected");
            compare_plan_display(&analyzed_plan, expected_plan);
        }
        #[cfg(not(feature = "federation"))]
        {
            // Should have exactly one wrapped plan at the filter level (since outer projection has
            // no functions)
            assert_eq!(wrapped_plans.len(), 1, "Expected exactly one wrapped plan");
            assert!(wrapped_plans[0].contains("root"), "Expected wrapping at filter level, root");
        }
        Ok(())
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_aggregate_blocks_pushdown() -> Result<()> {
        // Test: SELECT col2, COUNT(*) FROM table1 WHERE clickhouse(exp(col1), 'Float64') > 5 GROUP
        // BY col2 Expected: Function should be wrapped at a level above the aggregate
        let sql = "SELECT col2, COUNT(*) FROM table1 WHERE clickhouse(exp(col1), 'Float64') > 5 \
                   GROUP BY col2";
        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        {
            assert!(wrapped_plans.is_empty(), "Expected no wrapped plans");
            assert!(
                analyzed_plan.display().to_string().to_lowercase().starts_with("projection"),
                "Expected projection"
            );
        }

        #[cfg(not(feature = "federation"))]
        {
            // Should have exactly one wrapped plan, and it should be at the aggregate input level
            assert_eq!(wrapped_plans.len(), 1, "Expected exactly one wrapped plan");
            // The wrapped plan should be at the input to the aggregate (aggregate blocks pushdown)
            assert!(
                wrapped_plans.iter().any(|w| w.contains("root")),
                "Expected function to be wrapped at aggregate input level due to blocking"
            );
        }
        Ok(())
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_multiple_clickhouse_functions_same_table() -> Result<()> {
        // Test: SELECT clickhouse(exp(col1), 'Float64'), clickhouse(exp(col2), 'Float64') FROM
        // table1 Expected: Both functions use same table, should be wrapped together at
        // root level
        let sql = "SELECT clickhouse(exp(col1), 'Float64') AS f1, clickhouse(exp(col2), \
                   'Float64') AS f2 FROM table1";
        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        {
            assert!(wrapped_plans.is_empty(), "Expected no wrapped plans");
            assert!(
                analyzed_plan.display().to_string().to_lowercase().starts_with("projection"),
                "Expected projection"
            );
        }

        #[cfg(not(feature = "federation"))]
        {
            // Should have exactly one wrapped plan containing both functions
            assert_eq!(
                wrapped_plans.len(),
                1,
                "Expected exactly one wrapped plan for both functions"
            );
            assert!(
                wrapped_plans[0].starts_with("root:"),
                "Expected both functions wrapped together at root level"
            );
        }
        Ok(())
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_no_functions_no_wrapping() -> Result<()> {
        // Test: SELECT col1, col2 FROM table1
        // Expected: No exp functions, so no wrapping should occur
        let sql = "SELECT col1, col2 FROM table1";
        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);
        // Should have no wrapped plans
        assert_eq!(wrapped_plans.len(), 0, "Expected no wrapped plans when no functions present");
        Ok(())
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_wrapped_disjoint_tables() -> Result<()> {
        // Test: Verify the disjoint check wraps the entire tree, since `table1` and `table2` have
        // the same context during testing.
        let sql = "SELECT t1.col1, clickhouse(exp(t2.id), 'Float64') FROM (SELECT col1 FROM \
                   table1) t1 JOIN (SELECT id from table2) t2 ON t1.col1 = t2.id";
        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        {
            assert!(wrapped_plans.is_empty(), "Expected no wrapped plans");
            assert!(
                analyzed_plan.display().to_string().to_lowercase().starts_with("projection"),
                "Expected projection"
            );
        }

        #[cfg(not(feature = "federation"))]
        {
            assert_eq!(wrapped_plans.len(), 1, "Expected function wrapped entire plan");
            assert!(
                wrapped_plans[0].contains("root"),
                "Expected function wrapped on right side of JOIN"
            );
        }
        Ok(())
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_disjoint_tables() -> Result<()> {
        // Test: Verify the disjoint check wraps plan on join side only.
        //       The test tables "test1" and "test2" will use the same context, "test" will not.
        let sql = "SELECT t3.col1, clickhouse(exp(t2.id), 'Float64')
            FROM (SELECT col1 FROM table3) t3
            JOIN (SELECT id from table2) t2 ON t3.col1 = t2.id
        ";

        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        // TODO: Update with expected plan display
        #[cfg(feature = "federation")]
        {
            let expected_plan = r#"
            Projection: t3.col1, clickhouse(exp(t2.id),Utf8("Float64"))
              Inner Join: t3.col1 = t2.id
                SubqueryAlias: t3
                  TableScan: table3 projection=[col1]
                SubqueryAlias: t2
                  Projection: table2.id, clickhouse(exp(CAST(table2.id AS Float64)), Utf8("Float64")) AS clickhouse(exp(t2.id),Utf8("Float64"))
                    TableScan: table2 projection=[id]
            "#;
            assert!(wrapped_plans.is_empty(), "Expected no wrapped plans");
            compare_plan_display(&analyzed_plan, expected_plan);
        }

        #[cfg(not(feature = "federation"))]
        {
            // This test verifies that the algorithm correctly handles complex JOIN scenarios:
            // 1. Function column refs resolve to table2 source (identified by "grouped source")
            // 2. Projection column refs include both table and table2 sources
            // 3. Algorithm should route function to the right side of JOIN where t2.id is available
            // 4. Should have exactly one wrapped plan on the right side
            assert_eq!(wrapped_plans.len(), 1, "Expected function routed to right side of JOIN");
            assert!(
                wrapped_plans[0].contains("input[1]"),
                "Expected function wrapped on right side of JOIN"
            );
        }

        Ok(())
    }

    // TODO: This plan represents a feature that needs to be implemented: how to handle "mixed"
    // functions in a plan node. Ideally the functions would be separated and the clickhouse
    // function lowered, but that will take quite a bit of logic.
    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_complex_agg() -> Result<()> {
        let sql = "SELECT
                clickhouse(pow(t.id, 2), 'Int32') as id_mod,
                COUNT(t.id) as total,
                MAX(clickhouse(exp(t.id), 'Float64')) as max_exp
            FROM table2 t
            GROUP BY id_mod";
        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        {
            let expected_plan = r#"
            Projection: clickhouse(power(t.id,Int64(2)),Utf8("Int32")) AS id_mod, count(t.id) AS total, max(clickhouse(exp(t.id),Utf8("Float64"))) AS max_exp
              Aggregate: groupBy=[[clickhouse(power(CAST(t.id AS Int64), Int64(2)), Utf8("Int32"))]], aggr=[[count(t.id), max(clickhouse(exp(CAST(t.id AS Float64)), Utf8("Float64")))]]
                SubqueryAlias: t
                  TableScan: table2 projection=[id]
            "#
            .trim();
            assert!(wrapped_plans.is_empty(), "No wrapping expected");
            compare_plan_display(&analyzed_plan, expected_plan);
        }

        #[cfg(not(feature = "federation"))]
        {
            assert_eq!(wrapped_plans.len(), 1, "Expected entire plan wrapped");
            assert!(wrapped_plans[0].contains("root"), "Expected function wrapped at root");
        }

        Ok(())
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_union() -> Result<()> {
        let sql = "
            SELECT col1 as id, clickhouse(exp(col1), 'Float64') as func_id
            FROM table1 WHERE table1.col1 = 1
            UNION ALL
            SELECT id, clickhouse(pow(id, 2), 'Float64') as func_id
            FROM table2 WHERE table2.id = 1
        ";
        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        {
            let expected_plan = r#"
            Union
              Projection: table1.col1 AS id, clickhouse(exp(CAST(table1.col1 AS Float64)), Utf8("Float64")) AS func_id
                TableScan: table1 projection=[col1], full_filters=[table1.col1 = Int32(1)]
              Projection: table2.id, clickhouse(power(CAST(table2.id AS Int64), Int64(2)), Utf8("Float64")) AS func_id
                TableScan: table2 projection=[id], full_filters=[table2.id = Int32(1)]
            "#
            .trim();
            assert!(wrapped_plans.is_empty(), "No wrapping expected");
            compare_plan_display(&analyzed_plan, expected_plan);
        }

        #[cfg(not(feature = "federation"))]
        {
            assert_eq!(wrapped_plans.len(), 1, "Expected entire plan wrapped");
            assert!(wrapped_plans[0].contains("root"), "Expected function wrapped at root");
        }

        Ok(())
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_limit() -> Result<()> {
        let sql = "SELECT clickhouse(abs(t2.id), 'Int32') FROM table2 t2 LIMIT 1";
        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        {
            let expected_plan = r#"
                Projection: clickhouse(abs(t2.id), Utf8("Int32"))
                  SubqueryAlias: t2
                    Limit: skip=0, fetch=1
                      TableScan: table2 projection=[id], fetch=1
            "#
            .trim();
            assert!(wrapped_plans.is_empty(), "No wrapping expected");
            compare_plan_display(&analyzed_plan, expected_plan);
        }

        #[cfg(not(feature = "federation"))]
        {
            assert_eq!(wrapped_plans.len(), 1, "Expected entire plan wrapped");
            assert!(wrapped_plans[0].contains("root"), "Expected function wrapped at root");
        }
        Ok(())
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_sort() -> Result<()> {
        let sql = "SELECT t2.id FROM table2 t2 ORDER BY clickhouse(abs(t2.id), 'Int64')";
        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        {
            let expected_plan = r#"
            Sort: clickhouse(abs(t2.id), Utf8("Int64")) ASC NULLS LAST
              SubqueryAlias: t2
                TableScan: table2 projection=[id]
            "#
            .trim();
            assert!(wrapped_plans.is_empty(), "No wrapping expected");
            compare_plan_display(&analyzed_plan, expected_plan);
        }

        #[cfg(not(feature = "federation"))]
        {
            assert_eq!(wrapped_plans.len(), 1, "Expected entire plan wrapped");
            assert!(wrapped_plans[0].contains("root"), "Expected function wrapped at root");
        }
        Ok(())
    }

    // NOTE: First query will fail. That is expected due to how DataFusion groups joins. The
    // unresolved table will block pushing the function to the left side of the join, and the usage
    // of both tables will block pushing the function to the right side of the join.
    //
    // But, in the second query, the joins are re-organized. This groups the "clickhouse" tables on
    // the same side of the join.
    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn test_multiple_cols_same_function() -> Result<()> {
        let sql = "SELECT t3.col2
                , t1.col2
                , t2.id
                , clickhouse(t1.col1 + t2.id, 'Int64') as sum_ids
            FROM table3 t3
            JOIN table1 t1 ON t1.col1 = t3.col1
            JOIN table2 t2 ON t2.id = t1.col1
        ";
        let result = run_query(sql).await;
        assert!(result.is_err(), "Cannot push to either side of join");

        let sql = "SELECT t3.col2
                , t1.col2
                , t2.id
                , clickhouse(t1.col1 + t2.id, 'Int64') as sum_ids
            FROM table1 t1
            JOIN table2 t2 ON t2.id = t1.col1
            JOIN table3 t3 ON t2.id = t3.col1
        ";

        let analyzed_plan = run_query(sql).await?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        {
            let expected_plan = r#"
            Projection: t3.col2, t1.col2, t2.id, clickhouse(t1.col1 + t2.id,Utf8("Int64")) AS sum_ids
              Inner Join: t2.id = t3.col1
                Projection: t1.col2, t2.id, clickhouse(t1.col1 + t2.id, Utf8("Int64")) AS clickhouse(t1.col1 + t2.id,Utf8("Int64"))
                  Inner Join: t1.col1 = t2.id
                    SubqueryAlias: t1
                      TableScan: table1 projection=[col1, col2]
                    SubqueryAlias: t2
                      TableScan: table2 projection=[id]
              SubqueryAlias: t3
                TableScan: table3 projection=[col1, col2]
            "#.trim();

            assert!(wrapped_plans.is_empty(), "No wrapping expected");
            compare_plan_display(&analyzed_plan, expected_plan);
        }

        #[cfg(not(feature = "federation"))]
        {
            assert_eq!(wrapped_plans.len(), 1, "Expected entire plan wrapped");
            assert!(wrapped_plans[0].contains("root"), "Expected function wrapped at root");
        }
        Ok(())
    }
}
