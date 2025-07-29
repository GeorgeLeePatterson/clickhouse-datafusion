use std::collections::HashSet;
use std::sync::Arc;

use clickhouse_arrow::rustc_hash::FxHashMap;
use datafusion::arrow::datatypes::Field;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result, plan_err};
#[cfg(not(feature = "federation"))]
use datafusion::logical_expr::Extension;
use datafusion::logical_expr::{
    Aggregate, Distinct, Filter, Join, Limit, LogicalPlan, Projection, SubqueryAlias, Union,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;

#[cfg(not(feature = "federation"))]
use super::plan_node::ClickHouseFunctionNode;
use super::pushdown::{
    extract_pushed_function, find_clickhouse_function, is_clickhouse_function,
    unwrap_clickhouse_function, use_clickhouse_function_context,
};
use super::source_visitor::{ColumnId, ResolvedSource, SourceLineageVistor};

/// State for tracking `ClickHouse` functions during pushdown analysis
#[derive(Default, Debug, Clone)]
struct PushdownState {
    /// `ClickHouse` functions being pushed down, organized by resolved sources
    functions: FxHashMap<ResolvedSource, Vec<Expr>>,
    /// Tracks the resolved sources of all functions collected, for non-branching plans
    resolved:  ResolvedSource,
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

        analyze_and_transform_plan(plan, &lineage_visitor, PushdownState::default()).map(|t| t.data)
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
/// Analyze and transform a plan. Functions flow DOWN - only recurse if current level cannot handle
/// them.
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
#[expect(clippy::too_many_lines)]
fn analyze_and_transform_plan(
    plan: LogicalPlan,
    lineage_visitor: &SourceLineageVistor,
    mut state: PushdownState,
) -> Result<Transformed<LogicalPlan>> {
    // First determine any semantic violations with pushed functions
    let function_violations = check_state_for_violations(&state, &plan, lineage_visitor);

    // Then check any plan violations for plan functions
    let plan_violations = check_plan_for_violations(&plan);

    // Possible exit early for passthrough plans
    if matches!(
        plan,
        LogicalPlan::Union(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Distinct(Distinct::All(_))
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
    ) && handle_passthrough_plan(
        &plan,
        &mut state,
        lineage_visitor,
        &function_violations,
        &plan_violations,
    )? {
        let wrapped_plan = wrap_plan_with_functions(plan, state)?;
        return Ok(Transformed::yes(wrapped_plan));
    }

    let mut has_clickhouse_functions = !state.functions.is_empty();

    match plan {
        // Extract ClickHouse functions from projections
        //
        // NOTE: A lot of this logic is duplicated in the wildcard branch, but since Projections are
        //       quite common, we can avoid some clones by matching directly
        LogicalPlan::Projection(Projection { expr, input, ref schema, .. }) => {
            // Update the ResolvedSource of the state with any new clickhouse functions
            state.resolved = state.resolved.merge(
                lineage_visitor.resolve_exprs(expr.iter().filter(|e| find_clickhouse_function(e))),
            );

            // If column references resolve to the same source, wrap the plan
            if state.resolved.is_known() {
                let plan_sources = lineage_visitor.resolve_schema(schema);
                if state.resolved.resolves_eq(&plan_sources) {
                    let wrapped_plan = wrap_plan_with_functions(
                        LogicalPlan::Projection(Projection::try_new(expr, input)?),
                        state,
                    )?;
                    return Ok(Transformed::yes(wrapped_plan));
                }
            }

            let schema_change_needed =
                expr.iter().any(find_clickhouse_function) || !state.functions.is_empty();

            // Update w/ schema and expression changes
            let aliased_exprs = collect_and_transform_exprs(expr, lineage_visitor, &mut state)?;

            // Function dependencies not satisfied - recurse deeper with function state
            let updated_plan =
                analyze_and_transform_plan(Arc::unwrap_or_clone(input), lineage_visitor, state)?;

            // Reconstruct new projection with transformed input and aliased expressions
            let new_projection = Projection::try_new(aliased_exprs, Arc::new(updated_plan.data))?;
            let new_plan = LogicalPlan::Projection(new_projection);
            if updated_plan.transformed || schema_change_needed {
                Ok(Transformed::yes(new_plan))
            } else {
                Ok(Transformed::no(new_plan))
            }
        }

        LogicalPlan::Limit(Limit { input, fetch, skip }) => {
            if let Some(ref f) = fetch
                && find_clickhouse_function(f)
            {
                state.resolved = state.resolved.merge(lineage_visitor.resolve_expr(f));
                has_clickhouse_functions |= true;
            }

            if let Some(ref s) = skip
                && find_clickhouse_function(s)
            {
                state.resolved = state.resolved.merge(lineage_visitor.resolve_expr(s));
                has_clickhouse_functions |= true;
            }

            // Generate the ResolvedSource for the input's expressions
            if state.resolved.is_known() {
                // Determine if inner schema resolves to pushed functions here
                let inner_resolves_here =
                    state.resolved.resolves_eq(&lineage_visitor.resolve_schema(input.schema()));
                if inner_resolves_here {
                    let wrapped_plan = wrap_plan_with_functions(
                        LogicalPlan::Limit(Limit { skip, fetch, input }),
                        state,
                    )?;
                    return Ok(Transformed::yes(wrapped_plan));
                }
            }

            if has_clickhouse_functions {
                // Ensure no semantic violations in the result of pushdown
                semantic_err(
                    "Limit",
                    "SQL unsupported, pushed functions violate sql semantics in plan.",
                    &function_violations,
                )?;

                // Ensure no plan violations in the result of pushdown
                semantic_err(
                    "Limit",
                    "SQL unsupported, plan violates sql semantics in expressions.",
                    &plan_violations,
                )?;
            }

            let new_fetch = if let Some(f) = fetch {
                Some(collect_and_transform_function(*f, lineage_visitor, &mut state)?.data)
            } else {
                None
            }
            .map(Box::new);

            let new_skip = if let Some(s) = skip {
                Some(collect_and_transform_function(*s, lineage_visitor, &mut state)?.data)
            } else {
                None
            }
            .map(Box::new);

            // Dependencies not satisfied - recurse deeper
            let updated_plan =
                analyze_and_transform_plan(Arc::unwrap_or_clone(input), lineage_visitor, state)?;

            // Reconstruct limit with transformed input if needed
            let new_limit =
                Limit { fetch: new_fetch, skip: new_skip, input: Arc::new(updated_plan.data) };
            if updated_plan.transformed {
                Ok(Transformed::yes(LogicalPlan::Limit(new_limit)))
            } else {
                Ok(Transformed::no(LogicalPlan::Limit(new_limit)))
            }
        }

        // Handle filters with ClickHouse functions or blocking logic
        LogicalPlan::Filter(Filter { predicate, input, .. }) => {
            if find_clickhouse_function(&predicate) {
                state.resolved = state.resolved.merge(lineage_visitor.resolve_expr(&predicate));
                has_clickhouse_functions |= true;
            }

            // Generate the ResolvedSource for the input's expressions
            if state.resolved.is_known() {
                // Determine if inner schema resolves to pushed functions here
                let inner_resolves_here =
                    state.resolved.resolves_eq(&lineage_visitor.resolve_schema(input.schema()));
                if inner_resolves_here {
                    let wrapped_plan = wrap_plan_with_functions(
                        LogicalPlan::Filter(Filter::try_new(predicate, input)?),
                        state,
                    )?;
                    return Ok(Transformed::yes(wrapped_plan));
                }
            }

            if has_clickhouse_functions {
                // Ensure no semantic violations in the result of pushdown
                semantic_err(
                    "Filter",
                    "SQL unsupported, pushed functions violate sql semantics in filter plan.",
                    &function_violations,
                )?;

                // Ensure no plan violations in the result of pushdown
                semantic_err(
                    "Filter",
                    "SQL unsupported, filter plan violates sql semantics in filter expressions.",
                    &plan_violations,
                )?;
            }

            // Add extracted functions to pending functions for recursion ONLY when we recurse
            let new_predicate =
                collect_and_transform_function(predicate, lineage_visitor, &mut state)?.data;

            // Dependencies not satisfied - recurse deeper
            let updated_plan =
                analyze_and_transform_plan(Arc::unwrap_or_clone(input), lineage_visitor, state)?;

            // Reconstruct filter with transformed input if needed
            let new_filter = Filter::try_new(new_predicate, Arc::new(updated_plan.data))?;
            if updated_plan.transformed {
                Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
            } else {
                Ok(Transformed::no(LogicalPlan::Filter(new_filter)))
            }
        }

        // Aggregates block functions from crossing boundaries but can inspect their inputs
        LogicalPlan::Aggregate(agg) => {
            state.resolved = state
                .resolved
                .merge(
                    lineage_visitor.resolve_exprs(
                        agg.group_expr
                            .iter()
                            .filter(|e| find_clickhouse_function(e))
                            .inspect(|_| has_clickhouse_functions |= true),
                    ),
                )
                .merge(
                    lineage_visitor.resolve_exprs(
                        agg.aggr_expr
                            .iter()
                            .filter(|e| find_clickhouse_function(e))
                            .inspect(|_| has_clickhouse_functions |= true),
                    ),
                );

            // If pending functions pushed, attempt to wrap the aggregate
            if state.resolved.is_known() {
                let agg_resolved = lineage_visitor.resolve_schema(&agg.schema);

                // TODO: (Feature) Determine if any non-clickhouse functions are present and issue
                //       a warning. Also, see if splitting out the non-clickhouse functions from the
                //       clickhouse functions is possible

                // Cannot push past agg if columns resolved to different sources
                if state.resolved.resolves_eq(&agg_resolved) {
                    let wrapped_plan =
                        wrap_plan_with_functions(LogicalPlan::Aggregate(agg), state)?;
                    return Ok(Transformed::yes(wrapped_plan));
                }
            }

            if has_clickhouse_functions {
                // Ensure no semantic violations in the result of pushdown
                semantic_err(
                    "Aggregate",
                    "SQL unsupported, pushed functions violate sql semantics in aggregate plan.",
                    &function_violations,
                )?;

                // Ensure no plan violations in the result of pushdown
                semantic_err(
                    "Aggregate",
                    "SQL unsupported, aggregate plan violates sql semantics in aggregate \
                     expressions.",
                    &plan_violations,
                )?;
            }

            // Collect aggregate expressions
            let aliased_aggr =
                collect_and_transform_exprs(agg.aggr_expr, lineage_visitor, &mut state)?;
            let aliased_group =
                collect_and_transform_exprs(agg.group_expr, lineage_visitor, &mut state)?;

            // No pending functions pushed, recurse into input to handle internal functions
            let new_input = analyze_and_transform_plan(
                Arc::unwrap_or_clone(agg.input),
                lineage_visitor,
                state,
            )?;

            let was_transformed = new_input.transformed || has_clickhouse_functions;

            let new_plan = LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(new_input.data),
                aliased_aggr,
                aliased_group,
            )?);

            if was_transformed {
                Ok(Transformed::yes(new_plan))
            } else {
                Ok(Transformed::no(new_plan))
            }
        }

        // For joins - route functions to appropriate sides based on column ownership
        LogicalPlan::Join(join) => {
            // Cache filter expression for rebuild
            let mut join_filter = join.filter.clone();

            let mut left_pushdown_state = PushdownState::default();
            let mut right_pushdown_state = PushdownState::default();

            if !state.functions.is_empty() {
                // Route functions based on column ownership
                let left_schema = join.left.schema();
                let right_schema = join.right.schema();

                let left_sources = lineage_visitor.resolve_schema(left_schema);
                let right_sources = lineage_visitor.resolve_schema(right_schema);

                // Entire join can be wrapped
                if state.resolved.resolves_eq(&left_sources)
                    && state.resolved.resolves_eq(&right_sources)
                {
                    let wrapped_plan = wrap_plan_with_functions(LogicalPlan::Join(join), state)?;
                    return Ok(Transformed::yes(wrapped_plan));
                }

                // Since the join filter applies to left & right schemas it must be collected first.
                if let Some(f) = join_filter {
                    let new_filter =
                        collect_and_transform_function(f, lineage_visitor, &mut state)?;
                    has_clickhouse_functions |= new_filter.transformed;
                    join_filter = Some(new_filter.data);
                }

                let mut left_resolved = ResolvedSource::Unknown;
                let mut right_resolved = ResolvedSource::Unknown;

                // Extract functions from left sources
                let left_functions = state
                    .functions
                    .extract_if(|resolved, _| {
                        let resolves_eq = resolved.resolves_eq(&left_sources);
                        if resolves_eq {
                            left_resolved =
                                std::mem::take(&mut left_resolved).merge(resolved.clone());
                        }
                        resolves_eq
                    })
                    .collect::<FxHashMap<_, _>>();
                // Extract functions from right sources
                let right_functions = state
                    .functions
                    .extract_if(|resolved, _| {
                        let resolved_eq = resolved.resolves_eq(&right_sources);
                        if resolved_eq {
                            right_resolved =
                                std::mem::take(&mut right_resolved).merge(resolved.clone());
                        }
                        resolved_eq
                    })
                    .collect::<FxHashMap<_, _>>();

                // Ensure all functions are resolved
                if !state.functions.is_empty() {
                    return plan_err!(
                        "SQL not supported, could not determine sources of following functions: \
                         {:?}",
                        state.functions
                    );
                }

                // Set the pushdown states
                left_pushdown_state =
                    PushdownState { resolved: left_resolved, functions: left_functions };
                right_pushdown_state =
                    PushdownState { resolved: right_resolved, functions: right_functions };
            }

            if has_clickhouse_functions {
                // Ensure no semantic violations in the result of pushdown
                semantic_err(
                    "Join",
                    "SQL unsupported, pushed functions violate sql semantics in join plan.",
                    &function_violations,
                )?;

                // Ensure no plan violations in the result of pushdown
                semantic_err(
                    "Join",
                    "SQL unsupported, join plan violates sql semantics in join expressions.",
                    &plan_violations,
                )?;
            }

            let new_on = join
                .on
                .into_iter()
                .map(|(expr1, expr2)| {
                    Ok((
                        collect_and_transform_function(
                            expr1,
                            lineage_visitor,
                            &mut left_pushdown_state,
                        )?
                        .data,
                        collect_and_transform_function(
                            expr2,
                            lineage_visitor,
                            &mut right_pushdown_state,
                        )?
                        .data,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;

            // Process each side with their respective functions
            let left_plan = analyze_and_transform_plan(
                Arc::unwrap_or_clone(join.left),
                lineage_visitor,
                left_pushdown_state,
            )?;
            let right_plan = analyze_and_transform_plan(
                Arc::unwrap_or_clone(join.right),
                lineage_visitor,
                right_pushdown_state,
            )?;

            let was_transformed = left_plan.transformed || right_plan.transformed;
            let new_join = Join::try_new(
                Arc::new(left_plan.data),
                Arc::new(right_plan.data),
                new_on,
                join_filter,
                join.join_type,
                join.join_constraint,
                join.null_equality,
            )?;
            if was_transformed {
                Ok(Transformed::yes(LogicalPlan::Join(new_join)))
            } else {
                Ok(Transformed::no(LogicalPlan::Join(new_join)))
            }
        }

        // Unions handled specially, can entire union can be wrapped since inputs are homogenous?
        LogicalPlan::Union(union) => {
            let mut was_transformed = false;
            let new_inputs = union
                .inputs
                .into_iter()
                .map(Arc::unwrap_or_clone)
                .map(|input| {
                    handle_input_plan(input, &mut state, lineage_visitor)
                        .inspect(|t| was_transformed |= t.transformed)
                        .map(|t| t.data)
                        .map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let new_plan = LogicalPlan::Union(Union::try_new_with_loose_types(new_inputs)?);
            if was_transformed {
                Ok(Transformed::yes(new_plan))
            } else {
                Ok(Transformed::no(new_plan))
            }
        }

        // TableScan is leaf - wrap functions here if any
        LogicalPlan::TableScan(scan) => {
            if !state.functions.is_empty() {
                let wrapped_plan = wrap_plan_with_functions(LogicalPlan::TableScan(scan), state)?;
                return Ok(Transformed::yes(wrapped_plan));
            }
            Ok(Transformed::no(LogicalPlan::TableScan(scan)))
        }

        // Otherwise analyze top level plan then recurse into inputs
        _ => {
            let expressions = plan.expressions();

            // Update the ResolvedSource of the state with any new clickhouse functions
            state.resolved = state.resolved.merge(
                lineage_visitor.resolve_exprs(
                    expressions
                        .iter()
                        .filter(|e| find_clickhouse_function(e))
                        .inspect(|_| has_clickhouse_functions |= true),
                ),
            );

            // If the column references are resolved, attempt to wrap
            if state.resolved.is_known() {
                let plan_sources = lineage_visitor.resolve_schema(plan.schema());
                if state.resolved.resolves_eq(&plan_sources) {
                    let wrapped_plan = wrap_plan_with_functions(plan, state)?;
                    return Ok(Transformed::yes(wrapped_plan));
                }
            }

            if has_clickhouse_functions {
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

            // Update w/ schema and expression changes
            let aliased_exprs =
                collect_and_transform_exprs(expressions, lineage_visitor, &mut state)?;

            let inputs = plan.inputs();
            let mut new_inputs = Vec::with_capacity(inputs.len());
            let mut was_transformed = false;

            for input in inputs {
                let new_input = handle_input_plan(input.clone(), &mut state, lineage_visitor)?;
                was_transformed |= new_input.transformed;
                new_inputs.push(new_input.data);
            }

            // All ClickHouse functions need to be resolved by this point
            if !state.functions.is_empty() {
                return plan_err!(
                    "SQL not supported, could not determine sources of following functions: {:?}",
                    state.functions.into_iter().collect::<Vec<_>>()
                );
            }

            // Costly, try and match individual LogicalPlans above when possible
            let new_plan = plan.with_new_exprs(aliased_exprs, new_inputs)?;

            if was_transformed || has_clickhouse_functions {
                Ok(Transformed::yes(new_plan))
            } else {
                Ok(Transformed::no(new_plan))
            }
        }
    }
}

type QualifiedField = (Option<TableReference>, Arc<Field>);

// Handle case where no functions found yet, ie Union tends to be top level
fn handle_passthrough_plan(
    plan: &LogicalPlan,
    state: &mut PushdownState,
    visitor: &SourceLineageVistor,
    function_violations: &HashSet<Column>,
    plan_violations: &HashSet<Column>,
) -> Result<bool> {
    if state.functions.is_empty() {
        let mut functions_resolved = ResolvedSource::default();

        for input in plan.inputs() {
            let _ = input
                .apply_expressions(|expr| {
                    use_clickhouse_function_context(expr, |e| {
                        functions_resolved =
                            std::mem::take(&mut functions_resolved).merge(visitor.resolve_expr(e));
                        Ok(TreeNodeRecursion::Stop)
                    })
                    .unwrap();
                    Ok(TreeNodeRecursion::Continue)
                })
                .unwrap();
        }

        // If the column references are resolved, attempt to wrap
        if functions_resolved.is_known() {
            let union_resolved = visitor.resolve_schema(plan.schema());
            if functions_resolved.resolves_eq(&union_resolved) {
                return Ok(true);
            }
        }
    }

    // Ensure no semantic violations in the result of pushdown
    semantic_err(
        "Filter",
        "SQL unsupported, pushed functions violate sql semantics in filter plan.",
        function_violations,
    )?;

    // Ensure no plan violations in the result of pushdown
    semantic_err(
        "Filter",
        "SQL unsupported, filter plan violates sql semantics in filter expressions.",
        plan_violations,
    )?;

    Ok(false)
}

/// Helper function to generically handle a plan's input plan
fn handle_input_plan(
    input: LogicalPlan,
    state: &mut PushdownState,
    visitor: &SourceLineageVistor,
) -> Result<Transformed<LogicalPlan>> {
    let mut input_state = PushdownState::default();
    if !state.functions.is_empty() {
        // Resolve input schema, extract any pushdown functions, update resolved sources
        let input_sources = visitor.resolve_schema(input.schema());
        let relevant_functions = state
            .functions
            .extract_if(|resolved, _| input_sources.resolves_contains(resolved))
            .collect::<FxHashMap<_, _>>();
        let pushdown_source =
            relevant_functions.keys().cloned().reduce(ResolvedSource::merge).unwrap_or_default();
        input_state = PushdownState { functions: relevant_functions, resolved: pushdown_source };
    }

    // Otherwise recurse into the plan's input
    analyze_and_transform_plan(input, visitor, input_state)
}

// TODO: Use ResolvedSources to push scalars
/// Add functions to a plan by wrapping with Extension node that persists through optimization
fn wrap_plan_with_functions(plan: LogicalPlan, state: PushdownState) -> Result<LogicalPlan> {
    #[cfg(feature = "federation")]
    fn return_wrapped_plan(plan: LogicalPlan) -> LogicalPlan { plan }

    #[cfg(not(feature = "federation"))]
    fn return_wrapped_plan(plan: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Extension(Extension { node: Arc::new(ClickHouseFunctionNode::new(plan)) })
    }

    // Collect pushed functions
    let functions = state.functions.into_values().flatten().collect::<Vec<_>>();
    let (func_fields, func_cols) = functions_to_field_and_cols(functions)?;

    // Modify plan, handling inner clickhouse functions
    let plan = plan
        .transform_down_with_subqueries(|node| node.map_expressions(unwrap_clickhouse_function))?
        .data;
    // Remove catalog from table scan
    let plan = plan.transform_up_with_subqueries(strip_table_scan_catalog).unwrap().data;
    // Recompute schema
    let plan = plan.recompute_schema()?;

    Ok(match plan {
        LogicalPlan::SubqueryAlias(alias) => {
            let input = Arc::unwrap_or_clone(alias.input);
            let new_input = wrap_plan_in_projection(input, func_fields, func_cols)?.into();
            let new_alias = SubqueryAlias::try_new(new_input, alias.alias)?;
            return_wrapped_plan(LogicalPlan::SubqueryAlias(new_alias))
        }
        _ => return_wrapped_plan(wrap_plan_in_projection(plan, func_fields, func_cols)?),
    })
}

/// Extract inner functions from `ClickHouse` `UDF` wrappers
fn functions_to_field_and_cols(functions: Vec<Expr>) -> Result<(Vec<QualifiedField>, Vec<Expr>)> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for function in functions {
        let alias = function.schema_name().to_string();
        let (inner_function, data_type) = extract_pushed_function(function)?;
        fields.push((None, Arc::new(Field::new(&alias, data_type, true))));
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

/// Strip table scan of catalog name before passing to extension node
fn strip_table_scan_catalog(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    plan.transform_up_with_subqueries(|node| {
        if let LogicalPlan::TableScan(mut scan) = node {
            scan.table_name = match scan.table_name {
                TableReference::Full { schema, table, .. } => {
                    TableReference::Partial { schema, table }
                }
                t => t,
            };
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        } else {
            Ok(Transformed::no(node))
        }
    })
}

// Analyze state and current plan for any possible semantic violations
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

// Analyze state and current plan for any possible semantic violations
fn check_plan_for_violations(plan: &LogicalPlan) -> HashSet<Column> {
    violates_plan_semantics(plan).into_iter().cloned().collect()
}

/// Check the provided function expr's column references to ensure the plan being analyzed does not
/// disallow further pushed down.
///
/// NOTE: Important! The function must NOT be from the plan passed in but from a plan higher up
fn violates_pushdown_semantics<'a>(
    function: &Expr,
    plan: &'a LogicalPlan,
    visitor: &SourceLineageVistor,
) -> HashSet<&'a Column> {
    // Resolve the function's column references to match against plan expressions
    // NOTE: The resolved sources are not merged to allow granular equality checks
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
fn violates_plan_semantics(plan: &LogicalPlan) -> HashSet<&Column> {
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
                    return plan_err!("Aggregates must be in group by");
                }
                Ok(TreeNodeRecursion::Stop)
            }));

            if let Some(violations) = violations {
                return violations;
            }
        }
    }

    HashSet::new()
}

fn check_function_against_exprs<'a>(
    func: &Expr,
    exprs: &'a [Expr],
    func_column_ids: &HashSet<ColumnId>,
    visitor: &SourceLineageVistor,
    disjoint_required: bool,
) -> Option<HashSet<&'a Column>> {
    for expr in exprs {
        // Little safeguard to allow replacing functions in the plan.
        if expr == func {
            continue;
        }
        let col_refs = expr.column_refs();
        let related_col_ids =
            col_refs.iter().flat_map(|col| visitor.collect_column_ids(col)).collect::<HashSet<_>>();

        // Not sure when this would occur
        if related_col_ids.is_empty() && !disjoint_required {
            continue;
        }

        if related_col_ids.is_disjoint(func_column_ids) != disjoint_required {
            return Some(col_refs);
        }
    }
    None
}

// Helper to emit an error in the situation where a pushdown would violate query semantics
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
            state.resolved = std::mem::take(&mut state.resolved).merge(func_resolved.clone());
            let source_funcs = state.functions.entry(func_resolved).or_default();
            let alias = Expr::Column(Column::from_name(e.schema_name().to_string()));
            if !source_funcs.contains(&e) {
                source_funcs.push(e);
            }
            Ok(Transformed::new(alias, true, TreeNodeRecursion::Jump))
        } else {
            Ok(Transformed::no(e))
        }
    })
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::prelude::*;

    use super::super::plan_node::CLICKHOUSE_FUNCTION_NODE_NAME;
    use crate::prelude::ClickHouseFunctionPushdown;
    use crate::udfs::pushdown::clickhouse_udf_pushdown_udf;

    /// Helper to check if a plan is a `ClickHouse` Extension node
    fn is_clickhouse_extension(plan: &LogicalPlan) -> bool {
        if let LogicalPlan::Extension(ext) = plan {
            ext.node.name() == CLICKHOUSE_FUNCTION_NODE_NAME
        } else {
            false
        }
    }

    /// Helper to find all `ClickHouse` Extension nodes in the tree with their positions
    fn find_wrapped_plans(plan: &LogicalPlan) -> Vec<String> {
        fn traverse(plan: &LogicalPlan, wrapped_plans: &mut Vec<String>, path: &str) {
            if is_clickhouse_extension(plan) {
                wrapped_plans.push(format!("{}: {}", path, plan_type_name(plan)));
            }
            for (i, input) in plan.inputs().iter().enumerate() {
                traverse(input, wrapped_plans, &format!("{path}/input[{i}]"));
            }
        }
        let mut wrapped_plans = Vec::new();
        traverse(plan, &mut wrapped_plans, "root");
        wrapped_plans
    }

    /// Get a simple name for the plan type
    fn plan_type_name(plan: &LogicalPlan) -> &'static str {
        match plan {
            LogicalPlan::Projection(_) => "Projection",
            LogicalPlan::Filter(_) => "Filter",
            LogicalPlan::Aggregate(_) => "Aggregate",
            LogicalPlan::Join(_) => "Join",
            LogicalPlan::TableScan(_) => "TableScan",
            LogicalPlan::Extension(ext) => {
                if ext.node.name() == CLICKHOUSE_FUNCTION_NODE_NAME {
                    "ClickHouseExtension"
                } else {
                    "Extension"
                }
            }
            _ => "Other",
        }
    }

    /// Create a `SessionContext` with registered tables and analyzer for SQL testing
    fn create_test_context(disjoin: bool) -> Result<SessionContext> {
        let ctx = SessionContext::new();
        // Register the ClickHouse pushdown UDF
        ctx.register_udf(clickhouse_udf_pushdown_udf());
        // Register the ClickHouse function pushdown analyzer
        ctx.add_analyzer_rule(Arc::new(ClickHouseFunctionPushdown));
        // Register table1 (col1: Int32, col2: Int32, col3: Utf8)
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Int32, false),
            Field::new("col3", DataType::Utf8, false),
        ]));
        let table1 = Arc::new(EmptyTable::new(Arc::clone(&schema1)));
        let table_name = if disjoin { "table" } else { "table1" };
        drop(ctx.register_table(table_name, table1)?);
        // Register table2 (id: Int32)
        let schema2 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table2 = Arc::new(EmptyTable::new(Arc::clone(&schema2)));
        drop(ctx.register_table("table2", table2)?);
        Ok(ctx)
    }

    #[tokio::test]
    async fn test_simple_projection_with_clickhouse_function() -> Result<()> {
        // Test: SELECT clickhouse(exp(col1 + col2), 'Float64'), col2 * 2, UPPER(col3) FROM table1
        // Expected: Entire plan wrapped because all functions and columns from same table
        let sql =
            "SELECT clickhouse(exp(col1 + col2), 'Float64'), col2 * 2, UPPER(col3) FROM table1";
        let ctx = create_test_context(false)?;
        let analyzed_plan = ctx.sql(sql).await?.into_optimized_plan()?; // Analyzer runs automatically
        SQLOptions::default().verify_plan(&analyzed_plan)?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        assert!(wrapped_plans.is_empty(), "Expected no wrapped plans");
        #[cfg(not(feature = "federation"))]
        {
            assert_eq!(wrapped_plans.len(), 1, "Expected exactly one wrapped plan");
            assert!(wrapped_plans[0].starts_with("root:"), "Expected wrapping at root level");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_with_clickhouse_function() -> Result<()> {
        // Test: SELECT col2, col3 FROM table1 WHERE clickhouse(exp(col1), 'Float64') > 10
        // Expected: Filter wrapped because it contains ClickHouse function, outer projection has no
        // functions
        let sql = "SELECT col2, col3 FROM table1 WHERE clickhouse(exp(col1), 'Float64') > 10";
        let ctx = create_test_context(false)?;
        let analyzed_plan = ctx.sql(sql).await?.into_optimized_plan()?;
        SQLOptions::default().verify_plan(&analyzed_plan)?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);
        #[cfg(feature = "federation")]
        assert!(wrapped_plans.is_empty(), "Expected no wrapped plans");
        #[cfg(not(feature = "federation"))]
        {
            // Should have exactly one wrapped plan at the filter level (since outer projection has
            // no functions)
            assert_eq!(wrapped_plans.len(), 1, "Expected exactly one wrapped plan");
            assert!(
                wrapped_plans[0].contains("input[0]"),
                "Expected wrapping at filter level, not root, since outer projection has no \
                 ClickHouse functions"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate_blocks_pushdown() -> Result<()> {
        // Test: SELECT col2, COUNT(*) FROM table1 WHERE clickhouse(exp(col1), 'Float64') > 5 GROUP
        // BY col2 Expected: Function should be wrapped at a level above the aggregate
        let sql = "SELECT col2, COUNT(*) FROM table1 WHERE clickhouse(exp(col1), 'Float64') > 5 \
                   GROUP BY col2";
        let ctx = create_test_context(false)?;
        let analyzed_plan = ctx.sql(sql).await?.into_optimized_plan()?;
        SQLOptions::default().verify_plan(&analyzed_plan)?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);
        #[cfg(feature = "federation")]
        assert!(wrapped_plans.is_empty(), "Expected no wrapped plans");
        #[cfg(not(feature = "federation"))]
        {
            // Should have exactly one wrapped plan, and it should be at the aggregate input level
            assert_eq!(wrapped_plans.len(), 1, "Expected exactly one wrapped plan");
            // The wrapped plan should be at the input to the aggregate (aggregate blocks pushdown)
            assert!(
                wrapped_plans.iter().any(|w| w.contains("input[0]")),
                "Expected function to be wrapped at aggregate input level due to blocking"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_clickhouse_functions_same_table() -> Result<()> {
        // Test: SELECT clickhouse(exp(col1), 'Float64'), clickhouse(exp(col2), 'Float64') FROM
        // table1 Expected: Both functions use same table, should be wrapped together at
        // root level
        let sql = "SELECT clickhouse(exp(col1), 'Float64') AS f1, clickhouse(exp(col2), \
                   'Float64') AS f2 FROM table1";
        let ctx = create_test_context(false)?;
        let analyzed_plan = ctx.sql(sql).await?.into_optimized_plan()?;
        SQLOptions::default().verify_plan(&analyzed_plan)?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);
        #[cfg(feature = "federation")]
        assert!(wrapped_plans.is_empty(), "Expected no wrapped plans");
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

    #[tokio::test]
    async fn test_no_functions_no_wrapping() -> Result<()> {
        // Test: SELECT col1, col2 FROM table1
        // Expected: No exp functions, so no wrapping should occur
        let sql = "SELECT col1, col2 FROM table1";
        let ctx = create_test_context(false)?;
        let analyzed_plan = ctx.sql(sql).await?.into_optimized_plan()?;
        SQLOptions::default().verify_plan(&analyzed_plan)?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);
        // Should have no wrapped plans
        assert_eq!(
            wrapped_plans.len(),
            0,
            "Expected no wrapped plans when no exp functions present"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_wrapped_disjoint_tables() -> Result<()> {
        // Test: Verify the disjoint check wraps the entire tree, since `table1` and `table2` have
        // the same context during testing.
        let sql = "SELECT t1.col1, clickhouse(exp(t2.id), 'Float64') FROM (SELECT col1 FROM \
                   table1) t1 JOIN (SELECT id from table2) t2 ON t1.col1 = t2.id";
        let ctx = create_test_context(false)?;
        let analyzed_plan = ctx.sql(sql).await?.into_optimized_plan()?;
        SQLOptions::default().verify_plan(&analyzed_plan)?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);
        #[cfg(feature = "federation")]
        assert!(wrapped_plans.is_empty(), "Expected no wrapped plans");
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

    #[tokio::test]
    async fn test_disjoint_tables() -> Result<()> {
        // Test: Verify the disjoint check wraps plan on join side only.
        //       The test tables "test1" and "test2" will use the same context, "test" will not.
        let sql = "
            SELECT t1.col1, clickhouse(exp(t2.id), 'Float64')
            FROM (SELECT col1 FROM `table`) t1
            JOIN (SELECT id from table2) t2 ON t1.col1 = t2.id
            ";

        let ctx = create_test_context(true)?;
        let analyzed_plan = ctx.sql(sql).await?.into_optimized_plan()?;
        SQLOptions::default().verify_plan(&analyzed_plan)?;
        let wrapped_plans = find_wrapped_plans(&analyzed_plan);

        #[cfg(feature = "federation")]
        assert!(wrapped_plans.is_empty(), "Expected no wrapped plans");
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
    #[tokio::test]
    async fn test_complex_agg() -> Result<()> {
        let sql = "SELECT
                clickhouse(pow(t.id, 2), 'Int32') as id_mod,
                COUNT(t.id) as total,
                MAX(clickhouse(exp(t.id), 'Float64')) as max_exp
            FROM table2 t
            GROUP BY id_mod";

        let ctx = create_test_context(false)?;
        let analyzed_plan = ctx.sql(sql).await?.into_optimized_plan()?;
        SQLOptions::default().verify_plan(&analyzed_plan)?;

        #[cfg(feature = "federation")]
        assert!(
            analyzed_plan.display().to_string().contains("Projection"),
            "Expected unchanged plan"
        );

        #[cfg(not(feature = "federation"))]
        {
            let wrapped_plans = find_wrapped_plans(&analyzed_plan);
            assert_eq!(wrapped_plans.len(), 1, "Expected entire plan wrapped");
            assert!(wrapped_plans[0].contains("root"), "Expected function wrapped at root");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_union() -> Result<()> {
        let sql = "
            SELECT col1 as id, clickhouse(exp(col1), 'Float64') as func_id
            FROM table1 WHERE table1.col1 = 1
            UNION ALL
            SELECT id, clickhouse(pow(id, 2), 'Float64') as func_id
            FROM table2 WHERE table2.id = 1
        ";
        let ctx = create_test_context(false)?;
        let analyzed_plan = ctx.sql(sql).await?.into_optimized_plan()?;
        SQLOptions::default().verify_plan(&analyzed_plan)?;

        #[cfg(feature = "federation")]
        assert!(
            analyzed_plan.display().to_string().contains("Union"),
            "Expected entire plan wrapped"
        );

        #[cfg(not(feature = "federation"))]
        {
            let wrapped_plans = find_wrapped_plans(&analyzed_plan);
            assert_eq!(wrapped_plans.len(), 1, "Expected entire plan wrapped");
            assert!(wrapped_plans[0].contains("root"), "Expected function wrapped at root");
        }

        Ok(())
    }
}
