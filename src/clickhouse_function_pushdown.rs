use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Extension, Filter, LogicalPlan, Projection};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;

use crate::clickhouse_plan_node::ClickHouseFunctionNode;
use crate::column_lineage::{ColumnLineageVisitor, ResolvedSource};
use crate::udfs::pushdown::CLICKHOUSE_UDF_ALIASES;

/// State for tracking `ClickHouse` functions during pushdown analysis
#[derive(Debug, Clone)]
struct PushdownState {
    /// Functions that couldn't be handled at this level
    pending_functions:  Vec<Expr>,
    /// Pre-computed resolved source for pending functions (avoids recomputation)
    functions_resolved: ResolvedSource,
}

impl Default for PushdownState {
    fn default() -> Self {
        Self { pending_functions: Vec::new(), functions_resolved: ResolvedSource::Unknown }
    }
}

/// Check if expression is a `ClickHouse` pushdown UDF call
fn is_clickhouse_function(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(inner) => is_clickhouse_function(&inner.expr),
        Expr::ScalarFunction(ScalarFunction { func, args, .. }) => {
            // Must match one of the ClickHouse UDF aliases and have exactly 2 arguments
            CLICKHOUSE_UDF_ALIASES.contains(&func.name()) && args.len() == 2
        }
        _ => false,
    }
}

/// Extract inner function and `DataType` from `ClickHouse` UDF call
/// Expected format: `clickhouse(inner_function, 'DataType')`
fn extract_clickhouse_function_parts(expr: &Expr) -> Result<(Expr, DataType)> {
    match expr {
        Expr::Alias(inner) => extract_clickhouse_function_parts(&inner.expr),
        Expr::ScalarFunction(ScalarFunction { args, .. }) if args.len() == 2 => {
            let inner_function = args[0].clone();

            // Second argument should be a string literal containing the DataType
            match &args[1] {
                Expr::Literal(
                    ScalarValue::Utf8(Some(type_str))
                    | ScalarValue::Utf8View(Some(type_str))
                    | ScalarValue::LargeUtf8(Some(type_str)),
                    _,
                ) => {
                    let data_type = DataType::from_str(type_str).map_err(|e| {
                        datafusion::common::DataFusionError::Plan(format!(
                            "Invalid ClickHouse function DataType '{type_str}': {e}"
                        ))
                    })?;
                    Ok((inner_function, data_type))
                }
                _ => Err(datafusion::common::DataFusionError::Plan(
                    "ClickHouse function second argument must be a string literal DataType"
                        .to_string(),
                )),
            }
        }
        _ => Err(datafusion::common::DataFusionError::Plan(
            "Not a valid ClickHouse function call".to_string(),
        )),
    }
}

/// Add functions to a plan by wrapping with Extension node that persists through optimization
fn add_functions_to_plan(plan: LogicalPlan, state: PushdownState) -> Result<LogicalPlan> {
    let functions = state.pending_functions;
    let resolved = state.functions_resolved;

    eprintln!("\n🎯 ADD_FUNCTIONS_TO_PLAN CALLED:");
    eprintln!("   📦 Plan:\n{}", plan.display_indent());
    eprintln!("   🔧 Functions count: {}", functions.len());
    for (i, func) in functions.iter().enumerate() {
        eprintln!("   🔧 Function[{i}]: {func}");
    }
    eprintln!("   📊 Plan schema columns: {:?}", plan.schema().columns());
    eprintln!("   📥 Functions resolved: {resolved:?}");

    // Extract inner functions from ClickHouse UDF wrappers
    let mut inner_functions = Vec::new();
    for func in functions {
        let (inner_func, _data_type) = extract_clickhouse_function_parts(&func)?;
        eprintln!("   🎯 Extracted inner function: {inner_func}");
        inner_functions.push(inner_func);
    }

    // Create our ClickHouse extension node with ONLY the inner functions
    let clickhouse_node = ClickHouseFunctionNode::try_new(plan, inner_functions)?;

    // Wrap in Extension so DataFusion recognizes it as a UserDefinedLogicalNode
    let extension_node = Extension { node: Arc::new(clickhouse_node) };

    eprintln!("   ✅ Created Extension node with ClickHouseFunctionNode");

    Ok(LogicalPlan::Extension(extension_node))
}

/// Clean, simplified `ClickHouse` function pushdown analyzer
#[derive(Debug, Clone, Copy)]
pub struct ClickHouseFunctionPushdown;

impl AnalyzerRule for ClickHouseFunctionPushdown {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        // Create lineage visitor and build column lineage for the plan
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // Start with empty pushdown state
        analyze_and_transform_plan(plan, &lineage_visitor, PushdownState::default()).map(|t| t.data)
    }

    fn name(&self) -> &'static str { "clickhouse_function_pushdown" }
}

/// Analyze and transform a plan. Functions flow DOWN - only recurse if current level cannot handle
/// them.
#[expect(clippy::too_many_lines)]
fn analyze_and_transform_plan(
    plan: LogicalPlan,
    lineage_visitor: &ColumnLineageVisitor,
    mut pushdown_state: PushdownState,
) -> Result<Transformed<LogicalPlan>> {
    eprintln!("\n🔍 ANALYZE_AND_TRANSFORM_PLAN:");
    eprintln!("   📋 Plan:\n{}", plan.display_indent());
    eprintln!("   📥 Pending functions: {}", pushdown_state.pending_functions.len());
    for (i, func) in pushdown_state.pending_functions.iter().enumerate() {
        eprintln!("   📥 Pending[{i}]: {func}");
    }
    eprintln!("   📥 Functions resolved: {:?}", pushdown_state.functions_resolved);

    match plan {
        // Extract ClickHouse functions from projections
        LogicalPlan::Projection(Projection { ref expr, ref input, .. }) => {
            eprintln!("   🎯 PROJECTION ANALYSIS:");
            eprintln!("   📝 Projection expressions: {}", expr.len());
            for (i, expression) in expr.iter().enumerate() {
                eprintln!("   📝 Expr[{i}]: {expression}");
            }

            // Extract ClickHouse functions from this projection
            let mut extracted_functions = Vec::with_capacity(expr.len());
            let mut extracted_exprs = Vec::with_capacity(expr.len());

            for expression in expr {
                let _ = expression.apply(|e| {
                    if is_clickhouse_function(e) {
                        eprintln!("   ✨ FOUND CLICKHOUSE FUNCTION: {e}");
                        extracted_functions.push(e);
                        return Ok(TreeNodeRecursion::Jump);
                    }

                    extracted_exprs.push(e);
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }

            eprintln!("   🔧 Extracted functions from projection: {}", extracted_functions.len());
            for (i, func) in extracted_functions.iter().enumerate() {
                eprintln!("   🔧 Extracted[{i}]: {func}");
            }

            eprintln!("   🧐 DEPENDENCY ANALYSIS:");

            // Update resolved source for newly extracted functions only (optimization)
            let extracted_resolved = extracted_functions
                .iter()
                .flat_map(|f| f.column_refs())
                .map(|col| lineage_visitor.resolve_to_source(col))
                .fold(ResolvedSource::Unknown, ResolvedSource::merge);

            pushdown_state.functions_resolved =
                std::mem::take(&mut pushdown_state.functions_resolved).merge(extracted_resolved);

            // If the pushdown state indicates no functions, return early
            if matches!(pushdown_state.functions_resolved, ResolvedSource::Unknown) {
                return Ok(Transformed::no(plan));
            }

            // Generate the resolved sources for the extracted expressions
            let exprs_resolved = extracted_exprs
                .iter()
                .flat_map(|f| f.column_refs())
                .map(|col| lineage_visitor.resolve_to_source(col))
                .fold(ResolvedSource::Unknown, ResolvedSource::merge);

            // Check if this level can handle the merged resolved sources
            let disjoin_result = pushdown_state.functions_resolved.disjoin_tables(&exprs_resolved);
            eprintln!("   🔍 Disjoin result: {disjoin_result:?}");
            eprintln!("   🔍 Should wrap? {}", disjoin_result.is_empty());

            // If function dependencies satisfied by this projection's expressions, wrap HERE
            if disjoin_result.is_empty() {
                eprintln!("   ✅ WRAPPING AT PROJECTION LEVEL!");
                let wrapped_plan = add_functions_to_plan(
                    LogicalPlan::Projection(Projection::try_new(expr.clone(), Arc::clone(input))?),
                    pushdown_state,
                )?;
                return Ok(Transformed::yes(wrapped_plan));
            }

            eprintln!("   ⏭️  RECURSING DEEPER - dependencies not satisfied");

            // Dependencies not satisfied - recurse deeper with functions
            eprintln!("   🔽 RECURSING INTO INPUT PLAN");
            let updated_plan = analyze_and_transform_plan(
                Arc::unwrap_or_clone(Arc::clone(input)),
                lineage_visitor,
                pushdown_state,
            )?;
            eprintln!("   🔼 RETURNED FROM RECURSION - transformed: {}", updated_plan.transformed);

            // Reconstruct projection with transformed input if needed
            if updated_plan.transformed {
                let new_projection =
                    Projection::try_new(expr.clone(), Arc::new(updated_plan.data))?;
                Ok(Transformed::yes(LogicalPlan::Projection(new_projection)))
            } else {
                Ok(Transformed::no(plan))
            }
        }

        // Handle filters with ClickHouse functions or blocking logic
        LogicalPlan::Filter(Filter { predicate, input, .. }) => {
            // Extract ClickHouse functions from this projection
            let mut extracted_functions = Vec::new();
            let mut extracted_exprs = Vec::new();

            let _ = predicate.apply(|e| {
                if is_clickhouse_function(e) {
                    eprintln!("   ✨ FOUND CLICKHOUSE FUNCTION: {e}");
                    extracted_functions.push(e);
                    return Ok(TreeNodeRecursion::Jump);
                }

                extracted_exprs.push(e);
                Ok(TreeNodeRecursion::Continue)
            })?;

            // Update resolved source for newly extracted functions only (optimization)
            let extracted_resolved = extracted_functions
                .iter()
                .flat_map(|f| f.column_refs())
                .map(|col| lineage_visitor.resolve_to_source(col))
                .fold(ResolvedSource::Unknown, ResolvedSource::merge);

            pushdown_state.functions_resolved =
                std::mem::take(&mut pushdown_state.functions_resolved).merge(extracted_resolved);

            // If the pushdown state indicates no functions, return early
            if matches!(pushdown_state.functions_resolved, ResolvedSource::Unknown) {
                return Filter::try_new(predicate, input)
                    .map(LogicalPlan::Filter)
                    .map(Transformed::no);
            }

            let filter_resolved = extracted_exprs
                .iter()
                .flat_map(|f| f.column_refs())
                .map(|col| lineage_visitor.resolve_to_source(col))
                .fold(ResolvedSource::Unknown, ResolvedSource::merge);

            // Check if pending functions should be wrapped at this filter level

            // If filter uses same tables as functions, wrap HERE
            if pushdown_state.functions_resolved.disjoin_tables(&filter_resolved).is_empty() {
                let wrapped_plan = add_functions_to_plan(
                    LogicalPlan::Filter(Filter::try_new(predicate.clone(), input)?),
                    pushdown_state,
                )?;
                return Ok(Transformed::yes(wrapped_plan));
            }

            // Dependencies not satisfied - recurse deeper
            let new_plan = analyze_and_transform_plan(
                Arc::unwrap_or_clone(input),
                lineage_visitor,
                pushdown_state,
            )?;

            // Reconstruct filter with transformed input if needed
            if new_plan.transformed {
                let new_filter = Filter::try_new(predicate.clone(), Arc::new(new_plan.data))?;
                Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
            } else {
                Filter::try_new(predicate, Arc::new(new_plan.data))
                    .map(LogicalPlan::Filter)
                    .map(Transformed::no)
            }
        }

        // Aggregates block functions from crossing boundaries but can inspect their inputs
        LogicalPlan::Aggregate(agg) => {
            // If we have pending functions from above, wrap the aggregate with them
            if !pushdown_state.pending_functions.is_empty() {
                let wrapped_plan =
                    add_functions_to_plan(LogicalPlan::Aggregate(agg), pushdown_state)?;
                return Ok(Transformed::yes(wrapped_plan));
            }

            // No pending functions from above, but recurse into input to handle internal
            // functions
            let new_plan = analyze_and_transform_plan(
                agg.input.as_ref().clone(),
                lineage_visitor,
                PushdownState::default(),
            )?;

            // If input was transformed, reconstruct aggregate
            if new_plan.transformed {
                let mut all_exprs = agg.group_expr.clone();
                all_exprs.extend(agg.aggr_expr.clone());
                let new_agg =
                    LogicalPlan::Aggregate(agg).with_new_exprs(all_exprs, vec![new_plan.data])?;
                Ok(Transformed::yes(new_agg))
            } else {
                Ok(Transformed::no(LogicalPlan::Aggregate(agg)))
            }
        }

        // TableScan is leaf - wrap functions here if any
        LogicalPlan::TableScan(scan) => {
            if !pushdown_state.pending_functions.is_empty() {
                let wrapped_plan =
                    add_functions_to_plan(LogicalPlan::TableScan(scan), pushdown_state)?;
                return Ok(Transformed::yes(wrapped_plan));
            }

            Ok(Transformed::no(LogicalPlan::TableScan(scan)))
        }

        // For joins - route functions to appropriate sides based on column ownership
        LogicalPlan::Join(join) => {
            if !pushdown_state.pending_functions.is_empty() {
                // Route functions based on column ownership
                let left_schema = join.left.schema();
                let right_schema = join.right.schema();

                let mut left_functions = Vec::new();
                let mut right_functions = Vec::new();
                let mut invalid_functions = Vec::new();

                for func in pushdown_state.pending_functions {
                    let func_resolved = func
                        .column_refs()
                        .iter()
                        .map(|col| lineage_visitor.resolve_to_source(col))
                        .fold(ResolvedSource::Unknown, ResolvedSource::merge);

                    // Get resolved sources for left and right schemas
                    let left_columns_resolved = left_schema
                        .columns()
                        .iter()
                        .map(|col| lineage_visitor.resolve_to_source(col))
                        .fold(ResolvedSource::Unknown, ResolvedSource::merge);

                    let right_columns_resolved = right_schema
                        .columns()
                        .iter()
                        .map(|col| lineage_visitor.resolve_to_source(col))
                        .fold(ResolvedSource::Unknown, ResolvedSource::merge);

                    // Check if function's tables are subset of left or right
                    if func_resolved.disjoin_tables(&left_columns_resolved).is_empty() {
                        left_functions.push(func);
                    } else if func_resolved.disjoin_tables(&right_columns_resolved).is_empty() {
                        right_functions.push(func);
                    } else {
                        // Function references tables not in either side - invalid query
                        invalid_functions.push(func);
                    }
                }

                // Process each side with their respective functions
                let left_state = PushdownState {
                    functions_resolved: left_functions
                        .iter()
                        .flat_map(|f| f.column_refs())
                        .map(|col| lineage_visitor.resolve_to_source(col))
                        .fold(ResolvedSource::Unknown, ResolvedSource::merge),
                    pending_functions:  left_functions,
                };
                let new_left_plan = analyze_and_transform_plan(
                    join.left.as_ref().clone(),
                    lineage_visitor,
                    left_state,
                )?;
                let right_state = PushdownState {
                    functions_resolved: right_functions
                        .iter()
                        .flat_map(|f| f.column_refs())
                        .map(|col| lineage_visitor.resolve_to_source(col))
                        .fold(ResolvedSource::Unknown, ResolvedSource::merge),
                    pending_functions:  right_functions,
                };
                let new_right_plan = analyze_and_transform_plan(
                    join.right.as_ref().clone(),
                    lineage_visitor,
                    right_state,
                )?;

                if new_left_plan.transformed || new_right_plan.transformed {
                    let new_join = LogicalPlan::Join(join)
                        .with_new_exprs(vec![], vec![new_left_plan.data, new_right_plan.data])?;
                    return Ok(Transformed::yes(new_join));
                }

                // Invalid functions become pending (will cause error at root level)
                return Ok(Transformed::no(LogicalPlan::Join(join)));
            }

            // No functions to route - just process children normally
            let left_plan = analyze_and_transform_plan(
                join.left.as_ref().clone(),
                lineage_visitor,
                PushdownState::default(),
            )?;
            let right_plan = analyze_and_transform_plan(
                join.right.as_ref().clone(),
                lineage_visitor,
                PushdownState::default(),
            )?;

            if left_plan.transformed || right_plan.transformed {
                let new_join = LogicalPlan::Join(join)
                    .with_new_exprs(vec![], vec![left_plan.data, right_plan.data])?;
                return Ok(Transformed::yes(new_join));
            }

            // Invalid functions become pending (will cause error at root level)
            Ok(Transformed::no(LogicalPlan::Join(join)))
        }

        // For all other plan types, wrap functions here if any
        _ => {
            if !pushdown_state.pending_functions.is_empty() {
                let wrapped_plan = add_functions_to_plan(plan, pushdown_state)?;
                return Ok(Transformed::yes(wrapped_plan));
            }

            Ok(Transformed::no(plan))
        }
    }
}
