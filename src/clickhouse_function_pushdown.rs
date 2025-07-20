use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Extension, Filter, LogicalPlan, Projection};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::Expr;

use crate::clickhouse_plan_node::ClickHouseFunctionNode;
use crate::column_lineage::{ColumnLineageVisitor, ResolvedSource};
use crate::udfs::pushdown::CLICKHOUSE_UDF_ALIASES;

/// Function name to match for pushdown (configurable for testing)
const TARGET_FUNCTION_NAME: &str = "exp";

/// State for tracking `ClickHouse` functions during pushdown analysis
#[derive(Debug, Clone, Default)]
struct PushdownState {
    /// Functions that couldn't be handled at this level
    pending_functions: Vec<Expr>,
    /// Whether any transformation occurred
    transformed:       bool,
}

/// Check if expression is a target function call
fn is_clickhouse_function(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarFunction(ScalarFunction { func, .. }) => func.name() == TARGET_FUNCTION_NAME,
        _ => false,
    }
}

/// Add functions to a plan by wrapping with Extension node that persists through optimization
fn add_functions_to_plan(plan: LogicalPlan, functions: Vec<Expr>) -> Result<LogicalPlan> {
    eprintln!("\n🎯 ADD_FUNCTIONS_TO_PLAN CALLED:");
    eprintln!("   📦 Plan:\n{}", plan.display_indent());
    eprintln!("   🔧 Functions count: {}", functions.len());
    for (i, func) in functions.iter().enumerate() {
        eprintln!("   🔧 Function[{}]: {}", i, func);
    }
    eprintln!("   📊 Plan schema columns: {:?}", plan.schema().columns());

    // Create our ClickHouse extension node
    let clickhouse_node = ClickHouseFunctionNode::try_new(plan, functions)?;
    
    // Wrap in Extension so DataFusion recognizes it as a UserDefinedLogicalNode
    let extension_node = Extension {
        node: Arc::new(clickhouse_node),
    };

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

        // Start with no pending functions - extract from top level
        let (transformed_plan, state) =
            analyze_and_transform_plan(plan, &lineage_visitor, Vec::new())?;

        // If there are still pending functions at the root, add them here
        if state.pending_functions.is_empty() {
            Ok(transformed_plan)
        } else {
            Ok(add_functions_to_plan(transformed_plan, state.pending_functions)?)
        }
    }

    fn name(&self) -> &'static str { "clickhouse_function_pushdown" }
}

/// Analyze and transform a plan. Functions flow DOWN - only recurse if current level cannot handle
/// them.
#[expect(clippy::too_many_lines)]
fn analyze_and_transform_plan(
    plan: LogicalPlan,
    lineage_visitor: &ColumnLineageVisitor,
    mut pending_functions: Vec<Expr>,
) -> Result<(LogicalPlan, PushdownState)> {
    eprintln!("\n🔍 ANALYZE_AND_TRANSFORM_PLAN:");
    eprintln!("   📋 Plan:\n{}", plan.display_indent());
    eprintln!("   📥 Pending functions: {}", pending_functions.len());
    for (i, func) in pending_functions.iter().enumerate() {
        eprintln!("   📥 Pending[{}]: {}", i, func);
    }

    match plan {
        // Extract ClickHouse functions from projections
        LogicalPlan::Projection(Projection { ref expr, ref input, .. }) => {
            eprintln!("   🎯 PROJECTION ANALYSIS:");
            eprintln!("   📝 Projection expressions: {}", expr.len());
            for (i, expression) in expr.iter().enumerate() {
                eprintln!("   📝 Expr[{}]: {}", i, expression);
            }

            // Extract ClickHouse functions from this projection
            let mut extracted_functions = Vec::new();

            for expression in expr {
                let _ = expression.apply(|e| {
                    if is_clickhouse_function(e) {
                        eprintln!("   ✨ FOUND CLICKHOUSE FUNCTION: {}", e);
                        extracted_functions.push(e.clone());
                    }
                    Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
                })?;
            }

            eprintln!("   🔧 Extracted functions from projection: {}", extracted_functions.len());
            for (i, func) in extracted_functions.iter().enumerate() {
                eprintln!("   🔧 Extracted[{}]: {}", i, func);
            }

            // Combine with pending functions from above
            pending_functions.extend(extracted_functions);
            eprintln!("   📦 Total pending functions after extension: {}", pending_functions.len());

            // If we have functions, check if this level can handle them
            if !pending_functions.is_empty() {
                eprintln!("   🧐 DEPENDENCY ANALYSIS:");
                
                let functions_resolved = pending_functions
                    .iter()
                    .flat_map(|f| f.column_refs())
                    .map(|col| lineage_visitor.resolve_to_source(col))
                    .fold(ResolvedSource::Unknown, ResolvedSource::merge);

                let projection_columns_resolved = expr
                    .iter()
                    .flat_map(|e| e.column_refs())
                    .map(|col| lineage_visitor.resolve_to_source(col))
                    .fold(ResolvedSource::Unknown, ResolvedSource::merge);

                eprintln!("   🔍 Functions resolved: {:?}", functions_resolved);
                eprintln!("   🔍 Projection resolved: {:?}", projection_columns_resolved);
                
                let disjoin_result = functions_resolved.disjoin_tables(&projection_columns_resolved);
                eprintln!("   🔍 Disjoin result: {:?}", disjoin_result);
                eprintln!("   🔍 Should wrap? {}", disjoin_result.is_empty());

                // If function dependencies satisfied by this projection's expressions, wrap HERE
                if disjoin_result.is_empty() {
                    eprintln!("   ✅ WRAPPING AT PROJECTION LEVEL!");
                    let wrapped_plan = add_functions_to_plan(
                        LogicalPlan::Projection(Projection::try_new(
                            expr.clone(),
                            Arc::clone(input),
                        )?),
                        pending_functions,
                    )?;
                    return Ok((wrapped_plan, PushdownState {
                        pending_functions: Vec::new(),
                        transformed:       true,
                    }));
                }
            } else {
                eprintln!("   ⏭️  RECURSING DEEPER - dependencies not satisfied");
            }

            // Dependencies not satisfied - recurse deeper with functions
            eprintln!("   🔽 RECURSING INTO INPUT PLAN");
            let (transformed_input, child_state) =
                analyze_and_transform_plan((**input).clone(), lineage_visitor, pending_functions)?;
            eprintln!("   🔼 RETURNED FROM RECURSION - transformed: {}", child_state.transformed);

            // Reconstruct projection with transformed input if needed
            if child_state.transformed {
                let new_projection =
                    Projection::try_new(expr.clone(), Arc::new(transformed_input))?;
                Ok((LogicalPlan::Projection(new_projection), child_state))
            } else {
                Ok((plan, child_state))
            }
        }

        // Handle filters with ClickHouse functions or blocking logic
        LogicalPlan::Filter(Filter { ref predicate, ref input, .. }) => {
            // Extract any ClickHouse functions from filter predicate
            let mut extracted_functions = Vec::new();
            let _ = predicate.apply(|e| {
                if is_clickhouse_function(e) {
                    extracted_functions.push(e.clone());
                }
                Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
            })?;

            // Combine with pending functions from above
            pending_functions.extend(extracted_functions);

            // Check if pending functions should be wrapped at this filter level
            if !pending_functions.is_empty() {
                let filter_columns = predicate.column_refs();
                let filter_resolved = filter_columns
                    .iter()
                    .map(|col| lineage_visitor.resolve_to_source(col))
                    .fold(ResolvedSource::Unknown, ResolvedSource::merge);

                let functions_resolved = pending_functions
                    .iter()
                    .flat_map(|f| f.column_refs())
                    .map(|col| lineage_visitor.resolve_to_source(col))
                    .fold(ResolvedSource::Unknown, ResolvedSource::merge);

                // If filter uses same tables as functions, wrap HERE
                if functions_resolved.disjoin_tables(&filter_resolved).is_empty() {
                    let wrapped_plan = add_functions_to_plan(
                        LogicalPlan::Filter(Filter::try_new(predicate.clone(), Arc::clone(input))?),
                        pending_functions,
                    )?;
                    return Ok((wrapped_plan, PushdownState {
                        pending_functions: Vec::new(),
                        transformed:       true,
                    }));
                }
            }

            // Dependencies not satisfied - recurse deeper
            let (transformed_input, child_state) =
                analyze_and_transform_plan((**input).clone(), lineage_visitor, pending_functions)?;

            // Reconstruct filter with transformed input if needed
            if child_state.transformed {
                let new_filter = Filter::try_new(predicate.clone(), Arc::new(transformed_input))?;
                Ok((LogicalPlan::Filter(new_filter), child_state))
            } else {
                Ok((plan, child_state))
            }
        }

        // Aggregates block functions from crossing boundaries but can inspect their inputs
        LogicalPlan::Aggregate(agg) => {
            // If we have pending functions from above, wrap the aggregate with them
            if pending_functions.is_empty() {
                let wrapped_plan =
                    add_functions_to_plan(LogicalPlan::Aggregate(agg), pending_functions)?;
                return Ok((wrapped_plan, PushdownState {
                    pending_functions: Vec::new(),
                    transformed:       true,
                }));
            }

            // No pending functions from above, but recurse into input to handle internal
            // functions
            let (transformed_input, child_state) = analyze_and_transform_plan(
                agg.input.as_ref().clone(),
                lineage_visitor,
                Vec::new(),
            )?;

            // If input was transformed, reconstruct aggregate
            if child_state.transformed {
                let mut all_exprs = agg.group_expr.clone();
                all_exprs.extend(agg.aggr_expr.clone());
                let new_agg = LogicalPlan::Aggregate(agg)
                    .with_new_exprs(all_exprs, vec![transformed_input])?;
                Ok((new_agg, child_state))
            } else {
                Ok((LogicalPlan::Aggregate(agg), child_state))
            }
        }

        // TableScan is leaf - wrap functions here if any
        LogicalPlan::TableScan(scan) => {
            if !pending_functions.is_empty() {
                let wrapped_plan =
                    add_functions_to_plan(LogicalPlan::TableScan(scan), pending_functions)?;
                return Ok((wrapped_plan, PushdownState {
                    pending_functions: Vec::new(),
                    transformed:       true,
                }));
            }

            Ok((LogicalPlan::TableScan(scan), PushdownState::default()))
        }

        // For joins - route functions to appropriate sides based on column ownership
        LogicalPlan::Join(join) => {
            if !pending_functions.is_empty() {
                // Route functions based on column ownership
                let left_schema = join.left.schema();
                let right_schema = join.right.schema();

                let mut left_functions = Vec::new();
                let mut right_functions = Vec::new();
                let mut invalid_functions = Vec::new();

                for func in pending_functions {
                    let func_columns = func.column_refs();
                    let func_resolved = func_columns
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
                    let func_tables_left_disjoint =
                        func_resolved.disjoin_tables(&left_columns_resolved);
                    let func_tables_right_disjoint =
                        func_resolved.disjoin_tables(&right_columns_resolved);

                    if func_tables_left_disjoint.is_empty() {
                        left_functions.push(func);
                    } else if func_tables_right_disjoint.is_empty() {
                        right_functions.push(func);
                    } else {
                        // Function references tables not in either side - invalid query
                        invalid_functions.push(func);
                    }
                }

                // Process each side with their respective functions
                let (left_plan, left_state) = analyze_and_transform_plan(
                    join.left.as_ref().clone(),
                    lineage_visitor,
                    left_functions,
                )?;
                let (right_plan, right_state) = analyze_and_transform_plan(
                    join.right.as_ref().clone(),
                    lineage_visitor,
                    right_functions,
                )?;

                let new_join =
                    LogicalPlan::Join(join).with_new_exprs(vec![], vec![left_plan, right_plan])?;

                // Invalid functions become pending (will cause error at root level)
                return Ok((new_join, PushdownState {
                    pending_functions: invalid_functions,
                    transformed:       left_state.transformed || right_state.transformed,
                }));
            }

            // No functions to route - just process children normally
            let (left_plan, left_state) = analyze_and_transform_plan(
                join.left.as_ref().clone(),
                lineage_visitor,
                Vec::new(),
            )?;
            let (right_plan, right_state) = analyze_and_transform_plan(
                join.right.as_ref().clone(),
                lineage_visitor,
                Vec::new(),
            )?;

            let new_join =
                LogicalPlan::Join(join).with_new_exprs(vec![], vec![left_plan, right_plan])?;
            Ok((new_join, PushdownState {
                pending_functions: Vec::new(),
                transformed:       left_state.transformed || right_state.transformed,
            }))
        }

        // For all other plan types, wrap functions here if any
        _ => {
            if !pending_functions.is_empty() {
                let wrapped_plan = add_functions_to_plan(plan, pending_functions)?;
                return Ok((wrapped_plan, PushdownState {
                    pending_functions: Vec::new(),
                    transformed:       true,
                }));
            }

            Ok((plan, PushdownState::default()))
        }
    }
}
