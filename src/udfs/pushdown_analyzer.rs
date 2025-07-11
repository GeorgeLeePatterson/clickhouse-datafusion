use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DataFusionError, Result, qualified_name};
use datafusion::config::ConfigOptions;
#[cfg(not(feature = "federation"))]
use datafusion::logical_expr::Extension;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Aggregate, Expr, LogicalPlan};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;

use super::plan_node::ClickHouseFunctionNode;
use super::pushdown::{CLICKHOUSE_FUNCTION_NODE_NAME, CLICKHOUSE_UDF_ALIASES};

/// Represents a `ClickHouse` function found during analysis
#[derive(Debug, Clone)]
pub struct ClickHouseFunction {
    /// The original clickhouse(...) expression
    pub original_expr:      Expr,
    /// The inner `ClickHouse` function expression (first argument)
    pub inner_expr:         Expr,
    /// The return type parsed from the second argument
    pub return_type:        DataType,
    /// Columns referenced in this function
    pub referenced_columns: HashSet<Column>,
    /// Generated alias for the function output
    pub function_alias:     String,
    /// Tables this function references
    pub source_tables:      HashSet<TableReference>,
    /// The target table this function will be pushed to
    pub target_table:       TableReference,
}

/// `ClickHouse` function pushdown analyzer with proper state tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClickHouseFunctionPushdown;

impl ClickHouseFunctionPushdown {
    pub fn new() -> Self { Self }
}

impl Default for ClickHouseFunctionPushdown {
    fn default() -> Self { Self }
}

impl AnalyzerRule for ClickHouseFunctionPushdown {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Phase 1: Collect functions
        let mut collector = FunctionCollector::new();
        collector.collect(&plan)?;

        if collector.functions.is_empty() {
            return Ok(plan);
        }

        // Phase 2: Transform with state tracking
        let mut transformer = PlanTransformer::new(collector.functions, collector.table_functions);
        transformer.transform(plan)
    }

    fn name(&self) -> &'static str { "clickhouse_function_pushdown" }
}

/// Represents a collected `ClickHouse` function during the analysis phase.
/// This is a temporary representation used while collecting functions from the plan.
/// It gets converted to `ClickHouseFunction` when pushed down to specific tables.
#[derive(Debug, Clone)]
struct CollectedClickHouseFunction {
    alias:         String,   // Generated name
    inner_expr:    Expr,     // The inner expression (first arg of clickhouse())
    return_type:   DataType, // The return type (second arg of clickhouse())
    original_expr: Expr,     // Original clickhouse() function call
}

struct FunctionCollector {
    functions:       HashMap<String, CollectedClickHouseFunction>,
    table_functions: HashMap<String, Vec<String>>, // table -> function aliases
    alias_to_table:  HashMap<String, String>,      // alias -> actual table
    next_id:         usize,
}

impl FunctionCollector {
    fn new() -> Self {
        Self {
            functions:       HashMap::with_capacity(8),
            table_functions: HashMap::with_capacity(4),
            alias_to_table:  HashMap::with_capacity(4),
            next_id:         0,
        }
    }

    fn collect(&mut self, plan: &LogicalPlan) -> Result<()> {
        // First build alias mappings - using apply since we need to visit SubqueryAlias nodes
        let _ = plan.apply(|node| {
            if let LogicalPlan::SubqueryAlias(alias) = node {
                // Find TableScan in the subtree
                let mut table_name = None;
                let _ = alias.input.apply(|child| {
                    if let LogicalPlan::TableScan(scan) = child {
                        table_name = Some(scan.table_name.table().to_string());
                        Ok(TreeNodeRecursion::Stop)
                    } else {
                        Ok(TreeNodeRecursion::Continue)
                    }
                })?;
                if let Some(table) = table_name {
                    drop(self.alias_to_table.insert(alias.alias.table().to_string(), table));
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        // Then collect functions from all expressions in the plan
        let _ = plan.apply(|node| {
            // Process all expressions in this node
            node.apply_expressions(|expr| {
                expr.apply(|e| {
                    if let Expr::ScalarFunction(func) = e {
                        if is_clickhouse_function(func) {
                            self.process_function(func)?;
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        })?;

        Ok(())
    }

    fn process_function(&mut self, func: &ScalarFunction) -> Result<()> {
        if func.args.len() < 2 {
            return Ok(());
        }

        let inner_expr = func.args[0].clone();
        let return_type = match &func.args[1] {
            Expr::Literal(ScalarValue::Utf8(Some(type_str)), _) => DataType::from_str(type_str)
                .map_err(|e| DataFusionError::Plan(format!("Invalid return type: {e}")))?,
            _ => return Ok(()),
        };

        // Generate an alias that matches how DataFusion will display the expression
        let alias = format!("{}", Expr::ScalarFunction(func.clone()));

        // Resolve table for grouping purposes only - this resolves aliases to actual tables
        let table_name = self.resolve_table(&inner_expr);

        // Group by actual table name for pushdown targeting
        if let Some(table) = table_name {
            self.table_functions.entry(table).or_default().push(alias.clone());
        }

        drop(self.functions.insert(alias.clone(), CollectedClickHouseFunction {
            alias,
            inner_expr,
            return_type,
            original_expr: Expr::ScalarFunction(func.clone()),
        }));

        Ok(())
    }

    fn resolve_table(&self, expr: &Expr) -> Option<String> {
        let mut table = None;
        drop(expr.apply(|e| {
            if let Expr::Column(col) = e {
                if let Some(relation) = &col.relation {
                    match relation {
                        TableReference::Full { table: t, .. } => {
                            table = Some(t.to_string());
                            return Ok(TreeNodeRecursion::Stop);
                        }
                        TableReference::Bare { table: t } => {
                            // Resolve alias to actual table name
                            table = Some(
                                self.alias_to_table
                                    .get(t.as_ref())
                                    .map_or_else(|| t.to_string(), String::clone),
                            );
                            return Ok(TreeNodeRecursion::Stop);
                        }
                        TableReference::Partial { .. } => {}
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        }));
        table
    }
}

fn is_clickhouse_function(func: &ScalarFunction) -> bool {
    CLICKHOUSE_UDF_ALIASES.iter().any(|alias| func.name() == *alias)
}

struct PlanTransformer {
    functions:       HashMap<String, CollectedClickHouseFunction>,
    table_functions: HashMap<String, Vec<String>>,
    pushed_aliases:  HashSet<String>,
}

impl PlanTransformer {
    fn new(
        functions: HashMap<String, CollectedClickHouseFunction>,
        table_functions: HashMap<String, Vec<String>>,
    ) -> Self {
        // Collect all function aliases that have been pushed
        let pushed_aliases = functions.keys().cloned().collect();
        Self { functions, table_functions, pushed_aliases }
    }

    #[expect(clippy::too_many_lines)]
    fn transform(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // Transform bottom-up to ensure children are processed first
        plan.transform_up(|node| {
            match node {
                LogicalPlan::Extension(ext) if ext.node.name() == CLICKHOUSE_FUNCTION_NODE_NAME => {
                    Ok(Transformed::no(LogicalPlan::Extension(ext)))
                }
                LogicalPlan::TableScan(scan) => {
                    let table_name = scan.table_name.table();
                    if let Some(func_aliases) = self.table_functions.get(table_name) {
                        let funcs: Vec<_> = func_aliases
                            .iter()
                            .filter_map(|alias| self.functions.get(alias))
                            .map(|f| ClickHouseFunction {
                                function_alias:     f.alias.clone(),
                                inner_expr:         f.inner_expr.clone(),
                                return_type:        f.return_type.clone(),
                                original_expr:      f.original_expr.clone(),
                                referenced_columns: HashSet::default(),
                                source_tables:      HashSet::default(),
                                target_table:       TableReference::bare(table_name),
                            })
                            .collect();

                        let node = ClickHouseFunctionNode::try_new(funcs, scan)?;

                        #[cfg(feature = "federation")]
                        let plan = node.into_projection_plan()?;

                        #[cfg(not(feature = "federation"))]
                        let plan = LogicalPlan::Extension(Extension { node: Arc::new(node) });

                        Ok(Transformed::yes(plan))
                    } else {
                        Ok(Transformed::no(LogicalPlan::TableScan(scan)))
                    }
                }
                LogicalPlan::Projection(projection) => {
                    // Transform expressions using functional approach
                    let schema = projection.input.schema();
                    let transformed_exprs: Result<Vec<_>> = projection
                        .expr
                        .iter()
                        .map(|expr| self.transform_expr(expr.clone(), schema))
                        .collect();
                    let transformed_exprs = transformed_exprs?;

                    let mut needs_transform = transformed_exprs.iter().any(|t| t.transformed);
                    let mut new_exprs: Vec<_> =
                        transformed_exprs.into_iter().map(|t| t.data).collect();

                    // Get columns added by children and check input schema
                    let child_columns = get_child_columns(
                        &LogicalPlan::Projection(projection.clone()),
                        &self.pushed_aliases,
                    );

                    // For projection, we need to add missing columns from child
                    // The qualifiers should match what's in the child's schema
                    for (col_name, _) in child_columns {
                        let already_has = new_exprs.iter().any(|e| match e {
                            Expr::Column(c) => c.name == col_name,
                            Expr::Alias(a) => a.name == col_name,
                            _ => false,
                        });

                        if !already_has {
                            // When adding to projection, use unqualified column
                            // The schema will provide the proper qualification
                            new_exprs.push(Expr::Column(Column::new_unqualified(&col_name)));
                            needs_transform = true;
                        }
                    }

                    if needs_transform {
                        use datafusion::logical_expr::Projection;
                        let new_projection =
                            Projection::try_new(new_exprs, Arc::clone(&projection.input))?;
                        let new_plan = LogicalPlan::Projection(new_projection);
                        let recomputed = new_plan.recompute_schema()?;
                        Ok(Transformed::yes(recomputed))
                    } else {
                        Ok(Transformed::no(LogicalPlan::Projection(projection)))
                    }
                }
                LogicalPlan::SubqueryAlias(alias) => {
                    // Get columns from child
                    let child_columns = get_child_columns(
                        &LogicalPlan::SubqueryAlias(alias.clone()),
                        &self.pushed_aliases,
                    );
                    if child_columns.is_empty() {
                        // No columns added by children
                        Ok(Transformed::no(LogicalPlan::SubqueryAlias(alias)))
                    } else {
                        // We need to update expressions that reference these columns
                        // Build a mapping from old qualified names to new ones
                        let mut qualifier_map = HashMap::new();
                        for (col_name, old_qualifier) in child_columns {
                            let old_qualified = match old_qualifier {
                                Some(q) => qualified_name(Some(&q), &col_name),
                                None => col_name.clone(),
                            };
                            drop(qualifier_map.insert(
                                old_qualified,
                                Expr::Column(Column::new(Some(alias.alias.clone()), &col_name)),
                            ));
                        }
                        // For SubqueryAlias, we just need to recompute schema
                        // The qualifiers will be updated automatically
                        let new_plan = LogicalPlan::SubqueryAlias(alias);
                        let recomputed = new_plan.recompute_schema()?;
                        Ok(Transformed::yes(recomputed))
                    }
                }

                LogicalPlan::Aggregate(agg) => {
                    // For aggregates, we need to use the input schema (not the aggregate's output schema)
                    // when transforming group_by expressions
                    let input_schema = Arc::clone(agg.input.schema());
                    
                    // Transform group by expressions
                    let transformed_group_exprs: Result<Vec<_>> = agg
                        .group_expr
                        .iter()
                        .map(|expr| self.transform_expr(expr.clone(), &input_schema))
                        .collect();
                    let transformed_group_exprs = transformed_group_exprs?;
                    
                    // Transform aggregate expressions
                    let transformed_agg_exprs: Result<Vec<_>> = agg
                        .aggr_expr
                        .iter()
                        .map(|expr| self.transform_expr(expr.clone(), &input_schema))
                        .collect();
                    let transformed_agg_exprs = transformed_agg_exprs?;
                    
                    let group_transformed = transformed_group_exprs.iter().any(|t| t.transformed);
                    let agg_transformed = transformed_agg_exprs.iter().any(|t| t.transformed);
                    
                    if group_transformed || agg_transformed {
                        let new_group_exprs: Vec<_> = transformed_group_exprs.into_iter().map(|t| t.data).collect();
                        let new_agg_exprs: Vec<_> = transformed_agg_exprs.into_iter().map(|t| t.data).collect();
                        
                        let new_agg = LogicalPlan::Aggregate(Aggregate::try_new(
                            Arc::clone(&agg.input),
                            new_group_exprs,
                            new_agg_exprs,
                        )?);
                        let recomputed = new_agg.recompute_schema()?;
                        Ok(Transformed::yes(recomputed))
                    } else {
                        Ok(Transformed::no(LogicalPlan::Aggregate(agg)))
                    }
                }
                _ => {
                    // For other nodes, we need to transform expressions with the proper schema
                    // context
                    let input_schema = Arc::clone(node.schema());
                    let transformer = |expr: Expr| self.transform_expr(expr, &input_schema);

                    // Transform expressions in other nodes
                    let result = node.map_expressions(transformer)?;

                    // ALWAYS recompute schema if something changed
                    if result.transformed {
                        let recomputed = result.data.recompute_schema()?;
                        Ok(Transformed::yes(recomputed))
                    } else {
                        // Check if children added columns
                        let child_columns = get_child_columns(&result.data, &self.pushed_aliases);
                        if child_columns.is_empty() {
                            Ok(result)
                        } else {
                            let recomputed = result.data.recompute_schema()?;
                            Ok(Transformed::yes(recomputed))
                        }
                    }
                }
            }
        })
        .map(|t| t.data)
    }

    fn transform_expr(&self, expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        expr.transform_up(|e| {
            match e {
                Expr::ScalarFunction(func) if is_clickhouse_function(&func) => {
                    // Find matching function
                    for (alias, f) in &self.functions {
                        if functions_match(&func, &f.original_expr) {
                            // Find the qualifier for this column in the current schema
                            let qualifier = schema
                                .iter()
                                .find(|(_, field)| field.name() == alias)
                                .and_then(|(q, _)| q.cloned());
                            return Ok(Transformed::yes(Expr::Column(Column::new(
                                qualifier, alias,
                            ))));
                        }
                    }

                    // Not pushed - unwrap
                    if func.args.is_empty() {
                        Ok(Transformed::no(Expr::ScalarFunction(func)))
                    } else {
                        Ok(Transformed::yes(func.args[0].clone()))
                    }
                }
                _ => Ok(Transformed::no(e)),
            }
        })
    }
}

fn functions_match(f1: &ScalarFunction, expr: &Expr) -> bool {
    if let Expr::ScalarFunction(f2) = expr {
        // Both must be clickhouse functions
        if !is_clickhouse_function(f1) || !is_clickhouse_function(f2) {
            return false;
        }
        
        // Both must have at least 2 args (inner expr + type)
        if f1.args.len() < 2 || f2.args.len() < 2 {
            return false;
        }
        
        // Compare the inner expressions, normalizing qualifiers
        expressions_match_ignoring_qualifiers(&f1.args[0], &f2.args[0])
    } else {
        false
    }
}

/// Compare two expressions ignoring column qualifiers
fn expressions_match_ignoring_qualifiers(e1: &Expr, e2: &Expr) -> bool {
    match (e1, e2) {
        (Expr::Column(c1), Expr::Column(c2)) => c1.name == c2.name,
        (Expr::ScalarFunction(f1), Expr::ScalarFunction(f2)) => {
            f1.name() == f2.name()
                && f1.args.len() == f2.args.len()
                && f1.args.iter().zip(&f2.args).all(|(a1, a2)| expressions_match_ignoring_qualifiers(a1, a2))
        }
        (Expr::Literal(l1, _), Expr::Literal(l2, _)) => l1 == l2,
        (Expr::BinaryExpr(b1), Expr::BinaryExpr(b2)) => {
            b1.op == b2.op
                && expressions_match_ignoring_qualifiers(&b1.left, &b2.left)
                && expressions_match_ignoring_qualifiers(&b1.right, &b2.right)
        }
        _ => e1 == e2,
    }
}

/// Get added columns from children
fn get_child_columns(
    plan: &LogicalPlan,
    pushed_aliases: &HashSet<String>,
) -> HashMap<String, Option<TableReference>> {
    let mut columns = HashMap::new();
    // Collect columns added by children based on their schemas
    for input in plan.inputs() {
        let schema = input.schema();
        for (idx, field) in schema.fields().iter().enumerate() {
            // Check if this field is one of our pushed functions
            if pushed_aliases.contains(field.name()) {
                // Get the qualifier from the schema
                let qualifier = schema.qualified_field(idx).0.cloned();
                drop(columns.insert(field.name().clone(), qualifier));
            }
        }
    }

    columns
}
