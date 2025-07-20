use std::collections::{HashMap, HashSet};

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Column, Result, TableReference};
use datafusion::logical_expr::{Expr, LogicalPlan};

use crate::column_lineage::{ColumnLineageVisitor, PlanContext, ResolvedSource, UsageContext};

#[derive(Debug, Clone)]
pub enum PredicateType {
    Filter(Vec<Expr>), // Wrap these expressions in a Filter plan
}

/// Tracks the lowest depth a function can be pushed down to
/// Similar to ScanResult from datafusion-federation
#[derive(Debug, Clone)]
pub enum PushdownDepth {
    /// No blocking plans found - can push to TableScan (depth 0)
    TableScan(TableReference),
    /// Lowest blocking plan found - can push to this depth
    Blocked(PlanContext),
    /// Multiple conflicting depths - cannot determine safe pushdown
    Ambiguous,
}

impl PushdownDepth {
    /// Merge with another PushdownDepth result
    pub fn merge(&mut self, other: Self) {
        match (&self, &other) {
            (PushdownDepth::Ambiguous, _) | (_, PushdownDepth::Ambiguous) => {
                *self = PushdownDepth::Ambiguous;
            }
            (PushdownDepth::TableScan(table1), PushdownDepth::TableScan(table2)) => {
                // Both are TableScan - they should be the same table
                if table1 != table2 {
                    *self = PushdownDepth::Ambiguous;
                }
                // Otherwise keep current
            }
            (PushdownDepth::TableScan(_), PushdownDepth::Blocked(_)) => {
                // Other is more restrictive (blocked), take it
                *self = other;
            }
            (PushdownDepth::Blocked(_), PushdownDepth::TableScan(_)) => {
                // Self is more restrictive (blocked), keep it
            }
            (PushdownDepth::Blocked(ctx1), PushdownDepth::Blocked(ctx2)) => {
                // Take the one with LOWER depth (further from TableScan = more restrictive)
                if ctx1.depth < ctx2.depth {
                    // ctx1 is further from TableScan, keep it (more restrictive)
                } else if ctx2.depth < ctx1.depth {
                    // ctx2 is further from TableScan, take it (more restrictive)
                    *self = other;
                } else if ctx1.node_id != ctx2.node_id {
                    // Same depth but different plans - ambiguous
                    *self = PushdownDepth::Ambiguous;
                }
                // If same depth and same plan, keep current
            }
        }
    }

    /// Check if this depth allows pushdown
    pub fn is_pushable(&self) -> bool { !matches!(self, PushdownDepth::Ambiguous) }

    /// Get the target table for pushdown
    pub fn get_target_table(&self) -> Option<&TableReference> {
        match self {
            PushdownDepth::TableScan(table) => Some(table),
            PushdownDepth::Blocked(_) => None, // Blocked pushdown doesn't target TableScan
            PushdownDepth::Ambiguous => None,
        }
    }

    /// Get the target plan context for pushdown (for blocked cases)
    pub fn get_target_context(&self) -> Option<&PlanContext> {
        match self {
            PushdownDepth::TableScan(_) => None, // TableScan doesn't need PlanContext
            PushdownDepth::Blocked(ctx) => Some(ctx),
            PushdownDepth::Ambiguous => None,
        }
    }
}

/// Analyze usage contexts to determine lowest safe pushdown depth
fn analyze_pushdown_depth(
    usage_contexts: &[UsageContext],
    function_depth: usize,
    table: &TableReference,
) -> PushdownDepth {
    // Start with TableScan as default (can push all the way down)
    let mut result = PushdownDepth::TableScan(table.clone());

    // Consider usage contexts at the same depth or between function and TableScan
    // - Same depth: function is used within the same plan node (e.g., SUM(exp(value)))
    // - Lower depth: usage contexts between function and TableScan
    // - Higher depth: contexts above function (should not block)
    let relevant_contexts: Vec<_> = usage_contexts
        .iter()
        .filter(|ctx| ctx.plan_context.depth <= function_depth)  // Same depth or below
        .collect();

    for usage in relevant_contexts {
        // Check if this usage represents a semantic blocking condition
        if is_semantically_blocking(usage) {
            // This usage blocks pushdown - can't push below this depth
            // The blocking plan is at usage.plan_context.depth
            let blocking_depth = PushdownDepth::Blocked(usage.plan_context.clone());
            result.merge(blocking_depth);
        }
    }

    result
}

/// Check if an expression usage context represents a semantic blocking condition
/// This is different from just checking expression types - we need to consider
/// whether the usage actually creates a semantic dependency that blocks pushdown
fn is_semantically_blocking(usage: &UsageContext) -> bool {
    // The key insight: we need to check if the usage context represents a plan
    // that would change semantics if we pushed a function past it

    // Look at the plan context to determine if it's a blocking plan type
    let plan_details = &usage.plan_context.node_details;

    // These plan types create semantic dependencies:
    if plan_details.contains("Aggregate") || plan_details.contains("Window") {
        return true;
    }

    // For other plan types, we need to be more nuanced
    // Filter plans generally don't block function pushdown unless there's a specific conflict
    // Join plans don't block pushdown within the same table
    // Projection plans don't block pushdown

    // For now, let's be conservative only for truly blocking plan types
    // and allow pushdown for simple column usage in filters, joins, etc.
    match &usage.original_expr {
        // These expression types in any context are truly blocking:
        Expr::AggregateFunction(_) | Expr::WindowFunction(_) | Expr::ScalarSubquery(_) => true,

        // These are generally safe to push past:
        Expr::Column(_)
        | Expr::BinaryExpr(_)
        | Expr::InList(_)
        | Expr::Between(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Cast(_)
        | Expr::TryCast(_)
        | Expr::ScalarFunction(_)
        | Expr::Case(_)
        | Expr::Negative(_)
        | Expr::Not(_)
        | Expr::Alias(_)
        | Expr::Literal(..) => false,

        // For unknown expression types, be conservative
        _ => true,
    }
}

// TableScan-specific actions (things being pushed DOWN)
#[derive(Default, Debug, Clone)]
pub struct TableScanActions {
    pub functions_to_add:  Vec<Expr>,
    pub predicates_to_add: Vec<PredicateType>, // Knows HOW to wrap predicates
    pub columns_to_remove: Vec<String>,        // For Replace case
}

// Other plans actions (things being modified)
#[derive(Default, Debug, Clone)]
pub struct PlanActions {
    pub expressions_to_replace: HashMap<Expr, Expr>,
    pub predicates_to_remove:   Vec<Expr>,
}

// Separate action structures - no more confusing combined struct!
// TableScan contexts get TableScanActions, other contexts get PlanActions

#[derive(Debug, Clone)]
pub enum ReplacementAction {
    /// Replace the column with function alias
    Replace(String),
    /// Keep the column and augment with function alias
    Augment,
}

#[derive(Debug, Clone)]
pub struct ClickHouseFunctionCall {
    /// The full clickhouse function expression
    pub function_expr:   Expr,
    /// Semantic source relation - the bridge for pushdown logic
    pub source_relation: ResolvedSource,
}

#[derive(Debug, Clone)]
pub struct ClickHouseFunctionCollector {
    /// Function names to collect (needed for `f_up`)
    target_functions:      HashSet<String>,
    /// Functions grouped by source table for `TableScan` pushdown
    functions_by_table:    HashMap<TableReference, Vec<ClickHouseFunctionCall>>,
    /// Fast lookup: was this exact expression collected?
    collected_expressions: HashMap<Expr, ClickHouseFunctionCall>,
    /// Column lineage for resolution (REQUIRED)
    column_lineage:        ColumnLineageVisitor,

    /// Separate action maps for different plan types
    pub(crate) table_scan_actions: HashMap<PlanContext, TableScanActions>,
    pub(crate) plan_actions:       HashMap<PlanContext, PlanActions>,
}

impl ClickHouseFunctionCollector {
    pub fn new(target_functions: Vec<String>, lineage: ColumnLineageVisitor) -> Self {
        Self {
            target_functions:      target_functions.into_iter().collect(),
            functions_by_table:    HashMap::new(),
            collected_expressions: HashMap::new(),
            column_lineage:        lineage,
            table_scan_actions:    HashMap::new(),
            plan_actions:          HashMap::new(),
        }
    }

    /// Get all collected clickhouse functions
    pub fn get_all_functions(&self) -> Vec<&ClickHouseFunctionCall> {
        self.collected_expressions.values().collect()
    }

    /// Get functions that reference a specific source table
    pub fn get_functions_for_table(&self, table: &TableReference) -> &[ClickHouseFunctionCall] {
        self.functions_by_table.get(table).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get all source tables that have functions referencing them
    pub fn get_all_source_tables(&self) -> Vec<&TableReference> {
        self.functions_by_table.keys().collect()
    }

    /// Check if a specific expression was collected (for transformation)
    pub(crate) fn was_function_collected(&self, expr: &Expr) -> Option<&ClickHouseFunctionCall> {
        self.collected_expressions.get(expr)
    }


    /// Get the action maps for plan transformation
    pub fn get_table_scan_actions(&self) -> &HashMap<PlanContext, TableScanActions> {
        &self.table_scan_actions
    }

    pub fn get_plan_actions(&self) -> &HashMap<PlanContext, PlanActions> { &self.plan_actions }
}

impl<'n> TreeNodeVisitor<'n> for ClickHouseFunctionCollector {
    type Node = LogicalPlan;

    // f_down removed - we'll use entry API in f_up to initialize on-demand

    #[expect(clippy::too_many_lines)]
    fn f_up(&mut self, node: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        // Get current plan context with depth (only called once per node now)
        let current_plan_context = self.column_lineage.plan_ctx_manager.next(node);

        // Plan context generated correctly for each node

        // Initialize action map entry on-demand based on plan type
        if matches!(node, LogicalPlan::TableScan(_)) {
            self.table_scan_actions
                .entry(current_plan_context.clone())
                .or_insert_with(TableScanActions::default);
        } else {
            self.plan_actions
                .entry(current_plan_context.clone())
                .or_insert_with(PlanActions::default);
        }

        node.apply_expressions(|expression| {
            expression.apply(|e| {
                if let Expr::ScalarFunction(func) = e {
                    let func_name = func.func.name();
                    if self.target_functions.contains(func_name) {
                        // Use built-in column_refs method
                        let column_refs = e.column_refs();

                        // Resolve columns and merge in single pass, building normalization map
                        let mut merged_source: ResolvedSource = ResolvedSource::Unknown;
                        let mut all_usage_contexts: Vec<UsageContext> = Vec::new();
                        let mut replace_map: HashMap<Column, Column> = HashMap::new();

                        for col in &column_refs {
                            let (usage_contexts, resolved) =
                                self.column_lineage.resolve_to_source(col);
                            all_usage_contexts.extend(usage_contexts);
                            merged_source = merged_source.merge(resolved.clone());

                            // Build replacement map for normalization
                            match &resolved {
                                ResolvedSource::Exact { table, column } => {
                                    let table_col =
                                        Column::new(Some(table.clone()), column.clone());
                                    replace_map.insert((*col).clone(), table_col);
                                }
                                ResolvedSource::Simple { table, columns } => {
                                    // Skip functions with Simple cases that have multiple columns
                                    // This would require reconstructing the original expression
                                    if columns.len() > 1 {
                                        return Ok(TreeNodeRecursion::Continue);
                                    }
                                    // Single column case - this is a simplified version
                                    // In reality, we'd need to find the merge point where multiple
                                    // columns became one. For
                                    // now, we treat it like an Exact case.
                                    if let Some(first_col) = columns.first() {
                                        let table_col =
                                            Column::new(Some(table.clone()), first_col.clone());
                                        replace_map.insert((*col).clone(), table_col);
                                    }
                                }
                                _ => {
                                    // For compound/scalar/unknown cases, keep the original column
                                }
                            }
                        }

                        // Check if we can handle this source relation
                        if matches!(
                            merged_source,
                            ResolvedSource::Compound(_) | ResolvedSource::Unknown
                        ) {
                            return Ok(TreeNodeRecursion::Continue);
                        }

                        // Analyze pushdown depth using usage contexts
                        let function_depth = current_plan_context.depth;
                        
                        // Get the table reference from the merged source (collect once, reuse)
                        let tables = merged_source.collect_tables();
                        let Some(table) = tables.first() else {
                            return Ok(TreeNodeRecursion::Continue);
                        };
                        let pushdown_depth =
                            analyze_pushdown_depth(&all_usage_contexts, function_depth, table);

                        // Skip if pushdown is not possible
                        if !pushdown_depth.is_pushable() {
                            return Ok(TreeNodeRecursion::Continue);
                        }

                        // Normalize function expression to TableScan level (single transformation)
                        let normalized_function = e
                            .clone()
                            .transform_up(|expr| {
                                Ok(if let Expr::Column(c) = &expr {
                                    match replace_map.get(c) {
                                        Some(new_c) => {
                                            Transformed::yes(Expr::Column(new_c.clone()))
                                        }
                                        None => Transformed::no(expr),
                                    }
                                } else {
                                    Transformed::no(expr)
                                })
                            })?
                            .data;

                        // Generate function alias inline from normalized function
                        let function_alias = normalized_function.schema_name().to_string();

                        // Add the normalized function to the target depth context
                        match &pushdown_depth {
                            PushdownDepth::TableScan(target_table) => {
                                // Can push to TableScan - add to table_scan_actions
                                if let Some(table_scan_actions) = self
                                    .table_scan_actions
                                    .iter_mut()
                                    .find(|(ctx, _)| &ctx.table == target_table)
                                    .map(|(_, actions)| actions)
                                {
                                    // Add the aliased function to functions_to_add
                                    let aliased_function =
                                        normalized_function.clone().alias(function_alias.clone());
                                    table_scan_actions.functions_to_add.push(aliased_function);
                                }

                                // Add expression replacement for the original function
                                // Replace the original function with a column reference to the alias
                                let replacement_col =
                                    Column::new(Some(target_table.clone()), function_alias);
                                let replacement_expr = Expr::Column(replacement_col);

                                // Only add replacement to current plan context where function exists
                                if let Some(plan_actions) = self.plan_actions.get_mut(&current_plan_context) {
                                    plan_actions.expressions_to_replace.insert(e.clone(), replacement_expr);
                                }
                            }
                            PushdownDepth::Blocked(_target_context) => {
                                // Blocked at specific depth - this means we cannot push past this
                                // plan For now, we'll
                                // conservatively skip these functions entirely
                                // In the future, we could implement plan-level transformations
                                // that add the function at the blocking plan level

                                // Skip this function for now - cannot push down safely
                                return Ok(TreeNodeRecursion::Continue);
                            }
                            PushdownDepth::Ambiguous => {
                                // Should not reach here due to is_pushable() check above
                            }
                        }
                        let function_call = ClickHouseFunctionCall {
                            function_expr:   e.clone(),
                            source_relation: merged_source.clone(),
                        };

                        // Store in both structures
                        self.collected_expressions.insert(e.clone(), function_call.clone());

                        // Group by tables (reuse the tables we already collected)
                        let mut seen_tables = HashSet::new();
                        for table in &tables {
                            if seen_tables.insert(table.clone()) {
                                self.functions_by_table
                                    .entry(table.clone())
                                    .or_default()
                                    .push(function_call.clone());
                            }
                        }
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use datafusion::common::tree_node::TreeNode;
    use datafusion::prelude::*;

    use super::*;
    use crate::column_lineage::{ColumnLineageVisitor, PlanContextManager};

    #[tokio::test]
    async fn test_function_collector_comprehensive_verification() -> Result<()> {
        let ctx = SessionContext::new();

        // Create test tables
        drop(ctx.sql("CREATE TABLE users (id INT, name VARCHAR, score DECIMAL)").await?);
        drop(ctx.sql("CREATE TABLE orders (id INT, user_id INT, amount DECIMAL)").await?);

        // Query with functions that should be collected
        let sql = "
            SELECT
                u.name,
                exp(u.score) as exp_score,
                sqrt(o.amount) as sqrt_amount
            FROM users u
            JOIN orders o ON u.id = o.user_id
            WHERE u.score > 0
        ";

        let plan = ctx.sql(sql).await?.into_unoptimized_plan();

        // STEP 1: Collect column lineage
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // STEP 2: Collect functions
        let mut collector = ClickHouseFunctionCollector::new(
            vec!["exp".to_string(), "sqrt".to_string()],
            lineage_visitor,
        );
        let _ = plan.visit(&mut collector)?;

        // STEP 3: Walk the plan post-order and verify what's expected at each node
        // IMPORTANT: Use transform_up to match how the actual analyzer will work
        // Create a new PlanContextManager for post-order traversal (it's stateless except during
        // traversal)
        let mut plan_ctx_manager = PlanContextManager::new();

        let _ = plan.transform_up(|node| {
            let plan_context = plan_ctx_manager.next(&node);

            // Verify expected context based on plan type
            match &node {
                LogicalPlan::TableScan(table_scan) => {
                    assert_eq!(plan_context.depth, 0, "TableScan should be at depth 0");
                    assert_eq!(
                        plan_context.table, table_scan.table_name,
                        "TableScan context should match table name"
                    );

                    // Check if this table has functions to add
                    let table_scan_actions = collector.get_table_scan_actions();

                    if let Some(actions) = table_scan_actions.get(&plan_context) {
                        let table_name_str = table_scan.table_name.to_string();
                        if table_name_str == "users" {
                            // exp(u.score) should be pushable to TableScan level
                            // Using u.score in the WHERE clause doesn't create a semantic
                            // dependency that blocks pushdown - the
                            // filter operates on the original column value
                            assert_eq!(
                                actions.functions_to_add.len(),
                                1,
                                "Users table should have 1 function (exp)"
                            );

                            // Verify the function is properly aliased
                            let func_expr = &actions.functions_to_add[0];
                            if let Expr::Alias(alias) = func_expr {
                                assert_eq!(
                                    alias.name, "exp(users.score)",
                                    "Function should be aliased as 'exp(users.score)'"
                                );
                            } else {
                                panic!("Function should be aliased");
                            }
                        } else if table_name_str == "orders" {
                            assert_eq!(
                                actions.functions_to_add.len(),
                                1,
                                "Orders table should have 1 function (sqrt)"
                            );

                            // Verify the function is properly aliased
                            let func_expr = &actions.functions_to_add[0];
                            if let Expr::Alias(alias) = func_expr {
                                assert_eq!(
                                    alias.name, "sqrt(orders.amount)",
                                    "Function should be aliased as 'sqrt(orders.amount)'"
                                );
                            } else {
                                panic!("Function should be aliased");
                            }
                        }
                    }
                }
                LogicalPlan::Filter(_filter) => {
                    assert!(plan_context.depth > 0, "Filter should be above TableScan");

                    // In this specific query, the filter is "u.score > 0"
                    // This should have NO expression replacements because:
                    // 1. The filter uses the original column u.score
                    // 2. The exp(u.score) function is pushed down to TableScan level
                    // 3. Filter operates on original data, not transformed data
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Filter should have no expression replacements - it operates on \
                             original columns"
                        );
                        assert_eq!(
                            actions.predicates_to_remove.len(),
                            0,
                            "Filter should have no predicates to remove"
                        );
                    }
                }
                LogicalPlan::Join(_join) => {
                    assert!(plan_context.depth > 0, "Join should be above TableScan");

                    // In this specific query, the join is "u.id = o.user_id"
                    // This should have NO expression replacements because:
                    // 1. Join condition uses original columns (u.id, o.user_id)
                    // 2. Functions are pushed down to TableScan level
                    // 3. Join operates on original data, not transformed data
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Join should have no expression replacements - it operates on \
                             original columns"
                        );
                        assert_eq!(
                            actions.predicates_to_remove.len(),
                            0,
                            "Join should have no predicates to remove"
                        );
                    }
                }
                LogicalPlan::Projection(_projection) => {
                    assert!(plan_context.depth > 0, "Projection should be above TableScan");

                    // In this specific query, the projection contains:
                    // - u.name (no transformation)
                    // - exp(u.score) as exp_score (SHOULD be replaced with column reference)
                    // - sqrt(o.amount) as sqrt_amount (SHOULD be replaced with column reference)
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        // Should have exactly 2 replacements: exp and sqrt functions
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            2,
                            "Projection should have exactly 2 expression replacements"
                        );
                        assert_eq!(
                            actions.predicates_to_remove.len(),
                            0,
                            "Projection should have no predicates to remove"
                        );

                        // Verify each replacement is correct
                        let mut found_exp = false;
                        let mut found_sqrt = false;

                        for (original, replacement) in &actions.expressions_to_replace {
                            match original {
                                Expr::ScalarFunction(func) => {
                                    let func_name = func.func.name();
                                    match func_name {
                                        "exp" => {
                                            assert!(
                                                !found_exp,
                                                "Should only have one exp replacement"
                                            );
                                            found_exp = true;
                                            // Should be replaced with users.exp(users.score)
                                            if let Expr::Column(col) = replacement {
                                                assert_eq!(
                                                    col.relation,
                                                    Some(TableReference::bare("users")),
                                                    "exp replacement should reference users table"
                                                );
                                                assert_eq!(
                                                    col.name, "exp(users.score)",
                                                    "exp replacement should have correct alias"
                                                );
                                            } else {
                                                panic!(
                                                    "exp replacement should be a column reference"
                                                );
                                            }
                                        }
                                        "sqrt" => {
                                            assert!(
                                                !found_sqrt,
                                                "Should only have one sqrt replacement"
                                            );
                                            found_sqrt = true;
                                            // Should be replaced with orders.sqrt(orders.amount)
                                            if let Expr::Column(col) = replacement {
                                                assert_eq!(
                                                    col.relation,
                                                    Some(TableReference::bare("orders")),
                                                    "sqrt replacement should reference orders \
                                                     table"
                                                );
                                                assert_eq!(
                                                    col.name, "sqrt(orders.amount)",
                                                    "sqrt replacement should have correct alias"
                                                );
                                            } else {
                                                panic!(
                                                    "sqrt replacement should be a column reference"
                                                );
                                            }
                                        }
                                        _ => panic!(
                                            "Unexpected function in replacements: {}",
                                            func_name
                                        ),
                                    }
                                }
                                _ => panic!(
                                    "Only scalar functions should be replaced, found: {:?}",
                                    original
                                ),
                            }
                        }

                        assert!(found_exp, "Should have found exp function replacement");
                        assert!(found_sqrt, "Should have found sqrt function replacement");
                    } else {
                        // This is the key issue - the projection exists in the plan actions map at
                        // a different depth than what our new
                        // PlanContextManager is calculating

                        // The plan actions map shows the projection at depth 5, but we're
                        // calculating depth 4 This means the collection
                        // phase and verification phase are using different depth calculations

                        // The depth mismatch is expected due to different traversal methods (visit
                        // vs transform_up) Find the projection in the plan
                        // actions map by matching node_id and node_details
                        let plan_actions = collector.get_plan_actions();
                        for (ctx, actions) in plan_actions.iter() {
                            if ctx.node_id == plan_context.node_id
                                && ctx.node_details == plan_context.node_details
                            {
                                // Found the projection - verify it has the expected actions
                                assert_eq!(
                                    actions.expressions_to_replace.len(),
                                    2,
                                    "Projection should have exactly 2 expression replacements"
                                );

                                // Verify the replacements are correct
                                let mut found_exp = false;
                                let mut found_sqrt = false;

                                for (original, _replacement) in &actions.expressions_to_replace {
                                    if let Expr::ScalarFunction(func) = original {
                                        match func.func.name() {
                                            "exp" => found_exp = true,
                                            "sqrt" => found_sqrt = true,
                                            _ => {}
                                        }
                                    }
                                }

                                assert!(found_exp, "Should have found exp function replacement");
                                assert!(found_sqrt, "Should have found sqrt function replacement");

                                return Ok(Transformed::no(node));
                            }
                        }

                        panic!("Projection not found in plan actions map");
                    }
                }
                LogicalPlan::SubqueryAlias(_subquery_alias) => {
                    assert!(plan_context.depth > 0, "SubqueryAlias should be above TableScan");

                    // In this specific query, SubqueryAlias provides table aliases "u" and "o"
                    // This should have NO expression replacements because:
                    // 1. SubqueryAlias only changes the table name/alias
                    // 2. Functions are pushed down to TableScan level
                    // 3. SubqueryAlias is a pure aliasing operation
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        // TODO: Remove
                        eprintln!(
                            "SubqueryAlias actions:\nActions = {actions:?}\nPlanContext = \
                             {plan_context:?}"
                        );

                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "SubqueryAlias should have no expression replacements - it's just \
                             aliasing"
                        );
                        assert_eq!(
                            actions.predicates_to_remove.len(),
                            0,
                            "SubqueryAlias should have no predicates to remove"
                        );
                    }
                }
                _ => {
                    // Debug: print what plan type we found
                    println!(
                        "Found plan type: {:?} at depth {}",
                        std::mem::discriminant(&node),
                        plan_context.depth
                    );

                    // For now, let's handle unexpected plan types gracefully
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        // Most plan types should have no actions for this simple query
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Unexpected plan should have no expression replacements"
                        );
                        assert_eq!(
                            actions.predicates_to_remove.len(),
                            0,
                            "Unexpected plan should have no predicates to remove"
                        );
                    }
                }
            }

            Ok(Transformed::no(node))
        })?;

        // STEP 4: Verify overall collection results
        let all_functions = collector.get_all_functions();
        assert_eq!(all_functions.len(), 2, "Should find 2 functions (exp, sqrt)");

        // Verify functions by table
        let users_table = TableReference::bare("users");
        let orders_table = TableReference::bare("orders");

        let user_functions = collector.get_functions_for_table(&users_table);
        let order_functions = collector.get_functions_for_table(&orders_table);

        assert_eq!(user_functions.len(), 1, "Users should have 1 function (exp)");
        assert_eq!(order_functions.len(), 1, "Orders should have 1 function (sqrt)");

        // Verify source tables
        let source_tables = collector.get_all_source_tables();
        assert_eq!(source_tables.len(), 2, "Should have 2 source tables");

        Ok(())
    }

    #[tokio::test]
    async fn test_function_collector_aggregation_blocking() -> Result<()> {
        let ctx = SessionContext::new();

        // Create test table
        drop(ctx.sql("CREATE TABLE metrics (id INT, value DECIMAL, category VARCHAR)").await?);

        // Query with function inside aggregation - should NOT be pushable
        let sql = "
            SELECT
                category,
                SUM(exp(value)) as sum_exp_value
            FROM metrics
            GROUP BY category
        ";

        let plan = ctx.sql(sql).await?.into_unoptimized_plan();

        // Collect column lineage
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // Collect functions
        let mut collector =
            ClickHouseFunctionCollector::new(vec!["exp".to_string()], lineage_visitor);
        let _ = plan.visit(&mut collector)?;

        // Walk the plan and verify expectations
        let mut plan_ctx_manager = PlanContextManager::new();

        let _ = plan.transform_up(|node| {
            let plan_context = plan_ctx_manager.next(&node);

            match &node {
                LogicalPlan::TableScan(table_scan) => {
                    assert_eq!(plan_context.depth, 0);
                    assert_eq!(table_scan.table_name, TableReference::bare("metrics"));

                    // Should have NO functions because exp(value) is inside SUM() aggregation
                    // This creates a semantic dependency that blocks pushdown
                    let table_scan_actions = collector.get_table_scan_actions();
                    if let Some(actions) = table_scan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.functions_to_add.len(),
                            0,
                            "Functions inside aggregations should not be pushable"
                        );
                    }
                }
                LogicalPlan::Aggregate(_aggregate) => {
                    assert!(plan_context.depth > 0);

                    // Aggregate should have no actions - the function is part of the aggregate
                    // expression
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Aggregate should have no expression replacements"
                        );
                    }
                }
                _ => {
                    // Other plan types should have no actions
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Non-aggregate plans should have no expression replacements"
                        );
                    }
                }
            }

            Ok(Transformed::no(node))
        })?;

        // Verify no functions were collected (due to blocking)
        let all_functions = collector.get_all_functions();
        assert_eq!(all_functions.len(), 0, "Functions inside aggregations should be blocked");

        Ok(())
    }

    #[tokio::test]
    async fn test_function_collector_window_function_blocking() -> Result<()> {
        let ctx = SessionContext::new();

        // Create test table
        drop(ctx.sql("CREATE TABLE sales (id INT, amount DECIMAL, region VARCHAR)").await?);

        // Query with function inside window function - should NOT be pushable
        let sql = "
            SELECT
                region,
                amount,
                SUM(sqrt(amount)) OVER (PARTITION BY region) as running_sqrt_sum
            FROM sales
        ";

        let plan = ctx.sql(sql).await?.into_unoptimized_plan();

        // Collect column lineage
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // Collect functions
        let mut collector =
            ClickHouseFunctionCollector::new(vec!["sqrt".to_string()], lineage_visitor);
        let _ = plan.visit(&mut collector)?;

        // Walk the plan and verify expectations
        let mut plan_ctx_manager = PlanContextManager::new();

        let _ = plan.transform_up(|node| {
            let plan_context = plan_ctx_manager.next(&node);

            match &node {
                LogicalPlan::TableScan(table_scan) => {
                    assert_eq!(plan_context.depth, 0);
                    assert_eq!(table_scan.table_name, TableReference::bare("sales"));

                    // Should have NO functions because sqrt(amount) is inside window function
                    // This creates a semantic dependency that blocks pushdown
                    let table_scan_actions = collector.get_table_scan_actions();
                    if let Some(actions) = table_scan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.functions_to_add.len(),
                            0,
                            "Functions inside window functions should not be pushable"
                        );
                    }
                }
                LogicalPlan::Window(_window) => {
                    assert!(plan_context.depth > 0);

                    // Window should have no actions - the function is part of the window expression
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Window should have no expression replacements"
                        );
                    }
                }
                _ => {
                    // Other plan types should have no actions
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Non-window plans should have no expression replacements"
                        );
                    }
                }
            }

            Ok(Transformed::no(node))
        })?;

        // Verify no functions were collected (due to blocking)
        let all_functions = collector.get_all_functions();
        assert_eq!(all_functions.len(), 0, "Functions inside window functions should be blocked");

        Ok(())
    }

    #[tokio::test]
    async fn test_function_collector_complex_multi_table_join() -> Result<()> {
        let ctx = SessionContext::new();

        // Create test tables
        drop(ctx.sql("CREATE TABLE customers (id INT, name VARCHAR, credit_score DECIMAL)").await?);
        drop(
            ctx.sql(
                "CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL, date_placed DATE)",
            )
            .await?,
        );
        drop(ctx.sql("CREATE TABLE products (id INT, name VARCHAR, price DECIMAL)").await?);

        // Complex query with multiple joins and functions
        let sql = "
            SELECT
                c.name,
                log(c.credit_score) as log_credit,
                sqrt(o.amount) as sqrt_amount,
                exp(p.price) as exp_price
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            JOIN products p ON o.id = p.id
            WHERE c.credit_score > 500
            AND o.amount > 100
            AND p.price < 1000
        ";

        let plan = ctx.sql(sql).await?.into_unoptimized_plan();

        // Collect column lineage
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // Collect functions
        let mut collector = ClickHouseFunctionCollector::new(
            vec!["log".to_string(), "sqrt".to_string(), "exp".to_string()],
            lineage_visitor,
        );
        let _ = plan.visit(&mut collector)?;

        // Walk the plan and verify expectations
        let mut plan_ctx_manager = PlanContextManager::new();

        let _ = plan.transform_up(|node| {
            let plan_context = plan_ctx_manager.next(&node);

            match &node {
                LogicalPlan::TableScan(table_scan) => {
                    assert_eq!(plan_context.depth, 0);

                    let table_scan_actions = collector.get_table_scan_actions();
                    if let Some(actions) = table_scan_actions.get(&plan_context) {
                        let table_name = table_scan.table_name.to_string();
                        match table_name.as_str() {
                            "customers" => {
                                // Should have log(credit_score) pushed down
                                assert_eq!(
                                    actions.functions_to_add.len(),
                                    1,
                                    "Customers table should have 1 function (log)"
                                );

                                let func_expr = &actions.functions_to_add[0];
                                if let Expr::Alias(alias) = func_expr {
                                    assert_eq!(
                                        alias.name, "log(customers.credit_score)",
                                        "Function should be aliased correctly"
                                    );
                                }
                            }
                            "orders" => {
                                // Should have sqrt(amount) pushed down
                                assert_eq!(
                                    actions.functions_to_add.len(),
                                    1,
                                    "Orders table should have 1 function (sqrt)"
                                );

                                let func_expr = &actions.functions_to_add[0];
                                if let Expr::Alias(alias) = func_expr {
                                    assert_eq!(
                                        alias.name, "sqrt(orders.amount)",
                                        "Function should be aliased correctly"
                                    );
                                }
                            }
                            "products" => {
                                // Should have exp(price) pushed down
                                assert_eq!(
                                    actions.functions_to_add.len(),
                                    1,
                                    "Products table should have 1 function (exp)"
                                );

                                let func_expr = &actions.functions_to_add[0];
                                if let Expr::Alias(alias) = func_expr {
                                    assert_eq!(
                                        alias.name, "exp(products.price)",
                                        "Function should be aliased correctly"
                                    );
                                }
                            }
                            _ => panic!("Unexpected table: {}", table_name),
                        }
                    }
                }
                LogicalPlan::Filter(_filter) => {
                    // Filters should have no expression replacements
                    // They operate on original columns, not transformed data
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Filters should have no expression replacements"
                        );
                    }
                }
                LogicalPlan::Join(_join) => {
                    // Joins should have no expression replacements
                    // They operate on original columns, not transformed data
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Joins should have no expression replacements"
                        );
                    }
                }
                LogicalPlan::Projection(_projection) => {
                    // Projection should have exactly 3 replacements
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            3,
                            "Projection should have 3 expression replacements"
                        );

                        // Verify each replacement
                        let mut found_log = false;
                        let mut found_sqrt = false;
                        let mut found_exp = false;

                        for (original, replacement) in &actions.expressions_to_replace {
                            if let Expr::ScalarFunction(func) = original {
                                match func.func.name() {
                                    "log" => {
                                        assert!(!found_log, "Should only have one log replacement");
                                        found_log = true;
                                        if let Expr::Column(col) = replacement {
                                            assert_eq!(
                                                col.relation,
                                                Some(TableReference::bare("customers"))
                                            );
                                            assert_eq!(col.name, "log(customers.credit_score)");
                                        }
                                    }
                                    "sqrt" => {
                                        assert!(
                                            !found_sqrt,
                                            "Should only have one sqrt replacement"
                                        );
                                        found_sqrt = true;
                                        if let Expr::Column(col) = replacement {
                                            assert_eq!(
                                                col.relation,
                                                Some(TableReference::bare("orders"))
                                            );
                                            assert_eq!(col.name, "sqrt(orders.amount)");
                                        }
                                    }
                                    "exp" => {
                                        assert!(!found_exp, "Should only have one exp replacement");
                                        found_exp = true;
                                        if let Expr::Column(col) = replacement {
                                            assert_eq!(
                                                col.relation,
                                                Some(TableReference::bare("products"))
                                            );
                                            assert_eq!(col.name, "exp(products.price)");
                                        }
                                    }
                                    _ => panic!("Unexpected function: {}", func.func.name()),
                                }
                            }
                        }

                        assert!(
                            found_log && found_sqrt && found_exp,
                            "Should have found all three function replacements"
                        );
                    }
                }
                _ => {
                    // Other plan types should have no actions
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Other plan types should have no expression replacements"
                        );
                    }
                }
            }

            Ok(Transformed::no(node))
        })?;

        // Verify all functions were collected
        let all_functions = collector.get_all_functions();
        assert_eq!(all_functions.len(), 3, "Should have collected 3 functions");

        // Verify functions by table
        let customers_functions =
            collector.get_functions_for_table(&TableReference::bare("customers"));
        let orders_functions = collector.get_functions_for_table(&TableReference::bare("orders"));
        let products_functions =
            collector.get_functions_for_table(&TableReference::bare("products"));

        assert_eq!(customers_functions.len(), 1, "Customers should have 1 function");
        assert_eq!(orders_functions.len(), 1, "Orders should have 1 function");
        assert_eq!(products_functions.len(), 1, "Products should have 1 function");

        Ok(())
    }

    #[tokio::test]
    async fn test_function_collector_complex_subquery_and_multilevel_joins() -> Result<()> {
        let ctx = SessionContext::new();

        // Create complex table structure
        drop(
            ctx.sql(
                "CREATE TABLE customers (id INT, name VARCHAR, region VARCHAR, credit_score \
                 DECIMAL)",
            )
            .await?,
        );
        drop(
            ctx.sql(
                "CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL, date_placed DATE, \
                 status VARCHAR)",
            )
            .await?,
        );
        drop(
            ctx.sql(
                "CREATE TABLE products (id INT, name VARCHAR, price DECIMAL, category VARCHAR)",
            )
            .await?,
        );
        drop(
            ctx.sql(
                "CREATE TABLE order_items (order_id INT, product_id INT, quantity INT, discount \
                 DECIMAL)",
            )
            .await?,
        );

        // Complex query with multiple levels of joins, subqueries, and functions at different
        // levels
        let sql = "
            SELECT
                customer_summary.customer_name,
                customer_summary.avg_credit_score,
                log(customer_summary.total_spent) as log_total_spent,
                product_stats.product_name,
                sqrt(product_stats.avg_price) as sqrt_avg_price,
                exp(order_details.amount) as exp_final_amount
            FROM
                (SELECT
                    c.name as customer_name,
                    c.region,
                    avg(c.credit_score) as avg_credit_score,
                    sum(o.amount) as total_spent,
                    c.id as customer_id
                 FROM customers c
                 JOIN orders o ON c.id = o.customer_id
                 WHERE c.credit_score > 500
                 GROUP BY c.id, c.name, c.region
                ) customer_summary
            JOIN
                (SELECT
                    oi.order_id,
                    p.name as product_name,
                    avg(p.price) as avg_price,
                    sum(oi.quantity * p.price * (1 - oi.discount)) as final_amount
                 FROM order_items oi
                 JOIN products p ON oi.product_id = p.id
                 WHERE p.category = 'electronics'
                 GROUP BY oi.order_id, p.name
                ) product_stats ON customer_summary.customer_id IN (
                    SELECT customer_id FROM orders WHERE id = product_stats.order_id
                )
            JOIN orders order_details ON customer_summary.customer_id = order_details.customer_id
            WHERE customer_summary.total_spent > 1000
            AND product_stats.avg_price > 100
        ";

        let plan = ctx.sql(sql).await?.into_unoptimized_plan();

        // Collect column lineage
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // Collect functions - testing log, sqrt, and exp
        let mut collector = ClickHouseFunctionCollector::new(
            vec!["log".to_string(), "sqrt".to_string(), "exp".to_string()],
            lineage_visitor,
        );
        let _ = plan.visit(&mut collector)?;

        // STEP 1: Verify overall function collection
        let all_functions = collector.get_all_functions();
        println!("Total functions found: {}", all_functions.len());

        // Let's analyze each function in the query:
        // 1. log(customer_summary.total_spent) - total_spent = sum(o.amount), this is an
        //    aggregation result This should be BLOCKED because log() depends on sum() aggregation
        //    result
        // 2. sqrt(product_stats.avg_price) - avg_price = avg(p.price), this is an aggregation
        //    result This should be BLOCKED because sqrt() depends on avg() aggregation result
        // 3. exp(order_details.amount) - amount is a direct column from orders table This should be
        //    PUSHABLE because it's a direct column reference

        // Therefore, we expect exactly 1 function to be collected (exp)
        assert_eq!(
            all_functions.len(),
            1,
            "Should find exactly 1 function (exp) - log and sqrt should be blocked by aggregations"
        );

        // STEP 2: Verify functions are associated with correct tables
        let customers_table = TableReference::bare("customers");
        let products_table = TableReference::bare("products");
        let orders_table = TableReference::bare("orders");
        let order_items_table = TableReference::bare("order_items");

        let customers_functions = collector.get_functions_for_table(&customers_table);
        let products_functions = collector.get_functions_for_table(&products_table);
        let orders_functions = collector.get_functions_for_table(&orders_table);
        let order_items_functions = collector.get_functions_for_table(&order_items_table);

        // Based on our analysis:
        // - log and sqrt should be blocked (0 functions for customers and products)
        // - exp should be pushable to orders table (1 function for orders)
        // - order_items should have no functions (0 functions)

        assert_eq!(
            customers_functions.len(),
            0,
            "Customers should have 0 functions - log is blocked by aggregation"
        );
        assert_eq!(
            products_functions.len(),
            0,
            "Products should have 0 functions - sqrt is blocked by aggregation"
        );
        assert_eq!(orders_functions.len(), 1, "Orders should have 1 function - exp is pushable");
        assert_eq!(order_items_functions.len(), 0, "Order_items should have 0 functions");

        // Verify the collected function is exp for orders table
        let orders_func = &orders_functions[0];

        if let Expr::ScalarFunction(func) = &orders_func.function_expr {
            assert_eq!(func.func.name(), "exp", "Orders function should be exp");
        } else {
            panic!("Expected scalar function");
        }

        // Verify the source relation targets the correct table
        match &orders_func.source_relation {
            ResolvedSource::Exact { table, column } => {
                assert_eq!(table, &orders_table, "exp should target orders table");
                assert_eq!(column, "amount", "exp should target amount column");
            }
            _ => panic!("Expected exact resolved source, got: {:?}", orders_func.source_relation),
        }

        // STEP 3: Walk the plan and verify actions at each level
        let mut plan_ctx_manager = PlanContextManager::new();

        let _ = plan.transform_up(|node| {
            let plan_context = plan_ctx_manager.next(&node);

            match &node {
                LogicalPlan::TableScan(table_scan) => {
                    assert_eq!(plan_context.depth, 0, "TableScan should be at depth 0");

                    let table_name = table_scan.table_name.to_string();
                    let table_scan_actions = collector.get_table_scan_actions();

                    match table_name.as_str() {
                        "customers" => {
                            // Should have NO functions - log is blocked by aggregation
                            if let Some(actions) = table_scan_actions.get(&plan_context) {
                                assert_eq!(
                                    actions.functions_to_add.len(),
                                    0,
                                    "Customers TableScan should have no functions - log is blocked"
                                );
                            }
                        }
                        "products" => {
                            // Should have NO functions - sqrt is blocked by aggregation
                            if let Some(actions) = table_scan_actions.get(&plan_context) {
                                assert_eq!(
                                    actions.functions_to_add.len(),
                                    0,
                                    "Products TableScan should have no functions - sqrt is blocked"
                                );
                            }
                        }
                        "orders" => {
                            // Should have 1 function - exp is pushable
                            if let Some(actions) = table_scan_actions.get(&plan_context) {
                                assert_eq!(
                                    actions.functions_to_add.len(),
                                    1,
                                    "Orders TableScan should have 1 function - exp"
                                );

                                // Verify it's the exp function with correct alias
                                let func_expr = &actions.functions_to_add[0];
                                if let Expr::Alias(alias) = func_expr {
                                    // The alias should use the resolved table name for pushdown
                                    // order_details is an alias for orders, so the function should
                                    // be aliased as exp(orders.amount)
                                    assert_eq!(
                                        alias.name, "exp(orders.amount)",
                                        "Function should be aliased as exp(orders.amount)"
                                    );
                                } else {
                                    panic!("Function should be aliased");
                                }
                            } else {
                                panic!("Orders TableScan should have actions");
                            }
                        }
                        "order_items" => {
                            // Should have NO functions
                            if let Some(actions) = table_scan_actions.get(&plan_context) {
                                assert_eq!(
                                    actions.functions_to_add.len(),
                                    0,
                                    "Order_items TableScan should have no functions"
                                );
                            }
                        }
                        _ => {
                            // Unexpected table
                            panic!("Unexpected table: {}", table_name);
                        }
                    }
                }
                LogicalPlan::Aggregate(_agg) => {
                    // Aggregations should have no expression replacements
                    // They operate on original data before aggregation
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Aggregate should have no expression replacements"
                        );
                    }
                }
                LogicalPlan::Join(_join) => {
                    // Joins should have no expression replacements
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Join should have no expression replacements"
                        );
                    }
                }
                LogicalPlan::Projection(_projection) => {
                    // Find projections that should have exp function replacements
                    let plan_actions = collector.get_plan_actions();

                    // Look for the projection in the plan actions
                    for (ctx, actions) in plan_actions.iter() {
                        if ctx.node_id == plan_context.node_id
                            && ctx.node_details == plan_context.node_details
                        {
                            // Check if this projection contains the exp function
                            if ctx.node_details.contains("exp(") {
                                // This projection has exp function, should have 1 replacement
                                assert_eq!(
                                    actions.expressions_to_replace.len(),
                                    1,
                                    "Projection with exp function should have 1 expression \
                                     replacement"
                                );

                                // Verify it's the exp function being replaced
                                let (original, replacement) =
                                    actions.expressions_to_replace.iter().next().unwrap();
                                if let Expr::ScalarFunction(func) = original {
                                    assert_eq!(
                                        func.func.name(),
                                        "exp",
                                        "Should be replacing exp function"
                                    );

                                    if let Expr::Column(col) = replacement {
                                        assert_eq!(
                                            col.relation,
                                            Some(TableReference::bare("orders")),
                                            "Replacement should reference orders table"
                                        );
                                        assert_eq!(
                                            col.name, "exp(orders.amount)",
                                            "Replacement should be exp(orders.amount)"
                                        );
                                    } else {
                                        panic!("Replacement should be a column reference");
                                    }
                                } else {
                                    panic!("Original should be a scalar function");
                                }
                            } else {
                                // This projection doesn't have exp function, should have no
                                // replacements
                                assert_eq!(
                                    actions.expressions_to_replace.len(),
                                    0,
                                    "Projection without exp function should have no replacements"
                                );
                            }
                            break;
                        }
                    }
                }
                LogicalPlan::SubqueryAlias(_subquery_alias) => {
                    // SubqueryAlias should have no expression replacements
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "SubqueryAlias should have no expression replacements"
                        );
                    }
                }
                LogicalPlan::Filter(_filter) => {
                    // Filters should have no expression replacements
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Filter should have no expression replacements"
                        );
                    }
                }
                _ => {
                    // Other plan types should have no expression replacements
                    let plan_actions = collector.get_plan_actions();
                    if let Some(actions) = plan_actions.get(&plan_context) {
                        assert_eq!(
                            actions.expressions_to_replace.len(),
                            0,
                            "Other plan types should have no expression replacements"
                        );
                    }
                }
            }

            Ok(Transformed::no(node))
        })?;

        // STEP 4: Verify source tables are correctly identified
        let source_tables = collector.get_all_source_tables();

        // Should have identified exactly 1 source table - orders (where exp function is pushable)
        assert_eq!(source_tables.len(), 1, "Should have exactly 1 source table");
        assert!(source_tables.contains(&&orders_table), "Should contain orders table");

        // STEP 5: Verify final action counts
        let table_scan_actions = collector.get_table_scan_actions();
        let plan_actions = collector.get_plan_actions();

        // We should have:
        // - 4 table scan actions (one for each table: customers, products, orders, order_items)
        // - Multiple plan actions (for various plan nodes in the complex query)
        assert_eq!(table_scan_actions.len(), 4, "Should have 4 table scan actions");
        assert!(plan_actions.len() > 0, "Should have plan actions");

        // Verify that only orders table has functions to add
        let mut tables_with_functions = 0;
        for (ctx, actions) in table_scan_actions.iter() {
            if actions.functions_to_add.len() > 0 {
                tables_with_functions += 1;
                // Should be the orders table
                assert_eq!(ctx.table, orders_table, "Only orders table should have functions");
                assert_eq!(actions.functions_to_add.len(), 1, "Orders should have 1 function");
            }
        }
        assert_eq!(tables_with_functions, 1, "Only 1 table should have functions");

        // STEP 6: Final summary
        println!("\n=== FUNCTION PUSHDOWN ANALYSIS SUMMARY ===");
        println!("✅ Total functions collected: {} (exp only)", all_functions.len());
        println!("✅ Functions blocked by aggregation: 2 (log, sqrt)");
        println!("✅ Functions pushable to TableScan: 1 (exp to orders)");
        println!("✅ Source tables: {} (orders)", source_tables.len());
        println!("✅ TableScan actions: {}", table_scan_actions.len());
        println!("✅ Plan actions: {}", plan_actions.len());

        // This test validates that the function collector correctly:
        // 1. Identifies pushable vs blocked functions based on semantic analysis
        // 2. Associates functions with correct source tables
        // 3. Generates appropriate actions for plan transformation
        // 4. Handles complex queries with subqueries, joins, and aggregations

        println!("✅ COMPLEX QUERY FUNCTION PUSHDOWN TEST PASSED - READY FOR ANALYZER!");

        Ok(())
    }
}
