use datafusion::common::{Column, TableReference};
use crate::column_lineage::{PlanContext, ResolvedSource, UsageContext};

/// Per-column usage analysis with depth-based categorization
#[derive(Debug, Clone)]
pub struct ColumnUsageAnalysis {
    pub column: Column,
    pub resolved_source: ResolvedSource,
    pub same_depth_contexts: Vec<UsageContext>,     // Depth = function_depth
    pub above_function_contexts: Vec<UsageContext>, // Depth > function_depth  
    pub below_function_contexts: Vec<UsageContext>, // Depth < function_depth
}

/// Column action determination based on usage patterns
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnAction {
    /// Column is only used by this function - safe to replace with function alias
    Replace,
    /// Column is used by other contexts - must augment (keep both column and function alias)
    Augment,
}

/// Pushdown depth analysis for a single column
#[derive(Debug, Clone)]
pub enum ColumnPushdownDepth {
    /// Can push to TableScan (no blocking contexts below function)
    TableScan(TableReference),
    /// Blocked at specific depth by semantic constraints
    Blocked(PlanContext),
    /// Cannot determine safe pushdown (ambiguous contexts)
    Ambiguous,
}

/// Combined analysis result for a single column
#[derive(Debug, Clone)]
pub struct ColumnAnalysisResult {
    pub column: Column,
    pub action: ColumnAction,
    pub pushdown_depth: ColumnPushdownDepth,
    pub table: TableReference,
}

/// Function-wide analysis combining all column results
#[derive(Debug, Clone)]
pub struct FunctionAnalysisResult {
    pub columns: Vec<ColumnAnalysisResult>,
    pub overall_pushdown_depth: ColumnPushdownDepth,
    pub can_push_function: bool,
    pub requires_augmentation: bool,
}

impl ColumnUsageAnalysis {
    /// Create column usage analysis by categorizing usage contexts by depth
    pub fn new(
        column: Column,
        resolved_source: ResolvedSource,
        usage_contexts: Vec<UsageContext>,
        function_depth: usize,
    ) -> Self {
        let mut same_depth_contexts = Vec::new();
        let mut above_function_contexts = Vec::new();
        let mut below_function_contexts = Vec::new();

        for context in usage_contexts {
            match context.plan_context.depth.cmp(&function_depth) {
                std::cmp::Ordering::Equal => same_depth_contexts.push(context),
                std::cmp::Ordering::Greater => above_function_contexts.push(context),
                std::cmp::Ordering::Less => below_function_contexts.push(context),
            }
        }

        Self {
            column,
            resolved_source,
            same_depth_contexts,
            above_function_contexts,
            below_function_contexts,
        }
    }

    /// Determine column action based on usage patterns
    pub fn determine_action(&self) -> ColumnAction {
        // If any same-depth contexts use this column, need to augment
        if !self.same_depth_contexts.is_empty() {
            return ColumnAction::Augment;
        }

        // If any above-function contexts use this column, need to augment
        if !self.above_function_contexts.is_empty() {
            return ColumnAction::Augment;
        }

        // Only this function uses the column - can replace
        ColumnAction::Replace
    }

    /// Analyze pushdown depth for this column
    pub fn analyze_pushdown_depth(&self, table: &TableReference) -> ColumnPushdownDepth {
        // Start with TableScan as default (can push all the way down)
        let mut result = ColumnPushdownDepth::TableScan(table.clone());

        // Only consider contexts below the function (these can block pushdown)
        for usage in &self.below_function_contexts {
            if is_semantically_blocking(usage) {
                // This usage blocks pushdown - can't push below this depth
                let blocking_depth = ColumnPushdownDepth::Blocked(usage.plan_context.clone());
                result = result.merge(blocking_depth);
            }
        }

        result
    }

    /// Convert to column analysis result
    pub fn into_result(self, table: TableReference) -> ColumnAnalysisResult {
        let action = self.determine_action();
        let pushdown_depth = self.analyze_pushdown_depth(&table);

        ColumnAnalysisResult {
            column: self.column,
            action,
            pushdown_depth,
            table,
        }
    }
}

impl ColumnPushdownDepth {
    /// Merge two pushdown depth analyses (monadic operation)
    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (ColumnPushdownDepth::Ambiguous, _) | (_, ColumnPushdownDepth::Ambiguous) => {
                ColumnPushdownDepth::Ambiguous
            }
            (ColumnPushdownDepth::TableScan(table1), ColumnPushdownDepth::TableScan(table2)) => {
                // Both are TableScan - they should be the same table
                if table1 == table2 {
                    ColumnPushdownDepth::TableScan(table1)
                } else {
                    ColumnPushdownDepth::Ambiguous
                }
            }
            (ColumnPushdownDepth::TableScan(_), ColumnPushdownDepth::Blocked(ctx)) => {
                // Other is more restrictive (blocked), take it
                ColumnPushdownDepth::Blocked(ctx)
            }
            (ColumnPushdownDepth::Blocked(ctx), ColumnPushdownDepth::TableScan(_)) => {
                // Self is more restrictive (blocked), keep it
                ColumnPushdownDepth::Blocked(ctx)
            }
            (ColumnPushdownDepth::Blocked(ctx1), ColumnPushdownDepth::Blocked(ctx2)) => {
                // Take the one with HIGHER depth (closer to function = more restrictive)
                if ctx1.depth > ctx2.depth {
                    ColumnPushdownDepth::Blocked(ctx1)
                } else if ctx2.depth > ctx1.depth {
                    ColumnPushdownDepth::Blocked(ctx2)
                } else if ctx1.node_id == ctx2.node_id {
                    // Same context
                    ColumnPushdownDepth::Blocked(ctx1)
                } else {
                    // Same depth but different plans - ambiguous
                    ColumnPushdownDepth::Ambiguous
                }
            }
        }
    }

    /// Check if this depth allows pushdown
    pub fn is_pushable(&self) -> bool {
        !matches!(self, ColumnPushdownDepth::Ambiguous)
    }

    /// Get the target table for pushdown
    pub fn get_target_table(&self) -> Option<&TableReference> {
        match self {
            ColumnPushdownDepth::TableScan(table) => Some(table),
            ColumnPushdownDepth::Blocked(_) => None,
            ColumnPushdownDepth::Ambiguous => None,
        }
    }
}

impl FunctionAnalysisResult {
    /// Create function analysis by merging individual column results
    pub fn from_columns(columns: Vec<ColumnAnalysisResult>) -> Self {
        if columns.is_empty() {
            return Self {
                columns,
                overall_pushdown_depth: ColumnPushdownDepth::Ambiguous,
                can_push_function: false,
                requires_augmentation: false,
            };
        }

        // Merge all column pushdown depths to get overall depth
        let overall_pushdown_depth = columns
            .iter()
            .map(|col| col.pushdown_depth.clone())
            .reduce(|acc, depth| acc.merge(depth))
            .unwrap_or(ColumnPushdownDepth::Ambiguous);

        // Function can be pushed if overall depth is pushable
        let can_push_function = overall_pushdown_depth.is_pushable();

        // Function requires augmentation if any column requires augmentation
        let requires_augmentation = columns
            .iter()
            .any(|col| col.action == ColumnAction::Augment);

        Self {
            columns,
            overall_pushdown_depth,
            can_push_function,
            requires_augmentation,
        }
    }

    /// Get the unified target table (all columns should resolve to same table)
    pub fn get_target_table(&self) -> Option<TableReference> {
        // All columns should resolve to the same table
        let first_table = self.columns.first()?.table.clone();
        if self.columns.iter().all(|col| col.table == first_table) {
            Some(first_table)
        } else {
            None // Ambiguous - columns from different tables
        }
    }

    /// Get columns that need replacement (Replace action)
    pub fn get_replace_columns(&self) -> Vec<&Column> {
        self.columns
            .iter()
            .filter(|col| col.action == ColumnAction::Replace)
            .map(|col| &col.column)
            .collect()
    }

    /// Get columns that need augmentation (Augment action)
    pub fn get_augment_columns(&self) -> Vec<&Column> {
        self.columns
            .iter()
            .filter(|col| col.action == ColumnAction::Augment)
            .map(|col| &col.column)
            .collect()
    }
}

/// Check if a usage context represents a semantic blocking condition
fn is_semantically_blocking(usage: &UsageContext) -> bool {
    let plan_details = &usage.plan_context.node_details;
    let original_expr = &usage.original_expr;

    // Aggregation operations create semantic dependencies that block pushdown
    // Functions cannot be pushed below GROUP BY, HAVING, or aggregation functions
    if plan_details.contains("Aggregate") {
        return true;
    }

    // Window functions create ordering and partitioning dependencies
    // Functions cannot be pushed below OVER clauses or window boundaries
    if plan_details.contains("Window") {
        return true;
    }

    // Distinct operations create uniqueness constraints
    // Functions that could change row counts cannot be pushed below DISTINCT
    if plan_details.contains("Distinct") {
        return true;
    }

    // Limit operations create row count constraints
    // Functions that could change row ordering cannot be pushed below LIMIT
    if plan_details.contains("Limit") && affects_row_ordering(original_expr) {
        return true;
    }

    // Sort operations create ordering dependencies
    // Functions used in ORDER BY expressions create dependencies
    if plan_details.contains("Sort") && is_used_in_ordering(original_expr) {
        return true;
    }

    // Subquery operations with correlation create scoping dependencies
    // Correlated subqueries cannot have their outer references pushed down
    if plan_details.contains("SubqueryAlias") && is_correlated_reference(original_expr) {
        return true;
    }

    // Join operations with complex join conditions create dependencies
    // Functions in join predicates that reference both sides cannot be pushed
    if plan_details.contains("Join") && is_cross_table_dependency(original_expr) {
        return true;
    }

    // Union operations create row combination dependencies
    // Functions that could affect union semantics (e.g., type coercion) may block
    if plan_details.contains("Union") && affects_union_semantics(original_expr) {
        return true;
    }

    false
}

/// Check if an expression affects row ordering (e.g., random functions, row_number)
fn affects_row_ordering(expr: &Expr) -> bool {
    use datafusion::logical_expr::Expr;
    
    match expr {
        Expr::ScalarFunction(func) => {
            let func_name = func.func.name().to_lowercase();
            // Functions that produce non-deterministic or ordering-dependent results
            func_name.contains("random") || 
            func_name.contains("rand") || 
            func_name.contains("row_number") ||
            func_name.contains("now") ||
            func_name.contains("current_timestamp")
        }
        _ => false,
    }
}

/// Check if an expression is used in ordering context (ORDER BY)
fn is_used_in_ordering(expr: &Expr) -> bool {
    // In the context of our usage tracking, if we're seeing the expression
    // in a Sort plan, it's likely part of the ordering specification
    // This is a conservative approach - we could enhance with more detailed tracking
    match expr {
        Expr::Column(_) => true, // Column references in Sort are ordering dependencies
        Expr::ScalarFunction(_) => true, // Function calls in Sort create dependencies
        _ => false,
    }
}

/// Check if an expression represents a correlated subquery reference
fn is_correlated_reference(expr: &Expr) -> bool {
    // Correlated references typically show up as qualified column references
    // that reference outer query scope
    match expr {
        Expr::Column(col) => {
            // If column has a qualifier, it might be a correlated reference
            col.relation.is_some()
        }
        _ => false,
    }
}

/// Check if an expression creates cross-table dependencies in joins
fn is_cross_table_dependency(expr: &Expr) -> bool {
    // This would require more sophisticated analysis of the join context
    // For now, be conservative and assume complex expressions in joins create dependencies
    match expr {
        Expr::ScalarFunction(_) => true, // Function calls in join conditions create dependencies
        Expr::BinaryExpr(_) => true,     // Complex predicates in joins create dependencies
        _ => false,
    }
}

/// Check if an expression affects union semantics (type coercion, null handling)
fn affects_union_semantics(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarFunction(func) => {
            let func_name = func.func.name().to_lowercase();
            // Functions that affect type coercion or null handling
            func_name.contains("cast") ||
            func_name.contains("coalesce") ||
            func_name.contains("nullif") ||
            func_name.contains("case")
        }
        Expr::Case(_) => true, // CASE expressions affect union type resolution
        _ => false,
    }
}

/// Factory for creating column usage analyses
pub struct ColumnAnalysisFactory;

impl ColumnAnalysisFactory {
    /// Analyze a single column's usage patterns
    pub fn analyze_column(
        column: &Column,
        resolved_source: ResolvedSource,
        usage_contexts: Vec<UsageContext>,
        function_depth: usize,
    ) -> Option<ColumnUsageAnalysis> {
        // Only analyze columns with exact resolution
        match &resolved_source {
            ResolvedSource::Exact { .. } => {
                Some(ColumnUsageAnalysis::new(
                    column.clone(),
                    resolved_source,
                    usage_contexts,
                    function_depth,
                ))
            }
            ResolvedSource::Simple { columns, .. } if columns.len() == 1 => {
                // Single column Simple case - treat as Exact
                Some(ColumnUsageAnalysis::new(
                    column.clone(),
                    resolved_source,
                    usage_contexts,
                    function_depth,
                ))
            }
            _ => None, // Skip compound, multi-column, or unknown cases
        }
    }

    /// Analyze all columns in a function
    pub fn analyze_function_columns(
        column_refs: &[Column],
        resolve_fn: impl Fn(&Column) -> (Vec<UsageContext>, ResolvedSource),
        function_depth: usize,
    ) -> FunctionAnalysisResult {
        let mut column_results = Vec::new();

        for column in column_refs {
            let (usage_contexts, resolved_source) = resolve_fn(column);
            
            if let Some(analysis) = Self::analyze_column(
                column,
                resolved_source.clone(),
                usage_contexts,
                function_depth,
            ) {
                // Extract table from resolved source
                if let Some(table) = extract_table_from_resolved_source(&resolved_source) {
                    column_results.push(analysis.into_result(table));
                }
            }
        }

        FunctionAnalysisResult::from_columns(column_results)
    }
}

/// Extract table reference from resolved source
fn extract_table_from_resolved_source(resolved: &ResolvedSource) -> Option<TableReference> {
    match resolved {
        ResolvedSource::Exact { table, .. } => Some(table.clone()),
        ResolvedSource::Simple { table, .. } => Some(table.clone()),
        _ => None,
    }
}