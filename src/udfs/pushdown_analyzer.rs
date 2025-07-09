use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, DataFusionError, Result, Spans};
use datafusion::config::ConfigOptions;
#[cfg(not(feature = "federation"))]
use datafusion::logical_expr::Extension;
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
// use datafusion::expr::utils::expr_to_columns;
use datafusion::logical_expr::{
    Expr, InvariantLevel, LogicalPlan, Projection, SubqueryAlias, TableScan,
    UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use tracing::info;

use super::pushdown::{CLICKHOUSE_FUNCTION_NODE_NAME, CLICKHOUSE_UDF_ALIASES};

fn is_clickhouse_function(func: &ScalarFunction) -> bool {
    CLICKHOUSE_UDF_ALIASES.iter().any(|alias| func.name() == *alias)
}

fn extract_columns_from_expr(expr: &Expr) -> HashSet<Column> {
    let mut columns = HashSet::new();
    drop(expr.apply(|e| {
        if let Expr::Column(col) = e {
            let _ = columns.insert(col.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    }));
    columns
}

fn determine_target_table(func: &ClickHouseFunction) -> Result<TableReference> {
    // If function references columns from only one table, use that table
    if func.source_tables.len() == 1 {
        return Ok(func.source_tables.iter().next().unwrap().clone());
    }

    // For multi-table functions, assign to the first table deterministically
    // This could be enhanced with more sophisticated logic
    func.source_tables.iter().min().cloned().ok_or_else(|| {
        DataFusionError::Plan("ClickHouse function has no source tables".to_string())
    })
}

/// `ClickHouse` Function Pushdown Analyzer
///
/// This analyzer implements a two-phase approach:
/// 1. Analysis Phase: Discover `ClickHouse` functions and analyze column usage
/// 2. Transformation Phase: Transform the logical plan to push functions down
#[derive(Debug, Copy, Clone)]
pub struct ClickHousePushdownAnalyzer;

impl ClickHousePushdownAnalyzer {
    pub fn new() -> Self { Self }
}

impl Default for ClickHousePushdownAnalyzer {
    fn default() -> Self { Self::new() }
}

impl AnalyzerRule for ClickHousePushdownAnalyzer {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // TODO: Remove
        info!("Running analyze");
        // Phase 1: Analyze the plan to gather function information
        let mut analyzer = ClickHouseFunctionAnalyzer::new();

        // TODO: Remove
        info!("Created analyze");
        analyzer.analyze_plan(&plan)?;

        // TODO: Remove
        info!("Analyzed plan Phase 1");

        // Phase 2: Transform the plan if we found any ClickHouse functions
        if analyzer.has_functions() {
            // TODO: Remove
            info!("Analyzed has functions");

            let transformer = ClickHousePlanTransformer::new(analyzer.analysis);

            // TODO: Remove
            info!("Created Transformer");

            let result = plan.transform_up(|node| transformer.transform_node(node));

            // TODO: Remove
            info!("Transformed plan result: {result:?}");

            result.map(|t| t.data)
        } else {
            // TODO: Remove
            info!("Analyzed does NOT have functions");
            Ok(plan)
        }
    }

    fn name(&self) -> &'static str { "clickhouse_pushdown_analyzer" }
}

/// Analysis results from Phase 1
#[derive(Debug, Clone)]
pub struct ClickHouseFunctionAnalysis {
    /// Functions grouped by their target table scan
    functions_by_table: HashMap<TableReference, Vec<ClickHouseFunction>>,
    /// Column usage analysis
    column_usage:       ColumnUsageAnalysis,
}

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

/// Analysis of column usage patterns
#[derive(Debug, Clone, Default)]
pub struct ColumnUsageAnalysis {
    /// Columns used outside `ClickHouse` functions (must keep original)
    external_column_usage: HashSet<Column>,
    /// Columns used only within `ClickHouse` functions (can omit original)
    #[allow(dead_code)]
    function_only_columns: HashSet<Column>,
    /// All columns referenced in `ClickHouse` functions
    function_columns:      HashSet<Column>,
}

impl ColumnUsageAnalysis {
    /// Check if a column needs to be preserved in the original table scan
    pub fn needs_original_column(&self, col: &Column) -> bool {
        self.external_column_usage.contains(col)
    }
}

/// Phase 1: Fact Gathering & Analysis
struct ClickHouseFunctionAnalyzer {
    analysis:         ClickHouseFunctionAnalysis,
    function_counter: usize,
}

impl ClickHouseFunctionAnalyzer {
    fn new() -> Self {
        Self {
            analysis:         ClickHouseFunctionAnalysis {
                functions_by_table: HashMap::new(),
                column_usage:       ColumnUsageAnalysis::default(),
            },
            function_counter: 0,
        }
    }

    fn has_functions(&self) -> bool { !self.analysis.functions_by_table.is_empty() }

    fn analyze_plan(&mut self, plan: &LogicalPlan) -> Result<()> {
        // First pass: collect all subquery aliases and their schemas
        let mut subquery_aliases = HashMap::new();
        plan.apply(|node| {
            if let LogicalPlan::SubqueryAlias(subquery) = node {
                let _ = subquery_aliases.insert(subquery.alias.clone(), subquery.input.schema().clone());
                // Jump - we got what we need from this node, don't recurse into it
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;
        
        // Second pass: analyze nodes with subquery context
        plan.apply(|node| {
            match node {
                LogicalPlan::Extension(ext) => {
                    // Skip ClickHouseFunctionNodes - they're already processed
                    if ext.node.name() == CLICKHOUSE_FUNCTION_NODE_NAME {
                        Ok(TreeNodeRecursion::Jump)
                    } else {
                        Ok(TreeNodeRecursion::Continue)
                    }
                }
                LogicalPlan::SubqueryAlias(subquery_alias) => {
                    // Check if this is a federation-generated subquery alias
                    if let LogicalPlan::Projection(proj) = subquery_alias.input.as_ref() {
                        if let LogicalPlan::TableScan(_) = proj.input.as_ref() {
                            let has_processed_functions = proj.expr.iter().any(|expr| {
                                match expr {
                                    Expr::Alias(alias) => alias.name.starts_with("ch_func_"),
                                    _ => false,
                                }
                            });
                            
                            if has_processed_functions {
                                // This is already processed - jump
                                return Ok(TreeNodeRecursion::Jump);
                            }
                        }
                    }
                    
                    // Analyze this node but don't recurse - we handle the structure ourselves
                    self.analyze_node_with_context(node, &subquery_aliases)?;
                    Ok(TreeNodeRecursion::Jump)
                }
                LogicalPlan::Projection(projection) => {
                    // Check if this is a federation-generated projection
                    if let LogicalPlan::TableScan(_) = projection.input.as_ref() {
                        let has_processed_functions = projection.expr.iter().any(|expr| {
                            match expr {
                                Expr::Alias(alias) => alias.name.starts_with("ch_func_"),
                                _ => false,
                            }
                        });
                        
                        if has_processed_functions {
                            // This is already processed - jump
                            return Ok(TreeNodeRecursion::Jump);
                        }
                    }
                    
                    // Analyze this node and continue to children
                    self.analyze_node_with_context(node, &subquery_aliases)?;
                    Ok(TreeNodeRecursion::Continue)
                }
                _ => {
                    // Analyze this node and continue to children
                    self.analyze_node_with_context(node, &subquery_aliases)?;
                    Ok(TreeNodeRecursion::Continue)
                }
            }
        })
        .map(|_| ())?;
        Ok(())
    }

    fn analyze_node(&mut self, node: &LogicalPlan) -> Result<()> {
        self.analyze_node_with_context(node, &HashMap::new())
    }

    fn analyze_node_with_context(
        &mut self,
        node: &LogicalPlan,
        subquery_aliases: &HashMap<TableReference, DFSchemaRef>,
    ) -> Result<()> {
        match node {
            LogicalPlan::TableScan(scan) => {
                // Analyze expressions in this table scan
                for expr in &scan.filters {
                    self.analyze_expression_with_context(
                        expr,
                        Some(&scan.table_name),
                        subquery_aliases,
                    )?;
                }
                Ok(())
            }
            LogicalPlan::SubqueryAlias(alias) => {
                // For subqueries, analyze the expressions but use the alias as context
                for expr in node.expressions() {
                    self.analyze_expression_with_context(
                        &expr,
                        Some(&alias.alias),
                        subquery_aliases,
                    )?;
                }
                Ok(())
            }
            _ => {
                // Analyze all expressions in this node
                for expr in node.expressions() {
                    self.analyze_expression_with_context(&expr, None, subquery_aliases)?;
                }
                Ok(())
            }
        }
    }

    fn analyze_expression(
        &mut self,
        expr: &Expr,
        table_context: Option<&TableReference>,
    ) -> Result<()> {
        self.analyze_expression_with_context(expr, table_context, &HashMap::new())
    }

    fn analyze_expression_with_context(
        &mut self,
        expr: &Expr,
        table_context: Option<&TableReference>,
        subquery_aliases: &HashMap<TableReference, DFSchemaRef>,
    ) -> Result<()> {
        // First check if we're dealing with an aliased expression
        if let Expr::Alias(alias) = expr {
            if let Expr::ScalarFunction(func) = alias.expr.as_ref() {
                if is_clickhouse_function(func) {
                    // Parse with the alias name
                    if let Some(mut ch_func) = self.parse_clickhouse_function_with_context(
                        func,
                        table_context,
                        subquery_aliases,
                    )? {
                        // Use the user-provided alias instead of generated one
                        ch_func.function_alias = alias.name.clone();

                        // Determine target table for this function
                        let target_table = determine_target_table(&ch_func)?;

                        // Create function with target table
                        ch_func.target_table = target_table.clone();

                        // Add to functions by table
                        self.analysis
                            .functions_by_table
                            .entry(target_table)
                            .or_default()
                            .push(ch_func.clone());

                        // Update column usage analysis
                        for col in &ch_func.referenced_columns {
                            let _ = self.analysis.column_usage.function_columns.insert(col.clone());
                        }
                    }
                    return Ok(());
                }
            }
        }

        // Then walk the expression tree normally
        expr.apply(|e| {
            match e {
                Expr::ScalarFunction(func) if is_clickhouse_function(func) => {
                    eprintln!("Found ClickHouse function: {:?}", func.name());
                    // Found a ClickHouse function - analyze it
                    if let Some(ch_func) = self.parse_clickhouse_function_with_context(
                        func,
                        table_context,
                        subquery_aliases,
                    )? {
                        // Determine target table for this function
                        let target_table = determine_target_table(&ch_func)?;

                        // Create function with target table
                        let mut ch_func = ch_func;
                        ch_func.target_table = target_table.clone();

                        // Add to functions by table
                        self.analysis
                            .functions_by_table
                            .entry(target_table)
                            .or_default()
                            .push(ch_func.clone());

                        // Update column usage analysis
                        for col in &ch_func.referenced_columns {
                            let _ = self.analysis.column_usage.function_columns.insert(col.clone());
                        }
                    }
                    Ok(TreeNodeRecursion::Jump) // Don't recurse into function args
                }
                Expr::Column(col) => {
                    // Track column usage outside of ClickHouse functions
                    let _ = self.analysis.column_usage.external_column_usage.insert(col.clone());
                    Ok(TreeNodeRecursion::Continue)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            }
        })
        .map(|_| ())?;
        Ok(())
    }

    fn parse_clickhouse_function(
        &mut self,
        func: &ScalarFunction,
        _table_context: Option<&TableReference>,
    ) -> Result<Option<ClickHouseFunction>> {
        self.parse_clickhouse_function_with_context(
            func,
            _table_context,
            &HashMap::new(),
        )
    }

    fn parse_clickhouse_function_with_context(
        &mut self,
        func: &ScalarFunction,
        _table_context: Option<&TableReference>,
        subquery_aliases: &HashMap<TableReference, DFSchemaRef>,
    ) -> Result<Option<ClickHouseFunction>> {
        // Extract function arguments
        if func.args.len() < 2 {
            return Ok(None);
        }

        let inner_expr = func.args[0].clone();
        let return_type_expr = &func.args[1];

        // Parse return type from second argument using DataType::from_str
        let return_type = match return_type_expr {
            Expr::Literal(ScalarValue::Utf8(Some(type_str)), _) => DataType::from_str(type_str)
                .map_err(|e| DataFusionError::Plan(format!("Invalid return type: {e}")))?,
            _ => return Ok(None),
        };

        // Extract referenced columns
        let referenced_columns = extract_columns_from_expr(&inner_expr);

        eprintln!("  Referenced columns: {:?}", referenced_columns);

        // Determine source tables from referenced columns, resolving subquery aliases
        let mut source_tables = HashSet::new();

        for col in &referenced_columns {
            if let Some(relation) = &col.relation {
                match relation {
                    // Direct table reference - use as is
                    TableReference::Full { .. } => {
                        source_tables.insert(relation.clone());
                    }
                    // Subquery alias - resolve to underlying table
                    alias_ref @ (TableReference::Bare { .. } | TableReference::Partial { .. }) => {
                        if let Some(subquery_schema) = subquery_aliases.get(alias_ref) {
                            // Find the underlying table scan that provides this column
                            if let Some(underlying_table) =
                                self.resolve_column_to_table(col, subquery_schema)?
                            {
                                eprintln!(
                                    "  Resolved subquery alias {:?} column {} to table {:?}",
                                    alias_ref, col.name, underlying_table
                                );
                                source_tables.insert(underlying_table);
                            }
                        } else {
                            // No subquery found, treat as direct reference
                            source_tables.insert(relation.clone());
                        }
                    }
                }
            }
        }

        eprintln!("  Source tables (resolved): {:?}", source_tables);

        // If no source tables found, we can't push this function down
        if source_tables.is_empty() {
            eprintln!("  Skipping function - no resolvable source tables");
            return Ok(None);
        }

        // Generate function alias - use a simple counter-based approach
        let function_alias = format!("ch_func_{}", self.function_counter);
        self.function_counter += 1;

        Ok(Some(ClickHouseFunction {
            original_expr: Expr::ScalarFunction(func.clone()),
            inner_expr,
            return_type,
            referenced_columns,
            function_alias,
            source_tables,
            target_table: TableReference::bare(""), // Will be set later
        }))
    }

    /// Resolve a column reference to its underlying table scan
    fn resolve_column_to_table(
        &self,
        col: &Column,
        subquery_schema: &DFSchemaRef,
    ) -> Result<Option<TableReference>> {
        // Look for the column in the subquery schema
        for (table_ref, field) in subquery_schema.iter() {
            if field.name() == &col.name {
                // Found the column, return the table reference
                if let Some(table_ref) = table_ref {
                    return Ok(Some(table_ref.clone()));
                }
            }
        }

        // Column not found in subquery schema
        eprintln!("  Column {} not found in subquery schema", col.name);
        Ok(None)
    }
}

/// Phase 2: Plan Transformation
struct ClickHousePlanTransformer {
    analysis: ClickHouseFunctionAnalysis,
}


impl ClickHousePlanTransformer {
    fn new(analysis: ClickHouseFunctionAnalysis) -> Self { Self { analysis } }

    fn transform_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        plan.transform_up(|node| self.transform_node(node)).map(|transformed| transformed.data)
    }

    fn transform_node(&self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match node {
            LogicalPlan::Extension(ext) => {
                // If this is already a ClickHouseFunctionNode, don't transform it again
                if ext.node.name() == CLICKHOUSE_FUNCTION_NODE_NAME {
                    return Ok(Transformed::new(LogicalPlan::Extension(ext), false, TreeNodeRecursion::Jump));
                }
                Ok(Transformed::new(LogicalPlan::Extension(ext), false, TreeNodeRecursion::Jump))
            }
            LogicalPlan::TableScan(scan) => {
                // Check if this table has any ClickHouse functions
                if let Some(functions) = self.analysis.functions_by_table.get(&scan.table_name) {
                    let transformed_scan = self.transform_table_scan(scan, functions)?;
                    Ok(Transformed::yes(transformed_scan))
                } else {
                    // No functions for this table - jump to skip any unnecessary processing
                    Ok(Transformed::new(LogicalPlan::TableScan(scan), false, TreeNodeRecursion::Jump))
                }
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                // Check if this is a federation-generated subquery alias - if so, don't transform it
                if let LogicalPlan::Projection(proj) = subquery_alias.input.as_ref() {
                    if let LogicalPlan::TableScan(_) = proj.input.as_ref() {
                        // Check if any expressions are already transformed (ch_func_*)
                        let has_processed_functions = proj.expr.iter().any(|expr| {
                            match expr {
                                Expr::Alias(alias) => alias.name.starts_with("ch_func_"),
                                _ => false,
                            }
                        });
                        
                        if has_processed_functions {
                            // This looks like a federation-generated subquery - don't transform it
                            return Ok(Transformed::new(LogicalPlan::SubqueryAlias(subquery_alias), false, TreeNodeRecursion::Jump));
                        }
                    }
                }
                
                // SubqueryAlias doesn't have expressions directly, let transform_up handle the children
                Ok(Transformed::no(LogicalPlan::SubqueryAlias(subquery_alias)))
            }
            LogicalPlan::Projection(projection) => {
                // Check if this is a federation-generated projection
                if let LogicalPlan::TableScan(_) = projection.input.as_ref() {
                    let has_processed_functions = projection.expr.iter().any(|expr| {
                        match expr {
                            Expr::Alias(alias) => alias.name.starts_with("ch_func_"),
                            _ => false,
                        }
                    });
                    
                    if has_processed_functions {
                        // This looks like a federation-generated projection - don't transform it
                        return Ok(Transformed::new(LogicalPlan::Projection(projection), false, TreeNodeRecursion::Jump));
                    }
                }
                
                // Check if expressions need replacement
                let has_clickhouse_functions = projection.expr.iter().any(|expr| {
                    self.expr_has_clickhouse_functions(expr)
                });
                
                if !has_clickhouse_functions {
                    // No ClickHouse functions - let transform_up handle the children
                    Ok(Transformed::no(LogicalPlan::Projection(projection)))
                } else {
                    // Replace ClickHouse functions in expressions
                    let transformed_exprs = projection.expr
                        .into_iter()
                        .map(|expr| self.replace_clickhouse_functions(expr))
                        .collect::<Result<Vec<_>>>()?;
                    
                    let new_projection = Projection::try_new(
                        transformed_exprs,
                        projection.input,
                    )?;
                    Ok(Transformed::yes(LogicalPlan::Projection(new_projection)))
                }
            }
            _ => {
                // For other nodes, check if they have ClickHouse functions to replace
                let has_clickhouse_functions = node.expressions().iter().any(|expr| {
                    self.expr_has_clickhouse_functions(expr)
                });
                
                if !has_clickhouse_functions {
                    // No ClickHouse functions - no transformation needed, jump to skip children
                    return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
                }
                
                // Replace ClickHouse function calls with column references
                let transformed_exprs = node
                    .expressions()
                    .into_iter()
                    .map(|expr| self.replace_clickhouse_functions(expr))
                    .collect::<Result<Vec<_>>>()?;

                if transformed_exprs.iter().zip(node.expressions()).any(|(new, old)| new != &old) {
                    let new_plan = node.with_new_exprs(
                        transformed_exprs,
                        node.inputs().into_iter().cloned().collect(),
                    )?;
                    Ok(Transformed::yes(new_plan))
                } else {
                    Ok(Transformed::no(node))
                }
            }
        }
    }

    fn expr_has_clickhouse_functions(&self, expr: &Expr) -> bool {
        let mut has_functions = false;
        let _ = expr.apply(|e| {
            match e {
                Expr::ScalarFunction(func) if is_clickhouse_function(func) => {
                    has_functions = true;
                    Ok(TreeNodeRecursion::Jump)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            }
        });
        has_functions
    }

    fn transform_table_scan(
        &self,
        scan: TableScan,
        functions: &[ClickHouseFunction],
    ) -> Result<LogicalPlan> {
        // Check if we should only process functions that target this specific table
        let table_functions: Vec<ClickHouseFunction> =
            functions.iter().filter(|f| f.target_table == scan.table_name).cloned().collect();

        if table_functions.is_empty() {
            // No functions for this table, return as-is
            return Ok(LogicalPlan::TableScan(scan));
        }

        let node = ClickHouseFunctionNode::try_new(table_functions, scan)?;

        #[cfg(feature = "federation")]
        {
            // With federation: wrap in Projection for datafusion-federation unparsing
            node.into_projection_plan()
        }
        #[cfg(not(feature = "federation"))]
        {
            // Without federation: wrap in UserDefinedLogicalNode
            Ok(LogicalPlan::Extension(Extension { node: Arc::new(node) }))
        }
    }

    fn replace_clickhouse_functions(&self, expr: Expr) -> Result<Expr> {
        expr.transform_up(|e| {
            match &e {
                Expr::Alias(alias) if matches!(alias.expr.as_ref(), Expr::ScalarFunction(f) if is_clickhouse_function(f)) => {
                    // For aliased ClickHouse functions, replace with column reference using the alias name
                    let col_ref = Expr::Column(Column::new_unqualified(alias.name.clone()));
                    Ok(Transformed::yes(col_ref))
                }
                Expr::ScalarFunction(func) if is_clickhouse_function(func) => {
                    // Replace with column reference to the pushed-down function
                    let alias = self.find_function_alias(func)?;
                    let col_ref = Expr::Column(Column::new_unqualified(alias));
                    Ok(Transformed::yes(col_ref))
                }
                _ => Ok(Transformed::no(e)),
            }
        })
        .map(|transformed| transformed.data)
    }

    fn find_function_alias(&self, func: &ScalarFunction) -> Result<String> {
        // Find the function in our analysis by matching the original expression
        let target_expr = Expr::ScalarFunction(func.clone());

        for functions in self.analysis.functions_by_table.values() {
            for ch_func in functions {
                if ch_func.original_expr == target_expr {
                    return Ok(ch_func.function_alias.clone());
                }
            }
        }

        Err(DataFusionError::Plan("ClickHouse function not found in analysis".to_string()))
    }

    fn transform_subquery_alias(
        &self,
        subquery_alias: SubqueryAlias,
    ) -> Result<Transformed<LogicalPlan>> {
        // Transform the input plan first
        let original_input = Arc::clone(&subquery_alias.input);
        let alias = subquery_alias.alias.clone();
        let transformed_input = self.transform_plan(Arc::unwrap_or_clone(subquery_alias.input))?;

        // Check if the input was actually transformed
        if transformed_input == *original_input {
            return Ok(Transformed::no(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                original_input,
                alias,
            )?)));
        }

        // Create a new SubqueryAlias with the transformed input
        // This automatically recomputes the schema based on the new input
        let new_subquery = SubqueryAlias::try_new(Arc::new(transformed_input), alias)?;

        Ok(Transformed::yes(LogicalPlan::SubqueryAlias(new_subquery)))
    }

    fn transform_projection(&self, projection: Projection) -> Result<Transformed<LogicalPlan>> {
        // Extract values before potential moves
        let original_input = Arc::clone(&projection.input);
        let projection_exprs = projection.expr.clone();

        // Check if input is already a ClickHouseFunctionNode - if so, don't transform it again
        let transformed_input = match projection.input.as_ref() {
            LogicalPlan::Extension(ext) if ext.node.name() == CLICKHOUSE_FUNCTION_NODE_NAME => {
                Arc::unwrap_or_clone(projection.input)
            }
            _ => self.transform_plan(Arc::unwrap_or_clone(projection.input))?,
        };

        // Check if the input was actually transformed
        let input_was_transformed = transformed_input != *original_input;

        // Check if we need to add any function columns
        let input_schema = transformed_input.schema();
        let mut additional_exprs = Vec::new();

        // Look for function columns in the input that are not in the current projection
        for (relation, field) in input_schema.iter() {
            let field_name = field.name();

            // Check if this looks like a function column we added (ch_func_*)
            if field_name.starts_with("ch_func_") {
                // Check if this column is already in the projection
                let already_projected = projection_exprs.iter().any(|expr| match expr {
                    Expr::Column(col) => col.name == *field_name,
                    Expr::Alias(alias) => alias.name == *field_name,
                    _ => false,
                });

                if !already_projected {
                    additional_exprs.push(Expr::Column(Column::new(relation.cloned(), field_name)));
                }
            }
        }

        // Replace ClickHouse functions in the projection expressions
        let replaced_exprs = projection_exprs
            .into_iter()
            .map(|expr| self.replace_clickhouse_functions(expr))
            .collect::<Result<Vec<_>>>()?;

        // Check if expressions were replaced
        let exprs_were_replaced =
            replaced_exprs.iter().zip(projection.expr.iter()).any(|(new, old)| new != old);

        // If we have additional expressions or the input changed or expressions were replaced,
        // create a new projection
        if !additional_exprs.is_empty() || input_was_transformed || exprs_were_replaced {
            let mut new_exprs = replaced_exprs;
            new_exprs.extend(additional_exprs);

            let new_projection = Projection::try_new(new_exprs, Arc::new(transformed_input))?;

            Ok(Transformed::yes(LogicalPlan::Projection(new_projection)))
        } else {
            // Recreate the projection with the original input and expressions
            let recreated_projection = Projection::try_new(replaced_exprs, original_input)?;
            Ok(Transformed::no(LogicalPlan::Projection(recreated_projection)))
        }
    }
}

/// New `ClickHouse` Function Node for the logical plan
#[derive(Clone, Debug)]
pub struct ClickHouseFunctionNode {
    table_name: String,
    functions:  Vec<ClickHouseFunction>,
    exprs:      Vec<Expr>,
    input:      LogicalPlan,
    schema:     DFSchemaRef,
}

impl ClickHouseFunctionNode {
    /// Create a new `ClickHouseFunctionNode`.
    ///
    /// # Errors
    /// - TODO: Remove - docs
    pub fn try_new(functions: Vec<ClickHouseFunction>, input: TableScan) -> Result<Self> {
        let (exprs, schema) = Self::build_schema_and_exprs(&functions, &input)?;

        Ok(Self {
            table_name: input.table_name.table().to_string(),
            functions,
            exprs,
            input: LogicalPlan::TableScan(input),
            schema: Arc::new(schema),
        })
    }

    /// Create a `LogicalPlan::SubqueryAlias` wrapping a `LogicalPlan::Projection`.
    ///
    /// # Errors
    /// - TODO: Remove - docs
    pub fn into_projection_plan(self) -> Result<LogicalPlan> {
        use datafusion::logical_expr::{Projection, SubqueryAlias};

        let alias = TableReference::bare(self.table_name.as_str());
        let projection =
            Projection::try_new_with_schema(self.exprs, Arc::new(self.input), self.schema)?;

        let input = Arc::new(LogicalPlan::Projection(projection));
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(input, alias)?))
    }

    fn build_schema_and_exprs(
        functions: &[ClickHouseFunction],
        input: &TableScan,
    ) -> Result<(Vec<Expr>, DFSchema)> {
        let mut all_exprs = Vec::new();
        let mut all_fields = Vec::new();

        // Create a map of function aliases for quick lookup
        let func_map: HashMap<String, &ClickHouseFunction> =
            functions.iter().map(|f| (f.function_alias.clone(), f)).collect();

        // Process each field in the input schema
        for (table_ref, field) in input.projected_schema.iter() {
            if let Some(func) = func_map.get(field.name()) {
                // Replace this column with the function
                all_exprs.push(Expr::Alias(Alias {
                    relation: Some(input.table_name.clone()),
                    name:     func.function_alias.clone(),
                    expr:     Box::new(func.inner_expr.clone()),
                    metadata: None,
                }));

                let func_field = Arc::new(Field::new(
                    &func.function_alias,
                    func.return_type.clone(),
                    true, // nullable
                ));
                all_fields.push((Some(input.table_name.clone()), func_field));
            } else {
                // Keep the original column
                all_exprs.push(Expr::Column(Column {
                    relation: table_ref.cloned(),
                    name:     field.name().to_string(),
                    spans:    Spans::new(),
                }));
                all_fields.push((table_ref.cloned(), Arc::clone(field)));
            }
        }

        // Add any additional function columns that weren't in the original schema
        for func in functions {
            if !input.projected_schema.iter().any(|(_, field)| field.name() == &func.function_alias)
            {
                all_exprs.push(Expr::Alias(Alias {
                    relation: Some(input.table_name.clone()),
                    name:     func.function_alias.clone(),
                    expr:     Box::new(func.inner_expr.clone()),
                    metadata: None,
                }));

                let func_field = Arc::new(Field::new(
                    &func.function_alias,
                    func.return_type.clone(),
                    true, // nullable
                ));
                all_fields.push((Some(input.table_name.clone()), func_field));
            }
        }

        // Create the schema
        let schema =
            DFSchema::new_with_metadata(all_fields, input.projected_schema.metadata().clone())?;

        Ok((all_exprs, schema))
    }
}

impl UserDefinedLogicalNodeCore for ClickHouseFunctionNode {
    fn name(&self) -> &str { CLICKHOUSE_FUNCTION_NODE_NAME }

    fn inputs(&self) -> Vec<&LogicalPlan> { vec![&self.input] }

    fn schema(&self) -> &DFSchemaRef { &self.schema }

    fn expressions(&self) -> Vec<Expr> { self.exprs.clone() }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} functions", CLICKHOUSE_FUNCTION_NODE_NAME, self.functions.len())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "ClickHouseFunctionNode expects exactly one input".to_string(),
            ));
        }

        let input = inputs.swap_remove(0);

        if exprs == self.exprs && input == self.input {
            return Ok(self.clone());
        }

        Ok(Self {
            table_name: self.table_name.clone(),
            functions: self.functions.clone(),
            exprs,
            input,
            schema: Arc::clone(&self.schema),
        })
    }

    fn check_invariants(&self, _check: InvariantLevel, _plan: &LogicalPlan) -> Result<()> { Ok(()) }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }

    fn supports_limit_pushdown(&self) -> bool { false }
}

impl PartialEq for ClickHouseFunctionNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.functions == other.functions
            && self.exprs == other.exprs
            && self.input == other.input
    }
}

impl Eq for ClickHouseFunctionNode {}

impl std::hash::Hash for ClickHouseFunctionNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.functions.hash(state);
        // Note: We don't hash exprs or input as they don't implement Hash
    }
}

impl PartialOrd for ClickHouseFunctionNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl Ord for ClickHouseFunctionNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.table_name.cmp(&other.table_name).then_with(|| self.functions.cmp(&other.functions))
    }
}

// Implement Hash for ClickHouseFunction
impl std::hash::Hash for ClickHouseFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.function_alias.hash(state);
        self.return_type.hash(state);
        self.target_table.hash(state);
        // Note: We don't hash expressions as they don't implement Hash
    }
}

impl PartialEq for ClickHouseFunction {
    fn eq(&self, other: &Self) -> bool {
        self.function_alias == other.function_alias
            && self.return_type == other.return_type
            && self.target_table == other.target_table
            && self.original_expr == other.original_expr
    }
}

impl Eq for ClickHouseFunction {}

impl PartialOrd for ClickHouseFunction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl Ord for ClickHouseFunction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.function_alias
            .cmp(&other.function_alias)
            .then_with(|| self.target_table.cmp(&other.target_table))
    }
}
