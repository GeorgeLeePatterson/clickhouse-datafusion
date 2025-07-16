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
use datafusion::logical_expr::{Expr, LogicalPlan, SubqueryAlias};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use tracing::{debug, info};

use super::plan_node::ClickHouseFunctionNode;
use super::pushdown::{
    CLICKHOUSE_FUNCTION_NODE_NAME, CLICKHOUSE_UDF_ALIASES, clickhouse_udf_pushdown_udf,
};

/// Action to take for a column during transformation
#[derive(Debug, Clone)]
enum ColumnAction {
    /// Replace column with function alias
    Replace { function_alias: String },
    /// Keep column and add function alias
    Augment { function_alias: String },
}

/// Represents a `ClickHouse` function found during analysis
#[derive(Debug, Clone)]
pub struct ClickHouseFunction {
    /// The original clickhouse(...) expression
    pub(crate) original_expr:  Expr,
    /// The inner `ClickHouse` function expression (first argument)
    pub(crate) inner_expr:     Expr,
    /// The return type parsed from the second argument
    pub(crate) return_type:    DataType,
    // TODO: Remove - This is NOT used
    // /// Columns referenced in this function
    // pub referenced_columns: HashSet<Column>,
    /// Generated alias for the function output
    pub(crate) function_alias: String,
    /// Tables this function references
    pub(crate) source_tables:  HashSet<TableReference>,
    /// The target table this function will be pushed to
    pub(crate) target_table:   TableReference,
}

/// Generate a consistent key for a clickhouse function that can be used for
/// both collection and transformation phases. This is the core of ensuring
/// consistent fact gathering and recognition.
///
/// This function ONLY uses information available in both phases:
/// - The inner expression (first arg of clickhouse())
/// - The return type (second arg of clickhouse())
/// - The table reference extracted from the expression itself
fn generate_function_key(inner_expr: &Expr, return_type: &DataType) -> Result<String> {
    // Strip qualifiers to normalize the expression
    let stripped_inner = strip_qualifiers(inner_expr)?;

    // Create a normalized clickhouse function with stripped expression
    // This ensures we generate the same key regardless of qualifiers
    let normalized_func = Expr::ScalarFunction(ScalarFunction {
        func: Arc::new(clickhouse_udf_pushdown_udf()),
        args: vec![
            stripped_inner.clone(),
            Expr::Literal(ScalarValue::Utf8(Some(return_type.to_string())), None),
        ],
    });

    // Use DataFusion's schema_name() for consistent naming
    let base_name = normalized_func.schema_name().to_string();

    // Extract table from the expression itself to generate suffix
    // This ensures we can recreate the same suffix during transformation
    let table_suffix = extract_table_suffix(&inner_expr)?;

    Ok(format!("{}{}", base_name, table_suffix))
}

/// Extract a table suffix from an expression by finding the first table reference
/// This is used to disambiguate functions from different tables
fn extract_table_suffix(expr: &Expr) -> Result<String> {
    let mut table_ref = None;

    // Use DataFusion's apply method to find the first column with a table reference
    expr.apply(|e| {
        if let Expr::Column(col) = e {
            if let Some(relation) = &col.relation {
                table_ref = Some(relation.clone());
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    // Generate a deterministic suffix from the table reference
    Ok(match table_ref {
        Some(table) => {
            // Use a simple hash of the table name to create a short suffix
            let table_str = table.to_string();
            let hash = table_str.chars().fold(0u32, |acc, c| acc.wrapping_add(c as u32));
            format!("_t{}", hash % 1000) // Keep it short
        }
        None => String::new(), // No table reference, no suffix
    })
}

/// `ClickHouse` function pushdown analyzer with proper state tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClickHouseFunctionPushdown;

impl ClickHouseFunctionPushdown {
    pub(crate) fn new() -> Self { Self }
}

impl Default for ClickHouseFunctionPushdown {
    fn default() -> Self { Self }
}

impl AnalyzerRule for ClickHouseFunctionPushdown {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        info!("ClickHouseFunctionPushdown::analyze starting");
        info!("Federation enabled: {}", cfg!(feature = "federation"));

        eprintln!("Original plan\n{plan:#?}");

        // Phase 1: Collect functions
        let mut collector = FunctionCollector::new();
        collector.collect(&plan)?;

        if collector.functions.is_empty() {
            debug!("No clickhouse functions found");
            return Ok(plan);
        }

        debug!("Collected {} clickhouse functions", collector.functions.len());
        for (alias, func) in &collector.functions {
            debug!("  Function alias '{}': {}", alias, func.original_expr);
        }

        eprintln!("Table functions map:");
        for (table, funcs) in &collector.table_functions {
            eprintln!("  Table '{}' has functions: {:?}", table, funcs);
        }

        // Build column action map
        let column_actions = collector.build_column_action_map();

        // Phase 2: Transform with state tracking
        let mut transformer =
            PlanTransformer::new(collector.functions, collector.table_functions, column_actions);
        let result = transformer.transform(plan);

        eprintln!("===== FINAL TRANSFORMED PLAN =====");
        eprintln!("{:?}", result);
        eprintln!("===== END PLAN =====");

        Ok(result?)
    }

    fn name(&self) -> &'static str { "clickhouse_function_pushdown" }
}

/*
SELECT p3.name FROM  (
  SELECT p1.name, p2.name
  FROM person1 p1
  JOIN person2 p2 ON p1.id = p2.id
) AS p3
*/

struct AliasVisitor {
    // Table name -> Column w/ alias
    mapping: HashMap<String, String>,
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
    table_to_index:  HashMap<String, usize>,       // resolved table name -> index
    next_id:         usize,

    // Column usage tracking
    columns_in_functions:      HashSet<String>, /* Qualified columns used inside clickhouse
                                                 * functions */
    columns_outside_functions: HashSet<String>, /* Qualified columns used outside clickhouse
                                                 * functions */
    column_to_function:        HashMap<String, String>, /* Qualified column -> function alias
                                                         * that uses it */
}

impl FunctionCollector {
    fn new() -> Self {
        Self {
            functions:                 HashMap::with_capacity(8),
            table_functions:           HashMap::with_capacity(4),
            alias_to_table:            HashMap::with_capacity(4),
            table_to_index:            HashMap::with_capacity(4),
            next_id:                   0,
            columns_in_functions:      HashSet::new(),
            columns_outside_functions: HashSet::new(),
            column_to_function:        HashMap::new(),
        }
    }

    fn collect(&mut self, plan: &LogicalPlan) -> Result<()> {
        let mut alias_mapping = HashMap::new();
        // First build alias mappings - using apply since we need to visit SubqueryAlias nodes
        let _ = plan.apply_subqueries(|node| {
            if let LogicalPlan::SubqueryAlias(SubqueryAlias { alias, input, .. }) = node {
                // Find TableScan in the subtree
                let _ = input
                    .apply(|child| {
                        if let LogicalPlan::TableScan(scan) = child {
                            drop(alias_mapping.insert(
                                alias.table().to_string(),
                                scan.table_name.table().to_string(),
                            ));
                            Ok(TreeNodeRecursion::Stop)
                        } else {
                            Ok(TreeNodeRecursion::Continue)
                        }
                    })
                    .expect("Infallible apply_subqueries");
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        // First pass: discover all tables referenced by clickhouse functions
        let _ = plan.apply_subqueries(|node| {
            node.apply_expressions(|expr| {
                expr.apply(|e| {
                    if let Expr::ScalarFunction(func) = e {
                        if is_clickhouse_function(func) {
                            let mut column_refs: HashSet<(Option<String>, String)> = HashSet::new();
                            // Just resolve the table to populate table_to_index
                            func.args[0].apply(|e| {
                                if let Expr::Column(col) = e {
                                    let t_ref = col
                                        .relation
                                        .as_ref()
                                        .map(|t| t.table())
                                        .map(ToString::to_string);
                                    let name = col.name();
                                    column_refs.insert((t_ref, name.to_string()));
                                    // if let Some(relation) = &col.relation {
                                    //     // match relation {
                                    //     //     TableReference::Full { table: t, .. } => {
                                    //     //         table = Some(t.to_string());
                                    //     //         return Ok(TreeNodeRecursion::Stop);
                                    //     //     }
                                    //     //     TableReference::Bare { table: t } => {
                                    //     //         // Resolve alias to actual table name
                                    //     //         table = Some(
                                    //     //             self.alias_to_table
                                    //     //                 .get(t.as_ref())
                                    //     //                 .map_or_else(
                                    //     //                     || t.to_string(),
                                    //     //                     String::clone,
                                    //     //                 ),
                                    //     //         );
                                    //     //         return Ok(TreeNodeRecursion::Stop);
                                    //     //     }
                                    //     //     TableReference::Partial { .. } => {}
                                    //     // }
                                    // }
                                }
                                Ok(TreeNodeRecursion::Continue)
                            })?;
                            // let _ = self.resolve_table_with_index(&func.args[0]);
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        })?;

        debug!("Discovered {} tables with clickhouse functions", self.table_to_index.len());

        // 1. Collect all functions where the name of the function is a specific constant string
        //    passed to the visitor.
        // 2. Using the column lineage, resolve the source table of the function and track it along
        //    with the original function.
        // 3. Given a source table name, produce each function found.

        // X. Pushdown
        // * Resolve Aliases -> Tables
        // * Resolve Functions
        // * Pull out column ref in functions
        // * Resolve Augment/Replace w/ Column Refs
        // * Use Table Ref + Column to Update Augment/Replace
        // * Resolve Function Aliases
        //
        //
        // X. Replace/Augment
        //
        // x. Pushdown requires that Extension Node can lookup ALL functions it needs. Rest is easy.
        //  - How look up?
        //  - Gets function ALIASES by table scan name
        //  - Table Scan Name -> Function Aliases -> Function
        //  - Table Scan Name -> Function Aliases::Bare alias
        //  - Function, Bare Alias, Replace/Augment
        // x. Using Replace/Augment map, decide which to add or replace
        //  - How look up?
        // x. Replacing/Augmenting functions should use hash-like lookup to find function.
        //  - Just Vec find on ClickHouseFunctionCollected instead?
        //  - Does it get the alias? YES!
        //  - * Function -> Bare Alias
        //  - * Local Context -> Reference
        //  - *
        // x. Subquery Aliases
        //  - * Subquery -> Alias -> Table Scan Name
        // x.
        // x. Storing functions
        //  - * Function -> Is ClickHouse?
        //  - * Functions -> (Bare Alias, Function, DataType, TableRef)
        //   - * Function -> Bare Alias
        //     - * Table Ref -> Replace(Alias)
        //   - * Function -> Function
        //   - * Function -> DataType
        //   - * Function -> ** SubqueryAliases -> Table Scan Name

        // Second pass: now collect functions with proper table count knowledge
        let _ = plan.apply(|node| {
            // Process all expressions in this node
            node.apply_expressions(|expr| {
                expr.apply(|e| {
                    if let Expr::ScalarFunction(func) = e {
                        if is_clickhouse_function(func) {
                            self.record_functions(func)?;
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        })?;

        // Third pass: collect columns used outside clickhouse functions
        let _ = plan.apply(|node| {
            // We need to check expressions in different contexts
            match node {
                LogicalPlan::Join(join) => {
                    // Join conditions use columns outside functions
                    for (left_expr, right_expr) in &join.on {
                        self.record_columns_in_expr(left_expr, false)?;
                        self.record_columns_in_expr(right_expr, false)?;
                    }
                    if let Some(filter) = &join.filter {
                        self.record_columns_in_expr(filter, false)?;
                    }
                }
                LogicalPlan::Filter(filter) => {
                    self.record_columns_in_expr(&filter.predicate, false)?;
                }
                _ => {
                    // For other nodes, check expressions but skip clickhouse functions
                    node.apply_expressions(|expr| {
                        self.record_columns_in_expr(expr, false)?;
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(())
    }

    /// Record columns in an expression, tracking whether they're inside a clickhouse function
    fn record_columns_in_expr(&mut self, expr: &Expr, inside_function: bool) -> Result<()> {
        expr.apply(|e| {
            match e {
                Expr::ScalarFunction(func) if is_clickhouse_function(func) => {
                    // Record columns inside this function
                    if func.args.len() >= 1 {
                        self.record_columns_in_expr(&func.args[0], true)?;
                    }
                    // Don't traverse deeper into the function
                    Ok(TreeNodeRecursion::Jump)
                }
                Expr::Column(col) => {
                    let qualified_name = qualified_name(col.relation.as_ref(), &col.name);
                    if inside_function {
                        self.columns_in_functions.insert(qualified_name);
                    } else {
                        self.columns_outside_functions.insert(qualified_name);
                    }
                    Ok(TreeNodeRecursion::Continue)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            }
        })?;
        Ok(())
    }

    fn record_functions(&mut self, func: &ScalarFunction) -> Result<()> {
        if func.args.len() < 2 {
            return Ok(());
        }

        let inner_expr = func.args[0].clone();
        let return_type = match &func.args[1] {
            Expr::Literal(ScalarValue::Utf8(Some(type_str)), _) => DataType::from_str(type_str)
                .map_err(|e| DataFusionError::Plan(format!("Invalid return type: {e}")))?,
            _ => return Ok(()),
        };

        // Generate the consistent key using ONLY information available during transformation
        let key = generate_function_key(&inner_expr, &return_type)?;

        eprintln!("Collecting function:");
        eprintln!("  Original: {}", Expr::ScalarFunction(func.clone()));
        eprintln!("  Inner expr: {}", inner_expr);
        eprintln!("  Generated key: {}", key);

        // Resolve table for pushdown targeting
        let table_info = self.resolve_table_with_index(&inner_expr);
        if let Some((table_name, _)) = &table_info {
            self.table_functions.entry(table_name.clone()).or_default().push(key.clone());

            // Also store under any table aliases found in the expression
            // This ensures we can find functions when the TableScan uses the actual table name
            let _ = inner_expr.apply(|e| {
                if let Expr::Column(col) = e {
                    if let Some(relation) = &col.relation {
                        let alias_name = relation.to_string();
                        if &alias_name != table_name {
                            eprintln!("  Also storing function under alias '{}'", alias_name);
                            self.table_functions.entry(alias_name).or_default().push(key.clone());
                        }
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }

        // Track columns used by this function using DataFusion's column_refs
        let columns = inner_expr.column_refs();
        for col in columns {
            let qualified_name = qualified_name(col.relation.as_ref(), &col.name);
            self.columns_in_functions.insert(qualified_name.clone());
            self.column_to_function.insert(qualified_name, key.clone());
        }

        // Store the function with its key
        drop(self.functions.insert(key.clone(), CollectedClickHouseFunction {
            alias: key,
            inner_expr,
            return_type,
            original_expr: Expr::ScalarFunction(func.clone()),
        }));

        Ok(())
    }

    fn resolve_table_with_index(&mut self, expr: &Expr) -> Option<(String, usize)> {
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

        // TODO: Remove - this is wrong, the function shouldn't "record" is should parse
        // Get or assign table index
        table.map(|table_name| {
            let index = if let Some(&idx) = self.table_to_index.get(&table_name) {
                idx
            } else {
                let idx = self.next_id;
                self.next_id += 1;
                drop(self.table_to_index.insert(table_name.clone(), idx));
                idx
            };
            (table_name, index)
        })
    }

    /// Build the column action map based on collected usage information
    fn build_column_action_map(&self) -> HashMap<String, ColumnAction> {
        let mut action_map = HashMap::new();

        // For each column used in functions, determine the action
        for (col_name, func_alias) in &self.column_to_function {
            let action = if self.columns_outside_functions.contains(col_name) {
                // Column used both inside and outside functions - augment
                ColumnAction::Augment { function_alias: func_alias.clone() }
            } else {
                // Column only used inside functions - replace
                ColumnAction::Replace { function_alias: func_alias.clone() }
            };
            action_map.insert(col_name.clone(), action);
        }

        debug!("Column action map:");
        for (col, action) in &action_map {
            match action {
                ColumnAction::Replace { function_alias } => {
                    debug!("  {} -> Replace with {}", col, function_alias);
                }
                ColumnAction::Augment { function_alias } => {
                    debug!("  {} -> Augment with {}", col, function_alias);
                }
            }
        }

        action_map
    }
}

fn is_clickhouse_function(func: &ScalarFunction) -> bool {
    CLICKHOUSE_UDF_ALIASES.iter().any(|alias| func.name() == *alias) && func.args.len() > 1
}

/// Strip all qualifiers from columns in an expression
fn strip_qualifiers(expr: &Expr) -> Result<Expr> {
    expr.clone()
        .transform_up(|e| {
            match e {
                Expr::Column(col) => {
                    // Remove any qualifier, keeping only the column name
                    Ok(Transformed::yes(Expr::Column(Column::new_unqualified(&col.name))))
                }
                _ => Ok(Transformed::no(e)),
            }
        })
        .map(|t| t.data)
}

/// Extract the first qualifier found in an expression
fn extract_qualifier_from_expr(expr: &Expr) -> Option<TableReference> {
    let mut qualifier = None;

    let _ = expr.apply(|e| {
        if let Expr::Column(col) = e {
            if let Some(relation) = &col.relation {
                qualifier = Some(relation.clone());
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });

    qualifier
}

struct PlanTransformer {
    functions:       HashMap<String, CollectedClickHouseFunction>,
    table_functions: HashMap<String, Vec<String>>,
    pushed_aliases:  HashSet<String>,
    column_actions:  HashMap<String, ColumnAction>,
}

impl PlanTransformer {
    fn new(
        functions: HashMap<String, CollectedClickHouseFunction>,
        table_functions: HashMap<String, Vec<String>>,
        column_actions: HashMap<String, ColumnAction>,
    ) -> Self {
        // Collect all function aliases that have been pushed
        let pushed_aliases = functions.keys().cloned().collect();
        Self { functions, table_functions, pushed_aliases, column_actions }
    }

    fn transform(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // Transform bottom-up to ensure children are processed first
        plan.transform_up(|node| {
            eprintln!("===== WORKING A NEW PLAN: {} =====", node.display());

            match node {
                LogicalPlan::Extension(ext) if ext.node.name() == CLICKHOUSE_FUNCTION_NODE_NAME => {
                    Ok(Transformed::no(LogicalPlan::Extension(ext)))
                }
                LogicalPlan::TableScan(scan) => {
                    let table_name = scan.table_name.table();
                    eprintln!("Checking TableScan '{table_name}' for functions");
                    if let Some(func_aliases) = self.table_functions.get(table_name) {
                        eprintln!(
                            "  Found {} functions for table '{}'",
                            func_aliases.len(),
                            table_name
                        );
                        let funcs: Vec<_> = func_aliases
                            .iter()
                            .filter_map(|key| self.functions.get(key))
                            .map(|f| ClickHouseFunction {
                                function_alias: f.alias.clone(),
                                inner_expr:     f.inner_expr.clone(),
                                return_type:    f.return_type.clone(),
                                original_expr:  f.original_expr.clone(),
                                source_tables:  HashSet::default(),
                                target_table:   TableReference::bare(table_name),
                            })
                            .collect();

                        eprintln!("Creating ClickHouseFunctionNode with {} functions", funcs.len());
                        let node = ClickHouseFunctionNode::try_new(funcs, scan)?;

                        #[cfg(feature = "federation")]
                        {
                            debug!("Creating projection plan for federation mode");
                            let plan = node.into_projection_plan()?;
                            debug!("Federation projection plan: {:?}", plan);
                            Ok(Transformed::yes(plan))
                        }

                        #[cfg(not(feature = "federation"))]
                        {
                            eprintln!("Creating Extension node for non-federation mode");
                            let plan = LogicalPlan::Extension(Extension { node: Arc::new(node) });
                            Ok(Transformed::yes(plan))
                        }
                    } else {
                        Ok(Transformed::no(LogicalPlan::TableScan(scan)))
                    }
                }
                _ => {
                    eprintln!("===== PROCESSING SUBSEQUENT NODE: {} =====", node.display());

                    // Transform expressions in other nodes
                    let result = node.map_expressions(|expr| {
                        expr.transform_up(|e| {
                            match e {
                                Expr::ScalarFunction(func) if is_clickhouse_function(&func) => {
                                    let func_display = Expr::ScalarFunction(func.clone());
                                    debug!("Transforming clickhouse function: {}", func_display);

                                    // Extract arguments
                                    if func.args.len() >= 2 {
                                        let inner_expr = &func.args[0];
                                        if let Expr::Literal(ScalarValue::Utf8(Some(type_str)), _) =
                                            &func.args[1]
                                        {
                                            if let Ok(return_type) = DataType::from_str(type_str) {
                                                // Generate the same key used during collection
                                                if let Ok(key) =
                                                    generate_function_key(inner_expr, &return_type)
                                                {
                                                    debug!(
                                                        "Generated key for transformation: {}",
                                                        key
                                                    );

                                                    // Simple lookup by key
                                                    if self.functions.contains_key(&key) {
                                                        debug!(
                                                            "Found matching function by key '{}', \
                                                             replacing with column",
                                                            key
                                                        );
                                                        // Extract qualifier from the original
                                                        // expression if present
                                                        let qualifier =
                                                            extract_qualifier_from_expr(inner_expr);
                                                        let column = match qualifier {
                                                            Some(qual) => {
                                                                Column::new(Some(qual), &key)
                                                            }
                                                            None => Column::new_unqualified(&key),
                                                        };
                                                        return Ok(Transformed::yes(Expr::Column(
                                                            column,
                                                        )));
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    debug!(
                                        "No matching function found for {}, available keys: {:?}",
                                        func_display,
                                        self.functions.keys().collect::<Vec<_>>()
                                    );
                                    // Not pushed - unwrap
                                    if func.args.is_empty() {
                                        Ok(Transformed::no(Expr::ScalarFunction(func)))
                                    } else {
                                        Ok(Transformed::yes(func.args[0].clone()))
                                    }
                                }
                                Expr::Column(col) => {
                                    // Check if this column needs to be replaced or augmented
                                    let qualified_name =
                                        qualified_name(col.relation.as_ref(), &col.name);

                                    if let Some(action) = self.column_actions.get(&qualified_name) {
                                        match action {
                                            ColumnAction::Replace { function_alias } => {
                                                debug!(
                                                    "Replacing column '{}' with function alias \
                                                     '{}'",
                                                    qualified_name, function_alias
                                                );
                                                Ok(Transformed::yes(Expr::Column(
                                                    Column::new_unqualified(function_alias),
                                                )))
                                            }
                                            ColumnAction::Augment { .. } => {
                                                // For augment, we keep the column as is
                                                // The function output is added separately
                                                Ok(Transformed::no(Expr::Column(col)))
                                            }
                                        }
                                    } else {
                                        Ok(Transformed::no(Expr::Column(col)))
                                    }
                                }
                                _ => Ok(Transformed::no(e)),
                            }
                        })
                    })?;

                    eprintln!("===== AFTER MAP_EXPRESSIONS for {} =====", result.data.display());
                    eprintln!("Transformed: {}", result.transformed);

                    // ALWAYS recompute schema if something changed
                    if result.transformed {
                        let recomputed = result
                            .data
                            .recompute_schema()
                            // TODO: Remove
                            .inspect_err(|error| {
                                eprintln!(">>> Error during recomputing schema: {error:?}");
                            })?;
                        Ok(Transformed::yes(recomputed))
                    } else {
                        Ok(result)
                    }
                }
            }
        })
        .map(|t| t.data)
    }

    fn transform_expr(&self, expr: Expr, _schema: &DFSchema) -> Result<Transformed<Expr>> {
        Ok(Transformed::no(expr))
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
