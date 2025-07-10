use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Field;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, Spans, plan_err, qualified_name};
use datafusion::error::Result;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{
    InvariantLevel, LogicalPlan, Projection, SubqueryAlias, TableScan, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::push_down_filter::replace_cols_by_name;
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;

use super::pushdown::CLICKHOUSE_FUNCTION_NODE_NAME;
use super::pushdown_analyzer::ClickHouseFunction;

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
        let (exprs, schema) = build_schema_and_exprs(&functions, &input)?;
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
    /// In federation mode, this creates a projection with only column references to avoid
    /// nested function calls when the federation layer unparsers the SQL.
    ///
    /// # Errors
    /// - TODO: Remove - docs
    ///
    /// # Panics
    /// - Should not panic, `TableScan` is guaranteed in the constructor
    pub fn into_projection_plan(self) -> Result<LogicalPlan> {
        let alias = TableReference::bare(self.table_name.as_str());
        let LogicalPlan::TableScan(input) = self.input else { unreachable!() };
        // Reuse the expressions and schema we already computed in the constructor
        let projection = Projection::try_new_with_schema(
            self.exprs.clone(),
            Arc::new(LogicalPlan::TableScan(input)),
            Arc::clone(&self.schema),
        )?;
        let input = Arc::new(LogicalPlan::Projection(projection));
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(input, alias)?))
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
            return plan_err!("ClickHouseFunctionNode expects exactly one input");
        }

        let input = inputs.swap_remove(0);

        if !matches!(input, LogicalPlan::TableScan(_)) {
            return plan_err!("ClickHouseFunctionNode expects a TableScan input");
        }

        if exprs == self.exprs && input == self.input {
            return Ok(self.clone());
        }

        Ok(Self {
            table_name: self.table_name.clone(),
            functions: self.functions.clone(),
            exprs,
            input: self.input.clone(),
            schema: Arc::clone(&self.schema),
        })
    }

    fn check_invariants(&self, _check: InvariantLevel, _plan: &LogicalPlan) -> Result<()> { Ok(()) }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }

    fn supports_limit_pushdown(&self) -> bool { false }
}

fn build_schema_and_exprs(
    functions: &[ClickHouseFunction],
    input: &TableScan,
) -> Result<(Vec<Expr>, DFSchema)> {
    // For both federation and non-federation modes, we always use the unwrapped inner function
    // This prevents the clickhouse() wrapper from being sent to the database
    let mut all_exprs = Vec::new();
    let mut all_fields = Vec::new();

    // Create a map of function aliases for quick lookup
    let mut func_map = HashMap::with_capacity(functions.len());
    for func in functions {
        let _ = func_map.insert(func.function_alias.clone(), func);
    }

    // Build a replace_map following DataFusion's pattern (see push_down_filter.rs:837-849)
    // This maps from alias qualifiers (e.g., "p2.name") to table qualifiers (e.g.,
    // "people2.name")
    let mut replace_map = HashMap::with_capacity(functions.len() * 2);

    // Collect all column references from the function expressions
    for func in functions {
        let _ = func.inner_expr.apply(|expr| {
            if let Expr::Column(col) = expr {
                if let Some(relation) = &col.relation {
                    // Map from the qualified name used in the expression to the table's
                    // qualified column
                    let old_qualified = qualified_name(Some(relation), &col.name);
                    let new_col =
                        Expr::Column(Column::new(Some(input.table_name.clone()), &col.name));
                    drop(replace_map.insert(old_qualified, new_col));
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }

    // Process each field in the input schema
    for (table_ref, field) in input.projected_schema.iter() {
        if let Some(func) = func_map.get(field.name()) {
            // Rewrite the expression using the replace_map (following DataFusion pattern)
            let rewritten_expr = if replace_map.is_empty() {
                func.inner_expr.clone()
            } else {
                replace_cols_by_name(func.inner_expr.clone(), &replace_map)?
            };

            // Replace this column with the unwrapped inner function
            all_exprs.push(Expr::Alias(Alias {
                relation: None, // Use unqualified alias to avoid qualifier mismatch
                name:     func.function_alias.clone(),
                expr:     Box::new(rewritten_expr),
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
        if !input.projected_schema.iter().any(|(_, field)| field.name() == &func.function_alias) {
            // Rewrite the expression using the replace_map (following DataFusion pattern)
            let rewritten_expr = if replace_map.is_empty() {
                func.inner_expr.clone()
            } else {
                replace_cols_by_name(func.inner_expr.clone(), &replace_map)?
            };

            all_exprs.push(Expr::Alias(Alias {
                relation: None, // Use unqualified alias to avoid qualifier mismatch
                name:     func.function_alias.clone(),
                expr:     Box::new(rewritten_expr),
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
