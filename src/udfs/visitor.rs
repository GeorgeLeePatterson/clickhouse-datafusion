use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use clickhouse_arrow::rustc_hash::FxHashMap;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, plan_datafusion_err};
use datafusion::error::Result;
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::{LogicalPlan, SubqueryAlias};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;

use super::pushdown::{CLICKHOUSE_UDF_ALIASES, ClickHousePushdownUDF};

// TODO: Docs - What does it do? What is it used for?
//
// It's used to wrap a ScalarFunction that has been identified as a ClickHouse function and stored
// in a state map that's passed around
#[derive(Clone, Debug)]
pub struct ClickHouseFunction {
    pub(super) alias:       String,
    pub(super) func:        ScalarFunction,
    pub(super) field:       FieldRef,
    pub(super) table:       Option<TableReference>,
    pub(super) projections: Vec<Column>,
}

impl ClickHouseFunction {
    /// Create a new `ClickHouseFunction`
    ///
    /// # Errors
    /// - Returns an error if the data type is invalid
    pub fn try_new(
        mut inner_func: ScalarFunction,
        type_str: &str,
        alias: Option<&str>,
        table: Option<TableReference>,
        nullable: bool,
    ) -> Result<Self> {
        // Parse data type
        let data_type = DataType::from_str(type_str)
            .map_err(|e| plan_datafusion_err!("Invalid data type: {e}"))?;

        // Gather inner column projections
        let mut projections = Some(HashSet::<Column>::new());

        // Update function args, removing any table aliases
        let (name, args) = rewrite_function_name_and_args(&inner_func, &mut projections.as_mut());
        inner_func.args = args;

        // Gather projections
        let projections = projections.unwrap_or_default().into_iter().collect::<Vec<_>>();

        // Create alias
        let alias = alias.map(ToString::to_string).unwrap_or(name.clone());

        // Create arrow field
        let field = Arc::new(Field::new(&alias, data_type.clone(), nullable));

        // Attempt to store a table reference of some type
        let table = table.or(projections.first().and_then(|p| p.relation.clone()));

        Ok(Self { alias, func: inner_func, field, table, projections })
    }

    /// Update the table reference for this function.
    fn with_table(&mut self, table: &TableReference) { self.table = Some(table.clone()); }

    /// Get the table alias for the first column parsed out
    /// NOTE: I don't see a need to handle multiple columns, as it would be required that all
    /// columns come from the same table.
    pub(super) fn table_alias(&self) -> Option<&str> {
        self.projections.first().and_then(|p| p.relation.as_ref().map(TableReference::table))
    }

    pub(super) fn table(&self) -> Option<&str> { self.table.as_ref().map(TableReference::table) }

    pub(super) fn alias(&self) -> &str { &self.alias }
}

pub(super) fn is_clickhouse_pushdown_udf(func: &ScalarFunction) -> bool {
    CLICKHOUSE_UDF_ALIASES.iter().any(|f| func.name() == *f)
        && func.args.len() >= ClickHousePushdownUDF::ARG_LEN
}

/// Return the inner function (first argument) if it is a `ClickHouse` UDF
pub(super) fn get_clickhouse_inner_function(func: &ScalarFunction) -> Option<&ScalarFunction> {
    if is_clickhouse_pushdown_udf(func) {
        return match &func.args[0] {
            Expr::ScalarFunction(inner_func) => Some(inner_func),
            _ => None,
        };
    }

    None
}

/// Generate a consistent name without table aliases and strip aliases out of column exprs
pub(super) fn rewrite_function_name_and_args(
    func: &ScalarFunction,
    collect_projections: &mut Option<&mut HashSet<Column>>,
) -> (String, Vec<Expr>) {
    let func = get_clickhouse_inner_function(func).unwrap_or(func);

    let args = func
        .args
        .iter()
        .map(|arg| {
            arg.clone()
                .transform_up(|e| match e {
                    Expr::Column(c) => {
                        let _ = collect_projections.as_mut().map(|p| p.insert(c.clone()));
                        Ok(Transformed::yes(Expr::Column(Column::new_unqualified(c.name))))
                    }
                    _ => Ok(Transformed::no(e)),
                })
                // Safe to unwrap, no errors thrown
                .unwrap()
                .data
        })
        .collect::<Vec<_>>();
    let inner_display = func.func.display_name(&args).unwrap_or(func.name().to_string());

    (format!("clickhouse({inner_display})"), args)
}

#[derive(Clone, Debug, Default)]
pub struct PushdownVisitor {
    alias_map: FxHashMap<String, TableReference>, // alias -> table_name
    pushdowns: FxHashMap<String, ClickHouseFunction>, // key -> clickhouse function
}

impl PushdownVisitor {
    pub fn new() -> Self { PushdownVisitor::default() }

    pub fn pushdowns(&self) -> &FxHashMap<String, ClickHouseFunction> { &self.pushdowns }

    /// Visit the plan top-down and collect all aliases and identified pushdowns
    ///
    /// # Errors
    /// - Returns an error if the return type is invalid and could not be parsed
    pub fn visit_plan(&mut self, node: &LogicalPlan) -> Result<()> {
        let _ = node.apply(|plan| {
            // Look for any SubqueryAliases. This ensures schemas align during rewrites
            if let LogicalPlan::SubqueryAlias(SubqueryAlias { alias, input, .. }) = plan {
                return input.apply(|inner_plan| {
                    let mut table = None;
                    let mut finished = false;
                    if let LogicalPlan::TableScan(scan) = inner_plan {
                        table = Some(&scan.table_name);
                        finished = true;
                        drop(
                            self.alias_map
                                .insert(alias.table().to_string(), scan.table_name.clone()),
                        );
                    }
                    // Collect any inner pushdowns
                    collect_pushdowns(inner_plan, &mut self.pushdowns, table)?;
                    Ok(if finished { TreeNodeRecursion::Jump } else { TreeNodeRecursion::Continue })
                });
            }

            // Then collect any pushdowns
            collect_pushdowns(plan, &mut self.pushdowns, None)?;

            Ok(TreeNodeRecursion::Continue)
        })?;

        // Run through functions, updating table where possible.
        for func in self.pushdowns.values_mut() {
            if let Some(aliased_table) = func.table_alias().and_then(|a| self.alias_map.get(a)) {
                func.with_table(aliased_table);
            }
        }

        Ok(())
    }

    pub fn find_table_functions(&self, table_name: &str) -> Vec<ClickHouseFunction> {
        self.pushdowns
            .iter()
            .filter_map(|(_, func)| {
                if let Some(table) = func.table() {
                    (table_name == table
                        || func.table_alias().is_some_and(|a| {
                            self.alias_map.get(a).is_some_and(|t| t.table() == table_name)
                        }))
                    .then_some(func)
                } else {
                    None
                }
            })
            .cloned()
            .collect::<Vec<_>>()
    }
}

fn collect_pushdowns(
    node: &LogicalPlan,
    pushdowns: &mut FxHashMap<String, ClickHouseFunction>,
    table: Option<&TableReference>,
) -> Result<()> {
    let _ = node.apply_expressions(|expr| {
        expr.apply(|e| {
            if let Some(clickhouse_func) = unwrap_clickhouse_expr(e, table, None)? {
                let _ =
                    pushdowns.entry(clickhouse_func.alias().to_string()).or_insert(clickhouse_func);
                return Ok(TreeNodeRecursion::Jump);
            }
            Ok(TreeNodeRecursion::Continue)
        })
    })?;
    Ok(())
}

/// Given an expression try and create a [`ClickHouseFunction`]
fn unwrap_clickhouse_expr(
    expr: &Expr,
    table: Option<&TableReference>,
    alias: Option<&str>,
) -> Result<Option<ClickHouseFunction>> {
    match expr {
        Expr::Alias(Alias { expr, name, .. }) => unwrap_clickhouse_expr(expr, table, Some(name)),
        Expr::ScalarFunction(scalar) if get_clickhouse_inner_function(scalar).is_some() => {
            if let (
                Some(Expr::ScalarFunction(func)),
                Some(Expr::Literal(ScalarValue::Utf8(Some(type_str)), _)),
            ) = (scalar.args.first(), scalar.args.get(1))
            {
                Ok(Some(ClickHouseFunction::try_new(
                    func.clone(),
                    type_str,
                    alias,
                    table.cloned(),
                    // TODO: Implement nullable
                    true,
                )?))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}
