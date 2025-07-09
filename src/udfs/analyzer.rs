use std::collections::HashSet;
use std::sync::Arc;

use clickhouse_arrow::rustc_hash::FxHashMap;
use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{Extension, LogicalPlan, Projection};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use tracing::error;

use super::plan_node::ClickHouseFunctionNode;
use super::pushdown::CLICKHOUSE_FUNCTION_NODE_NAME;
use super::visitor::{
    ClickHouseFunction, PushdownVisitor, is_clickhouse_pushdown_udf, rewrite_function_name_and_args,
};

// TODO: Docs - What does it do?
//
// Analyzer Rule
#[derive(Default, Debug, Clone, Copy)]
pub struct ClickHouseUDFPushdownAnalyzerRule {}
impl AnalyzerRule for ClickHouseUDFPushdownAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut rewriter = ClickHouseRewriter::try_new(&plan)?;

        if rewriter.skip_rewrite() {
            return Ok(plan);
        }

        let transformed_plan = plan.rewrite(&mut rewriter)?;
        Ok(transformed_plan.data)
    }

    fn name(&self) -> &'static str { "ClickHouseUDFPushdownAnalyzerRule" }
}

// TODO: Docs
// Rewriter for transforming the plan
struct ClickHouseRewriter {
    visitor: PushdownVisitor,
    // Keep track of table refs during f_up
    stack:   Vec<TableReference>,
}

impl ClickHouseRewriter {
    fn try_new(plan: &LogicalPlan) -> Result<Self> {
        let mut visitor = PushdownVisitor::default();
        visitor.visit_plan(plan)?;
        Ok(Self { visitor, stack: Vec::new() })
    }

    fn skip_rewrite(&self) -> bool { self.visitor.pushdowns().is_empty() }
}

impl TreeNodeRewriter for ClickHouseRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        // TODO: Remove
        eprintln!(
            "
            =====
            ClickHouseRewriter::f_up:

            plan = {}
            =====
            ",
            node.display_indent()
        );

        // Handle TableScan first
        let node = if let LogicalPlan::TableScan(scan) = node {
            let table_name = scan.table_name.table();

            // Reset projections
            self.stack.clear();

            // Check if any pushdowns target this table. Can't pushdown without table_ref
            let funcs = self.visitor.find_table_functions(table_name);
            if !funcs.is_empty() {
                // TODO: Remove
                eprintln!(
                    "_____________ TableScan - ADDING TABLE NAME TO STACK: table = {:?}",
                    scan.table_name
                );

                // Add pushdown table scan to stack
                self.stack.push(scan.table_name.clone());

                // Create pushdown node wrapper
                let ext_node = ClickHouseFunctionNode::try_new(funcs, scan)?;

                // In the case of federation, wrap the extension node in a projection and subquery
                // alias. This will get picked up by the federation planner and the found functions
                // will get resolved within the scope of federated sql execution.
                #[cfg(feature = "federation")]
                let plan = ext_node.into_projection_plan()?;

                // But when federation is disabled, the extension node is enough.
                #[cfg(not(feature = "federation"))]
                let plan = LogicalPlan::Extension(Extension { node: Arc::new(ext_node) });

                return Ok(Transformed::yes(plan));
            }

            return Ok(Transformed::new(
                LogicalPlan::TableScan(scan),
                false,
                TreeNodeRecursion::Jump,
            ));
        } else {
            node
        };

        // Update subquery aliases
        if let LogicalPlan::SubqueryAlias(subquery) = &node {
            // TODO: Remove
            eprintln!(
                "_____________ SubqueryAlias - ADDING TABLE NAME TO STACK: alias = {:?}",
                subquery.alias
            );

            self.stack.push(subquery.alias.clone());
        }

        // TODO: Remove - or keep, this logic is confusing
        // // If the projection wraps a subquery alias and the alias is being tracked, unwrap the
        // added // projection and return the subquery alias itself.
        // //
        // // Since this is transforming "up" and the "Jump" when encountering a TableScan, the
        // block // before this identifies a previously visited subquery alias in the
        // context of a // modified TableScan.
        // #[cfg(feature = "federation")]
        // let node = match node {
        //     LogicalPlan::Projection(proj) => {
        //         if let LogicalPlan::SubqueryAlias(subquery) = proj.input.as_ref() {
        //             if let TableReference::Bare { table } = &subquery.alias {
        //                 if Some(table.as_ref()) == self.stack.last().map(TableReference::table) {
        //                     // TODO: Remove
        //                     eprintln!(
        //                         "ClickHouseRewriter::f_up  RETURNING SUBQUERY

        //                         ________ Using stack table: {table}

        //                         plan = {}

        //                         ",
        //                         proj.input.display_indent()
        //                     );

        //                     return Ok(Transformed::yes(proj.input.as_ref().clone()));
        //                 }
        //             }
        //         }
        //         LogicalPlan::Projection(proj)
        //     }
        //     n => n,
        // };

        // Again, due to the "Jump" above on TableScan, by this point we are continuing to process
        // "up" a relevant branch w/ a clickhouse extension node at the leaf
        match &node {
            // Ignored
            _ if !needs_schema_rewrite(&node) => Ok(Transformed::no(node)),
            // Need expr rewrite
            _ => replace_expressions_with_pushdowns(node, &self.visitor, self.stack.last()),
        }
    }
}

// TODO: Remove lint
#[allow(clippy::too_many_lines)]
fn replace_expressions_with_pushdowns(
    plan: LogicalPlan,
    visitor: &PushdownVisitor,
    table_ref: Option<&TableReference>,
) -> Result<Transformed<LogicalPlan>> {
    let mut current_projs = HashSet::<String>::default();
    let mut modified = false;
    let pushdowns = visitor.pushdowns();

    // TODO: Remove
    eprintln!(
        "

        |* replace_expressions_with_pushdowns
        |*
        |* table_ref = {table_ref:?}
        |*
        |* plan = {plan:#?}

        "
    );

    // Visit plans top-level expressions
    let new_plan = plan
        .map_expressions(|expr| match expr {
            Expr::Alias(Alias { name, expr, .. }) => {
                // TODO: Why store the alias always?
                let _ = current_projs.insert(name.to_string());
                replace_clickhouse_function(
                    *expr,
                    table_ref,
                    Some(&name),
                    pushdowns,
                    &mut None,
                    &mut modified,
                )
            }
            Expr::Column(column) => {
                let _ = current_projs.insert(column.name.to_string());
                Ok(Transformed::new(Expr::Column(column), false, TreeNodeRecursion::Jump))
            }
            Expr::ScalarFunction(f) => replace_clickhouse_function(
                Expr::ScalarFunction(f),
                table_ref,
                None,
                pushdowns,
                &mut Some(&mut current_projs),
                &mut modified,
            ),
            expr => replace_clickhouse_function(
                expr,
                table_ref,
                None,
                pushdowns,
                &mut Some(&mut current_projs),
                &mut modified,
            ),
        })?
        .data;

    // TODO: Remove
    eprintln!(
        "----
        > ClickHouseRewriter::f_up::replace_clickhouse_function

        modified = {modified}

        current_projs = {current_projs:?}

        new_plan = {new_plan:#?}
        ----
        "
    );

    // Projections are handled specially. This function is only called when a rewrite has occurred,
    // so all other plans will need recomputed schemas.
    Ok(match &new_plan {
        LogicalPlan::Projection(Projection { input, .. }) => {
            let pushdown_aliases = pushdowns
                .iter()
                .filter_map(|(_, func)| {
                    let table = table_ref.map(TableReference::table);
                    ((func.table() == table || func.table_alias() == table)
                        && !current_projs.contains(func.alias()))
                    .then_some(func.alias())
                })
                .map(|name| Expr::Column(Column::new(table_ref.cloned(), name.to_string())))
                .collect::<Vec<_>>();

            // TODO: Remove
            eprintln!(
                "

                | Projection pushdown aliases
                |
                | modified = {modified}
                |
                | aliases = {pushdown_aliases:?}

                "
            );

            // Return early if no relevant pushdowns and no modifications
            if pushdown_aliases.is_empty() && !modified {
                return Ok(Transformed::no(new_plan));
            }

            let mut exprs = new_plan.expressions();
            exprs.extend(pushdown_aliases);

            // TODO: Remove
            eprintln!(
                "

                | [before] Projection pushdown
                |
                | exprs = {exprs:#?}
                |
                | input = {input:#?}

                "
            );

            // TODO: Remove
            let proj = Projection::try_new(exprs, Arc::clone(input))
                .inspect_err(|error| error!("Error creating projection: {error:?}"))?;

            // TODO: Remove
            eprintln!(
                "

                | Projection pushdown MODIFIED
                |
                | new_proj = {proj:#?}

                "
            );

            Transformed::yes(LogicalPlan::Projection(proj))
        }
        _ => Transformed::yes(
            new_plan
                .recompute_schema()
                .inspect_err(|error| error!("Error recomputing schema: {error:?}"))?,
        ),
    })
}

fn replace_clickhouse_function(
    expr: Expr,
    table_ref: Option<&TableReference>,
    alias: Option<&str>,
    pushdowns: &FxHashMap<String, ClickHouseFunction>,
    current_projs: &mut Option<&mut HashSet<String>>,
    modified: &mut bool,
) -> Result<Transformed<Expr>> {
    let e = expr.transform_up(|e| {
        if let Expr::ScalarFunction(f) = &e {
            if is_clickhouse_pushdown_udf(f) {
                let name = if let Some(a) = alias {
                    a
                } else {
                    &rewrite_function_name_and_args(f, &mut None).0
                };
                if let Some(func) = pushdowns.get(name) {
                    *modified = true;
                    let _ = current_projs.as_mut().map(|c| c.insert(func.alias().to_string()));
                    return Ok(Transformed::new(
                        Expr::Column(Column::new(table_ref.cloned(), func.alias())),
                        true,
                        TreeNodeRecursion::Jump,
                    ));
                }
            }
        }
        Ok(Transformed::no(e))
    });

    // TODO: Remove
    eprintln!("*** replace_clickhouse_function: expr = {e:?}");

    e
}

fn needs_schema_rewrite(node: &LogicalPlan) -> bool {
    match node {
        LogicalPlan::Extension(Extension { node: inner_node, .. })
            if inner_node.name() == CLICKHOUSE_FUNCTION_NODE_NAME =>
        {
            false
        }
        _ => !matches!(
            node,
            LogicalPlan::Subquery(_)
                | LogicalPlan::Dml(_)
                | LogicalPlan::Copy(_)
                | LogicalPlan::Repartition(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Limit(_)
                | LogicalPlan::Ddl(_)
                | LogicalPlan::RecursiveQuery(_)
                | LogicalPlan::Analyze(_)
                | LogicalPlan::Explain(_)
                | LogicalPlan::TableScan(_)
                | LogicalPlan::EmptyRelation(_)
                | LogicalPlan::Statement(_)
                | LogicalPlan::DescribeTable(_)
        ),
    }
}
