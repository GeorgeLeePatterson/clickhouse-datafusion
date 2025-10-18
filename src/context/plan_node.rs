//! A `UserDefinedLogicalNodeCore` implementation for wrapping largest sub-trees of a `DataFusion`
//! logical plan for execution on `ClickHouse` directly.
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, plan_err};
use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::logical_expr::{InvariantLevel, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::sql::unparser::Unparser;

use crate::ClickHouseConnectionPool;
use crate::dialect::ClickHouseDialect;
use crate::providers::table::ClickHouseTableProvider;
use crate::sql::ClickHouseSqlExec;

pub const CLICKHOUSE_FUNCTION_NODE_NAME: &str = "ClickHouseFunctionNode";

/// Extension node for `ClickHouse` function pushdown
///
/// This extension node serves as a wrapper only, so that during planner execution, the input plan
/// can be unparsed into sql and executed on `ClickHouse`.
#[derive(Clone, Debug)]
pub struct ClickHouseFunctionNode {
    /// The input plan that this node wraps
    pub(super) input: LogicalPlan,
    schema:           DFSchemaRef,
    pool:             Arc<ClickHouseConnectionPool>,
    coerce_schema:    bool,
}

impl ClickHouseFunctionNode {
    /// Create a new `ClickHouseFunctionNode`
    ///
    /// # Errors
    /// - Returns an error if the `TableProvider` cannot be found or downcast.
    ///
    /// # Panics
    /// - Does not panic. No errors are thrown in plan recursion.
    pub fn try_new(input: LogicalPlan) -> Result<Self> {
        let schema = Arc::clone(input.schema());
        // If the table provider is set to coerce schemas, ensure the execution plan coerces
        let mut coerce_schema = false;
        let mut pool = None;
        let _ = input
            .apply(|plan| {
                if let LogicalPlan::TableScan(scan) = plan {
                    // Convert to TableProvider
                    let provider = source_as_provider(&scan.source)?;
                    if let Some(provider) =
                        provider.as_any().downcast_ref::<ClickHouseTableProvider>()
                    {
                        coerce_schema = provider.coerce_schema();
                        pool = Some(Arc::clone(provider.pool()));
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
            // No error thrown
            .unwrap();

        let Some(pool) = pool else {
            return plan_err!(
                "ClickHouseFunctionNode: cannot execute without a connection pool, most likely a \
                 ClickHouseTableProvider was never found in the plan."
            );
        };

        Ok(Self { input, schema, pool, coerce_schema })
    }

    pub(crate) fn execute(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let sql = Unparser::new(&ClickHouseDialect).plan_to_sql(&self.input)?.to_string();
        ClickHouseSqlExec::try_new(None, self.input.schema().inner(), Arc::clone(&self.pool), sql)
            .map(|ex| ex.with_coercion(self.coerce_schema))
            .map(|ex| Arc::new(ex) as Arc<dyn ExecutionPlan>)
    }
}

impl UserDefinedLogicalNodeCore for ClickHouseFunctionNode {
    fn name(&self) -> &str { CLICKHOUSE_FUNCTION_NODE_NAME }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        // NOTE: This is disabled currently, see note in `with_exprs_and_inputs`.
        //
        // // Pass through to input so optimizations can be applied
        // vec![&self.input]
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef { &self.schema }

    // NOTE: This is disabled currently, see note in `with_exprs_and_inputs`.
    fn expressions(&self) -> Vec<Expr> { vec![] }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClickHouseFunctionNode")
    }

    // Pass through to input so optimizations can be applied
    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("ClickHouseFunctionNode expects no expressions");
        }
        if !inputs.is_empty() {
            return plan_err!("ClickHouseFunctionNode expects no inputs");
        }

        Ok(self.clone())
        // NOTE: The following logic has been commented out. Originally the idea was to allow
        // optimization of the input logical plan by DataFusion's optimizers, and for almost all
        // cases, this works fine.
        //
        // But, until the strange output seen when Unparsing in the case when the input to a
        // `Projection` is a `TableScan` wrapped by another node, like `Limit` or `Sort`, then this
        // must be disabled. In those aforementioned cases, a subquery will be produced that
        // uses a wildcard but the aliases of the projection will NOT be updated, causing an
        // error.
        //
        // if inputs.len() != 1 {
        //     return plan_err!("ClickHouseFunctionNode expects exactly one input");
        // }
        // let input = inputs.remove(0);
        // // If not coercing schema, check input schema
        // if !self.coerce_schema {
        //     self.schema
        //         .has_equivalent_names_and_types(input.schema())
        //         .map_err(|e| e.context("Invalid input schema for ClickHouseFunctionNode"))?;
        // }
        // Ok(Self {
        //     input,
        //     schema: Arc::clone(&self.schema),
        //     pool: self.pool.clone(),
        //     coerce_schema: self.coerce_schema,
        // })
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<()> {
        // No invariant checks needed for now
        Ok(())
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // No special column requirements
        None
    }

    /// `false` is the default behavior. It's included here specifically to note that pushing limits
    /// down causes a bug in `Unparsing` where subquery aliases are wrapped in parentheses in a way
    /// that causes an error in `ClickHouse`. But, since the query will run on `ClickHouse`,
    /// optimization should be done there if possible.
    fn supports_limit_pushdown(&self) -> bool { false }
}

// Implement required traits for LogicalPlan integration
impl PartialEq for ClickHouseFunctionNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.name() == other.name()
            && self.pool.join_push_down() == other.pool.join_push_down()
    }
}

impl Eq for ClickHouseFunctionNode {}

impl std::hash::Hash for ClickHouseFunctionNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.input.hash(state);
        self.pool.join_push_down().hash(state);
    }
}

impl PartialOrd for ClickHouseFunctionNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::sql::TableReference;

    use super::*;

    #[test]
    fn test_plan_node_requires_provider() {
        let provider = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
        let err_plan = LogicalPlanBuilder::scan(
            TableReference::bare("test"),
            provider_as_source(provider),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let result = ClickHouseFunctionNode::try_new(err_plan);
        assert!(result.is_err(), "ClickHouseTableProvider required for a ClickHouseFunctionNode");
    }

    #[cfg(feature = "mocks")]
    #[test]
    fn test_verify_no_exprs_input() {
        use datafusion::common::Column;

        let pool = Arc::new(ClickHouseConnectionPool::new("pool".to_string(), ()));
        let provider = Arc::new(ClickHouseTableProvider::new_with_schema_unchecked(
            Arc::clone(&pool),
            "table1".into(),
            Arc::new(Schema::empty()),
        ));
        let plan = LogicalPlanBuilder::scan(
            TableReference::bare("test"),
            provider_as_source(provider),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let node = ClickHouseFunctionNode::try_new(plan.clone()).unwrap();

        let result =
            node.with_exprs_and_inputs(vec![Expr::Column(Column::from_name("test_col"))], vec![]);
        assert!(result.is_err(), "ClickHouseFunctionNode must take no exprs");

        let result = node.with_exprs_and_inputs(vec![], vec![plan.clone()]);
        assert!(result.is_err(), "ClickHouseFunctionNode must take no inputs");

        assert!(
            !node.supports_limit_pushdown(),
            "ClickHouseFunctionNode does not support limit pushdown"
        );

        let other = ClickHouseFunctionNode::try_new(plan.clone()).unwrap();
        assert_eq!(node, other, "PartialEq must be consistent");
        assert_eq!(node.partial_cmp(&other), Some(std::cmp::Ordering::Equal), "Same node");
    }
}
