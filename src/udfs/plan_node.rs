use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, plan_datafusion_err};
use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::logical_expr::{InvariantLevel, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::sql::unparser::Unparser;

use crate::ClickHouseTableProvider;
use crate::dialect::ClickHouseDialect;
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
}

impl ClickHouseFunctionNode {
    /// Create a new `ClickHouseFunctionNode`
    pub fn new(input: LogicalPlan) -> Self { Self { input } }

    pub(crate) fn execute(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let mut pool = None;
        let _ = self
            .input
            .apply(|plan| {
                if let LogicalPlan::TableScan(scan) = plan {
                    // Convert to TableProvider
                    let provider = source_as_provider(&scan.source)?;
                    if let Some(provider) =
                        provider.as_any().downcast_ref::<ClickHouseTableProvider>()
                    {
                        pool = Some(provider.writer().clone());
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
            // No error thrown
            .unwrap();

        let pool =
            pool.ok_or(plan_datafusion_err!("Expected a ClickHouseTableProvider in the plan"))?;
        let sql = Unparser::new(&ClickHouseDialect).plan_to_sql(&self.input)?.to_string();
        ClickHouseSqlExec::try_new(None, self.input.schema().inner(), pool, sql)
            .map(|e| Arc::new(e) as Arc<dyn ExecutionPlan>)
    }
}

impl UserDefinedLogicalNodeCore for ClickHouseFunctionNode {
    fn name(&self) -> &str { CLICKHOUSE_FUNCTION_NODE_NAME }

    // Pass through to input so optimizations can be applied
    fn inputs(&self) -> Vec<&LogicalPlan> { self.input.inputs() }

    // Pass through to input so optimizations can be applied
    fn schema(&self) -> &DFSchemaRef { self.input.schema() }

    // Pass through to input so optimizations can be applied
    fn expressions(&self) -> Vec<Expr> {
        // Return the functions we're storing - these represent the ClickHouse functions
        // that should be executed remotely
        self.input.expressions()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClickHouseFunctionNode")
    }

    // Pass through to input so optimizations can be applied
    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let new_input = self.input.with_new_exprs(exprs, inputs)?;
        // Create new node with updated expressions and input
        Ok(Self::new(new_input))
    }

    fn check_invariants(&self, _check: InvariantLevel, _plan: &LogicalPlan) -> Result<()> {
        // No invariant checks needed for now
        Ok(())
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // No special column requirements
        None
    }

    fn supports_limit_pushdown(&self) -> bool {
        // ClickHouse supports LIMIT, but for now keep it simple
        false
    }
}

// Implement required traits for LogicalPlan integration
impl PartialEq for ClickHouseFunctionNode {
    fn eq(&self, other: &Self) -> bool { self.input == other.input }
}

impl Eq for ClickHouseFunctionNode {}

impl std::hash::Hash for ClickHouseFunctionNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.input.hash(state); }
}

impl PartialOrd for ClickHouseFunctionNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.input.partial_cmp(&other.input)
    }
}
