use std::sync::Arc;

use datafusion::common::{DFSchemaRef, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::{InvariantLevel, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::Expr;

pub const CLICKHOUSE_FUNCTION_NODE_NAME: &str = "ClickHouseFunctionNode";

/// Extension node for ClickHouse function pushdown
/// Stores functions that should be executed on ClickHouse and persists through DataFusion
/// optimization
#[derive(Clone, Debug)]
pub struct ClickHouseFunctionNode {
    /// The input plan that this node wraps
    input:     LogicalPlan,
    /// Functions that should be executed on ClickHouse (currently exp() functions for testing)
    functions: Vec<Expr>,
    /// Computed schema for this node
    schema:    DFSchemaRef,
}

impl ClickHouseFunctionNode {
    /// Create a new ClickHouseFunctionNode
    pub fn try_new(input: LogicalPlan, functions: Vec<Expr>) -> Result<Self> {
        // For now, use the input's schema as-is
        // In the future, this will be computed based on function return types
        let schema = Arc::clone(input.schema());

        eprintln!("🏗️  CREATING EXTENSION NODE:");
        eprintln!("   📦 Input plan: {}", input.display_indent());
        eprintln!("   🔧 Functions: {}", functions.len());
        for (i, func) in functions.iter().enumerate() {
            eprintln!("   🔧 Function[{}]: {}", i, func);
        }
        eprintln!("   📊 Schema columns: {:?}", schema.columns());

        Ok(Self { input, functions, schema })
    }
}

impl UserDefinedLogicalNodeCore for ClickHouseFunctionNode {
    fn name(&self) -> &str { CLICKHOUSE_FUNCTION_NODE_NAME }

    fn inputs(&self) -> Vec<&LogicalPlan> { vec![&self.input] }

    fn schema(&self) -> &DFSchemaRef { &self.schema }

    fn expressions(&self) -> Vec<Expr> {
        // Return the functions we're storing - these represent the ClickHouse functions
        // that should be executed remotely
        self.functions.clone()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClickHouseFunctionNode: {} functions", self.functions.len())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        // Critical for optimization persistence!
        eprintln!("🔄 WITH_EXPRS_AND_INPUTS called:");
        eprintln!("   📝 New expressions: {}", exprs.len());
        eprintln!("   📦 New inputs: {}", inputs.len());

        if inputs.len() != 1 {
            return plan_err!(
                "ClickHouseFunctionNode expects exactly one input, got {}",
                inputs.len()
            );
        }

        let input = inputs.swap_remove(0);

        // Check if anything actually changed to avoid unnecessary cloning
        if exprs == self.functions && input == self.input {
            eprintln!("   ✅ No changes detected, returning clone");
            return Ok(self.clone());
        }

        eprintln!("   🔄 Changes detected, creating new node");

        // Create new node with updated expressions and input
        Self::try_new(input, exprs)
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
    fn eq(&self, other: &Self) -> bool {
        self.functions == other.functions && self.input == other.input
    }
}

impl Eq for ClickHouseFunctionNode {}

impl std::hash::Hash for ClickHouseFunctionNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Note: We don't hash input as it doesn't implement Hash
        // We hash the function count and their string representations as a proxy
        self.functions.len().hash(state);
        for func in &self.functions {
            func.to_string().hash(state);
        }
    }
}

impl PartialOrd for ClickHouseFunctionNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl Ord for ClickHouseFunctionNode {
    // TODO: Remove - this is WAY to costly for an Ord impl
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare function counts first, then function string representations
        match self.functions.len().cmp(&other.functions.len()) {
            std::cmp::Ordering::Equal => {
                // Compare function string representations
                let self_funcs: Vec<String> =
                    self.functions.iter().map(|f| f.to_string()).collect();
                let other_funcs: Vec<String> =
                    other.functions.iter().map(|f| f.to_string()).collect();
                self_funcs.cmp(&other_funcs)
            }
            other => other,
        }
    }
}
