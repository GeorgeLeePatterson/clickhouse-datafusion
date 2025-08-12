//! An `ExtensionPlanner` implementation for executing [`ClickHouseFunctionNode`]s
use std::sync::Arc;

use datafusion::common::plan_datafusion_err;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use super::plan_node::{CLICKHOUSE_FUNCTION_NODE_NAME, ClickHouseFunctionNode};

// TODO: Docs - This actually executes the custom `ClickHouseFunctionNode` `UserDefinedLogicalNode`.
#[derive(Clone, Copy, Debug)]
pub struct ClickHouseExtensionPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for ClickHouseExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if node.name() == CLICKHOUSE_FUNCTION_NODE_NAME {
            let clickhouse_node = node
                .as_any()
                .downcast_ref::<ClickHouseFunctionNode>()
                .ok_or(plan_datafusion_err!("Failed to downcast to ClickHouseFunctionNode"))?;
            clickhouse_node.execute().map(Some)
        } else {
            Ok(None)
        }
    }
}
