// use std::sync::Arc;

// use datafusion::catalog::TableProvider;
// use datafusion::common::{plan_datafusion_err, plan_err};
// use datafusion::datasource::source_as_provider;
// use datafusion::error::Result;
// use datafusion::execution::SessionState;
// use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
// use datafusion::physical_plan::ExecutionPlan;
// use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

// use super::clickhouse_plan_node::{CLICKHOUSE_FUNCTION_NODE_NAME, ClickHouseFunctionNode};
// use crate::table_provider::ClickHouseTableProvider;

// // TODO: Docs - This actually executes the custom `ClickHouseFunctionNode`
// `UserDefinedLogicalNode`. #[derive(Clone, Copy, Debug)]
// pub struct ClickHouseExtensionPlanner;

// #[async_trait::async_trait]
// impl ExtensionPlanner for ClickHouseExtensionPlanner {
//     async fn plan_extension(
//         &self,
//         _planner: &dyn PhysicalPlanner,
//         node: &dyn UserDefinedLogicalNode,
//         _logical_inputs: &[&LogicalPlan],
//         _physical_inputs: &[Arc<dyn ExecutionPlan>],
//         session_state: &SessionState,
//     ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
//         if node.name() == CLICKHOUSE_FUNCTION_NODE_NAME {
//             let clickhouse_node = node
//                 .as_any()
//                 .downcast_ref::<ClickHouseFunctionNode>()
//                 .ok_or(plan_datafusion_err!("Failed to downcast to ClickHouseFunctionNode"))?;

//             let inputs = clickhouse_node.inputs();
//             let Some(&LogicalPlan::TableScan(scan)) = inputs.first() else {
//                 return plan_err!("Expected a TableScan logical plan");
//             };

//             // Convert to TableProvider
//             let provider = source_as_provider(&scan.source)?;
//             let provider = provider
//                 .as_any()
//                 .downcast_ref::<ClickHouseTableProvider>()
//                 .ok_or(plan_datafusion_err!("Expected a ClickHouseTableProvider for input"))?;
//             let table = scan.table_name.clone();
//             let projection = scan.projection.as_ref();
//             let filters = scan.filters.as_ref();
//             let limit = scan.fetch;
//             let writer = provider.writer().clone();
//             let expressions = node.expressions();
//             let schema = node.schema();

//             // Create a new ClickHouseTableProvider with exprs
//             let new_provider = Arc::new(ClickHouseTableProvider::new_with_schema_and_exprs(
//                 writer,
//                 table,
//                 Arc::new(schema.as_arrow().clone()),
//                 expressions,
//             ));

//             new_provider.scan(session_state, projection, filters, limit).await.map(Some)
//         } else {
//             Ok(None)
//         }
//     }
// }
