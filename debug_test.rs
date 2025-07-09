use std::sync::Arc;
use datafusion::logical_expr::{LogicalPlan, TableScan, Extension};
use clickhouse_datafusion::udfs::pushdown_analyzer::ClickHouseFunctionNode;

fn main() {
    // Test the pattern matching
    let test_scan = TableScan {
        table_name: datafusion::sql::TableReference::bare("test"),
        source: Arc::new(datafusion::datasource::MemTable::try_new(
            Arc::new(datafusion::arrow::datatypes::Schema::empty()),
            vec![]
        ).unwrap()),
        projection: None,
        projected_schema: Arc::new(datafusion::common::DFSchema::empty()),
        filters: vec![],
        fetch: None,
    };
    
    let plan = LogicalPlan::TableScan(test_scan);
    
    // This should match
    if let LogicalPlan::TableScan(scan) = &plan {
        println!("Pattern matched successfully!");
        println!("Table name: {:?}", scan.table_name);
    } else {
        println!("Pattern did not match");
    }
    
    // Now test with inputs()
    let inputs = vec![&plan];
    if let Some(LogicalPlan::TableScan(scan)) = inputs.first() {
        println!("First input pattern matched!");
        println!("Table name: {:?}", scan.table_name);
    } else {
        println!("First input pattern did not match");
    }
}