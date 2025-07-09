//! Various UDFs providing `DataFusion`'s sql parsing with some `ClickHouse` specific functionality.
//!
//! [`self::function::ClickHouseFunc`] is a sort of 'escape-hatch' to allow passing
//! syntax directly to `ClickHouse` as SQL.

// pub mod analyzer;
pub mod lambda;
pub mod placeholder;
pub mod plan_node;
pub mod planner;
pub mod pushdown;
pub mod pushdown_analyzer;
pub mod simple;
// pub mod visitor;

use std::str::FromStr;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::ReturnFieldArgs;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

// TODO: Docs - explain how this registers the best-effort UDF that can be used when the full
// `ClickHouseQueryPlanner` is not available.
//
/// Registers `ClickHouse`-specific UDFs with the provided [`SessionContext`].
pub fn register_clickhouse_functions(ctx: &SessionContext) {
    ctx.register_udf(simple::clickhouse_func_udf());
}

// Helper function to extract return DataType from second UDF arg
fn extract_return_field_from_args(name: &str, args: &ReturnFieldArgs<'_>) -> Result<FieldRef> {
    if let Some(Some(
        ScalarValue::Utf8(Some(return_type_str))
        | ScalarValue::Utf8View(Some(return_type_str))
        | ScalarValue::LargeUtf8(Some(return_type_str)),
    )) = &args.scalar_arguments.last()
    {
        let dt = DataType::from_str(return_type_str.as_str())
            .map_err(|e| plan_datafusion_err!("Invalid return type: {e}"))?;
        Ok(Field::new(name, dt, false).into())
    } else {
        plan_err!("Expected Utf8 literal for return type")
    }
}
