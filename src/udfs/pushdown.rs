use std::str::FromStr;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, internal_err, not_impl_err, plan_datafusion_err, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;

pub const CLICKHOUSE_UDF_ALIASES: [&str; 4] =
    ["clickhouse", "clickhouse_udf", "clickhouse_pushdown", "clickhouse_pushdown_udf"];
pub const CLICKHOUSE_FUNCTION_NODE_NAME: &str = "ClickHouseFunctionNode";

pub fn clickhouse_udf_pushdown_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ClickHousePushdownUDF::new())
}

// TODO: Implement nullability arg
#[derive(Debug)]
pub struct ClickHousePushdownUDF {
    signature: Signature,
    aliases:   Vec<String>,
}

impl Default for ClickHousePushdownUDF {
    fn default() -> Self {
        Self {
            signature: Signature::any(Self::ARG_LEN, Volatility::Immutable),
            aliases:   CLICKHOUSE_UDF_ALIASES.iter().map(ToString::to_string).collect(),
        }
    }
}

impl ClickHousePushdownUDF {
    pub const ARG_LEN: usize = 2;

    pub fn new() -> Self { Self::default() }
}

impl ScalarUDFImpl for ClickHousePushdownUDF {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn name(&self) -> &str { CLICKHOUSE_UDF_ALIASES[0] }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type_from_args used")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        super::extract_return_field_from_args(self.name(), &args)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!(
            "ClickHouse UDF is for planning only, a syntax error may have occurred. Sometimes, \
             ClickHouse functions need to be backticked"
        )
    }
}

// Helper functions for identifying ClickHouse functions

// Per expression check
pub(super) fn is_clickhouse_function(expr: &Expr) -> bool {
    if let Expr::ScalarFunction(ScalarFunction { func, args, .. }) = expr {
        if CLICKHOUSE_UDF_ALIASES.contains(&func.name()) && args.len() == 2 {
            return true;
        }
    }
    false
}

/// Recursive check if expression is a `ClickHouse` pushdown UDF call
pub(super) fn find_clickhouse_function(expr: &Expr) -> bool {
    let mut found = false;
    use_clickhouse_function_context(expr, |_| {
        found = true;
        Ok(TreeNodeRecursion::Stop)
    })
    .unwrap();
    found
}

/// Check if expression is a `ClickHouse` pushdown UDF call
pub(super) fn use_clickhouse_function_context<F>(expr: &Expr, mut f: F) -> Result<()>
where
    F: FnMut(&Expr) -> Result<TreeNodeRecursion>,
{
    expr.apply(|e| if is_clickhouse_function(e) { f(e) } else { Ok(TreeNodeRecursion::Continue) })
        .map(|_| ())
}

/// Extract inner function and `DataType` from `ClickHouse` UDF call and remove table qualifiers
/// Expected format: `clickhouse(inner_function, 'DataType')`
pub(super) fn extract_pushed_function(expr: Expr) -> Result<(Expr, DataType)> {
    let mut data_type = None;
    let new_expr = expr
        .transform_up(|e| {
            if is_clickhouse_function(&e) {
                let Expr::ScalarFunction(ScalarFunction { mut args, .. }) = e else {
                    return plan_err!("Not a valid ClickHouse function call");
                };
                // Second argument should be a string literal containing the DataType
                let Expr::Literal(
                    ScalarValue::Utf8(Some(type_str))
                    | ScalarValue::Utf8View(Some(type_str))
                    | ScalarValue::LargeUtf8(Some(type_str)),
                    _,
                ) = &args[1]
                else {
                    return plan_err!(
                        "ClickHouse function second argument must be a string literal DataType"
                    );
                };
                data_type = Some(DataType::from_str(type_str).map_err(|e| {
                    plan_datafusion_err!("Invalid ClickHouse function DataType '{type_str}': {e}")
                })?);
                let inner_func = args
                    .remove(0)
                    .transform_up(|c| {
                        if let Expr::Column(column) = c {
                            Ok(Transformed::new(
                                Expr::Column(Column::new_unqualified(column.name)),
                                true,
                                TreeNodeRecursion::Jump,
                            ))
                        } else {
                            Ok(Transformed::no(c))
                        }
                    })?
                    .data;
                return Ok(Transformed::new(inner_func, true, TreeNodeRecursion::Jump));
            }
            Ok(Transformed::no(e))
        })?
        .data;
    let Some(data_type) = data_type else {
        return plan_err!("Expected ClickHouse function, no DataType found");
    };
    Ok((new_expr, data_type))
}

/// When wrapping plan ready clickhouse functions for execution on the remote `ClickHouse` instance.
pub(super) fn unwrap_clickhouse_function(expr: Expr) -> Result<Transformed<Expr>> {
    expr.transform_down(|e| {
        if is_clickhouse_function(&e) {
            let alias = e.schema_name().to_string();
            let Expr::ScalarFunction(ScalarFunction { mut args, .. }) = e else {
                // Guaranteed by call to `is_clickhouse_function`
                unreachable!()
            };
            return Ok(Transformed::new(
                args.remove(0).alias(alias),
                true,
                TreeNodeRecursion::Jump,
            ));
        }
        Ok(Transformed::no(e))
    })
}
