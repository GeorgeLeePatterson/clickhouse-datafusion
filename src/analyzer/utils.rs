use std::str::FromStr;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;

use crate::udfs::clickhouse::CLICKHOUSE_UDF_ALIASES;

// Per expression check
pub(super) fn is_clickhouse_function(expr: &Expr) -> bool {
    if let Expr::ScalarFunction(ScalarFunction { func, args, .. }) = expr
        && CLICKHOUSE_UDF_ALIASES.contains(&func.name())
        && args.len() == 2
    {
        return true;
    }
    false
}

/// Recursive check if expression is a `clickhouse` UDF call
pub(super) fn find_clickhouse_function(expr: &Expr) -> bool {
    let mut found = false;
    use_clickhouse_function_context(expr, |_| {
        found = true;
        Ok(TreeNodeRecursion::Stop)
    })
    .unwrap();
    found
}

/// Recursively traverse the expr, calling the provided function if a `clickhouse` UDF is found.
pub(super) fn use_clickhouse_function_context<F>(expr: &Expr, mut f: F) -> Result<()>
where
    F: FnMut(&Expr) -> Result<TreeNodeRecursion>,
{
    expr.apply(|e| if is_clickhouse_function(e) { f(e) } else { Ok(TreeNodeRecursion::Continue) })
        .map(|_| ())
}

/// Extract inner function and `DataType` from `ClickHouse` UDF call and remove table qualifiers
/// Expected format: `clickhouse(inner_function, 'DataType')`
pub(super) fn extract_function_and_return_type(expr: Expr) -> Result<(Expr, DataType)> {
    let mut data_type = None;
    let new_expr = expr
        .transform_up(|e| {
            if is_clickhouse_function(&e) {
                let Expr::ScalarFunction(clickhouse_func) = e else {
                    // Handled via is_clickhouse_function
                    unreachable!();
                };

                // Last argument should be a string literal containing the DataType
                if let Some(Expr::Literal(
                    ScalarValue::Utf8(Some(type_str))
                    | ScalarValue::Utf8View(Some(type_str))
                    | ScalarValue::LargeUtf8(Some(type_str)),
                    _,
                )) = clickhouse_func.args.last()
                {
                    // Store DataType
                    data_type = Some(DataType::from_str(type_str).map_err(|e| {
                        plan_datafusion_err!(
                            "Invalid ClickHouse function DataType '{type_str}': {e}"
                        )
                    })?);
                } else {
                    return plan_err!(
                        "ClickHouse function second argument must be a string literal DataType"
                    );
                }

                return Ok(Transformed::new(
                    Expr::ScalarFunction(clickhouse_func),
                    true,
                    TreeNodeRecursion::Jump,
                ));
            }
            Ok(Transformed::no(e))
        })?
        .data;
    let Some(data_type) = data_type else {
        return plan_err!("Expected ClickHouse function, no DataType found");
    };
    Ok((new_expr, data_type))
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{ScalarUDF, col};
    use datafusion::prelude::lit;

    use super::*;
    use crate::udfs::clickhouse::clickhouse_udf;

    #[test]
    fn test_is_clickhouse_function_true() {
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_udf()),
            args: vec![lit("count()"), lit("Int64")],
        });
        assert!(is_clickhouse_function(&expr));
    }

    #[test]
    fn test_is_clickhouse_function_false() {
        let expr = col("test_column");
        assert!(!is_clickhouse_function(&expr));
    }

    #[test]
    fn test_is_clickhouse_function_different_function() {
        // Create a different scalar function
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(crate::udfs::eval::ClickHouseEval::new())),
            args: vec![lit("test"), lit("String")],
        });
        assert!(!is_clickhouse_function(&expr));
    }

    #[test]
    fn test_find_clickhouse_function_true() {
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_udf()),
            args: vec![lit("count()"), lit("Int64")],
        });
        assert!(find_clickhouse_function(&expr));
    }

    #[test]
    fn test_find_clickhouse_function_false() {
        let expr = col("test_column");
        assert!(!find_clickhouse_function(&expr));
    }

    #[test]
    fn test_find_clickhouse_function_nested() {
        // Test nested expression with clickhouse function
        let clickhouse_expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_udf()),
            args: vec![lit("count()"), lit("Int64")],
        });
        let nested_expr = clickhouse_expr.clone() + lit(1);
        assert!(find_clickhouse_function(&nested_expr));
    }

    #[test]
    fn test_use_clickhouse_function_context() {
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_udf()),
            args: vec![lit("count()"), lit("Int64")],
        });

        let mut found = false;
        let result = use_clickhouse_function_context(&expr, |_| {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        });

        assert!(result.is_ok());
        assert!(found);
    }

    #[test]
    fn test_use_clickhouse_function_context_not_found() {
        let expr = col("test_column");

        let mut found = false;
        let result = use_clickhouse_function_context(&expr, |_| {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        });

        assert!(result.is_ok());
        assert!(!found);
    }

    #[test]
    fn test_extract_pushed_function_valid() {
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_udf()),
            args: vec![lit("count()"), lit("Int64")],
        });

        let result = extract_function_and_return_type(expr);
        assert!(result.is_ok());
        let (clickhouse_expr, data_type) = result.unwrap();
        assert_eq!(data_type, DataType::Int64);
        // Check that inner expression is the literal
        if let Expr::ScalarFunction(ScalarFunction { args, .. }) = clickhouse_expr {
            if let Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) = args.first() {
                assert_eq!(s, "count()");
            } else {
                panic!("Expected literal expression");
            }
        } else {
            panic!("Expected clickhouse function");
        }
    }

    #[test]
    fn test_extract_pushed_function_invalid_type() {
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_udf()),
            args: vec![lit("count()"), lit("InvalidType")],
        });

        let result = extract_function_and_return_type(expr);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_function_non_clickhouse() {
        let expr = col("test_column");

        let result = extract_function_and_return_type(expr.clone());
        assert!(result.is_err());
    }
}
