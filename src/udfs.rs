//! Various UDFs providing `DataFusion`'s sql parsing with some `ClickHouse` specific functionality.
//!
//! [`self::function::ClickHouseFunc`] is a sort of 'escape-hatch' to allow passing syntax directly
//! to `ClickHouse` as SQL.
pub mod apply;
pub mod clickhouse;
pub mod eval;
pub mod placeholder;

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
    ctx.register_udf(eval::clickhouse_eval_udf());
    ctx.register_udf(clickhouse::clickhouse_udf());
    ctx.register_udf(apply::clickhouse_apply_udf());
}

/// Helper function to extract return [`DataType`] from second UDF arg
fn extract_return_field_from_args(name: &str, args: &ReturnFieldArgs<'_>) -> Result<FieldRef> {
    if let Some(Some(
        ScalarValue::Utf8(Some(return_type_str))
        | ScalarValue::Utf8View(Some(return_type_str))
        | ScalarValue::LargeUtf8(Some(return_type_str)),
    )) = &args.scalar_arguments.last()
    {
        let dt = DataType::from_str(return_type_str.as_str())
            .map_err(|e| plan_datafusion_err!("Invalid return type for {name}: {e}"))?;
        Ok(udf_field_from_fields(name, dt, args.arg_fields))
    } else {
        plan_err!("Expected return type literal in scalar arguments for {name}")
    }
}

fn udf_field_from_fields(name: &str, dt: DataType, fields: &[FieldRef]) -> FieldRef {
    // Apply/lambda will be indicated by placeholder fields. These are flagged as nullable by
    // DataFusion during expr creation. But, the return field should only be nullable if any of the
    // columns the placeholders refer to are nullable.
    //
    // Apply functions have at least 3 args.
    let mut placeholder_nullable = false;
    if fields.len() >= 3 && fields.first().is_some_and(|f| f.name().starts_with('$')) {
        let rev_fields = fields.iter().rev();
        for (pl, col) in fields.iter().zip(rev_fields) {
            if pl.name().starts_with('$') {
                placeholder_nullable |= col.is_nullable();
            } else {
                break;
            }
        }
        return Field::new(name, dt, placeholder_nullable).into();
    }

    // Otherwise determine from the provided args

    // Treat array returns differently. ClickHouse doesn't support nullable arrays.
    let nullable = fields.iter().any(|a| {
        !matches!(
            a.data_type(),
            &DataType::List(_) | &DataType::ListView(_) | &DataType::LargeList(_)
        ) && a.is_nullable()
    });
    Field::new(name, dt, nullable).into()
}

pub mod functions {
    //! List of functions to create the various `ClickHouse`-specific UDFs.
    use datafusion::common::Column;
    use datafusion::logical_expr::expr::Placeholder;
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;

    /// Create a `ClickHouse` UDF to be 'evaluated' on the `ClickHouse` server.
    pub fn clickhouse_eval(expr: impl Into<String>, return_type: &str) -> Expr {
        super::eval::clickhouse_eval_udf().call(vec![
            Expr::Literal(ScalarValue::Utf8(Some(expr.into())), None),
            Expr::Literal(ScalarValue::Utf8(Some(return_type.to_string())), None),
        ])
    }

    /// Create a `ClickHouse` UDF that will be executed on the `ClickHouse` server.
    pub fn clickhouse(expr: Expr, return_type: &str) -> Expr {
        super::clickhouse::clickhouse_udf()
            .call(vec![expr, Expr::Literal(ScalarValue::Utf8(Some(return_type.to_string())), None)])
    }

    /// Create a `ClickHouse` Higher Order Function UDF that will be executed on the `ClickHouse`
    /// server.
    pub fn apply<C: IntoIterator<Item = Column>>(
        expr: Expr,
        columns: C,
        return_type: &str,
    ) -> Expr {
        let (mut args, columns): (Vec<_>, Vec<_>) = columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                (
                    Expr::Placeholder(Placeholder { id: format!("x{i}"), data_type: None }),
                    Expr::Column(c),
                )
            })
            .unzip();
        args.push(expr);
        args.extend(columns);
        let apply_udf = super::apply::clickhouse_apply_udf().call(args);
        clickhouse(apply_udf, return_type)
    }

    /// Alias for [`self::apply`]
    pub fn lambda<C: IntoIterator<Item = Column>>(
        expr: Expr,
        columns: C,
        return_type: &str,
    ) -> Expr {
        apply(expr, columns, return_type)
    }

    /// Alias for [`self::apply`]
    pub fn clickhouse_apply<C: IntoIterator<Item = Column>>(
        expr: Expr,
        columns: C,
        return_type: &str,
    ) -> Expr {
        apply(expr, columns, return_type)
    }

    /// Alias for [`self::apply`]
    pub fn clickhouse_lambda<C: IntoIterator<Item = Column>>(
        expr: Expr,
        columns: C,
        return_type: &str,
    ) -> Expr {
        apply(expr, columns, return_type)
    }

    /// Alias for [`self::apply`]
    pub fn clickhouse_map<C: IntoIterator<Item = Column>>(
        expr: Expr,
        columns: C,
        return_type: &str,
    ) -> Expr {
        apply(expr, columns, return_type)
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;

        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::expr::ScalarFunction;
        use datafusion::prelude::{Expr, lit};

        use super::*;
        use crate::prelude::clickhouse_eval_udf;
        use crate::udfs::apply::clickhouse_apply_udf;
        use crate::udfs::clickhouse::clickhouse_udf;
        use crate::udfs::functions::clickhouse_eval;

        #[test]
        fn test_create_simple_udf() {
            assert_eq!(
                clickhouse_eval("count(*)", "UInt64"),
                Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(clickhouse_eval_udf()),
                    args: vec![
                        Expr::Literal(ScalarValue::Utf8(Some("count(*)".to_string())), None),
                        Expr::Literal(ScalarValue::Utf8(Some("UInt64".to_string())), None),
                    ],
                })
            );
        }

        #[test]
        fn test_clickhouse_udf() {
            assert_eq!(
                clickhouse(
                    Expr::Literal(ScalarValue::Utf8(Some("count(*)".to_string())), None),
                    "UInt64"
                ),
                Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(clickhouse_udf()),
                    args: vec![
                        Expr::Literal(ScalarValue::Utf8(Some("count(*)".to_string())), None),
                        Expr::Literal(ScalarValue::Utf8(Some("UInt64".to_string())), None),
                    ],
                })
            );
        }

        #[test]
        fn test_clickhouse_apply_udf() {
            let expr = Expr::Column(Column::from_name("id")) + lit(5);
            let columns = vec![Column::from_name("id")];
            let return_type = "UInt64";
            let apply_expr = apply(expr.clone(), columns.clone(), return_type);
            assert_eq!(
                apply_expr,
                Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(clickhouse_udf()),
                    args: vec![
                        // apply is a HOF
                        Expr::ScalarFunction(ScalarFunction {
                            func: Arc::new(clickhouse_apply_udf()),
                            args: vec![
                                Expr::Placeholder(Placeholder {
                                    id:        "x0".to_string(),
                                    data_type: None,
                                }),
                                Expr::Column(Column::from_name("id")) + lit(5),
                                Expr::Column(Column::from_name("id")),
                            ],
                        }),
                        Expr::Literal(ScalarValue::Utf8(Some("UInt64".to_string())), None),
                    ],
                })
            );

            let lambda_expr = lambda(expr.clone(), columns.clone(), return_type);
            let ch_apply_expr = clickhouse_apply(expr.clone(), columns.clone(), return_type);
            let ch_lambda_expr = clickhouse_lambda(expr.clone(), columns.clone(), return_type);
            let ch_map_expr = clickhouse_map(expr.clone(), columns.clone(), return_type);
            assert_eq!(apply_expr, lambda_expr);
            assert_eq!(apply_expr, ch_apply_expr);
            assert_eq!(apply_expr, ch_lambda_expr);
            assert_eq!(apply_expr, ch_map_expr);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::ReturnFieldArgs;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[test]
    fn test_register_clickhouse_functions() {
        let ctx = SessionContext::new();
        register_clickhouse_functions(&ctx);

        // Check that the clickhouse function was registered
        let state = ctx.state();
        let functions = state.scalar_functions();
        assert!(functions.contains_key("clickhouse_eval"));
        assert!(functions.contains_key("clickhouse"));
        assert!(functions.contains_key("apply"));
    }

    #[test]
    fn test_extract_return_field_from_args_utf8() {
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8, false));
        let scalar = [
            Some(ScalarValue::Utf8(Some("count()".to_string()))),
            Some(ScalarValue::Utf8(Some("Int64".to_string()))),
        ];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };
        let result = extract_return_field_from_args("test_func", &args);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.name(), "test_func");
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(!field.is_nullable());
    }

    #[test]
    fn test_extract_return_field_from_args_utf8_view() {
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8View, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8View, false));
        let scalar = [
            Some(ScalarValue::Utf8View(Some("sum(x)".to_string()))),
            Some(ScalarValue::Utf8View(Some("Float64".to_string()))),
        ];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = extract_return_field_from_args("test_func", &args);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_extract_return_field_from_args_large_utf8() {
        let field1 = Arc::new(Field::new("syntax", DataType::LargeUtf8, false));
        let field2 = Arc::new(Field::new("type", DataType::LargeUtf8, false));
        let scalar = [
            Some(ScalarValue::LargeUtf8(Some("avg(y)".to_string()))),
            Some(ScalarValue::LargeUtf8(Some("Boolean".to_string()))),
        ];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = extract_return_field_from_args("test_func", &args);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_extract_return_field_from_args_invalid_type() {
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8, false));
        let scalar = [
            Some(ScalarValue::Utf8(Some("count()".to_string()))),
            Some(ScalarValue::Utf8(Some("InvalidDataType".to_string()))),
        ];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = extract_return_field_from_args("test_func", &args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid return type"));
    }

    #[test]
    fn test_extract_return_field_from_args_no_last_arg() {
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let args = ReturnFieldArgs { arg_fields: &[field1], scalar_arguments: &[] };

        let result = extract_return_field_from_args("test_func", &args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected return type"));
    }

    #[test]
    fn test_extract_return_field_from_args_null_last_arg() {
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8, false));
        let scalar = [Some(ScalarValue::Utf8(Some("count()".to_string()))), None];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = extract_return_field_from_args("test_func", &args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected return type"));
    }

    #[test]
    fn test_extract_return_field_from_args_non_string_last_arg() {
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Int32, false));
        let scalar = [
            Some(ScalarValue::Utf8(Some("count()".to_string()))),
            Some(ScalarValue::Int32(Some(42))),
        ];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = extract_return_field_from_args("test_func", &args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected return type"));
    }

    #[test]
    fn test_extract_return_field_from_args_empty_string_last_arg() {
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8, false));
        let scalar = [
            Some(ScalarValue::Utf8(Some("count()".to_string()))),
            Some(ScalarValue::Utf8(Some(String::new()))),
        ];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = extract_return_field_from_args("test_func", &args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid return type"));
    }

    #[test]
    fn test_extract_return_field_from_args_null_string_last_arg() {
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8, false));
        let scalar =
            [Some(ScalarValue::Utf8(Some("count()".to_string()))), Some(ScalarValue::Utf8(None))];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = extract_return_field_from_args("test_func", &args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected return type"));
    }
}
