// TODO: Remove - Important! explain how `apply` and `lambda` are only used when nested
//! `ScalarUDFImpl` for [`ClickHouseApplyUDF`]
//!
//! Currently this provides little value over using a `ClickHouse` lambda function directly in
//! [`super::clickhouse::ClickHouseUDF`] since both will be parsed the same. This UDF will be
//! expanded to allow using it directly similarly to the `clickhouse` function.
use std::collections::HashMap;
use std::str::FromStr;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, not_impl_err, plan_datafusion_err, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::expr::{Placeholder, ScalarFunction};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::unparser::Unparser;

use super::udf_field_from_fields;

pub const CLICKHOUSE_APPLY_ALIASES: [&str; 7] = [
    "apply",
    "lambda",
    "clickhouse_apply",
    "clickhouse_lambda",
    "clickhouse_map",
    "clickhouse_fmap",
    "clickhouse_hof",
];

pub fn clickhouse_apply_udf() -> ScalarUDF { ScalarUDF::new_from_impl(ClickHouseApplyUDF::new()) }

#[derive(Debug)]
pub struct ClickHouseApplyUDF {
    signature: Signature,
    aliases:   Vec<String>,
}

impl Default for ClickHouseApplyUDF {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases:   CLICKHOUSE_APPLY_ALIASES.iter().map(ToString::to_string).collect(),
        }
    }
}

impl ClickHouseApplyUDF {
    pub fn new() -> Self { Self::default() }
}

impl ScalarUDFImpl for ClickHouseApplyUDF {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn name(&self) -> &str { CLICKHOUSE_APPLY_ALIASES[0] }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types
            .last()
            .cloned()
            .ok_or(plan_datafusion_err!("ClickHouseApplyUDF requires at least one argument"))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        if let Ok(ret) = super::extract_return_field_from_args(self.name(), &args) {
            Ok(ret)
        } else {
            let data_types =
                args.arg_fields.iter().map(|f| f.data_type()).cloned().collect::<Vec<_>>();
            let return_type = self.return_type(&data_types)?;
            Ok(udf_field_from_fields(self.name(), return_type, args.arg_fields))
        }
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!(
            "ClickHouseApplyUDF is for planning only - lambda functions are pushed down to \
             ClickHouse"
        )
    }

    /// Set to true to prevent optimizations. There is no way to know what the function will
    /// produce, so these settings must be conservative.
    fn short_circuits(&self) -> bool { true }
}

pub(crate) struct ClickHouseApplyRewriter {
    pub name:      String,
    pub body:      Expr,
    pub param_map: HashMap<Placeholder, Column>,
}

impl ClickHouseApplyRewriter {
    pub(crate) fn try_new(expr: &Expr) -> Result<Self> {
        if !is_clickhouse_lambda(expr) {
            return plan_err!("Unknown function passed to ClickHouseApplyRewriter");
        }

        let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
            // Guaranteed by `is_clickhouse_lambda`
            unreachable!();
        };

        // Unwrap the aliased function if it's wrapped
        let (name, mut args) = if CLICKHOUSE_APPLY_ALIASES.contains(&func.name()) {
            let Some(Expr::ScalarFunction(ScalarFunction { func, args })) = args.first() else {
                return plan_err!("ClickHouseApplyUDF must be higher order function");
            };
            (func.name().to_string(), args.clone())
        } else {
            (func.name().to_string(), args.clone())
        };

        // Attempt to extract data type from the last argument, unused
        let _data_type = args
            .pop_if(|expr| matches!(expr, Expr::Literal(_, _)))
            .map(|expr| match expr.as_literal() {
                Some(
                    ScalarValue::Utf8(Some(ret))
                    | ScalarValue::Utf8View(Some(ret))
                    | ScalarValue::LargeUtf8(Some(ret)),
                ) => DataType::from_str(ret.as_str())
                    .map_err(|e| plan_datafusion_err!("Invalid return type: {e}"))
                    .map(Some),
                _ => Ok(None),
            })
            .transpose()?
            .flatten();

        let (param_map, body) = extract_apply_args(args)?;
        Ok(Self { name, body, param_map })
    }

    pub(crate) fn rewrite_to_ast(self, unparser: &Unparser<'_>) -> Result<ast::Expr> {
        let Self { name, body, param_map, .. } = self;

        // Transform the body expression, replacing columns with parameters
        let transformed_body = body
            .transform(|expr| {
                if let Expr::Placeholder(ref placeholder) = expr
                    && let Some((param_name, _)) =
                        param_map.iter().find(|(p, _)| p.id == placeholder.id)
                {
                    let variable = param_name.id.trim_start_matches('$');
                    // Use unqualified column which should unparse without quotes
                    return Ok(Transformed::new(
                        Expr::Column(Column::new_unqualified(variable)),
                        true,
                        TreeNodeRecursion::Jump,
                    ));
                }
                Ok(Transformed::no(expr))
            })
            .unwrap()
            .data;

        // Convert body to SQL
        let body_sql = unparser.expr_to_sql(&transformed_body)?;

        // Strip all '$' from param names
        let (mut params, mut columns): (Vec<_>, Vec<_>) = param_map
            .into_iter()
            .map(|(p, c)| (p.id.trim_start_matches('$').to_string(), c))
            .unzip();

        // Create lambda function parameters
        let lambda_params = if params.len() == 1 {
            ast::OneOrManyWithParens::One(ast::Ident::new(params.remove(0)))
        } else {
            ast::OneOrManyWithParens::Many(params.into_iter().map(ast::Ident::new).collect())
        };

        let column_params = if columns.len() == 1 {
            let col = columns.remove(0);
            vec![ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                unparser
                    .expr_to_sql(&Expr::Column(col.clone()))
                    .unwrap_or_else(|_| ast::Expr::Identifier(ast::Ident::new(&col.name))),
            ))]
        } else {
            columns
                .into_iter()
                .map(|c| {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                        unparser
                            .expr_to_sql(&Expr::Column(c.clone()))
                            .unwrap_or_else(|_| ast::Expr::Identifier(ast::Ident::new(&c.name))),
                    ))
                })
                .collect::<Vec<_>>()
        };

        // Create the lambda expression
        let lambda_expr = ast::Expr::Lambda(ast::LambdaFunction {
            params: lambda_params,
            body:   Box::new(body_sql),
        });

        // Now create the higher-order function call with the lambda and original
        // columns
        let hof_args: Vec<ast::FunctionArg> = std::iter::once(
            // First arg is the lambda
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(lambda_expr)),
        )
        .chain(column_params)
        .collect();

        Ok(ast::Expr::Function(ast::Function {
            name:             ast::ObjectName(vec![ast::ObjectNamePart::Identifier(
                ast::Ident::new(name),
            )]),
            args:             ast::FunctionArguments::List(ast::FunctionArgumentList {
                duplicate_treatment: None,
                args:                hof_args,
                clauses:             vec![],
            }),
            filter:           None,
            null_treatment:   None,
            over:             None,
            within_group:     vec![],
            parameters:       ast::FunctionArguments::None,
            uses_odbc_syntax: false,
        }))
    }
}

pub(crate) fn is_clickhouse_lambda(expr: &Expr) -> bool {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return false;
    };
    CLICKHOUSE_APPLY_ALIASES.contains(&func.name())
        || args.first().is_some_and(|a| matches!(a, Expr::Placeholder(_)))
}

pub(crate) fn extract_apply_args(
    mut args: Vec<Expr>,
) -> Result<(HashMap<Placeholder, Column>, Expr)> {
    if args.len() < 3 {
        return plan_err!(
            "ClickHouseApplyUDF requires at least 3 arguments: placeholders, body, and column \
             references"
        );
    }

    let mut columns = Vec::with_capacity(args.len());

    // Pull out columns and body
    let body = loop {
        match args.pop() {
            Some(Expr::Column(col)) => columns.push(col),
            Some(e) => break e,
            None => {
                return plan_err!("ClickHouseApplyUDF missing body expression");
            }
        }
    };

    // Finally confirm placeholders
    let placeholders = args
        .into_iter()
        .map(
            |e| if let Expr::Placeholder(p) = e { Ok(p) } else { plan_err!("Invalid placeholder") },
        )
        .collect::<Result<Vec<_>>>()?;

    if columns.len() != placeholders.len() {
        return plan_err!("Number of placeholders and columns must match");
    }

    let param_map = placeholders.into_iter().zip(columns).collect::<HashMap<_, _>>();

    Ok((param_map, body))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::*;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{BinaryExpr, Operator, ReturnFieldArgs, ScalarFunctionArgs};
    use datafusion::prelude::lit;
    use datafusion::sql::TableReference;

    use super::*;
    use crate::udfs::placeholder::{PlaceholderUDF, placeholder_udf_from_placeholder};

    #[test]
    fn test_apply_udf() {
        let udf = clickhouse_apply_udf();

        // Ensure short circuits
        assert!(udf.short_circuits());

        // Test that the return field will return the data type if it is passed in
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

        let result = udf.return_field_from_args(args);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.name(), CLICKHOUSE_APPLY_ALIASES[0]);
        assert_eq!(field.data_type(), &DataType::Int64);

        // Test that invoking will fail
        let args = ScalarFunctionArgs {
            args:         vec![],
            arg_fields:   vec![],
            number_rows:  1,
            return_field: Arc::new(Field::new("", DataType::Int32, false)),
        };
        let result = udf.invoke_with_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("planning only"));
    }

    #[test]
    fn test_apply_rewriter() {
        let placeholder = Placeholder::new("$x".to_string(), None);

        // Ensure `extract_apply_args` works correctly
        let result = extract_apply_args(vec![Expr::Placeholder(placeholder.clone())]);
        assert!(result.is_err(), "Apply expects at least 3 args");
        let result = extract_apply_args(vec![Expr::Column(Column::from_name("test"))]);
        assert!(result.is_err(), "Apply expects a body arg before columns");
        let exprs_fail = vec![
            Expr::Placeholder(placeholder.clone()),
            lit("1"),
            Expr::Column(Column::from_name("test1")),
            Expr::Column(Column::from_name("test2")),
        ];
        let result = extract_apply_args(exprs_fail);
        assert!(result.is_err(), "Placeholder count must match column count");

        let common_args = vec![
            Expr::Placeholder(placeholder.clone()),
            Expr::BinaryExpr(BinaryExpr {
                left:  Box::new(Expr::Placeholder(placeholder)),
                op:    Operator::Plus,
                right: Box::new(lit(1)),
            }),
            Expr::Column(Column::new(None::<TableReference>, "test_col")),
            lit("Int64"),
        ];

        let expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_apply_udf()),
            args: common_args.clone(),
        });

        let result = ClickHouseApplyRewriter::try_new(&expr);
        assert!(result.is_err(), "Apply/Lambda must be a higher order function");

        // Modify expr to be HOF

        let expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(clickhouse_apply_udf()),
            args: vec![Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(placeholder_udf_from_placeholder(PlaceholderUDF::new("arrayMap"))),
                args: common_args.clone(),
            })],
        });

        let result = ClickHouseApplyRewriter::try_new(&expr);
        assert!(result.is_ok(), "Apply/Lambda expected to be higher order function");
    }
}
