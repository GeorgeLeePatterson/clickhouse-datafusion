use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;
use datafusion::sql::sqlparser::{ast, dialect};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::Dialect as UnparserDialect;

use crate::udfs::simple::CLICKHOUSE_FUNC_ALIASES;

// TODO: Docs - where is this used?
//
/// A custom [`UnparserDialect`] for `ClickHouse`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ClickHouseDialect;

impl UnparserDialect for ClickHouseDialect {
    fn identifier_quote_style(&self, _: &str) -> Option<char> { Some('`') }

    fn scalar_function_to_sql_overrides(
        &self,
        _unparser: &Unparser<'_>,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        if CLICKHOUSE_FUNC_ALIASES.contains(&func_name) {
            if let Some(Expr::Literal(
                ScalarValue::Utf8(Some(s))
                | ScalarValue::Utf8View(Some(s))
                | ScalarValue::LargeUtf8(Some(s)),
                _,
            )) = args.first()
            {
                if s.is_empty() {
                    return plan_err!("`clickhouse` syntax argument cannot be empty");
                }

                // Tokenize the string with ClickHouseDialect
                let mut tokenizer = Tokenizer::new(&dialect::ClickHouseDialect {}, s);
                let tokens = tokenizer.tokenize().map_err(|e| {
                    plan_datafusion_err!("Failed to tokenize ClickHouse expression '{s}': {e}")
                })?;
                // Create a Parser instance
                let mut parser = Parser::new(&dialect::ClickHouseDialect {}).with_tokens(tokens);
                Ok(Some(parser.parse_expr().map_err(|e| {
                    plan_datafusion_err!("Invalid ClickHouse expression '{s}': {e}")
                })?))
            } else {
                plan_err!(
                    "`clickhouse` expects a string literal syntax argument, found: {:?}",
                    args[0]
                )
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use datafusion::sql::unparser::Unparser;

    use super::*;

    #[test]
    fn test_identifier_quote_style() {
        let dialect = ClickHouseDialect;
        assert_eq!(dialect.identifier_quote_style("test"), Some('`'));
        assert_eq!(dialect.identifier_quote_style(""), Some('`'));
    }

    #[test]
    fn test_scalar_function_to_sql_overrides_clickhouse_func() {
        let dialect = ClickHouseDialect;
        let unparser = Unparser::new(&dialect);

        // Test valid clickhouse_func function with string literal
        let args = vec![Expr::Literal(ScalarValue::Utf8(Some("count()".to_string())), None)];
        let result = dialect.scalar_function_to_sql_overrides(&unparser, "clickhouse_func", &args);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_scalar_function_to_sql_overrides_clickhouse_func_utf8view() {
        let dialect = ClickHouseDialect;
        let unparser = Unparser::new(&dialect);

        // Test with Utf8View
        let args = vec![Expr::Literal(ScalarValue::Utf8View(Some("sum(x)".to_string())), None)];
        let result = dialect.scalar_function_to_sql_overrides(&unparser, "clickhouse_func", &args);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_scalar_function_to_sql_overrides_clickhouse_func_large_utf8() {
        let dialect = ClickHouseDialect;
        let unparser = Unparser::new(&dialect);

        // Test with LargeUtf8
        let args = vec![Expr::Literal(ScalarValue::LargeUtf8(Some("avg(y)".to_string())), None)];
        let result = dialect.scalar_function_to_sql_overrides(&unparser, "clickhouse_func", &args);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_scalar_function_to_sql_overrides_clickhouse_func_empty_string() {
        let dialect = ClickHouseDialect;
        let unparser = Unparser::new(&dialect);

        // Test empty string should return error
        let args = vec![Expr::Literal(ScalarValue::Utf8(Some(String::new())), None)];
        let result = dialect.scalar_function_to_sql_overrides(&unparser, "clickhouse_func", &args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_scalar_function_to_sql_overrides_clickhouse_func_invalid_arg() {
        let dialect = ClickHouseDialect;
        let unparser = Unparser::new(&dialect);

        // Test non-string literal should return error
        let args = vec![Expr::Literal(ScalarValue::Int32(Some(42)), None)];
        let result = dialect.scalar_function_to_sql_overrides(&unparser, "clickhouse_func", &args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expects a string literal"));
    }

    #[test]
    fn test_scalar_function_to_sql_overrides_clickhouse_func_invalid_syntax() {
        let dialect = ClickHouseDialect;
        let unparser = Unparser::new(&dialect);

        // Test invalid ClickHouse syntax - should actually fail parsing
        let args = vec![Expr::Literal(ScalarValue::Utf8(Some("invalid(((".to_string())), None)];
        let result = dialect.scalar_function_to_sql_overrides(&unparser, "clickhouse_func", &args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid ClickHouse expression"));
    }

    #[test]
    fn test_scalar_function_to_sql_overrides_unknown_function() {
        let dialect = ClickHouseDialect;
        let unparser = Unparser::new(&dialect);

        // Test unknown function should return None
        let args = vec![Expr::Literal(ScalarValue::Utf8(Some("test".to_string())), None)];
        let result = dialect.scalar_function_to_sql_overrides(&unparser, "unknown_func", &args);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_clickhouse_dialect_debug_clone_default() {
        // Test Debug trait
        let debug_str = format!("{ClickHouseDialect:?}");
        assert_eq!(debug_str, "ClickHouseDialect");
    }
}
