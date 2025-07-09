use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;
use datafusion::sql::sqlparser::{ast, dialect};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::Dialect as UnparserDialect;

use crate::udfs::lambda::CLICKHOUSE_APPLY_ALIASES;
use crate::udfs::simple::CLICKHOUSE_FUNC_ALIASES;

// TODO: Docs - where is this used?
//
/// A custom [`UnparserDialect`] for `ClickHouse`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ClickHouseDialect;

impl UnparserDialect for ClickHouseDialect {
    fn identifier_quote_style(&self, _: &str) -> Option<char> { Some('"') }

    fn scalar_function_to_sql_overrides(
        &self,
        _unparser: &Unparser<'_>,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        // TODO: Remove
        eprintln!(
            "
            ~~~~~~~~~~~~~~~~~~~~~~~~~
            * Inside Unparser *

            name = {func_name}

            args = {args:?}
            ~~~~~~~~~~~~~~~~~~~~~~~~~


            "
        );

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
        } else if CLICKHOUSE_APPLY_ALIASES.contains(&func_name) {
            if let Some(Expr::Literal(
                ScalarValue::Utf8(Some(s))
                | ScalarValue::Utf8View(Some(s))
                | ScalarValue::LargeUtf8(Some(s)),
                _,
            )) = args.first()
            {
                if s.is_empty() {
                    return plan_err!("clickhouse_func syntax argument cannot be empty");
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
                    "clickhouse_func expects a string literal syntax argument, found: {:?}",
                    args[0]
                )
            }
        } else {
            Ok(None)
        }
    }
}
