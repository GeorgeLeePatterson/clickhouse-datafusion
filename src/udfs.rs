//! Various UDFs providing `DataFusion`'s sql parsing with some `ClickHouse` specific functionality.
//!
//! [`self::function::ClickHouseFunc`] is a sort of 'escape-hatch' to allow passing syntax directly
//! to `ClickHouse` as SQL.

pub mod analyzer;
// TODO: Remove
// pub mod lambda;
pub mod placeholder;
pub mod plan_node;
pub mod planner;
pub mod pushdown;
pub mod simple;
pub mod source_visitor;

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
        assert!(functions.contains_key("clickhouse_func"));
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
        assert!(result.unwrap_err().to_string().contains("Expected Utf8 literal for return type"));
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
        assert!(result.unwrap_err().to_string().contains("Expected Utf8 literal for return type"));
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
        assert!(result.unwrap_err().to_string().contains("Expected Utf8 literal for return type"));
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
        assert!(result.unwrap_err().to_string().contains("Expected Utf8 literal for return type"));
    }
}
