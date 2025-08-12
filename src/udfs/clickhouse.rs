// TODO: Remove - better docs
//! Custom `clickhouse` UDF implementation that allows intelligent "pushdown" to execute
//! `ClickHouse` functions directly on the remote server.
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{internal_err, not_impl_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

pub const CLICKHOUSE_UDF_ALIASES: [&str; 4] =
    ["clickhouse", "clickhouse_udf", "clickhouse_pushdown", "clickhouse_pushdown_udf"];

pub fn clickhouse_udf() -> ScalarUDF { ScalarUDF::new_from_impl(ClickHouseUDF::new()) }

// TODO: Remove - docs
#[derive(Debug)]
pub struct ClickHouseUDF {
    signature: Signature,
    aliases:   Vec<String>,
}

impl Default for ClickHouseUDF {
    fn default() -> Self {
        Self {
            signature: Signature::any(Self::ARG_LEN, Volatility::Immutable),
            aliases:   CLICKHOUSE_UDF_ALIASES.iter().map(ToString::to_string).collect(),
        }
    }
}

impl ClickHouseUDF {
    pub const ARG_LEN: usize = 2;

    pub fn new() -> Self { Self::default() }
}

impl ScalarUDFImpl for ClickHouseUDF {
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
            "ClickHouseUDF is for planning only, a syntax error may have occurred. Sometimes, \
             ClickHouse functions need to be backticked"
        )
    }

    /// Set to true to prevent optimizations. There is no way to know what the function will
    /// produce, so these settings must be conservative.
    fn short_circuits(&self) -> bool { true }
}

// Helper functions for identifying ClickHouse functions

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{
        ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, TypeSignature,
    };

    use super::*;

    #[test]
    fn test_clickhouse_udf_pushdown_udf_creation() {
        let udf = clickhouse_udf();
        assert_eq!(udf.name(), CLICKHOUSE_UDF_ALIASES[0]);
    }

    #[test]
    fn test_clickhouse_pushdown_udf_new() {
        let udf = ClickHouseUDF::new();
        assert_eq!(udf.name(), CLICKHOUSE_UDF_ALIASES[0]);
        assert_eq!(udf.aliases(), &CLICKHOUSE_UDF_ALIASES);
    }

    #[test]
    fn test_clickhouse_pushdown_udf_default() {
        let udf = ClickHouseUDF::default();
        assert_eq!(udf.name(), CLICKHOUSE_UDF_ALIASES[0]);
    }

    #[test]
    fn test_clickhouse_pushdown_udf_constants() {
        assert_eq!(ClickHouseUDF::ARG_LEN, 2);
    }

    #[test]
    fn test_return_field_from_args_valid() {
        let udf = ClickHouseUDF::new();
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
        assert_eq!(field.name(), CLICKHOUSE_UDF_ALIASES[0]);
        assert_eq!(field.data_type(), &DataType::Int64);
    }

    #[test]
    fn test_return_field_from_args_invalid_type() {
        let udf = ClickHouseUDF::new();
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8, false));
        let scalar = [
            Some(ScalarValue::Utf8(Some("count()".to_string()))),
            Some(ScalarValue::Utf8(Some("InvalidType".to_string()))),
        ];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = udf.return_field_from_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid return type"));
    }

    #[test]
    fn test_invoke_with_args_not_implemented() {
        let udf = ClickHouseUDF::new();
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
    fn test_as_any() {
        let udf = ClickHouseUDF::new();
        let any_ref = udf.as_any();
        assert!(any_ref.downcast_ref::<ClickHouseUDF>().is_some());
    }

    #[test]
    fn test_signature() {
        let udf = ClickHouseUDF::new();
        let signature = udf.signature();

        assert!(matches!(signature.type_signature, TypeSignature::Any(ClickHouseUDF::ARG_LEN)));
    }
}
