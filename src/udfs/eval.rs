use std::any::Any;
use std::str::FromStr;
use std::sync::LazyLock;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{ScalarValue, internal_err, not_impl_err, plan_datafusion_err, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, DocSection, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

use super::udf_field_from_fields;

static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(DocSection::default(), "Add one to an int32", "add_one(2)")
        .with_argument("arg1", "The string representation of the ClickHouse function")
        .with_argument("arg2", "The string representation of the expected DataType")
        .build()
});

pub const CLICKHOUSE_EVAL_UDF_ALIASES: &[&str] = &["clickhouse_eval"];

pub fn clickhouse_eval_udf() -> ScalarUDF { ScalarUDF::from(ClickHouseEval::new()) }

fn get_doc() -> &'static Documentation { &DOCUMENTATION }

// TODO: Docs - explain how this can be used if the full custom ClickHouseQueryPlanner CANNOT be
// used. This provides an alternative syntax for specifying clickhouse functions
//
/// [`ClickHouseFunc`] is an escape hatch to pass syntax that `DataFusion` does not support directly
/// to `ClickHouse`.
#[derive(Debug)]
pub struct ClickHouseEval {
    signature: Signature,
    aliases:   Vec<String>,
}

impl Default for ClickHouseEval {
    fn default() -> Self { Self::new() }
}

impl ClickHouseEval {
    pub const ARG_LEN: usize = 2;

    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                vec![DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8],
                Volatility::Volatile,
            ),
            aliases:   CLICKHOUSE_EVAL_UDF_ALIASES.iter().map(ToString::to_string).collect(),
        }
    }
}

impl ScalarUDFImpl for ClickHouseEval {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &'static str { CLICKHOUSE_EVAL_UDF_ALIASES[0] }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    /// # Errors
    /// Returns an error if the arguments are invalid or the data type cannot be parsed.
    ///
    /// # Panics
    /// Unwrap is used but it's guarded by a bounds check.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!(
                "Expected two string arguments, syntax and datatype, received fields {:?}",
                arg_types
            );
        }

        // Length is confirmed above, ok to unwrap
        Ok(arg_types.get(1).cloned().unwrap())
    }

    /// # Errors
    /// Returns an error if the arguments are invalid or the data type cannot be parsed.
    ///
    /// # Panics
    /// Unwrap is used but it's guarded by a bounds check.
    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        if args.arg_fields.len() != 2 || args.scalar_arguments.len() != 2 {
            return plan_err!(
                "Expected two string arguments, syntax and datatype, received fields {:?}",
                args.arg_fields
            );
        }

        // Length is confirmed above, ok to unwrap
        let syntax_arg = args
            .scalar_arguments
            .first()
            .unwrap()
            .ok_or(plan_datafusion_err!("First argument (syntax) missing"))?;
        let type_arg = args
            .scalar_arguments
            .get(1)
            .unwrap()
            .ok_or(plan_datafusion_err!("Second argument (data type) missing"))?;

        if let (
            ScalarValue::Utf8(syntax)
            | ScalarValue::Utf8View(syntax)
            | ScalarValue::LargeUtf8(syntax),
            ScalarValue::Utf8(data_type)
            | ScalarValue::Utf8View(data_type)
            | ScalarValue::LargeUtf8(data_type),
        ) = (syntax_arg, type_arg)
        {
            // Extract syntax string from first argument
            if syntax.is_none() {
                return internal_err!("Missing syntax argument");
            }

            // Extract type string from second argument
            let Some(type_str) = data_type else {
                return internal_err!("Missing data type argument");
            };

            // Parse type string to DataType
            let data_type = DataType::from_str(type_str)
                .map_err(|e| plan_datafusion_err!("Invalid type string: {e}"))?;
            Ok(udf_field_from_fields(self.name(), data_type, args.arg_fields))
        } else {
            internal_err!("clickhouse_func expects string arguments")
        }
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("UDFs are evaluated after data has been fetched.")
    }

    fn documentation(&self) -> Option<&Documentation> { Some(get_doc()) }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow;
    use datafusion::arrow::datatypes::*;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{ReturnFieldArgs, ScalarUDFImpl};
    use datafusion::prelude::SessionContext;

    use super::*;

    #[test]
    fn test_clickhouse_eval_new() {
        let func = ClickHouseEval::new();
        assert_eq!(func.name(), CLICKHOUSE_EVAL_UDF_ALIASES[0]);
        assert_eq!(func.aliases(), CLICKHOUSE_EVAL_UDF_ALIASES);
    }

    #[test]
    fn test_clickhouse_eval_default() {
        let func = ClickHouseEval::default();
        assert_eq!(func.name(), CLICKHOUSE_EVAL_UDF_ALIASES[0]);
    }

    #[test]
    fn test_clickhouse_func_constants() {
        assert_eq!(ClickHouseEval::ARG_LEN, 2);
    }

    #[test]
    fn test_clickhouse_eval_udf_creation() {
        let udf = clickhouse_eval_udf();
        assert_eq!(udf.name(), CLICKHOUSE_EVAL_UDF_ALIASES[0]);
    }

    #[test]
    fn test_return_type_valid_args() {
        let func = ClickHouseEval::new();
        let arg_types = vec![DataType::Utf8, DataType::Int32];
        let result = func.return_type(&arg_types);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Int32);
    }

    #[test]
    fn test_return_type_valid_args_utf8_view() {
        let func = ClickHouseEval::new();
        let arg_types = vec![DataType::Utf8View, DataType::Float64];
        let result = func.return_type(&arg_types);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Float64);
    }

    #[test]
    fn test_return_type_valid_args_large_utf8() {
        let func = ClickHouseEval::new();
        let arg_types = vec![DataType::LargeUtf8, DataType::Boolean];
        let result = func.return_type(&arg_types);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Boolean);
    }

    #[test]
    fn test_return_type_wrong_arg_count() {
        let func = ClickHouseEval::new();

        // Too few arguments
        let arg_types = vec![DataType::Utf8];
        let result = func.return_type(&arg_types);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected two string arguments"));

        // Too many arguments
        let arg_types = vec![DataType::Utf8, DataType::Int32, DataType::Float64];
        let result = func.return_type(&arg_types);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected two string arguments"));
    }

    #[test]
    fn test_return_field_from_args_valid() {
        let func = ClickHouseEval::new();
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

        let result = func.return_field_from_args(args);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.name(), CLICKHOUSE_EVAL_UDF_ALIASES[0]);
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(!field.is_nullable(), "Expect non-nullable - no nullable input fields");
    }

    #[test]
    fn test_return_field_from_args_utf8_view() {
        let func = ClickHouseEval::new();
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

        let result = func.return_field_from_args(args);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_return_field_from_args_large_utf8() {
        let func = ClickHouseEval::new();
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

        let result = func.return_field_from_args(args);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_return_field_from_args_wrong_field_count() {
        let func = ClickHouseEval::new();
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let scalar = [Some(ScalarValue::Utf8(Some("count()".to_string())))];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1],
            scalar_arguments: &[scalar[0].as_ref()],
        };

        let result = func.return_field_from_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected two string arguments"));
    }

    #[test]
    fn test_return_field_from_args_wrong_scalar_count() {
        let func = ClickHouseEval::new();
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8, false));
        let scalar = [Some(ScalarValue::Utf8(Some("count()".to_string())))];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref()],
        };

        let result = func.return_field_from_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected two string arguments"));
    }

    #[test]
    fn test_return_field_from_args_missing_syntax() {
        let func = ClickHouseEval::new();
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8, false));
        let scalar = [None, Some(ScalarValue::Utf8(Some("Int64".to_string())))];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = func.return_field_from_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("First argument (syntax) missing"));
    }

    #[test]
    fn test_return_field_from_args_missing_type() {
        let func = ClickHouseEval::new();
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8, false));
        let scalar = [Some(ScalarValue::Utf8(Some("count()".to_string()))), None];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = func.return_field_from_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Second argument (data type) missing"));
    }

    #[test]
    fn test_return_field_from_args_null_syntax() {
        let func = ClickHouseEval::new();
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8, false));
        let scalar =
            [Some(ScalarValue::Utf8(None)), Some(ScalarValue::Utf8(Some("Int64".to_string())))];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = func.return_field_from_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Missing syntax argument"));
    }

    #[test]
    fn test_return_field_from_args_null_type() {
        let func = ClickHouseEval::new();
        let field1 = Arc::new(Field::new("syntax", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("type", DataType::Utf8, false));
        let scalar =
            [Some(ScalarValue::Utf8(Some("count()".to_string()))), Some(ScalarValue::Utf8(None))];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = func.return_field_from_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Missing data type argument"));
    }

    #[test]
    fn test_return_field_from_args_invalid_type_string() {
        let func = ClickHouseEval::new();
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

        let result = func.return_field_from_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid type string"));
    }

    #[test]
    fn test_return_field_from_args_non_string_arguments() {
        let func = ClickHouseEval::new();
        let field1 = Arc::new(Field::new("syntax", DataType::Int32, false));
        let field2 = Arc::new(Field::new("type", DataType::Int32, false));
        let scalar = [Some(ScalarValue::Int32(Some(42))), Some(ScalarValue::Int32(Some(24)))];
        let args = ReturnFieldArgs {
            arg_fields:       &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = func.return_field_from_args(args);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("clickhouse_func expects string arguments")
        );
    }

    #[test]
    fn test_invoke_with_args_not_implemented() {
        let func = ClickHouseEval::new();
        let args = ScalarFunctionArgs {
            args:         vec![],
            arg_fields:   vec![],
            number_rows:  1,
            return_field: Arc::new(Field::new("", DataType::Int32, false)),
        };
        let result = func.invoke_with_args(args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("UDFs are evaluated after data has been fetched")
        );
    }

    #[test]
    fn test_documentation() {
        let func = ClickHouseEval::new();
        let doc = func.documentation();
        assert!(doc.is_some());

        let documentation = get_doc();
        assert!(documentation.description.contains("Add one to an int32"));
    }

    #[test]
    fn test_as_any() {
        let func = ClickHouseEval::new();
        let any_ref = func.as_any();
        assert!(any_ref.downcast_ref::<ClickHouseEval>().is_some());
    }

    #[tokio::test]
    async fn test_clickhouse_udf() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        ctx.register_udf(clickhouse_eval_udf());

        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("names", DataType::Utf8, false),
        ]));

        let provider =
            Arc::new(datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![
                arrow::record_batch::RecordBatch::try_new(schema, vec![
                    Arc::new(arrow::array::Int32Array::from(vec![1])),
                    Arc::new(arrow::array::StringArray::from(vec!["John,Jon,J"])),
                ])?,
            ]])?);
        drop(ctx.register_table("people", provider)?);
        let sql =
            "SELECT id, clickhouse_eval('splitByChar('','', names)', 'List(Utf8)') FROM people";
        let df = ctx.sql(&format!("EXPLAIN {sql}")).await?;
        let results = df.collect().await?;
        println!("EXPLAIN: {results:?}");
        Ok(())
    }
}
