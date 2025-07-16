use std::any::Any;
use std::str::FromStr;
use std::sync::LazyLock;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{ScalarValue, internal_err, not_impl_err, plan_datafusion_err, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, DocSection, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(DocSection::default(), "Add one to an int32", "add_one(2)")
        .with_argument("arg1", "The string representation of the ClickHouse function")
        .with_argument("arg2", "The string representation of the expected DataType")
        .build()
});

pub const CLICKHOUSE_FUNC_ALIASES: &[&str] = &["clickhousefunc", "clickhouse_func"];

pub fn clickhouse_func_udf() -> ScalarUDF { ScalarUDF::from(ClickHouseFunc::new()) }

fn get_doc() -> &'static Documentation { &DOCUMENTATION }

// TODO: Docs - explain how this can be used if the full custom ClickHouseQueryPlanner CANNOT be
// used. This provides an alternative syntax for specifying clickhouse functions
//
/// [`ClickHouseFunc`] is an escape hatch to pass syntax that `DataFusion` does not support directly
/// to `ClickHouse`.
#[derive(Debug)]
pub struct ClickHouseFunc {
    signature: Signature,
    aliases:   Vec<String>,
}

impl Default for ClickHouseFunc {
    fn default() -> Self { Self::new() }
}

impl ClickHouseFunc {
    pub const ARG_LEN: usize = 2;

    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                vec![DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8],
                Volatility::Volatile,
            ),
            aliases:   CLICKHOUSE_FUNC_ALIASES.iter().map(ToString::to_string).collect(),
        }
    }
}

impl ScalarUDFImpl for ClickHouseFunc {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &'static str { "clickhouse_func" }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    /// # Errors
    /// Returns an error if the arguments are invalid or the data type cannot be parsed.
    ///
    /// # Panics
    /// Unwrap is used but it's guarded by a bounds check.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 || arg_types.len() != 2 {
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

            Ok(Field::new(self.name(), data_type, true).into())
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
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_clickhouse_udf() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        ctx.register_udf(super::clickhouse_func_udf());

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
            "SELECT name, clickhousefunc('splitByChar('','', names)', 'List(Utf8)') FROM people";
        let df = ctx.sql(&format!("EXPLAIN {sql}")).await?;
        let results = df.collect().await?;
        println!("EXPLAIN: {results:?}");
        Ok(())
    }
}
