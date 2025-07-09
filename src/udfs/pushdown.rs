use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{internal_err, not_impl_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

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
