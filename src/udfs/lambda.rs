use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{DFSchema, internal_err, not_impl_err};
use datafusion::error::Result;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
// use datafusion::prelude::Expr;
// use datafusion::scalar::ScalarValue;
// use datafusion::sql::sqlparser::ast::BinaryOperator;

pub const CLICKHOUSE_APPLY_ALIASES: [&str; 4] =
    ["clickhouse_apply", "clickhouse_lambda", "clickhouse_map", "clickhouse_fmap"];

pub fn clickhouse_apply_udf() -> ScalarUDF { ScalarUDF::new_from_impl(ClickHouseApplyUDF::new()) }

#[derive(Debug)]
pub struct ClickHouseApplyUDF {
    signature: Signature,
    aliases:   Vec<String>,
}

impl Default for ClickHouseApplyUDF {
    fn default() -> Self {
        Self {
            signature: Signature::any(Self::ARG_LEN, Volatility::Immutable),
            aliases:   CLICKHOUSE_APPLY_ALIASES.iter().map(ToString::to_string).collect(),
        }
    }
}

impl ClickHouseApplyUDF {
    pub const ARG_LEN: usize = 2;

    pub fn new() -> Self { Self::default() }
}

impl ScalarUDFImpl for ClickHouseApplyUDF {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn name(&self) -> &str { CLICKHOUSE_APPLY_ALIASES[0] }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        internal_err!("return_type_from_args used")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        super::extract_return_field_from_args(self.name(), &args)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!(
            "ClickHouseApplyUDF is for planning only - lambda functions are pushed down to \
             ClickHouse"
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ClickHouseLambdaPlanner;

impl ExprPlanner for ClickHouseLambdaPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        // TODO: Remove
        eprintln!("Expr planner: {expr:#?}");

        // if expr.op == BinaryOperator::Arrow {
        //     // Convert: x -> concat(x, 'hello')
        //     // To: Literal("x -> concat(x, 'hello')")
        //     let lambda_string = format!("{} -> {}", expr.left, expr.right);
        //     Ok(PlannerResult::Planned(Expr::Literal(ScalarValue::Utf8(Some(lambda_string)),
        // None))) } else {
        //     Ok(PlannerResult::Original(expr))
        // }
        Ok(PlannerResult::Original(expr))
    }
}
