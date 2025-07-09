use datafusion::arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub fn placeholder_udf_from_placeholder(placeholder: PlaceholderUDF) -> ScalarUDF {
    ScalarUDF::new_from_impl(placeholder)
}

// TODO: Docs - explain why this is necessary. Essentially the `ContextProvider` used in
// `SessionContext` and `SessionState` will fail since `ClickHouse` specific functions are not
// recognized and not accounted for in the function registry. This placeholder UDF allows returning
// "something" instead of an error.
//
/// Placeholder UDF implementation
#[derive(Debug)]
pub struct PlaceholderUDF {
    pub name:        String,
    pub signature:   Signature,
    pub return_type: DataType,
}

impl PlaceholderUDF {
    pub fn new(name: &str) -> Self {
        Self {
            name:        name.to_string(),
            signature:   Signature::variadic_any(Volatility::Immutable),
            return_type: DataType::Utf8,
        }
    }

    #[must_use]
    pub fn with_name(self, name: &str) -> Self {
        Self {
            name:        name.to_string(),
            signature:   self.signature,
            return_type: self.return_type,
        }
    }

    #[must_use]
    pub fn with_signature(self, signature: Signature) -> Self {
        Self { name: self.name, signature, return_type: self.return_type }
    }

    #[must_use]
    pub fn with_return_type(self, return_type: DataType) -> Self {
        Self { name: self.name, signature: self.signature, return_type }
    }
}

impl ScalarUDFImpl for PlaceholderUDF {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn name(&self) -> &str { &self.name }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!("Placeholder UDF '{}' should not be executed", self.name)
    }
}
