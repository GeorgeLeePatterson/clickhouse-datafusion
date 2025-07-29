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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{Signature, Volatility};

    use super::*;

    #[test]
    fn test_placeholder_udf_new() {
        let placeholder = PlaceholderUDF::new("test_function");
        assert_eq!(placeholder.name, "test_function");
        assert_eq!(placeholder.return_type, DataType::Utf8);

        // Test that signature is variadic_any
        assert_eq!(
            placeholder.signature.type_signature,
            datafusion::logical_expr::TypeSignature::VariadicAny
        );
    }

    #[test]
    fn test_placeholder_udf_with_name() {
        let original = PlaceholderUDF::new("original_name");
        let renamed = original.with_name("new_name");

        assert_eq!(renamed.name, "new_name");
        assert_eq!(renamed.return_type, DataType::Utf8);
    }

    #[test]
    fn test_placeholder_udf_with_signature() {
        let original = PlaceholderUDF::new("test");
        let new_signature = Signature::exact(vec![DataType::Int32], Volatility::Stable);
        let updated = original.with_signature(new_signature.clone());

        assert_eq!(updated.name, "test");
        assert_eq!(updated.signature, new_signature);
        assert_eq!(updated.return_type, DataType::Utf8);
    }

    #[test]
    fn test_placeholder_udf_with_return_type() {
        let original = PlaceholderUDF::new("test");
        let updated = original.with_return_type(DataType::Int64);

        assert_eq!(updated.name, "test");
        assert_eq!(updated.return_type, DataType::Int64);
    }

    #[test]
    fn test_placeholder_udf_chaining() {
        let placeholder = PlaceholderUDF::new("original")
            .with_name("chained")
            .with_return_type(DataType::Float64)
            .with_signature(Signature::exact(
                vec![DataType::Int32, DataType::Utf8],
                Volatility::Volatile,
            ));

        assert_eq!(placeholder.name, "chained");
        assert_eq!(placeholder.return_type, DataType::Float64);

        // Verify the signature has exact types
        match &placeholder.signature.type_signature {
            datafusion::logical_expr::TypeSignature::Exact(types) => {
                assert_eq!(types.len(), 2);
                assert_eq!(types[0], DataType::Int32);
                assert_eq!(types[1], DataType::Utf8);
            }
            _ => panic!("Expected Exact signature"),
        }
    }

    #[test]
    fn test_scalar_udf_impl_name() {
        let placeholder = PlaceholderUDF::new("test_name");
        assert_eq!(placeholder.name(), "test_name");
    }

    #[test]
    fn test_scalar_udf_impl_signature() {
        let placeholder = PlaceholderUDF::new("test");
        let signature = placeholder.signature();

        // Verify it's variadic_any
        assert_eq!(signature.type_signature, datafusion::logical_expr::TypeSignature::VariadicAny);
        assert_eq!(signature.volatility, Volatility::Immutable);
    }

    #[test]
    fn test_scalar_udf_impl_return_type() {
        let placeholder = PlaceholderUDF::new("test");
        let result = placeholder.return_type(&[DataType::Int32, DataType::Utf8]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Utf8);

        // Test with custom return type
        let custom_placeholder = placeholder.with_return_type(DataType::Boolean);
        let custom_result = custom_placeholder.return_type(&[]);
        assert!(custom_result.is_ok());
        assert_eq!(custom_result.unwrap(), DataType::Boolean);
    }

    #[test]
    fn test_scalar_udf_impl_invoke_with_args_fails() {
        let placeholder = PlaceholderUDF::new("test_function");

        // Create empty ScalarFunctionArgs that should cause failure
        let return_field =
            Arc::new(datafusion::arrow::datatypes::Field::new("result", DataType::Utf8, true));
        let args =
            ScalarFunctionArgs { args: vec![], number_rows: 0, arg_fields: vec![], return_field };

        let result = placeholder.invoke_with_args(args);
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Placeholder UDF 'test_function' should not be executed"));
    }

    #[test]
    fn test_scalar_udf_impl_as_any() {
        let placeholder = PlaceholderUDF::new("test");
        let any_ref = placeholder.as_any();

        // Verify we can downcast back to PlaceholderUDF
        assert!(any_ref.downcast_ref::<PlaceholderUDF>().is_some());
    }

    #[test]
    fn test_placeholder_udf_from_placeholder() {
        let placeholder = PlaceholderUDF::new("test_func");
        let scalar_udf = placeholder_udf_from_placeholder(placeholder);

        assert_eq!(scalar_udf.name(), "test_func");
    }

    #[test]
    fn test_debug_implementation() {
        let placeholder = PlaceholderUDF::new("debug_test");
        let debug_str = format!("{placeholder:?}");

        assert!(debug_str.contains("PlaceholderUDF"));
        assert!(debug_str.contains("debug_test"));
    }
}
