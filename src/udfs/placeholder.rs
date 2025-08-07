use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::udf_field_from_fields;

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
    pub name:      String,
    pub signature: Signature,
}

impl PlaceholderUDF {
    pub fn new(name: &str) -> Self {
        Self {
            name:      name.to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    #[must_use]
    pub fn with_name(self, name: &str) -> Self {
        Self { name: name.to_string(), signature: self.signature }
    }

    #[must_use]
    pub fn with_signature(self, signature: Signature) -> Self {
        Self { name: self.name, signature }
    }
}

impl ScalarUDFImpl for PlaceholderUDF {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn name(&self) -> &str { &self.name }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.last().cloned().ok_or(plan_datafusion_err!(
            "Placeholder UDF '{}' requires at least one argument",
            self.name
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        if let Ok(ret) = super::extract_return_field_from_args(self.name(), &args) {
            Ok(ret)
        } else {
            let data_types =
                args.arg_fields.iter().map(|f| f.data_type()).cloned().collect::<Vec<_>>();
            let return_type = self.return_type(&data_types)?;
            Ok(udf_field_from_fields(self.name(), return_type, args.arg_fields))
        }
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!("Placeholder UDF '{}' should not be executed", self.name)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::logical_expr::{Signature, Volatility};

    use super::*;

    #[test]
    fn test_placeholder_udf_new() {
        let placeholder = PlaceholderUDF::new("test_function");
        assert_eq!(placeholder.name, "test_function");

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
    }

    #[test]
    fn test_placeholder_udf_with_signature() {
        let original = PlaceholderUDF::new("test");
        let new_signature = Signature::exact(vec![DataType::Int32], Volatility::Stable);
        let updated = original.with_signature(new_signature.clone());

        assert_eq!(updated.name, "test");
        assert_eq!(updated.signature, new_signature);
    }

    #[test]
    fn test_placeholder_udf_chaining() {
        let placeholder = PlaceholderUDF::new("original").with_name("chained").with_signature(
            Signature::exact(vec![DataType::Int32, DataType::Utf8], Volatility::Volatile),
        );

        assert_eq!(placeholder.name, "chained");

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
    fn test_scalar_udf_impl_invoke_with_args_fails() {
        let placeholder = PlaceholderUDF::new("test_function");

        // Create empty ScalarFunctionArgs that should cause failure
        let return_field = Arc::new(Field::new("result", DataType::Utf8, true));
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
