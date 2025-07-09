use std::sync::Arc;

use datafusion::arrow::datatypes::Field;
use datafusion::common::{Column, DFSchema, Spans};
use datafusion::error::Result;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::TableScan;
use datafusion::prelude::Expr;

// Re-export the ClickHouseFunction and ClickHouseFunctionNode from pushdown_analyzer
pub use crate::udfs::pushdown_analyzer::{ClickHouseFunction, ClickHouseFunctionNode};

// The ClickHouseFunctionNode implementation is now in pushdown_analyzer.rs

// UserDefinedLogicalNodeCore implementation is now in pushdown_analyzer.rs

/// Helper function to add a Vec of [`ClickHouseFunction`]s to the input schema
///
/// This preserves the original column order while replacing function-matched columns
/// with their `ClickHouse` function equivalents.
#[allow(dead_code)]
fn merge_function_exprs_in_schema(
    funcs: Vec<ClickHouseFunction>,
    input: &TableScan,
) -> Result<(Vec<Expr>, DFSchema)> {
    // Create a map of function aliases for quick lookup
    let func_map: std::collections::HashMap<_, _> = funcs
        .into_iter()
        .map(|f| {
            // TODO: Remove debug
            eprintln!(
                "Function mapping: alias={}, target_table={:?}",
                f.function_alias,
                f.target_table
            );
            (f.function_alias.clone(), f)
        })
        .collect();

    // Process each field in the input schema, maintaining order
    let mut all_exprs = Vec::new();
    let mut all_fields = Vec::new();

    for (table_ref, field) in input.projected_schema.iter() {
        // TODO: Remove debug
        eprintln!(
            "Checking field: table_ref={:?}, name={}, has_func={}",
            table_ref,
            field.name(),
            func_map.contains_key(field.name())
        );

        if let Some(func) = func_map.get(field.name()) {
            // Replace this column with the function
            all_exprs.push(Expr::Alias(Alias {
                relation: Some(input.table_name.clone()),
                name:     func.function_alias.clone(),
                expr:     Box::new(func.inner_expr.clone()),
                metadata: None,
            }));
            all_fields.push((Some(input.table_name.clone()), Arc::new(Field::new(
                &func.function_alias,
                func.return_type.clone(),
                true, // nullable
            ))));
        } else {
            // Keep the original column
            all_exprs.push(Expr::Column(Column {
                relation: table_ref.cloned(),
                name:     field.name().to_string(),
                spans:    Spans::new(),
            }));
            all_fields.push((table_ref.cloned(), Arc::clone(field)));
        }
    }

    // Create the schema with proper ordering
    let schema =
        DFSchema::new_with_metadata(all_fields, input.projected_schema.metadata().clone())?;

    Ok((all_exprs, schema))
}

// TODO: Decide whether to remove the following functions

// type FunctionExprs = (Vec<Expr>, Vec<(Option<TableReference>, FieldRef)>);

// /// Helper function to add a Vec of [`ClickHouseFunction`]s to the input schema
// #[expect(unused)]
// fn add_function_exprs_in_schema(
//     funcs: Vec<ClickHouseFunction>,
//     input: &TableScan,
// ) -> FunctionExprs {
//     input
//         .projected_schema
//         .iter()
//         .map(|(table_ref, field)| {
//             (
//                 Expr::Column(Column {
//                     relation: table_ref.cloned(),
//                     name:     field.name().to_string(),
//                     spans:    Spans::new(),
//                 }),
//                 (table_ref.cloned(), Arc::clone(field)),
//             )
//         })
//         .chain(funcs.into_iter().map(|func| {
//             let field = func.field;
//             (
//                 Expr::Alias(Alias {
//                     relation: Some(input.table_name.clone()),
//                     name:     field.name().to_string(),
//                     expr:     Box::new(Expr::ScalarFunction(func.func)),
//                     metadata: None,
//                 }),
//                 (Some(input.table_name.clone()), field),
//             )
//         }))
//         .unzip()
// }

// TODO: Remove - or keep, not sure why this should be preserved
// /// Helper function to convert a Vec of [`ClickHouseFunction`]s to exprs and [`DFSchema`]
// #[expect(unused)]
// fn replace_function_exprs_in_schema(
//     funcs: Vec<ClickHouseFunction>,
//     input: &TableScan,
// ) -> FunctionExprs {
//     input
//         .projected_schema
//         .iter()
//         .map(|(table_ref, field)| {
//             if let Some(func) = funcs
//                 .iter()
//                 .find(|f| f.projections.first().map(|c| c.name.as_str()) == Some(field.name()))
//             {
//                 (table_ref.cloned(), Arc::clone(func.field()), Some(func.func().clone()))
//             } else {
//                 (table_ref.cloned(), field.clone(), None)
//             }
//         })
//         .map(|(table_ref, field, func)| {
//             if let Some(func) = func {
//                 (
//                     Expr::Alias(Alias {
//                         expr:     Box::new(Expr::ScalarFunction(func)),
//                         relation: table_ref.clone(),
//                         name:     field.name().to_string(),
//                         metadata: None,
//                     }),
//                     (table_ref, field),
//                 )
//             } else {
//                 (
//                     Expr::Column(Column {
//                         relation: table_ref.clone(),
//                         name:     field.name().to_string(),
//                         spans:    Spans::new(),
//                     }),
//                     (table_ref, field),
//                 )
//             }
//         })
//         .unzip()
// }
