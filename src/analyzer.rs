//! A `DataFusion` `AnalyzerRule` that identifies largest subtree of a plan to wrap with an
//! extension node, and "pushes down" `ClickHouse` functions when required
pub mod function_pushdown;
mod source_context;
mod source_visitor;
mod utils;
