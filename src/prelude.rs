//!
//! To simplify compatibility, crates for [`clickhouse_arrow`], [`datafusion`], and
//! [`datafusion::arrow`] are re-exported.

/// Re-exports
mod reexports {
    pub use datafusion::arrow;
    pub use {clickhouse_arrow, datafusion};
}

pub use reexports::*;

pub use super::analyzer::function_pushdown::ClickHouseFunctionPushdown;
pub use super::builders::ClickHouseBuilder;
#[cfg(not(feature = "mocks"))]
pub use super::connection::ArrowPoolConnection;
pub use super::connection::{ClickHouseConnection, ClickHouseConnectionPool};
pub use super::providers::*;
pub use super::sink::ClickHouseDataSink;
pub use super::sql::SqlTable;
pub use super::table_factory::{ClickHouseTableFactory, ClickHouseTableProviderFactory};
pub use super::udfs::eval::clickhouse_eval_udf;
pub use super::udfs::register_clickhouse_functions;

// TODO: crate::federation exports (esp traits)
// TODO: crate::context exports (esp traits)
