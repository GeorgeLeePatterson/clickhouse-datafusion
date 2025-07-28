#![doc = include_str!("../README.md")]
mod builders;
mod catalog_provider;
mod connection;
mod context;
pub mod dialect;
#[cfg(feature = "federation")]
pub mod federation;
pub mod prelude;
mod sink;
pub mod sql;
pub mod stream;
mod table_factory;
mod table_provider;
pub mod udfs;
pub mod utils;

pub use builders::*;
pub use catalog_provider::*;
pub use connection::*;
// ---
// TODO: Docs - LOTS MORE DOCS NEEDED HERE!!! The structures in context must be used IN PLACE
// of DataFusion's provided structures. So this is IMPORTANT if the user wants to leverage
// ClickHouse functions
// ---
/// Builders, utilities, traits, and implementations for bridging `ClickHouse`, through
/// [`clickhouse_arrow`], and [`DataFusion`](https://docs.rs/datafusion/latest/datafusion/)
///
/// The simplest way to use `ClickHouse` in `DataFusion` is to use the builders
/// [`ClickHouseOptions`] and [`ClickHouseBuilder`].
///
/// If the "federation" feature is enabled (enabled by default) then `ClickHouse` databases and
/// tables can be queried and joined across non-ClickHouse sources.
pub use context::*;
pub use sink::*;
pub use table_factory::*;
pub use table_provider::*;

#[cfg(feature = "test-utils")]
mod dev_deps {
    use tokio as _;
}
