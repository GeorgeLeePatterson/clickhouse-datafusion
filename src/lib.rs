#![doc = include_str!("../README.md")]
pub mod analyzer;
mod builders;
mod connection;
mod context;
pub mod dialect;
#[cfg(feature = "federation")]
pub mod federation;
pub mod prelude;
pub mod providers;
mod sink;
pub mod sql;
pub mod stream;
pub mod udfs;
pub mod utils;

pub use builders::*;
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
pub use providers::*;
pub use sink::*;

#[cfg(feature = "test-utils")]
mod dev_deps {
    use {tokio as _, tracing_subscriber as _};
}
