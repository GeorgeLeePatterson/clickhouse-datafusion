//! Helpers for bridging between `ClickHouse`, `DataFusion`, and [`clickhouse_arrow`].
mod create;
mod errors;
pub(crate) mod params;

use datafusion::execution::SessionStateDefaults;
use datafusion::prelude::SessionContext;

pub use self::create::*;
pub use self::errors::*;
pub use self::params::*;

/// Helper function to register built-in functions in a session context.
///
/// `DataFusion` is quite useless without these imo.
pub fn register_builtins(ctx: &SessionContext) {
    // Register all udf functions so that items like "make_array" are available
    SessionStateDefaults::register_builtin_functions(&mut ctx.state_ref().write());
    // Make sure all ch functions are available
    super::udfs::register_clickhouse_functions(ctx);
}

pub mod provider {
    use std::sync::Arc;

    use datafusion::catalog::TableProvider;

    use crate::ClickHouseTableProvider;

    /// Helper function to extract a `ClickHouseTableProvider` from a `dyn TableProvider`.
    #[cfg(feature = "federation")]
    pub fn extract_clickhouse_provider(
        provider: &Arc<dyn TableProvider>,
    ) -> Option<&ClickHouseTableProvider> {
        let fed_provider = provider
            .as_any()
            .downcast_ref::<datafusion_federation::FederatedTableProviderAdaptor>()?;
        fed_provider
            .table_provider
            .as_ref()
            .and_then(|p| p.as_any().downcast_ref::<ClickHouseTableProvider>())
    }

    /// Helper function to extract a `ClickHouseTableProvider` from a `dyn TableProvider`.
    #[cfg(not(feature = "federation"))]
    pub fn extract_clickhouse_provider(
        provider: &Arc<dyn TableProvider>,
    ) -> Option<&ClickHouseTableProvider> {
        provider.as_any().downcast_ref::<ClickHouseTableProvider>()
    }
}
