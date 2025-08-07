use std::sync::Arc;

use datafusion::catalog::TableProvider;

use super::table::ClickHouseTableProvider;

/// Helper function to extract a `ClickHouseTableProvider` from a `dyn TableProvider`.
#[cfg(feature = "federation")]
pub fn extract_clickhouse_provider(
    provider: &Arc<dyn TableProvider>,
) -> Option<&ClickHouseTableProvider> {
    let fed_provider =
        provider.as_any().downcast_ref::<datafusion_federation::FederatedTableProviderAdaptor>()?;
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
