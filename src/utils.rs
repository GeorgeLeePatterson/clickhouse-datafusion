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
