pub(crate) mod helpers;

use std::sync::Arc;

use clickhouse_arrow::test_utils::{self, ClickHouseContainer};
use clickhouse_datafusion::prelude::datafusion::error::Result;
use tracing::{debug, error};

// Env variables to control aspects of testing
#[allow(unused)]
pub(crate) const DISABLE_CLEANUP_ENV: &str = "DISABLE_CLEANUP";
#[allow(unused)]
pub(crate) const DISABLE_CLEANUP_ON_ERROR_ENV: &str = "DISABLE_CLEANUP_ON_ERROR";

/// Panic that test
#[macro_export]
macro_rules! test_func {
    ($n:ident, $t:expr) => {
        async fn $n(ch: ::std::sync::Arc<::clickhouse_arrow::test_utils::ClickHouseContainer>) {
            $t(ch).await.inspect_err(|error| eprintln!("Test failed: {error:?}")).unwrap();
        }
    };
}

/// Macro to run tests using the below test harness.
#[macro_export]
macro_rules! e2e_test {
    ($name:ident, $test_fn:expr, $dirs:expr, $conf:expr) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() -> ::clickhouse_datafusion::prelude::datafusion::error::Result<()> {
            let name = stringify!($name);
            $crate::common::run_test_with_errors(name, $test_fn, Some($dirs), $conf).await
        }
    };
}

#[allow(unused)]
pub(crate) async fn run_test_with_errors<F, Fut>(
    name: &str,
    test_fn: F,
    directives: Option<&[(&str, &str)]>,
    clickhouse_conf: Option<&str>,
) -> Result<()>
where
    F: FnOnce(Arc<ClickHouseContainer>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let disable_cleanup = std::env::var(DISABLE_CLEANUP_ENV)
        .ok()
        .is_some_and(|e| e.eq_ignore_ascii_case("true") || e == "1");

    let disable_cleanup_on_error = std::env::var(DISABLE_CLEANUP_ON_ERROR_ENV)
        .ok()
        .is_some_and(|e| e.eq_ignore_ascii_case("true") || e == "1");

    // Initialize container and tracing
    test_utils::init_tracing(directives);
    let ch = test_utils::create_container(clickhouse_conf).await;

    let result = test_fn(Arc::clone(&ch)).await;

    // Either path will not update TESTS_RUNNING, and will keep containers running
    if disable_cleanup || (disable_cleanup_on_error && result.is_err()) {
        if result.is_err() {
            error!(">>> Exiting test w/o shutdown: {name}");
        } else {
            debug!(">>> Exiting test w/o shutdown: {name}");
        }
        return result;
    }

    ch.shutdown().await.expect("Shutting down container");

    result
}
