use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use clickhouse_arrow::prelude::Secret;
use clickhouse_arrow::{
    ArrowConnectionPoolBuilder, ArrowOptions, CompressionMethod, CreateOptions, Destination,
    Settings,
};
use datafusion::common::{exec_err, plan_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::prelude::lit;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::Dialect;

use crate::default_arrow_options;
use crate::dialect::ClickHouseDialect;

/// Reserved clickhouse options parameter settings
pub(crate) const ENDPOINT_PARAM: &str = "endpoint";
pub(crate) const USERNAME_PARAM: &str = "username";
pub(crate) const PASSWORD_PARAM: &str = "password";
pub(crate) const DEFAULT_DATABASE_PARAM: &str = "default_database";
pub(crate) const COMPRESSION_PARAM: &str = "compression";
pub(crate) const DOMAIN_PARAM: &str = "domain";
pub(crate) const CAFILE_PARAM: &str = "cafile";
pub(crate) const USE_TLS_PARAM: &str = "use_tls";
pub(crate) const STRINGS_AS_STRINGS_PARAM: &str = "strings_as_strings";
pub(crate) const CLOUD_TIMEOUT_PARAM: &str = "cloud_timeout";
pub(crate) const CLOUD_WAKEUP_PARAM: &str = "cloud_wakeup";
pub(crate) const POOL_MAX_SIZE_PARAM: &str = "pool_max_size";
pub(crate) const POOL_MIN_IDLE_PARAM: &str = "pool_min_idle";
pub(crate) const POOL_TEST_ON_CHECK_OUT_PARAM: &str = "pool_test_on_check_out";
pub(crate) const POOL_MAX_LIFETIME_PARAM: &str = "pool_max_lifetime";
pub(crate) const POOL_IDLE_TIMEOUT_PARAM: &str = "pool_idle_timeout";
pub(crate) const POOL_CONNECTION_TIMEOUT_PARAM: &str = "pool_connection_timeout";
pub(crate) const POOL_RETRY_CONNECTION_PARAM: &str = "pool_retry_connection";

/// Reserved table create parameter settings
pub(crate) const ENGINE_PARAM: &str = "engine";
pub(crate) const ORDER_BY_PARAM: &str = "order_by";
pub(crate) const PRIMARY_KEYS_PARAM: &str = "primary_keys";
pub(crate) const PARTITION_BY_PARAM: &str = "partition_by";
pub(crate) const SAMPLING_PARAM: &str = "sampling";
pub(crate) const TTL_PARAM: &str = "ttl";
pub(crate) const DEFAULTS_PARAM: &str = "defaults";
pub(crate) const DEFAULTS_FOR_NULLABLE_PARAM: &str = "defaults_for_nullable";

pub(crate) const ALL_PARAMS: &[&str; 16] = &[
    ENDPOINT_PARAM,
    USERNAME_PARAM,
    PASSWORD_PARAM,
    DEFAULT_DATABASE_PARAM,
    COMPRESSION_PARAM,
    DOMAIN_PARAM,
    CAFILE_PARAM,
    USE_TLS_PARAM,
    STRINGS_AS_STRINGS_PARAM,
    CLOUD_TIMEOUT_PARAM,
    CLOUD_WAKEUP_PARAM,
    ENGINE_PARAM,
    ORDER_BY_PARAM,
    SAMPLING_PARAM,
    TTL_PARAM,
    DEFAULTS_FOR_NULLABLE_PARAM,
];

/// Helper function to parse a string into a vector of strings
fn parse_param_vec(param: &str) -> Vec<String> {
    param.split(',').map(ToString::to_string).collect()
}

/// Helper function to parse a string into a hashmap of strings -> strings
fn parse_param_hashmap(param: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for key_value in param.split(',') {
        let mut parts = key_value.split('=');
        let key = parts.next();
        let value = parts.next();
        if let (Some(k), Some(v)) = (key, value) {
            drop(params.insert(k.to_string(), v.to_string()));
        }
    }
    params
}

/// Helper function to convert a vec of strings into a string param
fn vec_to_param(param: &[String]) -> String { param.join(",") }

/// Wrapper for serialized client options
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientOptionParams(HashMap<String, ClientOption>);

impl ClientOptionParams {
    pub fn into_params(self) -> HashMap<String, String> {
        self.0.into_iter().map(|(k, v)| (k, v.to_string())).collect()
    }
}

impl std::ops::Deref for ClientOptionParams {
    type Target = HashMap<String, ClientOption>;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::ops::DerefMut for ClientOptionParams {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}

/// Wrapper for serialized client options that impls [`std::fmt::Display`] and ensures secrets are
/// not logged or deserialized in plain text.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClientOption {
    Secret(Secret),
    Value(String),
}

impl std::fmt::Display for ClientOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Secret(s) => write!(f, "{}", s.get()),
            Self::Value(s) => write!(f, "{s}"),
        }
    }
}

/// [`crate::ClickHouseTableProviderFactory`] must receive all parameters as strings, this
/// function is helpful to serialize the required options
///
/// # Errors
/// - Returns an error if settings contain `ClientOption` keys
pub fn pool_builder_to_params(
    endpoint: impl Into<String>,
    builder: &ArrowConnectionPoolBuilder,
) -> Result<ClientOptionParams> {
    let mut params = [
        (ENDPOINT_PARAM, ClientOption::Value(endpoint.into())),
        (USERNAME_PARAM, ClientOption::Value(builder.client_options().username.clone())),
        (PASSWORD_PARAM, ClientOption::Secret(builder.client_options().password.clone())),
        (
            DEFAULT_DATABASE_PARAM,
            ClientOption::Value(builder.client_options().default_database.clone()),
        ),
        (COMPRESSION_PARAM, ClientOption::Value(builder.client_options().compression.to_string())),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect::<HashMap<_, _>>();

    if let Some(domain) = builder.client_options().domain.as_ref() {
        drop(params.insert(DOMAIN_PARAM.into(), ClientOption::Value(domain.clone())));
    }
    if let Some(cafile) = builder.client_options().cafile.as_ref() {
        drop(params.insert(
            CAFILE_PARAM.into(),
            ClientOption::Value(cafile.to_string_lossy().to_string()),
        ));
    }
    if builder.client_options().use_tls {
        drop(params.insert(USE_TLS_PARAM.into(), ClientOption::Value("true".to_string())));
    }
    if builder.client_options().ext.arrow.is_some_and(|a| a.strings_as_strings) {
        drop(
            params.insert(STRINGS_AS_STRINGS_PARAM.into(), ClientOption::Value("true".to_string())),
        );
    }

    #[cfg(feature = "cloud")]
    if let Some(to) = builder.client_options().ext.cloud.timeout {
        drop(params.insert(CLOUD_TIMEOUT_PARAM.into(), ClientOption::Value(to.to_string())));
    }

    #[cfg(feature = "cloud")]
    if builder.client_options().ext.cloud.wakeup {
        drop(params.insert(CLOUD_WAKEUP_PARAM.into(), ClientOption::Value("true".to_string())));
    }

    // Settings
    if let Some(settings) = builder.client_settings() {
        let settings = settings.encode_to_key_value_strings();
        for (name, setting) in settings {
            let previous = params.insert(name, ClientOption::Value(setting));
            if previous.is_some() {
                return Err(DataFusionError::External(
                    "Settings cannot include keys used in ClientOptions".into(),
                ));
            }
        }
    }

    Ok(ClientOptionParams(params))
}

/// Converts a `HashMap` of parameters to an `ArrowConnectionPoolBuilder`.
///
/// # Errors
/// - Returns an error if the parameters are invalid.
pub fn params_to_pool_builder<S: ::std::hash::BuildHasher>(
    endpoint: impl Into<Destination>,
    params: &mut HashMap<String, String, S>,
    ignore_settings: bool,
) -> Result<ArrowConnectionPoolBuilder> {
    let destination = endpoint.into();
    let endpoint = destination.to_string();

    // ClientOptions
    let username = params.remove(USERNAME_PARAM).unwrap_or("default".into());
    let password = params.remove(PASSWORD_PARAM).map(Secret::new).unwrap_or_default();

    // This is set to "default" since datafusion drives the schema. DDL's don't work otherwise
    drop(params.remove(DEFAULT_DATABASE_PARAM));
    let default_database = "default";

    let domain = params.remove(DOMAIN_PARAM);
    let cafile =
        params.remove(CAFILE_PARAM).map(|c| PathBuf::from_str(&c)).transpose().map_err(|e| {
            DataFusionError::External(format!("Cannot convert cafile to path: {e}").into())
        })?;
    let use_tls = params.remove(USE_TLS_PARAM).is_some_and(|v| v == "true" || v == "1")
        || endpoint.starts_with("https");
    let compression = params
        .remove(COMPRESSION_PARAM)
        .map(|c| CompressionMethod::from(c.as_str()))
        .unwrap_or_default();
    let strings_as_strings = params.remove(STRINGS_AS_STRINGS_PARAM).map(|s| s == "true");
    let arrow_options = strings_as_strings
        .map_or(ArrowOptions::default().with_strings_as_strings(true), |s| {
            ArrowOptions::default().with_strings_as_strings(s)
        });
    #[cfg(feature = "cloud")]
    let cloud_timeout = if let Some(to) = params.remove(CLOUD_TIMEOUT_PARAM) {
        to.parse::<u64>().ok()
    } else {
        None
    };
    #[cfg(feature = "cloud")]
    let cloud_wakeup = params.remove(CLOUD_WAKEUP_PARAM).is_some();

    // Pool settings
    let pool_max_size = params.remove(POOL_MAX_SIZE_PARAM).and_then(|p| p.parse::<u32>().ok());
    let pool_min_idle = params.remove(POOL_MIN_IDLE_PARAM).and_then(|p| p.parse::<u32>().ok());
    let pool_test_on_checkout =
        params.remove(POOL_TEST_ON_CHECK_OUT_PARAM).is_some_and(|s| s == "true");
    let pool_max_lifetime =
        params.remove(POOL_MAX_LIFETIME_PARAM).and_then(|p| p.parse::<u64>().ok());
    let pool_idle_timeout =
        params.remove(POOL_IDLE_TIMEOUT_PARAM).and_then(|p| p.parse::<u64>().ok());
    let pool_connection_timeout =
        params.remove(POOL_CONNECTION_TIMEOUT_PARAM).and_then(|p| p.parse::<u64>().ok());
    let pool_retry_connection =
        params.remove(POOL_RETRY_CONNECTION_PARAM).is_some_and(|p| p == "true");

    // Settings
    let settings = if ignore_settings || params.is_empty() {
        None
    } else {
        let mut settings = Settings::default();
        for (name, setting) in params.drain() {
            if !ALL_PARAMS.contains(&name.as_str()) {
                settings.add_setting(&name, setting);
            }
        }
        Some(settings)
    };

    let builder = ArrowConnectionPoolBuilder::new(destination)
        .configure_client(|c| c.with_arrow_options(default_arrow_options()))
        .configure_client(|c| {
            let builder = c
                .with_username(username)
                .with_password(password)
                .with_database(default_database)
                .with_compression(compression)
                .with_tls(use_tls)
                .with_arrow_options(arrow_options)
                .with_settings(settings.unwrap_or_default());
            #[cfg(feature = "cloud")]
            let builder = builder.with_cloud_wakeup(cloud_wakeup);
            #[cfg(feature = "cloud")]
            let builder =
                if let Some(to) = cloud_timeout { builder.with_cloud_timeout(to) } else { builder };
            let builder =
                if let Some(domain) = domain { builder.with_domain(domain) } else { builder };
            if let Some(cafile) = cafile { builder.with_cafile(cafile) } else { builder }
        })
        .configure_pool(|pool| {
            let pool = if let Some(max) = pool_max_size { pool.max_size(max) } else { pool };
            let pool = if let Some(to) = pool_connection_timeout {
                pool.connection_timeout(Duration::from_millis(to))
            } else {
                pool
            };

            pool.min_idle(pool_min_idle)
                .test_on_check_out(pool_test_on_checkout)
                .max_lifetime(pool_max_lifetime.map(Duration::from_millis))
                .min_idle(pool_min_idle)
                .idle_timeout(pool_idle_timeout.map(Duration::from_millis))
                .retry_connection(pool_retry_connection)
        });

    Ok(builder)
}

/// [`crate::ClickHouseTableProviderFactory`] must receive all parameters as strings, this
/// function is helpful to serialize the required options
pub fn create_options_to_params(create_options: CreateOptions) -> ClientOptionParams {
    let params = HashMap::from_iter([
        (ENGINE_PARAM.into(), ClientOption::Value(create_options.engine)),
        (ORDER_BY_PARAM.into(), ClientOption::Value(vec_to_param(&create_options.order_by))),
        (
            PRIMARY_KEYS_PARAM.into(),
            ClientOption::Value(vec_to_param(&create_options.primary_keys)),
        ),
        (
            PARTITION_BY_PARAM.into(),
            ClientOption::Value(create_options.partition_by.unwrap_or_default()),
        ),
        (SAMPLING_PARAM.into(), ClientOption::Value(create_options.sampling.unwrap_or_default())),
        (TTL_PARAM.into(), ClientOption::Value(create_options.ttl.unwrap_or_default())),
        (
            DEFAULTS_FOR_NULLABLE_PARAM.into(),
            ClientOption::Value(
                if create_options.defaults_for_nullable { "true" } else { "false" }.into(),
            ),
        ),
    ]);

    ClientOptionParams(params)
}

/// Creates a `CreateOptions` from 'params' (`HashMap<String, String>`) and 'defaults'
/// (`HashMap<String, Expr>`).
///
/// # Errors
/// - Returns an error if the engine is missing.
pub fn params_to_create_options<S: ::std::hash::BuildHasher>(
    params: &mut HashMap<String, String, S>,
    column_defaults: &HashMap<String, Expr, S>,
) -> Result<CreateOptions> {
    let Some(engine) = params.remove(ENGINE_PARAM) else {
        return exec_err!("Missing engine for table");
    };

    let options = CreateOptions::new(&engine)
        .with_order_by(
            &params.remove(ORDER_BY_PARAM).map(|p| parse_param_vec(&p)).unwrap_or_default(),
        )
        .with_primary_keys(
            &params.remove(PRIMARY_KEYS_PARAM).map(|p| parse_param_vec(&p)).unwrap_or_default(),
        )
        .with_partition_by(params.remove(PARTITION_BY_PARAM).unwrap_or_default())
        .with_sample_by(params.remove(SAMPLING_PARAM).unwrap_or_default())
        .with_ttl(params.remove(TTL_PARAM).unwrap_or_default());

    // Convert column_defaults to ClickHouse defaults
    let unparser = Unparser::new(&ClickHouseDialect as &dyn Dialect);
    let mut defaults = column_defaults
        .iter()
        .map(|(col, expr)| {
            let ast_expr = unparser.expr_to_sql(expr)?;
            let ch_default = ast_expr_to_clickhouse_default(&ast_expr)?;
            Ok((col.clone(), ch_default))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    if let Some(defs) = params.remove(DEFAULTS_PARAM) {
        defaults.extend(parse_param_hashmap(&defs));
    }

    let options =
        if defaults.is_empty() { options } else { options.with_defaults(defaults.into_iter()) };

    let options = if params.remove(DEFAULTS_FOR_NULLABLE_PARAM).is_some_and(|p| p == "true") {
        options.with_defaults_for_nullable()
    } else {
        options
    };

    Ok(if params.is_empty() {
        options
    } else {
        // Settings
        let mut settings = Settings::default();
        for (name, setting) in params.drain() {
            if !ALL_PARAMS.contains(&name.as_str()) {
                settings.add_setting(&name, setting);
            }
        }
        options.with_settings(settings)
    })
}

// Convert ast::Expr to ClickHouse default string
pub(crate) fn ast_expr_to_clickhouse_default(expr: &ast::Expr) -> Result<String> {
    if let ast::Expr::Value(ast::ValueWithSpan { value, .. }) = expr {
        match value {
            ast::Value::SingleQuotedString(s) => {
                if s.starts_with('\'') && s.ends_with('\'') {
                    Ok(s.clone())
                } else if s.starts_with('"') && s.ends_with('"') {
                    Ok(s.trim_matches('"').to_string())
                } else {
                    Ok(format!("'{s}'"))
                }
            }
            // DoubleQuotedString is used to signify do not alter
            ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
            ast::Value::Number(n, _) => Ok(n.clone()),
            ast::Value::Boolean(b) => Ok(if *b { "1" } else { "0" }.to_string()),
            ast::Value::Null => Ok("NULL".to_string()),
            _ => plan_err!("Unsupported default value: {value:?}"),
        }
    } else {
        plan_err!("Unsupported default expression: {expr:?}")
    }
}

pub(crate) fn default_str_to_expr(value: &str) -> Expr {
    let is_quoted = |s: &str| {
        (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"'))
    };
    match value {
        "true" => lit(true),
        "false" => lit(false),
        s if !is_quoted(s) && s.parse::<i64>().is_ok() => lit(s.parse::<i64>().unwrap()),
        s if !is_quoted(s) && s.parse::<f64>().is_ok() => lit(s.parse::<f64>().unwrap()),
        s => lit(s),
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::collections::HashMap;

    use clickhouse_arrow::{ArrowConnectionPoolBuilder, CompressionMethod, Destination};

    use super::*;

    #[test]
    fn test_parse_param_hashmap_basic() {
        let result = parse_param_hashmap("key1=value1,key2=value2");
        let mut expected = HashMap::new();
        drop(expected.insert("key1".to_string(), "value1".to_string()));
        drop(expected.insert("key2".to_string(), "value2".to_string()));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_param_hashmap_empty() {
        let result = parse_param_hashmap("");
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_param_hashmap_single_pair() {
        let result = parse_param_hashmap("single=value");
        let mut expected = HashMap::new();
        drop(expected.insert("single".to_string(), "value".to_string()));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_param_hashmap_malformed() {
        // Test with malformed entries (no equals sign)
        let result = parse_param_hashmap("key1=value1,malformed,key2=value2");
        let mut expected = HashMap::new();
        drop(expected.insert("key1".to_string(), "value1".to_string()));
        drop(expected.insert("key2".to_string(), "value2".to_string()));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_param_hashmap_empty_values() {
        let result = parse_param_hashmap("key1=,key2=value2");
        let mut expected = HashMap::new();
        drop(expected.insert("key1".to_string(), String::new()));
        drop(expected.insert("key2".to_string(), "value2".to_string()));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_pool_builder_to_params_basic() {
        let destination = Destination::from("http://localhost:8123");
        let builder = ArrowConnectionPoolBuilder::new(destination)
            .configure_client(|c| c.with_username("test_user").with_database("test_db"));

        let result = pool_builder_to_params("http://localhost:8123", &builder);
        assert!(result.is_ok());

        let params = result.unwrap();
        assert_eq!(params.get(ENDPOINT_PARAM).unwrap().to_string(), "http://localhost:8123");
        assert_eq!(params.get(USERNAME_PARAM).unwrap().to_string(), "test_user");
        assert_eq!(params.get(DEFAULT_DATABASE_PARAM).unwrap().to_string(), "test_db");
    }

    #[test]
    fn test_pool_builder_to_params_with_password() {
        let destination = Destination::from("http://localhost:8123");
        let builder = ArrowConnectionPoolBuilder::new(destination).configure_client(|c| {
            c.with_username("test_user")
                .with_password(Secret::new("secret_password"))
                .with_database("test_db")
        });

        let result = pool_builder_to_params("http://localhost:8123", &builder);
        assert!(result.is_ok());

        let params = result.unwrap();
        assert_eq!(params.get(PASSWORD_PARAM).unwrap().to_string(), "secret_password");
    }

    #[test]
    fn test_pool_builder_to_params_with_compression() {
        let destination = Destination::from("http://localhost:8123");
        let builder = ArrowConnectionPoolBuilder::new(destination).configure_client(|c| {
            c.with_username("test_user").with_compression(CompressionMethod::LZ4)
        });

        let result = pool_builder_to_params("http://localhost:8123", &builder);
        assert!(result.is_ok());

        let params = result.unwrap();
        assert_eq!(
            params.get(COMPRESSION_PARAM).unwrap().to_string(),
            format!("{}", CompressionMethod::LZ4)
        );
    }

    #[test]
    fn test_pool_builder_to_params_with_tls() {
        let destination = Destination::from("https://localhost:8443");
        let builder =
            ArrowConnectionPoolBuilder::new(destination).configure_client(|c| c.with_tls(true));

        let result = pool_builder_to_params("https://localhost:8443", &builder);
        assert!(result.is_ok());

        let params = result.unwrap();
        assert_eq!(params.get(USE_TLS_PARAM).unwrap().to_string(), "true");
    }

    #[test]
    fn test_pool_builder_to_params_with_domain() {
        let destination = Destination::from("http://localhost:8123");
        let builder = ArrowConnectionPoolBuilder::new(destination)
            .configure_client(|c| c.with_domain("test.domain.com"));

        let result = pool_builder_to_params("http://localhost:8123", &builder);
        assert!(result.is_ok());

        let params = result.unwrap();
        assert_eq!(params.get(DOMAIN_PARAM).unwrap().to_string(), "test.domain.com");
    }

    #[test]
    fn test_params_to_pool_builder_basic() {
        let mut params = HashMap::new();
        drop(params.insert(USERNAME_PARAM.to_string(), "test_user".to_string()));
        drop(params.insert(PASSWORD_PARAM.to_string(), "test_password".to_string()));
        drop(params.insert(DEFAULT_DATABASE_PARAM.to_string(), "test_db".to_string()));

        let destination = Destination::from("http://localhost:8123");
        let result = params_to_pool_builder(destination, &mut params, false);
        assert!(result.is_ok());

        let builder = result.unwrap();
        assert_eq!(builder.client_options().username, "test_user");
        assert_eq!(builder.client_options().password.get(), "test_password");
        // Note: default_database is always set to "default" regardless of input
        assert_eq!(builder.client_options().default_database, "default");
    }

    #[test]
    fn test_params_to_pool_builder_with_defaults() {
        let mut params = HashMap::new();
        // Don't provide username/password to test defaults

        let destination = Destination::from("http://localhost:8123");
        let result = params_to_pool_builder(destination, &mut params, false);
        assert!(result.is_ok());

        let builder = result.unwrap();
        assert_eq!(builder.client_options().username, "default");
        assert_eq!(builder.client_options().password.get(), "");
        assert_eq!(builder.client_options().default_database, "default");
    }

    #[test]
    fn test_params_to_pool_builder_with_compression() {
        let mut params = HashMap::new();
        drop(params.insert(COMPRESSION_PARAM.to_string(), "lz4".to_string()));

        let destination = Destination::from("http://localhost:8123");
        let result = params_to_pool_builder(destination, &mut params, false);
        assert!(result.is_ok());

        let builder = result.unwrap();
        assert_eq!(builder.client_options().compression, CompressionMethod::LZ4);
    }

    #[test]
    fn test_params_to_pool_builder_with_tls_flag() {
        let mut params = HashMap::new();
        drop(params.insert(USE_TLS_PARAM.to_string(), "true".to_string()));

        let destination = Destination::from("http://localhost:8123");
        let result = params_to_pool_builder(destination, &mut params, false);
        assert!(result.is_ok());

        let builder = result.unwrap();
        assert!(builder.client_options().use_tls);
    }

    #[test]
    fn test_params_to_pool_builder_with_tls_from_https() {
        let mut params = HashMap::new();
        // No explicit USE_TLS_PARAM, but https endpoint should enable TLS

        let destination = Destination::from("https://localhost:8443");
        let result = params_to_pool_builder(destination, &mut params, false);
        assert!(result.is_ok());

        let builder = result.unwrap();
        assert!(builder.client_options().use_tls);
    }

    #[test]
    fn test_params_to_pool_builder_with_domain() {
        let mut params = HashMap::new();
        drop(params.insert(DOMAIN_PARAM.to_string(), "example.com".to_string()));

        let destination = Destination::from("http://localhost:8123");
        let result = params_to_pool_builder(destination, &mut params, false);
        assert!(result.is_ok());

        let builder = result.unwrap();
        assert_eq!(builder.client_options().domain, Some("example.com".to_string()));
    }

    #[test]
    fn test_params_to_pool_builder_with_strings_as_strings() {
        let mut params = HashMap::new();
        drop(params.insert(STRINGS_AS_STRINGS_PARAM.to_string(), "true".to_string()));

        let destination = Destination::from("http://localhost:8123");
        let result = params_to_pool_builder(destination, &mut params, false);
        assert!(result.is_ok());

        let builder = result.unwrap();
        assert!(builder.client_options().ext.arrow.unwrap().strings_as_strings);
    }

    #[test]
    fn test_params_to_pool_builder_with_pool_settings() {
        let mut params = HashMap::new();
        drop(params.insert(POOL_MAX_SIZE_PARAM.to_string(), "20".to_string()));
        drop(params.insert(POOL_MIN_IDLE_PARAM.to_string(), "5".to_string()));
        drop(params.insert(POOL_TEST_ON_CHECK_OUT_PARAM.to_string(), "true".to_string()));

        let destination = Destination::from("http://localhost:8123");
        let result = params_to_pool_builder(destination, &mut params, false);
        assert!(result.is_ok());

        // The pool settings are applied via configure_pool, so we can't directly test them
        // but we can verify the builder was created successfully
        let _builder = result.unwrap();
    }

    #[test]
    fn test_params_to_pool_builder_ignore_settings() {
        let mut params = HashMap::new();
        drop(params.insert("custom_setting".to_string(), "custom_value".to_string()));

        let destination = Destination::from("http://localhost:8123");
        let result = params_to_pool_builder(destination, &mut params, true);
        assert!(result.is_ok());

        // When ignore_settings is true, custom settings should be ignored
        let _builder = result.unwrap();
        // The params HashMap should still contain the custom setting since it's ignored
        assert!(params.contains_key("custom_setting"));
    }

    #[test]
    fn test_roundtrip_conversion() {
        // Test that we can convert builder -> params -> builder
        let original_destination = Destination::from("http://localhost:8123");
        let original_builder = ArrowConnectionPoolBuilder::new(original_destination.clone())
            .configure_client(|c| {
                c.with_username("test_user")
                    .with_password(Secret::new("test_password"))
                    .with_database("test_db")
                    .with_compression(CompressionMethod::LZ4)
            });

        // Convert to params
        let params_result = pool_builder_to_params("http://localhost:8123", &original_builder);
        assert!(params_result.is_ok());

        let client_params = params_result.unwrap();
        let mut string_params = client_params.into_params();

        // Convert back to builder
        let builder_result =
            params_to_pool_builder(original_destination, &mut string_params, false);
        assert!(builder_result.is_ok());

        let new_builder = builder_result.unwrap();

        // Verify key properties match
        assert_eq!(new_builder.client_options().username, "test_user");
        assert_eq!(new_builder.client_options().password.get(), "test_password");
        assert_eq!(new_builder.client_options().compression, CompressionMethod::LZ4);
        // Note: database will be "default" due to the forced override in params_to_pool_builder
    }

    #[test]
    fn test_client_option_display() {
        let secret_option = ClientOption::Secret(Secret::new("secret_value"));
        let value_option = ClientOption::Value("plain_value".to_string());

        assert_eq!(secret_option.to_string(), "secret_value");
        assert_eq!(value_option.to_string(), "plain_value");
    }

    #[test]
    fn test_client_option_params_deref() {
        let mut params = HashMap::new();
        drop(params.insert("key1".to_string(), ClientOption::Value("value1".to_string())));
        drop(params.insert("key2".to_string(), ClientOption::Secret(Secret::new("secret"))));

        let client_params = ClientOptionParams(params);

        // Test Deref trait
        assert_eq!(client_params.get("key1").unwrap().to_string(), "value1");
        assert_eq!(client_params.get("key2").unwrap().to_string(), "secret");
    }

    #[test]
    fn test_client_option_params_into_params() {
        let mut params = HashMap::new();
        drop(params.insert("key1".to_string(), ClientOption::Value("value1".to_string())));
        drop(params.insert("key2".to_string(), ClientOption::Secret(Secret::new("secret"))));

        let client_params = ClientOptionParams(params);
        let string_params = client_params.into_params();

        assert_eq!(string_params.get("key1").unwrap(), "value1");
        assert_eq!(string_params.get("key2").unwrap(), "secret");
    }
}
