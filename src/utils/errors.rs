use clickhouse_arrow::Error as ClickhouseNativeError;
use datafusion::error::DataFusionError;

/// Helper to map [`ClickhouseNativeError`] to [`DataFusionError`]
pub fn map_clickhouse_err(error: ClickhouseNativeError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Helper to map any [`std::error::Error`] to [`DataFusionError`]
pub fn map_external_err<E>(error: E) -> DataFusionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use clickhouse_arrow::Error as ClickhouseNativeError;

    use super::*;

    #[derive(Debug)]
    struct TestError {
        message: String,
    }

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.message) }
    }

    impl std::error::Error for TestError {}

    #[test]
    fn test_map_clickhouse_err() {
        let clickhouse_error = ClickhouseNativeError::Protocol("test error".to_string());
        let datafusion_error = map_clickhouse_err(clickhouse_error);

        match datafusion_error {
            DataFusionError::External(boxed_error) => {
                assert_eq!(boxed_error.to_string(), "protocol error: test error");
            }
            _ => panic!("Expected External error"),
        }
    }

    #[test]
    fn test_map_external_err() {
        let test_error = TestError { message: "custom test error".to_string() };
        let datafusion_error = map_external_err(test_error);

        match datafusion_error {
            DataFusionError::External(boxed_error) => {
                assert_eq!(boxed_error.to_string(), "custom test error");
            }
            _ => panic!("Expected External error"),
        }
    }

    #[test]
    fn test_map_external_err_with_io_error() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let datafusion_error = map_external_err(io_error);

        match datafusion_error {
            DataFusionError::External(boxed_error) => {
                assert_eq!(boxed_error.to_string(), "file not found");
            }
            _ => panic!("Expected External error"),
        }
    }
}
