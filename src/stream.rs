use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures_util::{Stream, StreamExt};
use pin_project::pin_project;

// TODO: Docs - also does DataFusion provide anything that makes this unnecessary?
//
// Stream adapter for ClickHouse query results
#[pin_project]
pub struct RecordBatchStream {
    schema: SchemaRef,
    #[pin]
    stream: Pin<Box<dyn Stream<Item = DataFusionResult<RecordBatch>> + Send>>,
}

impl RecordBatchStream {
    pub fn new(
        schema: SchemaRef,
        stream: Pin<Box<dyn Stream<Item = DataFusionResult<RecordBatch>> + Send>>,
    ) -> Self {
        Self { schema, stream }
    }
}

impl Stream for RecordBatchStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.as_mut().project().stream.poll_next(cx)
    }
}

impl datafusion::physical_plan::RecordBatchStream for RecordBatchStream {
    fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }
}

/// Helper function to create a `SendableRecordBatchStream` from a stream of `RecordBatch`es where
/// the schema must be extracted from the first batch.
///
/// # Errors
/// - Returns an error if the stream is empty or the first batch fails.
pub async fn record_batch_stream_from_stream(
    mut stream: impl Stream<Item = DataFusionResult<RecordBatch>> + Send + Unpin + 'static,
) -> DataFusionResult<SendableRecordBatchStream> {
    let Some(first_batch) = stream.next().await else {
        return exec_err!("No schema provided and record batch stream is empty");
    };
    let first_batch = first_batch?;
    let schema = first_batch.schema();
    let stream = Box::pin(futures_util::stream::once(async { Ok(first_batch) }).chain(stream));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::RecordBatchStream as RecordBatchStreamTrait;
    use futures_util::stream;

    use super::*;

    fn create_test_record_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_record_batch_stream_new() {
        let batch = create_test_record_batch();
        let schema = batch.schema();
        let stream = Box::pin(stream::once(async move { Ok(batch) }));

        let record_batch_stream = RecordBatchStream::new(Arc::clone(&schema), stream);
        assert_eq!(record_batch_stream.schema(), schema);
    }

    #[test]
    fn test_record_batch_stream_schema() {
        let batch = create_test_record_batch();
        let schema = batch.schema();
        let stream = Box::pin(stream::once(async move { Ok(batch) }));

        let record_batch_stream = RecordBatchStream::new(Arc::clone(&schema), stream);
        let returned_schema = record_batch_stream.schema();

        assert_eq!(returned_schema.fields().len(), 2);
        assert_eq!(returned_schema.field(0).name(), "id");
        assert_eq!(returned_schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_record_batch_stream_poll_next() {
        let batch = create_test_record_batch();
        let schema = batch.schema();
        let stream = Box::pin(stream::once(async move { Ok(batch.clone()) }));

        let mut record_batch_stream = RecordBatchStream::new(schema, stream);

        // Create a mock context for polling
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);

        // Poll the stream
        let pinned = Pin::new(&mut record_batch_stream);
        if let Poll::Ready(Some(result)) = pinned.poll_next(&mut context) {
            let received_batch = result.unwrap();
            assert_eq!(received_batch.num_rows(), 3);
            assert_eq!(received_batch.num_columns(), 2);
        } else {
            panic!("Expected Poll::Ready with batch");
        }
    }

    #[tokio::test]
    async fn test_record_batch_stream_from_stream_success() {
        let batch1 = create_test_record_batch();
        let batch2 = create_test_record_batch();
        let test_stream = stream::iter(vec![Ok(batch1.clone()), Ok(batch2)]);

        let result = record_batch_stream_from_stream(test_stream).await;
        assert!(result.is_ok());

        let mut sendable_stream = result.unwrap();
        let first_batch = sendable_stream.next().await.unwrap().unwrap();
        assert_eq!(first_batch.num_rows(), 3);
        assert_eq!(first_batch.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_record_batch_stream_from_stream_empty() {
        let empty_stream = stream::iter(Vec::<DataFusionResult<RecordBatch>>::new());

        match record_batch_stream_from_stream(empty_stream).await {
            Ok(_) => panic!("Expected error for empty stream"),
            Err(error) => {
                assert!(error.to_string().contains("record batch stream is empty"));
            }
        }
    }

    #[tokio::test]
    async fn test_record_batch_stream_from_stream_first_batch_error() {
        use datafusion::common::DataFusionError;

        let error_stream =
            stream::iter(vec![Err(DataFusionError::Internal("test error".to_string()))]);

        match record_batch_stream_from_stream(error_stream).await {
            Ok(_) => panic!("Expected error from first batch"),
            Err(error) => {
                assert!(error.to_string().contains("test error"));
            }
        }
    }

    #[tokio::test]
    async fn test_record_batch_stream_from_stream_multiple_batches() {
        let batch1 = create_test_record_batch();
        let batch2 = create_test_record_batch();
        let batch3 = create_test_record_batch();

        let test_stream = stream::iter(vec![Ok(batch1), Ok(batch2), Ok(batch3)]);

        let result = record_batch_stream_from_stream(test_stream).await;
        assert!(result.is_ok());

        let mut sendable_stream = result.unwrap();
        let mut count = 0;
        while let Some(batch_result) = sendable_stream.next().await {
            let batch = batch_result.unwrap();
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 2);
            count += 1;
        }
        assert_eq!(count, 3);
    }
}
