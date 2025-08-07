use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
use datafusion::error::Result as DataFusionResult;
use futures_util::{Stream, StreamExt, TryStreamExt, ready};
use pin_project::pin_project;

use crate::ClickHouseConnectionPool;

pub type RecordBatchStreamWrapper =
    RecordBatchStream<Pin<Box<dyn Stream<Item = DataFusionResult<RecordBatch>> + Send>>>;

// TODO: Docs - also does DataFusion provide anything that makes this unnecessary?
//
// Stream adapter for ClickHouse query results
#[pin_project]
pub struct RecordBatchStream<S> {
    schema:        SchemaRef,
    #[pin]
    stream:        S,
    // TODO: Support actual arrow CastOptions, using that as indicator of whether to coerce types
    coerce_schema: bool,
}

impl<S> RecordBatchStream<S> {
    pub fn new(stream: S, schema: SchemaRef) -> Self {
        Self { schema, stream, coerce_schema: false }
    }

    #[must_use]
    pub fn with_coercion(mut self, coerce: bool) -> Self {
        self.coerce_schema = coerce;
        self
    }

    /// Coerce the schema of the returned record batch to match the expected schema
    ///
    /// Assumes the fields are in the same order. Will blindly attempt to cast, returning an error
    /// if the arrow cast fails.
    fn coerce_batch_schema(&self, batch: RecordBatch) -> DataFusionResult<RecordBatch> {
        if self.coerce_schema {
            let (batch_schema, mut arrays, _) = batch.into_parts();

            let from_fields = batch_schema.fields();
            let to_fields = self.schema.fields();
            if from_fields.len() != to_fields.len() {
                return exec_err!("Cannot coerce types, incompatible schemas");
            }

            let mut new_arrays = Vec::with_capacity(arrays.len());
            let field_map = batch_schema.fields().iter().zip(self.schema.fields().iter());

            // Reverse to allow popping from the end
            for (from_field, to_field) in field_map.rev() {
                let Some(current_array) = arrays.pop() else {
                    return exec_err!("Cannot coerce types, missing array");
                };

                if from_field.data_type() == to_field.data_type() {
                    new_arrays.push(current_array);
                } else {
                    let new_array = cast(&current_array, to_field.data_type())?;
                    new_arrays.push(new_array);
                }
            }

            // Reverse the arrays back
            new_arrays.reverse();
            Ok(RecordBatch::try_new(Arc::clone(&self.schema), new_arrays)?)
        } else {
            Ok(batch)
        }
    }
}

impl RecordBatchStreamWrapper {
    pub fn new_from_stream(
        stream: Pin<Box<dyn Stream<Item = DataFusionResult<RecordBatch>> + Send>>,
        schema: SchemaRef,
    ) -> Self {
        Self { schema, stream, coerce_schema: false }
    }

    pub fn new_from_query(
        sql: impl Into<String>,
        pool: Arc<ClickHouseConnectionPool>,
        schema: SchemaRef,
        coerce_schema: bool,
    ) -> Self {
        let sql = sql.into();
        let pool_schema = Arc::clone(&schema);
        let stream = Box::pin(
            futures_util::stream::once(async move {
                pool.connect()
                    .await?
                    .query_arrow_with_schema(&sql, &[], pool_schema, coerce_schema)
                    .await
            })
            .try_flatten(),
        );
        Self { schema, stream, coerce_schema: false }
    }
}

impl<S> Stream for RecordBatchStream<S>
where
    S: Stream<Item = DataFusionResult<RecordBatch>>,
{
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if !self.coerce_schema {
            return self.as_mut().project().stream.poll_next(cx);
        }

        Poll::Ready(match ready!(self.as_mut().project().stream.poll_next(cx)) {
            Some(batch) => Some(self.coerce_batch_schema(batch?)),
            None => None,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) { self.stream.size_hint() }
}

impl<S> datafusion::physical_plan::RecordBatchStream for RecordBatchStream<S>
where
    S: Stream<Item = DataFusionResult<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }
}

/// Helper function to create a `SendableRecordBatchStream` from a stream of `RecordBatch`es where
/// the schema must be extracted from the first batch.
///
/// # Errors
/// - Returns an error if the stream is empty or the first batch fails.
pub async fn record_batch_stream_from_stream(
    mut stream: impl Stream<Item = DataFusionResult<RecordBatch>> + Send + Unpin + 'static,
) -> DataFusionResult<RecordBatchStreamWrapper> {
    let Some(first_batch) = stream.next().await else {
        return exec_err!("No schema provided and record batch stream is empty");
    };
    let first_batch = first_batch?;
    let schema = first_batch.schema();
    let stream = Box::pin(futures_util::stream::once(async { Ok(first_batch) }).chain(stream));
    Ok(RecordBatchStream::new_from_stream(stream, schema))
}

#[cfg(all(test, feature = "test-utils"))]
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

        let record_batch_stream =
            RecordBatchStreamWrapper::new_from_stream(stream, Arc::clone(&schema));
        assert_eq!(record_batch_stream.schema(), schema);
    }

    #[test]
    fn test_record_batch_stream_schema() {
        let batch = create_test_record_batch();
        let schema = batch.schema();
        let stream = Box::pin(stream::once(async move { Ok(batch) }));

        let record_batch_stream =
            RecordBatchStreamWrapper::new_from_stream(stream, Arc::clone(&schema));
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

        let mut record_batch_stream = RecordBatchStreamWrapper::new_from_stream(stream, schema);

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
