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
