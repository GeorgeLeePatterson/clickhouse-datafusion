use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::DisplayAs;
use datafusion::sql::TableReference;
use futures_util::StreamExt;

use crate::connection::ClickHouseConnectionPool;

// TODO: Docs
//
/// [`DataSink`] for `ClickHouse` [`datafusion::datasource::TableProvider::insert_into`]
#[derive(Debug)]
pub struct ClickHouseDataSink {
    writer: ClickHouseConnectionPool,
    table:  TableReference,
    schema: SchemaRef,
}

impl ClickHouseDataSink {
    pub fn new(writer: ClickHouseConnectionPool, table: TableReference, schema: SchemaRef) -> Self {
        Self { writer, table, schema }
    }

    // It's important that the schemas align, but ordering and metadata don't matter
    fn verify_input_schema(&self, input: &SchemaRef) -> Result<()> {
        let sink_fields = self.schema.fields();
        let input_fields = input.fields();
        if sink_fields.len() != input_fields.len() {
            let (input_len, sink_len) = (input_fields.len(), sink_fields.len());
            return exec_err!(
                "Schema fields must match, input has {input_len} fields, sink {sink_len}"
            );
        }

        for field in sink_fields {
            let name = field.name();
            let data_type = field.data_type();
            let is_nullable = field.is_nullable();

            let Some((_, input_field)) = input_fields.find(name) else {
                return exec_err!("Sink field {name} missing from input");
            };

            if data_type != input_field.data_type() {
                return exec_err!(
                    "Sink field {name} expected data type {data_type:?} but found {:?}",
                    input_field.data_type()
                );
            }

            if is_nullable != input_field.is_nullable() {
                return exec_err!(
                    "Sink field {name} expected nullability {is_nullable} but found {}",
                    input_field.is_nullable()
                );
            }
        }

        Ok(())
    }
}

impl DisplayAs for ClickHouseDataSink {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "ClickHouseDataSink: table={}", self.table)
    }
}

#[async_trait]
impl DataSink for ClickHouseDataSink {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn schema(&self) -> &SchemaRef { &self.schema }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let db = self.table.schema();
        let table = self.table.table();

        let query = if let Some(db) = db {
            format!("INSERT INTO {db}.{table} FORMAT Native")
        } else {
            format!("INSERT INTO {table} FORMAT Native")
        };

        let mut row_count = 0;

        let pool =
            self.writer.pool().get().await.map_err(|e| DataFusionError::External(Box::new(e)))?;

        while let Some(batch) = data.next().await.transpose()? {
            // Runtime schema validation
            self.verify_input_schema(batch.schema_ref())?;

            let num_rows = batch.num_rows();
            let mut results = pool
                .insert(&query, batch, None)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Drain the result stream to ensure the insert completes
            while let Some(result) = results.next().await {
                result.map_err(|e| DataFusionError::External(Box::new(e)))?;
            }

            row_count += num_rows as u64;
        }

        Ok(row_count)
    }
}
