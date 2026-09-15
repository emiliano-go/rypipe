use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::Result;

/// Consumer for streaming `RecordBatch` output.
///
/// The engine transfers ownership of each batch to `consume`. Consumers that
/// drop batches avoid accumulating output; input and parser buffers still
/// contribute to peak memory. See `BoundedExecutor::run_stream`.
pub trait BatchConsumer {
    fn consume(&mut self, batch: RecordBatch) -> Result<()>;
}

/// Collecting consumer that accumulates batches into a `Vec`.
///
/// Used to implement the legacy `run` / `run_bytes` methods via `run_stream`.
pub struct CollectingConsumer(pub Vec<RecordBatch>);

impl CollectingConsumer {
    /// Align collected batches when columns first appear in later chunks.
    pub fn finish(self) -> Result<Vec<RecordBatch>> {
        let Some(first) = self.0.first() else {
            return Ok(self.0);
        };
        if self.0.iter().all(|batch| batch.schema() == first.schema()) {
            return Ok(self.0);
        }
        let mut encodings = std::collections::HashMap::<String, u8>::new();
        for batch in &self.0 {
            for field in batch.schema().fields() {
                let encoding = match field.data_type() {
                    DataType::Utf8 => 1,
                    DataType::Dictionary(_, value) if **value == DataType::Utf8 => 2,
                    _ => continue,
                };
                *encodings.entry(field.name().clone()).or_default() |= encoding;
            }
        }
        let schema = std::sync::Arc::new(Schema::try_merge(self.0.iter().map(|batch| {
            let schema = batch.schema();
            let fields = schema
                .fields()
                .iter()
                .map(|field| {
                    if encodings.get(field.name()) == Some(&3) {
                        field.as_ref().clone().with_data_type(DataType::Utf8)
                    } else {
                        field.as_ref().clone()
                    }
                })
                .collect::<Vec<_>>();
            Schema::new_with_metadata(fields, schema.metadata().clone())
        }))?);
        self.0
            .into_iter()
            .map(|batch| {
                let columns = schema
                    .fields()
                    .iter()
                    .map(|field| -> Result<_> {
                        match batch.column_by_name(field.name()) {
                            Some(column) if column.data_type() != field.data_type() => {
                                Ok(arrow::compute::cast(column, field.data_type())?)
                            }
                            Some(column) => Ok(column.clone()),
                            None => Ok(crate::arrow_export::null_array(
                                field.data_type(),
                                batch.num_rows(),
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(RecordBatch::try_new_with_options(
                    schema.clone(),
                    columns,
                    &arrow::record_batch::RecordBatchOptions::new()
                        .with_row_count(Some(batch.num_rows())),
                )?)
            })
            .collect()
    }
}

impl BatchConsumer for CollectingConsumer {
    fn consume(&mut self, batch: RecordBatch) -> Result<()> {
        self.0.push(batch);
        Ok(())
    }
}

/// No-op consumer that drops batches immediately.
///
/// Useful for throughput benchmarks with constant memory.
pub struct DiscardingConsumer;

impl BatchConsumer for DiscardingConsumer {
    fn consume(&mut self, _batch: RecordBatch) -> Result<()> {
        Ok(())
    }
}
