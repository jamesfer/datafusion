use std::sync::Arc;
use arrow::array::RecordBatchOptions;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::DataFusionError;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use crate::projection::ProjectionExec;

// impl TryInto<ProjectionStreamingTask> for ProjectionExec {
//     type Error = ();
//
//     fn try_into(self) -> Result<ProjectionStreamingTask, Self::Error> {
//         todo!()
//     }
// }
//
// impl TryFrom<ProjectionExec> for ProjectionStreamingTask {
//     type Error = ();
//
//     fn try_from(value: ProjectionExec) -> Result<Self, Self::Error> {
//         value.expr()
//         todo!()
//     }
// }

pub struct ProjectionStreamingTask {
    schema: SchemaRef,
    expr: Vec<Arc<dyn PhysicalExpr>>,
}

impl ProjectionStreamingTask {
    pub fn new(schema: SchemaRef, expr: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self { schema, expr }
    }
}

impl ProjectionStreamingTask {
    pub fn process_batch(&self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        // Records time on drop
        // let _timer = self.baseline_metrics.elapsed_compute().timer();
        let arrays = self
            .expr
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        if arrays.is_empty() {
            let options =
                RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(Arc::clone(&self.schema), arrays, &options)
                .map_err(Into::into)
        } else {
            RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(Into::into)
        }
    }
}
