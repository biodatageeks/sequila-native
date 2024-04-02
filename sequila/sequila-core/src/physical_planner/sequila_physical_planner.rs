use crate::physical_planner::joins::IntervalSearchJoinExec;
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::DFSchema;
use datafusion::config::ConfigOptions;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan, Operator};
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use log::info;
use std::sync::Arc;

#[derive(Default)]
pub struct SeQuiLaPhysicalPlanner {
    planner: DefaultPhysicalPlanner,
}

#[derive(Debug, Default)]
pub struct RangeJoinPhysicalOptimizationRule;

impl PhysicalOptimizerRule for RangeJoinPhysicalOptimizationRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        info!("Applying {}...", self.name());
        plan.transform_down(&|plan| {
            match plan.as_any().downcast_ref::<HashJoinExec>() {
                //TODO: Add a check if the plan is a range join (e.g. pattern matching on join and filters) and not only a hash join
                Some(join_exec) if is_range_join(join_exec) => {
                    info!("Detected HashJoinExec with Range filters. Optimizing into IntervalSearchJoin...");
                    // info!("Join {:#?}", join_exec);
                    let new_plan = IntervalSearchJoinExec::try_new(
                        join_exec.left().clone(),
                        join_exec.right().clone(),
                        join_exec.on.clone(),
                        join_exec.filter.clone(),
                        &join_exec.join_type,
                        join_exec.partition_mode().clone(),
                        join_exec.null_equals_null,
                    )?;

                    Ok(Transformed::Yes(Arc::new(new_plan)))
                },
                _ => Ok(Transformed::No(plan))
            }
        })
    }

    fn name(&self) -> &str {
        "RangeJoinOptimizationRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn is_range_join(join_expr: &HashJoinExec) -> bool {
    join_expr
        .filter
        .as_ref()
        .map(|filter| count_range(filter.expression()))
        .map_or(false, |has_range| has_range >= 2)
}

fn count_range(expr: &Arc<dyn PhysicalExpr>) -> i32 {
    match expr.as_any().downcast_ref::<BinaryExpr>() {
        None => 0,
        Some(expr) => {
            let c = if is_range(expr) { 1 } else { 0 };
            count_range(expr.left()) + c + count_range(expr.right())
        }
    }
}

fn is_range(expr: &BinaryExpr) -> bool {
    match expr.op() {
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => true,
        _ => false,
    }
}

#[async_trait]
impl PhysicalPlanner for SeQuiLaPhysicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let plan = self
            .planner
            .create_physical_plan(logical_plan, session_state)
            .await?;
        Ok(plan)
    }

    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        self.planner
            .create_physical_expr(expr, input_dfschema, session_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::JoinSide;
    use datafusion::common::JoinType;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::joins::utils::ColumnIndex;
    use datafusion::physical_plan::joins::utils::JoinFilter;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::physical_plan::memory::MemoryExec;

    pub fn build_table_i32(
        a: (&str, &Vec<&str>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Utf8, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap()
    }

    pub fn build_table_scan(
        a: (&str, &Vec<&str>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    #[test]
    fn test_is_range_join() {
        let reads = build_table_scan(
            ("contig", &vec!["chr1", "chr1", "chr1"]),
            ("pos_start", &vec![150, 190, 300]),
            ("pos_end", &vec![250, 300, 501]),
        );
        let targets = build_table_scan(
            ("contig", &vec!["chr1", "chr1", "chr1"]),
            ("pos_start", &vec![100, 200, 400]),
            ("pos_end", &vec![190, 290, 600]),
        );

        let on = vec![(
            Column::new_with_schema("contig", &reads.schema()).unwrap(),
            Column::new_with_schema("contig", &targets.schema()).unwrap(),
        )];

        let column_indices = vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ];
        let intermediate_schema = Schema::new(vec![
            Field::new("pos_start", DataType::Int32, true),
            Field::new("pos_start", DataType::Int32, true),
            Field::new("pos_end", DataType::Int32, true),
            Field::new("pos_end", DataType::Int32, true),
        ]);

        let left_filter = BinaryExpr::new(
            Arc::new(Column::new("pos_start", 0)),
            Operator::GtEq,
            Arc::new(Column::new("pos_start", 2)),
        );

        let right_filter = BinaryExpr::new(
            Arc::new(Column::new("pos_end", 1)),
            Operator::LtEq,
            Arc::new(Column::new("pos_end", 3)),
        );

        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(left_filter),
            Operator::And,
            Arc::new(right_filter),
        )) as Arc<dyn PhysicalExpr>;

        let filter = JoinFilter::new(
            filter_expression,
            column_indices.clone(),
            intermediate_schema.clone(),
        );

        let join = HashJoinExec::try_new(
            reads,
            targets,
            on,
            Some(filter),
            &JoinType::Inner,
            PartitionMode::CollectLeft,
            true,
        )
        .unwrap();

        assert!(is_range_join(&join));
    }
}
