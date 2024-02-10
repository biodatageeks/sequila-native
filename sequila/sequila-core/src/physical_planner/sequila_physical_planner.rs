use crate::physical_planner::joins::IntervalSearchJoinExec;
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::DFSchema;
use datafusion::config::ConfigOptions;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan};
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
            Ok(
                if let Some(join_exec) = plan.as_any().downcast_ref::<HashJoinExec>(){
                    info!("Detected HashJoinExec with Range filters. Optimizing into IntervalSearchJoin...");
                    Transformed::Yes(Arc::new(IntervalSearchJoinExec::try_new(
                        join_exec.left().clone(),
                        join_exec.right().clone(),
                        join_exec.on.clone(),
                        join_exec.filter.clone(),
                        &join_exec.join_type,
                        join_exec.partition_mode().clone(),
                        join_exec.null_equals_null,
                    )?))
                } else {
                    Transformed::No(plan)
                })
        })
    }

    fn name(&self) -> &str {
        "RangeJoinOptimizationRule"
    }

    fn schema_check(&self) -> bool {
        true
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
