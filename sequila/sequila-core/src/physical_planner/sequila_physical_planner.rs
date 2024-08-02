use crate::physical_planner::joins::interval_join::{parse_intervals, IntervalJoinExec};
use crate::session_context::SequilaConfig;
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
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
pub struct IntervalJoinPhysicalOptimizationRule;

impl PhysicalOptimizerRule for IntervalJoinPhysicalOptimizationRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        info!("Applying {}...", self.name());
        let default = SequilaConfig::default();
        let sequila_config = config.extensions.get::<SequilaConfig>().unwrap_or(&default);

        if !sequila_config.prefer_interval_join {
            return Ok(plan);
        }

        plan.transform_down(|plan| {
            match plan.as_any().downcast_ref::<HashJoinExec>() {
                //TODO: Add a check if the plan is a range join (e.g. pattern matching on join and filters) and not only a hash join
                Some(join_exec) => {
                    if let Some(intervals) = join_exec.filter().and_then(parse_intervals) {
                        info!("Detected HashJoinExec with Range filters. Optimizing into IntervalSearchJoin...");
                        info!("sequila_config: {sequila_config:?}");
                        let new_plan = IntervalJoinExec::try_new(
                            join_exec.left().clone(),
                            join_exec.right().clone(),
                            join_exec.on.clone(),
                            join_exec.filter.clone(),
                            intervals,
                            &join_exec.join_type,
                            join_exec.projection.clone(),
                            *join_exec.partition_mode(),
                            join_exec.null_equals_null,
                            sequila_config.interval_join_algorithm.clone(),
                        )?;

                        Ok(Transformed::yes(Arc::new(new_plan)))
                    } else {
                        info!("{} was not applied", self.name());
                        Ok(Transformed::no(plan))
                    }
                },
                _ => Ok(Transformed::no(plan))
            }
        }).data()
    }

    fn name(&self) -> &str {
        "IntervalJoinOptimizationRule"
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
