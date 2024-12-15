use crate::physical_planner::IntervalJoinPhysicalOptimizationRule;
use crate::physical_planner::SeQuiLaQueryPlanner;
use async_trait::async_trait;
use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::coalesce_batches::CoalesceBatches;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_optimizer::limited_distinct_aggregation::LimitedDistinctAggregation;
use datafusion::physical_optimizer::output_requirements::OutputRequirements;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_optimizer::topk_aggregation::TopKAggregation;
use datafusion::physical_optimizer::update_aggr_exprs::OptimizeAggregateOrder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::prelude::{SessionConfig, SessionContext};
use log::info;
use std::str::FromStr;
use std::sync::Arc;

/// Extension trait for [`SessionContext`] that adds Exon-specific functionality.
#[async_trait]
pub trait SeQuiLaSessionExt {
    fn new_with_sequila(config: SessionConfig) -> SessionContext;
    fn with_config_rt_sequila(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> SessionContext;
}

impl SeQuiLaSessionExt for SessionContext {
    fn new_with_sequila(config: SessionConfig) -> SessionContext {
        let plugin = emojis::get_by_shortcode("electric_plug").unwrap();
        info!("Loading SeQuiLaSessionExt {plugin}...");
        let runtime = Arc::new(RuntimeEnv::default());
        Self::with_config_rt_sequila(config, runtime)
    }

    fn with_config_rt_sequila(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> SessionContext {
        let rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![
            Arc::new(OutputRequirements::new_add_mode()),
            Arc::new(AggregateStatistics::new()),
            //We need to have control over swapping left and right sides for nearest operation
            // as it's not symmetric.
            //FIXME: disable only one
            // Arc::new(JoinSelection::new()),
            Arc::new(LimitedDistinctAggregation::new()),
            Arc::new(EnforceDistribution::new()),
            Arc::new(CombinePartialFinalAggregate::new()),
            Arc::new(EnforceSorting::new()),
            Arc::new(OptimizeAggregateOrder::new()),
            Arc::new(ProjectionPushdown::new()),
            Arc::new(CoalesceBatches::new()),
            Arc::new(OutputRequirements::new_remove_mode()),
            Arc::new(TopKAggregation::new()),
            Arc::new(ProjectionPushdown::new()),
            Arc::new(LimitPushdown::new()),
            Arc::new(SanityCheckPlan::new()),
            Arc::new(IntervalJoinPhysicalOptimizationRule),
        ];
        let ctx: SessionContext = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_query_planner(Arc::new(SeQuiLaQueryPlanner))
            .with_physical_optimizer_rules(rules)
            .build()
            .into();

        let hammer = emojis::get_by_shortcode("hammer_and_wrench").unwrap();
        info!("Initialized SeQuiLaQueryPlanner {hammer}...");

        ctx
    }
}

extensions_options! {
    pub struct SequilaConfig {
        pub prefer_interval_join: bool, default = true
        pub interval_join_algorithm: Algorithm, default = Algorithm::default()
    }
}

impl ConfigExtension for SequilaConfig {
    const PREFIX: &'static str = "sequila";
}

#[derive(Debug, Eq, PartialEq, Default, Clone, Copy)]
pub enum Algorithm {
    #[default]
    Coitrees,
    IntervalTree,
    ArrayIntervalTree,
    AIList,
    Lapper,
    CoitreesNearest,
}

#[derive(Debug)]
pub struct ParseAlgorithmError(String);

impl std::fmt::Display for ParseAlgorithmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseAlgorithmError {}

impl FromStr for Algorithm {
    type Err = ParseAlgorithmError;

    #[inline]
    fn from_str(s: &str) -> Result<Algorithm, Self::Err> {
        match s.to_lowercase().as_str() {
            "coitrees" => Ok(Algorithm::Coitrees),
            "intervaltree" => Ok(Algorithm::IntervalTree),
            "arrayintervaltree" => Ok(Algorithm::ArrayIntervalTree),
            "ailist" => Ok(Algorithm::AIList),
            "lapper" => Ok(Algorithm::Lapper),
            "coitreesnearest" => Ok(Algorithm::CoitreesNearest),
            _ => Err(ParseAlgorithmError(format!(
                "Can't parse '{}' as Algorithm",
                s
            ))),
        }
    }
}

impl std::fmt::Display for Algorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = match self {
            Algorithm::Coitrees => "Coitrees",
            Algorithm::IntervalTree => "IntervalTree",
            Algorithm::ArrayIntervalTree => "ArrayIntervalTree",
            Algorithm::AIList => "AIList",
            Algorithm::Lapper => "Lapper",
            Algorithm::CoitreesNearest => "CoitreesNearest",
        };
        write!(f, "{}", val)
    }
}
