use crate::physical_planner::RangeJoinPhysicalOptimizationRule;
use crate::physical_planner::SeQuiLaQueryPlanner;
use async_trait::async_trait;
use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use log::info;
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
        let state = SessionState::new_with_config_rt(config, runtime);
        let ctx = SessionContext::new_with_state(
            state
                .with_query_planner(Arc::new(SeQuiLaQueryPlanner::default()))
                .add_physical_optimizer_rule(
                    Arc::new(RangeJoinPhysicalOptimizationRule::default()),
                ),
        );
        let hammer = emojis::get_by_shortcode("hammer_and_wrench").unwrap();
        info!("Initialized SeQuiLaQueryPlanner {hammer}...");
        ctx
    }
}

extensions_options! {
    pub struct SequilaConfig {
        pub prefer_interval_join: bool, default = false
    }
}

impl ConfigExtension for SequilaConfig {
    const PREFIX: &'static str = "sequila";
}
