use crate::physical_planner::IntervalJoinPhysicalOptimizationRule;
use crate::physical_planner::SeQuiLaQueryPlanner;
use async_trait::async_trait;
use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
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
        let state = SessionState::new_with_config_rt(config, runtime);
        let ctx = SessionContext::new_with_state(
            state
                .with_query_planner(Arc::new(SeQuiLaQueryPlanner::default()))
                .add_physical_optimizer_rule(Arc::new(
                    IntervalJoinPhysicalOptimizationRule::default(),
                )),
        );
        let hammer = emojis::get_by_shortcode("hammer_and_wrench").unwrap();
        info!("Initialized SeQuiLaQueryPlanner {hammer}...");
        ctx
    }
}

extensions_options! {
    pub struct SequilaConfig {
        pub prefer_interval_join: bool, default = false
        pub interval_join_algorithm: Algorithm, default = Algorithm::default()
    }
}

impl ConfigExtension for SequilaConfig {
    const PREFIX: &'static str = "sequila";
}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub enum Algorithm {
    #[default]
    Coitrees,
    IntervalTree,
    ArrayIntervalTree,
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
        };
        write!(f, "{}", val)
    }
}
