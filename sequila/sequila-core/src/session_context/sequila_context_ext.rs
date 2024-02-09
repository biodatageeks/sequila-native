use async_trait::async_trait;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use env_logger::Env;
use log::{info, trace, warn};
use std::sync::Arc;

/// Extension trait for [`SessionContext`] that adds Exon-specific functionality.
#[async_trait]
pub trait SeQuiLaSessionExt {
    fn new_with_sequila(config: SessionConfig) -> SessionContext;
    fn with_config_rt_sequila(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> SessionContext;
}

impl SeQuiLaSessionExt for SessionContext {
    fn new_with_sequila(config: SessionConfig) -> SessionContext {
        info!("Loading SeQuiLaSessionExt...");
        let runtime = Arc::new(RuntimeEnv::default());
        Self::with_config_rt_sequila(config, runtime)
    }
    fn with_config_rt_sequila(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> SessionContext {
        // let mut state = SessionState::new_with_config_rt(config, runtime);
        let ctx = SessionContext::new();
        ctx
    }
}
