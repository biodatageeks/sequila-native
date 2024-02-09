use datafusion::common::DataFusionError;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use datafusion_cli::exec;
use datafusion_cli::print_options::PrintOptions;
use log::info;
use sequila_core::session_context::SeQuiLaSessionExt;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let options = ConfigOptions::new();
    let config = SessionConfig::from(options);
    let rocket = emojis::get_by_shortcode("rocket").unwrap();
    info!("Starting SeQuiLa-native CLI {rocket}...");
    let mut ctx = SessionContext::new_with_sequila(config);
    let mut print_options = PrintOptions {
        format: datafusion_cli::print_format::PrintFormat::Table,
        quiet: false,
        maxrows: datafusion_cli::print_options::MaxRows::Limited(100),
    };
    exec::exec_from_repl(&mut ctx, &mut print_options)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
        .expect("Error");
    Ok(())
}
