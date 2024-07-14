use datafusion::common::DataFusionError;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use datafusion_cli::exec;
use datafusion_cli::print_options::PrintOptions;
use log::info;
use sequila_core::session_context::{SeQuiLaSessionExt, SequilaConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let mut options = ConfigOptions::new();
    options.extensions.insert(SequilaConfig::default());
    let config = SessionConfig::from(options).with_information_schema(true);
    let rocket = emojis::get_by_shortcode("rocket").unwrap();
    info!("Starting SeQuiLa-native CLI {rocket}...");
    let mut ctx = SessionContext::new_with_sequila(config);
    let mut print_options = PrintOptions {
        format: datafusion_cli::print_format::PrintFormat::Table,
        quiet: false,
        maxrows: datafusion_cli::print_options::MaxRows::Limited(100),
        color: true,
    };
    exec::exec_from_repl(&mut ctx, &mut print_options)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
        .expect("Error");
    Ok(())
}

#[tokio::test]
async fn test_interval_rule_eq() -> datafusion::error::Result<()> {
    env_logger::init();
    let options = ConfigOptions::new();
    let config = SessionConfig::from(options);
    let mut ctx = SessionContext::new_with_sequila(config);
    let sql = "CREATE TABLE target (contig TEXT, pos_start INT, pos_end INT)";
    ctx.sql(sql).await.unwrap();
    let sql = "CREATE TABLE read (contig TEXT, pos_start INT, pos_end INT)";
    ctx.sql(sql).await.unwrap();
    let df = ctx
        .sql(
            "SELECT COUNT(*) FROM target a JOIN read b ON a.contig = b.contig \
           AND a.pos_end >= b.pos_start AND a.pos_start <= b.pos_end",
        )
        .await?;
    df.show().await?;
    Ok(())
}
