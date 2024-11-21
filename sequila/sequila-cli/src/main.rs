use clap::Parser;
use datafusion::common::DataFusionError;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_cli::exec;
use datafusion_cli::print_options::PrintOptions;
use log::info;
use sequila_core::session_context::{SeQuiLaSessionExt, SequilaConfig};
use std::path::Path;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(
        short,
        long,
        num_args = 0..,
        help = "Execute commands from file(s), then exit",
        value_parser(parse_valid_file)
    )]
    file: Vec<String>,
}

fn parse_valid_file(dir: &str) -> Result<String, String> {
    if Path::new(dir).is_file() {
        Ok(dir.to_string())
    } else {
        Err(format!("Invalid file '{}'", dir))
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let mut options = ConfigOptions::new();
    options.extensions.insert(SequilaConfig::default());
    let config = SessionConfig::from(options)
        .with_information_schema(true)
        .with_repartition_joins(false);
    let rocket = emojis::get_by_shortcode("rocket").unwrap();
    info!("Starting SeQuiLa-native CLI {rocket}...");
    let ctx = SessionContext::new_with_sequila(config);
    let mut print_options = PrintOptions {
        format: datafusion_cli::print_format::PrintFormat::Table,
        quiet: false,
        maxrows: datafusion_cli::print_options::MaxRows::Limited(100),
        color: true,
    };

    if !args.file.is_empty() {
        exec::exec_from_files(&ctx, args.file, &print_options).await?;
    } else {
        exec::exec_from_repl(&ctx, &mut print_options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
            .expect("Error");
    }

    Ok(())
}

#[tokio::test]
async fn test_interval_rule_eq() -> datafusion::error::Result<()> {
    env_logger::init();
    let options = ConfigOptions::new();
    let config = SessionConfig::from(options);
    let ctx = SessionContext::new_with_sequila(config);
    let sql = "CREATE TABLE target (contig TEXT, pos_start INT, pos_end INT)";
    ctx.sql(sql).await?;
    let sql = "CREATE TABLE read (contig TEXT, pos_start INT, pos_end INT)";
    ctx.sql(sql).await?;

    // from HashJoinExec
    let df = ctx
        .sql(
            "SELECT COUNT(*) FROM target a JOIN read b ON a.contig = b.contig AND a.pos_end >= b.pos_start AND a.pos_start <= b.pos_end",
        )
        .await?;

    let plan = df.explain(false, false)?.collect().await?;
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&plan)?.to_string();
    assert!(formatted.contains("IntervalJoinExec: "));

    // from NestedLoopJoinExec
    let df = ctx
        .sql(
            "SELECT COUNT(*) FROM target a JOIN read b ON a.pos_end >= b.pos_start AND a.pos_start <= b.pos_end",
        )
        .await?;

    let plan = df.explain(false, false)?.collect().await?;
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&plan)?.to_string();
    assert!(formatted.contains("IntervalJoinExec: "));

    // cannot be transformed
    let df = ctx
        .sql(
            "SELECT COUNT(*) FROM target a JOIN read b ON a.pos_end > b.pos_start AND a.pos_start <= b.pos_end",
        )
        .await?;

    let plan = df.explain(false, false)?.collect().await?;
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&plan)?.to_string();
    assert!(formatted.contains("IntervalJoinExec: "));

    Ok(())
}
