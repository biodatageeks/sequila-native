use bio::data_structures::interval_tree::ArrayBackedIntervalTree;
use bio::data_structures::interval_tree::IntervalTree as BioIntervalTree;
use bio::utils::Interval;
use coitrees::{COITree, IntervalTree};
use datafusion::common::DataFusionError;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use datafusion_cli::exec;
use datafusion_cli::print_options::PrintOptions;
use fnv::FnvHashMap;
use log::info;
use memmap2::Mmap;
use sequila_core::session_context::SeQuiLaSessionExt;
use std::error::Error;
use std::fs::File;
use std::io::BufRead;
use std::str;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let mut overlaps = 0;
    // let mut tree = ArrayBackedIntervalTree::new();
    let mut tree = BioIntervalTree::new();
    let mut coitrees = FnvHashMap::<String, COITree<i32, u32>>::default();
    let mut node_arr: Vec<coitrees::Interval<i32>> = Vec::new();
    let mut file = File::open("/Users/mwiewior/research/data/AIListTestData/chainRn4_chr1.csv")?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut lines = 0;
    let mut records: Vec<&str> = Vec::with_capacity(3);
    for line in mmap.split(|c| *c as char == '\n') {
        if (line.len() > 0 && line[0] != b'#') {
            records = str::from_utf8(&line).unwrap().split('\t').collect();
            let post_start = records[1].trim().parse::<i32>();
            let post_end = records[2].trim().parse::<i32>();
            if (!post_start.is_err() && !post_end.is_err()) {
                tree.insert(post_start.unwrap()..post_end.unwrap(), 0);
                // node_arr.push(coitrees::Interval::new(post_start.unwrap(), post_end.unwrap(), 0));
                lines += 1;
            }
        }
    }
    // tree.index();
    // coitrees.insert("chr1".to_string(), COITree::new(&node_arr));
    info!("Lines: {lines}");
    // a csv parser from a file
    // let reader =  csv::ReaderBuilder::new()
    //     .delimiter(b'\t')
    //     .from_path("/Users/mwiewior/research/data/AIListTestData/chainRn4_chr1.csv");
    // for record in reader?.records() {
    //     let string_record = record?;
    //
    // }
    // let mut lines = 0;
    // let reader =  csv::ReaderBuilder::new()
    //     .delimiter(b'\t')
    //     .from_path("/Users/mwiewior/research/data/AIListTestData/chainXenTro3Link_chr1.csv");
    // for record in reader?.records() {
    //     let string_record = record?;
    //     lines += 1;
    //
    // }
    let mut file =
        File::open("/Users/mwiewior/research/data/AIListTestData/chainVicPac2_chr1.csv")?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut lines = 0;
    for line in mmap.split(|c| *c as char == '\n') {
        if (line.len() > 0 && line[0] != b'#') {
            records = str::from_utf8(&line).unwrap().split('\t').collect();
            let post_start = records[1].trim().parse::<i32>();
            let post_end = records[2].trim().parse::<i32>();
            if (!post_start.is_err() && !post_end.is_err()) {
                lines += 1;
                // if let Some(seqname_tree) = coitrees.get("chr1") {
                //     seqname_tree.query(post_start.unwrap(), post_end.unwrap(), |interval| {
                //         let a = interval;
                //         overlaps += 1;
                //     });
                // }
                // let intervals = &tree.find(post_start.unwrap()..post_end.unwrap());
                // if intervals.len() >0 {overlaps+=1}
                for r in tree.find(post_start.unwrap()..post_end.unwrap()) {
                    overlaps += 1;
                }
            }
        }
    }
    info!("Lines: {lines}");
    info!("Overlaps: {overlaps}");

    // let options = ConfigOptions::new();
    // let config = SessionConfig::from(options);
    // let rocket = emojis::get_by_shortcode("rocket").unwrap();
    // info!("Starting SeQuiLa-native CLI {rocket}...");
    // let mut ctx = SessionContext::new_with_sequila(config);
    // let mut print_options = PrintOptions {
    //     format: datafusion_cli::print_format::PrintFormat::Table,
    //     quiet: false,
    //     maxrows: datafusion_cli::print_options::MaxRows::Limited(100),
    // };
    // exec::exec_from_repl(&mut ctx, &mut print_options)
    //     .await
    //     .map_err(|e| DataFusionError::External(Box::new(e)))
    //     .expect("Error");
    Ok(())
}

// write an unit test that creates two tables reads and targets, insert sample rows  adds assertion macro checking  interval join between tables with equlaity condition on text column contig and ranges on pos_start and pos_end
#[tokio::test]
async fn test_interval_rule_eq() -> Result<()> {
    let options = ConfigOptions::new();
    let config = SessionConfig::from(options);
    let mut ctx = SessionContext::new_with_sequila(config);
    let sql = "CREATE TABLE target (contig TEXT, pos_start INT, pos_end INT)";
    ctx.sql(sql).await.unwrap();
    let sql = "CREATE TABLE read (contig TEXT, pos_start INT, pos_end INT)";
    ctx.sql(sql).await.unwrap();

    // Insert sample rows
    let sql = "INSERT INTO target (contig, pos_start, pos_end) VALUES ('chr1', 100, 200)";
    ctx.sql(sql).await.unwrap();
    let sql = "INSERT INTO read (contig, pos_start, pos_end) VALUES ('chr1', 150, 250)";
    ctx.sql(sql).await.unwrap();

    // Assertion macro
    let sql = "SELECT COUNT(*) FROM target a JOIN read b ON a.contig = b.contig \
                AND a.pos_end >= b.pos_start AND a.pos_start <= b.pos_end";
    let df = ctx.sql(sql).await?;
    assert_eq!(df.get_column_data(0).to_string(), "1");

    Ok(())
}

//add test for interval joins
#[tokio::test]
async fn test_interval_rule() -> Result<()> {
    let options = ConfigOptions::new();
    let config = SessionConfig::from(options);
    let mut ctx = SessionContext::new_with_sequila(config);
    let sql = "CREATE TABLE target (contig TEXT, pos_start INT, pos_end INT)";
    ctx.sql(sql).await.unwrap();
    let sql = "CREATE TABLE read (contig TEXT, pos_start INT, pos_end INT)";
    ctx.sql(sql).await.unwrap();
    let sql = "INSERT INTO target (contig, pos_start, pos_end) VALUES ('chr1', 100, 200)";
    ctx.sql(sql).await.unwrap();
    let sql = "INSERT INTO read (contig, pos_start, pos_end) VALUES ('chr1', 150, 250)";
    ctx.sql(sql).await.unwrap();
    let sql = "SELECT COUNT(*) FROM target a JOIN read b ON a.contig = b.contig \
                AND a.pos_end >= b.pos_start AND a.pos_start <= b.pos_end";
    let df = ctx.sql(sql).await?;
    assert_eq(df.get_column_data(0).to_string(), "1");
    Ok(())
}

// env_logger::init();
// use datafusion::prelude::*;

// #[tokio::test]
// async fn test_interval_rule_eq() -> Result<()> {
//     let options = ConfigOptions::new();
//     let config = SessionConfig::from(options);
//     let mut ctx = SessionContext::new_with_sequila(config);
//     let sql = "CREATE TABLE target (contig TEXT, pos_start INT, pos_end INT)";
//     ctx.sql(sql).await.unwrap();
//     let sql = "CREATE TABLE read (contig TEXT, pos_start INT, pos_end INT)";
//     ctx.sql(sql).await.unwrap();
//     let sql = "SELECT COUNT(*) FROM target a JOIN read b ON a.contig = b.contig \
//                AND a.pos_end >= b.pos_start AND a.pos_start <= b.pos_end";
//     let df = ctx.sql(sql).await?;
//     df.show().await?;
//     Ok(())
// }
