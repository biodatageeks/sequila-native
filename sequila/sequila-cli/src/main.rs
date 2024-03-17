mod qacoitrees;
mod qbio;

// use qbio::data_structures::interval_tree::ArrayBackedIntervalTree;
use bio::data_structures::interval_tree::IntervalTree as BioIntervalTree;
use bio::utils::Interval;
// use qacoitrees::{COITree, IntervalTree};
use coitrees::IntervalTree;
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
use qbio::array_backed_interval_tree::ArrayBackedIntervalTree;
use sequila_core::session_context::SeQuiLaSessionExt;
use std::error::Error;
use std::fs::File;
use std::io::BufRead;
use std::pin::Pin;
use std::str;
use std::time::Instant;

use crate::qacoitrees::QABasicCOITree;
use cxx::CxxVector;

#[cxx::bridge]
mod iitii {

    struct RustNode {
        pos_start: i32,
        pos_end: i32,
    }

    unsafe extern "C++" {
        include!("sequila-cli/include/IITree.h");
        type IntIITree;

        fn new_int_iitree() -> UniquePtr<IntIITree>;
        fn add(self: Pin<&mut IntIITree>, start: &i32, end: &i32, d: &i32);

        fn index(self: Pin<&mut IntIITree>);
        fn size(&self) -> usize;
        fn overlap(&self, start: &i32, end: &i32, out: Pin<&mut CxxVector<usize>>) -> bool;

    }

    unsafe extern "C++" {
        include!("sequila-cli/include/IITreeBFS.h");
        type IntIITreeBFS;

        fn new_int_iitree_bfs() -> UniquePtr<IntIITreeBFS>;
        fn add(self: Pin<&mut IntIITreeBFS>, start: &i32, end: &i32, d: &i32);

        fn index(self: Pin<&mut IntIITreeBFS>);
        fn size(&self) -> usize;
        fn overlap(&self, start: &i32, end: &i32, out: Pin<&mut CxxVector<usize>>) -> bool;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let mut overlaps = 0;

    let mut iit_tree_rustbio = ArrayBackedIntervalTree::new();
    let mut avl_tree = BioIntervalTree::new();
    let mut coitrees = FnvHashMap::<String, QABasicCOITree<i32, u32>>::default();
    let mut node_arr: Vec<coitrees::Interval<i32>> = Vec::new();
    let mut int_iitree = iitii::new_int_iitree();
    let mut int_iitree_bfs = iitii::new_int_iitree_bfs();
    let mut file = File::open("/Users/mwiewior/research/data/AIListTestData/chainRn4_chr1.csv")?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut lines = 0;
    let mut records: Vec<&str> = Vec::with_capacity(3);
    let start = Instant::now();
    for line in mmap.split(|c| *c as char == '\n') {
        if (line.len() > 0 && line[0] != b'#') {
            records = str::from_utf8(&line).unwrap().split('\t').collect();
            let mut post_start = records[1].trim().parse::<i32>();
            let mut post_end = records[2].trim().parse::<i32>();
            if (!post_start.is_err() && !post_end.is_err()) {
                avl_tree.insert(post_start.clone().unwrap()..post_end.clone().unwrap(), 0);
                iit_tree_rustbio.insert(post_start.clone().unwrap()..post_end.clone().unwrap(), 0);
                int_iitree.as_mut().unwrap().add(
                    &post_start.clone().unwrap(),
                    &post_end.clone().unwrap(),
                    &0,
                );
                int_iitree_bfs.as_mut().unwrap().add(
                    &post_start.clone().unwrap(),
                    &post_end.clone().unwrap(),
                    &0,
                );
                node_arr.push(coitrees::Interval::new(
                    post_start.clone().unwrap(),
                    post_end.clone().unwrap(),
                    0,
                ));
                lines += 1;
            }
        }
    }
    // tree.index();
    //indexing
    coitrees.insert("chr1".to_string(), QABasicCOITree::new(&node_arr));
    iit_tree_rustbio.index();
    int_iitree.as_mut().unwrap().index();
    int_iitree_bfs.as_mut().unwrap().index();
    let duration = start.elapsed();
    info!("Total indexes build time: {:?}", duration);
    info!("---------------------------------");
    info!("Index lines: {lines}");
    // info!("Tree size {}", int_iitree.size());
    let mut file = File::open("/Users/mwiewior/research/data/AIListTestData/learned-index-sandbox/chainXenTro3Link_chr1.csv")?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut query_intervals = Vec::new();
    let mut lines = 0;
    for line in mmap.split(|c| *c as char == '\n') {
        if (line.len() > 0 && line[0] != b'#') {
            records = str::from_utf8(&line).unwrap().split('\t').collect();
            let post_start = records[1].trim().parse::<i32>();
            let post_end = records[2].trim().parse::<i32>();
            if (!post_start.is_err() && !post_end.is_err()) {
                lines += 1;
                query_intervals.push((post_start.unwrap(), post_end.unwrap()));
            }
        }
    }
    info!("Query lines  {lines}");
    info!("---------------------------------");

    // overlaps = 0;
    // let query_intervals_avl = query_intervals.clone();
    // let start = Instant::now();
    // for q in query_intervals_avl {
    //     for r in avl_tree.find(q.0..q.1) {
    //         overlaps += 1;
    //     }
    // }
    // info!("Overlaps [avl-rustbio]: {overlaps}");
    // let duration = start.elapsed();
    // info!("Query time [[avl-rustbio]]: {:?}", duration);
    // info!("---------------------------------");

    fn bench_coitrees(
        query_intervals: &Vec<(i32, i32)>,
        coitrees: &FnvHashMap<String, QABasicCOITree<i32, u32>>,
    ) {
        let mut overlaps: i64 = 0;
        let query_intervals_coit = query_intervals.clone();
        let start = Instant::now();
        for q in query_intervals_coit {
            coitrees.get("chr1").unwrap().query(q.0, q.1, |interval| {
                let a = interval;
                overlaps += 1;
            });
        }
        info!("Overlaps [Coitrees]: {overlaps}");
        let duration = start.elapsed();
        info!("Query time [Coitrees]: {:?}", duration);
        info!("---------------------------------");
    }

    // overlaps = 0;
    // let query_intervals_iit = query_intervals.clone();
    // let start = Instant::now();
    // for q in query_intervals_iit {
    //     let mut vec = CxxVector::new();
    //     let found = int_iitree.overlap(&q.0, &q.1, vec.as_mut().unwrap());
    //     if vec.len() > 0 {
    //         overlaps += vec.len();
    //     }
    // }
    // info!("Overlaps [IIT-C++]: {overlaps}");
    // let duration = start.elapsed();
    // info!("Query time [IIT-C++]: {:?}", duration);
    // info!("---------------------------------");
    //
    // overlaps = 0;
    // let query_intervals_iit_bfs = query_intervals.clone();
    // let start = Instant::now();
    // for q in query_intervals_iit_bfs {
    //     let mut vec = CxxVector::new();
    //     let found = int_iitree_bfs.overlap(&q.0, &q.1, vec.as_mut().unwrap());
    //     if vec.len() > 0 {
    //         overlaps += vec.len();
    //     }
    // }
    // info!("Overlaps [IIT-BFS-C++]: {overlaps}");
    // let duration = start.elapsed();
    // info!("Query time [IIT-BFS-C++]: {:?}", duration);
    // info!("---------------------------------");
    //
    //
    fn bench_iit(
        query_intervals: &Vec<(i32, i32)>,
        iit_tree_rustbio: &ArrayBackedIntervalTree<i32>,
    ) {
        let mut overlaps: i64 = 0;
        let query_intervals_iit_rustbio = query_intervals.clone();
        let start = Instant::now();
        for q in query_intervals_iit_rustbio {
            // for r in iit_tree_rustbio.find(q.0..q.1) {
            //     overlaps += 1;
            // }

            iit_tree_rustbio.query(q.0, q.1, |interval| {
                let a = interval;
                overlaps += 1;
            });
        }
        info!("Overlaps [IIT-rustbio]: {overlaps}");
        let duration = start.elapsed();
        info!("Query time [IIT-rustbio]: {:?}", duration);
        info!("---------------------------------");
    }

    fn bench_iit_cgranges(query_intervals: &Vec<(i32, i32)>, int_iitree: &iitii::IntIITree) {
        let mut overlaps: i64 = 0;
        let query_intervals = query_intervals.clone();
        let start = Instant::now();
        for q in query_intervals {
            let mut vec = CxxVector::new();
            let found = int_iitree.overlap(&q.0, &q.1, vec.as_mut().unwrap());
            if vec.len() > 0 {
                overlaps += vec.len() as i64;
            }
        }
        info!("Overlaps [IIT-cgranges]: {overlaps}");
        let duration = start.elapsed();
        info!("Query time [IIT-cgranges]: {:?}", duration);
        info!("---------------------------------");
    }

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

    bench_coitrees(&query_intervals, &coitrees);
    bench_iit(&query_intervals, &iit_tree_rustbio);
    bench_iit_cgranges(&query_intervals, &int_iitree);
    Ok(())
}
