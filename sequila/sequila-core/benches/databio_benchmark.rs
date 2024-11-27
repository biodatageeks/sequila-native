use coitrees::{COITree, Interval, IntervalTree};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{ArrowNativeType, Int32Type};
use datafusion::config::{ConfigField, ConfigOptions};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use fnv::FnvHashMap;
use sequila_core::session_context::{Algorithm, SeQuiLaSessionExt, SequilaConfig};
use std::env;
use strum_macros::Display;

type IntervalHashMap = FnvHashMap<String, Vec<Interval<()>>>;

#[derive(PartialEq, Eq, Display, Clone)]
enum TableSize {
    Small,  // <2k
    Medium, // 2-10k
    Large,  // >10k
}
#[derive(Clone)]
struct Table {
    id: u8,
    name: String,
    non_flat: u8,
    table_size: TableSize,
}

struct TestCase {
    s1: Table,
    s2: Table,
}

fn get_test_name(algorithm: Algorithm, t1: Table, t2: Table, prefix: Option<String>) -> String {
    let prefix = prefix
        .map(|p| p.to_string() + "-")
        .unwrap_or("".to_string());

    let test_name = format!(
        "{}{}-{}-{}-{}-{}-{}-{}",
        prefix, algorithm, t1.table_size, t2.table_size, t1.non_flat, t2.non_flat, t1.id, t2.id
    );
    test_name
}

async fn prepare_coitrees(
    s1_path: String,
    s2_path: String,
) -> (
    FnvHashMap<String, COITree<(), u32>>,
    Vec<(String, i32, i32)>,
) {
    let mut _row_count = 0;
    let mut nodes = IntervalHashMap::default();
    let ctx = SessionContext::new();
    let s1 = ctx
        .read_parquet(s1_path.clone(), ParquetReadOptions::default())
        .await
        .expect("")
        .collect()
        .await
        .expect("");
    for batch in s1 {
        let contig = batch.column(0).as_string_view();
        let pos_start = batch.column(1).as_primitive::<Int32Type>();
        let pos_end = batch.column(2).as_primitive::<Int32Type>();
        for row_index in 0..batch.num_rows() {
            let contig = contig.value(row_index).to_string();
            let pos_start = pos_start.value(row_index).to_isize().unwrap();
            let pos_end = pos_end.value(row_index).to_isize().unwrap();
            let node_arr = if let Some(node_arr) = nodes.get_mut(&contig) {
                node_arr
            } else {
                nodes.entry(contig).or_insert(Vec::new())
            };
            node_arr.push(Interval::new(pos_start as i32, pos_end as i32, ()));
            _row_count += 1;
        }
    }
    let mut trees = FnvHashMap::<String, COITree<(), u32>>::default();
    for (seqname, seqname_nodes) in nodes {
        trees.insert(seqname, COITree::new(&seqname_nodes));
    }
    let mut query_intervals: Vec<(String, i32, i32)> = Vec::new();
    let s2 = ctx
        .read_parquet(s2_path.clone(), ParquetReadOptions::default())
        .await
        .expect("")
        .collect()
        .await
        .expect("");
    for batch in s2 {
        let contig = batch.column(0).as_string_view();
        let pos_start = batch.column(1).as_primitive::<Int32Type>();
        let pos_end = batch.column(2).as_primitive::<Int32Type>();
        for row_index in 0..batch.num_rows() {
            let contig = contig.value(row_index).to_string();
            let pos_start = pos_start.value(row_index).to_isize().unwrap();
            let pos_end = pos_end.value(row_index).to_isize().unwrap();
            query_intervals.push((contig, pos_start as i32, pos_end as i32));
            _row_count += 1;
        }
    }
    (trees, query_intervals)
}
fn bench_coitrees(
    query_intervals: &Vec<(String, i32, i32)>,
    coitrees: &FnvHashMap<String, COITree<(), u32>>,
) {
    let mut overlaps: i64 = 0;
    let query_intervals_coit = query_intervals.clone();
    for q in query_intervals_coit {
        match coitrees.get(&q.0) {
            Some(tree) => tree.query(q.1, q.2, |interval| {
                let _a = interval;
                overlaps += 1;
            }),
            _ => {} // Do nothing for None
        };
    }
}

fn create_context(algorithm: Algorithm) -> SessionContext {
    let mut options = ConfigOptions::new();
    let tuning_options = vec![
        ("datafusion.execution.target_partitions", "1"),
        ("datafusion.optimizer.repartition_joins", "false"),
        ("datafusion.execution.coalesce_batches", "false"),
    ];

    for o in tuning_options {
        options.set(o.0, o.1).expect("TODO: panic message");
    }

    let mut sequila_config = SequilaConfig::default();
    sequila_config.prefer_interval_join = true;
    sequila_config.interval_join_algorithm = algorithm;

    let config = SessionConfig::from(options)
        .with_option_extension(sequila_config)
        .with_information_schema(true)
        .with_batch_size(8192)
        .with_target_partitions(1);

    SessionContext::new_with_sequila(config)
}

pub fn databio_benchmark(c: &mut Criterion) {
    let root_path = env::var("BENCH_DATA_ROOT").unwrap_or("/data/bench_data/databio/".to_string());
    let tables = [
        Table {
            id: 0,
            name: String::from("chainRn4"),
            non_flat: 6,
            table_size: TableSize::Medium,
        },
        Table {
            id: 1,
            name: String::from("fBrain-DS14718"),
            non_flat: 1,
            table_size: TableSize::Small,
        },
        Table {
            id: 2,
            name: String::from("exons"),
            non_flat: 2,
            table_size: TableSize::Small,
        },
        Table {
            id: 3,
            name: String::from("chainOrnAna1"),
            non_flat: 6,
            table_size: TableSize::Medium,
        },
        Table {
            id: 4,
            name: String::from("chainVicPac2"),
            non_flat: 8,
            table_size: TableSize::Medium,
        },
        Table {
            id: 5,
            name: String::from("chainXenTro3Link"),
            non_flat: 7,
            table_size: TableSize::Large,
        },
        Table {
            id: 6,
            name: String::from("chainMonDom5Link"),
            non_flat: 7,
            table_size: TableSize::Large,
        },
        Table {
            id: 7,
            name: String::from("ex-anno"),
            non_flat: 2,
            table_size: TableSize::Small,
        },
        Table {
            id: 8,
            name: String::from("ex-rna"),
            non_flat: 7,
            table_size: TableSize::Medium,
        },
    ];

    const QUERY: &str = r#"
            SELECT
                count(*)
            FROM
                s1 a, s2 b
            WHERE
                a.contig=b.contig
            AND
                a.pos_end>=b.pos_start
            AND
                a.pos_start<=b.pos_end
        "#;

    // Actutal test cases
    let test_cases = vec![
        TestCase {
            s1: tables[0].clone(),
            s2: tables[1].clone(),
        },
        TestCase {
            s1: tables[0].clone(),
            s2: tables[3].clone(),
        },
        TestCase {
            s1: tables[0].clone(),
            s2: tables[7].clone(),
        },
        TestCase {
            s1: tables[0].clone(),
            s2: tables[8].clone(),
        },
        TestCase {
            s1: tables[7].clone(),
            s2: tables[8].clone(),
        },
    ];
    let algorithms = [
        Algorithm::Coitrees,
        Algorithm::IntervalTree,
        Algorithm::ArrayIntervalTree,
        Algorithm::AIList,
        Algorithm::Lapper,
    ];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("Algorithms");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(7200));
    group.significance_level(0.1);

    for test in test_cases {
        let s1_path = format!("{}{}{}", &root_path, &test.s1.name, "/*.parquet");
        let s2_path = format!("{}{}{}", &root_path, &test.s2.name, "/*.parquet");
        let test_baseline = get_test_name(
            Algorithm::Coitrees,
            test.s1.clone(),
            test.s2.clone(),
            Some(String::from("baseline")),
        );
        print!("test {test_baseline} ... ");
        group.bench_function(test_baseline, |b| {
            b.to_async(&runtime).iter(|| async {
                let (trees, query) = prepare_coitrees(s1_path.clone(), s2_path.clone()).await;
                bench_coitrees(&query, &trees);
            });
        });
        for alg in algorithms {
            let test_name = get_test_name(alg, test.s1.clone(), test.s2.clone(), None);
            print!("test {test_name} ... ");
            group.bench_function(test_name, |b| {
                b.to_async(&runtime).iter(|| async {
                    let ctx = create_context(alg);
                    ctx.register_parquet("s1", s1_path.clone(), ParquetReadOptions::new())
                        .await
                        .expect("");
                    ctx.register_parquet("s2", s2_path.clone(), ParquetReadOptions::new())
                        .await
                        .expect("");
                    let df = ctx.sql(QUERY).await.expect("");
                    let _a = df.collect().await.expect("");
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, databio_benchmark);
criterion_main!(benches);
