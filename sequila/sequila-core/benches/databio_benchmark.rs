use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::config::ConfigOptions;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use sequila_core::session_context::{Algorithm, SeQuiLaSessionExt, SequilaConfig};

pub struct Table {
    name: String,
    non_flat: u8,
}

impl Table {
    fn new(name: String, non_flat: u8) -> Self {
        Self { name, non_flat }
    }
}
fn create_context(algorithm: Option<Algorithm>) -> SessionContext {
    let options = ConfigOptions::new();

    let mut sequila_config = SequilaConfig::default();
    sequila_config.prefer_interval_join = true;
    sequila_config.interval_join_algorithm = algorithm.unwrap_or_default();

    let config = SessionConfig::from(options)
        .with_option_extension(sequila_config)
        .with_information_schema(true)
        .with_batch_size(2000)
        .with_target_partitions(1);

    SessionContext::new_with_sequila(config)
}

pub fn databio_benchmark(c: &mut Criterion) {
    let root_path = String::from("/data/bench_data/databio/");
    // let root_path = String::from("/Users/mwiewior/research/git/openstack-bdg-runners/ansible/roles/gha_runner/files/databio/");
    let tables = [
        // Table { name: String::from("chainRn4"), non_flat: 6 },
        Table {
            name: String::from("fBrain-DS14718"),
            non_flat: 1,
        },
        Table {
            name: String::from("exons"),
            non_flat: 2,
        },
        Table {
            name: String::from("chainOrnAna1"),
            non_flat: 6,
        },
        Table {
            name: String::from("chainVicPac2"),
            non_flat: 8,
        },
        Table {
            name: String::from("chainXenTro3Link"),
            non_flat: 7,
        },
        Table {
            name: String::from("chainMonDom5Link"),
            non_flat: 7,
        },
        Table {
            name: String::from("ex-anno"),
            non_flat: 2,
        },
        Table {
            name: String::from("ex-rna"),
            non_flat: 7,
        },
    ];

    const QUERY: &str = r#"
            SELECT
                count(*)
            FROM
                s1 a, s2 b
            WHERE
                (a.contig=b.contig AND a.pos_end>=b.pos_start AND a.pos_start<=b.pos_end)
        "#;

    let algorithms = [
        Some(Algorithm::Coitrees),
        Some(Algorithm::IntervalTree),
        Some(Algorithm::ArrayIntervalTree),
        Some(Algorithm::AIList),
    ];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("Algorithms");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(900));
    group.significance_level(0.1);

    for t_1 in &tables {
        for t_2 in &tables {
            if (t_1.name != t_2.name) {
                let s1_path = root_path.clone() + &t_1.name + &String::from("/*.parquet");
                let s2_path = root_path.clone() + &t_2.name + &String::from("/*.parquet");
                for alg in algorithms {
                    let test_name = alg.unwrap().to_string()
                        + &String::from("-")
                        + &t_1.name.clone()
                        + &String::from("-")
                        + &t_2.name.clone()
                        + &String::from("-")
                        + &t_1.non_flat.to_string().clone()
                        + &String::from("-")
                        + &t_2.non_flat.to_string().clone();
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
                            let a = df.collect().await.expect("");
                        });
                    });
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, databio_benchmark);
criterion_main!(benches);
