use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::assert_batches_sorted_eq;
use datafusion::common::assert_contains;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext};

use sequila_core::session_context::{Algorithm, SeQuiLaSessionExt, SequilaConfig};

const READS_PATH: &str = "../../testing/data/interval/reads.csv";
const TARGETS_PATH: &str = "../../testing/data/interval/targets.csv";

#[rstest::fixture]
fn ctx() -> SessionContext {
    let config = SessionConfig::from(ConfigOptions::new())
        .with_option_extension(SequilaConfig::default())
        .with_information_schema(true)
        .with_repartition_joins(false);

    SessionContext::new_with_sequila(config)
}

async fn init_tables(ctx: &SessionContext) -> Result<()> {
    let reads = format!(
        "CREATE EXTERNAL TABLE reads STORED AS CSV LOCATION '{}' OPTIONS ('has_header' 'true')",
        READS_PATH
    );
    ctx.sql(reads.as_str()).await?;

    let targets = format!(
        "CREATE EXTERNAL TABLE targets STORED AS CSV LOCATION '{}' OPTIONS ('has_header' 'true')",
        TARGETS_PATH
    );
    ctx.sql(targets.as_str()).await?;

    Ok(())
}

#[rstest::fixture]
#[once]
fn expected_equi() -> [&'static str; 20] {
    [
        "+--------+-----------+---------+--------+-----------+---------+",
        "| contig | pos_start | pos_end | contig | pos_start | pos_end |",
        "+--------+-----------+---------+--------+-----------+---------+",
        "| chr1   | 150       | 250     | chr1   | 100       | 190     |",
        "| chr1   | 150       | 250     | chr1   | 200       | 290     |",
        "| chr1   | 190       | 300     | chr1   | 100       | 190     |",
        "| chr1   | 190       | 300     | chr1   | 200       | 290     |",
        "| chr1   | 300       | 501     | chr1   | 400       | 600     |",
        "| chr1   | 500       | 700     | chr1   | 400       | 600     |",
        "| chr1   | 15000     | 15000   | chr1   | 10000     | 20000   |",
        "| chr1   | 22000     | 22300   | chr1   | 22100     | 22100   |",
        "| chr2   | 150       | 250     | chr2   | 100       | 190     |",
        "| chr2   | 150       | 250     | chr2   | 200       | 290     |",
        "| chr2   | 190       | 300     | chr2   | 100       | 190     |",
        "| chr2   | 190       | 300     | chr2   | 200       | 290     |",
        "| chr2   | 300       | 500     | chr2   | 400       | 600     |",
        "| chr2   | 500       | 700     | chr2   | 400       | 600     |",
        "| chr2   | 15000     | 15000   | chr2   | 10000     | 20000   |",
        "| chr2   | 22000     | 22300   | chr2   | 22100     | 22100   |",
        "+--------+-----------+---------+--------+-----------+---------+",
    ]
}

#[tokio::test(flavor = "multi_thread")]
#[rstest::rstest]
#[case::hash_join(None)]
#[case::interval_join_coitrees(Some(Algorithm::Coitrees))]
#[case::interval_join_interval_tree(Some(Algorithm::IntervalTree))]
#[case::interval_join_array_interval_tree(Some(Algorithm::ArrayIntervalTree))]
#[case::interval_join_lapper(Some(Algorithm::Lapper))]
async fn test_equi_and_range_condition(
    #[case] algorithm: Option<Algorithm>,
    ctx: SessionContext,
    expected_equi: &[&str; 20],
) -> Result<()> {
    init_tables(&ctx).await?;

    ctx.sql(format!("SET sequila.prefer_interval_join = {}", algorithm.is_some()).as_str())
        .await?;
    ctx.sql(
        format!(
            "SET sequila.interval_join_algorithm = {}",
            algorithm.unwrap_or_default()
        )
        .as_str(),
    )
    .await?;

    let query = r#"SELECT *
               FROM reads
               JOIN targets
               ON reads.contig = targets.contig
                  AND reads.pos_start <= targets.pos_end
                  AND reads.pos_end >= targets.pos_start
               ORDER BY reads.contig, reads.pos_start, reads.pos_end,
                        targets.contig, targets.pos_start, targets.pos_end"#;

    let plan: Vec<RecordBatch> = ctx
        .sql(format!("EXPLAIN {}", query).as_str())
        .await?
        .collect()
        .await?;
    let formatted: String = pretty_format_batches(&plan)?.to_string();
    let expected_plan = match algorithm {
        None => "HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(contig@0, contig@0)], filter=pos_start@0 <= pos_end@3 AND pos_end@1 >= pos_start@2".to_string(),
        Some(alg) => format!("IntervalJoinExec: mode=CollectLeft, join_type=Inner, on=[(contig@0, contig@0)], filter=pos_start@0 <= pos_end@3 AND pos_end@1 >= pos_start@2, alg={}", alg),
    };
    assert_contains!(formatted, expected_plan);

    let result: Vec<RecordBatch> = ctx.sql(query).await?.collect().await?;
    assert_batches_sorted_eq!(expected_equi, &result);

    Ok(())
}

#[rstest::fixture]
#[once]
fn expected_range() -> [&'static str; 36] {
    [
        "+--------+-----------+---------+--------+-----------+---------+",
        "| contig | pos_start | pos_end | contig | pos_start | pos_end |",
        "+--------+-----------+---------+--------+-----------+---------+",
        "| chr1   | 150       | 250     | chr1   | 100       | 190     |",
        "| chr1   | 150       | 250     | chr1   | 200       | 290     |",
        "| chr1   | 150       | 250     | chr2   | 100       | 190     |",
        "| chr1   | 150       | 250     | chr2   | 200       | 290     |",
        "| chr1   | 190       | 300     | chr1   | 100       | 190     |",
        "| chr1   | 190       | 300     | chr1   | 200       | 290     |",
        "| chr1   | 190       | 300     | chr2   | 100       | 190     |",
        "| chr1   | 190       | 300     | chr2   | 200       | 290     |",
        "| chr1   | 300       | 501     | chr1   | 400       | 600     |",
        "| chr1   | 300       | 501     | chr2   | 400       | 600     |",
        "| chr1   | 500       | 700     | chr1   | 400       | 600     |",
        "| chr1   | 500       | 700     | chr2   | 400       | 600     |",
        "| chr1   | 15000     | 15000   | chr1   | 10000     | 20000   |",
        "| chr1   | 15000     | 15000   | chr2   | 10000     | 20000   |",
        "| chr1   | 22000     | 22300   | chr1   | 22100     | 22100   |",
        "| chr1   | 22000     | 22300   | chr2   | 22100     | 22100   |",
        "| chr2   | 150       | 250     | chr1   | 100       | 190     |",
        "| chr2   | 150       | 250     | chr1   | 200       | 290     |",
        "| chr2   | 150       | 250     | chr2   | 100       | 190     |",
        "| chr2   | 150       | 250     | chr2   | 200       | 290     |",
        "| chr2   | 190       | 300     | chr1   | 100       | 190     |",
        "| chr2   | 190       | 300     | chr1   | 200       | 290     |",
        "| chr2   | 190       | 300     | chr2   | 100       | 190     |",
        "| chr2   | 190       | 300     | chr2   | 200       | 290     |",
        "| chr2   | 300       | 500     | chr1   | 400       | 600     |",
        "| chr2   | 300       | 500     | chr2   | 400       | 600     |",
        "| chr2   | 500       | 700     | chr1   | 400       | 600     |",
        "| chr2   | 500       | 700     | chr2   | 400       | 600     |",
        "| chr2   | 15000     | 15000   | chr1   | 10000     | 20000   |",
        "| chr2   | 15000     | 15000   | chr2   | 10000     | 20000   |",
        "| chr2   | 22000     | 22300   | chr1   | 22100     | 22100   |",
        "| chr2   | 22000     | 22300   | chr2   | 22100     | 22100   |",
        "+--------+-----------+---------+--------+-----------+---------+",
    ]
}

#[tokio::test(flavor = "multi_thread")]
#[rstest::rstest]
#[case::nested_loop_join(None)]
#[case::interval_join_coitrees(Some(Algorithm::Coitrees))]
#[case::interval_join_interval_tree(Some(Algorithm::IntervalTree))]
#[case::interval_join_array_interval_tree(Some(Algorithm::ArrayIntervalTree))]
#[case::interval_join_lapper(Some(Algorithm::Lapper))]
async fn test_range_condition(
    #[case] algorithm: Option<Algorithm>,
    ctx: SessionContext,
    expected_range: &[&str; 36],
) -> Result<()> {
    init_tables(&ctx).await?;

    ctx.sql(format!("SET sequila.prefer_interval_join = {}", algorithm.is_some()).as_str())
        .await?;
    ctx.sql(
        format!(
            "SET sequila.interval_join_algorithm = {}",
            algorithm.unwrap_or_default()
        )
        .as_str(),
    )
    .await?;

    let query = r#"SELECT *
               FROM reads
               JOIN targets
               ON reads.pos_start <= targets.pos_end AND reads.pos_end >= targets.pos_start
               ORDER BY reads.contig, reads.pos_start, reads.pos_end,
                        targets.contig, targets.pos_start, targets.pos_end"#;

    let plan: Vec<RecordBatch> = ctx
        .sql(format!("EXPLAIN {}", query).as_str())
        .await?
        .collect()
        .await?;
    let formatted: String = pretty_format_batches(&plan)?.to_string();
    let expected_plan = match algorithm {
        None => "NestedLoopJoinExec: join_type=Inner, filter=pos_start@0 <= pos_end@3 AND pos_end@1 >= pos_start@2".to_string(),
        Some(alg) => format!("IntervalJoinExec: mode=CollectLeft, join_type=Inner, on=[(1, 1)], filter=pos_start@0 <= pos_end@3 AND pos_end@1 >= pos_start@2, alg={}", alg),
    };
    assert_contains!(formatted, expected_plan);

    let result: Vec<RecordBatch> = ctx.sql(query).await?.collect().await?;
    assert_batches_sorted_eq!(expected_range, &result);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[rstest::rstest]
async fn test_all_gteq_lteq_conditions(ctx: SessionContext) -> Result<()> {
    let a = r#"
        CREATE TABLE a (contig TEXT, start INTEGER, end INTEGER) AS VALUES
        ('a', 5, 10)
    "#;

    let b = r#"
        CREATE TABLE b (contig TEXT, start INTEGER, end INTEGER) AS VALUES
        ('a', 11, 15),
        ('a', 10, 15),
        ('a', 10, 10),
        ('a',  9, 15),
        ('a',  5, 15),
        ('a',  4, 15),
        ('a',  4, 10),
        ('a',  6, 8),
        ('a',  4, 8),
        ('a',  4, 5),
        ('a',  5, 5),
        ('a',  4, 4)
    "#;

    let q0 = r#"
        SELECT * FROM a JOIN b
        ON a.contig = b.contig AND a.start <= b.end AND a.end >= b.start
    "#;

    let q1 = r#"
        SELECT a.*, b.* FROM b JOIN a
        ON a.contig = b.contig AND a.start <= b.end AND a.end >= b.start
    "#;

    let q2 = r#"
        SELECT a.*, b.* FROM a, b
        WHERE a.contig = b.contig AND a.start <= b.end AND a.end >= b.start
    "#;

    let q3 = r#"
        SELECT a.*, b.* FROM b, a
        WHERE a.contig = b.contig AND b.start <= a.end AND b.end >= a.start
    "#;

    ctx.sql(a).await?;
    ctx.sql(b).await?;

    let expected = vec![
        "+--------+-------+-----+--------+-------+-----+",
        "| contig | start | end | contig | start | end |",
        "+--------+-------+-----+--------+-------+-----+",
        "| a      | 5     | 10  | a      | 10    | 15  |",
        "| a      | 5     | 10  | a      | 10    | 10  |",
        "| a      | 5     | 10  | a      | 9     | 15  |",
        "| a      | 5     | 10  | a      | 5     | 15  |",
        "| a      | 5     | 10  | a      | 4     | 15  |",
        "| a      | 5     | 10  | a      | 4     | 10  |",
        "| a      | 5     | 10  | a      | 6     | 8   |",
        "| a      | 5     | 10  | a      | 4     | 8   |",
        "| a      | 5     | 10  | a      | 5     | 5   |",
        "| a      | 5     | 10  | a      | 4     | 5   |",
        "+--------+-------+-----+--------+-------+-----+",
    ];

    let results = ctx.sql(q0).await?.collect().await?;
    assert_batches_sorted_eq!(expected, &results);

    let results = ctx.sql(q1).await?.collect().await?;
    assert_batches_sorted_eq!(expected, &results);

    let results = ctx.sql(q2).await?.collect().await?;
    assert_batches_sorted_eq!(expected, &results);

    let results = ctx.sql(q3).await?.collect().await?;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[rstest::rstest]
async fn test_all_gt_lt_conditions(ctx: SessionContext) -> Result<()> {
    let a = r#"
        CREATE TABLE a (contig TEXT, start INTEGER, end INTEGER) AS VALUES
        ('a', 5, 10)
    "#;

    let b = r#"
        CREATE TABLE b (contig TEXT, start INTEGER, end INTEGER) AS VALUES
        ('a', 11, 15),
        ('a', 10, 15), -- a.end = b.start
        ('a', 10, 10), -- a.end = b.start
        ('a',  9, 15),
        ('a',  5, 15),
        ('a',  4, 15),
        ('a',  4, 10),
        ('a',  6, 8),
        ('a',  4, 8),
        ('a',  4, 5), -- a.start = b.end
        ('a',  5, 5), -- a.start = b.end
        ('a',  4, 4)
    "#;

    let q0 = r#"
        SELECT * FROM a JOIN b
        ON a.contig = b.contig AND a.start < b.end AND a.end > b.start
    "#;

    let q1 = r#"
        SELECT a.*, b.* FROM b JOIN a
        ON a.contig = b.contig AND a.end > b.start AND a.start < b.end
    "#;

    ctx.sql(a).await?;
    ctx.sql(b).await?;

    let expected = [
        "+--------+-------+-----+--------+-------+-----+",
        "| contig | start | end | contig | start | end |",
        "+--------+-------+-----+--------+-------+-----+",
        "| a      | 5     | 10  | a      | 9     | 15  |",
        "| a      | 5     | 10  | a      | 5     | 15  |",
        "| a      | 5     | 10  | a      | 4     | 15  |",
        "| a      | 5     | 10  | a      | 4     | 10  |",
        "| a      | 5     | 10  | a      | 6     | 8   |",
        "| a      | 5     | 10  | a      | 4     | 8   |",
        "+--------+-------+-----+--------+-------+-----+",
    ];

    let results = ctx.sql(q0).await?.collect().await?;
    assert_batches_sorted_eq!(expected, &results);

    let results = ctx.sql(q1).await?.collect().await?;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[rstest::rstest]
async fn test_nearest(ctx: SessionContext) -> Result<()> {
    let a = r#"
        CREATE TABLE a (contig TEXT, strand TEXT, start INTEGER, end INTEGER) AS VALUES
        ('a', 's', 5, 10)
    "#;

    let b = r#"
        CREATE TABLE b (contig TEXT, strand TEXT, start INTEGER, end INTEGER) AS VALUES
        ('a', 's', 11, 13),
        ('a', 's', 20, 21),
        ('a', 'x', 0, 1),
        ('b', 's', 1, 2)
    "#;

    ctx.sql("SET sequila.interval_join_algorithm TO CoitreesNearest")
        .await?;

    ctx.sql(a).await?;
    ctx.sql(b).await?;

    let q = r#"
        SELECT * FROM a JOIN b
        ON a.contig = b.contig AND a.strand = b.strand
            AND a.start < b.end AND a.end > b.start
    "#;

    let result = ctx.sql(q).await?;
    result.clone().show().await?;

    let results = result.collect().await?;

    let expected = [
        "+--------+--------+-------+-----+--------+--------+-------+-----+",
        "| contig | strand | start | end | contig | strand | start | end |",
        "+--------+--------+-------+-----+--------+--------+-------+-----+",
        "|        |        |       |     | a      | x      | 0     | 1   |",
        "|        |        |       |     | b      | s      | 1     | 2   |",
        "| a      | s      | 5     | 10  | a      | s      | 11    | 13  |",
        "| a      | s      | 5     | 10  | a      | s      | 20    | 21  |",
        "+--------+--------+-------+-----+--------+--------+-------+-----+",
    ];

    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}
