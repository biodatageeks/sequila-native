use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::prelude::*;
use sequila_core::physical_planner::joins::interval_join::{
    IntervalJoinAlgorithm, SequilaInterval,
};
use sequila_core::session_context::Algorithm;
use std::collections::HashMap;
use std::time::Instant;

/// Quick optimization test with smaller datasets for immediate results
fn generate_test_intervals(count: usize, seed: u64) -> Vec<SequilaInterval> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|i| {
            let start = rng.gen_range(0..1_000_000);
            let end = start + rng.gen_range(100..10_000);
            SequilaInterval {
                start,
                end,
                position: i,
            }
        })
        .collect()
}

fn benchmark_quick_optimization_comparison(c: &mut Criterion) {
    let dataset_sizes = vec![50_000, 100_000];

    let mut group = c.benchmark_group("quick_optimization_comparison");
    group.sample_size(10);

    for &size in &dataset_sizes {
        println!("Testing optimization with {} intervals", size);
        let intervals = generate_test_intervals(size, 12345);

        // Traditional approach
        group.bench_function(&format!("Traditional_{}M", size / 1_000_000), |b| {
            b.iter(|| {
                let start = Instant::now();
                let mut hashmap = HashMap::new();
                hashmap.insert(12345u64, black_box(intervals.clone()));
                let algorithm = IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, hashmap);
                let elapsed = start.elapsed();
                black_box((algorithm, elapsed));
            });
        });

        // Optimized approach with pre-allocation
        group.bench_function(&format!("Optimized_{}M", size / 1_000_000), |b| {
            b.iter(|| {
                let start = Instant::now();
                let mut optimized_hashmap = HashMap::<u64, Vec<SequilaInterval>>::with_capacity(10);
                optimized_hashmap.insert(12345u64, Vec::with_capacity(size));

                for interval in black_box(&intervals) {
                    optimized_hashmap
                        .get_mut(&12345u64)
                        .unwrap()
                        .push(interval.clone());
                }

                let algorithm =
                    IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, optimized_hashmap);
                let elapsed = start.elapsed();
                black_box((algorithm, elapsed));
            });
        });
    }

    group.finish();
}

fn benchmark_probe_optimization_quick(c: &mut Criterion) {
    // Smaller scale probe test
    let build_size = 100_000;
    let probe_size = 10_000;

    println!("Building {}K interval index...", build_size / 1000);
    let build_intervals = generate_test_intervals(build_size, 11111);
    let probe_intervals = generate_test_intervals(probe_size, 22222);

    let mut build_hashmap = HashMap::new();
    build_hashmap.insert(12345u64, build_intervals);
    let algorithm = IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, build_hashmap);

    let mut group = c.benchmark_group("quick_probe_optimization");

    // Traditional individual processing
    group.bench_function("Traditional_Individual_50K", |b| {
        b.iter(|| {
            let start = Instant::now();
            let mut results = Vec::new();

            for interval in black_box(&probe_intervals) {
                let mut matches = Vec::new();
                algorithm.get(12345u64, interval.start, interval.end, |pos| {
                    matches.push(pos);
                });
                results.push(matches);
            }

            let elapsed = start.elapsed();
            black_box((results.len(), elapsed));
        });
    });

    // Optimized with pre-allocated buffers
    group.bench_function("Optimized_PreAlloc_50K", |b| {
        b.iter(|| {
            let start = Instant::now();
            let mut results = Vec::with_capacity(probe_size);
            let mut match_buffer = Vec::with_capacity(50);

            for interval in black_box(&probe_intervals) {
                match_buffer.clear();
                algorithm.get(12345u64, interval.start, interval.end, |pos| {
                    match_buffer.push(pos);
                });
                results.push(match_buffer.clone());
            }

            let elapsed = start.elapsed();
            black_box((results.len(), elapsed));
        });
    });

    group.finish();
}

fn benchmark_algorithm_comparison_quick(c: &mut Criterion) {
    let build_size = 200_000;
    let probe_size = 5_000;

    println!(
        "Building {}M intervals for algorithm comparison...",
        build_size / 1_000_000
    );
    let build_intervals = generate_test_intervals(build_size, 33333);
    let probe_intervals = generate_test_intervals(probe_size, 44444);

    let mut group = c.benchmark_group("quick_algorithm_comparison");

    // SuperIntervals
    let mut super_hashmap = HashMap::new();
    super_hashmap.insert(12345u64, build_intervals.clone());
    let super_algorithm = IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, super_hashmap);

    // Coitrees
    let mut coitrees_hashmap = HashMap::new();
    coitrees_hashmap.insert(12345u64, build_intervals);
    let coitrees_algorithm = IntervalJoinAlgorithm::new(&Algorithm::Coitrees, coitrees_hashmap);

    // SuperIntervals benchmark
    group.bench_function("SuperIntervals_2M_vs_25K", |b| {
        b.iter(|| {
            let start = Instant::now();
            let mut total_matches = 0;

            for interval in black_box(&probe_intervals) {
                super_algorithm.get(12345u64, interval.start, interval.end, |_| {
                    total_matches += 1;
                });
            }

            let elapsed = start.elapsed();
            black_box((total_matches, elapsed));
        });
    });

    // Coitrees benchmark
    group.bench_function("Coitrees_2M_vs_25K", |b| {
        b.iter(|| {
            let start = Instant::now();
            let mut total_matches = 0;

            for interval in black_box(&probe_intervals) {
                coitrees_algorithm.get(12345u64, interval.start, interval.end, |_| {
                    total_matches += 1;
                });
            }

            let elapsed = start.elapsed();
            black_box((total_matches, elapsed));
        });
    });

    group.finish();
}

fn benchmark_optimization_impact(c: &mut Criterion) {
    println!("=== Quick Optimization Impact Test ===");

    let build_size = 100_000;
    let probe_size = 10_000;

    let build_intervals = generate_test_intervals(build_size, 55555);
    let probe_intervals = generate_test_intervals(probe_size, 66666);

    let mut group = c.benchmark_group("optimization_impact");

    // Baseline: All traditional approaches
    group.bench_function("Baseline_All_Traditional", |b| {
        b.iter(|| {
            let total_start = Instant::now();

            // Traditional build
            let mut hashmap = HashMap::new();
            hashmap.insert(12345u64, black_box(build_intervals.clone()));
            let algorithm = IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, hashmap);

            // Traditional probe
            let mut results = Vec::new();
            for interval in black_box(&probe_intervals) {
                let mut matches = Vec::new();
                algorithm.get(12345u64, interval.start, interval.end, |pos| {
                    matches.push(pos);
                });
                results.push(matches);
            }

            let total_elapsed = total_start.elapsed();
            black_box((results.len(), total_elapsed));
        });
    });

    // Optimized: All optimizations applied
    group.bench_function("Optimized_All_Improvements", |b| {
        b.iter(|| {
            let total_start = Instant::now();

            // Optimized build with pre-allocation
            let mut optimized_hashmap = HashMap::<u64, Vec<SequilaInterval>>::with_capacity(10);
            optimized_hashmap.insert(12345u64, Vec::with_capacity(build_size));

            for interval in black_box(&build_intervals) {
                optimized_hashmap
                    .get_mut(&12345u64)
                    .unwrap()
                    .push(interval.clone());
            }

            let algorithm =
                IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, optimized_hashmap);

            // Optimized probe with pre-allocated buffers and chunking
            let mut results = Vec::with_capacity(probe_size);
            let mut match_buffer = Vec::with_capacity(100);

            const CHUNK_SIZE: usize = 128;
            for chunk in black_box(&probe_intervals).chunks(CHUNK_SIZE) {
                for interval in chunk {
                    match_buffer.clear();
                    algorithm.get(12345u64, interval.start, interval.end, |pos| {
                        match_buffer.push(pos);
                    });
                    results.push(match_buffer.clone());
                }
            }

            let total_elapsed = total_start.elapsed();
            black_box((results.len(), total_elapsed));
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_quick_optimization_comparison,
    benchmark_probe_optimization_quick,
    benchmark_algorithm_comparison_quick,
    benchmark_optimization_impact
);
criterion_main!(benches);
