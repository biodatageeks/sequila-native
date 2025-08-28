use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::prelude::*;
use sequila_core::physical_planner::joins::interval_join::{
    IntervalJoinAlgorithm, SequilaInterval,
};
use sequila_core::session_context::Algorithm;
use std::collections::HashMap;
use std::time::Instant;

/// Benchmark testing DataFusion optimizations applied to SuperIntervals and Coitrees
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

fn benchmark_superintervals_optimized(c: &mut Criterion) {
    let dataset_sizes = vec![100_000, 500_000];

    let mut group = c.benchmark_group("superintervals_datafusion_optimized");
    group.sample_size(10);

    for &size in &dataset_sizes {
        println!(
            "Testing DataFusion optimized SuperIntervals with {} intervals",
            size
        );
        let build_intervals = generate_test_intervals(size, 12345);
        let probe_intervals = generate_test_intervals(size / 10, 54321);

        // Traditional SuperIntervals approach
        group.bench_function(
            &format!("Traditional_SuperIntervals_{}K", size / 1000),
            |b| {
                b.iter(|| {
                    let start = Instant::now();

                    // Traditional build: single HashMap insert
                    let mut traditional_hashmap = HashMap::new();
                    traditional_hashmap.insert(12345u64, black_box(build_intervals.clone()));
                    let traditional_algo =
                        IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, traditional_hashmap);

                    // Traditional probe: individual processing with new allocations
                    let mut results = Vec::new();
                    for interval in black_box(&probe_intervals) {
                        let mut matches = Vec::new();
                        traditional_algo.get(12345u64, interval.start, interval.end, |pos| {
                            matches.push(pos);
                        });
                        results.push(matches);
                    }

                    let elapsed = start.elapsed();
                    black_box((results.len(), elapsed));
                })
            },
        );

        // DataFusion optimized SuperIntervals
        group.bench_function(&format!("Optimized_SuperIntervals_{}K", size / 1000), |b| {
            b.iter(|| {
                let start = Instant::now();

                // Optimized build: streaming with pre-allocated HashMap
                let estimated_keys = (size / 1000).max(16);
                let mut optimized_hashmap =
                    HashMap::<u64, Vec<SequilaInterval>>::with_capacity(estimated_keys);
                let estimated_intervals_per_key = (size / estimated_keys).max(100);

                // Pre-allocate vector with estimated capacity
                optimized_hashmap.insert(12345u64, Vec::with_capacity(estimated_intervals_per_key));

                // Stream intervals incrementally
                for interval in black_box(&build_intervals) {
                    optimized_hashmap
                        .get_mut(&12345u64)
                        .unwrap()
                        .push(interval.clone());
                }

                let optimized_algo =
                    IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, optimized_hashmap);

                // Optimized probe: memory reuse patterns
                let mut results = Vec::with_capacity(probe_intervals.len());
                let mut reusable_buffer = Vec::with_capacity(100);

                for interval in black_box(&probe_intervals) {
                    reusable_buffer.clear();
                    optimized_algo.get(12345u64, interval.start, interval.end, |pos| {
                        reusable_buffer.push(pos);
                    });
                    results.push(reusable_buffer.clone());
                }

                let elapsed = start.elapsed();
                black_box((results.len(), elapsed));
            })
        });
    }

    group.finish();
}

fn benchmark_coitrees_optimized(c: &mut Criterion) {
    let dataset_sizes = vec![100_000, 500_000];

    let mut group = c.benchmark_group("coitrees_datafusion_optimized");
    group.sample_size(10);

    for &size in &dataset_sizes {
        println!(
            "Testing DataFusion optimized Coitrees with {} intervals",
            size
        );
        let build_intervals = generate_test_intervals(size, 67890);
        let probe_intervals = generate_test_intervals(size / 10, 98765);

        // Traditional Coitrees approach
        group.bench_function(&format!("Traditional_Coitrees_{}K", size / 1000), |b| {
            b.iter(|| {
                let start = Instant::now();

                let mut traditional_hashmap = HashMap::new();
                traditional_hashmap.insert(12345u64, black_box(build_intervals.clone()));
                let traditional_algo =
                    IntervalJoinAlgorithm::new(&Algorithm::Coitrees, traditional_hashmap);

                let mut results = Vec::new();
                for interval in black_box(&probe_intervals) {
                    let mut matches = Vec::new();
                    traditional_algo.get(12345u64, interval.start, interval.end, |pos| {
                        matches.push(pos);
                    });
                    results.push(matches);
                }

                let elapsed = start.elapsed();
                black_box((results.len(), elapsed));
            })
        });

        // DataFusion optimized Coitrees
        group.bench_function(&format!("Optimized_Coitrees_{}K", size / 1000), |b| {
            b.iter(|| {
                let start = Instant::now();

                // Apply same optimizations to Coitrees
                let estimated_keys = (size / 1000).max(16);
                let mut optimized_hashmap =
                    HashMap::<u64, Vec<SequilaInterval>>::with_capacity(estimated_keys);
                let estimated_intervals_per_key = (size / estimated_keys).max(100);

                optimized_hashmap.insert(12345u64, Vec::with_capacity(estimated_intervals_per_key));

                for interval in black_box(&build_intervals) {
                    optimized_hashmap
                        .get_mut(&12345u64)
                        .unwrap()
                        .push(interval.clone());
                }

                let optimized_algo =
                    IntervalJoinAlgorithm::new(&Algorithm::Coitrees, optimized_hashmap);

                let mut results = Vec::with_capacity(probe_intervals.len());
                let mut reusable_buffer = Vec::with_capacity(100);

                for interval in black_box(&probe_intervals) {
                    reusable_buffer.clear();
                    optimized_algo.get(12345u64, interval.start, interval.end, |pos| {
                        reusable_buffer.push(pos);
                    });
                    results.push(reusable_buffer.clone());
                }

                let elapsed = start.elapsed();
                black_box((results.len(), elapsed));
            })
        });
    }

    group.finish();
}

fn benchmark_algorithm_comparison_optimized(c: &mut Criterion) {
    let build_size = 200_000;
    let probe_size = 10_000;

    println!(
        "Comparing optimized SuperIntervals vs Coitrees with {} build, {} probe",
        build_size, probe_size
    );
    let build_intervals = generate_test_intervals(build_size, 11111);
    let probe_intervals = generate_test_intervals(probe_size, 22222);

    let mut group = c.benchmark_group("algorithm_comparison_optimized");

    for algorithm in &[Algorithm::SuperIntervals, Algorithm::Coitrees] {
        let algorithm_name = format!("{:?}", algorithm);

        group.bench_function(&format!("Optimized_{}", algorithm_name), |b| {
            b.iter(|| {
                let start = Instant::now();

                // Apply DataFusion optimizations
                let estimated_keys = (build_size / 1000).max(16);
                let mut optimized_hashmap =
                    HashMap::<u64, Vec<SequilaInterval>>::with_capacity(estimated_keys);
                let estimated_intervals_per_key = (build_size / estimated_keys).max(100);

                optimized_hashmap.insert(12345u64, Vec::with_capacity(estimated_intervals_per_key));

                for interval in black_box(&build_intervals) {
                    optimized_hashmap
                        .get_mut(&12345u64)
                        .unwrap()
                        .push(interval.clone());
                }

                let optimized_algo = IntervalJoinAlgorithm::new(algorithm, optimized_hashmap);

                // Optimized probe
                let mut total_matches = 0;
                let mut reusable_buffer: Vec<usize> = Vec::with_capacity(50);

                for interval in black_box(&probe_intervals) {
                    reusable_buffer.clear();
                    optimized_algo.get(12345u64, interval.start, interval.end, |_| {
                        total_matches += 1;
                    });
                }

                let elapsed = start.elapsed();
                black_box((total_matches, elapsed));
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_superintervals_optimized,
    benchmark_coitrees_optimized,
    benchmark_algorithm_comparison_optimized
);
criterion_main!(benches);
