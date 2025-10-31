use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::prelude::*;
use sequila_core::physical_planner::joins::interval_join::{
    IntervalJoinAlgorithm, SequilaInterval,
};
use sequila_core::session_context::Algorithm;
use std::collections::HashMap;
use std::time::Instant;

/// Direct comparison benchmark between current and optimized implementations
/// Tests specific optimization components in isolation

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

fn benchmark_build_optimization_direct(c: &mut Criterion) {
    let dataset_sizes = vec![1_000_000, 5_000_000, 10_000_000];

    let mut group = c.benchmark_group("build_optimization_direct");
    group.sample_size(10);

    for &size in &dataset_sizes {
        println!("Generating {} intervals for build benchmark...", size);
        let intervals = generate_test_intervals(size, 12345);

        // Traditional approach: single large HashMap insert
        group.bench_function(&format!("Traditional_Build_{}M", size / 1_000_000), |b| {
            b.iter(|| {
                let start = Instant::now();
                let mut hashmap = HashMap::new();
                hashmap.insert(12345u64, black_box(intervals.clone()));
                let algorithm = IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, hashmap);
                let elapsed = start.elapsed();
                black_box((algorithm, elapsed));
            });
        });

        // Streaming approach: incremental building
        group.bench_function(&format!("Streaming_Build_{}M", size / 1_000_000), |b| {
            b.iter(|| {
                let start = Instant::now();
                let mut streaming_hashmap = HashMap::<u64, Vec<SequilaInterval>>::new();

                // Simulate streaming by processing in chunks
                const CHUNK_SIZE: usize = 50_000;
                for chunk in black_box(&intervals).chunks(CHUNK_SIZE) {
                    for interval in chunk {
                        streaming_hashmap
                            .entry(12345u64)
                            .or_insert_with(Vec::new)
                            .push(interval.clone());
                    }
                }

                let algorithm =
                    IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, streaming_hashmap);
                let elapsed = start.elapsed();
                black_box((algorithm, elapsed));
            });
        });

        // Optimized streaming with pre-allocation
        group.bench_function(&format!("Optimized_Streaming_{}M", size / 1_000_000), |b| {
            b.iter(|| {
                let start = Instant::now();
                let mut optimized_hashmap =
                    HashMap::<u64, Vec<SequilaInterval>>::with_capacity(100);

                // Pre-allocate with better estimates
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

fn benchmark_probe_optimization_direct(c: &mut Criterion) {
    // Build a large index for probing
    let build_size = 5_000_000;
    let probe_sizes = vec![100_000, 500_000, 1_000_000];

    println!(
        "Building {}M interval index for probe benchmarks...",
        build_size / 1_000_000
    );
    let build_intervals = generate_test_intervals(build_size, 11111);
    let mut build_hashmap = HashMap::new();
    build_hashmap.insert(12345u64, build_intervals);

    let superintervals_algo =
        IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, build_hashmap.clone());
    let coitrees_algo = IntervalJoinAlgorithm::new(&Algorithm::Coitrees, build_hashmap);

    let mut group = c.benchmark_group("probe_optimization_direct");

    for &probe_size in &probe_sizes {
        println!("Generating {} probe intervals...", probe_size);
        let probe_intervals = generate_test_intervals(probe_size, 22222);

        // SuperIntervals individual processing
        group.bench_function(
            &format!("SuperIntervals_Individual_{}K", probe_size / 1000),
            |b| {
                b.iter(|| {
                    let start = Instant::now();
                    let mut total_matches = 0;
                    for interval in black_box(&probe_intervals) {
                        superintervals_algo.get(12345u64, interval.start, interval.end, |_| {
                            total_matches += 1;
                        });
                    }
                    let elapsed = start.elapsed();
                    black_box((total_matches, elapsed));
                });
            },
        );

        // Coitrees individual processing
        group.bench_function(
            &format!("Coitrees_Individual_{}K", probe_size / 1000),
            |b| {
                b.iter(|| {
                    let start = Instant::now();
                    let mut total_matches = 0;
                    for interval in black_box(&probe_intervals) {
                        coitrees_algo.get(12345u64, interval.start, interval.end, |_| {
                            total_matches += 1;
                        });
                    }
                    let elapsed = start.elapsed();
                    black_box((total_matches, elapsed));
                });
            },
        );

        // Chunked processing (vectorization simulation)
        group.bench_function(
            &format!("SuperIntervals_Chunked_{}K", probe_size / 1000),
            |b| {
                b.iter(|| {
                    let start = Instant::now();
                    let mut total_matches = 0;

                    // Process in chunks to simulate vectorized benefits
                    const CHUNK_SIZE: usize = 256;
                    for chunk in black_box(&probe_intervals).chunks(CHUNK_SIZE) {
                        // Simulate reduced overhead per chunk
                        for interval in chunk {
                            superintervals_algo.get(12345u64, interval.start, interval.end, |_| {
                                total_matches += 1;
                            });
                        }
                    }

                    let elapsed = start.elapsed();
                    black_box((total_matches, elapsed));
                });
            },
        );

        // Pre-allocated results (memory optimization)
        group.bench_function(
            &format!("SuperIntervals_PreAlloc_{}K", probe_size / 1000),
            |b| {
                b.iter(|| {
                    let start = Instant::now();
                    let mut results = Vec::with_capacity(probe_size);
                    let mut reusable_buffer = Vec::with_capacity(50); // Estimate 50 matches per interval

                    for interval in black_box(&probe_intervals) {
                        reusable_buffer.clear();
                        superintervals_algo.get(12345u64, interval.start, interval.end, |pos| {
                            reusable_buffer.push(pos);
                        });
                        results.push(reusable_buffer.clone());
                    }

                    let elapsed = start.elapsed();
                    black_box((results.len(), elapsed));
                });
            },
        );
    }

    group.finish();
}

fn benchmark_memory_allocation_patterns(c: &mut Criterion) {
    let test_size = 500_000;
    let probe_intervals = generate_test_intervals(test_size, 33333);

    let build_intervals = generate_test_intervals(1_000_000, 44444);
    let mut build_hashmap = HashMap::new();
    build_hashmap.insert(12345u64, build_intervals);
    let algorithm = IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, build_hashmap);

    let mut group = c.benchmark_group("memory_allocation_optimization");

    // High allocation pattern (current approach simulation)
    group.bench_function("High_Allocation_500K", |b| {
        b.iter(|| {
            let start = Instant::now();
            let mut all_results = Vec::new();

            for interval in black_box(&probe_intervals) {
                let mut interval_matches = Vec::new(); // New allocation per interval
                algorithm.get(12345u64, interval.start, interval.end, |pos| {
                    interval_matches.push(pos);
                });
                all_results.push(interval_matches);
            }

            let elapsed = start.elapsed();
            black_box((all_results.len(), elapsed));
        });
    });

    // Pre-allocated buffers (optimized)
    group.bench_function("Pre_Allocated_Buffers_500K", |b| {
        b.iter(|| {
            let start = Instant::now();
            let mut results = Vec::with_capacity(test_size);
            let mut match_buffer = Vec::with_capacity(1024); // Reusable buffer

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

    // Single large allocation (most optimized)
    group.bench_function("Single_Large_Allocation_500K", |b| {
        b.iter(|| {
            let start = Instant::now();
            let mut all_matches = Vec::with_capacity(test_size * 10); // Pre-allocate for estimated matches
            let mut boundaries = Vec::with_capacity(test_size);

            for interval in black_box(&probe_intervals) {
                let start_idx = all_matches.len();
                algorithm.get(12345u64, interval.start, interval.end, |pos| {
                    all_matches.push(pos);
                });
                boundaries.push((start_idx, all_matches.len()));
            }

            let elapsed = start.elapsed();
            black_box((boundaries.len(), elapsed));
        });
    });

    group.finish();
}

fn benchmark_algorithm_comparison_large_scale(c: &mut Criterion) {
    let build_sizes = vec![1_000_000, 5_000_000];
    let probe_size = 100_000;

    let mut group = c.benchmark_group("algorithm_comparison_large_scale");
    group.sample_size(10);

    for &build_size in &build_sizes {
        println!(
            "Generating {}M build intervals for algorithm comparison...",
            build_size / 1_000_000
        );
        let build_intervals = generate_test_intervals(build_size, 55555);
        let probe_intervals = generate_test_intervals(probe_size, 66666);

        // Test both SuperIntervals and Coitrees at scale
        for algorithm_type in &[Algorithm::SuperIntervals, Algorithm::Coitrees] {
            let algorithm_name = format!("{:?}", algorithm_type);

            // Build phase benchmark
            let build_bench_name = format!("Build_{}_{}", algorithm_name, build_size / 1_000_000);
            group.bench_function(&build_bench_name, |b| {
                b.iter(|| {
                    let start = Instant::now();
                    let mut hashmap = HashMap::new();
                    hashmap.insert(12345u64, black_box(build_intervals.clone()));
                    let algorithm = IntervalJoinAlgorithm::new(algorithm_type, hashmap);
                    let elapsed = start.elapsed();
                    black_box((algorithm, elapsed));
                });
            });

            // Pre-build for probe testing
            let mut hashmap = HashMap::new();
            hashmap.insert(12345u64, build_intervals.clone());
            let algorithm = IntervalJoinAlgorithm::new(algorithm_type, hashmap);

            // Probe phase benchmark
            let probe_bench_name = format!("Probe_{}_{}", algorithm_name, build_size / 1_000_000);
            group.bench_function(&probe_bench_name, |b| {
                b.iter(|| {
                    let start = Instant::now();
                    let mut total_matches = 0;

                    for interval in black_box(&probe_intervals) {
                        algorithm.get(12345u64, interval.start, interval.end, |_| {
                            total_matches += 1;
                        });
                    }

                    let elapsed = start.elapsed();
                    black_box((total_matches, elapsed));
                });
            });
        }
    }

    group.finish();
}

fn benchmark_optimization_impact_summary(c: &mut Criterion) {
    println!("=== Optimization Impact Summary Benchmark ===");

    // Test the complete optimization stack
    let build_size = 2_000_000;
    let probe_size = 200_000;

    let build_intervals = generate_test_intervals(build_size, 77777);
    let probe_intervals = generate_test_intervals(probe_size, 88888);

    let mut group = c.benchmark_group("optimization_impact_summary");
    group.sample_size(15);

    // Baseline: Traditional approach
    group.bench_function("Baseline_Traditional", |b| {
        b.iter(|| {
            let total_start = Instant::now();

            // Traditional build
            let mut hashmap = HashMap::new();
            hashmap.insert(12345u64, black_box(build_intervals.clone()));
            let algorithm = IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, hashmap);

            // Traditional probe with allocations
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

    // Optimized: Streaming build + pre-allocated probe
    group.bench_function("Optimized_Complete", |b| {
        b.iter(|| {
            let total_start = Instant::now();

            // Optimized streaming build
            let mut streaming_hashmap = HashMap::<u64, Vec<SequilaInterval>>::with_capacity(100);
            streaming_hashmap.insert(12345u64, Vec::with_capacity(build_size));

            for interval in black_box(&build_intervals) {
                streaming_hashmap
                    .get_mut(&12345u64)
                    .unwrap()
                    .push(interval.clone());
            }

            let algorithm =
                IntervalJoinAlgorithm::new(&Algorithm::SuperIntervals, streaming_hashmap);

            // Optimized probe with memory reuse
            let mut results = Vec::with_capacity(probe_size);
            let mut match_buffer = Vec::with_capacity(100);

            const CHUNK_SIZE: usize = 256;
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
    benchmark_build_optimization_direct,
    benchmark_probe_optimization_direct,
    benchmark_memory_allocation_patterns,
    benchmark_algorithm_comparison_large_scale,
    benchmark_optimization_impact_summary
);
criterion_main!(benches);
