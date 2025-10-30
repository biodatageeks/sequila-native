# Project Context

## Purpose
sequila-native provides high-performance native implementations of bioinformatics algorithms for genomic interval operations. The project extends Apache DataFusion with specialized physical operators for efficient interval joins, enabling fast querying of genomic data (BED, BAM, VCF files) using SQL. It aims to bring the performance and expressiveness of SeQuiLa (Spark-based genomics toolkit) to the Rust/DataFusion ecosystem with native speed.

## Tech Stack
- **Language**: Rust (edition 2021, minimum version 1.76)
- **Query Engine**: Apache DataFusion 48.0.1
- **CLI Framework**: datafusion-cli 48.0.1, clap 4.5.20
- **Interval Data Structures**:
  - coitrees 0.4.0 (COITree - implicit augmented interval tree)
  - superintervals (local crate, SIMD-optimized interval operations)
  - rust-lapper 1.1.0 (nested containment list)
- **Bioinformatics**: bio 2.0.1 (parsing genomic file formats)
- **Concurrency**: tokio 1.36.0 (async runtime)
- **Performance Tools**: criterion 0.5.1 (benchmarking), flamegraph (profiling)
- **Data Structures**: ahash 0.8.11, hashbrown 0.14.5, parking_lot 0.12.3

## Project Conventions

### Code Style
- Use standard Rust formatting (`cargo fmt`)
- Follow Rust naming conventions (snake_case for functions/variables, PascalCase for types)
- Prefer explicit error handling over panics in production code
- Use workspace-level dependencies for version consistency across crates
- Enable native CPU optimizations via `RUSTFLAGS="-C target-cpu=native"`

### Architecture Patterns
- **Physical Planner Extension**: Custom `SequilaQueryPlanner` extends DataFusion's physical planning
- **Algorithm Polymorphism**: `IntervalJoinAlgorithm` enum allows runtime selection of join strategies (coitrees, superintervals, lapper, nested-loop)
- **Session Context**: Custom `SequilaSessionContext` wraps DataFusion context with bioinformatics-specific configuration
- **SIMD Optimization**: Platform-specific SIMD code (AVX2, NEON) for interval scanning
- **Workspace Structure**:
  - `sequila-core`: Core algorithms and DataFusion extensions
  - `sequila-cli`: Command-line interface
  - `superintervals`: Standalone SIMD interval library

### Testing Strategy
- **Unit Tests**: Use `rstest` for parameterized testing of interval algorithms
- **Integration Tests**: Located in `sequila/sequila-core/tests/`
- **Benchmarking**: Criterion-based benchmarks in `benches/` directory
  - `databio_benchmark.rs`: Main performance benchmark suite
  - `quick_optimization_test.rs`: Fast iteration benchmarks
- **Performance Profiling**: Use flamegraph with perf on Linux
- **Test Data**: External benchmark datasets via `BENCH_DATA_ROOT` environment variable
- **Cross-Algorithm Validation**: Compare results across coitrees, superintervals, and lapper for correctness

### Git Workflow
- **Main Branch**: `master`
- **Feature Branches**: Descriptive names (e.g., `upgrade-datafusion-50.3.0`, `add-superintervals`)
- **Commit Convention**: Use conventional commit style when possible
- **Pull Requests**: Required for merging to master (see recent PRs: #64, #63, #62, #61)
- **Pre-commit Hooks**: May be configured for formatting/linting

## Domain Context

### Bioinformatics Interval Operations
- **Genomic Intervals**: Represent regions on chromosomes (chromosome, start, end, metadata)
- **Interval Joins**: Core operation for finding overlapping genomic regions between two datasets
  - Example: Find all genes overlapping with ChIP-seq peaks
  - Example: Annotate variants with overlapping regulatory elements
- **File Formats**: BED (tab-delimited intervals), BAM (aligned sequences), VCF (variants)
- **Performance Critical**: Genomic datasets often contain millions of intervals; queries must be efficient

### Interval Join Algorithms
1. **Nested Loops**: Simple but slow O(n*m) baseline
2. **COITrees** (Constant Order Interval Tree): Implicit augmented tree with SIMD scanning
3. **SuperIntervals**: SIMD-optimized sorted intervals with binary search
4. **Lapper** (LAyer PARallel): Nested containment list, good for deeply nested intervals

### Configuration Parameters
- `sequila.prefer_interval_join`: Enable interval join optimization
- `sequila.interval_join_algorithm`: Algorithm selection (coitrees, superintervals, lapper, nestedloops)
- `datafusion.optimizer.repartition_joins`: Disable for better interval join performance
- `datafusion.execution.coalesce_batches`: Disable for more predictable benchmarking
- `datafusion.execution.target_partitions`: Control parallelism level

## Important Constraints

### Performance Requirements
- Must process millions of intervals efficiently (target: sub-millisecond per query on modern hardware)
- Memory allocation should be minimized in hot paths
- SIMD optimizations are critical for competitive performance
- Native CPU features must be enabled (`-C target-cpu=native`)

### Platform Considerations
- Primary platforms: Linux (x86_64, aarch64), macOS (Apple Silicon, Intel)
- SIMD implementations: AVX2 (x86_64), NEON (aarch64), fallback (generic)
- Apple Silicon optimization is an active area of work (see APPLE_SILICON_INTERVAL_JOINS_REPORT.md)

### DataFusion Integration
- Must remain compatible with DataFusion's physical planning interface
- Physical operators must implement standard DataFusion traits (ExecutionPlan, etc.)
- Custom session context extends but does not replace DataFusion functionality

### Data Correctness
- Interval join results must match semantically across all algorithms
- Edge cases: empty intervals, duplicate intervals, single-point intervals
- Numerical stability: interval boundaries are typically 64-bit integers

## External Dependencies

### Core Dependencies
- **Apache DataFusion**: Query planning, execution, optimization framework
  - Version 48.0.1 (upgrading to 50.3.0 on current branch)
  - Breaking changes between versions may require adaptation
- **datafusion-cli**: Provides REPL interface and SQL execution utilities

### Interval Libraries
- **coitrees**: Third-party crate for augmented interval trees
- **rust-lapper**: Third-party crate for nested containment lists
- **superintervals**: Internal crate with custom SIMD implementations

### Bioinformatics Ecosystem
- **rust-bio**: Parsing and data structures for genomic file formats
- May integrate with external tools/pipelines for data ingestion

### Benchmark Data
- External datasets downloaded separately (not in repository)
- Specified via `BENCH_DATA_ROOT` environment variable
- Required for running full benchmark suite
