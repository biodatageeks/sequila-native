# Deprecation note
This reopository is no longer active. Please use [https://github.com/biodatageeks/datafusion-bio-functions](https://github.com/biodatageeks/datafusion-bio-functions) instead.

# sequila-native
A set of native implementation of common bioinformatics algorithms to be used as Arrow-DataFusion or SeQuiLa (Apache Spark) extensions.

#
```bash
RUSTFLAGS="-C target-cpu=native" RUST_LOG=info cargo run --release
```

# Run a sql file
```bash
RUST_LOG=info cargo run -p sequila-cli -- --file queries/q1-coitrees.sql
```

# Perf

https://docs.rs/crate/flamegraph/0.6.5

## On ArchLinux

```bash
sudo pacman -S perf gcc-libs glibc
cargo install flamegraph
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid

cargo build --release
flamegraph -- target/release/sequila-cli -f queries/q1-coitrees.sql
```

## Recommended parameters
```sql
SET sequila.prefer_interval_join TO true;
SET sequila.interval_join_algorithm TO coitrees;
SET datafusion.optimizer.repartition_joins TO false;
SET datafusion.execution.coalesce_batches TO false;

-- for controlling parallism level (only for bechmarking purposes otherwise use defaults)
SET datafusion.execution.target_partitions=1;    
```

## How to run benchmark locally:
1. Download and unpack test [dataset](https://drive.google.com/file/d/1lctmude31mSAh9fWjI60K1bDrbeDPGfm/view?usp=sharing).
2. Export env variable with path to the root folder with benchmark data, e.g.:
```bash
export BENCH_DATA_ROOT=/Users/mwiewior/research/databio/ 
```
3. Run benchmark
```bash
RUSTFLAGS="-Ctarget-cpu=native" cargo bench --bench databio_benchmark -- --quick
```
