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