# sequila-native
A set of native implementation of common bioinformatics algorithms to be used as Arrow-DataFusion or SeQuiLa (Apache Spark) extensions.

#
```bash
RUST_LOG=info cargo run 
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