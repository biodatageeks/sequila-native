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
