# datafusion-integration Specification

## Purpose
TBD - created by archiving change upgrade-datafusion-50. Update Purpose after archive.
## Requirements
### Requirement: DataFusion Version Compatibility
The system SHALL integrate with Apache DataFusion version 50.3.0 (or latest available 50.x series release) and datafusion-cli 50.3.0, maintaining compatibility with DataFusion's execution planning, physical operator, and session configuration APIs.

#### Scenario: ExecutionPlan trait implementation
- **GIVEN** custom `IntervalJoinExec` physical operator exists
- **WHEN** DataFusion's `ExecutionPlan` trait requires `reset_state()` method
- **THEN** `IntervalJoinExec` SHALL implement `reset_state()` to support recursive queries with dynamic filters

#### Scenario: Session configuration access
- **GIVEN** code accesses `ConfigOptions` from `SessionState`
- **WHEN** `SessionState::options()` returns `&Arc<ConfigOptions>` instead of `&ConfigOptions`
- **THEN** code SHALL handle `Arc`-wrapped configuration options correctly

#### Scenario: Physical expression metadata
- **GIVEN** custom physical expressions may exist in the future
- **WHEN** DataFusion requires `PhysicalExpr` implementations to provide field-level metadata
- **THEN** physical expressions SHALL implement `return_field()` method to return field information including metadata

#### Scenario: Projection expression structure
- **GIVEN** code uses projection expressions for column transformations
- **WHEN** DataFusion changes `ProjectionExpr` from tuple `(Arc<dyn PhysicalExpr>, String)` to named struct
- **THEN** code SHALL use `ProjectionExpr::new(expr, alias)` for construction and `.expr`/`.alias` for field access

### Requirement: Arrow Compatibility
The system SHALL use Apache Arrow 56.0.0 data structures and APIs as required by DataFusion 50.x, ensuring correct handling of RecordBatch, Array types, and column operations in interval join implementations.

#### Scenario: RecordBatch operations in interval joins
- **GIVEN** interval join algorithms process Arrow RecordBatch data
- **WHEN** Arrow 56.0.0 is used via DataFusion 50.x
- **THEN** all RecordBatch creation, column access, and data type conversions SHALL work correctly

#### Scenario: Array type handling
- **GIVEN** interval coordinates are stored as PrimitiveArray types
- **WHEN** using Arrow 56.0.0 array APIs
- **THEN** array construction, access, and casting operations SHALL maintain data integrity

### Requirement: Backward Compatibility Preservation
The system SHALL maintain identical query semantics, result correctness, and configuration parameter behavior after upgrading to DataFusion 50.x, ensuring existing SQL queries and interval join algorithms continue to function without user-visible changes.

#### Scenario: Interval join correctness across algorithms
- **GIVEN** existing interval join algorithms (coitrees, superintervals, lapper, nested-loop)
- **WHEN** executed with DataFusion 50.x
- **THEN** all algorithms SHALL produce identical results to DataFusion 48.0.1 for equivalent queries

#### Scenario: Configuration parameters remain functional
- **GIVEN** sequila-specific configuration parameters (`sequila.prefer_interval_join`, `sequila.interval_join_algorithm`)
- **WHEN** set via SessionState configuration
- **THEN** parameters SHALL control query behavior identically to DataFusion 48.0.1

#### Scenario: SQL query compatibility
- **GIVEN** existing SQL queries using interval joins
- **WHEN** executed via sequila-cli with DataFusion 50.x
- **THEN** queries SHALL execute successfully and return correct results

### Requirement: Performance Characteristics
The system SHALL maintain or improve query performance after upgrading to DataFusion 50.x, leveraging performance improvements in nested loop joins (5X speedup, 99% memory reduction) where applicable while ensuring no regression in custom interval join algorithms.

#### Scenario: Benchmark suite execution
- **GIVEN** existing benchmark suite with databio_benchmark
- **WHEN** executed with DataFusion 50.x
- **THEN** all benchmarks SHALL complete successfully without panics or errors

#### Scenario: Memory usage in interval joins
- **GIVEN** interval join operations on large genomic datasets
- **WHEN** executed with DataFusion 50.x
- **THEN** memory usage SHALL not exceed DataFusion 48.0.1 baseline for equivalent operations

