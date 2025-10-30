# Upgrade DataFusion to 50.3.0

## Why

The project currently uses DataFusion 48.0.1, which is two major versions behind the latest stable release (50.x series). Upgrading to DataFusion 50.3.0 provides:

1. **Performance improvements**: 5X speedup in nested loop joins with 99% less memory usage
2. **Bug fixes**: Fixes for recursive queries with dynamic filters via new `reset_state` mechanism
3. **API modernization**: Better support for UDF metadata, field-level type information, and expression handling
4. **Arrow 56.0.0**: Access to latest Arrow features and performance improvements
5. **Maintenance**: Staying current reduces technical debt and enables future upgrades

## What Changes

This upgrade involves adapting sequila-native code to DataFusion 50.x breaking changes:

1. **ExecutionPlan trait**: Implement new `reset_state()` method on `IntervalJoinExec`
2. **UDF trait changes**: Update any custom UDFs to use `PartialEq`, `Eq`, `Hash` traits instead of `equals`/`hash_value` methods (currently no custom UDFs exist)
3. **PhysicalExpr changes**: Implement `return_field()` method if using custom physical expressions
4. **ProjectionExpr refactoring**: Update tuple-style `(expr, alias)` to `ProjectionExpr::new(expr, alias)` struct
5. **ConfigOptions API**: Update code accessing `ConfigOptions` from `SessionState` to handle `Arc<ConfigOptions>` return type
6. **Dependencies**: Update `datafusion` and `datafusion-cli` from 48.0.1 to 50.3.0 (note: verify 50.3.0 availability; latest confirmed is 50.2.0)

## Impact

### Affected Code
- **Core Files**:
  - `sequila/sequila-core/src/physical_planner/joins/interval_join.rs` - ExecutionPlan implementation
  - `sequila/sequila-core/src/physical_planner/sequila_query_planner.rs` - QueryPlanner trait
  - `sequila/sequila-core/src/physical_planner/sequila_physical_planner.rs` - Physical planner
  - `sequila/sequila-core/src/session_context.rs` - Session configuration
  - `sequila/sequila-cli/src/main.rs` - CLI integration

- **Test & Benchmark Files**:
  - `sequila/sequila-core/tests/integration_test.rs`
  - `sequila/sequila-core/benches/databio_benchmark.rs`
  - All other files using DataFusion APIs

### Breaking Changes
- Requires Rust compiler updates if using new language features in DataFusion 50.x
- Potential behavioral changes in nested loop joins (should be transparent but needs testing)
- API signature changes may require adjustments in multiple files

### Testing Requirements
- All existing unit tests must pass
- Integration tests must pass
- Benchmark suite must run successfully (performance comparison optional but recommended)
- Verify interval join algorithms (coitrees, superintervals, lapper, nested-loop) still work correctly

### Migration Risk
**Medium Risk**: Multiple breaking API changes across core traits, but most are mechanical transformations with clear migration paths documented in DataFusion upgrade guide.
