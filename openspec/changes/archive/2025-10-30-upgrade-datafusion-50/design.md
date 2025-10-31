# Design: DataFusion 50.3.0 Upgrade

## Context

sequila-native currently depends on DataFusion 48.0.1 and datafusion-cli 48.0.1. DataFusion 50.x introduces several breaking API changes across core traits (ExecutionPlan, PhysicalExpr, UDF traits) along with significant performance improvements in nested loop joins and bug fixes for recursive queries.

The upgrade requires adapting to new trait requirements while maintaining compatibility with existing interval join algorithms and bioinformatics-specific query planning logic.

### Stakeholders
- Developers maintaining interval join implementations
- Users running genomic queries via CLI
- Benchmark/performance testing workflows

### Current Architecture
- Custom `IntervalJoinExec` implements DataFusion's `ExecutionPlan` trait
- `SeQuiLaQueryPlanner` implements `QueryPlanner` trait
- `SeQuiLaPhysicalPlanner` extends DataFusion's physical planning
- No custom UDFs currently implemented

## Goals / Non-Goals

### Goals
- Upgrade to DataFusion 50.3.0 (or latest 50.x available) with full API compatibility
- Maintain correctness of all four interval join algorithms (coitrees, superintervals, lapper, nested-loop)
- Ensure existing SQL queries and configuration parameters continue to work
- Pass all existing tests and benchmarks
- Take advantage of improved nested loop join performance if applicable

### Non-Goals
- Refactoring interval join implementation beyond what's required for API compatibility
- Adding new features or capabilities specific to DataFusion 50.x
- Performance optimization beyond what DataFusion 50.x provides automatically
- Updating to future DataFusion versions beyond 50.x series

## Decisions

### Decision 1: Implement reset_state() as No-Op Initially
**Rationale**: The new `reset_state()` method is required on all ExecutionPlan implementations to support recursive queries with dynamic filters. IntervalJoinExec likely doesn't maintain state that needs resetting beyond what's handled by reconstructing the plan.

**Implementation**:
```rust
fn reset_state(&self) -> datafusion::common::Result<()> {
    // IntervalJoinExec uses OnceAsync for left side building which is
    // single-use per execution. No persistent state needs resetting.
    Ok(())
}
```

**Validation**: If recursive query issues arise later, we can revisit and add proper state reset for `left_fut` if needed.

### Decision 2: Target 50.2.0 as Fallback
**Rationale**: Web search confirmed DataFusion 50.2.0 exists but 50.3.0 was not found. We'll attempt 50.3.0 first but document 50.2.0 as the fallback.

**Implementation**: Check crates.io during implementation; use latest available in 50.x series.

### Decision 3: Handle ConfigOptions Arc Wrapping Transparently
**Rationale**: SessionState now returns `&Arc<ConfigOptions>` instead of `&ConfigOptions`. Rust's auto-deref should handle most cases, but explicit `.as_ref()` calls may be needed in some contexts.

**Implementation**: Let the compiler guide fixes during `cargo check`; only add `.as_ref()` where compilation fails.

### Decision 4: No Design.md for Spec Deltas
**Rationale**: This is primarily a dependency upgrade with mechanical API changes rather than a feature addition. Specs are not affected since functionality remains the same—only internal implementation adapts to new DataFusion APIs.

**Decision**: Skip spec deltas entirely; this is a technical debt reduction/maintenance task rather than a capability change.

## Risks / Trade-offs

### Risk 1: Subtle Behavioral Changes in Nested Loop Joins
**Impact**: DataFusion 50.x rewrote nested loop joins for better performance. While semantically equivalent, there may be subtle differences in output ordering or edge case handling.

**Mitigation**:
- Run full test suite including integration tests
- Compare benchmark results with 48.0.1 baseline
- Manual testing with representative genomic queries

### Risk 2: Undocumented API Changes
**Impact**: Not all breaking changes may be captured in upgrade guide; some may only surface during compilation or runtime.

**Mitigation**:
- Thorough `cargo check` and `cargo clippy` review
- Read DataFusion CHANGELOG between 48.0.1 and 50.x
- Test all code paths, not just happy paths

### Risk 3: Performance Regression
**Impact**: While DataFusion 50.x improves nested loop joins, there's risk of performance regression in other areas or interaction with custom interval join algorithms.

**Mitigation**:
- Run benchmark suite before/after upgrade
- Monitor for memory usage changes (DataFusion 50.x claims 99% memory reduction in some cases)
- Document any performance changes in PR

### Risk 4: Rust Version Requirements
**Impact**: DataFusion 50.x may require newer Rust version than 1.76 specified in workspace Cargo.toml.

**Mitigation**:
- Check DataFusion 50.x rust-version requirement
- Update workspace rust-version if needed
- Verify CI/CD pipelines support required Rust version

## Trade-offs

| Aspect | DataFusion 48.0.1 | DataFusion 50.3.0 | Decision |
|--------|-------------------|-------------------|----------|
| **API Stability** | Stable (no changes needed) | Breaking changes require code updates | Accept: Necessary for long-term maintenance |
| **Performance** | Known baseline | Improved nested loop joins (5X faster) | Win: Free performance improvement |
| **Maintenance** | Growing technical debt | Current version | Win: Easier to upgrade incrementally |
| **Risk** | Zero (no change) | Medium (API changes, potential bugs) | Accept: Mitigated by testing |
| **Arrow Version** | 53.x | 56.0.0 | Win: Access to latest Arrow features |

## Migration Plan

### Phase 1: Dependency Update (Low Risk)
1. Update Cargo.toml versions
2. Run `cargo check` to identify compilation errors
3. Document all errors for systematic fixing

### Phase 2: Core API Fixes (Medium Risk)
1. Add `reset_state()` to IntervalJoinExec
2. Fix ConfigOptions Arc wrapping issues
3. Update ProjectionExpr usage if found
4. Fix any PhysicalExpr trait changes

### Phase 3: Compilation Success (Medium Risk)
1. Resolve all compiler errors
2. Address deprecation warnings
3. Run `cargo clippy` for additional issues

### Phase 4: Testing & Validation (High Confidence Gate)
1. Unit tests must pass 100%
2. Integration tests must pass 100%
3. Benchmark suite must run without panics
4. Manual CLI testing with sample queries

### Phase 5: Performance Validation (Optional)
1. Run full benchmark suite with BENCH_DATA_ROOT
2. Compare results with 48.0.1 baseline
3. Document any significant performance changes

### Rollback Plan
If critical issues are discovered:
1. Revert Cargo.toml changes
2. Revert any API-specific code changes
3. Run `cargo check` to verify rollback success
4. Document issues for future upgrade attempt

## Open Questions

1. **Q**: Does DataFusion 50.3.0 exist on crates.io?
   - **Status**: To be verified during implementation; fallback to 50.2.0

2. **Q**: Are there any DataFusion module path changes affecting imports?
   - **Status**: Will be discovered during compilation

3. **Q**: Does Arrow 56.0.0 introduce any changes to RecordBatch or Array APIs used in interval joins?
   - **Status**: Monitor during compilation and testing

4. **Q**: Should we update rust-version in workspace Cargo.toml?
   - **Status**: Check DataFusion 50.x requirements and update if necessary
