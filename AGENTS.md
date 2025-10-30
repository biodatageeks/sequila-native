<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# Repository Guidelines

## Project Structure & Module Organization
- Workspace: Rust 2021, crates in `sequila/`.
- Core library: `sequila/sequila-core` — DataFusion extensions, physical planner, and interval-join optimization.
- CLI: `sequila/sequila-cli` — REPL and file execution for SQL.
- Tests: unit/integration under `sequila/sequila-core/tests` and crate-local tests; benches in `sequila/sequila-core/benches`.
- Utilities/data: `queries/` for sample SQL, `testing/data/` for local datasets, `bin/env.sh` for env helpers.

## Build, Test, and Development Commands
- Build: `cargo build` (release: `cargo build --release`).
- Run CLI: `RUST_LOG=info cargo run -p sequila-cli -- --file queries/q1-coitrees.sql`.
- REPL: `RUST_LOG=info cargo run -p sequila-cli`.
- Tests: `cargo test --workspace` (async tests use Tokio).
- Benchmarks: `RUSTFLAGS="-Ctarget-cpu=native" cargo bench --bench databio_benchmark -- --quick`.

## Coding Style & Naming Conventions
- Formatter: rustfmt via pre-commit. Run `cargo fmt --all` before pushing.
- Lints: `cargo check` runs in pre-commit; keep builds warning-free.
- Naming: crates/modules `snake_case`, types/enums `PascalCase`, functions/vars `snake_case`, constants `SCREAMING_SNAKE_CASE`.
- Indentation: 4 spaces; avoid long lines unless readability benefits.

## Testing Guidelines
- Frameworks: Rust built-in tests, `rstest` for parametrization, `tokio::test` for async.
- Locations: unit tests inline in `src/`; integration tests in `sequila-core/tests/`.
- Conventions: name tests descriptively (e.g., `interval_rule_eq`); include edge cases for join predicates and config options.
- Coverage: no strict target; add tests for new features and bug fixes.

## Commit & Pull Request Guidelines
- Commits: imperative mood, concise title; optionally use prefixes (`feat:`, `fix:`, `perf:`). Reference issues/PRs where relevant.
- PRs: include scope/intent, key changes, how to test (commands/queries), and performance notes if applicable. Add screenshots/log snippets when helpful.
- Keep diffs focused; update README or examples when behavior changes.

## Architecture & Configuration Tips
- Core idea: replace/augment DataFusion planning with `SeQuiLaQueryPlanner` and an interval-join physical optimization rule.
- Useful SQL settings:
  - `SET sequila.prefer_interval_join TO true;`
  - `SET sequila.interval_join_algorithm TO coitrees;`
  - `SET datafusion.optimizer.repartition_joins TO false;`
- Bench data: export `BENCH_DATA_ROOT` to point at local datasets.

