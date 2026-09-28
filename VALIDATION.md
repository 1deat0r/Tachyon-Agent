# Validation

Use [`cargo verify`](docs/DEVELOPMENT_WORKFLOW.md) for the canonical local
quality gate. It runs formatting, workspace compilation, tests, and strict
Clippy. `cargo verify fast` is for quick iteration; `cargo verify full` adds
the scheduled security, recovery, fixture, matrix, and performance checks.
CI also uses `cargo verify platform` to run workspace tests on Windows and
macOS without repeating formatting and Clippy there.

The Rust toolchain is pinned in `rust-toolchain.toml`. Bootstrap scripts install
that toolchain when needed and then call the same canonical verification entry
point.
