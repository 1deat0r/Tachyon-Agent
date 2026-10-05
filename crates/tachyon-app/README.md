# tachyon-app

The `tachyon` CLI client of the Tachyon runtime.

The binary owns argument parsing, configuration, tracing, and output
formatting. It owns no agent decision logic: local subcommands inspect the
environment (`doctor`, `config`); task subcommands talk to the gateway as a
client; `gateway` runs the runtime in the foreground. Bare `tachyon` opens
the TUI only against a running gateway.

## Executed verification

```bash
cargo fmt -p tachyon-app
cargo test -p tachyon-app
cargo clippy -p tachyon-app --all-targets -- -D warnings
```
