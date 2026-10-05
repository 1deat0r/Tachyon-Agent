# tachyon-gateway

Local IPC lifecycle and command dispatch over `tachyon-core` (spec §36–§37).

The gateway is a transport adapter: Unix domain socket, length-prefixed JSON
frames, one supervisor registry. Remote transport is a separate, opt-in,
authenticated surface and stays disabled by default.

## Executed verification

```bash
cargo fmt -p tachyon-gateway
cargo test -p tachyon-gateway
cargo clippy -p tachyon-gateway --all-targets -- -D warnings
```
