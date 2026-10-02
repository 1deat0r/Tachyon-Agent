//! `tachyon-acp` — the ACP v1 stdio adapter (ADR-0005).
//!
//! stdout carries only ACP frames: the frame writer below is the sole
//! writer of stdout, and every log line goes to stderr. The adapter
//! never starts the gateway — it resolves the data dir the same way
//! `tachyon-app` does (`TACHYON_DATA_DIR`, else the config file's
//! `data_dir`, else the platform data dir), reads
//! `<data_dir>/gateway.json`, and fails every request with one
//! actionable error when the gateway is down.

use std::process::ExitCode;

use tachyon_acp::client::{EndpointConnector, EndpointProbe};
use tachyon_acp::config::resolve_data_dir;
use tachyon_acp::server::serve;
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    init_tracing();
    let data_dir = resolve_data_dir();
    tracing::info!(data_dir = %data_dir.display(), "tachyon-acp stdio loop starting");
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("tachyon-acp: starting async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(serve(
        tokio::io::stdin(),
        tokio::io::stdout(),
        EndpointProbe::new(data_dir.clone()),
        EndpointConnector::new(data_dir),
    ));
    match result {
        Ok(()) => {
            tracing::info!("stdin closed; exiting");
            ExitCode::SUCCESS
        }
        Err(error) => {
            tracing::error!(error = %error, "stdio loop failed");
            ExitCode::FAILURE
        }
    }
}

/// The tracing subscriber: everything to stderr, `RUST_LOG` overridable,
/// default level `info` (stdout stays frame-pure).
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}
