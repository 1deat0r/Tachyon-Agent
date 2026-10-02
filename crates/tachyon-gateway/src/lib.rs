//! Tachyon Gateway.
//!
//! Local IPC lifecycle and command dispatch over the core (spec §36–§37).
//! The gateway is a transport adapter: Unix domain socket, length-prefixed
//! JSON frames, one supervisor registry. Remote transport is a separate,
//! opt-in, authenticated surface and stays disabled.

#![warn(unsafe_code)]

mod endpoint;
mod mcp;
mod query;
mod server;
pub mod transport;

pub use endpoint::{ClaimPaths, EndpointInfo, claim_runtime_dir, read_endpoint_info};
pub use server::{
    FAKE_PROVIDER_LABEL, GatewayError, GatewayRuntime, RunningGateway, start, start_with,
};
