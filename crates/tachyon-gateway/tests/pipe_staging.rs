#![cfg(windows)]
//! Windows pipe-staging regression for #35.
//!
//! `Listener::bind` stages exactly one listening pipe instance and the next
//! one is created only inside `Listener::accept`. Between a client consuming
//! that instance and the accept loop staging its replacement, every other
//! connect sees `ERROR_PIPE_BUSY` ("All pipe instances are busy"). Real
//! clients hit this whenever they connect before the accept loop has had a
//! turn — the gateway published its endpoint first and then recovered — so
//! `transport::connect` must wait that window out instead of surfacing the
//! raw OS error to the caller.
use std::path::PathBuf;
use std::time::Duration;

use tachyon_gateway::transport::{Listener, connect};

#[tokio::test]
async fn connect_waits_out_the_instance_staging_window() {
    // A unique parent directory per run: the pipe name is a hash of it, so
    // parallel gateways on this machine never share an instance chain.
    let address: PathBuf = std::env::temp_dir()
        .join(format!("tachyon-pipe-staging-{}", uuid::Uuid::now_v7()))
        .join("gateway.sock");
    let listener = Listener::bind(&address).expect("bind the pipe listener");

    // The first client consumes the single listening instance while no
    // accept loop is running, so no replacement instance exists yet.
    let _first = connect(&listener.local_address())
        .await
        .expect("first connect");

    // A second client must not fail fast with ERROR_PIPE_BUSY: it waits,
    // bounded, for the accept loop to stage the replacement. Returning
    // here is the defect — kill_restart.rs:119 panicked on exactly this.
    let waiting_address = listener.local_address();
    let mut waiter = tokio::spawn(async move { connect(&waiting_address).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut waiter)
            .await
            .is_err(),
        "connect returned before the accept loop staged an instance"
    );

    // The accept hands off the consumed instance and stages its
    // replacement *before* waiting for its own client, which is what
    // unblocks the waiter above.
    let _accepted = listener.accept().await.expect("stage the replacement");

    let _second = tokio::time::timeout(Duration::from_secs(5), &mut waiter)
        .await
        .expect("the waiting connect never resolved")
        .expect("the connect task panicked")
        .expect("connect waited out ERROR_PIPE_BUSY");
}
