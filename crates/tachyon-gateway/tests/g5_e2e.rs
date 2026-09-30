//! G5 gate (plan item 6): run end to end through the PROTOCOL —
//! scratch COPY of `fixtures/auth-refresh` (the checked-in fixture must
//! end byte-identical; this test asserts full directory content-map
//! equality on the ORIGINAL root, the documented equivalent of the
//! plan's `git diff --exit-code` since running git inside tests is
//! awkward), gateway started with a fake/scripted provider armed
//! EXACTLY like the `bench_matrix` example, `StartRun` with default
//! acceptance detection (Cargo workspace), the shared driver driven to
//! durable `Completed`, and a LIVE subscriber observing `stage`,
//! `changed_files`, `agent_message` and `verification_finished`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tachyon_gateway::transport::{Stream, connect};
use tachyon_gateway::{FAKE_PROVIDER_LABEL, GatewayRuntime, start_with};
use tachyon_models::ModelProvider;
use tachyon_models::fake::{FakeModelProvider, FakeResponse};
use tachyon_mutation::blake3_hex;
use tachyon_protocol::{
    Command, PROTOCOL_VERSION, RequestEnvelope, ResponseEnvelope, ServerFrame, decode_server_frame,
    encode_frame,
};
use tachyon_tools::credential::CredentialBroker;
use tachyon_types::{EventId, ProviderId, TaskId};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

mod common;
use common::{ok, test_dir};

const TARGET: &str = "auth-session/src/session.rs";
const BROKEN_BODY: &str = "    pub fn complete_refresh(&mut self, ticket: RefreshTicket, token: impl Into<String>) {\n        self.active_generation = ticket.generation;\n        self.token = token.into();\n    }";
const FIXED_BODY: &str = "    pub fn complete_refresh(&mut self, ticket: RefreshTicket, token: impl Into<String>) {\n        if ticket.generation > self.active_generation {\n            self.active_generation = ticket.generation;\n            self.token = token.into();\n        }\n    }";
const OBJECTIVE: &str =
    "Find why authentication occasionally fails after token refresh and fix it.";

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/auth-refresh")
}

fn copy_dir(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &to)?;
        } else {
            std::fs::copy(entry.path(), to)?;
        }
    }
    Ok(())
}

/// Full directory content map: relative path -> bytes for every file.
/// Equality before/after is strictly stronger than `git diff --exit-code`
/// (it also catches untracked additions) — the plan's documented
/// equivalent for an in-test fixture-integrity check.
fn snapshot_all(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(base: &Path, dir: &Path, map: &mut BTreeMap<String, Vec<u8>>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(base, &path, map);
            } else {
                let rel = path
                    .strip_prefix(base)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/");
                map.insert(rel, std::fs::read(&path).unwrap_or_default());
            }
        }
    }
    let mut map = BTreeMap::new();
    walk(root, root, &mut map);
    map
}

async fn read_server_frame(stream: &mut Stream) -> Option<ServerFrame> {
    let mut prefix = [0_u8; 4];
    if let Err(error) = stream.read_exact(&mut prefix).await {
        eprintln!("[g5] read prefix failed: {error}");
        return None;
    }
    let len = u32::from_le_bytes(prefix) as usize;
    let mut body = vec![0_u8; len];
    if let Err(error) = stream.read_exact(&mut body).await {
        eprintln!("[g5] read body failed: {error}");
        return None;
    }
    // `decode_frame` reads the length prefix from the head of the buffer.
    let mut framed = prefix.to_vec();
    framed.extend_from_slice(&body);
    match decode_server_frame(&framed) {
        Ok((frame, _)) => Some(frame),
        Err(error) => {
            eprintln!(
                "[g5] decode failed: {error}; body: {}",
                String::from_utf8_lossy(&body)
            );
            None
        }
    }
}

/// Mirrors `TaskStatus::is_terminal` (core): only Completed, Failed and
/// Cancelled settle a run; everything else — including `Verifying` —
/// keeps the poll alive.
fn is_terminal(status: &str) -> bool {
    matches!(status, "Completed" | "Failed" | "Cancelled")
}

#[tokio::test]
// The gate is one scripted end-to-end story; splitting it would hide
// the single narrative the plan's G5 clause asks for.
#[allow(clippy::too_many_lines)]
async fn g5_run_reaches_durable_completed_with_live_events_and_untouched_fixture() {
    let fixture = fixture_root();
    let original_before = snapshot_all(&fixture);
    assert!(!original_before.is_empty(), "fixture found at {fixture:?}");

    // Fresh scratch COPY; the checked-in fixture is never patched.
    let dir = test_dir();
    let ws = dir.join("ws");
    copy_dir(&fixture, &ws).expect("fixture copy");

    // Scripted test/replay provider, armed exactly like the example:
    // one fixed transformation of the target bytes, queued pre-run.
    // Normalize CRLF→LF: Windows checkouts and the BROKEN/FIXED consts
    // are LF; without this, contains(BROKEN_BODY) fails on windows-latest.
    let broken = std::fs::read(ws.join(TARGET)).expect("target exists");
    let broken_text = String::from_utf8(broken.clone()).expect("utf8");
    let broken_text = broken_text.replace("\r\n", "\n");
    assert!(
        broken_text.contains(BROKEN_BODY),
        "fixture no longer contains the known stale-refresh body"
    );
    let fixed_text = broken_text.replacen(BROKEN_BODY, FIXED_BODY, 1);
    let script = serde_json::json!({
        "decision": "propose_execution",
        "operations": [{
            "capability": "mutation.patch",
            "reason": "Refresh the stale token before returning it",
            "args": {
                "path": TARGET,
                "base_hash": blake3_hex(&broken),
                "new_content": fixed_text,
            }
        }]
    });
    let fake = FakeModelProvider::new(ProviderId("bench-script".into()));
    fake.push_response(FakeResponse {
        text: script.to_string(),
        decision: serde_json::from_value(script.clone()).expect("typed proposal fixture"),
        input_tokens: 0,
        output_tokens: 0,
    });
    let runtime = GatewayRuntime {
        provider: Some(Arc::new(fake) as Arc<dyn ModelProvider>),
        label: FAKE_PROVIDER_LABEL.to_owned(),
        model: "scripted-replay-1".to_owned(),
        redactor: CredentialBroker::default(),
    };

    let gateway = start_with(&dir, runtime).await.expect("gateway starts");
    let socket = gateway.address().to_owned();

    let session = ok(
        &socket,
        Command::CreateSession {
            workspace_root: None,
        },
    )
    .await;
    let session_id = session["session_id"].as_str().unwrap().to_owned();
    let task = ok(
        &socket,
        Command::CreateTask {
            session_id: session_id.parse().unwrap(),
            objective: OBJECTIVE.to_owned(),
        },
    )
    .await;
    let task_id: TaskId = task["task_id"].as_str().unwrap().parse().unwrap();

    // LIVE subscriber, attached BEFORE StartRun: full replay from seq 0.
    let mut sub = connect(&socket).await.expect("subscriber connects");
    let subscribe = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: EventId::generate(),
        command: Command::Subscribe {
            task_id,
            after_seq: -1,
        },
    };
    sub.write_all(&encode_frame(&subscribe).unwrap())
        .await
        .unwrap();
    let ack = read_server_frame(&mut sub).await.expect("ack frame");
    let ServerFrame::Response(ResponseEnvelope {
        result: ack_result, ..
    }) = ack
    else {
        panic!("first frame must be the subscribe ack");
    };
    let tachyon_protocol::CommandResult::Ok {
        payload: ack_payload,
    } = ack_result
    else {
        panic!("subscribe must be acknowledged: {ack_result:?}");
    };
    assert_eq!(ack_payload["subscribed"], true, "ack: {ack_payload}");

    // Reader task: consumes event frames into a shared kind list.
    let seen: Arc<tokio::sync::Mutex<Vec<(String, String)>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let reader_seen = seen.clone();
    let reader = tokio::spawn(async move {
        while let Some(frame) = read_server_frame(&mut sub).await {
            if let ServerFrame::Event(envelope) = frame
                && let tachyon_protocol::GatewayEvent::Journal { kind, payload, .. } =
                    envelope.event
            {
                reader_seen.lock().await.push((kind, payload.to_string()));
            }
        }
    });

    // StartRun through the protocol; default acceptance detection
    // (Cargo workspace -> cargo/locked default contract, item 9).
    let started = ok(
        &socket,
        Command::StartRun {
            task_id,
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(started["provider"], FAKE_PROVIDER_LABEL, "label echoed");

    // Wait for the run to reach a durable terminal state (or record a
    // drive failure, which makes the assertion diagnostic instead of a
    // blind timeout).
    let mut status;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(240);
    loop {
        let state = ok(&socket, Command::GetTask { task_id }).await;
        status = state["task"]["status"].as_str().unwrap_or("").to_owned();
        if is_terminal(&status) {
            break;
        }
        if gateway.task_failure(task_id).await.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "run never settled; status={status} failure={:?}",
            gateway.task_failure(task_id).await
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    let original_after = snapshot_all(&fixture);

    // The live subscriber must have observed the four plan events.
    let mut grace = 0;
    loop {
        let kinds: Vec<String> = {
            let guard = seen.lock().await;
            guard.iter().map(|(kind, _)| kind.clone()).collect()
        };
        if [
            "stage",
            "changed_files",
            "agent_message",
            "verification_finished",
        ]
        .iter()
        .all(|want| kinds.contains(&want.to_string()))
            || grace > 50
        {
            break;
        }
        grace += 1;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let collected = seen.lock().await.clone();

    // Durable Completed with no recorded failure.
    let failure = gateway.task_failure(task_id).await;
    assert_eq!(
        status, "Completed",
        "expected durable Completed; failure={failure:?}"
    );

    // Live subscriber observation (the G5 event contract).
    for want in [
        "stage",
        "changed_files",
        "agent_message",
        "verification_finished",
    ] {
        assert!(
            collected.iter().any(|(kind, _)| kind == want),
            "live subscriber never saw {want:?}; saw {:?}",
            collected.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>()
        );
    }
    let changed = collected
        .iter()
        .find(|(kind, _)| kind == "changed_files")
        .expect("changed_files event");
    assert!(
        changed.1.contains("auth-session/src/session.rs"),
        "changed_files must carry the patched target: {}",
        changed.1
    );

    // The journal (what `tachyon trace`/replay reads) carries the same
    // four kinds.
    let journal = gateway
        .store()
        .load_events_since(&task_id.to_string(), -1)
        .await
        .unwrap();
    let journal_kinds: Vec<&str> = journal.iter().map(|row| row.kind.as_str()).collect();
    for want in [
        "stage",
        "changed_files",
        "agent_message",
        "verification_finished",
    ] {
        assert!(
            journal_kinds.contains(&want),
            "journal missing {want}; has {journal_kinds:?}"
        );
    }

    // Checked-in fixture byte-identical (content-map equality).
    assert_eq!(
        original_before, original_after,
        "the checked-in fixture must end byte-identical"
    );

    // The scratch copy DID take the scripted patch (changed_files above).
    let patched = std::fs::read(ws.join(TARGET)).expect("target still exists");
    assert!(
        String::from_utf8_lossy(&patched)
            .replace("\r\n", "\n")
            .contains(FIXED_BODY)
    );

    gateway.shutdown().await;
    let _ = reader.await;
}
