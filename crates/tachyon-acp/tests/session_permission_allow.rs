//! Permission bridge ticket 01 at the serve-loop seam: the synthetic
//! request→response round trip through `server::serve` IN PROCESS (the
//! production loop, no child process) plus the Subscribe-replay park.
//!
//! What is pinned here:
//! - the outgoing `tool_call` announcement and `session/request_permission`
//!   frames byte-for-byte against the ACP schema-v1.23.0 shapes, with
//!   exactly the two one-shot options (ADR-0005:48);
//! - the adapter-minted request id comes from the NEGATIVE outbound
//!   counter and its response correlates back to the waiting turn;
//! - an unsolicited, unrelated response frame is ignored safely (it can
//!   never reach a waiter);
//! - a park whose `approval_request` arrives in the Subscribe ack
//!   (replay, not a `GetTask` snapshot) produces the SAME emission, and
//!   `allow_once` drives the turn to `end_turn`.

use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

mod common;
use common::scripted::{
    ApproveAnswer, ReplayRow, SCRIPT_TASK_ID, Script, ScriptedGateway, Step, Subscription,
    agent_payload, status_payload, task_completed, task_status,
};

const WAIT: Duration = Duration::from_secs(30);

/// Any valid UUID string parses as a session id; the fixture answers
/// `GetSession` for whatever the adapter asks.
const SESSION_ID: &str = "01990f9e-1111-7000-8000-000000000000";

/// The approval the scripted park journalled (the exact
/// `StateEvent::ApprovalRequest` payload shape).
const APPROVAL_ID: &str = "01990f9e-5555-7000-8000-000000000000";
const APPROVAL_SUMMARY: &str = "Apply patch to src/lib.rs";

fn approval_payload() -> Value {
    json!({
        "t": "ApprovalRequest",
        "v": {"request": {
            "id": APPROVAL_ID,
            "capability": "mutation.patch",
            "scope": "workspace",
            "operation_hash": "abc123",
            "summary": APPROVAL_SUMMARY,
        }},
    })
}

/// The golden `tool_call` announcement, byte-pinned (ACP `ToolCall`
/// requires `toolCallId` + `title`).
fn golden_tool_call() -> String {
    format!(
        r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"{SESSION_ID}","update":{{"sessionUpdate":"tool_call","title":"{APPROVAL_SUMMARY}","toolCallId":"{APPROVAL_ID}"}}}}}}"#
    )
}

/// The golden `session/request_permission` frame, byte-pinned (ACP
/// `RequestPermissionRequest`: `sessionId` + `toolCall` + `options`
/// with EXACTLY `allow_once` and `reject_once`; the id is the first
/// adapter-minted outbound id, −1 from the NEGATIVE counter).
fn golden_request() -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":-1,"method":"session/request_permission","params":{{"options":[{{"kind":"allow_once","name":"Allow once","optionId":"allow_once"}},{{"kind":"reject_once","name":"Reject once","optionId":"reject_once"}}],"sessionId":"{SESSION_ID}","toolCall":{{"title":"{APPROVAL_SUMMARY}","toolCallId":"{APPROVAL_ID}"}}}}}}"#
    )
}

/// The golden `tool_call_update completed` frame, byte-pinned (ACP
/// `ToolCallUpdate`: `toolCallId` + a legal `status`).
fn golden_tool_call_completed() -> String {
    format!(
        r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"{SESSION_ID}","update":{{"sessionUpdate":"tool_call_update","status":"completed","toolCallId":"{APPROVAL_ID}"}}}}}}"#
    )
}

fn prompt_line(id: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/prompt","params":{{"sessionId":"{SESSION_ID}","prompt":[{{"type":"text","text":"Permission round trip."}}]}}}}"#
    )
}

/// One in-process ACP client driving [`tachyon_acp::server::serve`]
/// over a duplex: the production loop exactly as the binary runs it,
/// minus the process boundary.
struct ServeClient {
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    reader: tokio::io::BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl ServeClient {
    fn spawn(dir: &Path) -> Self {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let (server_reader, server_writer) = tokio::io::split(server_io);
        let task = tokio::spawn(tachyon_acp::server::serve(
            server_reader,
            server_writer,
            tachyon_acp::client::EndpointProbe::new(dir),
            tachyon_acp::client::EndpointConnector::new(dir),
        ));
        let (client_reader, client_writer) = tokio::io::split(client_io);
        Self {
            writer: client_writer,
            reader: tokio::io::BufReader::new(client_reader),
            task,
        }
    }

    async fn send(&mut self, line: &str) {
        self.writer
            .write_all(format!("{line}\n").as_bytes())
            .await
            .expect("write to serve");
        self.writer.flush().await.expect("flush to serve");
    }

    async fn read_line(&mut self, label: &str) -> String {
        let mut line = String::new();
        let read = tokio::time::timeout(WAIT, self.reader.read_line(&mut line))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {label}"));
        match read {
            Ok(0) => panic!("serve closed stdout before {label}"),
            Ok(_) => line.trim_end().to_owned(),
            Err(error) => panic!("reading from serve: {error}"),
        }
    }

    /// Reads until the response for `id`; every prior line (notifications
    /// and agent→client requests) is returned in arrival order.
    async fn read_until_response(&mut self, id: Value) -> (Vec<String>, String) {
        let mut prior = Vec::new();
        loop {
            let line = self.read_line("the prompt response").await;
            let frame: Value = serde_json::from_str(&line).expect("stdout is JSON");
            if frame.get("method").is_some() {
                prior.push(line);
                continue;
            }
            assert_eq!(
                frame["id"], id,
                "expected the response for {id}, got: {line}"
            );
            return (prior, line);
        }
    }

    /// Closes the client side entirely (both duplex halves, so `serve`
    /// sees EOF) and waits for it to drain and exit cleanly.
    async fn finish(self) {
        let Self {
            writer,
            reader,
            task,
        } = self;
        drop(writer);
        drop(reader);
        let outcome = tokio::time::timeout(WAIT, task)
            .await
            .expect("serve exits after EOF")
            .expect("serve task joins");
        assert!(outcome.is_ok(), "serve exits cleanly: {outcome:?}");
    }
}

/// Park WITH its ask → the golden pair is emitted through the serve
/// loop, an unsolicited unrelated response is ignored safely, and the
/// client's `allow_once` answer correlates back to the waiting turn:
/// `Approve` lands with the parked identity, the tool call closes
/// `completed`, streaming resumes, the prompt ends `end_turn`.
#[tokio::test]
async fn permission_request_round_trips_through_the_serve_loop() {
    let dir = common::test_dir();
    let script = Script {
        get_tasks: vec![
            task_status("Executing"),
            task_status("WaitingApproval"),
            task_completed("final answer"),
        ],
        subscribes: vec![Subscription {
            replay: vec![],
            post: vec![
                Step::Journal {
                    seq: 1,
                    kind: "status",
                    payload: status_payload("WaitingApproval"),
                },
                Step::Journal {
                    seq: 2,
                    kind: "approval_request",
                    payload: approval_payload(),
                },
            ],
        }],
        approves: vec![ApproveAnswer {
            task: task_status("Executing"),
            post: vec![
                Step::Journal {
                    seq: 3,
                    kind: "approval",
                    payload: json!({
                        "t": "Approval",
                        "v": {"approval": APPROVAL_ID, "granted": true, "reason": ""},
                    }),
                },
                Step::Journal {
                    seq: 4,
                    kind: "status",
                    payload: status_payload("Executing"),
                },
                Step::Journal {
                    seq: 5,
                    kind: "agent_message",
                    payload: agent_payload("resumed after approval"),
                },
            ],
        }],
    };
    let fixture = ScriptedGateway::start(&dir, script);
    let mut client = ServeClient::spawn(&dir);

    client.send(&prompt_line(1)).await;

    // The golden pair, in order: announcement BEFORE request.
    let tool_call_line = client.read_line("the tool_call announcement").await;
    assert_eq!(tool_call_line, golden_tool_call());
    let request_line = client
        .read_line("the session/request_permission frame")
        .await;
    assert_eq!(request_line, golden_request());

    // THE id pin: adapter-minted outbound ids are NEGATIVE (a distinct
    // id space from client-minted ids), and this is the first one.
    let request: Value = serde_json::from_str(&request_line).expect("request parses");
    assert_eq!(request["id"].as_i64(), Some(-1), "first outbound id");
    assert!(
        request["params"]["options"]
            .as_array()
            .is_some_and(|options| options.len() == 2),
        "exactly two options: {request_line}"
    );

    // An unsolicited, unrelated response: routed nowhere, ignored
    // safely — it must never reach the waiting turn.
    client
        .send(r#"{"jsonrpc":"2.0","id":9999,"result":{"bogus":true}}"#)
        .await;

    // The real answer: echo the request's own id, `selected` +
    // `allow_once`.
    client
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":{},"result":{{"outcome":"selected","optionId":"allow_once"}}}}"#,
            request["id"]
        ))
        .await;

    let (updates, response_line) = client.read_until_response(json!(1)).await;
    assert_eq!(
        updates.len(),
        2,
        "tool_call_update then the resumed chunk: {updates:?}"
    );
    assert_eq!(updates[0], golden_tool_call_completed());
    let chunk: Value = serde_json::from_str(&updates[1]).expect("chunk parses");
    assert_eq!(
        chunk["params"]["update"]["content"]["text"], "resumed after approval",
        "streaming resumed after the grant: {}",
        updates[1]
    );
    let response: Value = serde_json::from_str(&response_line).expect("response parses");
    assert_eq!(response["result"]["stopReason"], "end_turn");
    assert_eq!(response["result"]["content"][0]["text"], "final answer");

    // The decision reached the gateway with the parked identity, once.
    assert_eq!(
        fixture.approvals_seen(),
        [(SCRIPT_TASK_ID.to_owned(), APPROVAL_ID.to_owned())]
    );
    assert_eq!(fixture.subscribe_cursors(), [0]);
    assert_eq!(fixture.get_task_calls(), 3);
    fixture.shutdown();

    client.finish().await;
}

/// Replay-ack park: the `approval_request` is already IN the Subscribe
/// ack's replay rows — no `GetTask` snapshot ever read the park — and
/// the emission is byte-identical to the live-frame case. The final
/// `GetTask` count proves it: one fresh read + one post-grant read, no
/// settlement read was needed to see the ask.
#[tokio::test]
async fn replay_ack_park_emits_the_same_request() {
    let dir = common::test_dir();
    let script = Script {
        get_tasks: vec![task_status("Executing"), task_completed("final answer")],
        subscribes: vec![Subscription {
            replay: vec![
                // The park's real journal order, replayed in the ack.
                ReplayRow {
                    seq: 1,
                    kind: "status",
                    payload: status_payload("WaitingApproval"),
                },
                ReplayRow {
                    seq: 2,
                    kind: "approval_request",
                    payload: approval_payload(),
                },
            ],
            post: vec![],
        }],
        approves: vec![ApproveAnswer {
            task: task_status("Executing"),
            post: vec![
                Step::Journal {
                    seq: 3,
                    kind: "status",
                    payload: status_payload("Executing"),
                },
                Step::Journal {
                    seq: 4,
                    kind: "agent_message",
                    payload: agent_payload("resumed after approval"),
                },
            ],
        }],
    };
    let fixture = ScriptedGateway::start(&dir, script);
    let mut client = ServeClient::spawn(&dir);

    client.send(&prompt_line(1)).await;

    // Byte-identical to the live-frame emission — the replay path asks
    // exactly the same question.
    let tool_call_line = client.read_line("the tool_call announcement").await;
    assert_eq!(tool_call_line, golden_tool_call());
    let request_line = client
        .read_line("the session/request_permission frame")
        .await;
    assert_eq!(request_line, golden_request());

    let request: Value = serde_json::from_str(&request_line).expect("request parses");
    client
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":{},"result":{{"outcome":"selected","optionId":"allow_once"}}}}"#,
            request["id"]
        ))
        .await;

    let (updates, response_line) = client.read_until_response(json!(1)).await;
    assert_eq!(updates.len(), 2, "tool_call_update then chunk: {updates:?}");
    assert_eq!(updates[0], golden_tool_call_completed());
    let response: Value = serde_json::from_str(&response_line).expect("response parses");
    assert_eq!(response["result"]["stopReason"], "end_turn");
    assert_eq!(response["result"]["content"][0]["text"], "final answer");

    assert_eq!(
        fixture.approvals_seen(),
        [(SCRIPT_TASK_ID.to_owned(), APPROVAL_ID.to_owned())]
    );
    assert_eq!(fixture.subscribe_cursors(), [0], "one subscribe, no resync");
    // ONE fresh read + ONE post-grant read: the ask itself was seen in
    // the replayed journal, never derived from a GetTask snapshot.
    assert_eq!(
        fixture.get_task_calls(),
        2,
        "the park is journal-driven: no settlement read was needed to ask"
    );
    fixture.shutdown();

    client.finish().await;
}
