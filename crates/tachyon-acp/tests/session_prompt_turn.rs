//! Ticket 02 acceptance: the full `session/prompt` turn against a live
//! test gateway — `session/update` `agent_message_chunk` frames arrive
//! BEFORE the final response, the final response is byte-exactly the
//! documented shape with `stopReason: end_turn` and the turn's final
//! agent text, and non-message journals never become fake chunks.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tachyon_models::fake::FakeModelProvider;
use tachyon_types::ProviderId;

mod common;
use common::{
    Adapter, AdapterFrame, armed_runtime, cargo_package, classify, gw_ok, parse_frame,
    patch_response, patched_content, test_dir,
};

/// Generous bound: the turn runs a real `cargo test` verification tail.
const TURN_WAIT: Duration = Duration::from_secs(180);

fn prompt_line(id: u64, session_id: &str, text: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/prompt","params":{{"sessionId":"{session_id}","prompt":[{{"type":"text","text":{text}}}]}}}}"#,
        text = serde_json::to_string(text).expect("text serializes")
    )
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one narrative: reject → stream → pin → journal cross-check
async fn prompt_turn_streams_chunks_then_answers_end_turn() {
    let dir = test_dir();
    let workspace = cargo_package(&dir.join("ws"));
    let original = std::fs::read(workspace.join("src/lib.rs")).expect("fixture bytes");

    // One scripted patch: evidence → model → mutation → `cargo test`
    // verification → durable Completed (the g5 shape, miniaturized).
    let fake = FakeModelProvider::new(ProviderId("bench-acp-turn".into()));
    fake.push_response(patch_response(
        "src/lib.rs",
        &original,
        &patched_content(&String::from_utf8_lossy(&original)),
    ));
    let gateway = tachyon_gateway::start_with(&dir, armed_runtime(Arc::new(fake)))
        .await
        .expect("gateway starts");
    let socket = gateway.address().to_owned();

    let mut adapter = Adapter::spawn(&dir);
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"session/new","params":{{"cwd":{}}}}}"#,
            json!(workspace.display().to_string())
        ))
        .await;
    let (created, created_line) = adapter.read_until_response(json!(1), TURN_WAIT).await;
    assert!(
        created.is_empty(),
        "session/new streams nothing: {created:?}"
    );
    let created_frame = parse_frame(&created_line);
    let session_id = created_frame["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();

    adapter
        .send(&prompt_line(2, &session_id, "Tidy up the addition helper."))
        .await;
    let (updates, response_line) = adapter.read_until_response(json!(2), TURN_WAIT).await;

    // ≥1 chunk arrived BEFORE the final response, and every one is an
    // `agent_message_chunk` Text frame for THIS session — a stage,
    // status, or verification journal can never surface as a chunk.
    assert!(
        !updates.is_empty(),
        "the turn must stream at least one chunk"
    );
    let mut chunk_texts: Vec<String> = Vec::new();
    for line in &updates {
        let frame = classify(line);
        let AdapterFrame::Notification(frame) = frame else {
            panic!("streamed frame is not a notification: {line}");
        };
        assert_eq!(frame["method"], "session/update", "line: {line}");
        assert_eq!(
            frame["params"]["sessionId"],
            session_id.as_str(),
            "line: {line}"
        );
        let update = &frame["params"]["update"];
        assert_eq!(
            update["sessionUpdate"], "agent_message_chunk",
            "only agent messages map to chunks: {line}"
        );
        assert_eq!(update["content"]["type"], "text", "line: {line}");
        chunk_texts.push(update["content"]["text"].as_str().expect("text").to_owned());
    }

    // Final response: byte-exact golden (key order + field set) built
    // from the SAME text the chunks carried.
    let last_chunk = chunk_texts.last().expect("at least one chunk");
    let result = json!({
        "stopReason": "end_turn",
        "content": [{"type": "text", "text": last_chunk}],
    });
    let expected_line = format!(r#"{{"jsonrpc":"2.0","id":2,"result":{result}}}"#);
    assert_eq!(
        response_line, expected_line,
        "the prompt response is pinned byte-for-byte"
    );
    let response = parse_frame(&response_line);
    assert_eq!(response["result"]["stopReason"], "end_turn");
    let keys: Vec<&str> = response["result"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(keys, ["content", "stopReason"], "exact response shape");
    let content = &response["result"]["content"];
    assert_eq!(content.as_array().unwrap().len(), 1);
    assert_eq!(content[0]["type"], "text");
    assert_eq!(
        content[0]["text"].as_str(),
        Some(last_chunk.as_str()),
        "the response content IS the turn's final agent text (the \
         conversation tail), not a re-derived string"
    );
    assert!(
        !content[0]["text"].as_str().unwrap().is_empty(),
        "a completed turn answered something"
    );

    // Journal cross-check from the gateway's own durable store: every
    // `agent_message` journalled during the turn produced exactly one
    // chunk (all forwarded, none invented), while the non-message kinds
    // present produced none.
    let session = gw_ok(
        &socket,
        tachyon_protocol::Command::GetSession {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(session["turns"].as_array().unwrap().len(), 1, "one turn");
    let task_id = session["turns"][0]["task_id"].as_str().unwrap().to_owned();
    let journal = gateway
        .store()
        .load_events_since(&task_id, -1)
        .await
        .expect("journal reads");
    let kinds: Vec<&str> = journal.iter().map(|row| row.kind.as_str()).collect();
    assert!(
        kinds.contains(&"stage") && kinds.contains(&"verification_finished"),
        "the turn really ran: {kinds:?}"
    );
    let agent_messages = kinds
        .iter()
        .filter(|kind| **kind == "agent_message")
        .count();
    assert!(
        agent_messages >= 1,
        "the scripted model answered at least once: {kinds:?}"
    );
    assert_eq!(
        updates.len(),
        agent_messages,
        "one chunk per agent_message journal, zero from every other kind: {kinds:?}"
    );

    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok, "clean exit after a completed turn");
    gateway.shutdown().await;
}

/// Rejection paths: empty/whitespace text and non-text prompt blocks
/// are typed refusals, and — proven through the gateway's own session
/// history — no `CreateTask` ever happened. An unknown sessionId is a
/// typed gateway refusal, not a hang or a new session.
#[tokio::test]
async fn rejected_prompts_never_reach_create_task() {
    let dir = test_dir();
    let workspace = dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let gateway = tachyon_gateway::start(&dir).await.expect("gateway starts");
    let socket = gateway.address().to_owned();

    let mut adapter = Adapter::spawn(&dir);
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"session/new","params":{{"cwd":{}}}}}"#,
            json!(workspace.display().to_string())
        ))
        .await;
    let (_, created_line) = adapter
        .read_until_response(json!(1), Duration::from_secs(30))
        .await;
    let session_id = parse_frame(&created_line)["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();

    // Empty text, whitespace-only text, non-text content: all typed
    // refusals before any gateway command.
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{{"sessionId":"{session_id}","prompt":[]}}}}"#
        ))
        .await;
    adapter.send(&prompt_line(3, &session_id, "   \n\t ")).await;
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"session/prompt","params":{{"sessionId":"{session_id}","prompt":[{{"type":"resource_link","uri":"file:///tmp/a"}}]}}}}"#
        ))
        .await;
    adapter
        .send(
            r#"{"jsonrpc":"2.0","id":5,"method":"session/prompt","params":{"sessionId":"01990f9e-ffff-7000-8000-000000000000","prompt":[{"type":"text","text":"hello"}]}}"#,
        )
        .await;
    adapter.close_stdin();
    let (replies, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
    // The session/new reply (id 1) was already consumed interactively;
    // these are the four prompt replies that followed.
    assert_eq!(
        replies.len(),
        4,
        "one frame per rejected prompt: {replies:?}"
    );

    let empty = parse_frame(&replies[0]);
    assert_eq!(empty["id"], 2);
    assert_eq!(empty["error"]["code"], -32602);
    assert_eq!(empty["error"]["data"], json!("empty_prompt"));

    let whitespace = parse_frame(&replies[1]);
    assert_eq!(whitespace["id"], 3);
    assert_eq!(whitespace["error"]["code"], -32602);
    assert_eq!(whitespace["error"]["data"], json!("empty_prompt"));

    let resource = parse_frame(&replies[2]);
    assert_eq!(resource["id"], 4);
    assert_eq!(resource["error"]["code"], -32602);
    assert_eq!(
        resource["error"]["data"],
        json!("unsupported_prompt_content")
    );

    let unknown = parse_frame(&replies[3]);
    assert_eq!(unknown["id"], 5);
    assert_eq!(unknown["error"]["code"], -32002, "reply: {}", replies[3]);
    assert_eq!(unknown["error"]["data"], json!("unknown_session"));

    // THE proof for "no CreateTask": the session's durable turn history
    // is still empty after every rejected prompt.
    let session = gw_ok(
        &socket,
        tachyon_protocol::Command::GetSession {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(
        session["turns"].as_array().map(Vec::len),
        Some(0),
        "a rejected prompt must never create a task: {session}"
    );

    gateway.shutdown().await;
}
