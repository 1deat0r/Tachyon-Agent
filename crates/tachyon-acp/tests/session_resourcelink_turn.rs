//! TASK 01 S3: a prompt carrying `resource_link` blocks resolves them
//! against the pinned root and creates its task with the fenced blocks
//! appended to the objective (serve loop + scripted gateway).

use std::time::Duration;

use serde_json::{Value, json};
use tachyon_protocol::CommandResult;

mod common;
use common::scripted::{Script, ScriptedGateway};
use common::{Adapter, test_dir};

const WAIT: Duration = Duration::from_secs(30);

fn get_session_ok(root: &str) -> CommandResult {
    CommandResult::Ok {
        payload: json!({
            "session_id": "01990f9e-1111-7000-8000-000000000000",
            "workspace_root": root,
            "turns": [],
        }),
    }
}

fn empty_script() -> Script {
    Script {
        get_tasks: vec![],
        subscribes: vec![],
        approves: vec![],
    }
}

/// The resolved block reaches the created task: prompt text plus the
/// fenced provenance block for the file inside the pinned root.
#[tokio::test]
async fn linked_file_embeds_in_the_task_objective() {
    let dir = test_dir();
    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(ws.join("notes.txt"), "key evidence").unwrap();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        empty_script(),
        get_session_ok(ws.display().to_string().as_str()),
    );
    let mut adapter = Adapter::spawn(&dir);

    let uri = format!("file://{}/notes.txt", ws.display());
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{{"sessionId":"01990f9e-1111-7000-8000-000000000000","prompt":[{{"type":"text","text":"summarize"}},{{"type":"resource_link","uri":{}}}]}}}}"#,
            json!(uri),
        ))
        .await;

    // The turn creates exactly one task; its objective carries text +
    // the fenced block (poll: creation follows gateway round trips).
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        if !fixture.create_task_objectives().is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the turn never created its task"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let objectives = fixture.create_task_objectives();
    assert_eq!(objectives.len(), 1, "exactly one task created");
    assert!(
        objectives[0].starts_with("summarize\n[resource "),
        "text first, then the fenced block: {:?}",
        objectives[0]
    );
    assert!(objectives[0].contains("key evidence"));
    assert!(objectives[0].ends_with("\n[/resource]"));

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, _exit_ok) = adapter.finish().await;
}

/// An out-of-root link fails the prompt typed before any task exists.
#[tokio::test]
async fn out_of_root_link_refuses_without_creating() {
    let dir = test_dir();
    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        empty_script(),
        get_session_ok(ws.display().to_string().as_str()),
    );
    let mut adapter = Adapter::spawn(&dir);

    adapter
        .send(r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"01990f9e-1111-7000-8000-000000000000","prompt":[{"type":"text","text":"summarize"},{"type":"resource_link","uri":"file:///etc/hostname"}]}}"#)
        .await;
    let (_, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    let response: Value = serde_json::from_str(&response_line).expect("response is JSON");
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(response["error"]["data"], json!("out_of_root"));
    assert!(
        fixture.create_task_objectives().is_empty(),
        "refused prompts create nothing"
    );

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, _exit_ok) = adapter.finish().await;
}
