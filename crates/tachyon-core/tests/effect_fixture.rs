#![allow(deprecated)]

//! M12 effect fixture gate: §19 crash reconcile through `recover_task`.
//!
//! The fixture writes `effects.state` prepared→committed around a fake
//! remote (table barriers, WAL/FULL, no new journal kinds). Production
//! `recover_task` applies the §19 matrix: Keyed/Queryable/Compensatable
//! stay `prepared` for re-entry; NonIdempotent/Unknown become
//! `unknown_after_crash` and are never blindly replayed.

use std::sync::Arc;

use tachyon_core::{TaskStatus, create_task, recover_task};
use tachyon_store::StoreWriter;
use tachyon_types::{SessionId, TaskId, WorkspaceId};

async fn task_with_store(tag: &str) -> (Arc<StoreWriter>, std::path::PathBuf, TaskId) {
    let dir =
        std::env::temp_dir().join(format!("tachyon-m12-effect-{tag}-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(StoreWriter::open(&dir).await.unwrap());
    let session = SessionId::generate();
    store.create_session(&session.to_string()).await.unwrap();
    let task = create_task(
        session,
        WorkspaceId::generate(),
        "effect fixture".to_owned(),
        store.clone(),
    )
    .await
    .unwrap();
    (store, dir, task.task_id())
}

#[tokio::test]
async fn effect_fixture_gates_non_idempotent_prepared_marks_unknown_after_crash() {
    let (store, dir, task_id) = task_with_store("nonid").await;
    store
        .insert_effect_prepared(
            "eff-nonid",
            &task_id.to_string(),
            "DestructiveExternalMutation",
            "NonIdempotent",
        )
        .await
        .unwrap();

    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let state = handle.get_state().await.unwrap();
    assert_eq!(
        state.status,
        TaskStatus::Created,
        "pre-crash status restored after recover"
    );

    let row = store.load_effect("eff-nonid").await.unwrap().unwrap();
    assert_eq!(
        row.state, "unknown_after_crash",
        "§19: NonIdempotent prepared → UnknownAfterCrash, never replayed"
    );

    handle.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

/// Fail-safe: an unrecognized idempotency class must not stay
/// retry-eligible — §19 Unknown is the default for anything not
/// explicitly safe to retry.
#[tokio::test]
async fn effect_fixture_gates_unrecognized_idempotency_fails_safe_to_unknown() {
    let (store, dir, task_id) = task_with_store("badclass").await;
    store
        .insert_effect_prepared(
            "eff-bad",
            &task_id.to_string(),
            "Privileged",
            "SomethingNew",
        )
        .await
        .unwrap();

    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let row = store.load_effect("eff-bad").await.unwrap().unwrap();
    assert_eq!(
        row.state, "unknown_after_crash",
        "unrecognized class defaults to UnknownAfterCrash"
    );

    handle.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn effect_fixture_gates_unknown_idempotency_marks_unknown_after_crash() {
    let (store, dir, task_id) = task_with_store("unknown").await;
    store
        .insert_effect_prepared("eff-unk", &task_id.to_string(), "Privileged", "Unknown")
        .await
        .unwrap();

    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let row = store.load_effect("eff-unk").await.unwrap().unwrap();
    assert_eq!(row.state, "unknown_after_crash");

    handle.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn effect_fixture_gates_keyed_prepared_stays_prepared_for_reentry() {
    let (store, dir, task_id) = task_with_store("keyed").await;
    store
        .insert_effect_prepared(
            "eff-keyed",
            &task_id.to_string(),
            "ReversibleExternalMutation",
            "Keyed",
        )
        .await
        .unwrap();

    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let row = store.load_effect("eff-keyed").await.unwrap().unwrap();
    assert_eq!(
        row.state, "prepared",
        "§19: Keyed stays prepared so re-entry can retry the same key"
    );

    // Re-entry path: commit after restart with the same key (id).
    store
        .commit_effect("eff-keyed", "receipt-after-restart")
        .await
        .unwrap();
    let row = store.load_effect("eff-keyed").await.unwrap().unwrap();
    assert_eq!(row.state, "committed");
    assert_eq!(row.receipt.as_deref(), Some("receipt-after-restart"));

    handle.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn effect_fixture_gates_queryable_prepared_stays_prepared_for_inspect() {
    let (store, dir, task_id) = task_with_store("query").await;
    store
        .insert_effect_prepared(
            "eff-q",
            &task_id.to_string(),
            "ReversibleExternalMutation",
            "Queryable",
        )
        .await
        .unwrap();

    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let row = store.load_effect("eff-q").await.unwrap().unwrap();
    assert_eq!(
        row.state, "prepared",
        "§19: Queryable stays prepared so re-entry can inspect remote"
    );

    handle.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn effect_fixture_gates_committed_effect_is_untouched_by_recovery() {
    let (store, dir, task_id) = task_with_store("committed").await;
    store
        .insert_effect_prepared(
            "eff-c",
            &task_id.to_string(),
            "ReversibleExternalMutation",
            "Keyed",
        )
        .await
        .unwrap();
    store.commit_effect("eff-c", "r1").await.unwrap();

    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let row = store.load_effect("eff-c").await.unwrap().unwrap();
    assert_eq!(row.state, "committed");
    assert_eq!(row.receipt.as_deref(), Some("r1"));

    handle.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn effect_fixture_gates_double_commit_is_typed_error() {
    let (store, dir, task_id) = task_with_store("dbl").await;
    store
        .insert_effect_prepared("eff-d", &task_id.to_string(), "Keyed", "Keyed")
        .await
        .unwrap();
    store.commit_effect("eff-d", "r1").await.unwrap();
    let second = store.commit_effect("eff-d", "r2").await.unwrap_err();
    assert!(
        matches!(second, tachyon_store::StoreError::Corrupt { .. }),
        "double commit must be typed, got {second:?}"
    );

    let _ = task_id;
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}
