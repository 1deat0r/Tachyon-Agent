-- CreateTask idempotency keys (ACP session reconciliation, issue #57
-- ticket 01; ADR-0005 safe task-creation retry). One row per
-- (session_id, "key"): the request fingerprint plus the stored Ok
-- response JSON, written in the SAME transaction as the task row +
-- seq-0 journal event — the key row exists iff the task exists, so a
-- retry after a crash between commit and response replays the stored
-- response instead of minting a duplicate. Additive: new table only,
-- existing rows and columns untouched. No expiry/GC this slice.
CREATE TABLE create_task_idempotency (
    session_id    TEXT    NOT NULL REFERENCES sessions (id),
    "key"         TEXT    NOT NULL,
    fingerprint   TEXT    NOT NULL,
    response_json TEXT    NOT NULL,
    created_at    INTEGER NOT NULL,
    UNIQUE (session_id, "key")
) STRICT;
