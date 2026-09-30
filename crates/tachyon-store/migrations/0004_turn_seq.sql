-- Per-session monotonic turn sequence (ADR-0005 gateway/store ordered
-- replay, ACP slice a, ticket 02). Every task row carries the turn it
-- occupies in its session; the UNIQUE index makes a double assignment
-- impossible even under racing creation.
ALTER TABLE tasks ADD COLUMN turn_seq INTEGER NOT NULL DEFAULT 0;

-- Legacy rows are seeded deterministically by creation order: created_at
-- first, task id as the tiebreak for same-instant rows — never
-- wall-clock order alone. The index is created only after the backfill
-- so distinct ranks exist before uniqueness is enforced.
UPDATE tasks
SET turn_seq = (
    SELECT ranked.rn
    FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY session_id
                   ORDER BY created_at, id
               ) AS rn
        FROM tasks
    ) AS ranked
    WHERE ranked.id = tasks.id
);

CREATE UNIQUE INDEX tasks_by_session_turn ON tasks (session_id, turn_seq);
