-- Bind protocol-managed effects to the validated execution node that owns
-- the operation. NULL remains valid for legacy M12 fixture rows.
ALTER TABLE effects ADD COLUMN node_id TEXT;

CREATE INDEX effects_by_task_node ON effects (task_id, node_id);
