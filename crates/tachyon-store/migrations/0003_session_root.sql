-- Bind an optional durable Session root (ADR-0005 gateway/store session
-- identity, ACP slice a). NULL marks a legacy session created without a
-- root; existing rows upgrade untouched.
ALTER TABLE sessions ADD COLUMN workspace_root TEXT;
