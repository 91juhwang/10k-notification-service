-- Phase 5: control command workflow

CREATE TABLE IF NOT EXISTS control_commands (
  id TEXT PRIMARY KEY,
  external_device_id TEXT NOT NULL,
  command_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued', 'processing', 'succeeded', 'failed', 'cancelled')),
  idempotency_key TEXT NOT NULL UNIQUE,
  requested_by TEXT,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_control_commands_status_updated
  ON control_commands (status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_control_commands_device_created
  ON control_commands (external_device_id, created_at DESC);
