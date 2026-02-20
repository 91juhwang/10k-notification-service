-- Phase 3: alert evaluation + notification pipeline

CREATE TABLE IF NOT EXISTS alert_rules (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  metric TEXT NOT NULL CHECK (metric IN ('voltage', 'frequency', 'power_kw')),
  operator TEXT NOT NULL CHECK (operator IN ('gt', 'gte', 'lt', 'lte')),
  threshold DOUBLE PRECISION NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
  id BIGSERIAL PRIMARY KEY,
  device_id BIGINT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  telemetry_reading_id BIGINT NOT NULL REFERENCES telemetry_readings(id) ON DELETE CASCADE,
  rule_id BIGINT NOT NULL REFERENCES alert_rules(id) ON DELETE RESTRICT,
  severity TEXT NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
  message TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'ACKED', 'RESOLVED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (rule_id, telemetry_reading_id)
);

CREATE TABLE IF NOT EXISTS notifications (
  id BIGSERIAL PRIMARY KEY,
  alert_id BIGINT REFERENCES alerts(id) ON DELETE SET NULL,
  source_ingest_id TEXT NOT NULL,
  type TEXT NOT NULL,
  message TEXT NOT NULL,
  is_read BOOLEAN NOT NULL DEFAULT false,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (alert_id)
);

CREATE INDEX IF NOT EXISTS idx_alert_rules_enabled
  ON alert_rules (enabled);

CREATE INDEX IF NOT EXISTS idx_alerts_device_created
  ON alerts (device_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_status_created
  ON alerts (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_notifications_is_read_created
  ON notifications (is_read, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_notifications_source_ingest
  ON notifications (source_ingest_id);

INSERT INTO alert_rules (name, metric, operator, threshold, severity, enabled)
VALUES
  ('voltage_high_default', 'voltage', 'gt', 250, 'HIGH', true),
  ('voltage_low_default', 'voltage', 'lt', 200, 'MEDIUM', true),
  ('frequency_high_default', 'frequency', 'gt', 61, 'MEDIUM', true),
  ('frequency_low_default', 'frequency', 'lt', 59, 'MEDIUM', true),
  ('power_high_default', 'power_kw', 'gt', 120, 'HIGH', true)
ON CONFLICT (name) DO NOTHING;
