-- Phase 2: telemetry ingestion persistence

CREATE TABLE IF NOT EXISTS devices (
  id BIGSERIAL PRIMARY KEY,
  external_device_id TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS telemetry_readings (
  id BIGSERIAL PRIMARY KEY,
  device_id BIGINT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL,
  voltage DOUBLE PRECISION NOT NULL,
  frequency DOUBLE PRECISION NOT NULL,
  power_kw DOUBLE PRECISION NOT NULL,
  ingest_id TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_readings_device_ts
  ON telemetry_readings (device_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_telemetry_readings_ts
  ON telemetry_readings (ts DESC);
