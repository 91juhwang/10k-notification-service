# Phase 6: Performance + Resilience

## Queue topology

- Main telemetry queue: `microgrid-telemetry`
- Telemetry DLQ: `microgrid-telemetry-dlq`
- Main control queue: `microgrid-control.fifo`
- Control DLQ: `microgrid-control-dlq.fifo`

LocalStack init config applies redrive with `maxReceiveCount=5` for both main queues.

## Retry + DLQ behavior

- Worker deletes SQS messages only after successful processing (or explicit terminal skip).
- If processing throws, the message is not deleted. SQS retries after visibility timeout.
- Malformed telemetry/control payloads are intentionally not deleted; they retry and then move to DLQ via redrive policy.
- Control messages for missing/terminal command records are deleted intentionally to avoid useless retries.

## Load test command

Run sustained ingest load:

```bash
pnpm run dev:load
```

Key env controls:

- `SIM_LOAD_RATE_PER_MINUTE` (default `10000`)
- `SIM_LOAD_DURATION_SECONDS` (default `300`)
- `SIM_LOAD_MAX_IN_FLIGHT` (default `300`)
- `SIM_SLO_MIN_SUCCESS_RATE` (default `0.99`)
- `SIM_SLO_MAX_P95_LATENCY_MS` (default `1500`)

The load script exits non-zero if SLO checks fail.

## Basic SLO metrics

Worker logs `[worker] metrics` snapshots every `WORKER_METRICS_LOG_INTERVAL_SECONDS` (default `30`).

The snapshot includes:

- Telemetry counters (`processed`, `duplicate`, `malformed`, `failed`)
- Control counters (`processed`, `malformed`, `failed`, `missing`, `terminal`)
- End-to-end latency summaries (`avgMs`, `p50Ms`, `p95Ms`) for telemetry and control paths
- Queue depth status for main queues and DLQs (`visible`, `inFlight`)
