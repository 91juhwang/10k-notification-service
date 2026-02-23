# Microgrid Ingestion + Alert + Control Monorepo

## Summary
- This repo is a full-stack telemetry pipeline for ingesting device readings, generating alerts, and handling control commands.
- The API accepts writes and reads, the worker handles async queue processing, and the web app shows live operational state.
- Data durability is in PostgreSQL, realtime fanout is through Redis pub/sub + SSE, and async decoupling is through SQS.
- It is organized as a monorepo so shared API contracts/types stay consistent across backend and frontend.
- The `10k` goal is sustained throughput (`10,000 messages/min`), not `10,000` concurrent requests.

## Data Flow (High Level)
Entry point summary (full cycle):
- Start stack with `pnpm run dev` (API, worker, web) and traffic with `pnpm run dev:load` (`backend/simulator/src/load-test.ts`).
- Simulator sends telemetry to API `POST /api/v1/telemetry` (`backend/api/src/server.ts` -> `backend/api/src/app.ts`).
- API validates payload and enqueues SQS message (fast `202 accepted` response path).
- Worker (`backend/worker/src/index.ts`) consumes SQS, writes DB rows, and evaluates alert/control lifecycle.
- Worker publishes realtime events to Redis channel.
- API SSE endpoint streams those events to connected clients.
- Web dashboard receives SSE events and updates UI state without refresh.

1. Telemetry ingestion path:
   `device/simulator -> API (/api/v1/telemetry) -> telemetry SQS queue -> worker -> Postgres (devices + telemetry_readings)`
2. Alert + notification path:
   `new telemetry row -> worker evaluates alert_rules -> alerts + notifications tables -> Redis publish -> API SSE stream -> web dashboard`
3. Control command path:
   `web/API client -> API (/api/v1/control/commands) -> control SQS queue -> worker lifecycle update (queued -> processing -> succeeded/failed) -> Postgres + Redis publish -> web updates`
4. Delivery/reliability behavior:
   - Queues are polled with SQS long polling.
   - Messages are deleted only after successful handling (or explicit terminal skip for control records).
   - DLQs receive repeatedly failing messages via redrive policy.

## Fresh Engineer Runbook (Start + Feature Test)
1. Setup:
   - Install Node + pnpm.
   - Copy env values from `.env.example` into your local shell/session.
2. Install and boot:
   - `pnpm install`
   - `pnpm run dev` (starts infra + API + worker + web)
3. Prepare DB:
   - In a second terminal, run `pnpm run db:migrate`
4. Verify basic health:
   - Open `http://localhost:4000/` and confirm API responds.
   - Open `http://localhost:5173/` and confirm dashboard loads.
5. Test telemetry + alerts:
   - Run `pnpm run dev:sim` to send telemetry.
   - In the dashboard, log in (use demo credentials from env) and confirm alerts/notifications appear.
6. Test realtime stream:
   - Keep the dashboard open while simulator sends data.
   - Confirm new notifications/updates appear without refreshing.
7. Test control commands:
   - Log in as `operator` or `admin`.
   - Submit a command from the UI control form.
   - Confirm status transitions through queue lifecycle and settles at succeeded/failed.
8. Optional sustained load test:
   - Run `pnpm run dev:load` and review summary/SLO output.
9. Pre-handoff checks:
   - `pnpm -r run check`
   - `pnpm run openapi:check`
   - `pnpm -r run test`

## Development Contract
- OpenAPI-first development is mandatory.

## Services
- `backend/api`: Express ingestion + query API + SSE gateway.
- `backend/worker`: SQS consumers and alert/control processors.
- `backend/simulator`: device telemetry sender.
- `backend/db`: migrations and DB repositories.
- `frontend/web`: React dashboard.
- `shared`: shared schemas/contracts.

## Minimum Environment
At minimum for local development:
- `PUBLIC_ORIGIN`
- `DATABASE_URL`
- `ENCRYPTION_KEY` (reserved for future encrypted payload support)

Copy `.env.example` values into your local environment as needed.

## Local Infrastructure
`infra/docker-compose.yml` starts:
- PostgreSQL (`localhost:5432`)
- Redis (`localhost:6379`)
- LocalStack with SQS (`localhost:4566`)

Queue bootstrap script `infra/localstack-init/01-create-queues.sh` creates telemetry/control queues plus DLQs with redrive policy.

## Core Commands
- `pnpm run dev`: start infra + API + worker + web.
- `pnpm run dev:sim`: run the telemetry simulator.
- `pnpm run dev:load`: run sustained telemetry load test + SLO check.
- `pnpm run db:migrate`: apply SQL migrations.
- `pnpm run openapi:generate`: generate shared OpenAPI types.
- `pnpm run openapi:check`: fail if generated types are stale.

## Auth + Realtime Notes
- API authentication uses bearer tokens from `POST /api/v1/auth/login`.
- Demo users are configured through env vars: `AUTH_ADMIN_*`, `AUTH_OPERATOR_*`, `AUTH_VIEWER_*`.
- `/api/v1/alerts`, `/api/v1/notifications`, and `/api/v1/stream` require bearer auth.
- `/api/v1/notifications/{id}/ack` requires `operator` or `admin` role.
- Realtime events are published by worker to Redis channel `REDIS_CHANNEL` and streamed by API SSE endpoint.

## OpenAPI-First Workflow
1. Update OpenAPI contracts in:
   - root document: `api-specs/internal/microgrid.openapi.yaml`
   - path modules: `api-specs/internal/paths/*.yaml`
   - schema modules: `api-specs/internal/components/schemas/*.yaml`
   - auth modules: `api-specs/internal/components/securitySchemes/*.yaml`
2. Run `pnpm run openapi:generate`.
3. Implement/adjust API handlers and UI client usage.
4. Ensure `pnpm run openapi:check` passes before commit.

## Adding A New API
1. Define the endpoint first in `api-specs/internal/microgrid.openapi.yaml` with schema and auth requirements.
2. Run `pnpm run openapi:generate`.
3. Verify spec/codegen sync with `pnpm run openapi:check`.
4. Implement route and service logic in `backend/api`, then wire the route in `backend/api/src/app.ts`.
5. If async processing is needed, enqueue from API and implement the consumer in `backend/worker`.
6. Run `pnpm --filter @microgrid/api check` and `pnpm -r run check`.

## Adding DB Changes
1. Generate a migration file with `pnpm run db:generate -- <migration_name>`.
2. Edit the generated SQL under `backend/db/migrations`.
3. Apply migrations locally with `pnpm run db:migrate`.
4. Update DB access code in `backend/db/src`.
5. If API contracts change, update OpenAPI spec and regenerate types.
6. Run `pnpm -r run check`.

## Development Flow
1. Start from contract and schema changes (OpenAPI, then DB migration).
2. Implement backend (`backend/api`, `backend/worker`, `backend/db`) and frontend (`frontend/web`) updates.
3. Validate behavior manually with running services and simulator.
4. Run checks and update `NEXT.md`.
5. Stop for human validation and commit at each phase gate.

## Resilience + SLO Notes

Phase 6 operational details are documented in `docs/phase6-performance-resilience.md`.
