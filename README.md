# Microgrid Ingestion + Alert + Control Monorepo

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

## Core Commands
- `pnpm run dev`: start infra + API + worker + web.
- `pnpm run dev:sim`: run the telemetry simulator.
- `pnpm run db:migrate`: apply SQL migrations.
- `pnpm run openapi:generate`: generate shared OpenAPI types.
- `pnpm run openapi:check`: fail if generated types are stale.

## Runbook
1. Copy `.env.example` values into your local environment.
2. Install dependencies with `pnpm install`.
3. Start the stack with `pnpm run dev`.
4. Optionally run telemetry simulation with `pnpm run dev:sim`.
5. Run `pnpm -r run check` before creating a commit.
6. Update phase status in `NEXT.md` and stop at the current gate for human validation and commit.

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

## Gated Delivery
Delivery is phased and controlled via `NEXT.md`:
1. Implement one phase.
2. Stop at gate.
3. Human validates.
4. Human commits.
5. Continue to next phase.
