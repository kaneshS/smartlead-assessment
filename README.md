# Send Worker

TypeScript worker that drains a Postgres `scheduled_emails` backlog through a simulated email provider. Multiple workers can run concurrently without double-sends, with per-mailbox hourly caps, retries, and graceful shutdown.

See [DESIGN.md](./DESIGN.md) for architecture decisions (SKIP LOCKED claiming, idempotency, rate limits, retry policy).

Total time spent: ~4.5 hours.

## Prerequisites

- **Node.js 20+** (Docker image uses Node 22)
- **Docker & Docker Compose** (recommended), **or**
- **Postgres 16** running locally with a database matching `DATABASE_URL` (default: `sendworker` / user `sendworker` / password `sendworker` on port `5432`)

## Quick start (Docker Compose)

One command starts Postgres, runs migrations, seeds ~10k emails, and starts one worker:

```bash
cp .env.example .env
docker compose up --build
```

Compose services (in order): `postgres` → `migrate` → `seed` → `worker`.

Scale workers:

```bash
docker compose up --build --scale worker=5
```

Stop and remove containers (keeps the Postgres volume):

```bash
docker compose down
```

## Local development setup

### Option A — Postgres via Docker, workers on the host

```bash
npm install
cp .env.example .env

# Postgres only
docker compose up -d postgres

# Wait for healthcheck, then schema + seed
npm run migrate
npm run seed
```

### Option B — Fully local Postgres

1. Create a database and user matching `.env.example` (or update `DATABASE_URL`).
2. Run `npm install`, `npm run migrate`, and `npm run seed` as above.

Environment variables are documented in [`.env.example`](./.env.example). Key tunables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `WORKER_BATCH_SIZE` | `10` | Rows claimed per poll |
| `WORKER_POLL_INTERVAL_MS` | `500` | Sleep when queue is empty |
| `WORKER_LEASE_TTL_SECONDS` | `60` | Lease expiry for crash recovery |
| `WORKER_MAX_ATTEMPTS` | `5` | Dead-letter after this many attempts |
| `METRICS_SUMMARY_INTERVAL_MS` | `30000` | Periodic metrics log interval |
| `SEED_MAILBOXES` / `SEED_CAMPAIGNS` / `SEED_EMAILS` / `SEED_DUE_RATIO` | `50` / `10` / `10000` / `0.9` | Seed volume |

## Running workers

### Single worker

```bash
npm run worker
```

Uses `WORKER_ID` from `.env` (default `worker-1`); if unset, falls back to `worker-<pid>`.

### Multiple workers (local)

Run one process per terminal with a **unique** `WORKER_ID`:

```bash
WORKER_ID=worker-1 npm run worker
WORKER_ID=worker-2 npm run worker
WORKER_ID=worker-3 npm run worker
```

### Multiple workers (Docker Compose)

```bash
docker compose up --build --scale worker=5
```

By default each container uses `WORKER_ID=worker` unless you override it in `.env`. For local multi-process runs, distinct IDs are recommended so lease ownership and shutdown logs are attributable.

## Seeding data

```bash
npm run seed
```

Truncates `mailboxes` and `scheduled_emails`, inserts mailboxes (random `hourly_limit` 30–60), and bulk-inserts scheduled emails. Override counts via env vars (see `.env.example`). Docker Compose seed service uses `SEED_MAILBOXES=50`, `SEED_EMAILS=10000`, `SEED_DUE_RATIO=0.9`.

Re-seed at any time; run `npm run migrate` first on a fresh database.

## Running tests

**Prerequisites:** Postgres reachable at `DATABASE_URL` (default matches compose). Tests use the real DB and truncate tables in helpers.

```bash
docker compose up -d postgres
npm run migrate
npm test
```

| Command | Purpose |
|---------|---------|
| `npm test` | Full Vitest suite (`vitest run`) |
| `npm run verify` | Targeted pillar checks (SKIP LOCKED, idempotency, rate cap, retry/dead-letter) |
| `npm run test:watch` | Watch mode |
| `npx vitest run tests/concurrency.test.ts` | Single file (swap path as needed) |
| `npx vitest run tests/claim.test.ts tests/rateLimit.test.ts` | Subset of files |

### Concurrency / no-duplicate test

End-to-end check that parallel workers never double-send. Run this on its own when you want to verify SKIP LOCKED claiming and idempotency under load.

**Prerequisites**

```bash
docker compose up -d postgres
npm run migrate
```

**Run only the concurrency test**

```bash
npx vitest run tests/concurrency.test.ts
```

**What it does:** Truncates test tables, seeds 200 scheduled emails across 5 mailboxes, starts **5 in-process workers** in parallel, waits for the queue to drain, then asserts all 200 emails are sent exactly once (no duplicate `idempotency_key` or `provider_message_id`) and mailbox hourly caps are respected.

**Important:** Do not run other workers (`npm run worker`, Docker Compose `worker` services, etc.) against the same database while this test runs — they would compete for the same rows and can cause false failures.

For the full suite, run `npm test`.

### Test coverage

| File | Coverage |
|------|----------|
| `claim.test.ts` | SKIP LOCKED claiming, lease metadata, parallel disjoint claims, reclaim, retry/schedule gates, max attempts |
| `rateLimit.test.ts` | Per-mailbox rolling-hour caps, other mailboxes unaffected, expired sent rows |
| `provider.test.ts` | Simulated provider outcomes, idempotency dedup, ambiguous + `getStatus`, determinism |
| `retry.test.ts` | Backoff calculation (transient/throttled/cap/jitter), dead-letter threshold |
| `worker.test.ts` | Send flow outcomes → sent/failed/dead, ambiguous recovery, max attempts |
| `shutdown.test.ts` | Graceful shutdown releases leases, stops new claims |
| `concurrency.test.ts` | 5 parallel workers, 200 emails, no duplicates, mailbox caps |
| `integration.test.ts` | End-to-end drain, lease recovery, retry gating, seed smoke |

## Checking logs and observability

### Worker stdout (structured JSON)

Every log line is JSON. Per-email events include `ts`, `event`, `worker_id`, `email_id`, and `mailbox_id` where relevant:

| Event | When |
|-------|------|
| `worker_started` | Worker loop begins |
| `email_sent` | Successful send |
| `email_sent_ambiguous_recovered` | Ambiguous timeout resolved via `getStatus` |
| `email_sent_after_error_status_lookup` | Exception recovered via `getStatus` |
| `email_retry_scheduled` | Transient/throttled → failed with `next_retry_at` |
| `email_dead` / `email_dead_max_attempts` | Hard bounce or max attempts |
| `leases_released_on_shutdown` | Leases cleared on shutdown |
| `worker_stopped` | Clean exit |

### Periodic metrics summary

Every `METRICS_SUMMARY_INTERVAL_MS` (default 30s), each worker emits:

```json
{
  "event": "metrics_summary",
  "worker_id": "worker-1",
  "claims": 120,
  "sent": 95,
  "retried": 8,
  "failed": 12,
  "dead": 3,
  "ambiguousRecovered": 1,
  "uptimeSec": 30.5,
  "emails_per_minute": 187.05
}
```

A final summary is logged on graceful shutdown.

### Docker Compose logs

```bash
docker compose logs -f worker
docker compose logs -f postgres migrate seed
docker compose logs -f          # all services
```

Pipe through `jq` for filtering, e.g. `docker compose logs worker 2>&1 | jq 'select(.event=="metrics_summary")'`.

### Useful SQL queries

Connect to Postgres (`docker compose exec postgres psql -U sendworker -d sendworker`, or your local client):

```sql
-- Outcome counts
SELECT status, COUNT(*) FROM scheduled_emails GROUP BY status ORDER BY status;

-- Sent volume in the last hour (rate-limit verification)
SELECT mailbox_id, COUNT(*) AS sent_last_hour
FROM scheduled_emails
WHERE status = 'sent' AND sent_at > NOW() - INTERVAL '1 hour'
GROUP BY mailbox_id
ORDER BY sent_last_hour DESC;

-- Compare against mailbox caps (should never exceed hourly_limit)
SELECT m.id, m.email_address, m.hourly_limit,
       COUNT(s.id) FILTER (WHERE s.sent_at > NOW() - INTERVAL '1 hour') AS sent_last_hour
FROM mailboxes m
LEFT JOIN scheduled_emails s ON s.mailbox_id = m.id AND s.status = 'sent'
GROUP BY m.id, m.email_address, m.hourly_limit
HAVING COUNT(s.id) FILTER (WHERE s.sent_at > NOW() - INTERVAL '1 hour') >= m.hourly_limit
ORDER BY sent_last_hour DESC;

-- In-flight / stuck leases
SELECT id, mailbox_id, leased_by, leased_until, attempts
FROM scheduled_emails
WHERE status = 'sending'
ORDER BY leased_until;

-- Failed rows waiting for retry
SELECT COUNT(*) FROM scheduled_emails
WHERE status = 'failed' AND next_retry_at <= NOW();

-- Duplicate-send check (should be 0 rows)
SELECT idempotency_key, COUNT(*)
FROM scheduled_emails
WHERE idempotency_key IS NOT NULL
GROUP BY idempotency_key
HAVING COUNT(*) > 1;
```

## Graceful shutdown and chaos testing

### Graceful shutdown (`SIGTERM` / `SIGINT`)

The worker stops claiming new batches, waits for in-flight sends (up to `WORKER_SHUTDOWN_TIMEOUT_MS`, default 30s), releases owned leases, logs a final metrics summary, and exits.

```bash
kill -TERM <pid>   # or Ctrl+C
```

### Chaos: kill mid-flight (`SIGKILL`)

Simulate a crash while a worker holds leases:

```bash
kill -9 <pid>
```

After `WORKER_LEASE_TTL_SECONDS` (~60s), remaining workers reclaim rows with the **same** idempotency key; the simulated provider dedupes, so no duplicate visible send occurs. Verify with the SQL queries above and watch for `email_sent` / recovery events in logs.

**Suggested no-duplicate reproduction:**

1. `docker compose up --build --scale worker=5`
2. Tail worker logs; watch `metrics_summary` lines.
3. `kill -9` one worker PID; confirm counts still converge with no duplicate `idempotency_key` rows.

## Project structure

```
src/           Worker loop, DB layer (claim/finalize), provider, metrics, config
migrations/    SQL schema (001_init.sql)
scripts/       Seed script
tests/         Vitest suite
docker-compose.yml
Dockerfile
```

## npm scripts reference

| Script | Command |
|--------|---------|
| `npm run build` | Compile TypeScript |
| `npm run migrate` | Apply `migrations/001_init.sql` |
| `npm run seed` | Populate mailboxes + scheduled emails |
| `npm run worker` | Start send worker |
| `npm test` | Run all tests once |
| `npm run test:watch` | Vitest watch mode |
