import dotenv from 'dotenv';

dotenv.config();

function envInt(key: string, fallback: number): number {
  const raw = process.env[key];
  if (raw === undefined || raw === '') return fallback;
  const n = Number.parseInt(raw, 10);
  if (Number.isNaN(n)) throw new Error(`Invalid integer for ${key}: ${raw}`);
  return n;
}

function envFloat(key: string, fallback: number): number {
  const raw = process.env[key];
  if (raw === undefined || raw === '') return fallback;
  const n = Number.parseFloat(raw);
  if (Number.isNaN(n)) throw new Error(`Invalid float for ${key}: ${raw}`);
  return n;
}

function envString(key: string, fallback: string): string {
  const raw = process.env[key];
  return raw === undefined || raw === '' ? fallback : raw;
}

export const config = {
  databaseUrl: envString('DATABASE_URL', 'postgres://sendworker:sendworker@localhost:5432/sendworker'),

  worker: {
    id: envString('WORKER_ID', `worker-${process.pid}`),
    batchSize: envInt('WORKER_BATCH_SIZE', 10),
    pollIntervalMs: envInt('WORKER_POLL_INTERVAL_MS', 500),
    leaseTtlSeconds: envInt('WORKER_LEASE_TTL_SECONDS', 60),
    maxAttempts: envInt('WORKER_MAX_ATTEMPTS', 5),
    shutdownTimeoutMs: envInt('WORKER_SHUTDOWN_TIMEOUT_MS', 30000),
  },

  metrics: {
    summaryIntervalMs: envInt('METRICS_SUMMARY_INTERVAL_MS', 30000),
  },

  provider: {
    successRate: envFloat('PROVIDER_SUCCESS_RATE', 0.8),
    transientRate: envFloat('PROVIDER_TRANSIENT_RATE', 0.12),
    throttledRate: envFloat('PROVIDER_THROTTLED_RATE', 0.05),
    hardBounceRate: envFloat('PROVIDER_HARD_BOUNCE_RATE', 0.03),
    ambiguousRate: envFloat('PROVIDER_AMBIGUOUS_RATE', 0.01),
    latencyMinMs: envInt('PROVIDER_LATENCY_MIN_MS', 50),
    latencyMaxMs: envInt('PROVIDER_LATENCY_MAX_MS', 250),
    rngSeed: process.env.PROVIDER_RNG_SEED ?? null,
  },

  seed: {
    mailboxes: envInt('SEED_MAILBOXES', 50),
    campaigns: envInt('SEED_CAMPAIGNS', 10),
    emails: envInt('SEED_EMAILS', 10000),
    dueRatio: envFloat('SEED_DUE_RATIO', 0.9),
  },
};
