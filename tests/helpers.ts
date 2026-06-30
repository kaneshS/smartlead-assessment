import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type pg from 'pg';
import { closePool, getPool } from '../src/db/pool.js';
import type { ScheduledEmailRow } from '../src/db/types.js';
import type { EmailMessage, EmailProvider, ProviderSendResult } from '../src/provider/types.js';
import { SimulatedProvider } from '../src/provider/simulated.js';
import { runWorker } from '../src/worker/loop.js';
import { createShutdownController } from '../src/worker/shutdown.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

export { closePool, getPool };

export async function migrate(pool: pg.Pool): Promise<void> {
  const sqlPath = join(__dirname, '..', 'migrations', '001_init.sql');
  const sql = await readFile(sqlPath, 'utf8');
  await pool.query(sql);
}

export async function truncateAll(pool: pg.Pool): Promise<void> {
  await pool.query('TRUNCATE scheduled_emails RESTART IDENTITY CASCADE');
  await pool.query('TRUNCATE mailboxes RESTART IDENTITY CASCADE');
}

export async function insertMailbox(
  pool: pg.Pool,
  emailAddress: string,
  hourlyLimit: number,
): Promise<number> {
  const result = await pool.query<{ id: number }>(
    `INSERT INTO mailboxes (email_address, hourly_limit) VALUES ($1, $2) RETURNING id`,
    [emailAddress, hourlyLimit],
  );
  return result.rows[0]!.id;
}

export interface InsertEmailOptions {
  campaignId?: number;
  mailboxId: number;
  toAddress?: string;
  subject?: string;
  body?: string;
  scheduledAt?: Date;
  status?: ScheduledEmailRow['status'];
  attempts?: number;
  idempotencyKey?: string | null;
  nextRetryAt?: Date | null;
  leasedBy?: string | null;
  leasedUntil?: Date | null;
  sentAt?: Date | null;
  providerMessageId?: string | null;
}

export async function insertEmail(
  pool: pg.Pool,
  opts: InsertEmailOptions,
): Promise<number> {
  const result = await pool.query<{ id: number }>(
    `
    INSERT INTO scheduled_emails (
      campaign_id, mailbox_id, to_address, subject, body, scheduled_at,
      status, attempts, idempotency_key, next_retry_at, leased_by, leased_until,
      sent_at, provider_message_id
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    RETURNING id
    `,
    [
      opts.campaignId ?? 1,
      opts.mailboxId,
      opts.toAddress ?? 'recipient@example.com',
      opts.subject ?? 'Test subject',
      opts.body ?? 'Test body',
      opts.scheduledAt ?? new Date(Date.now() - 1000),
      opts.status ?? 'pending',
      opts.attempts ?? 0,
      opts.idempotencyKey ?? null,
      opts.nextRetryAt ?? null,
      opts.leasedBy ?? null,
      opts.leasedUntil ?? null,
      opts.sentAt ?? null,
      opts.providerMessageId ?? null,
    ],
  );
  return result.rows[0]!.id;
}

export async function getEmail(pool: pg.Pool, id: number): Promise<ScheduledEmailRow> {
  const result = await pool.query<ScheduledEmailRow>(
    `SELECT * FROM scheduled_emails WHERE id = $1`,
    [id],
  );
  return result.rows[0]!;
}

export async function countByStatus(pool: pg.Pool): Promise<Record<string, number>> {
  const result = await pool.query<{ status: string; count: string }>(
    `SELECT status, COUNT(*)::text AS count FROM scheduled_emails GROUP BY status`,
  );
  const counts: Record<string, number> = {};
  for (const row of result.rows) {
    counts[row.status] = Number.parseInt(row.count, 10);
  }
  return counts;
}

export async function waitForCondition(
  fn: () => Promise<boolean>,
  timeoutMs = 30_000,
  intervalMs = 100,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error('Timed out waiting for condition');
}

export async function waitForDrain(pool: pg.Pool, timeoutMs = 90_000): Promise<void> {
  await waitForCondition(async () => {
    const counts = await countByStatus(pool);
    const remaining =
      (counts.pending ?? 0) + (counts.failed ?? 0) + (counts.sending ?? 0);
    return remaining === 0;
  }, timeoutMs);
}

export async function waitForEmailStatus(
  pool: pg.Pool,
  id: number,
  status: ScheduledEmailRow['status'],
  timeoutMs = 30_000,
): Promise<ScheduledEmailRow> {
  let row: ScheduledEmailRow | undefined;
  await waitForCondition(async () => {
    row = await getEmail(pool, id);
    return row.status === status;
  }, timeoutMs);
  return row!;
}

export interface RunTestWorkerOptions {
  pool: pg.Pool;
  workerId?: string;
  batchSize?: number;
  pollIntervalMs?: number;
  leaseTtlSeconds?: number;
  maxAttempts?: number;
  shutdownTimeoutMs?: number;
  provider?: SimulatedProvider;
}

export function startTestWorker(opts: RunTestWorkerOptions) {
  const shutdown = createShutdownController({ registerSignals: false });
  const provider =
    opts.provider ??
    new SimulatedProvider({
      successRate: 1,
      transientRate: 0,
      throttledRate: 0,
      hardBounceRate: 0,
      ambiguousRate: 0,
      latencyMinMs: 0,
      latencyMaxMs: 0,
    });

  const workerPromise = runWorker({
    pool: opts.pool,
    provider,
    workerId: opts.workerId ?? 'test-worker',
    batchSize: opts.batchSize ?? 5,
    pollIntervalMs: opts.pollIntervalMs ?? 50,
    leaseTtlSeconds: opts.leaseTtlSeconds ?? 30,
    maxAttempts: opts.maxAttempts ?? 8,
    shutdownTimeoutMs: opts.shutdownTimeoutMs ?? 5000,
    metricsSummaryIntervalMs: 600_000,
    shutdown,
  });

  return { shutdown, provider, workerPromise };
}

export async function stopTestWorker(
  shutdown: ReturnType<typeof createShutdownController>,
  workerPromise: Promise<void>,
): Promise<void> {
  shutdown.triggerShutdown();
  await workerPromise;
}

/** Provider whose send() blocks until unblocked — useful for shutdown tests. */
export class BlockingProvider implements EmailProvider {
  private unblockSend: (() => void) | null = null;
  private readonly sendGate = new Promise<void>((resolve) => {
    this.unblockSend = resolve;
  });

  unblock(): void {
    this.unblockSend?.();
  }

  async send(_idempotencyKey: string, _message: EmailMessage): Promise<ProviderSendResult> {
    await this.sendGate;
    return { kind: 'success', messageId: 'blocked-msg' };
  }

  getStatus(): null {
    return null;
  }
}
