import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { claimBatch } from '../src/db/claim.js';
import { SimulatedProvider } from '../src/provider/simulated.js';
import {
  closePool,
  countByStatus,
  getEmail,
  getPool,
  insertEmail,
  insertMailbox,
  migrate,
  startTestWorker,
  stopTestWorker,
  truncateAll,
  waitForCondition,
  waitForDrain,
  waitForEmailStatus,
} from './helpers.js';

describe('integration', () => {
  const pool = getPool();

  beforeAll(async () => {
    await migrate(pool);
  });

  beforeEach(async () => {
    await truncateAll(pool);
  });

  afterAll(async () => {
    await closePool();
  });

  it('drains mixed provider outcomes to terminal states', async () => {
    const mailboxId = await insertMailbox(pool, 'mixed@example.com', 100);
    for (let i = 0; i < 20; i++) {
      await insertEmail(pool, {
        mailboxId,
        toAddress: `mixed${i}@example.com`,
        subject: `Mixed ${i}`,
      });
    }

    const provider = new SimulatedProvider({
      successRate: 0.6,
      transientRate: 0,
      throttledRate: 0,
      hardBounceRate: 0.2,
      ambiguousRate: 0.2,
      latencyMinMs: 0,
      latencyMaxMs: 0,
      rngSeed: 'integration-mixed',
    });

    const { shutdown, workerPromise } = startTestWorker({
      pool,
      batchSize: 5,
      pollIntervalMs: 30,
      provider,
    });

    await waitForCondition(async () => {
      const counts = await countByStatus(pool);
      const terminal = (counts.sent ?? 0) + (counts.dead ?? 0);
      const inFlight = (counts.pending ?? 0) + (counts.sending ?? 0);
      return terminal === 20 && inFlight === 0;
    }, 60_000);

    await stopTestWorker(shutdown, workerPromise);

    const counts = await countByStatus(pool);
    expect((counts.pending ?? 0) + (counts.sending ?? 0)).toBe(0);
    expect((counts.sent ?? 0) + (counts.dead ?? 0)).toBe(20);

    const dupKeys = await pool.query<{ count: string }>(
      `
      SELECT COUNT(*)::text AS count FROM (
        SELECT idempotency_key
        FROM scheduled_emails
        WHERE idempotency_key IS NOT NULL
        GROUP BY idempotency_key
        HAVING COUNT(*) > 1
      ) d
      `,
    );
    expect(Number.parseInt(dupKeys.rows[0]?.count ?? '0', 10)).toBe(0);
  });

  it('reclaims expired leases with stable idempotency and single provider accept', async () => {
    const mailboxId = await insertMailbox(pool, 'lease-recovery@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });

    const firstClaim = await claimBatch(pool, 'crashed-worker', 1, 1, 8);
    const idempotencyKey = firstClaim[0]!.idempotency_key!;

    await pool.query(
      `UPDATE scheduled_emails SET leased_until = NOW() - INTERVAL '1 minute' WHERE id = $1`,
      [emailId],
    );

    const provider = new SimulatedProvider({
      successRate: 1,
      transientRate: 0,
      throttledRate: 0,
      hardBounceRate: 0,
      ambiguousRate: 0,
      latencyMinMs: 0,
      latencyMaxMs: 0,
    });

    const { shutdown, workerPromise } = startTestWorker({
      pool,
      workerId: 'recovery-worker',
      batchSize: 1,
      pollIntervalMs: 20,
      provider,
    });

    await waitForEmailStatus(pool, emailId, 'sent');
    await stopTestWorker(shutdown, workerPromise);

    const row = await getEmail(pool, emailId);
    expect(row.idempotency_key).toBe(idempotencyKey);
    expect(provider.acceptedCount()).toBe(1);
  });

  it('retries failed emails once next_retry_at becomes due', async () => {
    const mailboxId = await insertMailbox(pool, 'retry-due@example.com', 50);
    const emailId = await insertEmail(pool, {
      mailboxId,
      status: 'failed',
      attempts: 1,
      nextRetryAt: new Date(Date.now() + 200),
      idempotencyKey: '22222222-2222-4222-8222-222222222222',
    });

    const provider = new SimulatedProvider({
      successRate: 1,
      transientRate: 0,
      throttledRate: 0,
      hardBounceRate: 0,
      ambiguousRate: 0,
      latencyMinMs: 0,
      latencyMaxMs: 0,
    });

    const { shutdown, workerPromise } = startTestWorker({
      pool,
      batchSize: 1,
      pollIntervalMs: 20,
      provider,
    });

    await waitForCondition(async () => {
      const row = await getEmail(pool, emailId);
      return row.status === 'sent';
    }, 15_000);

    await stopTestWorker(shutdown, workerPromise);
    expect(provider.acceptedCount()).toBe(1);
  });

  it('handles empty backlog without errors until shutdown', async () => {
    await insertMailbox(pool, 'empty@example.com', 50);

    const { shutdown, workerPromise } = startTestWorker({
      pool,
      pollIntervalMs: 30,
    });

    await new Promise((r) => setTimeout(r, 300));
    await stopTestWorker(shutdown, workerPromise);

    const counts = await countByStatus(pool);
    expect(Object.keys(counts)).toHaveLength(0);
  });

  it('seed script inserts mailboxes and scheduled emails', async () => {
    vi.stubEnv('SEED_MAILBOXES', '3');
    vi.stubEnv('SEED_CAMPAIGNS', '2');
    vi.stubEnv('SEED_EMAILS', '25');
    vi.stubEnv('SEED_DUE_RATIO', '1');
    vi.resetModules();

    const { runSeed: runSeedFresh } = await import('../scripts/seed.js');
    await runSeedFresh(pool);

    const mailboxCount = await pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM mailboxes`,
    );
    const emailCount = await pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM scheduled_emails WHERE status = 'pending'`,
    );

    expect(Number.parseInt(mailboxCount.rows[0]?.count ?? '0', 10)).toBe(3);
    expect(Number.parseInt(emailCount.rows[0]?.count ?? '0', 10)).toBe(25);
  });

  it('drains a small multi-mailbox batch with rate limits enforced', async () => {
    const mailboxIds: number[] = [];
    for (let i = 1; i <= 3; i++) {
      mailboxIds.push(await insertMailbox(pool, `drain${i}@example.com`, 5));
    }

    for (let i = 0; i < 12; i++) {
      await insertEmail(pool, {
        mailboxId: mailboxIds[i % mailboxIds.length]!,
        toAddress: `drain${i}@example.com`,
      });
    }

    const provider = new SimulatedProvider({
      successRate: 1,
      transientRate: 0,
      throttledRate: 0,
      hardBounceRate: 0,
      ambiguousRate: 0,
      latencyMinMs: 0,
      latencyMaxMs: 0,
    });

    const { shutdown, workerPromise } = startTestWorker({
      pool,
      batchSize: 4,
      pollIntervalMs: 30,
      provider,
    });

    await waitForDrain(pool, 60_000);
    await stopTestWorker(shutdown, workerPromise);

    const counts = await countByStatus(pool);
    expect(counts.sent).toBe(12);
    expect(provider.acceptedCount()).toBe(12);

    for (const mailboxId of mailboxIds) {
      const sent = await pool.query<{ count: string }>(
        `
        SELECT COUNT(*)::text AS count
        FROM scheduled_emails
        WHERE mailbox_id = $1
          AND status = 'sent'
          AND sent_at > NOW() - INTERVAL '1 hour'
        `,
        [mailboxId],
      );
      expect(Number.parseInt(sent.rows[0]?.count ?? '0', 10)).toBeLessThanOrEqual(5);
    }
  });
});
