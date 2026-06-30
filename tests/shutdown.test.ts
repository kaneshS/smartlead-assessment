import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { claimBatch } from '../src/db/claim.js';
import { createShutdownController } from '../src/worker/shutdown.js';
import { SimulatedProvider } from '../src/provider/simulated.js';
import { runWorker } from '../src/worker/loop.js';
import {
  BlockingProvider,
  closePool,
  getEmail,
  getPool,
  insertEmail,
  insertMailbox,
  migrate,
  truncateAll,
  waitForCondition,
  waitForEmailStatus,
} from './helpers.js';

describe('shutdown', () => {
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

  it('releases in-flight leases after shutdown timeout when send is blocked', async () => {
    const mailboxId = await insertMailbox(pool, 'shutdown-release@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });
    const provider = new BlockingProvider();

    const shutdown = createShutdownController({ registerSignals: false });
    const workerPromise = runWorker({
      pool,
      provider,
      workerId: 'shutdown-worker',
      batchSize: 1,
      pollIntervalMs: 20,
      leaseTtlSeconds: 120,
      maxAttempts: 8,
      shutdownTimeoutMs: 200,
      metricsSummaryIntervalMs: 600_000,
      shutdown,
    });

    await waitForCondition(async () => {
      const row = await getEmail(pool, emailId);
      return row.status === 'sending';
    });

    shutdown.triggerShutdown();

    await waitForCondition(async () => {
      const row = await getEmail(pool, emailId);
      return row.status === 'failed';
    }, 5000);

    const row = await getEmail(pool, emailId);
    expect(row.leased_by).toBeNull();
    expect(row.leased_until).toBeNull();
    expect(row.next_retry_at).not.toBeNull();

    provider.unblock();
    await workerPromise;
  });

  it('stops claiming new work while a blocked send is in flight', async () => {
    const mailboxId = await insertMailbox(pool, 'stop-claim@example.com', 50);
    const firstId = await insertEmail(pool, { mailboxId, toAddress: 'first@example.com' });
    const secondId = await insertEmail(pool, { mailboxId, toAddress: 'second@example.com' });
    const provider = new BlockingProvider();

    const shutdown = createShutdownController({ registerSignals: false });
    const workerPromise = runWorker({
      pool,
      provider,
      workerId: 'stop-claim-worker',
      batchSize: 1,
      pollIntervalMs: 20,
      leaseTtlSeconds: 120,
      maxAttempts: 8,
      shutdownTimeoutMs: 200,
      metricsSummaryIntervalMs: 600_000,
      shutdown,
    });

    await waitForCondition(async () => {
      const row = await getEmail(pool, firstId);
      return row.status === 'sending';
    });

    shutdown.triggerShutdown();

    await waitForCondition(async () => {
      const row = await getEmail(pool, firstId);
      return row.status === 'failed';
    }, 5000);

    const second = await getEmail(pool, secondId);
    expect(second.status).toBe('pending');

    provider.unblock();
    await workerPromise;
  });

  it('shutdown controller exposes isShuttingDown and waitForShutdown', async () => {
    const shutdown = createShutdownController({ registerSignals: false });
    expect(shutdown.isShuttingDown()).toBe(false);

    shutdown.triggerShutdown();
    expect(shutdown.isShuttingDown()).toBe(true);
    await shutdown.waitForShutdown();
  });

  it('allows another worker to reclaim released leases', async () => {
    const mailboxId = await insertMailbox(pool, 'reclaim-after-shutdown@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });

    await claimBatch(pool, 'crashed-worker', 1, 60, 8);
    await pool.query(
      `
      UPDATE scheduled_emails
      SET status = 'failed',
          leased_by = NULL,
          leased_until = NULL,
          next_retry_at = NOW() - INTERVAL '1 second'
      WHERE id = $1
      `,
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

    const shutdown = createShutdownController({ registerSignals: false });
    const workerPromise = runWorker({
      pool,
      provider,
      workerId: 'recovery-worker',
      batchSize: 1,
      pollIntervalMs: 20,
      leaseTtlSeconds: 30,
      maxAttempts: 8,
      shutdownTimeoutMs: 5000,
      metricsSummaryIntervalMs: 600_000,
      shutdown,
    });

    await waitForEmailStatus(pool, emailId, 'sent');
    shutdown.triggerShutdown();
    await workerPromise;

    expect(provider.acceptedCount()).toBe(1);
  });
});
