import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { claimBatch, releaseLeasesForWorker } from '../src/db/claim.js';
import {
  closePool,
  getEmail,
  getPool,
  insertEmail,
  insertMailbox,
  migrate,
  truncateAll,
} from './helpers.js';

describe('claim', () => {
  const pool = getPool();
  const maxAttempts = 5;

  beforeAll(async () => {
    await migrate(pool);
  });

  beforeEach(async () => {
    await truncateAll(pool);
  });

  afterAll(async () => {
    await closePool();
  });

  it('claims pending rows and assigns lease metadata', async () => {
    const mailboxId = await insertMailbox(pool, 'claim@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });

    const claimed = await claimBatch(pool, 'worker-a', 10, 30, maxAttempts);
    expect(claimed).toHaveLength(1);
    expect(claimed[0]!.id).toBe(emailId);
    expect(claimed[0]!.status).toBe('sending');
    expect(claimed[0]!.leased_by).toBe('worker-a');
    expect(claimed[0]!.leased_until).not.toBeNull();
    expect(claimed[0]!.attempts).toBe(1);
    expect(claimed[0]!.idempotency_key).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    );
  });

  it('preserves idempotency_key when reclaiming an expired lease', async () => {
    const mailboxId = await insertMailbox(pool, 'reclaim@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });
    const firstClaim = await claimBatch(pool, 'worker-a', 1, 1, maxAttempts);
    const originalKey = firstClaim[0]!.idempotency_key!;

    await pool.query(
      `UPDATE scheduled_emails SET leased_until = NOW() - INTERVAL '1 minute' WHERE id = $1`,
      [emailId],
    );

    const reclaimed = await claimBatch(pool, 'worker-b', 1, 30, maxAttempts);
    expect(reclaimed).toHaveLength(1);
    expect(reclaimed[0]!.id).toBe(emailId);
    expect(reclaimed[0]!.idempotency_key).toBe(originalKey);
    expect(reclaimed[0]!.attempts).toBe(2);
  });

  it('returns disjoint rows under parallel claims (SKIP LOCKED)', async () => {
    const mailboxId = await insertMailbox(pool, 'parallel@example.com', 100);
    for (let i = 0; i < 20; i++) {
      await insertEmail(pool, {
        mailboxId,
        toAddress: `user${i}@example.com`,
        subject: `Subject ${i}`,
      });
    }

    const clients = await Promise.all(
      Array.from({ length: 5 }, () => pool.connect()),
    );

    try {
      const batches = await Promise.all(
        clients.map((client, i) =>
          claimBatch(client, `worker-${i}`, 10, 30, maxAttempts),
        ),
      );

      const allIds = batches.flat().map((row) => row.id);
      expect(allIds).toHaveLength(20);
      expect(new Set(allIds).size).toBe(20);
    } finally {
      await Promise.all(clients.map((c) => c.release()));
    }
  });

  it('does not claim rows with scheduled_at in the future', async () => {
    const mailboxId = await insertMailbox(pool, 'future@example.com', 50);
    await insertEmail(pool, {
      mailboxId,
      scheduledAt: new Date(Date.now() + 3600_000),
    });

    const claimed = await claimBatch(pool, 'worker-a', 10, 30, maxAttempts);
    expect(claimed).toHaveLength(0);
  });

  it('does not claim failed rows until next_retry_at is due', async () => {
    const mailboxId = await insertMailbox(pool, 'retry-gate@example.com', 50);
    await insertEmail(pool, {
      mailboxId,
      status: 'failed',
      attempts: 1,
      nextRetryAt: new Date(Date.now() + 3600_000),
    });

    const blocked = await claimBatch(pool, 'worker-a', 10, 30, maxAttempts);
    expect(blocked).toHaveLength(0);

    await pool.query(
      `UPDATE scheduled_emails SET next_retry_at = NOW() - INTERVAL '1 second'`,
    );

    const claimed = await claimBatch(pool, 'worker-a', 10, 30, maxAttempts);
    expect(claimed).toHaveLength(1);
    expect(claimed[0]!.status).toBe('sending');
  });

  it('does not claim rows at or above maxAttempts', async () => {
    const mailboxId = await insertMailbox(pool, 'max-attempts@example.com', 50);
    await insertEmail(pool, { mailboxId, attempts: maxAttempts });

    const claimed = await claimBatch(pool, 'worker-a', 10, 30, maxAttempts);
    expect(claimed).toHaveLength(0);
  });

  it('does not claim actively leased sending rows', async () => {
    const mailboxId = await insertMailbox(pool, 'active-lease@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });
    await claimBatch(pool, 'worker-a', 1, 60, maxAttempts);

    const blocked = await claimBatch(pool, 'worker-b', 10, 60, maxAttempts);
    expect(blocked).toHaveLength(0);

    const row = await getEmail(pool, emailId);
    expect(row.status).toBe('sending');
    expect(row.leased_by).toBe('worker-a');
  });

  it('releaseLeasesForWorker returns in-flight rows to failed with immediate retry', async () => {
    const mailboxId = await insertMailbox(pool, 'release@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });
    await claimBatch(pool, 'worker-shutdown', 1, 60, maxAttempts);

    const released = await releaseLeasesForWorker(pool, 'worker-shutdown');
    expect(released).toBe(1);

    const row = await getEmail(pool, emailId);
    expect(row.status).toBe('failed');
    expect(row.leased_by).toBeNull();
    expect(row.leased_until).toBeNull();
    expect(row.next_retry_at).not.toBeNull();
    expect(row.next_retry_at!.getTime()).toBeLessThanOrEqual(Date.now() + 1000);
  });

  it('does not release leases owned by other workers', async () => {
    const mailboxId = await insertMailbox(pool, 'other-worker@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });
    await claimBatch(pool, 'worker-a', 1, 60, maxAttempts);

    const released = await releaseLeasesForWorker(pool, 'worker-b');
    expect(released).toBe(0);

    const row = await getEmail(pool, emailId);
    expect(row.status).toBe('sending');
    expect(row.leased_by).toBe('worker-a');
  });
});
