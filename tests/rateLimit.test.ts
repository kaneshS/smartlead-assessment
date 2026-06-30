import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { claimBatch } from '../src/db/claim.js';
import { sentCountLastHour } from '../src/db/rateLimit.js';
import {
  closePool,
  getPool,
  insertEmail,
  insertMailbox,
  migrate,
  truncateAll,
} from './helpers.js';

describe('rateLimit', () => {
  const pool = getPool();
  const maxAttempts = 8;

  beforeAll(async () => {
    await migrate(pool);
  });

  beforeEach(async () => {
    await truncateAll(pool);
  });

  afterAll(async () => {
    await closePool();
  });

  async function insertSentInLastHour(mailboxId: number, count: number): Promise<void> {
    for (let i = 0; i < count; i++) {
      await insertEmail(pool, {
        mailboxId,
        status: 'sent',
        sentAt: new Date(),
        providerMessageId: `msg-${mailboxId}-${i}-${Date.now()}`,
        toAddress: `sent${i}@example.com`,
      });
    }
  }

  it('blocks claiming when mailbox has reached hourly_limit', async () => {
    const cappedMailbox = await insertMailbox(pool, 'capped@example.com', 2);
    await insertSentInLastHour(cappedMailbox, 2);
    await insertEmail(pool, { mailboxId: cappedMailbox, toAddress: 'blocked1@example.com' });
    await insertEmail(pool, { mailboxId: cappedMailbox, toAddress: 'blocked2@example.com' });

    expect(await sentCountLastHour(pool, cappedMailbox)).toBe(2);

    const claimed = await claimBatch(pool, 'worker-a', 10, 30, maxAttempts);
    expect(claimed).toHaveLength(0);
  });

  it('keeps other mailboxes flowing when one mailbox is capped', async () => {
    const cappedMailbox = await insertMailbox(pool, 'full@example.com', 1);
    const openMailbox = await insertMailbox(pool, 'open@example.com', 50);

    await insertSentInLastHour(cappedMailbox, 1);
    await insertEmail(pool, { mailboxId: cappedMailbox, toAddress: 'blocked@example.com' });
    const openEmailId = await insertEmail(pool, {
      mailboxId: openMailbox,
      toAddress: 'allowed@example.com',
    });

    const claimed = await claimBatch(pool, 'worker-a', 10, 30, maxAttempts);
    expect(claimed).toHaveLength(1);
    expect(claimed[0]!.id).toBe(openEmailId);
    expect(claimed[0]!.mailbox_id).toBe(openMailbox);
  });

  it('allows claiming again after sent rows fall outside the rolling hour', async () => {
    const mailboxId = await insertMailbox(pool, 'rolling@example.com', 1);
    await insertEmail(pool, {
      mailboxId,
      status: 'sent',
      sentAt: new Date(Date.now() - 2 * 3600_000),
      providerMessageId: 'msg-old',
      toAddress: 'old@example.com',
    });
    const pendingId = await insertEmail(pool, {
      mailboxId,
      toAddress: 'new@example.com',
    });

    expect(await sentCountLastHour(pool, mailboxId)).toBe(0);

    const claimed = await claimBatch(pool, 'worker-a', 10, 30, maxAttempts);
    expect(claimed).toHaveLength(1);
    expect(claimed[0]!.id).toBe(pendingId);
  });

  it('counts only sent rows in the last hour toward the cap', async () => {
    const mailboxId = await insertMailbox(pool, 'count@example.com', 2);
    await insertEmail(pool, {
      mailboxId,
      status: 'sent',
      sentAt: new Date(Date.now() - 2 * 3600_000),
      providerMessageId: 'msg-expired',
    });
    await insertEmail(pool, {
      mailboxId,
      status: 'sent',
      sentAt: new Date(),
      providerMessageId: 'msg-recent-1',
    });
    const pendingId = await insertEmail(pool, {
      mailboxId,
      toAddress: 'pending@example.com',
    });

    expect(await sentCountLastHour(pool, mailboxId)).toBe(1);

    const claimed = await claimBatch(pool, 'worker-a', 10, 30, maxAttempts);
    expect(claimed).toHaveLength(1);
    expect(claimed[0]!.id).toBe(pendingId);
  });

  it('does not count failed or sending rows toward the hourly cap', async () => {
    const mailboxId = await insertMailbox(pool, 'statuses@example.com', 1);
    await insertEmail(pool, {
      mailboxId,
      status: 'failed',
      attempts: 1,
      nextRetryAt: new Date(Date.now() - 1000),
    });
    await insertEmail(pool, {
      mailboxId,
      status: 'sending',
      attempts: 1,
      leasedBy: 'worker-x',
      leasedUntil: new Date(Date.now() + 60_000),
    });
    const pendingId = await insertEmail(pool, { mailboxId, toAddress: 'pending@example.com' });

    const claimed = await claimBatch(pool, 'worker-a', 10, 30, maxAttempts);
    expect(claimed.some((row) => row.id === pendingId)).toBe(true);
  });
});
