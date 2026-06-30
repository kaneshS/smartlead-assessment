import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SimulatedProvider } from '../src/provider/simulated.js';
import {
  closePool,
  countByStatus,
  getPool,
  insertEmail,
  insertMailbox,
  migrate,
  startTestWorker,
  stopTestWorker,
  truncateAll,
  waitForDrain,
} from './helpers.js';

const TEST_EMAILS = 200;
const TEST_MAILBOXES = 5;
const TEST_WORKERS = 5;
const MAILBOX_HOURLY_LIMIT = 40;

async function seedConcurrencyData(pool: ReturnType<typeof getPool>): Promise<void> {
  await truncateAll(pool);

  const mailboxIds: number[] = [];
  for (let i = 1; i <= TEST_MAILBOXES; i++) {
    mailboxIds.push(
      await insertMailbox(pool, `test-mailbox${i}@example.com`, MAILBOX_HOURLY_LIMIT),
    );
  }

  for (let i = 0; i < TEST_EMAILS; i++) {
    await insertEmail(pool, {
      campaignId: (i % 10) + 1,
      mailboxId: mailboxIds[i % TEST_MAILBOXES]!,
      toAddress: `user${i}@example.com`,
      subject: `Subject ${i}`,
      body: `Body ${i}`,
      scheduledAt: new Date(Date.now() - 1000),
    });
  }
}

describe('concurrency', () => {
  const pool = getPool();
  const provider = new SimulatedProvider({
    successRate: 1,
    transientRate: 0,
    throttledRate: 0,
    hardBounceRate: 0,
    ambiguousRate: 0,
    latencyMinMs: 0,
    latencyMaxMs: 0,
  });

  beforeAll(async () => {
    await migrate(pool);
    await seedConcurrencyData(pool);
  });

  afterAll(async () => {
    await closePool();
  });

  it('processes emails exactly once across parallel workers and respects mailbox caps', async () => {
    const workers = Array.from({ length: TEST_WORKERS }, (_, i) =>
      startTestWorker({
        pool,
        provider,
        workerId: `test-worker-${i}`,
        batchSize: 10,
        pollIntervalMs: 50,
      }),
    );

    await waitForDrain(pool);

    await Promise.all(workers.map(({ shutdown, workerPromise }) => stopTestWorker(shutdown, workerPromise)));

    const counts = await countByStatus(pool);
    expect(counts.sent).toBe(TEST_EMAILS);
    expect(counts.pending ?? 0).toBe(0);
    expect(counts.sending ?? 0).toBe(0);

    const dupMessages = await pool.query<{ count: string }>(
      `
      SELECT COUNT(*)::text AS count FROM (
        SELECT provider_message_id
        FROM scheduled_emails
        WHERE provider_message_id IS NOT NULL
        GROUP BY provider_message_id
        HAVING COUNT(*) > 1
      ) d
      `,
    );
    expect(Number.parseInt(dupMessages.rows[0]?.count ?? '0', 10)).toBe(0);

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

    const mailboxCounts = await pool.query<{ mailbox_id: number; sent_count: string }>(
      `
      SELECT mailbox_id, COUNT(*)::text AS sent_count
      FROM scheduled_emails
      WHERE status = 'sent'
        AND sent_at > NOW() - INTERVAL '1 hour'
      GROUP BY mailbox_id
      `,
    );

    for (const row of mailboxCounts.rows) {
      expect(Number.parseInt(row.sent_count, 10)).toBeLessThanOrEqual(MAILBOX_HOURLY_LIMIT);
    }

    expect(provider.acceptedCount()).toBe(TEST_EMAILS);
  });
});
