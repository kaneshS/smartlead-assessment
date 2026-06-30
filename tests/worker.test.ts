import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { EmailMessage, EmailProvider, ProviderSendResult, ProviderStatus } from '../src/provider/types.js';
import { SimulatedProvider } from '../src/provider/simulated.js';
import {
  closePool,
  getEmail,
  getPool,
  insertEmail,
  insertMailbox,
  migrate,
  startTestWorker,
  stopTestWorker,
  truncateAll,
  waitForEmailStatus,
} from './helpers.js';

class MockProvider implements EmailProvider {
  private readonly outcomes = new Map<string, ProviderSendResult>();
  private readonly statuses = new Map<string, ProviderStatus>();
  sendCalls = 0;

  setOutcome(key: string, outcome: ProviderSendResult): void {
    this.outcomes.set(key, outcome);
  }

  setStatus(key: string, status: ProviderStatus): void {
    this.statuses.set(key, status);
  }

  async send(idempotencyKey: string, _message: EmailMessage): Promise<ProviderSendResult> {
    this.sendCalls++;
    const outcome = this.outcomes.get(idempotencyKey);
    if (!outcome) {
      return { kind: 'success', messageId: `mock-${idempotencyKey}` };
    }
    return outcome;
  }

  getStatus(idempotencyKey: string): ProviderStatus | null {
    return this.statuses.get(idempotencyKey) ?? null;
  }
}

describe('worker send flow', () => {
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

  it('marks success outcomes as sent with provider_message_id', async () => {
    const mailboxId = await insertMailbox(pool, 'success@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });

    const { shutdown, workerPromise } = startTestWorker({
      pool,
      batchSize: 1,
      pollIntervalMs: 20,
    });

    const row = await waitForEmailStatus(pool, emailId, 'sent');
    await stopTestWorker(shutdown, workerPromise);

    expect(row.provider_message_id).not.toBeNull();
    expect(row.sent_at).not.toBeNull();
    expect(row.leased_by).toBeNull();
  });

  it('schedules retry for transient failures', async () => {
    const mailboxId = await insertMailbox(pool, 'transient@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });

    const provider = new SimulatedProvider({
      successRate: 0,
      transientRate: 1,
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
      maxAttempts: 8,
      provider,
    });

    const row = await waitForEmailStatus(pool, emailId, 'failed');
    await stopTestWorker(shutdown, workerPromise);

    expect(row.attempts).toBe(1);
    expect(row.next_retry_at).not.toBeNull();
    expect(row.next_retry_at!.getTime()).toBeGreaterThan(Date.now());
    expect(row.last_error).toContain('transient');
  });

  it('schedules longer retry for throttled failures', async () => {
    const mailboxId = await insertMailbox(pool, 'throttled@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });

    const provider = new SimulatedProvider({
      successRate: 0,
      transientRate: 0,
      throttledRate: 1,
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

    const row = await waitForEmailStatus(pool, emailId, 'failed');
    await stopTestWorker(shutdown, workerPromise);

    const delayMs = row.next_retry_at!.getTime() - Date.now();
    expect(delayMs).toBeGreaterThanOrEqual(250_000);
  });

  it('dead-letters hard bounce without retry', async () => {
    const mailboxId = await insertMailbox(pool, 'hard@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });

    const provider = new SimulatedProvider({
      successRate: 0,
      transientRate: 0,
      throttledRate: 0,
      hardBounceRate: 1,
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

    const row = await waitForEmailStatus(pool, emailId, 'dead');
    await stopTestWorker(shutdown, workerPromise);

    expect(row.next_retry_at).toBeNull();
    expect(row.last_error).toContain('Hard bounce');
    expect(provider.acceptedCount()).toBe(0);
  });

  it('recovers ambiguous outcomes via getStatus and marks sent', async () => {
    const mailboxId = await insertMailbox(pool, 'ambiguous@example.com', 50);
    const emailId = await insertEmail(pool, { mailboxId });

    const provider = new SimulatedProvider({
      successRate: 0,
      transientRate: 0,
      throttledRate: 0,
      hardBounceRate: 0,
      ambiguousRate: 1,
      latencyMinMs: 0,
      latencyMaxMs: 0,
    });

    const { shutdown, workerPromise } = startTestWorker({
      pool,
      batchSize: 1,
      pollIntervalMs: 20,
      provider,
    });

    const row = await waitForEmailStatus(pool, emailId, 'sent');
    await stopTestWorker(shutdown, workerPromise);

    expect(row.provider_message_id).not.toBeNull();
    expect(provider.acceptedCount()).toBe(1);
  });

  it('dead-letters after max attempts on repeated transient failures', async () => {
    const mailboxId = await insertMailbox(pool, 'max-retry@example.com', 50);
    const emailId = await insertEmail(pool, {
      mailboxId,
      status: 'failed',
      attempts: 4,
      nextRetryAt: new Date(Date.now() - 1000),
    });

    const provider = new SimulatedProvider({
      successRate: 0,
      transientRate: 1,
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
      maxAttempts: 5,
      provider,
    });

    const row = await waitForEmailStatus(pool, emailId, 'dead');
    await stopTestWorker(shutdown, workerPromise);

    expect(row.attempts).toBe(5);
    expect(row.last_error).toContain('Max attempts reached');
  });

  it('uses mock provider getStatus on ambiguous without stored acceptance', async () => {
    const mailboxId = await insertMailbox(pool, 'mock-ambiguous@example.com', 50);
    const fixedKey = '11111111-1111-4111-8111-111111111111';
    const emailId = await insertEmail(pool, {
      mailboxId,
      idempotencyKey: fixedKey,
    });

    const provider = new MockProvider();
    provider.setOutcome(fixedKey, {
      kind: 'ambiguous',
      message: 'timeout before ack',
    });
    provider.setStatus(fixedKey, {
      messageId: 'recovered-msg-id',
      status: 'accepted',
    });

    const { shutdown, workerPromise } = startTestWorker({
      pool,
      batchSize: 1,
      pollIntervalMs: 20,
      provider,
    });

    const row = await waitForEmailStatus(pool, emailId, 'sent');
    await stopTestWorker(shutdown, workerPromise);

    expect(row.provider_message_id).toBe('recovered-msg-id');
    expect(provider.sendCalls).toBe(1);
  });
});
