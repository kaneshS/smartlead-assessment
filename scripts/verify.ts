/**
 * Targeted verification of the four core pillars:
 * 1. SKIP LOCKED claiming
 * 2. Idempotency (stable key + provider dedup + ambiguous recovery)
 * 3. Per-mailbox rate limits
 * 4. Retry policy (backoff + dead-letter)
 */
import { claimBatch } from '../src/db/claim.js';
import { closePool, getPool } from '../src/db/pool.js';
import { sentCountLastHour } from '../src/db/rateLimit.js';
import { SimulatedProvider } from '../src/provider/simulated.js';
import { computeRetryAt, shouldDeadLetter } from '../src/worker/retry.js';
import {
  insertEmail,
  insertMailbox,
  migrate,
  startTestWorker,
  stopTestWorker,
  truncateAll,
  waitForEmailStatus,
} from '../tests/helpers.js';

type CheckResult = { name: string; ok: boolean; detail: string };

const results: CheckResult[] = [];

function check(name: string, ok: boolean, detail: string): void {
  results.push({ name, ok, detail });
  const status = ok ? 'PASS' : 'FAIL';
  console.log(`[${status}] ${name}: ${detail}`);
}

async function verifySkipLocked(pool: ReturnType<typeof getPool>): Promise<void> {
  const mailboxId = await insertMailbox(pool, 'verify-skip@example.com', 100);
  for (let i = 0; i < 15; i++) {
    await insertEmail(pool, { mailboxId, toAddress: `skip${i}@example.com` });
  }

  const clients = await Promise.all(Array.from({ length: 3 }, () => pool.connect()));
  try {
    const batches = await Promise.all(
      clients.map((client, i) => claimBatch(client, `verify-worker-${i}`, 10, 30, 5)),
    );
    const ids = batches.flat().map((row) => row.id);
    const unique = new Set(ids);
    check(
      'SKIP LOCKED — disjoint parallel claims',
      ids.length === 15 && unique.size === 15,
      `claimed ${ids.length} rows, ${unique.size} unique`,
    );
  } finally {
    await Promise.all(clients.map((c) => c.release()));
  }
}

async function verifyIdempotency(pool: ReturnType<typeof getPool>): Promise<void> {
  const mailboxId = await insertMailbox(pool, 'verify-idem@example.com', 50);
  const emailId = await insertEmail(pool, { mailboxId });

  const first = await claimBatch(pool, 'verify-idem-a', 1, 1, 5);
  const originalKey = first[0]!.idempotency_key!;

  await pool.query(
    `UPDATE scheduled_emails SET leased_until = NOW() - INTERVAL '1 minute' WHERE id = $1`,
    [emailId],
  );

  const reclaimed = await claimBatch(pool, 'verify-idem-b', 1, 30, 5);
  const sameKey = reclaimed[0]?.idempotency_key === originalKey;
  check(
    'Idempotency — stable key on lease reclaim',
    sameKey,
    sameKey ? `key preserved (${originalKey})` : 'key changed after reclaim',
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

  const msg = { to: 'user@example.com', subject: 'Hi', body: 'Body', mailboxId: 1 };
  const firstSend = await provider.send(originalKey, msg);
  const secondSend = await provider.send(originalKey, msg);
  const deduped =
    firstSend.kind === 'success' &&
    secondSend.kind === 'success' &&
    firstSend.messageId === secondSend.messageId &&
    provider.acceptedCount() === 1;
  check(
    'Idempotency — provider dedup by key',
    deduped,
    deduped ? 'single provider accept for duplicate send' : 'dedup failed',
  );

  const ambiguousProvider = new SimulatedProvider({
    successRate: 0,
    transientRate: 0,
    throttledRate: 0,
    hardBounceRate: 0,
    ambiguousRate: 1,
    latencyMinMs: 0,
    latencyMaxMs: 0,
  });
  const ambiguousMailbox = await insertMailbox(pool, 'verify-ambiguous@example.com', 50);
  const ambiguousEmailId = await insertEmail(pool, { mailboxId: ambiguousMailbox });

  const { shutdown, workerPromise } = startTestWorker({
    pool,
    batchSize: 1,
    pollIntervalMs: 20,
    provider: ambiguousProvider,
  });

  const row = await waitForEmailStatus(pool, ambiguousEmailId, 'sent', 15_000);
  await stopTestWorker(shutdown, workerPromise);

  check(
    'Idempotency — ambiguous timeout recovered via getStatus',
    row.provider_message_id !== null && ambiguousProvider.acceptedCount() === 1,
    row.provider_message_id
      ? `marked sent with ${row.provider_message_id}`
      : 'ambiguous recovery failed',
  );
}

async function verifyRateLimit(pool: ReturnType<typeof getPool>): Promise<void> {
  const capped = await insertMailbox(pool, 'verify-capped@example.com', 2);
  const open = await insertMailbox(pool, 'verify-open@example.com', 50);

  for (let i = 0; i < 2; i++) {
    await insertEmail(pool, {
      mailboxId: capped,
      status: 'sent',
      sentAt: new Date(),
      providerMessageId: `cap-msg-${i}`,
      toAddress: `sent${i}@example.com`,
    });
  }
  await insertEmail(pool, { mailboxId: capped, toAddress: 'blocked@example.com' });
  const openEmailId = await insertEmail(pool, { mailboxId: open, toAddress: 'allowed@example.com' });

  const blocked = await claimBatch(pool, 'verify-rate', 10, 30, 5);
  const cappedBlocked = blocked.every((row) => row.mailbox_id !== capped);
  const openClaimed = blocked.some((row) => row.id === openEmailId);

  check(
    'Rate limit — capped mailbox blocked in claim query',
    (await sentCountLastHour(pool, capped)) === 2 && cappedBlocked,
    `sent_last_hour=${await sentCountLastHour(pool, capped)}, capped rows claimed=${!cappedBlocked}`,
  );
  check(
    'Rate limit — other mailboxes still flow',
    openClaimed,
    openClaimed ? 'open mailbox row claimed' : 'open mailbox not claimed',
  );
}

async function verifyRetryPolicy(): Promise<void> {
  const fixedNow = Date.now();
  const transientFirst = computeRetryAt(1, false).getTime() - fixedNow;
  const throttledFirst = computeRetryAt(1, true).getTime() - fixedNow;
  const capped = computeRetryAt(10, false).getTime() - fixedNow;

  check(
    'Retry — transient base backoff ~30s',
    transientFirst >= 30_000 && transientFirst <= 35_000,
    `${Math.round(transientFirst / 1000)}s (with jitter)`,
  );
  check(
    'Retry — throttled base backoff ~5min',
    throttledFirst >= 300_000 && throttledFirst <= 305_000,
    `${Math.round(throttledFirst / 1000)}s (with jitter)`,
  );
  check(
    'Retry — exponential cap at 15min',
    capped >= 900_000 && capped <= 905_000,
    `${Math.round(capped / 1000)}s (with jitter)`,
  );
  check(
    'Retry — dead-letter at max attempts',
    shouldDeadLetter(5, 5) && !shouldDeadLetter(4, 5),
    'attempts >= maxAttempts → dead',
  );
}

async function main(): Promise<void> {
  const pool = getPool();
  await migrate(pool);

  console.log('Verifying four core pillars...\n');

  await truncateAll(pool);
  await verifySkipLocked(pool);

  await truncateAll(pool);
  await verifyIdempotency(pool);

  await truncateAll(pool);
  await verifyRateLimit(pool);

  await verifyRetryPolicy();

  await closePool();

  const failed = results.filter((r) => !r.ok);
  console.log(`\n${results.length - failed.length}/${results.length} checks passed`);

  if (failed.length > 0) {
    console.error('\nFailed checks:');
    for (const f of failed) {
      console.error(`  - ${f.name}: ${f.detail}`);
    }
    process.exit(1);
  }

  console.log('\nAll pillar checks passed.');
}

main().catch((err) => {
  console.error(JSON.stringify({ event: 'verify_error', error: String(err), stack: err?.stack }));
  process.exit(1);
});
