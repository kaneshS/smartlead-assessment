import type pg from 'pg';
import { claimBatch, releaseLeasesForWorker } from '../db/claim.js';
import { markDead, markFailed, markSent } from '../db/finalize.js';
import type { ScheduledEmailRow } from '../db/types.js';
import { logEvent, Metrics } from '../metrics/counters.js';
import type { EmailProvider } from '../provider/types.js';
import { computeRetryAt, shouldDeadLetter } from './retry.js';
import type { ShutdownController } from './shutdown.js';

export interface WorkerOptions {
  pool: pg.Pool;
  provider: EmailProvider;
  workerId: string;
  batchSize: number;
  pollIntervalMs: number;
  leaseTtlSeconds: number;
  maxAttempts: number;
  shutdownTimeoutMs: number;
  metricsSummaryIntervalMs: number;
  shutdown: ShutdownController;
  metrics?: Metrics;
}

export async function runWorker(opts: WorkerOptions): Promise<void> {
  const metrics = opts.metrics ?? new Metrics();
  const inFlight = new Set<Promise<void>>();

  opts.shutdown.onShutdown(async () => {
    metrics.stopPeriodicSummary();
    metrics.logSummary(opts.workerId);

    const deadline = Date.now() + opts.shutdownTimeoutMs;
    while (inFlight.size > 0 && Date.now() < deadline) {
      await Promise.race([...inFlight, sleep(100)]);
    }

    const released = await releaseLeasesForWorker(opts.pool, opts.workerId);
    if (released > 0) {
      logEvent({
        event: 'leases_released_on_shutdown',
        worker_id: opts.workerId,
        count: released,
      });
    }
  });

  metrics.startPeriodicSummary(opts.metricsSummaryIntervalMs, opts.workerId);
  logEvent({ event: 'worker_started', worker_id: opts.workerId });

  while (!opts.shutdown.isShuttingDown()) {
    const claimed = await claimBatch(
      opts.pool,
      opts.workerId,
      opts.batchSize,
      opts.leaseTtlSeconds,
      opts.maxAttempts,
    );

    if (claimed.length === 0) {
      await sleep(opts.pollIntervalMs);
      continue;
    }

    metrics.increment('claims', claimed.length);

    for (const email of claimed) {
      if (opts.shutdown.isShuttingDown()) break;

      const task = processEmail(opts, email, metrics).finally(() => {
        inFlight.delete(task);
      });
      inFlight.add(task);
    }

    await Promise.allSettled([...inFlight]);
  }

  await opts.shutdown.waitForShutdown();
  logEvent({ event: 'worker_stopped', worker_id: opts.workerId });
}

async function processEmail(
  opts: WorkerOptions,
  email: ScheduledEmailRow,
  metrics: Metrics,
): Promise<void> {
  const idempotencyKey = email.idempotency_key;
  if (!idempotencyKey) {
    logEvent({
      event: 'missing_idempotency_key',
      email_id: email.id,
      worker_id: opts.workerId,
    });
    return;
  }

  try {
    const result = await opts.provider.send(idempotencyKey, {
      to: email.to_address,
      subject: email.subject,
      body: email.body,
      mailboxId: email.mailbox_id,
    });

    switch (result.kind) {
      case 'success':
        await markSent(opts.pool, email.id, result.messageId);
        metrics.increment('sent');
        logEvent({
          event: 'email_sent',
          email_id: email.id,
          mailbox_id: email.mailbox_id,
          worker_id: opts.workerId,
          provider_message_id: result.messageId,
        });
        return;

      case 'ambiguous': {
        const status = opts.provider.getStatus(idempotencyKey);
        if (status) {
          await markSent(opts.pool, email.id, status.messageId);
          metrics.increment('sent');
          metrics.increment('ambiguousRecovered');
          logEvent({
            event: 'email_sent_ambiguous_recovered',
            email_id: email.id,
            mailbox_id: email.mailbox_id,
            worker_id: opts.workerId,
            provider_message_id: status.messageId,
          });
          return;
        }
        await handleRetry(opts, email, metrics, result.message, false);
        return;
      }

      case 'hard_bounce':
        await markDead(opts.pool, email.id, result.reason);
        metrics.increment('dead');
        logEvent({
          event: 'email_dead',
          email_id: email.id,
          mailbox_id: email.mailbox_id,
          worker_id: opts.workerId,
          reason: result.reason,
        });
        return;

      case 'throttled':
        await handleRetry(opts, email, metrics, result.reason, true);
        return;

      case 'transient':
        await handleRetry(opts, email, metrics, result.reason, false);
        return;
    }
  } catch (err) {
    const status = opts.provider.getStatus(idempotencyKey);
    if (status) {
      await markSent(opts.pool, email.id, status.messageId);
      metrics.increment('sent');
      metrics.increment('ambiguousRecovered');
      logEvent({
        event: 'email_sent_after_error_status_lookup',
        email_id: email.id,
        mailbox_id: email.mailbox_id,
        worker_id: opts.workerId,
        provider_message_id: status.messageId,
        error: String(err),
      });
      return;
    }

    await handleRetry(opts, email, metrics, String(err), false);
  }
}

async function handleRetry(
  opts: WorkerOptions,
  email: ScheduledEmailRow,
  metrics: Metrics,
  reason: string,
  throttled: boolean,
): Promise<void> {
  if (shouldDeadLetter(email.attempts, opts.maxAttempts)) {
    await markDead(opts.pool, email.id, `Max attempts reached: ${reason}`);
    metrics.increment('dead');
    logEvent({
      event: 'email_dead_max_attempts',
      email_id: email.id,
      mailbox_id: email.mailbox_id,
      worker_id: opts.workerId,
      attempts: email.attempts,
      reason,
    });
    return;
  }

  const nextRetryAt = computeRetryAt(email.attempts, throttled);
  await markFailed(opts.pool, email.id, nextRetryAt, reason);
  metrics.increment('failed');
  if (email.attempts > 1) {
    metrics.increment('retried');
  }

  logEvent({
    event: 'email_retry_scheduled',
    email_id: email.id,
    mailbox_id: email.mailbox_id,
    worker_id: opts.workerId,
    attempts: email.attempts,
    next_retry_at: nextRetryAt.toISOString(),
    throttled,
    reason,
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
