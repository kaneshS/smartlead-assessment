import type pg from 'pg';
import type { ScheduledEmailRow } from './types.js';

const CLAIM_SQL = `
WITH claimable AS (
  SELECT e.id
  FROM scheduled_emails e
  JOIN mailboxes m ON m.id = e.mailbox_id
  WHERE (
      e.status IN ('pending', 'failed')
      OR (e.status = 'sending' AND e.leased_until IS NOT NULL AND e.leased_until < NOW())
    )
    AND e.scheduled_at <= NOW()
    AND (e.next_retry_at IS NULL OR e.next_retry_at <= NOW())
    AND (e.leased_until IS NULL OR e.leased_until < NOW())
    AND e.attempts < $4
    AND (
      SELECT COUNT(*)::int FROM scheduled_emails s
      WHERE s.mailbox_id = e.mailbox_id
        AND s.status = 'sent'
        AND s.sent_at > NOW() - INTERVAL '1 hour'
    ) < m.hourly_limit
  ORDER BY e.scheduled_at, e.id
  LIMIT $1
  FOR UPDATE OF e SKIP LOCKED
)
UPDATE scheduled_emails e
SET status = 'sending',
    leased_by = $2,
    leased_until = NOW() + ($3::int * INTERVAL '1 second'),
    attempts = e.attempts + 1,
    idempotency_key = COALESCE(e.idempotency_key, gen_random_uuid()),
    updated_at = NOW()
FROM claimable c
WHERE e.id = c.id
RETURNING e.*;
`;

export async function claimBatch(
  client: pg.Pool | pg.PoolClient,
  workerId: string,
  batchSize: number,
  leaseTtlSeconds: number,
  maxAttempts: number,
): Promise<ScheduledEmailRow[]> {
  const result = await client.query<ScheduledEmailRow>(CLAIM_SQL, [
    batchSize,
    workerId,
    leaseTtlSeconds,
    maxAttempts,
  ]);
  return result.rows;
}

export async function releaseLeasesForWorker(
  client: pg.Pool | pg.PoolClient,
  workerId: string,
): Promise<number> {
  const result = await client.query<{ id: number }>(
    `
    UPDATE scheduled_emails
    SET status = 'failed',
        leased_by = NULL,
        leased_until = NULL,
        next_retry_at = NOW(),
        updated_at = NOW()
    WHERE leased_by = $1
      AND status = 'sending'
    RETURNING id
    `,
    [workerId],
  );
  return result.rowCount ?? result.rows.length;
}
