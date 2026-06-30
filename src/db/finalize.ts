import type pg from 'pg';

export async function markSent(
  client: pg.Pool | pg.PoolClient,
  emailId: number,
  providerMessageId: string,
): Promise<void> {
  await client.query(
    `
    UPDATE scheduled_emails
    SET status = 'sent',
        provider_message_id = $2,
        sent_at = NOW(),
        leased_by = NULL,
        leased_until = NULL,
        last_error = NULL,
        updated_at = NOW()
    WHERE id = $1
    `,
    [emailId, providerMessageId],
  );
}

export async function markFailed(
  client: pg.Pool | pg.PoolClient,
  emailId: number,
  nextRetryAt: Date,
  lastError: string,
): Promise<void> {
  await client.query(
    `
    UPDATE scheduled_emails
    SET status = 'failed',
        next_retry_at = $2,
        last_error = $3,
        leased_by = NULL,
        leased_until = NULL,
        updated_at = NOW()
    WHERE id = $1
    `,
    [emailId, nextRetryAt, lastError],
  );
}

export async function markDead(
  client: pg.Pool | pg.PoolClient,
  emailId: number,
  lastError: string,
): Promise<void> {
  await client.query(
    `
    UPDATE scheduled_emails
    SET status = 'dead',
        last_error = $2,
        leased_by = NULL,
        leased_until = NULL,
        updated_at = NOW()
    WHERE id = $1
    `,
    [emailId, lastError],
  );
}

export async function countSentInLastHour(
  client: pg.Pool | pg.PoolClient,
  mailboxId: number,
): Promise<number> {
  const result = await client.query<{ count: string }>(
    `
    SELECT COUNT(*)::text AS count
    FROM scheduled_emails
    WHERE mailbox_id = $1
      AND status = 'sent'
      AND sent_at > NOW() - INTERVAL '1 hour'
    `,
    [mailboxId],
  );
  return Number.parseInt(result.rows[0]?.count ?? '0', 10);
}
