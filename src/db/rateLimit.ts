import type pg from 'pg';

/** Rolling-hour sent count for a mailbox (used by tests and diagnostics). */
export async function sentCountLastHour(
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
