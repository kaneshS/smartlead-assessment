import type pg from 'pg';
import { config } from '../src/config.js';

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

export async function runSeed(pool: pg.Pool): Promise<void> {
  const { mailboxes: mailboxCount, campaigns, emails, dueRatio } = config.seed;

  await pool.query('BEGIN');
  try {
    await pool.query('TRUNCATE scheduled_emails RESTART IDENTITY CASCADE');
    await pool.query('TRUNCATE mailboxes RESTART IDENTITY CASCADE');

    const mailboxIds: number[] = [];
    for (let i = 1; i <= mailboxCount; i++) {
      const hourlyLimit = randomInt(30, 60);
      const result = await pool.query<{ id: number }>(
        `INSERT INTO mailboxes (email_address, hourly_limit)
         VALUES ($1, $2)
         RETURNING id`,
        [`mailbox${i}@example.com`, hourlyLimit],
      );
      mailboxIds.push(result.rows[0]!.id);
    }

    const batchSize = 500;
    let inserted = 0;

    while (inserted < emails) {
      const chunk = Math.min(batchSize, emails - inserted);
      const values: unknown[] = [];
      const placeholders: string[] = [];

      for (let i = 0; i < chunk; i++) {
        const idx = inserted + i;
        const campaignId = (idx % campaigns) + 1;
        const mailboxId = mailboxIds[idx % mailboxIds.length]!;
        const dueNow = Math.random() < dueRatio;
        const scheduledAt = dueNow
          ? new Date(Date.now() - randomInt(0, 3600_000))
          : new Date(Date.now() + randomInt(60_000, 7 * 24 * 3600_000));

        const base = placeholders.length * 6;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6})`,
        );
        values.push(
          campaignId,
          mailboxId,
          `recipient${idx}@example.com`,
          `Campaign ${campaignId} outreach #${idx}`,
          `Hello recipient ${idx}, this is a seeded message.`,
          scheduledAt,
        );
      }

      await pool.query(
        `INSERT INTO scheduled_emails
          (campaign_id, mailbox_id, to_address, subject, body, scheduled_at)
         VALUES ${placeholders.join(', ')}`,
        values,
      );
      inserted += chunk;
    }

    await pool.query('COMMIT');

    console.log(
      JSON.stringify({
        event: 'seed_complete',
        mailboxes: mailboxCount,
        campaigns,
        emails: inserted,
        due_ratio: dueRatio,
      }),
    );
  } catch (err) {
    await pool.query('ROLLBACK');
    throw err;
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { getPool, closePool } = await import('../src/db/pool.js');
  runSeed(getPool())
    .then(() => closePool())
    .catch(async (err) => {
      console.error(err);
      await closePool();
      process.exit(1);
    });
}
