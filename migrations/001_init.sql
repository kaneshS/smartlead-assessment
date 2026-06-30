CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS mailboxes (
  id SERIAL PRIMARY KEY,
  email_address TEXT NOT NULL UNIQUE,
  hourly_limit INTEGER NOT NULL CHECK (hourly_limit > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scheduled_emails (
  id BIGSERIAL PRIMARY KEY,
  campaign_id INTEGER NOT NULL,
  mailbox_id INTEGER NOT NULL REFERENCES mailboxes(id),
  to_address TEXT NOT NULL,
  subject TEXT NOT NULL,
  body TEXT NOT NULL,
  scheduled_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'sending', 'sent', 'failed', 'dead')),
  attempts INTEGER NOT NULL DEFAULT 0,
  idempotency_key UUID,
  provider_message_id TEXT,
  leased_by TEXT,
  leased_until TIMESTAMPTZ,
  next_retry_at TIMESTAMPTZ,
  last_error TEXT,
  sent_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scheduled_emails_claimable
  ON scheduled_emails (scheduled_at, mailbox_id)
  WHERE status IN ('pending', 'failed');

CREATE INDEX IF NOT EXISTS idx_scheduled_emails_sending_lease
  ON scheduled_emails (leased_until)
  WHERE status = 'sending';

CREATE INDEX IF NOT EXISTS idx_scheduled_emails_sent_rate
  ON scheduled_emails (mailbox_id, sent_at)
  WHERE status = 'sent';

CREATE INDEX IF NOT EXISTS idx_scheduled_emails_status
  ON scheduled_emails (status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_scheduled_emails_idempotency_key
  ON scheduled_emails (idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_scheduled_emails_provider_message_id
  ON scheduled_emails (provider_message_id)
  WHERE provider_message_id IS NOT NULL;
