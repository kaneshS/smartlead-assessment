export interface ScheduledEmailRow {
  id: number;
  campaign_id: number;
  mailbox_id: number;
  to_address: string;
  subject: string;
  body: string;
  scheduled_at: Date;
  status: 'pending' | 'sending' | 'sent' | 'failed' | 'dead';
  attempts: number;
  idempotency_key: string | null;
  provider_message_id: string | null;
  leased_by: string | null;
  leased_until: Date | null;
  next_retry_at: Date | null;
  last_error: string | null;
  sent_at: Date | null;
  created_at: Date;
  updated_at: Date;
}
