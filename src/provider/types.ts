export interface EmailMessage {
  to: string;
  subject: string;
  body: string;
  mailboxId: number;
}

export interface ProviderSuccess {
  kind: 'success';
  messageId: string;
}

export interface ProviderHardBounce {
  kind: 'hard_bounce';
  reason: string;
}

export interface ProviderThrottled {
  kind: 'throttled';
  reason: string;
}

export interface ProviderTransient {
  kind: 'transient';
  reason: string;
}

export interface ProviderAmbiguous {
  kind: 'ambiguous';
  message: string;
}

export type ProviderSendResult =
  | ProviderSuccess
  | ProviderHardBounce
  | ProviderThrottled
  | ProviderTransient
  | ProviderAmbiguous;

export interface ProviderStatus {
  messageId: string;
  status: 'accepted';
}

export interface EmailProvider {
  send(idempotencyKey: string, message: EmailMessage): Promise<ProviderSendResult>;
  getStatus(idempotencyKey: string): ProviderStatus | null;
}
