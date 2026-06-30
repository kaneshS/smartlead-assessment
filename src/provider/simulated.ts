import { createHash } from 'node:crypto';
import type { EmailMessage, EmailProvider, ProviderSendResult, ProviderStatus } from './types.js';

export interface SimulatedProviderConfig {
  successRate: number;
  transientRate: number;
  throttledRate: number;
  hardBounceRate: number;
  ambiguousRate: number;
  latencyMinMs: number;
  latencyMaxMs: number;
  rngSeed?: string | null;
}

type OutcomeKind = 'success' | 'transient' | 'throttled' | 'hard_bounce' | 'ambiguous';

interface StoredSend {
  messageId: string;
  accepted: boolean;
}

function hashKey(key: string): Buffer {
  return createHash('sha256').update(key).digest();
}

function messageIdForKey(key: string): string {
  return `msg-${hashKey(key).subarray(0, 16).toString('hex')}`;
}

function seededRoll(key: string, seed: string | null | undefined): number {
  const h = seed
    ? createHash('sha256').update(`${seed}:${key}`).digest()
    : hashKey(key);
  return h[0]! / 255;
}

function outcomeForRoll(roll: number, cfg: SimulatedProviderConfig): OutcomeKind {
  let cursor = 0;
  const bands: Array<[number, OutcomeKind]> = [
    [cfg.successRate, 'success'],
    [cfg.transientRate, 'transient'],
    [cfg.throttledRate, 'throttled'],
    [cfg.hardBounceRate, 'hard_bounce'],
    [cfg.ambiguousRate, 'ambiguous'],
  ];

  for (const [rate, kind] of bands) {
    cursor += rate;
    if (roll < cursor) return kind;
  }
  return 'success';
}

function latencyForKey(key: string, cfg: SimulatedProviderConfig): number {
  const span = Math.max(0, cfg.latencyMaxMs - cfg.latencyMinMs);
  const h = hashKey(`${key}:latency`);
  return cfg.latencyMinMs + (h[1]! % (span + 1));
}

export class SimulatedProvider implements EmailProvider {
  private readonly store = new Map<string, StoredSend>();
  private readonly cfg: SimulatedProviderConfig;

  constructor(cfg: SimulatedProviderConfig) {
    this.cfg = cfg;
  }

  async send(idempotencyKey: string, message: EmailMessage): Promise<ProviderSendResult> {
    const existing = this.store.get(idempotencyKey);
    if (existing?.accepted) {
      return { kind: 'success', messageId: existing.messageId };
    }

    await this.sleep(latencyForKey(idempotencyKey, this.cfg));

    const roll = seededRoll(idempotencyKey, this.cfg.rngSeed);
    const outcome = outcomeForRoll(roll, this.cfg);
    const messageId = messageIdForKey(idempotencyKey);

    switch (outcome) {
      case 'success':
        this.store.set(idempotencyKey, { messageId, accepted: true });
        return { kind: 'success', messageId };

      case 'transient':
        return { kind: 'transient', reason: 'Simulated transient provider error (5xx/timeout)' };

      case 'throttled':
        return { kind: 'throttled', reason: `Mailbox ${message.mailboxId} throttled by provider` };

      case 'hard_bounce':
        return { kind: 'hard_bounce', reason: `Hard bounce for ${message.to}` };

      case 'ambiguous': {
        this.store.set(idempotencyKey, { messageId, accepted: true });
        return {
          kind: 'ambiguous',
          message: 'Provider accepted message but client timed out before ack',
        };
      }
    }
  }

  getStatus(idempotencyKey: string): ProviderStatus | null {
    const stored = this.store.get(idempotencyKey);
    if (stored?.accepted) {
      return { messageId: stored.messageId, status: 'accepted' };
    }
    return null;
  }

  /** Test helper: count accepted sends in this process. */
  acceptedCount(): number {
    let n = 0;
    for (const v of this.store.values()) {
      if (v.accepted) n++;
    }
    return n;
  }

  /** Test helper: all accepted message IDs. */
  acceptedMessageIds(): string[] {
    return [...this.store.values()]
      .filter((v) => v.accepted)
      .map((v) => v.messageId);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
