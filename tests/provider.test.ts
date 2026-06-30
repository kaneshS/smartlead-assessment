import { describe, expect, it } from 'vitest';
import { SimulatedProvider } from '../src/provider/simulated.js';

function provider(overrides: Partial<ConstructorParameters<typeof SimulatedProvider>[0]> = {}) {
  return new SimulatedProvider({
    successRate: 0,
    transientRate: 0,
    throttledRate: 0,
    hardBounceRate: 0,
    ambiguousRate: 0,
    latencyMinMs: 0,
    latencyMaxMs: 0,
    ...overrides,
  });
}

const message = {
  to: 'user@example.com',
  subject: 'Hello',
  body: 'World',
  mailboxId: 1,
};

describe('SimulatedProvider', () => {
  describe('outcome bands', () => {
    it('returns success when roll falls in success band', async () => {
      const p = provider({ successRate: 1 });
      const result = await p.send('key-success', message);
      expect(result.kind).toBe('success');
      if (result.kind === 'success') {
        expect(result.messageId).toMatch(/^msg-[a-f0-9]{32}$/);
      }
      expect(p.acceptedCount()).toBe(1);
    });

    it('returns transient without accepting the send', async () => {
      const p = provider({ transientRate: 1 });
      const result = await p.send('key-transient', message);
      expect(result).toEqual({
        kind: 'transient',
        reason: 'Simulated transient provider error (5xx/timeout)',
      });
      expect(p.acceptedCount()).toBe(0);
      expect(p.getStatus('key-transient')).toBeNull();
    });

    it('returns throttled without accepting the send', async () => {
      const p = provider({ throttledRate: 1 });
      const result = await p.send('key-throttled', message);
      expect(result.kind).toBe('throttled');
      if (result.kind === 'throttled') {
        expect(result.reason).toContain('Mailbox 1');
      }
      expect(p.acceptedCount()).toBe(0);
    });

    it('returns hard_bounce without accepting the send', async () => {
      const p = provider({ hardBounceRate: 1 });
      const result = await p.send('key-hard', message);
      expect(result.kind).toBe('hard_bounce');
      if (result.kind === 'hard_bounce') {
        expect(result.reason).toContain('user@example.com');
      }
      expect(p.acceptedCount()).toBe(0);
    });

    it('returns ambiguous after accepting server-side', async () => {
      const p = provider({ ambiguousRate: 1 });
      const result = await p.send('key-ambiguous', message);
      expect(result.kind).toBe('ambiguous');
      expect(p.acceptedCount()).toBe(1);
      expect(p.getStatus('key-ambiguous')).toEqual({
        messageId: p.acceptedMessageIds()[0],
        status: 'accepted',
      });
    });
  });

  describe('idempotency', () => {
    it('deduplicates accepted sends for the same key', async () => {
      const p = provider({ successRate: 1 });
      const first = await p.send('dup-key', message);
      const second = await p.send('dup-key', message);
      expect(first).toEqual(second);
      expect(p.acceptedCount()).toBe(1);
    });

    it('produces stable message IDs for the same idempotency key', async () => {
      const p = provider({ successRate: 1 });
      const a = await p.send('stable-key', message);
      const b = await p.send('stable-key', message);
      if (a.kind === 'success' && b.kind === 'success') {
        expect(a.messageId).toBe(b.messageId);
      }
    });

    it('does not deduplicate transient failures until success is stored', async () => {
      const p = provider({ transientRate: 1 });
      await p.send('retry-key', message);
      await p.send('retry-key', message);
      expect(p.acceptedCount()).toBe(0);
    });
  });

  describe('getStatus', () => {
    it('returns null for keys never accepted', () => {
      const p = provider();
      expect(p.getStatus('missing')).toBeNull();
    });

    it('returns accepted status after success', async () => {
      const p = provider({ successRate: 1 });
      const result = await p.send('status-key', message);
      const status = p.getStatus('status-key');
      expect(status).not.toBeNull();
      if (result.kind === 'success' && status) {
        expect(status.messageId).toBe(result.messageId);
        expect(status.status).toBe('accepted');
      }
    });
  });

  describe('determinism', () => {
    it('uses rngSeed for reproducible outcomes', async () => {
      const cfg = {
        successRate: 0.5,
        transientRate: 0.2,
        throttledRate: 0.1,
        hardBounceRate: 0.1,
        ambiguousRate: 0.1,
        latencyMinMs: 0,
        latencyMaxMs: 0,
        rngSeed: 'test-seed',
      };
      const a = new SimulatedProvider(cfg);
      const b = new SimulatedProvider(cfg);
      const key = 'deterministic-key';
      const resultA = await a.send(key, message);
      const resultB = await b.send(key, message);
      expect(resultA).toEqual(resultB);
    });

    it('applies configured latency before responding', async () => {
      const p = provider({
        successRate: 1,
        latencyMinMs: 40,
        latencyMaxMs: 40,
      });
      const start = Date.now();
      await p.send('latency-key', message);
      expect(Date.now() - start).toBeGreaterThanOrEqual(35);
    });
  });
});
