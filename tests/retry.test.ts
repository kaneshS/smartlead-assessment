import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { computeRetryAt, shouldDeadLetter } from '../src/worker/retry.js';

describe('retry', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('shouldDeadLetter', () => {
    it('returns true when attempts equals maxAttempts', () => {
      expect(shouldDeadLetter(5, 5)).toBe(true);
    });

    it('returns true when attempts exceeds maxAttempts', () => {
      expect(shouldDeadLetter(6, 5)).toBe(true);
    });

    it('returns false when attempts is below maxAttempts', () => {
      expect(shouldDeadLetter(4, 5)).toBe(false);
      expect(shouldDeadLetter(1, 5)).toBe(false);
    });
  });

  describe('computeRetryAt', () => {
    const fixedNow = new Date('2026-06-30T12:00:00.000Z').getTime();

    beforeEach(() => {
      vi.spyOn(Date, 'now').mockReturnValue(fixedNow);
      vi.spyOn(Math, 'random').mockReturnValue(0);
    });

    it('uses 30s base backoff for transient failures on first attempt', () => {
      const retryAt = computeRetryAt(1, false);
      expect(retryAt.getTime()).toBe(fixedNow + 30_000);
    });

    it('exponentially increases transient backoff by attempt', () => {
      expect(computeRetryAt(1, false).getTime()).toBe(fixedNow + 30_000);
      expect(computeRetryAt(2, false).getTime()).toBe(fixedNow + 60_000);
      expect(computeRetryAt(3, false).getTime()).toBe(fixedNow + 120_000);
      expect(computeRetryAt(4, false).getTime()).toBe(fixedNow + 240_000);
    });

    it('uses 300s base backoff for throttled failures', () => {
      expect(computeRetryAt(1, true).getTime()).toBe(fixedNow + 300_000);
      expect(computeRetryAt(2, true).getTime()).toBe(fixedNow + 600_000);
    });

    it('caps backoff at 900 seconds', () => {
      expect(computeRetryAt(10, false).getTime()).toBe(fixedNow + 900_000);
      expect(computeRetryAt(10, true).getTime()).toBe(fixedNow + 900_000);
    });

    it('adds jitter up to 5000ms', () => {
      vi.spyOn(Math, 'random').mockReturnValue(0.999);
      const retryAt = computeRetryAt(1, false);
      expect(retryAt.getTime()).toBe(fixedNow + 30_000 + Math.floor(0.999 * 5000));
    });

    it('treats attempts below 1 as first backoff tier', () => {
      expect(computeRetryAt(0, false).getTime()).toBe(fixedNow + 30_000);
    });
  });
});
