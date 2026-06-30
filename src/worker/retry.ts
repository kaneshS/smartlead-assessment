const BASE_BACKOFF_MS = 30_000;
const THROTTLED_BASE_BACKOFF_MS = 300_000;
const MAX_BACKOFF_MS = 900_000;

function jitterMs(maxJitterMs = 5000): number {
  return Math.floor(Math.random() * maxJitterMs);
}

export function computeRetryAt(attempts: number, throttled = false): Date {
  const base = throttled ? THROTTLED_BASE_BACKOFF_MS : BASE_BACKOFF_MS;
  const exp = Math.min(MAX_BACKOFF_MS, base * 2 ** Math.max(0, attempts - 1));
  return new Date(Date.now() + exp + jitterMs());
}

export function shouldDeadLetter(attempts: number, maxAttempts: number): boolean {
  return attempts >= maxAttempts;
}
