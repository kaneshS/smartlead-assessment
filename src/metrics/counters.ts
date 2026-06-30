export interface WorkerCounters {
  claims: number;
  sent: number;
  retried: number;
  failed: number;
  dead: number;
  ambiguousRecovered: number;
}

export class Metrics {
  private readonly counters: WorkerCounters = {
    claims: 0,
    sent: 0,
    retried: 0,
    failed: 0,
    dead: 0,
    ambiguousRecovered: 0,
  };

  private readonly startedAt = Date.now();
  private summaryTimer: ReturnType<typeof setInterval> | null = null;

  increment(field: keyof WorkerCounters, by = 1): void {
    this.counters[field] += by;
  }

  snapshot(): WorkerCounters & { uptimeSec: number; emailsPerMinute: number } {
    const uptimeSec = (Date.now() - this.startedAt) / 1000;
    const emailsPerMinute = uptimeSec > 0 ? (this.counters.sent / uptimeSec) * 60 : 0;
    return { ...this.counters, uptimeSec, emailsPerMinute };
  }

  startPeriodicSummary(intervalMs: number, workerId: string): void {
    if (this.summaryTimer) return;
    this.summaryTimer = setInterval(() => this.logSummary(workerId), intervalMs);
    this.summaryTimer.unref?.();
  }

  stopPeriodicSummary(): void {
    if (this.summaryTimer) {
      clearInterval(this.summaryTimer);
      this.summaryTimer = null;
    }
  }

  logSummary(workerId: string): void {
    const snap = this.snapshot();
    console.log(
      JSON.stringify({
        event: 'metrics_summary',
        worker_id: workerId,
        ...snap,
        emails_per_minute: Number(snap.emailsPerMinute.toFixed(2)),
      }),
    );
  }
}

export function logEvent(payload: Record<string, unknown>): void {
  console.log(JSON.stringify({ ts: new Date().toISOString(), ...payload }));
}
