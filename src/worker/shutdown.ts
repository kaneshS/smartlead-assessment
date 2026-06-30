export interface ShutdownController {
  isShuttingDown(): boolean;
  waitForShutdown(): Promise<void>;
  onShutdown(fn: () => void | Promise<void>): void;
  triggerShutdown(): void;
}

export function createShutdownController(options?: { registerSignals?: boolean }): ShutdownController {
  let shuttingDown = false;
  const handlers: Array<() => void | Promise<void>> = [];

  const trigger = () => {
    if (shuttingDown) return;
    shuttingDown = true;
    for (const fn of handlers) {
      void Promise.resolve(fn()).catch((err) => {
        console.error(JSON.stringify({ event: 'shutdown_handler_error', error: String(err) }));
      });
    }
  };

  if (options?.registerSignals !== false) {
    process.once('SIGTERM', trigger);
    process.once('SIGINT', trigger);
  }

  return {
    isShuttingDown: () => shuttingDown,
    waitForShutdown: () =>
      new Promise((resolve) => {
        if (shuttingDown) {
          resolve();
          return;
        }
        handlers.push(resolve);
      }),
    onShutdown: (fn) => {
      handlers.push(fn);
    },
    triggerShutdown: trigger,
  };
}
