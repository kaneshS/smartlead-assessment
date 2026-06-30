import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from './config.js';
import { closePool, getPool } from './db/pool.js';
import { SimulatedProvider } from './provider/simulated.js';
import { runWorker } from './worker/loop.js';
import { createShutdownController } from './worker/shutdown.js';
import { runSeed } from '../scripts/seed.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

async function runMigrate(): Promise<void> {
  const pool = getPool();
  const sqlPath = join(__dirname, '..', 'migrations', '001_init.sql');
  const sql = await readFile(sqlPath, 'utf8');
  await pool.query(sql);
  console.log(JSON.stringify({ event: 'migration_complete' }));
  await closePool();
}

async function runWorkerCommand(): Promise<void> {
  const pool = getPool();
  const provider = new SimulatedProvider(config.provider);
  const shutdown = createShutdownController();

  await runWorker({
    pool,
    provider,
    workerId: config.worker.id,
    batchSize: config.worker.batchSize,
    pollIntervalMs: config.worker.pollIntervalMs,
    leaseTtlSeconds: config.worker.leaseTtlSeconds,
    maxAttempts: config.worker.maxAttempts,
    shutdownTimeoutMs: config.worker.shutdownTimeoutMs,
    metricsSummaryIntervalMs: config.metrics.summaryIntervalMs,
    shutdown,
  });

  await closePool();
}

async function main(): Promise<void> {
  const command = process.argv[2] ?? 'worker';

  switch (command) {
    case 'worker':
      await runWorkerCommand();
      break;
    case 'migrate':
      await runMigrate();
      break;
    case 'seed':
      await runSeed(getPool());
      await closePool();
      break;
    default:
      console.error(`Unknown command: ${command}`);
      process.exit(1);
  }
}

main().catch((err) => {
  console.error(JSON.stringify({ event: 'fatal_error', error: String(err), stack: err?.stack }));
  process.exit(1);
});
