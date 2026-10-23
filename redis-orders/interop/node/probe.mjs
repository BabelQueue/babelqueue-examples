// Interop probe (Node) for the R1 Redis scenarios.
// Environment contract and output lines: see ../go/main.go.
import { Redis } from "ioredis";
import { RedisConsumer } from "@babelqueue/redis";

const URN = "urn:babel:orders:created";
const url = process.env.REDIS_URL || "redis://localhost:6379/0";
const queue = process.env.QUEUE || "orders";
const layout = (process.env.LAYOUT || "zset").toLowerCase();
const retryAfterEnv = process.env.RETRY_AFTER || "";
const mode = process.env.MODE || "consume";
const hold = Number(process.env.HOLD || 0);
const timeout = Number(process.env.TIMEOUT || 30);

const retryAfter = retryAfterEnv === "none" ? null : retryAfterEnv === "" ? undefined : Number(retryAfterEnv);

const client = new Redis(url);
let done = false;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const consumer = new RedisConsumer(
  client,
  queue,
  {
    [URN]: async (envelope) => {
      const id = envelope.meta?.id ?? "";
      console.log(`PROBE node RECEIVED id=${id} attempts=${envelope.attempts} at_ms=${Date.now()}`);
      if (mode === "hang") {
        await new Promise(() => {}); // the runner kill -9's us while the reservation is held
      }
      if (mode === "slow") {
        await sleep(hold * 1000);
      }
      done = true;
      // The SDK acks after this returns; ACKED is logged by the caller below.
      globalThis.__probeId = id;
    },
  },
  {
    layout,
    retryAfter,
    blockTimeout: 1,
    onWarn: (message) => console.warn(`PROBE node WARN ${message}`),
    onError: (error) => console.error(`PROBE node ERROR ${error}`),
  },
);

console.log("PROBE node READY");
const deadline = Date.now() + timeout * 1000;
while (!done && Date.now() < deadline) {
  await consumer.poll();
}
await client.quit();
if (!done) {
  console.log("PROBE node TIMEOUT");
  process.exit(3);
}
console.log(`PROBE node ACKED id=${globalThis.__probeId}`);
