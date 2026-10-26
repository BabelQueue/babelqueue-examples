// Interop probe (Node) for the ADR-0037 SQS poison scenarios (broker-bindings §3.5).
//
// An SqsConsumer on QUEUE_URL whose handler records that it ran. The runner publishes with the Go probe and
// reads <QUEUE>-dlq back with it, so the record is asserted from another SDK. A poison delivery must never
// reach the handler.
//
// Environment contract (shared with the Java and .NET probes):
//
//   SQS_ENDPOINT   http://localhost:9324 (ElasticMQ)
//   AWS_REGION     eu-central-1
//   QUEUE_URL      the work queue (from `probe-go setup`)
//   DLQ_URL        the BabelQueue <queue>-dlq (from `probe-go setup`); used only when DLQ=on
//   DLQ            on (default): a DLQ queue URL is configured; off: none (dead-lettering off, left to RedrivePolicy)
//   UNKNOWN_URN    optional unknown-URN strategy (delete | fail | release | dead_letter); default: none set
//   RUN_S          seconds to keep consuming before exiting 0 (default 120; the runner kills it earlier)
//
// Output lines (parsed by scripts/lib/interop-common.sh):
//
//   PROBE node READY dlq=<on|off> unknown_urn=<strategy|none>
//   PROBE node HANDLED id=<meta.id> attempts=<envelope.attempts>
//   PROBE node ERROR kind=<error name>
//   PROBE node DONE
import { SQS } from "@aws-sdk/client-sqs";
import { SqsConsumer } from "@babelqueue/sqs";

const env = (key, def) => (process.env[key] ? process.env[key] : def);
const token = (s) => (s && String(s).trim() !== "" ? String(s).trim().replace(/\s+/g, "_") : "-");

const queueUrl = env("QUEUE_URL", "");
const dlqOn = env("DLQ", "on") !== "off";
const unknownUrn = env("UNKNOWN_URN", "");
const runUntil = Date.now() + Number.parseInt(env("RUN_S", "120"), 10) * 1000;
if (!queueUrl) throw new Error("QUEUE_URL is required");

// ElasticMQ accepts any credentials; real SQS uses the configured ones.
const sqs = new SQS({
  region: env("AWS_REGION", "eu-central-1"),
  endpoint: env("SQS_ENDPOINT", "http://localhost:9324"),
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

const options = {
  waitTimeSeconds: 1,
  onError: (error) => console.log(`PROBE node ERROR kind=${token(error?.name)}`),
};
if (dlqOn) options.deadLetterQueueUrl = env("DLQ_URL", "");
if (unknownUrn) options.unknownUrn = unknownUrn;

const consumer = new SqsConsumer(
  sqs,
  queueUrl,
  {
    "urn:babel:orders:created": async (envelope) => {
      console.log(`PROBE node HANDLED id=${envelope.meta.id} attempts=${envelope.attempts}`);
    },
  },
  options,
);

console.log(`PROBE node READY dlq=${dlqOn ? "on" : "off"} unknown_urn=${unknownUrn || "none"}`);
while (Date.now() < runUntil) {
  try {
    await consumer.poll();
  } catch (error) {
    console.log(`PROBE node ERROR kind=${token(error?.name)}`);
    await new Promise((resolve) => setTimeout(resolve, 500)); // do not spin while the broker is unreachable
  }
}
console.log("PROBE node DONE");
