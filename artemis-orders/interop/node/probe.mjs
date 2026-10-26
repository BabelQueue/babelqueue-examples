// Interop probe (Node) for the ADR-0037 Artemis scenarios (version gate and poison, broker-bindings §7).
//
// An ArtemisConsumer on QUEUE over AMQP 1.0 (rhea) whose handler records that it ran. The runner publishes
// with the Go probe and reads <QUEUE>.dlq back with it, so the record is asserted from another SDK.
// A version-gated or poison delivery must never reach the handler.
//
// Environment contract (shared with the Java and .NET probes):
//
//   ARTEMIS_URL    amqp://artemis:artemis@localhost:5672
//   QUEUE          queue (address) name
//   DLQ            on (default): a DLQ sender on <QUEUE>.dlq; off: no DLQ sender (dead-lettering off)
//   UNKNOWN_URN    optional unknown-URN strategy (delete | fail | release | dead_letter); default: none set
//   RUN_S          seconds to keep consuming before exiting 0 (default 120; the runner kills it earlier)
//
// Output lines (parsed by scripts/lib/interop-common.sh):
//
//   PROBE node READY dlq=<on|off> unknown_urn=<strategy|none>
//   PROBE node HANDLED id=<meta.id> attempts=<envelope.attempts>
//   PROBE node ERROR kind=<error name>
//   PROBE node DONE
import rhea from "rhea";
import { ArtemisConsumer } from "@babelqueue/artemis";

const env = (key, def) => (process.env[key] ? process.env[key] : def);
const token = (s) => (s && String(s).trim() !== "" ? String(s).trim().replace(/\s+/g, "_") : "-");

const url = new URL(env("ARTEMIS_URL", "amqp://artemis:artemis@localhost:5672"));
const queue = env("QUEUE", "orders");
const dlqOn = env("DLQ", "on") !== "off";
const unknownUrn = env("UNKNOWN_URN", "");
const runMs = Number.parseInt(env("RUN_S", "120"), 10) * 1000;

const connection = rhea.connect({
  host: url.hostname,
  port: Number.parseInt(url.port || "5672", 10),
  username: decodeURIComponent(url.username || "artemis"),
  password: decodeURIComponent(url.password || "artemis"),
  reconnect: false,
});
connection.on("connection_error", (context) => console.log(`PROBE node ERROR kind=connection_error ${token(context.error?.message)}`));

const receiver = connection.open_receiver({
  source: { address: queue, capabilities: ["queue"] },
  autoaccept: false,
  credit_window: 10,
});
const options = {
  queue,
  maxTries: 3,
  onError: (error) => console.log(`PROBE node ERROR kind=${token(error?.name)}`),
};
if (dlqOn) {
  options.deadLetterSender = connection.open_sender({ target: { address: `${queue}.dlq`, capabilities: ["queue"] } });
}
if (unknownUrn) options.unknownUrn = unknownUrn;

const consumer = new ArtemisConsumer(
  {
    "urn:babel:orders:created": async (envelope) => {
      console.log(`PROBE node HANDLED id=${envelope.meta.id} attempts=${envelope.attempts}`);
    },
  },
  options,
);
consumer.listen(receiver);

receiver.on("receiver_open", () => {
  console.log(`PROBE node READY dlq=${dlqOn ? "on" : "off"} unknown_urn=${unknownUrn || "none"}`);
});
setTimeout(() => {
  connection.close();
  console.log("PROBE node DONE");
  process.exit(0);
}, runMs);
