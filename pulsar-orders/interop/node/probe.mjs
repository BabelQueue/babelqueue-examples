// Interop probe (Node) for the post-R1 Pulsar exhaustion scenarios (ADR-0036).
//
// A PulsarConsumer on QUEUE whose handler always throws, with dead-lettering to <QUEUE>.dlq. The runner
// publishes with the Go probe and reads the DLQ back with it, so the record is asserted from another SDK.
// A poison body never reaches the handler.
//
// Environment contract (shared with Probe.java):
//
//   PULSAR_URL     pulsar://localhost:6650
//   QUEUE          queue (topic) name
//   SUBSCRIPTION   subscription name (default babelqueue)
//   SUB_TYPE       Exclusive (default) | Failover | Shared | Key_Shared
//   RETRY          republish (default): the consumer is given the client and re-publishes with bq-attempts + 1;
//                  nack: no client, the subscription is declared Shared and a retry is a negative acknowledgement
//   MAX_ATTEMPTS   default 3
//   RUN_S          seconds to keep consuming before exiting 0 (default 120; the runner kills it earlier)
//
// Output lines (parsed by scripts/lib/interop-common.sh):
//
//   PROBE node READY sub_type=<type> retry=<mode> max_attempts=<n>
//   PROBE node HANDLED id=<meta.id> attempts=<envelope.attempts> redelivery=<native count>
//   PROBE node ERROR kind=<error name>
//   PROBE node WARN <message>
//   PROBE node DONE
import Pulsar from "pulsar-client";
import { PulsarConsumer } from "@babelqueue/pulsar";

const env = (key, def) => (process.env[key] ? process.env[key] : def);
const token = (s) => (s && String(s).trim() !== "" ? String(s).trim().replace(/\s+/g, "_") : "-");

const url = env("PULSAR_URL", "pulsar://localhost:6650");
const queue = env("QUEUE", "orders");
const type = env("SUB_TYPE", "Exclusive");
const republish = env("RETRY", "republish") === "republish";
const maxAttempts = Number.parseInt(env("MAX_ATTEMPTS", "3"), 10);
const runUntil = Date.now() + Number.parseInt(env("RUN_S", "120"), 10) * 1000;

const client = new Pulsar.Client({ serviceUrl: url, operationTimeoutSeconds: 30 });
const subscription = await client.subscribe({
  topic: queue,
  subscription: env("SUBSCRIPTION", "babelqueue"),
  // pulsar-client names the type KeyShared; the adapter accepts either spelling.
  subscriptionType: type === "Key_Shared" ? "KeyShared" : type,
  subscriptionInitialPosition: "Earliest",
  // The nack path redelivers after this delay (one minute by default).
  nAckRedeliverTimeoutMs: 1000,
});
const deadLetter = await client.createProducer({ topic: `${queue}.dlq` });

const options = {
  maxAttempts,
  deadLetterProducer: deadLetter,
  queue,
  receiveTimeoutMs: 500,
  subscriptionType: type,
  onError: (error) => console.log(`PROBE node ERROR kind=${token(error?.name)}`),
  onWarn: (message) => console.log(`PROBE node WARN ${token(message)}`),
};
// republish: the consumer builds its own retry producer from the client, so the cap holds on Exclusive.
// nack: no producer, so the declared subscription type must be one the broker counts redeliveries on.
// `client` builds the retry producer only together with `topic` (the topic the subscription reads):
// pulsar-client's Consumer has no getTopic(), so without `topic` the constructor fails closed on Exclusive.
if (republish) {
  options.client = client;
  options.topic = queue;
}

const consumer = new PulsarConsumer(
  subscription,
  {
    "urn:babel:orders:created": async (envelope, message) => {
      console.log(
        `PROBE node HANDLED id=${envelope.meta.id} attempts=${envelope.attempts} redelivery=${message.getRedeliveryCount()}`,
      );
      throw new Error("probe handler always fails");
    },
  },
  options,
);

console.log(`PROBE node READY sub_type=${type} retry=${republish ? "republish" : "nack"} max_attempts=${maxAttempts}`);
while (Date.now() < runUntil) {
  try {
    await consumer.poll();
  } catch (error) {
    console.log(`PROBE node ERROR kind=${token(error?.name)}`);
  }
}
await subscription.close();
await deadLetter.close();
await client.close();
console.log("PROBE node DONE");
