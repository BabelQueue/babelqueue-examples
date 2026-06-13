// Consumer (Node) — reads the same RabbitMQ "orders" queue a Python service produced to,
// and routes each message to a handler by URN. It never knows which language wrote the
// message; it only sees the canonical envelope and routes on the AMQP `properties.type`
// (the URN) per §2 of the broker-bindings contract. Consume is basic.get + manual ack.
//
//   npm install
//   node consume.mjs
//
// BROKER_URL (default amqp://guest:guest@localhost:5672/) and CONSUME_MESSAGES (default 4)
// are env-configurable.

import amqp from "amqplib";
import { RabbitMQConsumer } from "@babelqueue/rabbitmq";

const BROKER_URL = process.env.BROKER_URL || "amqp://guest:guest@localhost:5672/";
const QUEUE = "orders";
const TARGET = Number(process.env.CONSUME_MESSAGES || 4);

const connection = await amqp.connect(BROKER_URL);
const channel = await connection.createChannel();
await channel.assertQueue(QUEUE, { durable: true });

let consumed = 0;

const handlers = {
  "urn:babel:orders:created": (envelope) => {
    consumed++;
    console.log(
      `[node] orders:created  order_id=${envelope.data.order_id}  amount=${envelope.data.amount}` +
        `  from lang=${envelope.meta.lang}  trace=${envelope.trace_id}  attempts=${envelope.attempts}`,
    );
  },
  "urn:babel:orders:shipped": (envelope) => {
    consumed++;
    console.log(
      `[node] orders:shipped  order_id=${envelope.data.order_id}  carrier=${envelope.data.carrier}` +
        `  from lang=${envelope.meta.lang}`,
    );
  },
};

const consumer = new RabbitMQConsumer(channel, QUEUE, handlers);

console.log(`[node] consuming from '${QUEUE}' until ${TARGET} message(s) handled...`);

// Poll until we've handled the expected number of messages (or the queue stays empty
// long enough that we know the producer is done).
let emptyPolls = 0;
while (consumed < TARGET && emptyPolls < 20) {
  const handled = await consumer.poll();
  if (handled) {
    emptyPolls = 0;
  } else {
    emptyPolls++;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

console.log(`[node] done — handled ${consumed} message(s).`);

await channel.close();
await connection.close();
