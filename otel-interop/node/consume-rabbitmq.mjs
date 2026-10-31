// Consumer (Node) for the OTel interop on RabbitMQ: handles the messages a Python producer sent
// inside a CONSUMER span "process <queue>" and writes the finished spans to OTEL_SPAN_FILE.
//
//   BROKER_URL=amqp://guest:guest@localhost:5672/ OTEL_SPAN_FILE=consumer.jsonl node consume-rabbitmq.mjs
//
// The Python producer carried a W3C traceparent in the AMQP message headers (ADR-0028). The
// @babelqueue/rabbitmq adapter hands the delivered headers to the handler as its third argument and
// wrapHandler starts the span as a true child of the producer span. `system: "rabbitmq"` is the
// broker name of ADR-0038 section 3 (messaging.system).
import amqp from "amqplib";
import { RabbitMQConsumer } from "@babelqueue/rabbitmq";
import { wrapHandler, SEMCONV_SCHEMA_URL } from "@babelqueue/core/otel";
import { BasicTracerProvider, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { JsonLinesSpanExporter } from "./spanfile.mjs";

const spanFile = process.env.OTEL_SPAN_FILE;
if (!spanFile) {
  console.error("OTEL_SPAN_FILE is required");
  process.exit(2);
}
const brokerUrl = process.env.BROKER_URL || "amqp://guest:guest@localhost:5672/";
const queue = process.env.QUEUE || "orders";
const expected = Number(process.env.OTEL_MESSAGES || 3);
const timeoutMs = Number(process.env.CONSUME_TIMEOUT || 60) * 1000;

const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(new JsonLinesSpanExporter(spanFile))] });
const tracer = provider.getTracer("otel-interop-consumer", undefined, { schemaUrl: SEMCONV_SCHEMA_URL });

const connection = await amqp.connect(brokerUrl);
const channel = await connection.createChannel();
await channel.assertQueue(queue, { durable: true });

let consumed = 0;
const process_ = async (envelope) => {
  consumed++;
  console.log(`PROBE node HANDLED id=${envelope.meta.id} order_id=${envelope.data.order_id}`);
};

const handlers = {
  "urn:babel:orders:created": (envelope, _message, headers) =>
    wrapHandler(tracer, process_, headers, { system: "rabbitmq" })(envelope),
};
const consumer = new RabbitMQConsumer(channel, queue, handlers);

console.log("PROBE node READY");
const deadline = Date.now() + timeoutMs;
while (consumed < expected && Date.now() < deadline) {
  if (!(await consumer.poll())) {
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

await provider.forceFlush();
await provider.shutdown();
await channel.close();
await connection.close();
console.log(`PROBE node DONE consumed=${consumed}`);
if (consumed < expected) {
  console.error(`consumed ${consumed} of ${expected} message(s)`);
  process.exit(1);
}
