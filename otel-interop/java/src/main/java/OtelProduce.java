import com.babelqueue.artemis.ArtemisPublisher;
import com.babelqueue.kafka.KafkaPublisher;
import com.babelqueue.otel.HeaderSender;
import com.babelqueue.otel.Tracing;
import com.babelqueue.pulsar.PulsarPublisher;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import java.util.Map;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.pulsar.client.api.PulsarClient;

/**
 * Producer (Java) for the OTel interop: publishes inside a PRODUCER span {@code send <queue>} and
 * writes the finished spans to {@code OTEL_SPAN_FILE}.
 *
 * <pre>BROKER=kafka|pulsar|artemis OTEL_SPAN_FILE=producer.jsonl mvn -q exec:java</pre>
 *
 * {@code Tracing.Options.system} is the broker name of ADR-0038 section 3 ({@code messaging.system}).
 * The transports' {@code publishWithHeaders} carries the {@code traceparent} out of band
 * (ADR-0028): a Kafka record header, a Pulsar message property, a JMS message property. The consumer
 * in another language must therefore start its span as a true child of the span written here.
 * Defaults target the compose files of the matching *-orders example; KAFKA_BROKERS, PULSAR_URL and
 * ARTEMIS_JMS_URL (the JMS broker URL) override them.
 */
public final class OtelProduce {
    private static final String URN = "urn:babel:orders:created";

    public static void main(String[] args) throws Exception {
        String broker = env("BROKER", "");
        String spanFile = env("OTEL_SPAN_FILE", "");
        if (spanFile.isEmpty()) {
            System.err.println("OTEL_SPAN_FILE is required");
            System.exit(2);
        }
        String queue = env("QUEUE", "orders");
        int count = Integer.parseInt(env("OTEL_MESSAGES", "3"));

        SdkTracerProvider provider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(new JsonLinesSpanExporter(spanFile)))
            .build();
        Tracer tracer = provider.tracerBuilder("otel-interop-producer").setSchemaUrl(Tracing.SCHEMA_URL).build();
        try {
            switch (broker) {
                case "kafka" -> produceKafka(tracer, queue, count);
                case "pulsar" -> producePulsar(tracer, queue, count);
                case "artemis" -> produceArtemis(tracer, queue, count);
                default -> {
                    System.err.println("unknown BROKER '" + broker + "' (kafka, pulsar, artemis)");
                    System.exit(2);
                }
            }
        } finally {
            provider.forceFlush().join(10, java.util.concurrent.TimeUnit.SECONDS);
            provider.shutdown().join(10, java.util.concurrent.TimeUnit.SECONDS);
        }
        System.out.println("PROBE java DONE published=" + count);
    }

    private static void produceKafka(Tracer tracer, String topic, int count) throws Exception {
        Map<String, Object> config = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BROKERS", "localhost:9092"),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            KafkaPublisher publisher = KafkaPublisher.create(producer, topic);
            publishAll(tracer, topic, count, "kafka", (envelope, headers) -> publisher.publishWithHeaders(envelope, headers));
            producer.flush();
        }
    }

    private static void producePulsar(Tracer tracer, String topic, int count) throws Exception {
        try (PulsarClient client = PulsarClient.builder().serviceUrl(env("PULSAR_URL", "pulsar://localhost:6650")).build();
             org.apache.pulsar.client.api.Producer<byte[]> producer = client.newProducer().topic(topic).create()) {
            PulsarPublisher publisher = PulsarPublisher.create(producer);
            publishAll(tracer, topic, count, "pulsar", (envelope, headers) -> publisher.publishWithHeaders(envelope, headers));
        }
    }

    private static void produceArtemis(Tracer tracer, String queue, int count) throws Exception {
        ConnectionFactory factory = new ActiveMQJMSConnectionFactory(env("ARTEMIS_JMS_URL", "tcp://localhost:61616"), "artemis", "artemis");
        try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(queue));
            ArtemisPublisher publisher = ArtemisPublisher.create(session, producer);
            publishAll(tracer, queue, count, "activemq", (envelope, headers) -> publisher.publishWithHeaders(envelope, headers));
        }
    }

    private static void publishAll(Tracer tracer, String queue, int count, String system, HeaderSender send) throws Exception {
        Tracing.Options options = Tracing.Options.create().system(system);
        for (int i = 1; i <= count; i++) {
            String id = Tracing.publish(tracer, URN, Map.of("order_id", 1000 + i, "amount", 10.0 * i), queue, send, options);
            System.out.printf("PROBE java PUBLISHED id=%s queue=%s system=%s%n", id, queue, system);
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : value;
    }
}
