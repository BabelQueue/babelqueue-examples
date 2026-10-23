import com.babelqueue.kafka.BabelHandler;
import com.babelqueue.kafka.KafkaConsumer;
import com.babelqueue.kafka.KafkaRetryReinjector;
import com.babelqueue.kafka.RetryTopics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Kafka cross-language retry probe (Java side), R1 Faz 10 scenario 5.
 *
 * <p>Environment: KAFKA_BROKERS, TOPIC (work topic), TIER_MS (retry.1 delay), GROUP (consumer
 * group), URN (handler route), RUN_S (give-up timeout in seconds, exit 3).
 *
 * <p>MODE=fail: a {@link KafkaConsumer} whose handler always throws. The SDK republishes the record
 * to {@code <TOPIC>.retry.1} with {@code attempts + 1} and commits; the probe then exits 0.
 *
 * <p>MODE=reinject: a {@link KafkaRetryReinjector} that forwards due {@code <TOPIC>.retry.1} records
 * to the work topic; the probe exits 0 after the first forwarded record.
 *
 * <p>Output lines: {@code PROBE java READY}, {@code PROBE java FAILED id= attempts= bq_attempts=
 * at_ms=}, {@code PROBE java RETRIED at_ms=}, {@code PROBE java ASSIGNED partitions=} (re-injector only: the
 * group join finished, so the tier clock cannot include JVM start or rebalance time), {@code PROBE java
 * REINJECTED forwarded= at_ms=}, {@code PROBE java TIMEOUT}.
 */
public final class RetryProbe {
    private RetryProbe() {}

    public static void main(String[] args) throws Exception {
        String mode = env("MODE", "fail");
        String brokers = env("KAFKA_BROKERS", "localhost:9092");
        String topic = env("TOPIC", "orders");
        String group = env("GROUP", "bq-interop-" + topic);
        long tierMs = Long.parseLong(env("TIER_MS", "10000"));
        long runMs = Long.parseLong(env("RUN_S", "60")) * 1000L;
        String urn = env("URN", "urn:babel:orders:created");

        RetryTopics retryTopics = RetryTopics.forTopic(topic).tier(Duration.ofMillis(tierMs)).build();

        int rc;
        // Resources are closed before System.exit so the consumer leaves its group cleanly: the next
        // member of the same group (the Python consumer) must not wait for a session timeout.
        try (Consumer<byte[], byte[]> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(Map.of(
                 ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                 ConsumerConfig.GROUP_ID_CONFIG, group,
                 ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                 ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                 ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class));
             Producer<byte[], byte[]> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(Map.of(
                 ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                 ProducerConfig.ACKS_CONFIG, "all",
                 ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                 ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            if ("fail".equals(mode)) {
                rc = runFail(consumer, producer, retryTopics, topic, urn, runMs);
            } else if ("reinject".equals(mode)) {
                rc = runReinject(consumer, producer, retryTopics, runMs);
            } else {
                System.err.println("unknown MODE: " + mode);
                rc = 2;
            }
            producer.flush();
        }
        System.out.flush();
        System.exit(rc);
    }

    private static int runFail(Consumer<byte[], byte[]> consumer, Producer<byte[], byte[]> producer,
                               RetryTopics retryTopics, String topic, String urn, long runMs) {
        AtomicInteger failures = new AtomicInteger();
        BabelHandler alwaysFail = (envelope, record) -> {
            failures.incrementAndGet();
            Header attemptsHeader = record.headers().lastHeader("bq-attempts");
            String headerAttempts = attemptsHeader == null
                ? "-"
                : new String(attemptsHeader.value(), StandardCharsets.UTF_8);
            System.out.printf("PROBE java FAILED id=%s attempts=%d bq_attempts=%s at_ms=%d%n",
                envelope.meta().id(), envelope.attempts(), headerAttempts, System.currentTimeMillis());
            System.out.flush();
            throw new IllegalStateException("interop probe: simulated handler failure");
        };
        KafkaConsumer worker = KafkaConsumer.builder(consumer)
            .handler(urn, alwaysFail)
            .producer(producer)
            .retryTopics(retryTopics)
            .maxTries(3)
            .pollTimeout(Duration.ofMillis(500))
            .onError((error, envelope, record) -> System.err.println("PROBE java WARN " + error.getMessage()))
            .build();
        consumer.subscribe(List.of(topic));
        System.out.println("PROBE java READY");
        System.out.flush();
        long deadline = System.currentTimeMillis() + runMs;
        while (System.currentTimeMillis() < deadline) {
            worker.poll();
            if (failures.get() > 0) {
                // poll() returns only after the retry-tier write was confirmed and the offset committed.
                System.out.printf("PROBE java RETRIED at_ms=%d%n", System.currentTimeMillis());
                return 0;
            }
        }
        System.out.println("PROBE java TIMEOUT");
        return 3;
    }

    private static int runReinject(Consumer<byte[], byte[]> consumer, Producer<byte[], byte[]> producer,
                                   RetryTopics retryTopics, long runMs) {
        KafkaRetryReinjector reinjector = KafkaRetryReinjector.builder(consumer, producer, retryTopics)
            .pollTimeout(Duration.ofMillis(500))
            .build();
        reinjector.subscribe();
        System.out.println("PROBE java READY");
        System.out.flush();
        long deadline = System.currentTimeMillis() + runMs;
        boolean assigned = false;
        while (System.currentTimeMillis() < deadline) {
            int forwarded = reinjector.poll();
            if (!assigned && !consumer.assignment().isEmpty()) {
                assigned = true;
                System.out.printf("PROBE java ASSIGNED partitions=%d%n", consumer.assignment().size());
                System.out.flush();
            }
            if (forwarded > 0) {
                producer.flush();
                System.out.printf("PROBE java REINJECTED forwarded=%d at_ms=%d%n", forwarded, System.currentTimeMillis());
                return 0;
            }
        }
        System.out.println("PROBE java TIMEOUT");
        return 3;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }
}
