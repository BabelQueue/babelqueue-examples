import com.babelqueue.kafka.KafkaPublisher;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Java produces canonical BabelQueue envelopes onto the Kafka "orders" topic. A Go service
 * (consumer-go) reads the same topic and routes by URN — same wire envelope, different
 * languages, one broker. Each record's value is the envelope JSON and the §6 {@code bq-}
 * headers let the consumer route on {@code bq-job} without decoding the body.
 */
public final class Produce {
    public static void main(String[] args) {
        String brokers = System.getenv().getOrDefault("KAFKA_BROKERS", "localhost:9092");
        Map<String, Object> cfg = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(cfg)) {
            KafkaPublisher publisher = KafkaPublisher.create(producer, "orders");

            for (int i = 1; i <= 3; i++) {
                int orderId = 1000 + i;
                String id = publisher.publish(
                    "urn:babel:orders:created",
                    Map.of("order_id", orderId, "amount", 19.99 * i));
                System.out.printf("[java] published orders:created order_id=%d meta.id=%s%n", orderId, id);
            }

            String shipId = publisher.publish(
                "urn:babel:orders:shipped",
                Map.of("order_id", 1002, "carrier", "DHL"));
            System.out.printf("[java] published orders:shipped order_id=1002 meta.id=%s%n", shipId);

            producer.flush();
            System.out.println("[java] done — 4 records on topic 'orders'.");
        }
    }
}
