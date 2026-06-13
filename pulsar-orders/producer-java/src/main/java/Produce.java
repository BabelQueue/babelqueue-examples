import com.babelqueue.pulsar.PulsarPublisher;
import java.util.Map;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

/**
 * Java produces canonical BabelQueue envelopes onto the Pulsar "orders" topic. A .NET
 * service (consumer-dotnet) reads the same topic and routes by URN — same wire envelope,
 * different languages, one broker. The producer sets the §5 bq- properties so the consumer
 * routes on bq-job without decoding the body.
 */
public final class Produce {
    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault("PULSAR_URL", "pulsar://localhost:6650");

        try (PulsarClient client = PulsarClient.builder().serviceUrl(url).build();
             Producer<byte[]> producer = client.newProducer().topic("orders").create()) {

            PulsarPublisher publisher = PulsarPublisher.create(producer);

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

            System.out.println("[java] done — 4 envelopes on topic 'orders'.");
        }
    }
}
