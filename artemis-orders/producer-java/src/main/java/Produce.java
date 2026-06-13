import com.babelqueue.artemis.ArtemisPublisher;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.Map;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;

/**
 * Java produces canonical BabelQueue envelopes onto the Artemis "orders" address over JMS (the
 * CORE protocol). A Python service (consumer-python) reads the same address over AMQP 1.0 and
 * routes by URN — same wire envelope, two protocols, two languages, one broker. Each message's
 * body is the envelope JSON and the §7 JMS projection (JMSType = URN, JMSCorrelationID =
 * trace_id, the bq- properties) lets the consumer route on the URN without decoding the body.
 */
public final class Produce {
    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault("ARTEMIS_URL", "tcp://localhost:61616");
        ConnectionFactory factory = new ActiveMQJMSConnectionFactory(url, "artemis", "artemis");

        try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("orders");
            MessageProducer producer = session.createProducer(queue);
            ArtemisPublisher publisher = ArtemisPublisher.create(session, producer);

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

            System.out.println("[java] done — 4 messages on address 'orders'.");
        }
    }
}
