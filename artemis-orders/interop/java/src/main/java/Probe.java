import com.babelqueue.artemis.ArtemisConsumer;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;

/**
 * Interop probe (Java, JMS) for the ADR-0037 Artemis scenarios (version gate and poison, broker-bindings §7).
 *
 * <p>An {@code ArtemisConsumer} on {@code QUEUE} over JMS (the CORE protocol) whose handler records that it ran. The
 * runner publishes with the Go probe over AMQP 1.0 and reads {@code <QUEUE>.dlq} back with it, so the record is asserted
 * from another SDK, and the {@code bq_schema_version} property crosses from an AMQP application property to a JMS
 * property. A version-gated or poison delivery must never reach the handler.
 *
 * <p>Environment contract (shared with probe.mjs and the .NET probe; the connection variables differ because JMS speaks
 * CORE, not AMQP 1.0):
 * <pre>
 *   ARTEMIS_JMS_URL   tcp://localhost:61616
 *   ARTEMIS_USER      artemis
 *   ARTEMIS_PASSWORD  artemis
 *   QUEUE             queue (address) name
 *   DLQ               on (default): a DLQ producer for {@code <QUEUE>.dlq}; off: no DLQ (dead-lettering off)
 *   UNKNOWN_URN       optional unknown-URN strategy (delete | fail | release | dead_letter); default: none set
 *   RUN_S             seconds to keep consuming before exiting 0 (default 120; the runner kills it earlier)
 * </pre>
 *
 * <p>Output lines (parsed by scripts/lib/interop-common.sh):
 * <pre>
 *   PROBE java READY dlq=&lt;on|off&gt; unknown_urn=&lt;strategy|none&gt;
 *   PROBE java HANDLED id=&lt;meta.id&gt; attempts=&lt;envelope.attempts&gt;
 *   PROBE java ERROR kind=&lt;exception class&gt;
 *   PROBE java DONE
 * </pre>
 */
public final class Probe {

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? def : v;
    }

    private static String token(String s) {
        return s == null || s.isBlank() ? "-" : s.trim().replaceAll("\\s+", "_");
    }

    public static void main(String[] args) throws Exception {
        String url = env("ARTEMIS_JMS_URL", "tcp://localhost:61616");
        String user = env("ARTEMIS_USER", "artemis");
        String password = env("ARTEMIS_PASSWORD", "artemis");
        String queue = env("QUEUE", "orders");
        boolean dlqOn = !"off".equals(env("DLQ", "on"));
        String unknownUrn = env("UNKNOWN_URN", "");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(Long.parseLong(env("RUN_S", "120")));

        ConnectionFactory factory = new ActiveMQJMSConnectionFactory(url, user, password);
        try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(queue));

            ArtemisConsumer.Builder builder = ArtemisConsumer.builder(consumer, session)
                .receiveTimeout(500)
                .maxTries(3)
                .handler("urn:babel:orders:created", (envelope, message) -> {
                    System.out.printf("PROBE java HANDLED id=%s attempts=%d%n", envelope.meta().id(), envelope.attempts());
                })
                .onError((error, envelope, message) ->
                    System.out.printf("PROBE java ERROR kind=%s%n", token(error.getClass().getSimpleName())));
            if (dlqOn) {
                MessageProducer dlqProducer = session.createProducer(null); // anonymous, for <queue>.dlq
                builder.deadLetterQueue(dlqProducer, queue + ".dlq");
            }
            if (!unknownUrn.isEmpty()) {
                builder.unknownUrn(unknownUrn);
            }
            ArtemisConsumer worker = builder.build();

            connection.start();
            System.out.printf("PROBE java READY dlq=%s unknown_urn=%s%n",
                dlqOn ? "on" : "off", unknownUrn.isEmpty() ? "none" : unknownUrn);
            System.out.flush();
            while (System.nanoTime() < deadline) {
                try {
                    worker.poll();
                } catch (JMSException error) {
                    System.out.printf("PROBE java ERROR kind=%s%n", token(error.getClass().getSimpleName()));
                }
                System.out.flush();
            }
        }
        System.out.println("PROBE java DONE");
    }
}
