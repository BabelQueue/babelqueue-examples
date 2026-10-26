import com.babelqueue.Envelope;
import com.babelqueue.pulsar.PulsarConsumer;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Interop probe (Java) for the post-R1 Pulsar exhaustion scenarios (ADR-0036).
 *
 * <p>A {@code PulsarConsumer} on {@code QUEUE} whose handler always throws, with dead-lettering to
 * {@code <QUEUE>.dlq}. The runner publishes with the Go probe and reads the DLQ back with it, so the record is
 * asserted from another SDK. A poison body never reaches the handler.
 *
 * <p>Environment contract (shared with probe.mjs):
 * <pre>
 *   PULSAR_URL     pulsar://localhost:6650
 *   QUEUE          queue (topic) name
 *   SUBSCRIPTION   subscription name (default babelqueue)
 *   SUB_TYPE       Exclusive (default) | Failover | Shared | Key_Shared
 *   RETRY          republish (default): the consumer is given the client and re-publishes with bq-attempts + 1;
 *                  nack: no client, the subscription is declared Shared and a retry is a negative acknowledgement
 *   MAX_ATTEMPTS   default 3
 *   RUN_S          seconds to keep consuming before exiting 0 (default 120; the runner kills it earlier)
 *   NO_DLQ         1: build the consumer WITHOUT a dead-letter producer (dead-lettering off); the version-gate
 *                  scenario uses it for the "acknowledged and dropped" row of ADR-0037 decision 1
 * </pre>
 *
 * <p>Output lines (parsed by scripts/lib/interop-common.sh):
 * <pre>
 *   PROBE java READY sub_type=&lt;type&gt; retry=&lt;mode&gt; max_attempts=&lt;n&gt;
 *   PROBE java HANDLED id=&lt;meta.id&gt; attempts=&lt;envelope.attempts&gt; redelivery=&lt;native count&gt;
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
        String url = env("PULSAR_URL", "pulsar://localhost:6650");
        String queue = env("QUEUE", "orders");
        SubscriptionType type = SubscriptionType.valueOf(env("SUB_TYPE", "Exclusive"));
        boolean republish = "republish".equals(env("RETRY", "republish"));
        int maxAttempts = Integer.parseInt(env("MAX_ATTEMPTS", "3"));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(Long.parseLong(env("RUN_S", "120")));

        PulsarClient client = PulsarClient.builder().serviceUrl(url).build();
        ConsumerBuilder<byte[]> subscription = client.newConsumer()
            .topic(queue)
            .subscriptionName(env("SUBSCRIPTION", "babelqueue"))
            .subscriptionType(type)
            // A negative acknowledgement redelivers after this delay (one minute by default). It is short in both
            // modes so that a republish retry that nacked instead of acknowledging the original would show up as
            // an extra delivery inside the runner's quiet window.
            .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS);
        Consumer<byte[]> consumer = subscription.subscribe();
        boolean noDlq = "1".equals(env("NO_DLQ", "0"));
        Producer<byte[]> deadLetter = noDlq ? null : client.newProducer().topic(queue + ".dlq").create();

        PulsarConsumer.Builder builder = PulsarConsumer.builder(consumer)
            .receiveTimeout(500)
            .maxAttempts(maxAttempts)
            .handler("urn:babel:orders:created", (Envelope envelope, org.apache.pulsar.client.api.Message<byte[]> message) -> {
                System.out.printf("PROBE java HANDLED id=%s attempts=%d redelivery=%d%n",
                    envelope.meta().id(), envelope.attempts(), message.getRedeliveryCount());
                throw new IllegalStateException("probe handler always fails");
            })
            .onError((error, envelope, message) ->
                System.out.printf("PROBE java ERROR kind=%s%n", token(error.getClass().getSimpleName())));
        if (deadLetter != null) {
            builder.deadLetterProducer(deadLetter);
        }
        // republish: the consumer builds its own retry producer from the client, so the cap holds on Exclusive.
        // nack: no producer, so the subscription type must be declared as one the broker counts redeliveries on.
        PulsarConsumer babel = republish ? builder.client(client).build() : builder.subscriptionType(type).build();

        System.out.printf("PROBE java READY sub_type=%s retry=%s max_attempts=%d%n",
            type, republish ? "republish" : "nack", maxAttempts);
        System.out.flush();
        while (System.nanoTime() < deadline) {
            try {
                babel.poll();
            } catch (PulsarClientException error) {
                System.out.printf("PROBE java ERROR kind=%s%n", token(error.getClass().getSimpleName()));
            }
            System.out.flush();
        }
        babel.close();
        if (deadLetter != null) {
            deadLetter.close();
        }
        consumer.close();
        client.close();
        System.out.println("PROBE java DONE");
    }
}
