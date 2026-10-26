import com.babelqueue.sqs.SqsConsumer;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Interop probe (Java) for the ADR-0037 SQS poison scenarios (broker-bindings §3.5).
 *
 * <p>An {@code SqsConsumer} on {@code QUEUE_URL} whose handler records that it ran. The runner publishes with the Go
 * probe and reads {@code <QUEUE>-dlq} back with it, so the record is asserted from another SDK. A poison delivery
 * must never reach the handler.
 *
 * <p>Environment contract (shared with probe.mjs and the .NET probe):
 * <pre>
 *   SQS_ENDPOINT   http://localhost:9324 (ElasticMQ)
 *   AWS_REGION     eu-central-1
 *   QUEUE_URL      the work queue (from {@code probe-go setup})
 *   DLQ_URL        the BabelQueue {@code <queue>-dlq} (from {@code probe-go setup}); used only when DLQ=on
 *   DLQ            on (default): a DLQ queue URL is configured; off: none (dead-lettering off, left to RedrivePolicy)
 *   UNKNOWN_URN    optional unknown-URN strategy (delete | fail | release | dead_letter); default: none set
 *   RUN_S          seconds to keep consuming before exiting 0 (default 120; the runner kills it earlier)
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

    public static void main(String[] args) {
        String queueUrl = env("QUEUE_URL", "");
        if (queueUrl.isEmpty()) {
            throw new IllegalStateException("QUEUE_URL is required");
        }
        boolean dlqOn = !"off".equals(env("DLQ", "on"));
        String unknownUrn = env("UNKNOWN_URN", "");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(Long.parseLong(env("RUN_S", "120")));

        // ElasticMQ accepts any credentials; real SQS uses the default credential chain.
        SqsClient sqs = SqsClient.builder()
            .region(Region.of(env("AWS_REGION", "eu-central-1")))
            .endpointOverride(URI.create(env("SQS_ENDPOINT", "http://localhost:9324")))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
            .build();

        SqsConsumer.Builder builder = SqsConsumer.builder(sqs, queueUrl)
            .waitTimeSeconds(1)
            .handler("urn:babel:orders:created", (envelope, message) -> {
                System.out.printf("PROBE java HANDLED id=%s attempts=%d%n", envelope.meta().id(), envelope.attempts());
            })
            .onError((error, envelope, message) ->
                System.out.printf("PROBE java ERROR kind=%s%n", token(error.getClass().getSimpleName())));
        if (dlqOn) {
            builder.deadLetterQueueUrl(env("DLQ_URL", ""));
        }
        if (!unknownUrn.isEmpty()) {
            builder.unknownUrnStrategy(unknownUrn);
        }
        SqsConsumer consumer = builder.build();

        System.out.printf("PROBE java READY dlq=%s unknown_urn=%s%n",
            dlqOn ? "on" : "off", unknownUrn.isEmpty() ? "none" : unknownUrn);
        System.out.flush();
        while (System.nanoTime() < deadline) {
            try {
                consumer.poll();
            } catch (RuntimeException error) {
                System.out.printf("PROBE java ERROR kind=%s%n", token(error.getClass().getSimpleName()));
            }
            System.out.flush();
        }
        sqs.close();
        System.out.println("PROBE java DONE");
    }
}
