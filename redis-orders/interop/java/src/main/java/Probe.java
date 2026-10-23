import com.babelqueue.redis.RedisConsumer;
import com.babelqueue.redis.RedisLayout;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

/**
 * Interop probe (Java) for the R1 Redis scenarios.
 * Environment contract and output lines: see redis-orders/interop/go/main.go.
 */
public final class Probe {

    private static final String URN = "urn:babel:orders:created";

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? def : v;
    }

    public static void main(String[] args) throws Exception {
        String url = env("REDIS_URL", "redis://localhost:6379/0");
        String queue = env("QUEUE", "orders");
        RedisLayout layout = "legacy".equalsIgnoreCase(env("LAYOUT", "zset")) ? RedisLayout.LEGACY : RedisLayout.ZSET;
        String retryAfter = env("RETRY_AFTER", "");
        String mode = env("MODE", "consume");
        double hold = Double.parseDouble(env("HOLD", "0"));
        double timeout = Double.parseDouble(env("TIMEOUT", "30"));

        RedisClient client = RedisClient.create(url);
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            boolean[] done = {false};
            String[] lastId = {""};

            RedisConsumer.Builder builder = RedisConsumer.builder(connection.sync(), queue)
                .layout(layout)
                .blockTimeoutSeconds(1)
                .handler(URN, (envelope, body) -> {
                    String id = envelope.meta().id();
                    System.out.printf("PROBE java RECEIVED id=%s attempts=%d at_ms=%d%n",
                        id, envelope.attempts(), System.currentTimeMillis());
                    System.out.flush();
                    if ("hang".equals(mode)) {
                        Thread.sleep(Long.MAX_VALUE); // the runner kill -9's us while the reservation is held
                    }
                    if ("slow".equals(mode)) {
                        Thread.sleep((long) (hold * 1000));
                    }
                    lastId[0] = id;
                    done[0] = true;
                })
                .onError((error, envelope, raw) -> System.err.println("PROBE java ERROR " + error));

            if ("none".equals(retryAfter)) {
                builder.retryAfterDisabled();
            } else if (!retryAfter.isEmpty()) {
                builder.retryAfterSeconds((long) Double.parseDouble(retryAfter));
            }
            RedisConsumer consumer = builder.build();

            System.out.println("PROBE java READY");
            System.out.flush();
            long deadline = System.currentTimeMillis() + (long) (timeout * 1000);
            while (!done[0] && System.currentTimeMillis() < deadline) {
                consumer.poll();
            }
            if (!done[0]) {
                System.out.println("PROBE java TIMEOUT");
                System.exit(3);
            }
            System.out.println("PROBE java ACKED id=" + lastId[0]);
        } finally {
            client.shutdown();
        }
    }
}
