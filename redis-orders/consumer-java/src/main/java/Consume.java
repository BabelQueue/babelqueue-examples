// Consumer (Java) — reads the canonical BabelQueue envelopes a producer in ANY
// language put on the shared Redis "orders" list, routes them by URN, and shows
// which language produced each one (meta.lang). It never knows which SDK wrote
// the message; it only sees the canonical envelope.
//
//   mvn -q compile exec:java
//
// Reservation uses the §1 reliable-queue pattern (BLMOVE orders -> orders:processing,
// LREM on ack) — the exact convention the PHP/Go/Python SDKs share, so they
// interoperate on the plain "orders" list.
import com.babelqueue.redis.RedisConsumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public final class Consume {

    public static void main(String[] args) {
        String url = System.getenv("REDIS_URL");
        if (url == null || url.isBlank()) {
            url = "redis://localhost:6379/0";
        }

        RedisClient client = RedisClient.create(url);
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> redis = connection.sync();

            RedisConsumer consumer = RedisConsumer.builder(redis, "orders")
                .blockTimeoutSeconds(2)
                .handler("urn:babel:orders:created", (env, body) ->
                    System.out.printf(
                        "[java] orders:created  order_id=%s  amount=%s %s  from lang=%s  trace=%s%n",
                        env.data().get("order_id"),
                        env.data().get("amount"),
                        env.data().get("currency"),
                        env.meta().lang(),
                        env.traceId()))
                .handler("urn:babel:orders:shipped", (env, body) ->
                    System.out.printf(
                        "[java] orders:shipped  order_id=%s  carrier=%s  from lang=%s  trace=%s%n",
                        env.data().get("order_id"),
                        env.data().get("carrier"),
                        env.meta().lang(),
                        env.traceId()))
                .onUnknownUrn((env, body) ->
                    System.out.printf("[java] (skipped unrouted urn=%s)%n", env.job()))
                .onError((error, env, body) ->
                    System.err.printf("[java] error: %s%n", error.getMessage()))
                .build();

            int target = 4; // 3x orders:created + 1x orders:shipped
            int processed = 0;
            while (processed < target) {
                int reserved = consumer.poll(); // blocks up to blockTimeoutSeconds
                if (reserved == 0) {
                    break; // queue drained before we hit the target — stop polling
                }
                processed += reserved;
            }

            System.out.printf(
                "[java] processed %d message(s) — same envelope, different language.%n",
                processed);
        } finally {
            client.shutdown();
        }
    }
}
