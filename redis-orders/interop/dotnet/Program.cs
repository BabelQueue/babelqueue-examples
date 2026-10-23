// Interop probe (.NET) for the R1 Redis scenarios.
// Environment contract and output lines: see ../go/main.go.
using BabelQueue;
using BabelQueue.Redis;
using StackExchange.Redis;

const string Urn = "urn:babel:orders:created";

static string Env(string key, string fallback)
{
    var value = Environment.GetEnvironmentVariable(key);
    return string.IsNullOrEmpty(value) ? fallback : value;
}

var redisUrl = Env("REDIS_URL", "redis://localhost:6379/0");
var queue = Env("QUEUE", "orders");
var layout = Env("LAYOUT", "zset").Equals("legacy", StringComparison.OrdinalIgnoreCase)
    ? RedisLayout.Legacy
    : RedisLayout.Zset;
var retryAfterEnv = Env("RETRY_AFTER", "");
var mode = Env("MODE", "consume");
var hold = double.Parse(Env("HOLD", "0"), System.Globalization.CultureInfo.InvariantCulture);
var timeout = double.Parse(Env("TIMEOUT", "30"), System.Globalization.CultureInfo.InvariantCulture);

TimeSpan? retryAfter = retryAfterEnv switch
{
    "" => new RedisConsumerOptions().RetryAfter,
    "none" => null,
    _ => TimeSpan.FromSeconds(double.Parse(retryAfterEnv, System.Globalization.CultureInfo.InvariantCulture)),
};

// redis://host:port/db -> StackExchange.Redis configuration.
var uri = new Uri(redisUrl);
var config = new ConfigurationOptions
{
    EndPoints = { { uri.Host, uri.Port > 0 ? uri.Port : 6379 } },
    DefaultDatabase = uri.AbsolutePath.Length > 1 ? int.Parse(uri.AbsolutePath.TrimStart('/'), System.Globalization.CultureInfo.InvariantCulture) : 0,
    AbortOnConnectFail = false,
};
await using var mux = await ConnectionMultiplexer.ConnectAsync(config);
var db = mux.GetDatabase();

var done = false;
var doneId = "";

var handlers = new Dictionary<string, BabelHandler>
{
    [Urn] = async (envelope, rawBody, headers, ct) =>
    {
        var id = envelope.Meta.Id ?? "";
        Console.WriteLine($"PROBE dotnet RECEIVED id={id} attempts={envelope.Attempts} at_ms={DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}");
        if (mode == "hang")
        {
            await Task.Delay(Timeout.Infinite, ct); // the runner kill -9's us while the reservation is held
        }
        if (mode == "slow")
        {
            await Task.Delay(TimeSpan.FromSeconds(hold), ct);
        }
        doneId = id;
        done = true;
    },
};

var consumer = new RedisConsumer(db, queue, handlers, new RedisConsumerOptions
{
    Layout = layout,
    RetryAfter = retryAfter,
    OnWarning = message => Console.Error.WriteLine($"PROBE dotnet WARN {message}"),
    OnError = (error, _, _) => Console.Error.WriteLine($"PROBE dotnet ERROR {error.Message}"),
});

Console.WriteLine("PROBE dotnet READY");
var deadline = DateTimeOffset.UtcNow.AddSeconds(timeout);
while (!done && DateTimeOffset.UtcNow < deadline)
{
    await consumer.PollAsync();
    if (!done)
    {
        await Task.Delay(100);
    }
}

if (!done)
{
    Console.WriteLine("PROBE dotnet TIMEOUT");
    return 3;
}

Console.WriteLine($"PROBE dotnet ACKED id={doneId}");
return 0;
