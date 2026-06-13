using BabelQueue;
using BabelQueue.Pulsar;
using DotPulsar;
using DotPulsar.Extensions;

// .NET reads the same Pulsar "orders" topic a Java service produced to, and routes each
// message to a handler by URN. It never knows which language wrote the message — it only
// sees the canonical envelope, routing on the bq-job property (§5 binding).
var url = Environment.GetEnvironmentVariable("PULSAR_URL") ?? "pulsar://localhost:6650";

await using var client = PulsarClient.Builder().ServiceUrl(new Uri(url)).Build();
await using var consumer = client.NewConsumer(Schema.ByteArray)
    .Topic("orders")
    .SubscriptionName("babelqueue")
    .SubscriptionType(SubscriptionType.Shared)
    .Create();

var handlers = new Dictionary<string, BabelHandler>
{
    ["urn:babel:orders:created"] = (env, _, _) =>
    {
        Console.WriteLine(
            $"[dotnet] orders:created  order_id={env.Data?["order_id"]}  amount={env.Data?["amount"]}  " +
            $"from lang={env.Meta?.Lang}  trace={env.TraceId}  attempts={env.Attempts}");
        return Task.CompletedTask;
    },
    ["urn:babel:orders:shipped"] = (env, _, _) =>
    {
        Console.WriteLine(
            $"[dotnet] orders:shipped  order_id={env.Data?["order_id"]}  carrier={env.Data?["carrier"]}  " +
            $"from lang={env.Meta?.Lang}");
        return Task.CompletedTask;
    },
};

var babel = new PulsarConsumer(consumer, handlers, new PulsarConsumerOptions
{
    OnError = (error, _, _) => Console.Error.WriteLine($"[dotnet] error: {error.Message}"),
});

var seconds = int.TryParse(Environment.GetEnvironmentVariable("CONSUME_SECONDS"), out var s) ? s : 15;
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
Console.WriteLine($"[dotnet] consuming from 'orders' for {seconds}s (Shared subscription 'babelqueue')...");
try
{
    await babel.RunAsync(cts.Token);
}
catch (OperationCanceledException)
{
    // The timeout fired — clean shutdown.
}
Console.WriteLine("[dotnet] done.");
