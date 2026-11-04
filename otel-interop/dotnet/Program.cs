using System.Diagnostics;
using System.Text.Json;
using BabelQueue;
using BabelQueue.Pulsar;
using BabelQueue.Tracing;
using DotPulsar;
using DotPulsar.Extensions;

// Consumer (.NET) for the OTel interop on Pulsar: handles the messages a Java producer sent inside a
// CONSUMER span "process <queue>" and writes the finished spans to OTEL_SPAN_FILE.
//
//   OTEL_SPAN_FILE=consumer.jsonl dotnet run
//
// The Java producer carried a W3C traceparent as a Pulsar message property (ADR-0028).
// PulsarTraceHeaders.Extract hands it to Telemetry.Wrap, which starts the Activity as a true child of
// the producer span. system: "pulsar" is the broker name of ADR-0038 section 3 (messaging.system).
// The core only needs the in-box System.Diagnostics: an ActivityListener on the "BabelQueue"
// ActivitySource stands in for an OpenTelemetry exporter and writes the flat span shape every
// otel-interop probe writes (see python/spanfile.py). The topic is also the destination (meta.queue).
var spanFile = Environment.GetEnvironmentVariable("OTEL_SPAN_FILE");
if (string.IsNullOrEmpty(spanFile))
{
    Console.Error.WriteLine("OTEL_SPAN_FILE is required");
    return 2;
}

var url = Environment.GetEnvironmentVariable("PULSAR_URL") ?? "pulsar://localhost:6650";
var topic = Environment.GetEnvironmentVariable("QUEUE") ?? "orders";
var expected = int.TryParse(Environment.GetEnvironmentVariable("OTEL_MESSAGES"), out var n) ? n : 3;
var timeoutSeconds = int.TryParse(Environment.GetEnvironmentVariable("CONSUME_TIMEOUT"), out var t) ? t : 60;
var subscription = Environment.GetEnvironmentVariable("SUBSCRIPTION") ?? "babelqueue-otel";

var gate = new object();
using var listener = new ActivityListener
{
    ShouldListenTo = source => source.Name == Telemetry.ActivitySourceName,
    Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
    ActivityStopped = activity =>
    {
        var attributes = new SortedDictionary<string, object?>(StringComparer.Ordinal);
        foreach (var tag in activity.TagObjects)
        {
            attributes[tag.Key] = tag.Value;
        }

        var record = new SortedDictionary<string, object?>(StringComparer.Ordinal)
        {
            ["name"] = activity.DisplayName,
            ["kind"] = activity.Kind.ToString().ToUpperInvariant(),
            ["trace_id"] = activity.TraceId.ToHexString(),
            ["span_id"] = activity.SpanId.ToHexString(),
            ["parent_span_id"] = activity.ParentSpanId == default ? "" : activity.ParentSpanId.ToHexString(),
            ["status"] = activity.Status.ToString().ToUpperInvariant(),
            ["attributes"] = attributes,
        };
        lock (gate)
        {
            File.AppendAllText(spanFile, JsonSerializer.Serialize(record) + "\n");
        }
    },
};
ActivitySource.AddActivityListener(listener);

await using var client = PulsarClient.Builder().ServiceUrl(new Uri(url)).Build();
// The topic is new for every run, so the subscription is too; it starts at the earliest position, so the messages the
// producer sent before this probe attached (scripts/otel-verify.sh runs the producer first) are all read.
await using var consumer = client.NewConsumer(Schema.ByteArray)
    .Topic(topic)
    .SubscriptionName(subscription)
    .SubscriptionType(SubscriptionType.Shared)
    .InitialPosition(SubscriptionInitialPosition.Earliest)
    .Create();

var consumed = 0;
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));

var handlers = new Dictionary<string, BabelHandler>
{
    ["urn:babel:orders:created"] = (envelope, message, cancellationToken) =>
    {
        var headers = PulsarTraceHeaders.Extract(message);
        Handler handler = e =>
        {
            Console.WriteLine($"PROBE dotnet HANDLED id={e.Meta?.Id} order_id={e.Data?["order_id"]}");
            if (Interlocked.Increment(ref consumed) >= expected)
            {
                // Let this message be acknowledged before the loop is stopped.
                _ = Task.Delay(TimeSpan.FromSeconds(1)).ContinueWith(_ => cts.Cancel(), TaskScheduler.Default);
            }
            return Task.CompletedTask;
        };
        return Telemetry.Wrap(handler, headers, system: Telemetry.Systems.Pulsar)(envelope);
    },
};

var babel = new PulsarConsumer(consumer, handlers, new PulsarConsumerOptions
{
    OnError = (error, _, _) => Console.Error.WriteLine($"PROBE dotnet ERROR {error.Message}"),
});

Console.WriteLine("PROBE dotnet READY");
try
{
    await babel.RunAsync(cts.Token);
}
catch (OperationCanceledException)
{
    // All messages handled, or the timeout fired — clean shutdown.
}

Console.WriteLine($"PROBE dotnet DONE consumed={consumed}");
if (consumed < expected)
{
    Console.Error.WriteLine($"consumed {consumed} of {expected} message(s)");
    return 1;
}

return 0;
