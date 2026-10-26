// Interop probe (.NET) for the ADR-0037 SQS poison scenarios (broker-bindings §3.5).
//
// An SqsConsumer on QUEUE_URL whose handler records that it ran. The runner publishes with the Go probe and
// reads <QUEUE>-dlq back with it, so the record is asserted from another SDK. A poison delivery must never
// reach the handler.
//
// Environment contract and output lines: see ../node/probe.mjs (PROBE dotnet ...).
using Amazon.Runtime;
using Amazon.SQS;
using BabelQueue.Sqs;

static string Env(string key, string fallback)
{
    var value = Environment.GetEnvironmentVariable(key);
    return string.IsNullOrEmpty(value) ? fallback : value;
}

static string Token(string? s) =>
    string.IsNullOrWhiteSpace(s) ? "-" : string.Join("_", s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

var queueUrl = Env("QUEUE_URL", "");
if (queueUrl.Length == 0)
{
    throw new InvalidOperationException("QUEUE_URL is required");
}
var dlqOn = Env("DLQ", "on") != "off";
var unknownUrn = Env("UNKNOWN_URN", "");
var runSeconds = double.Parse(Env("RUN_S", "120"), System.Globalization.CultureInfo.InvariantCulture);

// ElasticMQ accepts any credentials; real SQS uses the configured ones.
var sqs = new AmazonSQSClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonSQSConfig
    {
        ServiceURL = Env("SQS_ENDPOINT", "http://localhost:9324"),
        AuthenticationRegion = Env("AWS_REGION", "eu-central-1"),
    });

var options = new SqsConsumerOptions
{
    WaitTimeSeconds = 1,
    OnError = (error, envelope, message) => Console.WriteLine($"PROBE dotnet ERROR kind={Token(error.GetType().Name)}"),
};
if (dlqOn)
{
    options.DeadLetterQueueUrl = Env("DLQ_URL", "");
}
if (!string.IsNullOrEmpty(unknownUrn))
{
    options.UnknownUrnStrategy = unknownUrn;
}

var handlers = new Dictionary<string, BabelHandler>
{
    ["urn:babel:orders:created"] = (envelope, message, ct) =>
    {
        Console.WriteLine($"PROBE dotnet HANDLED id={envelope.Meta?.Id} attempts={envelope.Attempts}");
        return Task.CompletedTask;
    },
};

var consumer = new SqsConsumer(sqs, queueUrl, handlers, options);
Console.WriteLine($"PROBE dotnet READY dlq={(dlqOn ? "on" : "off")} unknown_urn={(string.IsNullOrEmpty(unknownUrn) ? "none" : unknownUrn)}");

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(runSeconds));
while (!cts.IsCancellationRequested)
{
    try
    {
        await consumer.PollAsync(cts.Token);
    }
    catch (OperationCanceledException)
    {
        break;
    }
    catch (Exception error)
    {
        Console.WriteLine($"PROBE dotnet ERROR kind={Token(error.GetType().Name)}");
    }
}
Console.WriteLine("PROBE dotnet DONE");
