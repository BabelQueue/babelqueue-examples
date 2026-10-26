// Interop probe (.NET) for the ADR-0037 Artemis scenarios (version gate and poison, broker-bindings §7).
//
// An ArtemisConsumer on QUEUE over AMQP 1.0 (AMQPNetLite) whose handler records that it ran. The runner publishes
// with the Go probe and reads <QUEUE>.dlq back with it, so the record is asserted from another SDK.
// A version-gated or poison delivery must never reach the handler.
//
// Environment contract and output lines: see ../node/probe.mjs (PROBE dotnet ...).
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using BabelQueue.Artemis;

static string Env(string key, string fallback)
{
    var value = Environment.GetEnvironmentVariable(key);
    return string.IsNullOrEmpty(value) ? fallback : value;
}

static string Token(string? s) =>
    string.IsNullOrWhiteSpace(s) ? "-" : string.Join("_", s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

var url = Env("ARTEMIS_URL", "amqp://artemis:artemis@localhost:5672");
var queue = Env("QUEUE", "orders");
var dlqOn = Env("DLQ", "on") != "off";
var unknownUrn = Env("UNKNOWN_URN", "");
var runSeconds = double.Parse(Env("RUN_S", "120"), System.Globalization.CultureInfo.InvariantCulture);

var connection = await Connection.Factory.CreateAsync(new Address(url));
var session = new Session(connection);
var receiverLink = new ReceiverLink(
    session,
    "bq-interop-dotnet-" + Guid.NewGuid().ToString("N"),
    new Source { Address = queue, Capabilities = new Symbol[] { "queue" } },
    null);

var options = new ArtemisConsumerOptions
{
    Queue = queue,
    MaxTries = 3,
    OnError = (error, envelope, message) => Console.WriteLine($"PROBE dotnet ERROR kind={Token(error.GetType().Name)}"),
};
if (dlqOn)
{
    var senderLink = new SenderLink(
        session,
        "bq-interop-dotnet-dlq-" + Guid.NewGuid().ToString("N"),
        new Target { Address = queue + ".dlq", Capabilities = new Symbol[] { "queue" } },
        null);
    options.DeadLetterSender = new AmqpSenderLink(senderLink, queue + ".dlq");
}
if (!string.IsNullOrEmpty(unknownUrn))
{
    options.UnknownUrn = unknownUrn;
}

var handlers = new Dictionary<string, BabelHandler>
{
    ["urn:babel:orders:created"] = (envelope, message, ct) =>
    {
        Console.WriteLine($"PROBE dotnet HANDLED id={envelope.Meta?.Id} attempts={envelope.Attempts}");
        return Task.CompletedTask;
    },
};

var consumer = new ArtemisConsumer(new AmqpReceiverLink(receiverLink), handlers, options);
Console.WriteLine($"PROBE dotnet READY dlq={(dlqOn ? "on" : "off")} unknown_urn={(string.IsNullOrEmpty(unknownUrn) ? "none" : unknownUrn)}");

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(runSeconds));
try
{
    await consumer.RunAsync(cts.Token);
}
catch (OperationCanceledException)
{
    // the run window elapsed
}
await connection.CloseAsync();
Console.WriteLine("PROBE dotnet DONE");
