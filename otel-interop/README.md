# OTel interop — a consumer span that is the child of a producer span in another language

The unit tests of every SDK prove that `traceparent` is injected and extracted. This directory proves
it **across a real broker and two languages** (R1-otel, ADR-0028 and ADR-0038): one SDK produces inside a
`PRODUCER` span, a consumer in another language handles the message inside a `CONSUMER` span, and both write
their finished spans to a JSON-lines file. [`scripts/otel-verify.sh`](../scripts/otel-verify.sh) then asserts,
with `jq` only:

- producer spans are named `send <queue>` (kind `PRODUCER`), consumer spans `process <queue>` (kind `CONSUMER`);
- both carry `messaging.system` = the **broker** (`redis`, `rabbitmq`, `aws_sqs`, `servicebus`, `pulsar`, `kafka`,
  `activemq`), `messaging.operation.type` / `.name`, `messaging.destination.name` = the queue, the BabelQueue ids
  (`babelqueue.urn`, `messaging.message.id`, `messaging.message.conversation_id`, `babelqueue.schema_version`)
  and no error status; consumer spans also carry `babelqueue.attempts`, and on Kafka
  `messaging.consumer.group.name`;
- **every consumer span's `parent_span_id` equals the `span_id` of the producer span of the same message** (same
  `messaging.message.id`) and the `trace_id` is the same: the W3C `traceparent` crossed the broker out of band;
- every produced message was consumed.

| Broker | Producer -> consumer | `messaging.system` | Probe |
| :--- | :--- | :--- | :--- |
| Redis | Python -> Go | `redis` | `python/produce.py`, `go/` |
| RabbitMQ | Python -> Node (`@babelqueue/rabbitmq` adapter) | `rabbitmq` | `python/produce.py`, `node/consume-rabbitmq.mjs` |
| Amazon SQS (ElasticMQ) | Python -> Go | `aws_sqs` | `python/produce.py`, `go/` |
| Pulsar | Java -> .NET | `pulsar` | `java/`, `dotnet/` |
| Kafka (Redpanda) | Java -> Go | `kafka` (+ consumer group) | `java/`, `go/` |
| Artemis | Java (JMS) -> Python (AMQP 1.0) | `activemq` | `java/`, `python/consume_artemis.py` |
| Azure Service Bus (emulator) | Python -> Go | `servicebus` | `python/produce.py`, `go/` |

Every probe exports spans through a tiny exporter of its own that writes the same flat record
(`name`, `kind`, `trace_id`, `span_id`, `parent_span_id`, `status`, `attributes`): `python/spanfile.py`,
`go/spanfile.go`, `node/spanfile.mjs`, `java/src/main/java/JsonLinesSpanExporter.java`, and an
`ActivityListener` in `dotnet/Program.cs` (the .NET core only needs `System.Diagnostics`). The path comes from
`OTEL_SPAN_FILE`. The existing `*-orders` examples are untouched, so their 7/7 envelope-interop checks are
unaffected.

## Run it

```bash
scripts/otel-verify.sh redis            # one broker: compose up, build the probes, produce, consume, verify
scripts/otel-verify.sh all              # the seven, one child process each; exit 1 if any fails
scripts/otel-verify.sh --check kafka producer.jsonl consumer.jsonl 3 orders   # only compare two span files (needs jq)
```

By default the probes install the **published** SDK releases pinned in their manifests
(`python/requirements.txt`, `go/go.mod`, `node/package.json`, `java/pom.xml`, `dotnet/Probe.csproj`). The
`system` option and the semconv span shape exist only from the R1-otel releases, so until those are published
(the pins are bumped when they are) run against the in-repo SDK sources of a BabelQueue checkout:

```bash
BQ_SDK_ROOT=/path/to/BabelQueue scripts/otel-verify.sh redis
```

`BQ_SDK_ROOT` uses a Go `replace` on a copy of the probe, npm tarballs, `mvn install` into a private Maven
repository under the work dir (the `~/.m2` jars of the same version are not overwritten), a copy of the Python
sources for `pip install`, and msbuild `ProjectReference`s (the Pulsar transport is built from a copy whose
`BabelQueue.Core` reference points at the in-repo core). Nothing in the SDK repositories is edited. Java needs
JDK 17.

Each broker uses a fresh queue/topic name per run (`bq-otel-<id>`), except the Service Bus emulator, whose
`Config.json` only declares `orders`: run that one against a fresh emulator. The Service Bus emulator needs an
`amd64` SQL Server image and about 2 GB of RAM.

`OTEL_RECORD_DIR=<dir>` makes the script write `otel-<broker>.json` (`status`: `passing` | `failing`), which
`scripts/interop-merge.sh` folds into the `otel` cell of each broker in `interop-results.json`. The nightly
workflow runs the `otel` matrix and fails the run when a cell is not `passing`.
