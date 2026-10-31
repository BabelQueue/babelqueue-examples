#!/usr/bin/env bash
# Cross-hop OpenTelemetry verification (R1-otel Faz 13, ADR-0028 / ADR-0038).
#
# For each broker, one SDK produces into a live broker inside a PRODUCER span and a consumer in another
# language handles the messages inside a CONSUMER span (probes: otel-interop/). Both write their finished
# spans to a JSON-lines file. This script then checks, with jq only:
#   - the producer spans are `send <queue>` / PRODUCER, the consumer spans `process <queue>` / CONSUMER, and
#     both carry messaging.system = the broker (ADR-0038 section 3), the destination, the BabelQueue ids and
#     no error status; a Kafka consumer span also carries messaging.consumer.group.name;
#   - every consumer span is the CHILD of the producer span of the same message (messaging.message.id):
#     its parent_span_id is that producer span's span_id and the trace_id is the same (the W3C traceparent
#     crossed the broker out of band);
#   - every produced message was consumed.
#
#   scripts/otel-verify.sh <broker>... | all        start the broker, run the probes, verify (exit 0 = all pass)
#   scripts/otel-verify.sh --check <broker> <producer.jsonl> <consumer.jsonl> [<messages> [<queue>]]
#                                                   verify two span files only (no broker, needs only jq)
#
#   broker                  producer -> consumer        messaging.system
#   redis                   python   -> go              redis
#   rabbitmq                python   -> node            rabbitmq        (@babelqueue/rabbitmq adapter)
#   sqs                     python   -> go              aws_sqs         (ElasticMQ)
#   pulsar                  java     -> dotnet          pulsar
#   kafka                   java     -> go              kafka           (+ consumer group)
#   artemis                 java     -> python          activemq        (JMS -> AMQP 1.0)
#   azureservicebus         python   -> go              servicebus      (emulator; needs an amd64 SQL Server image)
#
# SDK source selection (same convention as scripts/lib/interop-common.sh)
#   default            the published SDK versions pinned in the probe manifests (CI; the R1-otel releases)
#   BQ_SDK_ROOT=<dir>  the in-repo SDK sources of a BabelQueue checkout (pre-release verification: Go `replace`,
#                      npm tarballs, `mvn install` into a private repository under the work dir, msbuild
#                      ProjectReference, pip install of a copy of the Python sources). SDK repositories are
#                      read, never written (build output aside).
#
# Env: OTEL_MESSAGES (default 3)   OTEL_RECORD_DIR (write otel-<broker>.json for scripts/interop-merge.sh)
#      PYTHON (default python3)    INTEROP_WORK (keep the work dir, and its Maven repository, between runs)
#      KEEP_BROKERS=1              KEEP_WORK=1
# Requires: docker compose and the toolchains of the probes a broker uses (python3, go, node+npm, mvn+java 17,
# dotnet 8), jq.
set -euo pipefail
# shellcheck source=lib/interop-common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/interop-common.sh"

OTEL_SRC="$EXAMPLES_DIR/otel-interop"
OTEL_MESSAGES="${OTEL_MESSAGES:-3}"
PYTHON="${PYTHON:-python3}"
ALL_BROKERS=(redis rabbitmq sqs pulsar kafka artemis azureservicebus)

# broker_info <broker>: sets B_SYSTEM, B_PRODUCER, B_CONSUMER, B_GROUP (1 = the consumer span needs the Kafka group).
broker_info() {
  B_GROUP=0
  case "$1" in
    redis) B_SYSTEM=redis B_PRODUCER=python B_CONSUMER=go ;;
    rabbitmq) B_SYSTEM=rabbitmq B_PRODUCER=python B_CONSUMER=node ;;
    sqs) B_SYSTEM=aws_sqs B_PRODUCER=python B_CONSUMER=go ;;
    pulsar) B_SYSTEM=pulsar B_PRODUCER=java B_CONSUMER=dotnet ;;
    kafka) B_SYSTEM=kafka B_PRODUCER=java B_CONSUMER=go B_GROUP=1 ;;
    artemis) B_SYSTEM=activemq B_PRODUCER=java B_CONSUMER=python ;;
    azureservicebus) B_SYSTEM=servicebus B_PRODUCER=python B_CONSUMER=go ;;
    *) fail "unknown broker: $1 (${ALL_BROKERS[*]})" ;;
  esac
}

# ---- the verification (jq only) ---------------------------------------------------------------

# check_spans <broker> <producer.jsonl> <consumer.jsonl> [<messages> [<queue>]]: prints one line per problem and
# a summary; returns 0 only when there is none.
check_spans() {
  need jq
  local broker="$1" producer_file="$2" consumer_file="$3" expected="${4:-3}" queue="${5:-orders}"
  broker_info "$broker"
  [ -s "$producer_file" ] || { printf 'FAIL %s: producer span file is missing or empty: %s\n' "$broker" "$producer_file"; return 1; }
  [ -s "$consumer_file" ] || { printf 'FAIL %s: consumer span file is missing or empty: %s\n' "$broker" "$consumer_file"; return 1; }

  local result
  result="$(jq -n \
    --slurpfile producer "$producer_file" --slurpfile consumer "$consumer_file" \
    --arg system "$B_SYSTEM" --arg queue "$queue" --argjson expected "$expected" --argjson group "$B_GROUP" '
    def attr($s; $k): $s.attributes[$k];
    def text($s; $k): (attr($s; $k) | type) == "string" and (attr($s; $k) | length) > 0;
    def problems($label; $s; $op; $kind):
      [
        (if $s.kind != $kind then "\($label): kind is \($s.kind | tojson), want \($kind)" else empty end),
        (if $s.name != ($op + " " + $queue) then "\($label): span name is \($s.name | tojson), want \(($op + " " + $queue) | tojson)" else empty end),
        (if attr($s; "messaging.system") != $system then "\($label): messaging.system is \(attr($s; "messaging.system") | tojson), want \($system | tojson)" else empty end),
        (if attr($s; "messaging.operation.type") != $op then "\($label): messaging.operation.type is \(attr($s; "messaging.operation.type") | tojson), want \($op | tojson)" else empty end),
        (if attr($s; "messaging.operation.name") != $op then "\($label): messaging.operation.name is \(attr($s; "messaging.operation.name") | tojson), want \($op | tojson)" else empty end),
        (if attr($s; "messaging.destination.name") != $queue then "\($label): messaging.destination.name is \(attr($s; "messaging.destination.name") | tojson), want \($queue | tojson)" else empty end),
        (if text($s; "babelqueue.urn") | not then "\($label): babelqueue.urn is missing" else empty end),
        (if text($s; "messaging.message.id") | not then "\($label): messaging.message.id is missing" else empty end),
        (if text($s; "messaging.message.conversation_id") | not then "\($label): messaging.message.conversation_id is missing" else empty end),
        (if attr($s; "babelqueue.schema_version") != 1 then "\($label): babelqueue.schema_version is \(attr($s; "babelqueue.schema_version") | tojson), want 1" else empty end),
        (if $s.status == "ERROR" then "\($label): span status is ERROR" else empty end)
      ];
    ($producer | map(select(.kind == "PRODUCER"))) as $sends
    | ($consumer | map(select(.kind == "CONSUMER"))) as $procs
    | ($sends | map(select(text(.; "messaging.message.id")) | attr(.; "messaging.message.id"))) as $sentIds
    | ($procs | map(select(text(.; "messaging.message.id")) | attr(.; "messaging.message.id"))) as $handledIds
    | [
        (if ($sends | length) < $expected then "producer: \($sends | length) PRODUCER span(s), want at least \($expected)" else empty end),
        (if ($procs | length) < $expected then "consumer: \($procs | length) CONSUMER span(s), want at least \($expected)" else empty end),
        ($sends[] | problems("producer span \(.span_id)"; .; "send"; "PRODUCER")[]),
        ($procs[] | problems("consumer span \(.span_id)"; .; "process"; "CONSUMER")[]),
        ($procs[] | (if (attr(.; "babelqueue.attempts") | type) != "number" then "consumer span \(.span_id): babelqueue.attempts is missing" else empty end)),
        (if $group == 1 then ($procs[] | (if text(.; "messaging.consumer.group.name") | not then "consumer span \(.span_id): messaging.consumer.group.name is missing" else empty end)) else empty end),
        ($procs[] as $c
          | ($sends | map(select(attr(.; "messaging.message.id") == attr($c; "messaging.message.id")))) as $match
          | if ($match | length) == 0 then "consumer span \($c.span_id): no producer span for message \(attr($c; "messaging.message.id") | tojson)"
            elif ($match | length) > 1 then "consumer span \($c.span_id): \($match | length) producer spans for message \(attr($c; "messaging.message.id") | tojson)"
            else $match[0] as $p
              | (if $c.parent_span_id != $p.span_id then "consumer span \($c.span_id) (message \(attr($c; "messaging.message.id"))): parent_span_id is \($c.parent_span_id | tojson), the producer span_id is \($p.span_id | tojson)" else empty end),
                (if $c.trace_id != $p.trace_id then "consumer span \($c.span_id) (message \(attr($c; "messaging.message.id"))): trace_id \($c.trace_id) differs from the producer trace_id \($p.trace_id)" else empty end),
                (if attr($c; "messaging.message.conversation_id") != attr($p; "messaging.message.conversation_id") then "consumer span \($c.span_id) (message \(attr($c; "messaging.message.id"))): conversation_id differs from the producer span" else empty end)
            end),
        (($sentIds | unique) - ($handledIds | unique) | .[] | "no consumer span for message \(. | tojson)")
      ] as $errors
    | {ok: ($errors | length == 0), producer_spans: ($sends | length), consumer_spans: ($procs | length),
       linked: ([$procs[] | select(.parent_span_id as $p | $sends | any(.span_id == $p))] | length), errors: $errors}')"

  local ok producers consumers linked
  ok="$(jq -r '.ok' <<<"$result")"
  producers="$(jq -r '.producer_spans' <<<"$result")"
  consumers="$(jq -r '.consumer_spans' <<<"$result")"
  linked="$(jq -r '.linked' <<<"$result")"
  if [ "$ok" = true ]; then
    printf 'PASS %s (%s -> %s, messaging.system=%s): %s producer span(s), %s consumer span(s), %s linked parent -> child\n' \
      "$broker" "$B_PRODUCER" "$B_CONSUMER" "$B_SYSTEM" "$producers" "$consumers" "$linked"
    return 0
  fi
  printf 'FAIL %s (%s -> %s, messaging.system=%s): %s producer span(s), %s consumer span(s), %s linked parent -> child\n' \
    "$broker" "$B_PRODUCER" "$B_CONSUMER" "$B_SYSTEM" "$producers" "$consumers" "$linked"
  jq -r '.errors[] | "  - " + .' <<<"$result"
  return 1
}

# ---- building the probes ----------------------------------------------------------------------

# run_timeout <seconds> <command...>: bash has no timeout(1) on macOS; a watchdog kills the command.
run_timeout() {
  local secs="$1" pid watchdog rc=0
  shift
  "$@" &
  pid=$!
  ( sleep "$secs"; kill -9 "$pid" 2>/dev/null ) &
  watchdog=$!
  wait "$pid" || rc=$?
  kill "$watchdog" 2>/dev/null || true
  wait "$watchdog" 2>/dev/null || true
  return "$rc"
}

# otel_py_extra <broker>: the babelqueue extra of the Python side of a broker.
otel_py_extra() {
  case "$1" in
    redis) echo redis ;;
    rabbitmq) echo amqp ;;
    sqs) echo sqs ;;
    azureservicebus) echo azureservicebus ;;
    artemis) echo artemis ;;
  esac
}

# otel_build_python <broker>: a venv with the Python SDK, the OTel SDK and the probe's broker extra.
otel_build_python() {
  need "$PYTHON"
  local extra venv="$WORK/otel-venv-$1"
  extra="$(otel_py_extra "$1")"
  # A venv kept by INTEROP_WORK is reused only in published mode: with BQ_SDK_ROOT the in-repo sources may have
  # changed since the last run, so the venv (and its pip install of the SDK copy) is rebuilt every time.
  if [ -z "$BQ_SDK_ROOT" ] && [ -x "$venv/bin/python" ] && [ "$(cat "$venv/.bq-mode" 2>/dev/null)" = published ]; then
    return 0
  fi
  rm -rf "$venv"
  "$PYTHON" -m venv "$venv"
  "$venv/bin/pip" install --quiet --upgrade pip
  if [ -n "$BQ_SDK_ROOT" ]; then
    # A copy, so pip never writes build output into the SDK repository.
    rm -rf "$WORK/sdk-python" && mkdir -p "$WORK/sdk-python"
    (cd "$BQ_SDK_ROOT/babelqueue-python" && tar --exclude=.git --exclude=.venv --exclude=.mypy_cache \
      --exclude=dist --exclude=__pycache__ --exclude=tests -cf - .) | (cd "$WORK/sdk-python" && tar -xf -)
    "$venv/bin/pip" install --quiet "$WORK/sdk-python[otel,$extra]" "opentelemetry-sdk>=1.30.0" "azure-servicebus>=7.12.0"
  else
    "$venv/bin/pip" install --quiet -r "$OTEL_SRC/python/requirements.txt" "babelqueue[$extra]"
    echo published > "$venv/.bq-mode"
  fi
}

otel_build_go() {
  need go
  rm -rf "$WORK/otel-go" && cp -r "$OTEL_SRC/go" "$WORK/otel-go"
  (
    cd "$WORK/otel-go"
    if [ -n "$BQ_SDK_ROOT" ]; then
      go mod edit \
        -replace "github.com/babelqueue/babelqueue-go=$BQ_SDK_ROOT/babelqueue-go" \
        -replace "github.com/babelqueue/babelqueue-go/otel=$BQ_SDK_ROOT/babelqueue-go/otel" \
        -replace "github.com/babelqueue/babelqueue-go/redis=$BQ_SDK_ROOT/babelqueue-go/redis" \
        -replace "github.com/babelqueue/babelqueue-go/sqs=$BQ_SDK_ROOT/babelqueue-go/sqs" \
        -replace "github.com/babelqueue/babelqueue-go/kafka=$BQ_SDK_ROOT/babelqueue-go/kafka" \
        -replace "github.com/babelqueue/babelqueue-go/azureservicebus=$BQ_SDK_ROOT/babelqueue-go/azureservicebus"
    fi
    GOFLAGS=-mod=mod go build -o "$WORK/otel-go/consumer-go" .
  )
}

otel_build_node() {
  need node
  need npm
  rm -rf "$WORK/otel-node" && cp -r "$OTEL_SRC/node" "$WORK/otel-node"
  (
    cd "$WORK/otel-node"
    if [ -n "$BQ_SDK_ROOT" ]; then
      # Tarballs, not symlinks (see build_node in interop-common.sh): one copy of @babelqueue/core.
      mkdir -p "$WORK/tgz-otel"
      (cd "$BQ_SDK_ROOT/babelqueue-node" && { [ -d node_modules ] || npm ci --silent; } \
        && npm run --silent build && npm pack --silent --pack-destination "$WORK/tgz-otel" >/dev/null)
      # The adapter is built from a private copy against the in-repo core: the adapters workspace resolves
      # @babelqueue/core from the registry, which lacks the types its dead-letter observer wiring imports.
      local adapter="$WORK/adapter-rabbitmq"
      rm -rf "$adapter" && mkdir -p "$adapter/packages"
      cp "$BQ_SDK_ROOT/babelqueue-node-adapters/tsconfig.base.json" "$adapter/"
      (cd "$BQ_SDK_ROOT/babelqueue-node-adapters/packages" && tar --exclude=node_modules --exclude=dist --exclude=test -cf - rabbitmq) \
        | (cd "$adapter/packages" && tar -xf -)
      (cd "$adapter/packages/rabbitmq" \
        && npm install --silent --no-audit --no-fund --no-save "$WORK"/tgz-otel/babelqueue-core-*.tgz "tsup@^8" "typescript@^5.5" "@types/node@^22" \
        && npx --no-install tsup >/dev/null && npm pack --silent --pack-destination "$WORK/tgz-otel" >/dev/null)
      npm install --silent --no-audit --no-fund "$WORK"/tgz-otel/babelqueue-core-*.tgz \
        "$WORK"/tgz-otel/babelqueue-rabbitmq-*.tgz
    else
      npm install --silent --no-audit --no-fund
    fi
  )
}

OTEL_M2=""
# otel_mvn <args...>: Maven against the private repository when BQ_SDK_ROOT installs the in-repo SDKs, so the
# published jars of the same version in ~/.m2 are never overwritten.
otel_mvn() {
  if [ -n "$OTEL_M2" ]; then mvn -q -B "-Dmaven.repo.local=$OTEL_M2" "$@"; else mvn -q -B "$@"; fi
}
otel_mvn_version() { otel_mvn -f "$1" help:evaluate -Dexpression=project.version -DforceStdout; }

otel_build_java() {
  need mvn
  need java
  local core kafka pulsar artemis repo
  local -a versions=()
  if [ -n "$BQ_SDK_ROOT" ]; then
    OTEL_M2="$WORK/m2"
    for repo in babelqueue-java babelqueue-java-kafka babelqueue-java-pulsar babelqueue-java-artemis; do
      otel_mvn -f "$BQ_SDK_ROOT/$repo/pom.xml" install -DskipTests -Djacoco.skip=true
    done
    core="$(otel_mvn_version "$BQ_SDK_ROOT/babelqueue-java/pom.xml")"
    kafka="$(otel_mvn_version "$BQ_SDK_ROOT/babelqueue-java-kafka/pom.xml")"
    pulsar="$(otel_mvn_version "$BQ_SDK_ROOT/babelqueue-java-pulsar/pom.xml")"
    artemis="$(otel_mvn_version "$BQ_SDK_ROOT/babelqueue-java-artemis/pom.xml")"
    versions=("-Dbabelqueue-core.version=$core" "-Dbabelqueue-kafka.version=$kafka"
      "-Dbabelqueue-pulsar.version=$pulsar" "-Dbabelqueue-artemis.version=$artemis")
  fi
  rm -rf "$WORK/otel-java" && cp -r "$OTEL_SRC/java" "$WORK/otel-java"
  (
    cd "$WORK/otel-java"
    otel_mvn compile ${versions[@]+"${versions[@]}"}
    otel_mvn dependency:build-classpath -Dmdep.outputFile=cp.txt ${versions[@]+"${versions[@]}"}
  )
}

otel_build_dotnet() {
  need dotnet
  rm -rf "$WORK/otel-dotnet" && cp -r "$OTEL_SRC/dotnet" "$WORK/otel-dotnet"
  (
    cd "$WORK/otel-dotnet"
    if [ -n "$BQ_SDK_ROOT" ]; then
      # The transport csproj pins the published BabelQueue.Core package; build a copy of it against the in-repo
      # core so the transport and the probe see the same Telemetry / dead-letter types.
      local transport="$WORK/sdk-dotnet-pulsar"
      rm -rf "$transport" && mkdir -p "$transport/src"
      (cd "$BQ_SDK_ROOT/babelqueue-dotnet-pulsar" && tar --exclude=bin --exclude=obj -cf - src README.md) | (cd "$transport" && tar -xf -)
      python3 - "$transport/src/BabelQueue.Pulsar/BabelQueue.Pulsar.csproj" "$BQ_SDK_ROOT/babelqueue-dotnet/src/BabelQueue.Core/BabelQueue.Core.csproj" <<'PY'
import re
import sys

path, core = sys.argv[1], sys.argv[2]
text = open(path, encoding="utf-8").read()
text, count = re.subn(r'<PackageReference Include="BabelQueue\.Core" Version="[^"]*" />', f'<ProjectReference Include="{core}" />', text)
if count != 1:
    sys.exit("could not point the Pulsar transport at the in-repo core")
open(path, "w", encoding="utf-8").write(text)
PY
      dotnet build -c Release -nologo -v q -o "$WORK/otel-dotnet/out" "-p:BqSdkRoot=$BQ_SDK_ROOT" \
        "-p:BqPulsarProject=$transport/src/BabelQueue.Pulsar/BabelQueue.Pulsar.csproj"
    else
      dotnet build -c Release -nologo -v q -o "$WORK/otel-dotnet/out"
    fi
  )
}

# ---- the brokers --------------------------------------------------------------------------------

# broker_up <broker> <queue>: starts the broker (compose) and provisions the queue/topic.
broker_up() {
  local broker="$1" queue="$2"
  need docker
  case "$broker" in
    redis)
      log "starting redis"
      compose_up "$REDIS_COMPOSE_FILE" 120
      ;;
    rabbitmq)
      log "starting rabbitmq"
      compose_up "$EXAMPLES_DIR/rabbitmq-orders/docker-compose.yml" 180
      ;;
    sqs)
      sqs_up
      log "creating the SQS queue $queue"
      "$WORK/otel-venv-sqs/bin/python" - "$queue" <<'PY'
import os
import sys

import boto3

boto3.client(
    "sqs",
    region_name=os.environ["AWS_REGION"],
    endpoint_url=os.environ["SQS_ENDPOINT"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
).create_queue(QueueName=sys.argv[1])
PY
      ;;
    pulsar)
      log "starting pulsar"
      compose_up "$PULSAR_DIR/docker-compose.yml" 300
      ;;
    kafka)
      log "starting redpanda"
      compose_up "$KAFKA_DIR/docker-compose.yml" 180
      docker compose -f "$KAFKA_DIR/docker-compose.yml" exec -T redpanda rpk topic create "$queue" -p 1 -r 1 >/dev/null \
        || fail "could not create the kafka topic $queue"
      ;;
    artemis)
      artemis_up
      artemis_queue "$queue"
      ;;
    azureservicebus)
      log "starting the Service Bus emulator (the SQL Server backend needs ~2 GB and an amd64 image)"
      compose_up_no_wait "$EXAMPLES_DIR/asb-orders/docker-compose.yml"
      for _ in $(seq 1 90); do
        if curl -sf http://localhost:5300/health >/dev/null; then
          sleep 5
          return 0
        fi
        sleep 3
      done
      docker compose -f "$EXAMPLES_DIR/asb-orders/docker-compose.yml" logs --tail=60 >&2 || true
      fail "the Service Bus emulator did not become healthy"
      ;;
  esac
}

# compose_up_no_wait <file>: like compose_up without --wait (the emulator has no healthcheck of its own).
compose_up_no_wait() {
  local file="$1"
  if [ -n "$(docker compose -f "$file" ps -q --status running 2>/dev/null)" ]; then
    log "compose project of $file is already running: left as it is on exit"
  elif [ -n "$(docker compose -f "$file" ps -a -q 2>/dev/null)" ]; then
    COMPOSE_STARTED+=("stop:$file")
  else
    COMPOSE_STARTED+=("down:$file")
  fi
  docker compose -f "$file" up -d >/dev/null
}

# ---- running one broker ----------------------------------------------------------------------------

BROKER=""
RESULT=failing

write_record() {
  [ -n "${OTEL_RECORD_DIR:-}" ] || return 0
  [ -n "$BROKER" ] || return 0
  mkdir -p "$OTEL_RECORD_DIR"
  local run_url="${RUN_URL:-}"
  if [ -z "$run_url" ] && [ -n "${GITHUB_REPOSITORY:-}" ]; then
    run_url="${GITHUB_SERVER_URL:-https://github.com}/$GITHUB_REPOSITORY/actions/runs/${GITHUB_RUN_ID:-}"
  fi
  jq -n --arg broker "$BROKER" --arg producer "${B_PRODUCER:-}" --arg consumer "${B_CONSUMER:-}" \
    --arg system "${B_SYSTEM:-}" --arg status "$RESULT" \
    --arg run_url "$run_url" \
    '{kind: "otel", broker: $broker, producer: $producer, consumer: $consumer, system: $system, status: $status, run_url: $run_url}' \
    > "$OTEL_RECORD_DIR/otel-$BROKER.json"
}

on_exit() {
  local rc=$?
  write_record || true
  cleanup_interop
  return "$rc"
}

# run_producer / run_consumer: the probe commands, run with the env prepared by run_one.
run_producer() {
  case "$B_PRODUCER" in
    python) (cd "$OTEL_SRC/python" && "$WORK/otel-venv-$BROKER/bin/python" produce.py) ;;
    java) (cd "$WORK/otel-java" && java -cp "target/classes:$(cat cp.txt)" OtelProduce) ;;
  esac
}

run_consumer() {
  case "$B_CONSUMER" in
    go) "$WORK/otel-go/consumer-go" ;;
    node) (cd "$WORK/otel-node" && node consume-rabbitmq.mjs) ;;
    dotnet) dotnet "$WORK/otel-dotnet/out/Probe.dll" ;;
    python) (cd "$OTEL_SRC/python" && "$WORK/otel-venv-$BROKER/bin/python" consume_artemis.py) ;;
  esac
}

run_one() {
  BROKER="$1"
  broker_info "$BROKER"
  trap on_exit EXIT
  need jq

  local queue="bq-otel-$RUN_ID" spans="$WORK/otel-spans-$BROKER"
  # The emulator only has the queue its Config.json declares.
  [ "$BROKER" = azureservicebus ] && queue=orders
  mkdir -p "$spans"
  rm -f "$spans/producer.jsonl" "$spans/consumer.jsonl"

  log "otel $BROKER: $B_PRODUCER -> $B_CONSUMER, queue $queue ${BQ_SDK_ROOT:+(local SDKs: $BQ_SDK_ROOT)}"
  case "$B_PRODUCER" in python) otel_build_python "$BROKER" ;; java) otel_build_java ;; esac
  case "$B_CONSUMER" in go) otel_build_go ;; node) otel_build_node ;; dotnet) otel_build_dotnet ;; python) otel_build_python "$BROKER" ;; esac

  broker_up "$BROKER" "$queue"

  export OTEL_MESSAGES BROKER QUEUE="$queue" GROUP="bq-otel-$RUN_ID" CONSUME_TIMEOUT=60
  # The producer goes first: drain-style consumers (Go on Redis / SQS / Service Bus) stop when the queue is
  # empty. Pulsar (earliest position), Kafka (new group, first offset) and the durable Artemis / RabbitMQ
  # queues keep what was sent before the consumer attached.
  log "producer ($B_PRODUCER)"
  OTEL_SPAN_FILE="$spans/producer.jsonl" run_timeout 240 run_producer > "$WORK/otel-$BROKER-producer.log" 2>&1 \
    || { cat "$WORK/otel-$BROKER-producer.log" >&2; fail "the $B_PRODUCER producer failed"; }
  cat "$WORK/otel-$BROKER-producer.log"

  log "consumer ($B_CONSUMER)"
  OTEL_SPAN_FILE="$spans/consumer.jsonl" run_timeout 240 run_consumer > "$WORK/otel-$BROKER-consumer.log" 2>&1 \
    || { cat "$WORK/otel-$BROKER-consumer.log" >&2; fail "the $B_CONSUMER consumer failed"; }
  cat "$WORK/otel-$BROKER-consumer.log"

  if check_spans "$BROKER" "$spans/producer.jsonl" "$spans/consumer.jsonl" "$OTEL_MESSAGES" "$queue"; then
    RESULT=passing
    return 0
  fi
  return 1
}

# ---- main -----------------------------------------------------------------------------------------

usage() {
  sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//' >&2
  exit 2
}

main() {
  [ $# -ge 1 ] || usage
  case "$1" in
    -h|--help) usage ;;
    --check)
      [ $# -ge 4 ] || usage
      shift
      check_spans "$@"
      return
      ;;
  esac

  local -a brokers=()
  local arg
  for arg in "$@"; do
    if [ "$arg" = all ]; then brokers+=("${ALL_BROKERS[@]}"); else brokers+=("$arg"); fi
  done

  if [ "${#brokers[@]}" -eq 1 ]; then
    run_one "${brokers[0]}"
    return
  fi

  # Several brokers: one child per broker, so one failing broker does not stop the others and each cleans up
  # its own compose project.
  local broker failed=0 summary=""
  for broker in "${brokers[@]}"; do
    if "${BASH_SOURCE[0]}" "$broker"; then summary+="  PASS $broker"$'\n'; else summary+="  FAIL $broker"$'\n'; failed=1; fi
  done
  printf '\nOTel cross-hop summary:\n%s' "$summary"
  return "$failed"
}

main "$@"
