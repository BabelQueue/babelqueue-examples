#!/usr/bin/env bash
# shellcheck disable=SC2034
# Shared helpers for the R1 and post-R1 interop scenarios (redis-delay, redis-recovery, redis-late-ack,
# redis-laravel-shared, kafka-retry, pulsar-exhaustion, version-gate, poison-dlq). Source this file; it does nothing on its own.
#
# SDK source selection
#   default            the published SDK versions pinned in each probe's manifest
#   BQ_SDK_ROOT=<dir>  the in-repo SDK sources of a BabelQueue checkout (pre-release verification:
#                      Go `replace`, npm tarballs, `mvn install`, msbuild ProjectReference,
#                      `pip install -e`, the laravel/ vendor autoloader)
#
# Probe contract (redis-orders/interop/<lang>/): see redis-orders/interop/go/main.go.
# The Pulsar exhaustion probes (pulsar-orders/interop/{go,java,node}) have their own contract: see the
# header of pulsar-orders/interop/go/main.go. The Artemis (artemis-orders/interop/{go,java,node,dotnet}) and SQS
# (sqs-orders/interop/{go,java,node,dotnet}) probes of version-gate.sh / poison-dlq.sh (ADR-0037) share one
# consumer contract: see the header of artemis-orders/interop/node/probe.mjs; the Go side of each is a producer and
# DLQ reader (artemis-orders/interop/go/main.go, sqs-orders/interop/go/main.go).
#
# Requires: bash 3.2+ (no associative arrays), docker compose (redis / redpanda services), and the toolchains of the probes
# a scenario uses (go, node+npm, mvn+java, dotnet, python3, php+composer).

set -euo pipefail

INTEROP_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLES_DIR="$(cd "$INTEROP_LIB_DIR/../.." && pwd)"
REDIS_DIR="$EXAMPLES_DIR/redis-orders"
PROBE_SRC="$REDIS_DIR/interop"
KAFKA_DIR="$EXAMPLES_DIR/kafka-orders"
KAFKA_SRC="$KAFKA_DIR/interop" # used by kafka-retry.sh
PULSAR_DIR="$EXAMPLES_DIR/pulsar-orders"
PULSAR_SRC="$PULSAR_DIR/interop" # used by pulsar-exhaustion.sh and version-gate.sh
ARTEMIS_DIR="$EXAMPLES_DIR/artemis-orders"
ARTEMIS_SRC="$ARTEMIS_DIR/interop" # used by version-gate.sh and poison-dlq.sh
SQS_DIR="$EXAMPLES_DIR/sqs-orders"
SQS_SRC="$SQS_DIR/interop" # used by poison-dlq.sh

RUN_ID="$(date +%s)$$" # unique queue/topic suffix per run
WORK="${INTEROP_WORK:-$(mktemp -d "${TMPDIR:-/tmp}/bq-interop.XXXXXX")}"
BG_PIDS=()
COMPOSE_STARTED=() # "down:<file>" / "stop:<file>" for compose projects this run started; see compose_up

BQ_SDK_ROOT="${BQ_SDK_ROOT:-}"
# Published defaults (overridable). In local mode the in-repo versions are read from the manifests.
JAVA_CORE_VERSION="${BQ_JAVA_CORE_VERSION:-1.13.0}"
JAVA_REDIS_VERSION="${BQ_JAVA_REDIS_VERSION:-1.2.0}"
# babelqueue-pulsar 1.2.0 is the first release with the bounded redelivery of ADR-0036; 1.3.0 adds the ADR-0037 version gate. The Node probe
# pins @babelqueue/pulsar in pulsar-orders/interop/node/package.json the same way; the Go probe only
# produces and reads, so it uses the published Go modules.
JAVA_PULSAR_VERSION="${BQ_JAVA_PULSAR_VERSION:-1.3.0}"
# The ADR-0037 scenarios need the ADR-0037 releases: babelqueue-artemis 1.1.0 and babelqueue-sqs 1.4.0 (and
# babelqueue-pulsar 1.3.0 above); the manifests pin them, so the scenarios also run without BQ_SDK_ROOT.
JAVA_ARTEMIS_VERSION="${BQ_JAVA_ARTEMIS_VERSION:-1.1.0}"
JAVA_SQS_VERSION="${BQ_JAVA_SQS_VERSION:-1.4.0}"

export REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"

log() { printf '[interop] %s\n' "$*"; }
fail() { printf '[interop] FAIL: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "required tool not found: $1"; }

cleanup_interop() {
  local pid
  for pid in ${BG_PIDS[@]+"${BG_PIDS[@]}"}; do
    [ -n "$pid" ] && kill -9 "$pid" >/dev/null 2>&1 || true
  done
  # Only compose projects this run itself started (compose_up): "down:" ones did not exist before, so they and their
  # volumes go; "stop:" ones existed but were stopped, so they are stopped again with their volumes kept. A project
  # that was already running, and a broker you run yourself (*_NO_COMPOSE), is never touched. KEEP_BROKERS=1 touches none.
  local entry
  if [ -z "${KEEP_BROKERS:-}" ]; then
    for entry in ${COMPOSE_STARTED[@]+"${COMPOSE_STARTED[@]}"}; do
      case "$entry" in
        down:*) docker compose -f "${entry#down:}" down -v >/dev/null 2>&1 || true ;;
        stop:*) docker compose -f "${entry#stop:}" stop >/dev/null 2>&1 || true ;;
      esac
    done
  fi
  if [ -z "${INTEROP_WORK:-}" ] && [ -z "${KEEP_WORK:-}" ]; then
    rm -rf "$WORK"
  fi
}

# compose_up <file> <wait-timeout-seconds>: `docker compose up -d --wait`, remembering what the EXIT trap may undo.
# It is registered BEFORE the up, so a half-started project is cleaned up too.
compose_up() {
  local file="$1" timeout="$2"
  if [ -n "$(docker compose -f "$file" ps -q --status running 2>/dev/null)" ]; then
    log "compose project of $file is already running: left as it is on exit"
  elif [ -n "$(docker compose -f "$file" ps -a -q 2>/dev/null)" ]; then
    COMPOSE_STARTED+=("stop:$file") # existing, stopped: stop it again, keep its volumes
  else
    COMPOSE_STARTED+=("down:$file") # did not exist: remove it and the volumes this run created
  fi
  docker compose -f "$file" up -d --wait --wait-timeout "$timeout" >/dev/null
}
trap cleanup_interop EXIT

# ---- redis -----------------------------------------------------------------------------------

REDIS_COMPOSE_FILE="${REDIS_COMPOSE_FILE:-$REDIS_DIR/docker-compose.yml}"

rcli() {
  if [ -n "${REDIS_CLI:-}" ]; then
    "$REDIS_CLI" "$@"
  else
    docker compose -f "$REDIS_COMPOSE_FILE" exec -T redis redis-cli "$@"
  fi
}

redis_up() {
  if [ -z "${REDIS_CLI:-}" ]; then
    docker compose -f "$REDIS_COMPOSE_FILE" up -d redis >/dev/null
  fi
  local i
  for i in $(seq 1 30); do
    if [ "$(rcli PING 2>/dev/null | tr -d '\r')" = "PONG" ]; then return 0; fi
    sleep 1
  done
  fail "redis did not become ready"
}

# redis_int <command...>: prints the integer reply with the CR stripped.
redis_int() { rcli "$@" | tr -d '\r'; }

# ---- building the probes ---------------------------------------------------------------------

build_go() {
  need go
  rm -rf "$WORK/go" && cp -r "$PROBE_SRC/go" "$WORK/go"
  (
    cd "$WORK/go"
    if [ -n "$BQ_SDK_ROOT" ]; then
      go mod edit \
        -replace "github.com/babelqueue/babelqueue-go=$BQ_SDK_ROOT/babelqueue-go" \
        -replace "github.com/babelqueue/babelqueue-go/redis=$BQ_SDK_ROOT/babelqueue-go/redis"
    fi
    GOFLAGS=-mod=mod go build -o "$WORK/go/probe-go" .
  )
}

build_node() {
  need node
  need npm
  rm -rf "$WORK/node" && cp -r "$PROBE_SRC/node" "$WORK/node"
  (
    cd "$WORK/node"
    if [ -n "$BQ_SDK_ROOT" ]; then
      # Tarballs, not symlinks: a symlinked workspace package resolves its own node_modules copy
      # of @babelqueue/core and the two would not share classes.
      mkdir -p "$WORK/tgz"
      (cd "$BQ_SDK_ROOT/babelqueue-node" && { [ -d node_modules ] || npm ci --silent; } \
        && npm run --silent build && npm pack --silent --pack-destination "$WORK/tgz" >/dev/null)
      (cd "$BQ_SDK_ROOT/babelqueue-node-adapters" && { [ -d node_modules ] || npm ci --silent; } \
        && npm run --silent build -w @babelqueue/redis \
        && npm pack --silent -w @babelqueue/redis --pack-destination "$WORK/tgz" >/dev/null)
      npm install --silent --no-audit --no-fund "$WORK"/tgz/babelqueue-core-*.tgz \
        "$WORK"/tgz/babelqueue-redis-*.tgz ioredis
    else
      npm install --silent --no-audit --no-fund
    fi
  )
}

build_python() {
  need python3
  python3 -m venv "$WORK/venv"
  "$WORK/venv/bin/pip" install --quiet --upgrade pip
  if [ -n "$BQ_SDK_ROOT" ]; then
    "$WORK/venv/bin/pip" install --quiet -e "$BQ_SDK_ROOT/babelqueue-python[redis,kafka]"
  else
    "$WORK/venv/bin/pip" install --quiet -r "$PROBE_SRC/python/requirements.txt" "babelqueue[kafka]>=1.23.0"
  fi
}

# Every `mvn install` of an SDK below passes -Djacoco.skip=true: -DskipTests leaves a stale target/jacoco.exec behind
# from an earlier run, and the JaCoCo `check` goal bound to `verify` then fails the install on coverage the skipped
# run never measured. The probes need the jar, not a coverage gate.
mvn_version() { mvn -q -f "$1" help:evaluate -Dexpression=project.version -DforceStdout; }

build_java() {
  need mvn
  need java
  local core="$JAVA_CORE_VERSION" redis="$JAVA_REDIS_VERSION"
  if [ -n "$BQ_SDK_ROOT" ]; then
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java/pom.xml" install -DskipTests -Djacoco.skip=true
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java-redis/pom.xml" install -DskipTests -Djacoco.skip=true
    core="$(mvn_version "$BQ_SDK_ROOT/babelqueue-java/pom.xml")"
    redis="$(mvn_version "$BQ_SDK_ROOT/babelqueue-java-redis/pom.xml")"
  fi
  rm -rf "$WORK/java" && cp -r "$PROBE_SRC/java" "$WORK/java"
  (
    cd "$WORK/java"
    mvn -q -B compile "-Dbabelqueue-core.version=$core" "-Dbabelqueue-redis.version=$redis"
    mvn -q -B dependency:build-classpath -Dmdep.outputFile=cp.txt \
      "-Dbabelqueue-core.version=$core" "-Dbabelqueue-redis.version=$redis"
  )
}

# build_kafka_java: the Kafka retry probe (kafka-orders/interop/java) into $WORK/kjava.
build_kafka_java() {
  need mvn
  need java
  local core="$JAVA_CORE_VERSION" kafka="${BQ_JAVA_KAFKA_VERSION:-1.3.0}"
  if [ -n "$BQ_SDK_ROOT" ]; then
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java/pom.xml" install -DskipTests -Djacoco.skip=true
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java-kafka/pom.xml" install -DskipTests -Djacoco.skip=true
    core="$(mvn_version "$BQ_SDK_ROOT/babelqueue-java/pom.xml")"
    kafka="$(mvn_version "$BQ_SDK_ROOT/babelqueue-java-kafka/pom.xml")"
  fi
  rm -rf "$WORK/kjava" && cp -r "$KAFKA_SRC/java" "$WORK/kjava"
  (
    cd "$WORK/kjava"
    mvn -q -B compile "-Dbabelqueue-core.version=$core" "-Dbabelqueue-kafka.version=$kafka"
    mvn -q -B dependency:build-classpath -Dmdep.outputFile=cp.txt \
      "-Dbabelqueue-core.version=$core" "-Dbabelqueue-kafka.version=$kafka"
  )
}

# build_pulsar_go: the Pulsar exhaustion producer / DLQ reader (pulsar-orders/interop/go) into $WORK/pgo.
build_pulsar_go() {
  need go
  rm -rf "$WORK/pgo" && cp -r "$PULSAR_SRC/go" "$WORK/pgo"
  (
    cd "$WORK/pgo"
    if [ -n "$BQ_SDK_ROOT" ]; then
      go mod edit \
        -replace "github.com/babelqueue/babelqueue-go=$BQ_SDK_ROOT/babelqueue-go" \
        -replace "github.com/babelqueue/babelqueue-go/pulsar=$BQ_SDK_ROOT/babelqueue-go/pulsar"
    fi
    GOFLAGS=-mod=mod go build -o "$WORK/pgo/probe-go" .
  )
}

# build_pulsar_java: the Pulsar exhaustion consumer (pulsar-orders/interop/java) into $WORK/pjava.
build_pulsar_java() {
  need mvn
  need java
  local core="$JAVA_CORE_VERSION" pulsar="$JAVA_PULSAR_VERSION"
  if [ -n "$BQ_SDK_ROOT" ]; then
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java/pom.xml" install -DskipTests -Djacoco.skip=true
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java-pulsar/pom.xml" install -DskipTests -Djacoco.skip=true
    core="$(mvn_version "$BQ_SDK_ROOT/babelqueue-java/pom.xml")"
    pulsar="$(mvn_version "$BQ_SDK_ROOT/babelqueue-java-pulsar/pom.xml")"
  fi
  rm -rf "$WORK/pjava" && cp -r "$PULSAR_SRC/java" "$WORK/pjava"
  (
    cd "$WORK/pjava"
    mvn -q -B compile "-Dbabelqueue-core.version=$core" "-Dbabelqueue-pulsar.version=$pulsar"
    mvn -q -B dependency:build-classpath -Dmdep.outputFile=cp.txt \
      "-Dbabelqueue-core.version=$core" "-Dbabelqueue-pulsar.version=$pulsar"
  )
}

# build_pulsar_node: the Pulsar exhaustion consumer (pulsar-orders/interop/node) into $WORK/pnode.
build_pulsar_node() {
  need node
  need npm
  rm -rf "$WORK/pnode" && cp -r "$PULSAR_SRC/node" "$WORK/pnode"
  (
    cd "$WORK/pnode"
    if [ -n "$BQ_SDK_ROOT" ]; then
      # Tarballs, not symlinks (see build_node): one copy of @babelqueue/core for both packages.
      mkdir -p "$WORK/tgz-pulsar"
      (cd "$BQ_SDK_ROOT/babelqueue-node" && { [ -d node_modules ] || npm ci --silent; } \
        && npm run --silent build && npm pack --silent --pack-destination "$WORK/tgz-pulsar" >/dev/null)
      (cd "$BQ_SDK_ROOT/babelqueue-node-adapters" && { [ -d node_modules ] || npm ci --silent; } \
        && npm run --silent build -w @babelqueue/pulsar \
        && npm pack --silent -w @babelqueue/pulsar --pack-destination "$WORK/tgz-pulsar" >/dev/null)
      npm install --silent --no-audit --no-fund "$WORK"/tgz-pulsar/babelqueue-core-*.tgz \
        "$WORK"/tgz-pulsar/babelqueue-pulsar-*.tgz pulsar-client
    else
      npm install --silent --no-audit --no-fund
    fi
  )
}

build_dotnet() {
  need dotnet
  rm -rf "$WORK/dotnet" && cp -r "$PROBE_SRC/dotnet" "$WORK/dotnet"
  (
    cd "$WORK/dotnet"
    if [ -n "$BQ_SDK_ROOT" ]; then
      dotnet build -c Release -nologo -v q -o "$WORK/dotnet/out" "-p:BqSdkRoot=$BQ_SDK_ROOT"
    else
      dotnet build -c Release -nologo -v q -o "$WORK/dotnet/out"
    fi
  )
}

build_laravel() {
  need php
  rm -rf "$WORK/laravel" && cp -r "$PROBE_SRC/laravel" "$WORK/laravel"
  if [ -n "$BQ_SDK_ROOT" ]; then
    [ -f "$BQ_SDK_ROOT/laravel/vendor/autoload.php" ] \
      || (cd "$BQ_SDK_ROOT/laravel" && composer install --no-interaction --quiet)
    LARAVEL_AUTOLOAD="$BQ_SDK_ROOT/laravel/vendor/autoload.php"
  else
    need composer
    (cd "$WORK/laravel" && composer install --no-interaction --quiet)
    LARAVEL_AUTOLOAD="$WORK/laravel/vendor/autoload.php"
  fi
}

# need_adr0037_sdks: the version-gate and poison-dlq scenarios assert ADR-0037 behaviour. The probe manifests now
# pin the ADR-0037 releases, but a copy of the manifests edited back to older pins would fail confusingly, so a run
# without BQ_SDK_ROOT still needs BQ_ALLOW_PUBLISHED=1 (the nightly matrix sets it through `published: "1"`).
need_adr0037_sdks() {
  if [ -z "$BQ_SDK_ROOT" ] && [ -z "${BQ_ALLOW_PUBLISHED:-}" ]; then
    fail "$1 asserts ADR-0037 behaviour; run it with BQ_SDK_ROOT=<BabelQueue checkout>, or with BQ_ALLOW_PUBLISHED=1 to use the ADR-0037 releases pinned in the probe manifests"
  fi
}

# ---- artemis ---------------------------------------------------------------------------------

ARTEMIS_COMPOSE_FILE="${ARTEMIS_COMPOSE_FILE:-$ARTEMIS_DIR/docker-compose.yml}"
export ARTEMIS_URL="${ARTEMIS_URL:-amqp://artemis:artemis@localhost:5672}"
export ARTEMIS_JMS_URL="${ARTEMIS_JMS_URL:-tcp://localhost:61616}"

artemis_up() {
  if [ -z "${ARTEMIS_NO_COMPOSE:-}" ]; then
    need docker
    log "starting artemis"
    compose_up "$ARTEMIS_COMPOSE_FILE" 180
  elif [ -z "${ARTEMIS_CLI:-}" ]; then
    fail "ARTEMIS_NO_COMPOSE=1 needs ARTEMIS_CLI=<path to the broker's bin/artemis> to provision the anycast queues"
  fi
}

# artemis_queue <name>...: provision anycast queues (§7). An AMQP 1.0 attach that carries no queue capability
# would otherwise auto-create the address MULTICAST and a JMS consumer's anycast attach would be refused.
artemis_queue() {
  local name
  for name in "$@"; do
    if [ -n "${ARTEMIS_NO_COMPOSE:-}" ]; then
      "$ARTEMIS_CLI" queue create --url "$ARTEMIS_JMS_URL" \
        --user artemis --password artemis --name "$name" --address "$name" \
        --anycast --durable --auto-create-address --silent >/dev/null \
        || fail "could not provision the artemis queue $name with ARTEMIS_CLI=$ARTEMIS_CLI"
    else
      docker compose -f "$ARTEMIS_COMPOSE_FILE" exec -T artemis /var/lib/artemis-instance/bin/artemis queue create \
        --user artemis --password artemis --name "$name" --address "$name" \
        --anycast --durable --auto-create-address --silent >/dev/null \
        || fail "could not provision the artemis queue $name"
    fi
  done
}

# build_artemis_go: the Artemis producer / DLQ reader (artemis-orders/interop/go) into $WORK/ago.
build_artemis_go() {
  need go
  rm -rf "$WORK/ago" && cp -r "$ARTEMIS_SRC/go" "$WORK/ago"
  (
    cd "$WORK/ago"
    if [ -n "$BQ_SDK_ROOT" ]; then
      go mod edit -replace "github.com/babelqueue/babelqueue-go=$BQ_SDK_ROOT/babelqueue-go"
    fi
    GOFLAGS=-mod=mod go build -o "$WORK/ago/probe-go" .
  )
}

# build_artemis_java: the JMS consumer (artemis-orders/interop/java) into $WORK/ajava.
build_artemis_java() {
  need mvn
  need java
  local core="$JAVA_CORE_VERSION" artemis="$JAVA_ARTEMIS_VERSION"
  if [ -n "$BQ_SDK_ROOT" ]; then
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java/pom.xml" install -DskipTests -Djacoco.skip=true
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java-artemis/pom.xml" install -DskipTests -Djacoco.skip=true
    core="$(mvn_version "$BQ_SDK_ROOT/babelqueue-java/pom.xml")"
    artemis="$(mvn_version "$BQ_SDK_ROOT/babelqueue-java-artemis/pom.xml")"
  fi
  rm -rf "$WORK/ajava" && cp -r "$ARTEMIS_SRC/java" "$WORK/ajava"
  (
    cd "$WORK/ajava"
    mvn -q -B compile "-Dbabelqueue-core.version=$core" "-Dbabelqueue-artemis.version=$artemis"
    mvn -q -B dependency:build-classpath -Dmdep.outputFile=cp.txt \
      "-Dbabelqueue-core.version=$core" "-Dbabelqueue-artemis.version=$artemis"
  )
}

# build_artemis_node: the rhea consumer (artemis-orders/interop/node) into $WORK/anode.
build_artemis_node() {
  need node
  need npm
  rm -rf "$WORK/anode" && cp -r "$ARTEMIS_SRC/node" "$WORK/anode"
  (
    cd "$WORK/anode"
    if [ -n "$BQ_SDK_ROOT" ]; then
      # Tarballs, not symlinks (see build_node): one copy of @babelqueue/core for both packages.
      mkdir -p "$WORK/tgz-artemis"
      (cd "$BQ_SDK_ROOT/babelqueue-node" && { [ -d node_modules ] || npm ci --silent; } \
        && npm run --silent build && npm pack --silent --pack-destination "$WORK/tgz-artemis" >/dev/null)
      (cd "$BQ_SDK_ROOT/babelqueue-node-adapters" && { [ -d node_modules ] || npm ci --silent; } \
        && npm run --silent build -w @babelqueue/artemis \
        && npm pack --silent -w @babelqueue/artemis --pack-destination "$WORK/tgz-artemis" >/dev/null)
      npm install --silent --no-audit --no-fund "$WORK"/tgz-artemis/babelqueue-core-*.tgz \
        "$WORK"/tgz-artemis/babelqueue-artemis-*.tgz rhea
    else
      npm install --silent --no-audit --no-fund
    fi
  )
}

# build_artemis_dotnet: the AMQPNetLite consumer (artemis-orders/interop/dotnet) into $WORK/adotnet.
build_artemis_dotnet() {
  need dotnet
  rm -rf "$WORK/adotnet" && cp -r "$ARTEMIS_SRC/dotnet" "$WORK/adotnet"
  (
    cd "$WORK/adotnet"
    if [ -n "$BQ_SDK_ROOT" ]; then
      dotnet build -c Release -nologo -v q -o "$WORK/adotnet/out" "-p:BqSdkRoot=$BQ_SDK_ROOT"
    else
      dotnet build -c Release -nologo -v q -o "$WORK/adotnet/out"
    fi
  )
}

# ---- sqs (ElasticMQ) -------------------------------------------------------------------------

SQS_COMPOSE_FILE="${SQS_COMPOSE_FILE:-$SQS_DIR/docker-compose.yml}"
export SQS_ENDPOINT="${SQS_ENDPOINT:-http://localhost:9324}"
export AWS_REGION="${AWS_REGION:-eu-central-1}"
# ElasticMQ accepts any credentials; the probes pass their own, these cover any default-chain client.
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"

sqs_up() {
  if [ -z "${SQS_NO_COMPOSE:-}" ]; then
    need docker
    log "starting elasticmq"
    compose_up "$SQS_COMPOSE_FILE" 120
  fi
  local i
  for i in $(seq 1 30); do
    if curl -fsS "$SQS_ENDPOINT/?Action=ListQueues" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  fail "elasticmq did not become ready at $SQS_ENDPOINT"
}

# build_sqs_go: the SQS provisioner / producer / DLQ reader (sqs-orders/interop/go) into $WORK/sgo.
# It uses the AWS SDK only, so BQ_SDK_ROOT does not change it.
build_sqs_go() {
  need go
  rm -rf "$WORK/sgo" && cp -r "$SQS_SRC/go" "$WORK/sgo"
  (cd "$WORK/sgo" && GOFLAGS=-mod=mod go build -o "$WORK/sgo/probe-go" .)
}

# build_sqs_java: the Java consumer (sqs-orders/interop/java) into $WORK/sjava.
build_sqs_java() {
  need mvn
  need java
  local core="$JAVA_CORE_VERSION" sqs="$JAVA_SQS_VERSION"
  if [ -n "$BQ_SDK_ROOT" ]; then
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java/pom.xml" install -DskipTests -Djacoco.skip=true
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java-sqs/pom.xml" install -DskipTests -Djacoco.skip=true
    core="$(mvn_version "$BQ_SDK_ROOT/babelqueue-java/pom.xml")"
    sqs="$(mvn_version "$BQ_SDK_ROOT/babelqueue-java-sqs/pom.xml")"
  fi
  rm -rf "$WORK/sjava" && cp -r "$SQS_SRC/java" "$WORK/sjava"
  (
    cd "$WORK/sjava"
    mvn -q -B compile "-Dbabelqueue-core.version=$core" "-Dbabelqueue-sqs.version=$sqs"
    mvn -q -B dependency:build-classpath -Dmdep.outputFile=cp.txt \
      "-Dbabelqueue-core.version=$core" "-Dbabelqueue-sqs.version=$sqs"
  )
}

# build_sqs_node: the Node consumer (sqs-orders/interop/node) into $WORK/snode.
build_sqs_node() {
  need node
  need npm
  rm -rf "$WORK/snode" && cp -r "$SQS_SRC/node" "$WORK/snode"
  (
    cd "$WORK/snode"
    if [ -n "$BQ_SDK_ROOT" ]; then
      mkdir -p "$WORK/tgz-sqs"
      (cd "$BQ_SDK_ROOT/babelqueue-node" && { [ -d node_modules ] || npm ci --silent; } \
        && npm run --silent build && npm pack --silent --pack-destination "$WORK/tgz-sqs" >/dev/null)
      (cd "$BQ_SDK_ROOT/babelqueue-node-adapters" && { [ -d node_modules ] || npm ci --silent; } \
        && npm run --silent build -w @babelqueue/sqs \
        && npm pack --silent -w @babelqueue/sqs --pack-destination "$WORK/tgz-sqs" >/dev/null)
      npm install --silent --no-audit --no-fund "$WORK"/tgz-sqs/babelqueue-core-*.tgz \
        "$WORK"/tgz-sqs/babelqueue-sqs-*.tgz @aws-sdk/client-sqs
    else
      npm install --silent --no-audit --no-fund
    fi
  )
}

# build_sqs_dotnet: the .NET consumer (sqs-orders/interop/dotnet) into $WORK/sdotnet.
build_sqs_dotnet() {
  need dotnet
  rm -rf "$WORK/sdotnet" && cp -r "$SQS_SRC/dotnet" "$WORK/sdotnet"
  (
    cd "$WORK/sdotnet"
    if [ -n "$BQ_SDK_ROOT" ]; then
      dotnet build -c Release -nologo -v q -o "$WORK/sdotnet/out" "-p:BqSdkRoot=$BQ_SDK_ROOT"
    else
      dotnet build -c Release -nologo -v q -o "$WORK/sdotnet/out"
    fi
  )
}

build_probes() {
  local lang
  for lang in "$@"; do
    log "building probe: $lang ${BQ_SDK_ROOT:+(local SDK: $BQ_SDK_ROOT)}"
    "build_$lang"
  done
}

# probe_exec <lang> <args...>: replaces the current shell with the probe (call it inside a
# subshell so $! is the probe's own PID, which is what `kill -9` must hit).
probe_exec() {
  local lang="$1"
  shift
  case "$lang" in
    go) exec "$WORK/go/probe-go" "$@" ;;
    node) cd "$WORK/node" && exec node probe.mjs "$@" ;;
    java) cd "$WORK/java" && exec java -cp "target/classes:$(cat cp.txt)" Probe "$@" ;;
    dotnet) exec dotnet "$WORK/dotnet/out/Probe.dll" "$@" ;;
    python) exec "$WORK/venv/bin/python" "$PROBE_SRC/python/probe.py" "$@" ;;
    kjava) cd "$WORK/kjava" && exec java -cp "target/classes:$(cat cp.txt)" RetryProbe "$@" ;;
    kpython) exec "$WORK/venv/bin/python" "$KAFKA_SRC/python/kafka_probe.py" "$@" ;;
    pgo) exec "$WORK/pgo/probe-go" "$@" ;;
    pjava) cd "$WORK/pjava" && exec java -cp "target/classes:$(cat cp.txt)" Probe "$@" ;;
    pnode) cd "$WORK/pnode" && exec node probe.mjs "$@" ;;
    ago) exec "$WORK/ago/probe-go" "$@" ;;
    ajava) cd "$WORK/ajava" && exec java -cp "target/classes:$(cat cp.txt)" Probe "$@" ;;
    anode) cd "$WORK/anode" && exec node probe.mjs "$@" ;;
    adotnet) exec dotnet "$WORK/adotnet/out/Probe.dll" "$@" ;;
    sgo) exec "$WORK/sgo/probe-go" "$@" ;;
    sjava) cd "$WORK/sjava" && exec java -cp "target/classes:$(cat cp.txt)" Probe "$@" ;;
    snode) cd "$WORK/snode" && exec node probe.mjs "$@" ;;
    sdotnet) exec dotnet "$WORK/sdotnet/out/Probe.dll" "$@" ;;
    laravel) AUTOLOAD="$LARAVEL_AUTOLOAD" exec php "$WORK/laravel/probe.php" "$@" ;;
    *) fail "unknown probe language: $lang" ;;
  esac
}

# split_env <args...>: VAR=value pairs up to "--" go to PROBE_ENV, the rest to PROBE_ARGS.
split_env() {
  PROBE_ENV=()
  PROBE_ARGS=()
  while [ $# -gt 0 ] && [ "$1" != "--" ]; do
    PROBE_ENV+=("$1")
    shift
  done
  [ $# -gt 0 ] && shift
  PROBE_ARGS=("$@")
}

# start_probe <lang> <logfile> [VAR=value ...] -- <args...>
# Starts the probe in the background; stdout+stderr go to the logfile. Sets PROBE_PID.
start_probe() {
  local lang="$1" logfile="$2"
  shift 2
  split_env "$@"
  : >"$logfile"
  (
    # An empty PROBE_ENV must not reach `export`: bash >= 4.4 would dump the whole environment
    # (secrets included) to the log, bash 3.2 with `set -u` would abort on the empty array.
    if [ ${#PROBE_ENV[@]} -gt 0 ]; then export "${PROBE_ENV[@]}"; fi
    probe_exec "$lang" ${PROBE_ARGS[@]+"${PROBE_ARGS[@]}"}
  ) >"$logfile" 2>&1 &
  PROBE_PID=$!
  BG_PIDS+=("$PROBE_PID")
}

# run_probe <lang> <logfile> [VAR=value ...] -- <args...>: foreground; returns the probe's exit code.
run_probe() {
  local lang="$1" logfile="$2" rc=0
  shift 2
  split_env "$@"
  (
    if [ ${#PROBE_ENV[@]} -gt 0 ]; then export "${PROBE_ENV[@]}"; fi
    probe_exec "$lang" ${PROBE_ARGS[@]+"${PROBE_ARGS[@]}"}
  ) >"$logfile" 2>&1 || rc=$?
  return "$rc"
}

# ---- waiting and parsing ---------------------------------------------------------------------

# dump_logs <file>...: print probe logs to stderr (call before `fail` to keep the evidence).
dump_logs() {
  local f
  for f in "$@"; do
    [ -f "$f" ] && { printf '[interop] --- %s ---\n' "$f" >&2; cat "$f" >&2; }
  done
  return 0
}

# stop_probe <pid>: terminate a background probe and reap it.
stop_probe() {
  kill "$1" >/dev/null 2>&1 || true
  wait "$1" 2>/dev/null || true
}

# wait_line <logfile> <ERE> <timeout-seconds>
wait_line() {
  local logfile="$1" pattern="$2" timeout="$3" n
  for n in $(seq 1 $((timeout * 10))); do
    if grep -Eq -- "$pattern" "$logfile" 2>/dev/null; then return 0; fi
    sleep 0.1
  done
  return 1
}

# wait_exit <pid> <timeout-seconds>: waits for a background probe started by THIS shell and stores
# its exit code in WAIT_RC (or the string "timeout"). Call it directly, never inside $(...): `wait`
# only reaps children of the calling shell, so a command substitution loses the real exit code
# (bash 3.2 reports -1 for every status).
WAIT_RC=""
wait_exit() {
  local pid="$1" timeout="$2" i rc=0
  WAIT_RC="timeout"
  for i in $(seq 1 $((timeout * 10))); do
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid" 2>/dev/null || rc=$?
      WAIT_RC="$rc"
      return 0
    fi
    sleep 0.1
  done
  return 0
}

# field <logfile> <lang> <EVENT> <name>: value of name=... on the last "PROBE <lang> <EVENT>" line.
field() {
  local logfile="$1" lang="$2" event="$3" name="$4"
  grep -E "^PROBE $lang $event( |$)" "$logfile" | tail -n 1 | tr ' ' '\n' | sed -n "s/^$name=//p" | head -n 1 || true
}

event_count() { grep -Ec "^PROBE $2 $3( |\$)" "$1" || true; }

assert_eq() {
  [ "$1" = "$2" ] || fail "$3: expected '$2', got '$1'"
}

# in_range <value> <min> <max> (integers)
in_range() { [ "$1" -ge "$2" ] && [ "$1" -le "$3" ]; }

# ms_to_s <ms>: 2034 -> 2.034
ms_to_s() { awk -v ms="$1" 'BEGIN { printf "%.3f", ms / 1000 }'; }

# publish_one <queue> <layout> <logfile>: Go produces one message (plain publish, delay 0);
# echoes the message id.
publish_one() {
  local queue="$1" layout="$2" logfile="$3"
  run_probe go "$logfile" "QUEUE=$queue" "LAYOUT=$layout" DELAY=0 COUNT=1 -- produce \
    || fail "go produce failed: $(cat "$logfile")"
  field "$logfile" go PUBLISHED id
}
