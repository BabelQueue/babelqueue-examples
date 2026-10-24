#!/usr/bin/env bash
# shellcheck disable=SC2034
# Shared helpers for the R1 interop scenarios (redis-delay, redis-recovery, redis-late-ack,
# redis-laravel-shared, kafka-retry). Source this file; it does nothing on its own.
#
# SDK source selection
#   default            the published SDK versions pinned in each probe's manifest
#   BQ_SDK_ROOT=<dir>  the in-repo SDK sources of a BabelQueue checkout (pre-release verification:
#                      Go `replace`, npm tarballs, `mvn install`, msbuild ProjectReference,
#                      `pip install -e`, the laravel/ vendor autoloader)
#
# Probe contract (redis-orders/interop/<lang>/): see redis-orders/interop/go/main.go.
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

RUN_ID="$(date +%s)$$" # unique queue/topic suffix per run
WORK="${INTEROP_WORK:-$(mktemp -d "${TMPDIR:-/tmp}/bq-interop.XXXXXX")}"
BG_PIDS=()

BQ_SDK_ROOT="${BQ_SDK_ROOT:-}"
# Published defaults (overridable). In local mode the in-repo versions are read from the manifests.
JAVA_CORE_VERSION="${BQ_JAVA_CORE_VERSION:-1.13.0}"
JAVA_REDIS_VERSION="${BQ_JAVA_REDIS_VERSION:-1.2.0}"

export REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"

log() { printf '[interop] %s\n' "$*"; }
fail() { printf '[interop] FAIL: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "required tool not found: $1"; }

cleanup_interop() {
  local pid
  for pid in ${BG_PIDS[@]+"${BG_PIDS[@]}"}; do
    [ -n "$pid" ] && kill -9 "$pid" >/dev/null 2>&1 || true
  done
  if [ -z "${INTEROP_WORK:-}" ] && [ -z "${KEEP_WORK:-}" ]; then
    rm -rf "$WORK"
  fi
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
    "$WORK/venv/bin/pip" install --quiet -r "$PROBE_SRC/python/requirements.txt" "babelqueue[kafka]>=1.22.0"
  fi
}

mvn_version() { mvn -q -f "$1" help:evaluate -Dexpression=project.version -DforceStdout; }

build_java() {
  need mvn
  need java
  local core="$JAVA_CORE_VERSION" redis="$JAVA_REDIS_VERSION"
  if [ -n "$BQ_SDK_ROOT" ]; then
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java/pom.xml" install -DskipTests
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java-redis/pom.xml" install -DskipTests
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
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java/pom.xml" install -DskipTests
    mvn -q -B -f "$BQ_SDK_ROOT/babelqueue-java-kafka/pom.xml" install -DskipTests
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
