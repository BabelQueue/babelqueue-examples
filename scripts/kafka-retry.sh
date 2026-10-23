#!/usr/bin/env bash
# Kafka cross-language retry (R1 Faz 10, scope 5).
#
# A Java consumer (babelqueue-kafka) fails a record published by Python: the SDK republishes it to
# <topic>.retry.1 with attempts+1 and a bq-delay header. The Java KafkaRetryReinjector forwards it
# back to the work topic once the tier is due, and a Python consumer (KafkaTransport) processes it.
# kafka_probe.py `check` then reads the topics from the beginning and asserts:
#   - body attempts and the bq-attempts header both go up by one (5a)
#   - value and key of the re-injected record are byte-identical to the retry.1 record (5c)
#   - the re-injected record has no bq-delay / bq-original-topic and keeps traceparent (A7)
#   - the wait from the retry.1 record timestamp is at least the tier duration (A6); the re-injector
#     is already running and assigned before the retry write, and a second wall-clock check on the
#     probe lines (RETRIED -> REINJECTED) catches a re-injector that ignores bq-delay
#
# Usage: scripts/kafka-retry.sh            (published SDKs)
#        BQ_SDK_ROOT=/path/to/BabelQueue scripts/kafka-retry.sh   (in-repo SDK sources)
# Env:   KAFKA_BROKERS (default localhost:9092), KAFKA_NO_COMPOSE=1 to use an already running
#        broker (then set KAFKA_RPK to a topic-creation command, e.g. "rpk"), TIER_MS (default 10000).
set -euo pipefail
# shellcheck source=lib/interop-common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/interop-common.sh"

export KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
KAFKA_COMPOSE_FILE="${KAFKA_COMPOSE_FILE:-$KAFKA_DIR/docker-compose.yml}"
TIER_MS="${TIER_MS:-10000}"
TOPIC="bq-retry-$RUN_ID"
GROUP="bq-interop-$RUN_ID"
export TOPIC GROUP TIER_MS

rpk_cmd() {
  if [ -n "${KAFKA_RPK:-}" ]; then
    # shellcheck disable=SC2086
    $KAFKA_RPK "$@"
  else
    docker compose -f "$KAFKA_COMPOSE_FILE" exec -T redpanda rpk "$@"
  fi
}

if [ -z "${KAFKA_NO_COMPOSE:-}" ]; then
  log "starting redpanda"
  docker compose -f "$KAFKA_COMPOSE_FILE" up -d --wait redpanda >/dev/null
fi
rpk_cmd topic create "$TOPIC" "$TOPIC.retry.1" "$TOPIC.dlq" -p 1 -r 1 >/dev/null \
  || fail "could not create topics for $TOPIC"

build_probes kafka_java python

log "kafka-retry: topic=$TOPIC tier=${TIER_MS}ms"

# 1. Java re-injector (own group) goes first and is only trusted once its partitions are assigned:
#    JVM start and the group join must not be part of the wait that A6 measures.
start_probe kjava "$WORK/reinject.log" MODE=reinject "GROUP=$GROUP-reinject" RUN_S=150 -- run
reinject_pid="$PROBE_PID"
wait_line "$WORK/reinject.log" "^PROBE java ASSIGNED" 90 \
  || fail "java re-injector never got its partitions: $(cat "$WORK/reinject.log")"

# 2. Python publishes one envelope (record key + traceparent header).
run_probe kpython "$WORK/produce.log" -- produce || fail "python produce failed: $(cat "$WORK/produce.log")"
published_id="$(field "$WORK/produce.log" python PUBLISHED id)"
[ -n "$published_id" ] || fail "no published id: $(cat "$WORK/produce.log")"
traceparent="$(field "$WORK/produce.log" python PUBLISHED traceparent)"
[ -n "$traceparent" ] || fail "no traceparent printed by produce: $(cat "$WORK/produce.log")"

# 3. Java consumer fails the record; the SDK writes it to <topic>.retry.1 and commits the original.
run_probe kjava "$WORK/fail.log" MODE=fail RUN_S=60 -- run \
  || fail "java failing consumer exit=$?: $(cat "$WORK/fail.log")"
assert_eq "$(field "$WORK/fail.log" java FAILED id)" "$published_id" "java consumer saw the published message"
assert_eq "$(field "$WORK/fail.log" java FAILED attempts)" "0" "java first-delivery body attempts"
assert_eq "$(field "$WORK/fail.log" java FAILED bq_attempts)" "0" "java first-delivery bq-attempts header"
grep -q "^PROBE java RETRIED" "$WORK/fail.log" || fail "java consumer never reached the retry tier: $(cat "$WORK/fail.log")"
retried_at="$(field "$WORK/fail.log" java RETRIED at_ms)"
[ -n "$retried_at" ] || fail "RETRIED line has no at_ms: $(cat "$WORK/fail.log")"

# 4. Python consumer (same group as the failing consumer: it only sees records past the committed
#    original, so it can start after the retry write without missing the re-injected record).
start_probe kpython "$WORK/consume.log" TIMEOUT=120 -- consume
consumer_pid="$PROBE_PID"
wait_line "$WORK/consume.log" "^PROBE python READY" 30 || fail "python consumer never started: $(cat "$WORK/consume.log")"

# 5. The re-injector forwards the retry record once the tier is due. Its own clock: REINJECTED may
#    not come earlier than the tier after the retry write (500 ms slack for the commit after RETRIED).
wait_exit "$reinject_pid" 120
rc="$WAIT_RC"
[ "$rc" = "0" ] || fail "java re-injector exit=$rc: $(cat "$WORK/reinject.log")"
grep -q "^PROBE java REINJECTED" "$WORK/reinject.log" || fail "nothing re-injected: $(cat "$WORK/reinject.log")"
reinjected_at="$(field "$WORK/reinject.log" java REINJECTED at_ms)"
[ -n "$reinjected_at" ] || fail "REINJECTED line has no at_ms: $(cat "$WORK/reinject.log")"
reinject_wait=$((reinjected_at - retried_at))
[ "$reinject_wait" -ge $((TIER_MS - 500)) ] \
  || fail "re-injected ${reinject_wait}ms after the retry write, before the ${TIER_MS}ms tier elapsed (bq-delay ignored?)"
log "re-injected ${reinject_wait} ms after the retry write (tier ${TIER_MS} ms)"

# 6. Python processes the re-injected record: same message, attempts = 1.
wait_exit "$consumer_pid" 100
rc="$WAIT_RC"
[ "$rc" = "0" ] || fail "python consumer exit=$rc: $(cat "$WORK/consume.log")"
assert_eq "$(field "$WORK/consume.log" python RECEIVED id)" "$published_id" "python consumed the same message"
assert_eq "$(field "$WORK/consume.log" python RECEIVED attempts)" "1" "python received attempts"
log "kafka-retry: python processed $published_id with attempts=1"

# 7. Record-level assertions across the three topics.
run_probe kpython "$WORK/check.log" "TRACEPARENT=$traceparent" -- check || { cat "$WORK/check.log"; fail "kafka record checks failed"; }
grep '^KAFKA CHECK' "$WORK/check.log" | sed 's/^/[interop] /'

log "kafka-retry: OK"
