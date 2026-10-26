#!/usr/bin/env bash
# Version gate across brokers and languages (ADR-0037 decision 1, broker-bindings §3.7/§5.7/§7.7).
#
# A Go producer publishes a VALID envelope whose schema-version property is not the supported one
# (bq-schema-version "2" on Pulsar, bq_schema_version "2" on Artemis). The body would decode, so a consumer that
# settles it did so on the property alone. A consumer in another language (a handler that records that it ran) must:
#
#   - never call the handler, and settle the message on its FIRST delivery, never retrying it;
#   - dlq on, strategy not set     -> write the RAW record to <queue>.dlq: the body byte for byte (the Go reader compares
#                                     the SHA-256 of what it received with the SHA-256 of what it sent), with NO
#                                     dead_letter block and no re-encoding; then acknowledge. Exactly one record
#                                     arrives (the reader's quiet window shows nothing is retried or duplicated);
#   - dlq off (Pulsar, Artemis)    -> acknowledge and drop: nothing on <queue>.dlq, nothing left on the work queue;
#   - dlq on, on_unknown_urn delete (Artemis; the Java, Node and .NET Pulsar consumers have no such option)
#                                  -> acknowledge without a record.
#
# Brokers (GATE_BROKERS, default "pulsar artemis") and consumers:
#   pulsar   Go producer / DLQ reader; Java (babelqueue-pulsar) and Node (@babelqueue/pulsar) consumers
#   artemis  Go producer (AMQP 1.0) / DLQ reader; Java (JMS), Node (rhea) and .NET (AMQPNetLite) consumers
# SQS and Azure Service Bus hand a gated message to the broker's native mechanism when the BabelQueue DLQ is off and
# have no live scenario here; poison-dlq.sh covers SQS, and the conformance suite (`version_gate_sqs`, `version_gate_asb`)
# covers both rows.
#
# Usage: BQ_SDK_ROOT=/path/to/BabelQueue scripts/version-gate.sh
#        (without BQ_SDK_ROOT it needs BQ_ALLOW_PUBLISHED=1 and then runs against the ADR-0037 releases pinned in the
#        probe manifests.)
# Env:   GATE_BROKERS, PULSAR_CONSUMERS (default "java node"), ARTEMIS_CONSUMERS (default "java node dotnet"),
#        PULSAR_URL, PULSAR_ADMIN_URL, PULSAR_NO_COMPOSE=1, ARTEMIS_URL, ARTEMIS_JMS_URL, ARTEMIS_NO_COMPOSE=1,
#        QUIET_S (default 8: the DLQ reader's quiet window), SETTLE_S (default 6: how long a consumer is given when no
#        record is expected), CASE_TIMEOUT_S (default 90). Needs curl and jq besides the probe toolchains.
set -euo pipefail
# shellcheck source=lib/interop-common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/interop-common.sh"

need_adr0037_sdks version-gate.sh

GATE_BROKERS="${GATE_BROKERS:-pulsar artemis}"
PULSAR_CONSUMERS="${PULSAR_CONSUMERS:-java node}"
ARTEMIS_CONSUMERS="${ARTEMIS_CONSUMERS:-java node dotnet}"
QUIET_S="${QUIET_S:-8}"
SETTLE_S="${SETTLE_S:-6}"
CASE_TIMEOUT_S="${CASE_TIMEOUT_S:-90}"
export PULSAR_URL="${PULSAR_URL:-pulsar://localhost:6650}"
PULSAR_COMPOSE_FILE="${PULSAR_COMPOSE_FILE:-$PULSAR_DIR/docker-compose.yml}"
PULSAR_ADMIN_URL="${PULSAR_ADMIN_URL:-http://localhost:8080}"
CASE_NO=0

need curl
need jq

for broker in $GATE_BROKERS; do
  case "$broker" in
    pulsar)
      if [ -z "${PULSAR_NO_COMPOSE:-}" ]; then
        need docker
        log "starting pulsar standalone"
        compose_up "$PULSAR_COMPOSE_FILE" 300
      fi
      build_probes pulsar_go
      for lang in $PULSAR_CONSUMERS; do
        case "$lang" in
          java) build_probes pulsar_java ;;
          node) build_probes pulsar_node ;;
          *) fail "unknown Pulsar consumer language in PULSAR_CONSUMERS: $lang (java | node)" ;;
        esac
      done
      ;;
    artemis)
      artemis_up
      build_probes artemis_go
      for lang in $ARTEMIS_CONSUMERS; do
        case "$lang" in
          java) build_probes artemis_java ;;
          node) build_probes artemis_node ;;
          dotnet) build_probes artemis_dotnet ;;
          *) fail "unknown Artemis consumer language in ARTEMIS_CONSUMERS: $lang (java | node | dotnet)" ;;
        esac
      done
      ;;
    *) fail "unknown broker in GATE_BROKERS: $broker (pulsar | artemis)" ;;
  esac
done

# assert_raw_record <label> <watch-log> <published-id> <published-sha256>: the DLQ record is the raw version-gate record.
assert_raw_record() {
  local label="$1" f="$2" id="$3" sha="$4"
  assert_eq "$(field "$f" go DLQ_DONE count)" "1" "$label: records on the DLQ (one, nothing retried or duplicated)"
  assert_eq "$(field "$f" go DLQ shape)" "raw" "$label: DLQ record shape (a canonical envelope without a dead_letter block)"
  assert_eq "$(field "$f" go DLQ has_dead_letter)" "0" "$label: dead_letter block"
  assert_eq "$(field "$f" go DLQ body_sha256)" "$sha" "$label: DLQ body SHA-256 equals the published body's (byte for byte)"
  assert_eq "$(field "$f" go DLQ id)" "$id" "$label: DLQ meta.id"
}

# ---- Pulsar ----------------------------------------------------------------------------------

# pulsar_case <java|node> <dlq|nodlq>
pulsar_case() {
  local lang="$1" mode="$2"
  local label="pulsar/$lang/$mode"
  CASE_NO=$((CASE_NO + 1))
  local queue="bq-gate-p$CASE_NO-$RUN_ID"
  local dir="$WORK/case-$CASE_NO"
  mkdir -p "$dir"
  local probe
  case "$lang" in java) probe=pjava ;; node) probe=pnode ;; esac
  log "case $CASE_NO: $label (queue $queue)"

  local watch_pid=""
  if [ "$mode" = dlq ]; then
    # The DLQ reader first: its subscription must exist before anything is written to <queue>.dlq.
    start_probe pgo "$dir/watch.log" "QUEUE=$queue" EXPECT=1 "IDLE=$QUIET_S" "TIMEOUT=$CASE_TIMEOUT_S" -- watch-dlq
    watch_pid="$PROBE_PID"
    wait_line "$dir/watch.log" "^PROBE go READY" 60 || { dump_logs "$dir/watch.log"; fail "$label: DLQ reader never subscribed"; }
  fi

  local no_dlq=0
  [ "$mode" = nodlq ] && no_dlq=1
  start_probe "$probe" "$dir/consume.log" "QUEUE=$queue" "NO_DLQ=$no_dlq" RUN_S=300 --
  local consumer_pid="$PROBE_PID"
  wait_line "$dir/consume.log" "^PROBE $lang READY" 120 || { dump_logs "$dir/consume.log"; fail "$label: $lang consumer never subscribed"; }

  run_probe pgo "$dir/produce.log" "QUEUE=$queue" VERSION=2 -- gate || { dump_logs "$dir/produce.log"; fail "$label: go gate failed"; }
  local id sha
  id="$(field "$dir/produce.log" go GATED id)"
  sha="$(field "$dir/produce.log" go GATED sha256)"
  [ -n "$id" ] && [ -n "$sha" ] || { dump_logs "$dir/produce.log"; fail "$label: no published id / sha256"; }

  if [ "$mode" = dlq ]; then
    wait_exit "$watch_pid" "$CASE_TIMEOUT_S"
    if [ "$WAIT_RC" != "0" ]; then
      dump_logs "$dir/watch.log" "$dir/consume.log"
      fail "$label: DLQ reader exit=$WAIT_RC (no record on $queue.dlq?)"
    fi
  else
    sleep "$SETTLE_S"
  fi

  # Settled on the first delivery: after the window the work topic's subscription has no backlog (which includes
  # unacknowledged messages), so the original was acknowledged, not left for a redelivery.
  local stats backlog
  stats="$(curl -fsS "$PULSAR_ADMIN_URL/admin/v2/persistent/public/default/$queue/stats")" \
    || { dump_logs "$dir/consume.log"; fail "$label: could not read topic stats of $queue"; }
  backlog="$(printf '%s' "$stats" | jq -r '.subscriptions.babelqueue.msgBacklog')"
  stop_probe "$consumer_pid"
  assert_eq "$backlog" "0" "$label: backlog of the work topic"
  assert_eq "$(event_count "$dir/consume.log" "$lang" HANDLED)" "0" "$label: handler never runs for a version-gated message"

  if [ "$mode" = dlq ]; then
    assert_raw_record "$label" "$dir/watch.log" "$id" "$sha"
  else
    # Dead-lettering off: acknowledged and dropped. The consumer never created <queue>.dlq.
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' "$PULSAR_ADMIN_URL/admin/v2/persistent/public/default/$queue.dlq/stats")"
    assert_eq "$code" "404" "$label: nothing was written to $queue.dlq (the topic does not exist)"
  fi
  log "case $CASE_NO: $label OK"
}

# ---- Artemis ---------------------------------------------------------------------------------

# assert_artemis_native_dlq_empty <label> <dir>: nothing reached the broker's own dead-letter address (`DLQ` in the
# stock broker.xml, max-delivery-attempts 10). A consumer that kept recovering or releasing a message instead of settling
# it would push it there within moments and leave the work queue and <queue>.dlq empty, which would look like success.
assert_artemis_native_dlq_empty() {
  run_probe ago "$2/native-dlq.log" "QUEUE=x" "WATCH_QUEUE=${ARTEMIS_NATIVE_DLQ:-DLQ}" EXPECT=0 IDLE=3 TIMEOUT=30 -- watch-dlq \
    || { dump_logs "$2/native-dlq.log"; fail "$1: could not read the broker's native dead-letter address"; }
  assert_eq "$(field "$2/native-dlq.log" go DLQ_DONE count)" "0" "$1: messages on the broker's native DLQ address (settled on the first delivery, never retried)"
}

# artemis_case <java|node|dotnet> <on|off> <none|delete>   (dead-lettering, on_unknown_urn)
artemis_case() {
  local lang="$1" dlq="$2" strategy="$3"
  local label="artemis/$lang/dlq-$dlq/unknown-urn-$strategy"
  CASE_NO=$((CASE_NO + 1))
  local queue="bq-gate-a$CASE_NO-$RUN_ID"
  local dir="$WORK/case-$CASE_NO"
  mkdir -p "$dir"
  local probe="a$lang"
  local strategy_value="$strategy"
  [ "$strategy" = none ] && strategy_value=""
  local expect_record=0
  [ "$dlq" = on ] && [ "$strategy" = none ] && expect_record=1
  artemis_queue "$queue" "$queue.dlq"
  log "case $CASE_NO: $label (queue $queue)"

  local watch_pid=""
  if [ "$expect_record" = 1 ]; then
    start_probe ago "$dir/watch.log" "QUEUE=$queue" EXPECT=1 "IDLE=$QUIET_S" "TIMEOUT=$CASE_TIMEOUT_S" -- watch-dlq
    watch_pid="$PROBE_PID"
    wait_line "$dir/watch.log" "^PROBE go READY" 60 || { dump_logs "$dir/watch.log"; fail "$label: DLQ reader never attached"; }
  fi

  start_probe "$probe" "$dir/consume.log" "QUEUE=$queue" "DLQ=$dlq" "UNKNOWN_URN=$strategy_value" RUN_S=300 --
  local consumer_pid="$PROBE_PID"
  wait_line "$dir/consume.log" "^PROBE $lang READY" 120 || { dump_logs "$dir/consume.log"; fail "$label: $lang consumer never started"; }

  run_probe ago "$dir/produce.log" "QUEUE=$queue" VERSION=2 -- gate || { dump_logs "$dir/produce.log"; fail "$label: go gate failed"; }
  local id sha
  id="$(field "$dir/produce.log" go GATED id)"
  sha="$(field "$dir/produce.log" go GATED sha256)"
  [ -n "$id" ] && [ -n "$sha" ] || { dump_logs "$dir/produce.log"; fail "$label: no published id / sha256"; }

  if [ "$expect_record" = 1 ]; then
    wait_exit "$watch_pid" "$CASE_TIMEOUT_S"
    if [ "$WAIT_RC" != "0" ]; then
      dump_logs "$dir/watch.log" "$dir/consume.log"
      fail "$label: DLQ reader exit=$WAIT_RC (no record on $queue.dlq?)"
    fi
  else
    sleep "$SETTLE_S"
  fi
  stop_probe "$consumer_pid"
  assert_eq "$(event_count "$dir/consume.log" "$lang" HANDLED)" "0" "$label: handler never runs for a version-gated message"

  if [ "$expect_record" = 1 ]; then
    assert_raw_record "$label" "$dir/watch.log" "$id" "$sha"
  else
    # No record: acknowledged and dropped (dead-lettering off) or acknowledged without a record (delete).
    run_probe ago "$dir/dlq-empty.log" "QUEUE=$queue" EXPECT=0 IDLE=3 TIMEOUT=30 -- watch-dlq \
      || { dump_logs "$dir/dlq-empty.log"; fail "$label: could not read $queue.dlq"; }
    assert_eq "$(field "$dir/dlq-empty.log" go DLQ_DONE count)" "0" "$label: records on $queue.dlq"
  fi
  # Acknowledged, not left for a redelivery: the work queue is empty once the consumer is gone.
  run_probe ago "$dir/work-empty.log" "QUEUE=$queue" "WATCH_QUEUE=$queue" EXPECT=0 IDLE=3 TIMEOUT=30 -- watch-dlq \
    || { dump_logs "$dir/work-empty.log" "$dir/consume.log"; fail "$label: could not read the work queue $queue"; }
  assert_eq "$(field "$dir/work-empty.log" go DLQ_DONE count)" "0" "$label: messages left on the work queue"
  assert_artemis_native_dlq_empty "$label" "$dir"
  log "case $CASE_NO: $label OK"
}

for broker in $GATE_BROKERS; do
  case "$broker" in
    pulsar)
      for lang in $PULSAR_CONSUMERS; do
        pulsar_case "$lang" dlq
        pulsar_case "$lang" nodlq
      done
      ;;
    artemis)
      for lang in $ARTEMIS_CONSUMERS; do
        artemis_case "$lang" on none
        artemis_case "$lang" off none
        artemis_case "$lang" on delete
      done
      ;;
  esac
done

log "version-gate: OK ($CASE_NO cases)"
