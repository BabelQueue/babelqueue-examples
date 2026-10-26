#!/usr/bin/env bash
# Poison bodies on Artemis and SQS across languages (ADR-0037 decision 2, broker-bindings §3.5/§7.5, ADR-0035).
#
# A Go producer publishes a body that is not an envelope; a consumer in another language (a handler that records
# that it ran) must settle it on its FIRST delivery, never hand it to the handler and never retry it. A Go reader
# on <queue>.dlq asserts, from a third SDK, what the consumer wrote. (On SQS the DLQ is <queue>-dlq, the
# contract's SQS name (ADR-0037 decision 7): SQS and ElasticMQ reject a dot in a standard queue name.)
# Per consumer language:
#
#   dlq on     the ADR-0035 record: a JSON object body is spliced (the original members plus dead_letter.reason
#              "poison", original_queue, lang); anything else is wrapped under raw (base64 plus raw_encoding when it
#              is not valid UTF-8, Artemis only: an SQS body is text). Exactly one record; the original is gone.
#              On SQS the queue has a native RedrivePolicy and the native dead-letter queue stays EMPTY: with a
#              BabelQueue DLQ configured, leaving the body to the RedrivePolicy is not a conformant settle.
#   dlq off    Artemis: acknowledged and dropped (nothing on <queue>.dlq, the work queue is empty).
#              SQS: handed to the broker's own mechanism, not deleted by the SDK: the visibility timeout redelivers
#              it and the RedrivePolicy moves it, UNCHANGED (the reader compares the SHA-256 of the body it finds on
#              the native queue with the one the producer sent), to the native dead-letter queue; the BabelQueue DLQ stays empty.
#   delete     on_unknown_urn "delete" with a DLQ configured: acknowledged / deleted without a record.
#
# Brokers (POISON_BROKERS, default "artemis sqs") and consumers:
#   artemis  Go producer (AMQP 1.0) / DLQ reader; Java (JMS), Node (rhea) and .NET (AMQPNetLite) consumers
#   sqs      Go producer / DLQ reader (ElasticMQ); Java, Node and .NET consumers
#
# Usage: BQ_SDK_ROOT=/path/to/BabelQueue scripts/poison-dlq.sh
#        (without BQ_SDK_ROOT it needs BQ_ALLOW_PUBLISHED=1 and then runs against the ADR-0037 releases pinned in the
#        probe manifests.)
# Env:   POISON_BROKERS, ARTEMIS_CONSUMERS / SQS_CONSUMERS (default "java node dotnet"), ARTEMIS_URL, ARTEMIS_JMS_URL,
#        ARTEMIS_NO_COMPOSE=1, SQS_ENDPOINT, SQS_NO_COMPOSE=1, QUIET_S (default 8: the DLQ reader's quiet window),
#        SETTLE_S (default 6), CASE_TIMEOUT_S (default 90), NATIVE_TIMEOUT_S (default 120: the wait for the native
#        redrive). Needs curl and jq besides the probe toolchains.
set -euo pipefail
# shellcheck source=lib/interop-common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/interop-common.sh"

need_adr0037_sdks poison-dlq.sh

POISON_BROKERS="${POISON_BROKERS:-artemis sqs}"
ARTEMIS_CONSUMERS="${ARTEMIS_CONSUMERS:-java node dotnet}"
SQS_CONSUMERS="${SQS_CONSUMERS:-java node dotnet}"
QUIET_S="${QUIET_S:-8}"
SETTLE_S="${SETTLE_S:-6}"
CASE_TIMEOUT_S="${CASE_TIMEOUT_S:-90}"
NATIVE_TIMEOUT_S="${NATIVE_TIMEOUT_S:-120}"
BINARY_HEX="fffe62710080" # ff fe 'b' 'q' 00 80, see poisonBody in artemis-orders/interop/go/main.go
NOTJSON_HEX="$(printf 'this is not json' | od -An -tx1 | tr -d ' \n')" # the text KIND=notjson publishes
CASE_NO=0

need curl
need jq

for broker in $POISON_BROKERS; do
  case "$broker" in
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
    sqs)
      sqs_up
      build_probes sqs_go
      for lang in $SQS_CONSUMERS; do
        case "$lang" in
          java) build_probes sqs_java ;;
          node) build_probes sqs_node ;;
          dotnet) build_probes sqs_dotnet ;;
          *) fail "unknown SQS consumer language in SQS_CONSUMERS: $lang (java | node | dotnet)" ;;
        esac
      done
      ;;
    *) fail "unknown broker in POISON_BROKERS: $broker (artemis | sqs)" ;;
  esac
done

# assert_poison_record <label> <log> <lang> <queue> <kind>: the DLQ record is the ADR-0035 record of that body.
assert_poison_record() {
  local label="$1" f="$2" lang="$3" queue="$4" kind="$5"
  assert_eq "$(field "$f" go DLQ_DONE count)" "1" "$label: records on the DLQ (one, nothing retried or duplicated)"
  assert_eq "$(field "$f" go DLQ reason)" "poison" "$label: dead_letter.reason"
  assert_eq "$(field "$f" go DLQ original_queue)" "$queue" "$label: dead_letter.original_queue"
  assert_eq "$(field "$f" go DLQ lang)" "$lang" "$label: dead_letter.lang"
  assert_eq "$(field "$f" go DLQ has_dead_letter)" "1" "$label: dead_letter block"
  case "$kind" in
    noidentity)
      assert_eq "$(field "$f" go DLQ shape)" "splice" "$label: DLQ record shape (the object body spliced)"
      assert_eq "$(field "$f" go DLQ member_hello)" "world" "$label: original member kept in the splice"
      ;;
    binary)
      assert_eq "$(field "$f" go DLQ shape)" "wrapper" "$label: DLQ record shape (the ADR-0035 wrapper)"
      assert_eq "$(field "$f" go DLQ raw_encoding)" "base64" "$label: DLQ raw_encoding"
      assert_eq "$(field "$f" go DLQ raw_ok)" "1" "$label: decoded raw equals the published bytes"
      ;;
    notjson)
      assert_eq "$(field "$f" go DLQ shape)" "wrapper" "$label: DLQ record shape (the ADR-0035 wrapper)"
      assert_eq "$(field "$f" go DLQ raw_ok)" "1" "$label: raw equals the published text"
      ;;
  esac
}

expected_hex() {
  case "$1" in
    binary) printf '%s' "$BINARY_HEX" ;;
    notjson) printf '%s' "$NOTJSON_HEX" ;;
    *) printf '' ;;
  esac
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

# artemis_case <java|node|dotnet> <on|off> <none|delete> <noidentity|binary>
artemis_case() {
  local lang="$1" dlq="$2" strategy="$3" kind="$4"
  local label="artemis/$lang/dlq-$dlq/unknown-urn-$strategy/$kind"
  CASE_NO=$((CASE_NO + 1))
  local queue="bq-poison-a$CASE_NO-$RUN_ID"
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
    start_probe ago "$dir/watch.log" "QUEUE=$queue" EXPECT=1 "IDLE=$QUIET_S" "TIMEOUT=$CASE_TIMEOUT_S" \
      "EXPECT_RAW_HEX=$(expected_hex "$kind")" -- watch-dlq
    watch_pid="$PROBE_PID"
    wait_line "$dir/watch.log" "^PROBE go READY" 60 || { dump_logs "$dir/watch.log"; fail "$label: DLQ reader never attached"; }
  fi

  start_probe "$probe" "$dir/consume.log" "QUEUE=$queue" "DLQ=$dlq" "UNKNOWN_URN=$strategy_value" RUN_S=300 --
  local consumer_pid="$PROBE_PID"
  wait_line "$dir/consume.log" "^PROBE $lang READY" 120 || { dump_logs "$dir/consume.log"; fail "$label: $lang consumer never started"; }

  run_probe ago "$dir/produce.log" "QUEUE=$queue" "KIND=$kind" -- poison || { dump_logs "$dir/produce.log"; fail "$label: go poison failed"; }

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
  assert_eq "$(event_count "$dir/consume.log" "$lang" HANDLED)" "0" "$label: handler never runs for poison"

  if [ "$expect_record" = 1 ]; then
    assert_poison_record "$label" "$dir/watch.log" "$lang" "$queue" "$kind"
  else
    # Dropped (dead-lettering off, Artemis) or acknowledged without a record (delete): nothing on <queue>.dlq.
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

# ---- SQS -------------------------------------------------------------------------------------

# sqs_depth <log> <queue-url>: prints "visible in_flight" of a queue.
sqs_depth() {
  run_probe sgo "$1" "QUEUE_URL=$2" -- depth || { dump_logs "$1"; fail "could not read the depth of $2"; }
  printf '%s %s' "$(field "$1" go DEPTH visible)" "$(field "$1" go DEPTH in_flight)"
}

# assert_sqs_native_empty <label> <dir> <native-url>: the queue's RedrivePolicy did NOT take the message. Without it a
# consumer that left a deleted-or-dead-lettered message undeleted would pass the "work queue is empty" check once the
# visibility timeout and maxReceiveCount had moved it to <queue>-native.
assert_sqs_native_empty() {
  run_probe sgo "$2/native-empty.log" "WATCH_URL=$3" EXPECT=0 IDLE=3 TIMEOUT=30 -- watch-dlq \
    || { dump_logs "$2/native-empty.log"; fail "$1: could not read the native queue"; }
  assert_eq "$(field "$2/native-empty.log" go DLQ_DONE count)" "0" "$1: records on the native dead-letter queue"
}

# sqs_case <java|node|dotnet> <on|off> <none|delete> <noidentity|notjson>
sqs_case() {
  local lang="$1" dlq="$2" strategy="$3" kind="$4"
  local label="sqs/$lang/dlq-$dlq/unknown-urn-$strategy/$kind"
  CASE_NO=$((CASE_NO + 1))
  local queue="bq-poison-s$CASE_NO-$RUN_ID"
  local dir="$WORK/case-$CASE_NO"
  mkdir -p "$dir"
  local probe="s$lang"
  local strategy_value="$strategy"
  [ "$strategy" = none ] && strategy_value=""
  log "case $CASE_NO: $label (queue $queue)"

  # The work queue has a short visibility timeout and a native RedrivePolicy towards <queue>-native.
  run_probe sgo "$dir/setup.log" "QUEUE=$queue" -- setup || { dump_logs "$dir/setup.log"; fail "$label: queue setup failed"; }
  local queue_url dlq_url native_url
  queue_url="$(field "$dir/setup.log" go SETUP queue_url)"
  dlq_url="$(field "$dir/setup.log" go SETUP dlq_url)"
  native_url="$(field "$dir/setup.log" go SETUP native_url)"
  [ -n "$queue_url" ] && [ -n "$dlq_url" ] && [ -n "$native_url" ] || { dump_logs "$dir/setup.log"; fail "$label: no queue URLs"; }

  start_probe "$probe" "$dir/consume.log" "QUEUE_URL=$queue_url" "DLQ_URL=$dlq_url" "DLQ=$dlq" "UNKNOWN_URN=$strategy_value" RUN_S=300 --
  local consumer_pid="$PROBE_PID"
  wait_line "$dir/consume.log" "^PROBE $lang READY" 120 || { dump_logs "$dir/consume.log"; fail "$label: $lang consumer never started"; }

  run_probe sgo "$dir/produce.log" "QUEUE_URL=$queue_url" "KIND=$kind" -- poison || { dump_logs "$dir/produce.log"; fail "$label: go poison failed"; }
  local sha
  sha="$(field "$dir/produce.log" go POISONED sha256)"
  [ -n "$sha" ] || { dump_logs "$dir/produce.log"; fail "$label: no published sha256"; }

  if [ "$dlq" = on ] && [ "$strategy" = none ]; then
    # The BabelQueue record on <queue>-dlq, then the original is deleted.
    run_probe sgo "$dir/watch.log" "DLQ_URL=$dlq_url" EXPECT=1 "IDLE=$QUIET_S" "TIMEOUT=$CASE_TIMEOUT_S" \
      "EXPECT_RAW_HEX=$(expected_hex "$kind")" -- watch-dlq \
      || { dump_logs "$dir/watch.log" "$dir/consume.log"; fail "$label: no record on $queue-dlq"; }
    stop_probe "$consumer_pid"
    assert_poison_record "$label" "$dir/watch.log" "$lang" "$queue" "$kind"
    assert_eq "$(sqs_depth "$dir/depth.log" "$queue_url")" "0 0" "$label: work queue (visible in_flight): the original was deleted"
    # A BabelQueue DLQ is configured, so the native mechanism must NOT have taken it.
    assert_sqs_native_empty "$label" "$dir" "$native_url"
  elif [ "$dlq" = on ]; then
    # delete: deleted without a record.
    sleep "$SETTLE_S"
    stop_probe "$consumer_pid"
    assert_eq "$(sqs_depth "$dir/depth.log" "$queue_url")" "0 0" "$label: work queue (visible in_flight): the original was deleted"
    run_probe sgo "$dir/dlq-empty.log" "DLQ_URL=$dlq_url" EXPECT=0 IDLE=3 TIMEOUT=30 -- watch-dlq \
      || { dump_logs "$dir/dlq-empty.log"; fail "$label: could not read $queue-dlq"; }
    assert_eq "$(field "$dir/dlq-empty.log" go DLQ_DONE count)" "0" "$label: records on $queue-dlq"
    # Deleted, not left to the RedrivePolicy: the native queue is empty too.
    assert_sqs_native_empty "$label" "$dir" "$native_url"
  else
    # Dead-lettering off: the SDK leaves it to SQS. It is redelivered and the RedrivePolicy moves it, byte for byte,
    # to the native dead-letter queue. No BabelQueue record is written.
    run_probe sgo "$dir/native.log" "WATCH_URL=$native_url" EXPECT=1 "IDLE=3" "TIMEOUT=$NATIVE_TIMEOUT_S" \
      "EXPECT_SHA256=$sha" -- watch-dlq \
      || { dump_logs "$dir/native.log" "$dir/consume.log"; fail "$label: the RedrivePolicy never moved the body to the native queue"; }
    stop_probe "$consumer_pid"
    assert_eq "$(field "$dir/native.log" go DLQ_DONE count)" "1" "$label: records on the native dead-letter queue"
    assert_eq "$(field "$dir/native.log" go DLQ body_ok)" "1" "$label: the native copy equals the published body byte for byte"
    assert_eq "$(field "$dir/native.log" go DLQ has_dead_letter)" "0" "$label: the native copy has no dead_letter block"
    run_probe sgo "$dir/dlq-empty.log" "DLQ_URL=$dlq_url" EXPECT=0 IDLE=3 TIMEOUT=30 -- watch-dlq \
      || { dump_logs "$dir/dlq-empty.log"; fail "$label: could not read $queue-dlq"; }
    assert_eq "$(field "$dir/dlq-empty.log" go DLQ_DONE count)" "0" "$label: records on $queue-dlq"
  fi
  assert_eq "$(event_count "$dir/consume.log" "$lang" HANDLED)" "0" "$label: handler never runs for poison"
  log "case $CASE_NO: $label OK"
}

for broker in $POISON_BROKERS; do
  case "$broker" in
    artemis)
      for lang in $ARTEMIS_CONSUMERS; do
        artemis_case "$lang" on none noidentity
        # A JMS TextMessage cannot carry bytes that are not valid UTF-8 (the Java consumer reads a BytesMessage body
        # as empty text), so the binary body is asserted for the AMQP 1.0 consumers only.
        if [ "$lang" = java ]; then
          log "artemis/java/binary: skipped (a JMS text message cannot carry invalid UTF-8; the AMQP 1.0 consumers cover it)"
        else
          artemis_case "$lang" on none binary
        fi
        artemis_case "$lang" off none noidentity
        artemis_case "$lang" on delete noidentity
      done
      ;;
    sqs)
      for lang in $SQS_CONSUMERS; do
        sqs_case "$lang" on none noidentity
        sqs_case "$lang" on none notjson
        sqs_case "$lang" off none noidentity
        sqs_case "$lang" on delete noidentity
      done
      ;;
  esac
done

log "poison-dlq: OK ($CASE_NO cases)"
