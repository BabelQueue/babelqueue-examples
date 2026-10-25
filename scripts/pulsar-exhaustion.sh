#!/usr/bin/env bash
# Pulsar exhaustion across languages (post-R1 parity, ADR-0036 decisions 1, 2 and 4).
#
# A Go producer publishes to Apache Pulsar; a Java consumer (babelqueue-pulsar) and a Node consumer
# (@babelqueue/pulsar) read it with a handler that ALWAYS throws, dead-lettering to <queue>.dlq. A Go
# reader on <queue>.dlq then asserts, from a third SDK, what the consumer wrote. Per consumer language:
#
#   failed    a valid envelope; the handler throws on every delivery. After the quiet window the work topic's
#             subscription has no backlog, so the original was acknowledged, not just left alone.
#             - the handler runs exactly 3 times (the default max attempts) and sees attempts 0, 1, 2;
#             - with RETRY=republish (the default) the retry is a re-publish with bq-attempts + 1, so the native
#               redelivery count stays 0 on every delivery, on an Exclusive subscription, where a negative
#               acknowledgement would never advance it (the loop ADR-0036 closes);
#             - exactly ONE record lands on <queue>.dlq: the same meta.id and trace_id, dead_letter.reason
#               "failed", dead_letter.attempts 3, top-level attempts 3, bq-attempts "3", bq-job kept;
#             - nothing else arrives afterwards (the quiet window of the DLQ reader, IDLE seconds).
#   failed    again with RETRY=nack on a Shared subscription: no retry producer, the broker's own redelivery
#             count advances (native redelivery 0, 1, 2) and the outcome is the same record.
#   noidentity  a JSON object with neither job nor urn: poison (decision 4). The handler never runs; the DLQ
#             record is the ADR-0035 splice: the original members plus dead_letter.reason "poison".
#   binary    a body that is not valid UTF-8: poison. The DLQ record is the ADR-0035 wrapper, raw_encoding
#             "base64", and the decoded raw equals the published bytes.
#
# Usage: scripts/pulsar-exhaustion.sh                     (published SDKs; needs babelqueue-pulsar 1.2.0 and
#                                                          @babelqueue/pulsar 1.3.0 on the registries)
#        BQ_SDK_ROOT=/path/to/BabelQueue scripts/pulsar-exhaustion.sh   (in-repo SDK sources)
# Env:   PULSAR_URL (default pulsar://localhost:6650), PULSAR_NO_COMPOSE=1 to use an already running broker,
#        PULSAR_ADMIN_URL (default http://localhost:8080, for the topic stats),
#        PULSAR_CONSUMERS (default "java node"), QUIET_S (default 8: the DLQ reader's quiet window),
#        CASE_TIMEOUT_S (default 90). Needs curl and jq besides the probe toolchains.
set -euo pipefail
# shellcheck source=lib/interop-common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/interop-common.sh"

export PULSAR_URL="${PULSAR_URL:-pulsar://localhost:6650}"
PULSAR_COMPOSE_FILE="${PULSAR_COMPOSE_FILE:-$PULSAR_DIR/docker-compose.yml}"
PULSAR_CONSUMERS="${PULSAR_CONSUMERS:-java node}"
QUIET_S="${QUIET_S:-8}"
CASE_TIMEOUT_S="${CASE_TIMEOUT_S:-90}"
URN="urn:babel:orders:created"
BINARY_HEX="fffe62710080" # ff fe 'b' 'q' 00 80, see poisonBody in pulsar-orders/interop/go/main.go
PULSAR_ADMIN_URL="${PULSAR_ADMIN_URL:-http://localhost:8080}"
CASE_NO=0

need curl
need jq
if [ -z "${PULSAR_NO_COMPOSE:-}" ]; then
  need docker
  log "starting pulsar standalone"
  docker compose -f "$PULSAR_COMPOSE_FILE" up -d --wait --wait-timeout 300 >/dev/null
fi

build_probes pulsar_go
for lang in $PULSAR_CONSUMERS; do
  case "$lang" in
    java) build_probes pulsar_java ;;
    node) build_probes pulsar_node ;;
    *) fail "unknown consumer language in PULSAR_CONSUMERS: $lang (java | node)" ;;
  esac
done

dump_logs() {
  local f
  for f in "$@"; do
    [ -f "$f" ] && { printf '[interop] --- %s ---\n' "$f" >&2; cat "$f" >&2; }
  done
  return 0
}

# handled_seq <logfile> <lang> <field>: the values of one field on the HANDLED lines, comma separated.
handled_seq() {
  grep -E "^PROBE $2 HANDLED " "$1" | tr ' ' '\n' | sed -n "s/^$3=//p" | paste -sd, - || true
}

# run_case <consumer-lang> <failed|noidentity|binary> <Exclusive|Shared|...> <republish|nack>
run_case() {
  local lang="$1" kind="$2" sub_type="$3" retry="$4"
  local label="$lang/$kind/$sub_type/$retry"
  CASE_NO=$((CASE_NO + 1))
  local queue="bq-exh-$CASE_NO-$RUN_ID"
  local dir="$WORK/case-$CASE_NO"
  mkdir -p "$dir"
  local probe
  case "$lang" in java) probe=pjava ;; node) probe=pnode ;; esac
  log "case $CASE_NO: $label (queue $queue)"

  # 1. The DLQ reader first: its subscription must exist before anything is written to <queue>.dlq.
  local raw_hex=()
  [ "$kind" = binary ] && raw_hex=("EXPECT_RAW_HEX=$BINARY_HEX")
  start_probe pgo "$dir/watch.log" "QUEUE=$queue" EXPECT=1 "IDLE=$QUIET_S" "TIMEOUT=$CASE_TIMEOUT_S" \
    ${raw_hex[@]+"${raw_hex[@]}"} -- watch-dlq
  local watch_pid="$PROBE_PID"
  wait_line "$dir/watch.log" "^PROBE go READY" 60 || { dump_logs "$dir/watch.log"; fail "$label: DLQ reader never subscribed"; }

  # 2. The consumer; the work-queue subscription must exist before the Go producer publishes.
  start_probe "$probe" "$dir/consume.log" "QUEUE=$queue" "SUB_TYPE=$sub_type" "RETRY=$retry" RUN_S=300 --
  local consumer_pid="$PROBE_PID"
  wait_line "$dir/consume.log" "^PROBE $lang READY" 120 || { dump_logs "$dir/consume.log"; fail "$label: $lang consumer never subscribed"; }

  # 3. Go produces.
  local published_id="" trace=""
  if [ "$kind" = failed ]; then
    run_probe pgo "$dir/produce.log" "QUEUE=$queue" -- produce || { dump_logs "$dir/produce.log"; fail "$label: go produce failed"; }
    published_id="$(field "$dir/produce.log" go PUBLISHED id)"
    trace="$(field "$dir/produce.log" go PUBLISHED trace)"
    [ -n "$published_id" ] && [ -n "$trace" ] || { dump_logs "$dir/produce.log"; fail "$label: no published id / trace"; }
  else
    run_probe pgo "$dir/produce.log" "QUEUE=$queue" "KIND=$kind" -- poison || { dump_logs "$dir/produce.log"; fail "$label: go poison failed"; }
  fi

  # 4. The DLQ reader exits after the record plus its quiet window.
  wait_exit "$watch_pid" "$CASE_TIMEOUT_S"
  local rc="$WAIT_RC"
  if [ "$rc" != "0" ]; then
    dump_logs "$dir/watch.log" "$dir/consume.log"
    fail "$label: DLQ reader exit=$rc (no record on $queue.dlq?)"
  fi
  # The original was acknowledged, not just left alone: after the quiet window the work topic's subscription
  # has no backlog (which includes unacknowledged messages), and nothing was delivered a fourth time.
  local stats
  stats="$(curl -fsS "$PULSAR_ADMIN_URL/admin/v2/persistent/public/default/$queue/stats")" \
    || { dump_logs "$dir/consume.log"; fail "$label: could not read topic stats of $queue"; }
  assert_eq "$(printf '%s' "$stats" | jq -r '.subscriptions.babelqueue.msgBacklog')" "0" "$label: backlog of the work topic"
  kill "$consumer_pid" >/dev/null 2>&1 || true
  wait "$consumer_pid" 2>/dev/null || true

  # 5. Assertions.
  local f="$dir/watch.log" c="$dir/consume.log"
  assert_eq "$(field "$f" go DLQ_DONE count)" "1" "$label: records on $queue.dlq"
  assert_eq "$(field "$f" go DLQ reason)" "$([ "$kind" = failed ] && echo failed || echo poison)" "$label: dead_letter.reason"
  assert_eq "$(field "$f" go DLQ original_queue)" "$queue" "$label: dead_letter.original_queue"
  assert_eq "$(field "$f" go DLQ lang)" "$lang" "$label: dead_letter.lang"
  case "$kind" in
    failed)
      assert_eq "$(event_count "$c" "$lang" HANDLED)" "3" "$label: handler runs"
      assert_eq "$(handled_seq "$c" "$lang" attempts)" "0,1,2" "$label: attempts the handler saw"
      assert_eq "$(handled_seq "$c" "$lang" id | tr ',' '\n' | sort -u | paste -sd, -)" "$published_id" "$label: meta.id on every delivery"
      if [ "$retry" = republish ]; then
        # The count travels in the message; the broker's native count never moves, so a nack-only
        # retry on this subscription type could not have reached the cap.
        assert_eq "$(handled_seq "$c" "$lang" redelivery)" "0,0,0" "$label: native redelivery count"
      else
        assert_eq "$(handled_seq "$c" "$lang" redelivery)" "0,1,2" "$label: native redelivery count"
      fi
      assert_eq "$(field "$f" go DLQ id)" "$published_id" "$label: DLQ meta.id"
      assert_eq "$(field "$f" go DLQ trace)" "$trace" "$label: DLQ trace_id"
      assert_eq "$(field "$f" go DLQ shape)" "envelope" "$label: DLQ record shape"
      assert_eq "$(field "$f" go DLQ dl_attempts)" "3" "$label: dead_letter.attempts"
      assert_eq "$(field "$f" go DLQ attempts)" "3" "$label: DLQ body attempts"
      assert_eq "$(field "$f" go DLQ bq_attempts)" "3" "$label: DLQ bq-attempts property"
      assert_eq "$(field "$f" go DLQ bq_job)" "$URN" "$label: DLQ bq-job property"
      ;;
    noidentity)
      assert_eq "$(event_count "$c" "$lang" HANDLED)" "0" "$label: handler never runs for poison"
      assert_eq "$(field "$f" go DLQ shape)" "splice" "$label: DLQ record shape"
      assert_eq "$(field "$f" go DLQ member_hello)" "world" "$label: original member kept in the splice"
      ;;
    binary)
      assert_eq "$(event_count "$c" "$lang" HANDLED)" "0" "$label: handler never runs for poison"
      assert_eq "$(field "$f" go DLQ shape)" "wrapper" "$label: DLQ record shape"
      assert_eq "$(field "$f" go DLQ raw_encoding)" "base64" "$label: DLQ raw_encoding"
      assert_eq "$(field "$f" go DLQ raw_ok)" "1" "$label: decoded raw equals the published bytes"
      ;;
  esac
  log "case $CASE_NO: $label OK"
}

for lang in $PULSAR_CONSUMERS; do
  run_case "$lang" failed Exclusive republish
  run_case "$lang" failed Shared nack
  run_case "$lang" noidentity Exclusive republish
  run_case "$lang" binary Exclusive republish
done

log "pulsar-exhaustion: OK"
