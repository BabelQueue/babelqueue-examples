#!/usr/bin/env bash
# Redis kill -9 recovery across languages (R1 Faz 10, scope 3). The two layouts run separately and
# log separate lines.
#
#   zset   (Laravel layout): consumer A reserves, is kill -9'ed; after retry_after the :reserved
#          entry is migrated back and consumer B (another language) receives the message with
#          attempts + 1 (recovery counts as an attempt).
#   legacy (<q>:processing): same, but the message returns through the :processing:deadlines
#          sweep and attempts is UNCHANGED (A11). <q>:processing (LLEN) and
#          <q>:processing:deadlines (ZCARD) must both be 0 at the end.
#
# retry_after is deliberately short (RETRY_AFTER_S, default 5 s).
#
# Usage: scripts/redis-recovery.sh   [BQ_SDK_ROOT=/path/to/BabelQueue for in-repo SDK sources]
#        RECOVERY_PAIRS="go:node node:go" scripts/redis-recovery.sh   (subset)
set -euo pipefail
# shellcheck source=lib/interop-common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/interop-common.sh"

RETRY_AFTER_S="${RETRY_AFTER_S:-5}"
# A:B pairs (A is killed while holding the reservation, B recovers it).
PAIRS="${RECOVERY_PAIRS:-go:node node:java java:dotnet dotnet:python python:go}"

langs=""
for pair in $PAIRS; do langs="$langs ${pair%%:*} ${pair##*:}"; done
# shellcheck disable=SC2086
unique_langs="$(printf '%s\n' $langs go | sort -u | tr '\n' ' ')"

redis_up
# shellcheck disable=SC2086
build_probes $unique_langs

run_pair() {
  local layout="$1" a="$2" b="$3"
  local queue="bq-recovery-$layout-$a-$b-$RUN_ID"
  local alog="$WORK/recovery-$layout-$a.log" blog="$WORK/recovery-$layout-$b.log"
  local plog="$WORK/recovery-$layout-produce-$a-$b.log"
  local id

  id="$(publish_one "$queue" "$layout" "$plog")"

  start_probe "$a" "$alog" "QUEUE=$queue" "LAYOUT=$layout" "RETRY_AFTER=$RETRY_AFTER_S" \
    MODE=hang TIMEOUT=60 -- consume
  local a_pid="$PROBE_PID"
  wait_line "$alog" "^PROBE $a RECEIVED id=$id " 60 \
    || fail "[$layout] $a never reserved the message: $(cat "$alog")"
  local a_attempts
  a_attempts="$(field "$alog" "$a" RECEIVED attempts)"

  # A must still be alive at this point; a probe that already exited (crash, runtime panic) would
  # make the kill -9 meaningless and the scenario a false positive.
  kill -0 "$a_pid" 2>/dev/null || fail "[$layout] $a exited before kill -9: $(cat "$alog")"
  kill -9 "$a_pid" || fail "[$layout] kill -9 $a_pid failed"
  wait "$a_pid" 2>/dev/null || true

  # The message must still be held (reserved / processing) right after the kill.
  if [ "$layout" = "legacy" ]; then
    assert_eq "$(redis_int LLEN "$queue:processing")" 1 "[$layout] $queue:processing right after kill -9"
  else
    assert_eq "$(redis_int ZCARD "queues:$queue:reserved")" 1 "[$layout] queues:$queue:reserved right after kill -9"
  fi

  start_probe "$b" "$blog" "QUEUE=$queue" "LAYOUT=$layout" "RETRY_AFTER=$RETRY_AFTER_S" \
    MODE=consume TIMEOUT=$((RETRY_AFTER_S + 40)) -- consume
  local rc
  wait_exit "$PROBE_PID" $((RETRY_AFTER_S + 60))
  rc="$WAIT_RC"
  [ "$rc" = "0" ] || fail "[$layout] $b consumer exit=$rc: $(cat "$blog")"
  # The reservation was taken just before A printed RECEIVED, so B cannot legitimately get the
  # message earlier than RETRY_AFTER after that line (1s slack for clock/print skew). A broker
  # path that re-delivers immediately (ignoring the visibility timeout) fails here.
  local a_received_at b_received_at since_reserve
  a_received_at="$(field "$alog" "$a" RECEIVED at_ms)"
  b_received_at="$(field "$blog" "$b" RECEIVED at_ms)"
  [ -n "$a_received_at" ] && [ -n "$b_received_at" ] \
    || fail "[$layout] missing at_ms on RECEIVED lines (a='$a_received_at' b='$b_received_at')"
  since_reserve=$((b_received_at - a_received_at))
  [ "$since_reserve" -ge $((RETRY_AFTER_S * 1000 - 1000)) ] \
    || fail "[$layout] $a->$b recovered after only ${since_reserve}ms, before retry_after=${RETRY_AFTER_S}s elapsed"
  assert_eq "$(field "$blog" "$b" RECEIVED id)" "$id" "[$layout] $a->$b recovered message id"
  local b_attempts
  b_attempts="$(field "$blog" "$b" RECEIVED attempts)"

  if [ "$layout" = "legacy" ]; then
    assert_eq "$b_attempts" "$a_attempts" "[$layout] $a->$b attempts (A11: recovery must not count)"
    assert_eq "$(redis_int LLEN "$queue:processing")" 0 "[$layout] $queue:processing LLEN at end"
    assert_eq "$(redis_int ZCARD "$queue:processing:deadlines")" 0 "[$layout] $queue:processing:deadlines ZCARD at end"
    assert_eq "$(redis_int LLEN "$queue")" 0 "[$layout] $queue LLEN at end"
    log "RECOVERY legacy retry_after=${RETRY_AFTER_S}s kill9 $a->$b recovered ${since_reserve}ms after reserve (>= retry_after) attempts=$a_attempts->$b_attempts (unchanged) processing=0 deadlines=0"
  else
    assert_eq "$b_attempts" "$((a_attempts + 1))" "[$layout] $a->$b attempts (recovery counts as an attempt)"
    assert_eq "$(redis_int ZCARD "queues:$queue:reserved")" 0 "[$layout] queues:$queue:reserved ZCARD at end"
    assert_eq "$(redis_int LLEN "queues:$queue")" 0 "[$layout] queues:$queue LLEN at end"
    log "RECOVERY zset retry_after=${RETRY_AFTER_S}s kill9 $a->$b recovered ${since_reserve}ms after reserve (>= retry_after) attempts=$a_attempts->$b_attempts (+1) reserved=0"
  fi
}

for layout in zset legacy; do
  for pair in $PAIRS; do
    run_pair "$layout" "${pair%%:*}" "${pair##*:}"
  done
done

log "redis-recovery: OK"
