#!/usr/bin/env bash
# Redis legacy late-ack (R1 Faz 10, scope 4; A13 = K1).
#
# Consumer A (Go) holds the message past retry_after. The :processing:deadlines sweep returns it
# to the queue and consumer B (Node) reserves it and keeps holding. A then acks LATE. Expected:
#   - A's ack touches nothing and logs a "reservation expired" warning,
#   - B's reservation is intact while A's late ack happens (:processing / :deadlines still 1),
#   - B acks: :processing (LLEN) and :processing:deadlines (ZCARD) end at 0,
#   - the message is processed exactly once by B and is not lost.
#
# Timeline (retry_after 3 s): t0 A reserves -> t3 sweep + B reserves (B's own retry_after is long)
# -> t6 A acks late -> t8 B acks.
#
# Usage: scripts/redis-late-ack.sh   [BQ_SDK_ROOT=/path/to/BabelQueue for in-repo SDK sources]
set -euo pipefail
# shellcheck source=lib/interop-common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/interop-common.sh"

A="${LATE_ACK_A:-go}"
B="${LATE_ACK_B:-node}"
RETRY_AFTER_S=3
A_HOLD_S=6
B_HOLD_S=8

queue="bq-lateack-$RUN_ID"
alog="$WORK/late-ack-$A.log"
blog="$WORK/late-ack-$B.log"

redis_up
build_probes go "$B" "$A"

id="$(publish_one "$queue" legacy "$WORK/late-ack-produce.log")"

start_probe "$A" "$alog" "QUEUE=$queue" LAYOUT=legacy "RETRY_AFTER=$RETRY_AFTER_S" \
  MODE=slow "HOLD=$A_HOLD_S" TIMEOUT=30 -- consume
a_pid="$PROBE_PID"
wait_line "$alog" "^PROBE $A RECEIVED id=$id " 60 || fail "$A never reserved the message: $(cat "$alog")"
a_received_at="$(date +%s)"

# B starts polling right away; it can only get the message once the sweep has requeued it.
start_probe "$B" "$blog" "QUEUE=$queue" LAYOUT=legacy RETRY_AFTER=60 \
  MODE=slow "HOLD=$B_HOLD_S" TIMEOUT=40 -- consume
b_pid="$PROBE_PID"
wait_line "$blog" "^PROBE $B RECEIVED id=$id " 40 || fail "$B never received the swept message: $(cat "$blog")"
b_took_after=$(($(date +%s) - a_received_at))
assert_eq "$(field "$blog" "$B" RECEIVED attempts)" "$(field "$alog" "$A" RECEIVED attempts)" \
  "$B attempts (A11: sweep must not count)"

# A acks late (A_HOLD_S after its reserve): wait for its warning, then check B's reservation survived.
wait_line "$alog" "reservation expired" 30 || fail "$A never logged 'reservation expired': $(cat "$alog")"
assert_eq "$(redis_int LLEN "$queue:processing")" 1 "$queue:processing while B still holds (A's late ack must not touch it)"
assert_eq "$(redis_int ZCARD "$queue:processing:deadlines")" 1 "$queue:processing:deadlines while B still holds"

wait_exit "$a_pid" 30
rc="$WAIT_RC"
[ "$rc" = "0" ] || fail "$A exit=$rc: $(cat "$alog")"
wait_exit "$b_pid" 40
rc="$WAIT_RC"
[ "$rc" = "0" ] || fail "$B exit=$rc: $(cat "$blog")"

assert_eq "$(event_count "$blog" "$B" RECEIVED)" 1 "$B RECEIVED count (processed exactly once by B)"
assert_eq "$(event_count "$blog" "$B" ACKED)" 1 "$B ACKED count"
assert_eq "$(event_count "$alog" "$A" RECEIVED)" 1 "$A RECEIVED count (A must not get it again)"
assert_eq "$(redis_int LLEN "$queue:processing")" 0 "$queue:processing LLEN at end"
assert_eq "$(redis_int ZCARD "$queue:processing:deadlines")" 0 "$queue:processing:deadlines ZCARD at end"
assert_eq "$(redis_int LLEN "$queue")" 0 "$queue LLEN at end (message not lost, not requeued)"

warning="$(grep -m1 "reservation expired" "$alog" | tr -s ' ' | cut -c1-160)"
log "LATE-ACK legacy retry_after=${RETRY_AFTER_S}s: $A held ${A_HOLD_S}s, $B took the message after ${b_took_after}s and processed it once"
log "LATE-ACK $A late ack warning: $warning"
log "LATE-ACK processing=0 deadlines=0 after $B ack"
log "redis-late-ack: OK"
