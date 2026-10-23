#!/usr/bin/env bash
# Laravel babelqueue-redis connection and an SDK sharing one zset queue (R1 Faz 10, scope 2).
#
# Laravel pushes N messages and Go pushes N messages to the same Laravel-layout queue. The Laravel
# consumer (BabelQueueRedisQueue::pop) and a Go consumer drain it concurrently. Expected: every
# published id is acked exactly once across both consumers (nothing lost, nothing twice), and both
# consumers must have processed at least one message (the queue really is shared).
#
# Usage: scripts/redis-laravel-shared.sh   [BQ_SDK_ROOT=/path/to/BabelQueue for in-repo SDK sources]
set -euo pipefail
# shellcheck source=lib/interop-common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/interop-common.sh"

COUNT="${SHARED_COUNT:-20}"
# Each consumer holds every message for HOLD seconds before acking, so neither can drain the queue
# alone before the other starts: both must end up with a share.
HOLD_S="${SHARED_HOLD_S:-0.3}"
queue="bq-shared-$RUN_ID"

redis_up
build_probes go laravel

run_probe laravel "$WORK/shared-push-laravel.log" "QUEUE=$queue" "COUNT=$COUNT" FROM=laravel -- push \
  || fail "laravel push failed: $(cat "$WORK/shared-push-laravel.log")"
run_probe go "$WORK/shared-push-go.log" "QUEUE=$queue" LAYOUT=zset DELAY=0 "COUNT=$COUNT" FROM=go -- produce \
  || fail "go produce failed: $(cat "$WORK/shared-push-go.log")"

grep -h -o 'id=[^ ]*' "$WORK/shared-push-laravel.log" "$WORK/shared-push-go.log" | sort >"$WORK/shared-published.txt"
published="$(wc -l <"$WORK/shared-published.txt" | tr -d ' ')"
assert_eq "$published" "$((COUNT * 2))" "published message count"
assert_eq "$(sort -u "$WORK/shared-published.txt" | wc -l | tr -d ' ')" "$published" "published ids are unique"
assert_eq "$(redis_int LLEN "queues:$queue")" "$published" "queues:$queue LLEN after publishing"

start_probe laravel "$WORK/shared-consume-laravel.log" "QUEUE=$queue" RETRY_AFTER=60 "HOLD=$HOLD_S" IDLE=4 TIMEOUT=90 -- consume
lpid="$PROBE_PID"
start_probe go "$WORK/shared-consume-go.log" "QUEUE=$queue" LAYOUT=zset RETRY_AFTER=60 MODE=drain "HOLD=$HOLD_S" IDLE=4 TIMEOUT=90 -- consume
gpid="$PROBE_PID"

wait_exit "$lpid" 120
rc="$WAIT_RC"
[ "$rc" = "0" ] || fail "laravel consumer exit=$rc: $(cat "$WORK/shared-consume-laravel.log")"
wait_exit "$gpid" 120
rc="$WAIT_RC"
[ "$rc" = "0" ] || fail "go consumer exit=$rc: $(cat "$WORK/shared-consume-go.log")"

grep -h '^PROBE [a-z]* ACKED ' "$WORK/shared-consume-laravel.log" "$WORK/shared-consume-go.log" \
  | grep -o 'id=[^ ]*' | sort >"$WORK/shared-acked.txt"
acked="$(wc -l <"$WORK/shared-acked.txt" | tr -d ' ')"
laravel_n="$(event_count "$WORK/shared-consume-laravel.log" laravel ACKED)"
go_n="$(event_count "$WORK/shared-consume-go.log" go ACKED)"

[ "$laravel_n" -gt 0 ] || fail "laravel consumer processed no message: the queue was not shared (go consumer took all $go_n)"
[ "$go_n" -gt 0 ] || fail "go consumer processed no message: the queue was not shared (laravel consumer took all $laravel_n)"
assert_eq "$((laravel_n + go_n))" "$published" "laravel + go consumer ack counts add up to the published count"
assert_eq "$acked" "$published" "acked message count (nothing lost, nothing processed twice)"
diff "$WORK/shared-published.txt" "$WORK/shared-acked.txt" >/dev/null \
  || fail "acked ids differ from published ids"
assert_eq "$(redis_int LLEN "queues:$queue")" 0 "queues:$queue LLEN at end"
assert_eq "$(redis_int ZCARD "queues:$queue:reserved")" 0 "queues:$queue:reserved ZCARD at end"
assert_eq "$(redis_int ZCARD "queues:$queue:delayed")" 0 "queues:$queue:delayed ZCARD at end"

log "SHARED zset queue: published=$published (laravel=$COUNT go=$COUNT) acked=$acked (laravel consumer=$laravel_n, go consumer=$go_n) lost=0 duplicates=0"
log "redis-laravel-shared: OK"
