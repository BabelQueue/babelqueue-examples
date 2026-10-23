#!/usr/bin/env bash
# Redis delayed delivery across languages (R1 Faz 10, scope 1).
#
# Go publishes with delay=2s to a Laravel-layout (zset) queue; each of Node, Java, .NET and
# Python consumes it. The measured delay (received_at - published_at) must be in [1.0, 3.5] s:
# the delayed score is floor(now)+ceil(delay) and the consumer's block slice is <= 1 s, so the
# delivery may be a little shorter or longer than the nominal 2 s.
#
# Usage: scripts/redis-delay.sh            (published SDKs)
#        BQ_SDK_ROOT=/path/to/BabelQueue scripts/redis-delay.sh   (in-repo SDK sources)
set -euo pipefail
# shellcheck source=lib/interop-common.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/interop-common.sh"

DELAY_S=2
MIN_MS=1000
MAX_MS=3500
CONSUMERS="${DELAY_CONSUMERS:-node java dotnet python}"

redis_up
# shellcheck disable=SC2086
build_probes go $CONSUMERS

for lang in $CONSUMERS; do
  queue="bq-delay-$lang-$RUN_ID"
  clog="$WORK/delay-consume-$lang.log"
  plog="$WORK/delay-produce-$lang.log"

  start_probe "$lang" "$clog" "QUEUE=$queue" LAYOUT=zset RETRY_AFTER=30 TIMEOUT=25 -- consume
  consumer_pid="$PROBE_PID"
  wait_line "$clog" "^PROBE $lang READY" 60 || fail "$lang consumer never became ready: $(cat "$clog")"

  run_probe go "$plog" "QUEUE=$queue" LAYOUT=zset "DELAY=$DELAY_S" COUNT=1 FROM=go -- produce \
    || fail "go produce failed: $(cat "$plog")"
  published_at="$(field "$plog" go PUBLISHED at_ms)"
  published_id="$(field "$plog" go PUBLISHED id)"

  wait_exit "$consumer_pid" 40
  rc="$WAIT_RC"
  [ "$rc" = "0" ] || fail "$lang consumer exit=$rc: $(cat "$clog")"
  received_at="$(field "$clog" "$lang" RECEIVED at_ms)"
  received_id="$(field "$clog" "$lang" RECEIVED id)"
  [ -n "$received_at" ] || fail "$lang consumer logged no RECEIVED line: $(cat "$clog")"
  assert_eq "$received_id" "$published_id" "go->$lang message id"

  measured=$((received_at - published_at))
  log "DELAY go->$lang delay=${DELAY_S}s measured=$(ms_to_s "$measured")s (accepted ${MIN_MS}-${MAX_MS} ms)"
  in_range "$measured" "$MIN_MS" "$MAX_MS" \
    || fail "go->$lang measured delay ${measured} ms is outside [${MIN_MS}, ${MAX_MS}]"

  assert_eq "$(redis_int ZCARD "queues:$queue:delayed")" 0 "$lang: queues:$queue:delayed ZCARD"
  assert_eq "$(redis_int ZCARD "queues:$queue:reserved")" 0 "$lang: queues:$queue:reserved ZCARD"
done

log "redis-delay: OK"
