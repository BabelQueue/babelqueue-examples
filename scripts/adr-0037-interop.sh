#!/usr/bin/env bash
# Runs both ADR-0037 live scenarios (version-gate.sh: Pulsar + Artemis, 13 cases by default; poison-dlq.sh: Artemis +
# SQS, 23 cases by default) with the in-repo SDK sources, one command, and reports both results and the total.
#
#   scripts/adr-0037-interop.sh                       # BQ_SDK_ROOT defaults to this checkout's parent directory
#   BQ_SDK_ROOT=/path/to/BabelQueue scripts/adr-0037-interop.sh
#
# Needs Docker, curl, jq, bash 3.2+ and Go, Node + npm, JDK 17 + Maven, .NET 8. Each script brings the compose brokers
# it started down again when it exits (KEEP_BROKERS=1 keeps them); a compose project that was already running before the
# script, and a broker you run yourself (*_NO_COMPOSE=1), is left alone.
# Exits 0 only if every case of both scripts passes. Env knobs: see the headers of the two scripts.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -z "${BQ_SDK_ROOT:-}" ]; then
  root="$(cd "$here/../.." && pwd)"
  if [ -d "$root/babelqueue-go" ] && [ -d "$root/babelqueue-java" ] && [ -d "$root/babelqueue-node-adapters" ]; then
    export BQ_SDK_ROOT="$root"
  else
    printf '[interop] FAIL: set BQ_SDK_ROOT to the BabelQueue checkout that holds the SDK repositories\n' >&2
    exit 2
  fi
fi
printf '[interop] BQ_SDK_ROOT=%s\n' "$BQ_SDK_ROOT"

rc=0
out="$(mktemp "${TMPDIR:-/tmp}/bq-adr0037.XXXXXX")"
trap 'rm -f "$out"' EXIT
"$here/version-gate.sh" 2>&1 | tee -a "$out"
[ "${PIPESTATUS[0]}" -eq 0 ] || rc=1
"$here/poison-dlq.sh" 2>&1 | tee -a "$out"
[ "${PIPESTATUS[0]}" -eq 0 ] || rc=1
if [ "$rc" -eq 0 ]; then
  # The total is whatever the two scripts report, so a narrowed run (GATE_BROKERS, ARTEMIS_CONSUMERS, ...) is counted right.
  total=0
  while IFS= read -r n; do
    total=$((total + n))
  done < <(sed -n 's/^.*: OK (\([0-9][0-9]*\) cases)$/\1/p' "$out")
  printf '[interop] adr-0037-interop: OK (%s cases)\n' "$total"
else
  printf '[interop] adr-0037-interop: FAILED (see above)\n' >&2
fi
exit "$rc"
