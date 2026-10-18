#!/usr/bin/env bash
# Merge the per-broker nightly-interop job records into the published results file.
#
#   scripts/interop-merge.sh <results-dir> [<out-dir>]
#
# <results-dir> holds one JSON file per broker job, each shaped
#   {"broker","producer","consumer","status","run_url"}   (status: "passing" | "failing")
# <out-dir> (default: .) receives
#   interop-results.json  {"generated_at","run_url","brokers":[...]}   — always 7 entries
#
# The README badge is served by babelqueue.com/badges/nightly.json, built from this file
# (shields.io refuses github.com as an endpoint host).
#
# A broker with no record (its job was skipped or never wrote one) is listed as "missing"
# and counts as not passing, so the results never over-report. RUN_URL and GENERATED_AT may be
# set in the environment (GENERATED_AT defaults to the current UTC time).
set -euo pipefail

BROKERS=(redis rabbitmq sqs pulsar kafka artemis azureservicebus)
TOTAL=${#BROKERS[@]}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <results-dir> [<out-dir>]" >&2
  exit 2
fi
results_dir=$1
out_dir=${2:-.}

command -v jq >/dev/null 2>&1 || { echo "interop-merge: jq is required" >&2; exit 2; }
[[ -d "$results_dir" ]] || { echo "interop-merge: results dir not found: $results_dir" >&2; exit 2; }
mkdir -p "$out_dir"

generated_at=${GENERATED_AT:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}
run_url=${RUN_URL:-}

# Collect every record (recursively: download-artifact may nest one directory per job).
records=$(find "$results_dir" -type f -name '*.json' -print0 | xargs -0 -r jq -c 'select(type == "object" and has("broker"))' | jq -s '.')
[[ -n "$records" ]] || records='[]'

jq -n \
  --argjson records "$records" \
  --argjson expected "$(printf '%s\n' "${BROKERS[@]}" | jq -R . | jq -s .)" \
  --arg generated_at "$generated_at" \
  --arg run_url "$run_url" '
  {
    generated_at: $generated_at,
    run_url: $run_url,
    brokers: [
      $expected[] as $name
      | ($records | map(select(.broker == $name)) | last) as $r
      | if $r == null
        then {broker: $name, producer: "", consumer: "", status: "missing", run_url: $run_url}
        else {
          broker: $name,
          producer: ($r.producer // ""),
          consumer: ($r.consumer // ""),
          status: (if $r.status == "passing" then "passing" else "failing" end),
          run_url: ($r.run_url // $run_url)
        }
        end
    ]
  }' > "$out_dir/interop-results.json"

passing=$(jq '[.brokers[] | select(.status == "passing")] | length' "$out_dir/interop-results.json")

echo "interop-merge: ${passing}/${TOTAL} brokers passing -> $out_dir/interop-results.json"
