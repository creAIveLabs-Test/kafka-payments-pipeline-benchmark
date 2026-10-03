#!/usr/bin/env bash
# Runs every experiment in the README, then builds results/SUMMARY.md.
set -euo pipefail
cd "$(dirname "$0")/.."
N=${N:-2000000}
run() { echo; echo "######## $1"; env NAME="$1" N="$N" "${@:2}" ./scripts/run.sh | grep -E 'transactions/sec end to end|latency' ; }

run baseline-2M-12p
run latency-at-20k-per-sec   RATE=20000
run single-partition         PARTITIONS=1
run whale-30pct              WHALE=0.3
run whale-30pct-salted-4     WHALE=0.3 SALT=4
run scorer-exactly-once      GUARANTEE=exactly_once_v2

java -jar target/bench.jar report --summary=true --results=results
