#!/usr/bin/env bash
# Replays the IBM TabFormer credit-card file through the pipeline in growing sizes.
#
#   IBM_CSV=/mnt/e/credit_card/credit_card/transactions/card_transaction.v1.csv ./scripts/run-ibm.sh
#   SIZES="1000 100000" IBM_CSV=... ./scripts/run-ibm.sh
#
# Each size replays the first N rows of the file (the file is sorted by user, card and time, so a
# smaller size covers fewer cardholders, all of their history). "all" = every row (24,386,900).
# Results: results/ibm-tabformer/runs/ibm-<size>/RESULTS.md and FRAUD.md, plus results/ibm-tabformer/SUMMARY.md.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${IBM_CSV:?set IBM_CSV to the path of card_transaction.v1.csv}"
SIZES=${SIZES:-"1000 100000 2000000 all"}
ROOT=results/ibm-tabformer

for s in $SIZES; do
  if [ "$s" = "all" ]; then
    n=$(( $(wc -l < "$IBM_CSV") - 1 ))
    free=15     # measured: about 34 MB per 100K rows (Kafka + ledger), so about 8.5 GB for 24M, plus headroom
  else
    n=$s
    free=5
  fi
  echo; echo "######## ibm-$s ($n rows)"
  NAME="ibm-$s" N="$n" SOURCE=ibm IBM_CSV="$IBM_CSV" RESULTS_ROOT="$ROOT" MIN_FREE_GB="${MIN_FREE_GB:-$free}" \
    STALL_SEC="${STALL_SEC:-300}" ./scripts/run.sh
done

java -jar target/bench.jar report --summary=true --results="$ROOT" > /dev/null
echo; echo "Summary: $ROOT/SUMMARY.md"
