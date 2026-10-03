#!/usr/bin/env bash
# Distributed-systems experiments on a 3-broker cluster (replication factor 3):
# horizontal scaling, consumer crash, broker crash, scale-out rebalance, and LinkedIn's replication tests.
# Recommended on a machine with 16+ GB RAM given to Docker/WSL. Takes about 20-30 minutes for N=2,000,000.
set -euo pipefail
cd "$(dirname "$0")/.."
N=${N:-2000000}
P=${PARTITIONS:-24}
T=${THREADS:-4}
run() { echo; echo "######## $1"; env NAME="$1" N="$N" CLUSTER=1 PARTITIONS="$P" "${@:2}" ./scripts/run.sh \
  | grep -E 'transactions/sec end to end|latency \(|FAULT|Time to recover|Lowest throughput|Data check' || true; }

run cluster-1-instance           INSTANCES=1 THREADS="$T"
run cluster-2-instances          INSTANCES=2 THREADS="$T"
run cluster-4-instances          INSTANCES=4 THREADS="$T"
run cluster-fault-kill-consumer  INSTANCES=2 THREADS="$T" FAULT=kill-consumer
run cluster-fault-kill-broker    INSTANCES=2 THREADS="$T" FAULT=kill-broker
run cluster-fault-scale-out      INSTANCES=2 THREADS="$T" FAULT=scale-out

N="$N" CLUSTER=1 ./scripts/raw-kafka.sh
java -jar target/bench.jar report --summary=true --results=results
