#!/usr/bin/env bash
# One end-to-end benchmark run. Works on macOS, Linux and Windows (WSL2).
#
#   NAME=baseline N=2000000 ./scripts/run.sh
#   NAME=cluster CLUSTER=1 INSTANCES=2 ./scripts/run.sh
#   NAME=kill-broker CLUSTER=1 INSTANCES=2 FAULT=kill-broker ./scripts/run.sh
#
# Settings (environment variables):
#   N            transactions (default 2000000)
#   PARTITIONS   partitions per topic (default 12)
#   THREADS      consumer threads per instance of each stage (default 6)
#   INSTANCES    processes per stage: enricher, scorer, ledger (default 1)
#   CLUSTER      0 = 1 broker, RF 1; 1 = 3 brokers, RF 3, min.insync.replicas 2 (default 0)
#   RATE         generator tx/sec, 0 = as fast as possible (default 0)
#   GUARANTEE    scorer: at_least_once | exactly_once_v2 (default at_least_once)
#   WHALE, SALT  share of traffic from one huge account, and salt buckets for it (default 0, 0)
#   FAULT        none | kill-consumer | kill-broker | scale-out (default none)
#   FAULT_AT     inject when this share of N is committed to the ledger (default 0.4)
#   RESTART_AFTER seconds before a killed process/broker is restarted (default 10)
#   KEEP_DATA    1 = keep topics and ledger rows after the run (default 0)
#   MIN_FREE_GB  refuse to start below this much free disk (default 3)
set -euo pipefail
cd "$(dirname "$0")/.."

NAME=${NAME:-baseline}
N=${N:-2000000}
PARTITIONS=${PARTITIONS:-12}
THREADS=${THREADS:-6}
INSTANCES=${INSTANCES:-1}
CLUSTER=${CLUSTER:-0}
RATE=${RATE:-0}
GUARANTEE=${GUARANTEE:-at_least_once}
WHALE=${WHALE:-0}
SALT=${SALT:-0}
FAULT=${FAULT:-none}
FAULT_AT=${FAULT_AT:-0.4}
RESTART_AFTER=${RESTART_AFTER:-10}
JAVA_OPTS=${JAVA_OPTS:--Xms512m -Xmx1g}
RUN_ID="$NAME-$(date +%s)"
OUT="results/runs/$NAME"
JAR=target/bench.jar

if [ "$CLUSTER" = "1" ]; then
  COMPOSE=(docker compose -p bench-cluster -f docker-compose.cluster.yml)
  OTHER=(docker compose -p bench-single -f docker-compose.yml)
  KC=bench-kafka-1; INNER=kafka-1:29092; BOOT=localhost:9092,localhost:9094,localhost:9096; RF=3
  KAFKA_DESC="apache/kafka:3.8.0 (KRaft), 3 brokers, replication factor 3, min.insync.replicas 2"
else
  COMPOSE=(docker compose -p bench-single -f docker-compose.yml)
  OTHER=(docker compose -p bench-cluster -f docker-compose.cluster.yml)
  KC=bench-kafka; INNER=localhost:9092; BOOT=localhost:9092; RF=1
  KAFKA_DESC="apache/kafka:3.8.0 (KRaft), 1 broker, replication factor 1"
fi

now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }
kt() { docker exec "$KC" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$INNER" "$@"; }
pgcount() { docker exec bench-postgres psql -U bench -d payments -qAtc "SELECT count(*) FROM ledger_entries" 2>/dev/null || echo 0; }
machine() {
  if sysctl -n machdep.cpu.brand_string >/dev/null 2>&1; then
    echo "$(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) logical cores, $(( $(sysctl -n hw.memsize) / 1073741824 )) GB RAM, macOS"
  else
    local cpu; cpu=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')
    echo "$cpu, $(nproc) logical cores, $(awk '/MemTotal/ {printf "%d", $2/1048576}' /proc/meminfo) GB RAM visible to $(uname -s) $(uname -r | grep -qi microsoft && echo '(WSL2)')"
  fi
}

[ -f "$JAR" ] || ./mvnw -q -B package -DskipTests

FREE_GB=$(df -Pk "$HOME" | awk 'NR==2 {printf "%d", $4/1048576}')
if [ "${FREE_GB:-0}" -lt "${MIN_FREE_GB:-3}" ]; then
  echo "Only ${FREE_GB} GB free on disk; need at least ${MIN_FREE_GB:-3} GB. Free space and retry." >&2
  exit 1
fi

"${OTHER[@]}" down >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --wait >/dev/null

echo "== resetting topics ($PARTITIONS partitions, replication factor $RF) and state"
for t in transactions tx.enriched tx.decisions $(kt --list | grep -E '^scorer-' || true); do kt --delete --topic "$t" >/dev/null 2>&1 || true; done
sleep 3
for t in transactions tx.enriched tx.decisions; do
  kt --create --topic "$t" --partitions "$PARTITIONS" --replication-factor "$RF" >/dev/null
done
rm -rf "$OUT" && mkdir -p "$OUT"

echo "== seeding Postgres and warming Redis"
java $JAVA_OPTS -jar $JAR seed --results="$OUT" | tee "$OUT/seed.log"

cat > "$OUT/run.json" <<JSON
{"transactions": "$N", "partitions": "$PARTITIONS", "instancesPerStage": "$INSTANCES", "threadsPerInstance": "$THREADS",
 "scorerGuarantee": "$GUARANTEE", "whaleShare": "$WHALE", "saltBuckets": "$SALT", "generatorRate": "${RATE} tx/s (0 = unlimited)",
 "fault": "$FAULT", "kafka": "$KAFKA_DESC", "redis": "redis:7-alpine", "postgres": "postgres:16-alpine", "machine": "$(machine)"}
JSON

PIDS="$OUT/pids"; : > "$PIDS"
start_stage() { # command instance [extra args]
  local cmd=$1 i=$2; shift 2
  java $JAVA_OPTS -jar $JAR "$cmd" --run-id="$RUN_ID" --results="$OUT" --threads="$THREADS" --instance="$i" \
    --bootstrap="$BOOT" "$@" >> "$OUT/$cmd-$i.log" 2>&1 &
  echo "$! $cmd $i" >> "$PIDS"
}
pid_of() { awk -v c="$1" -v i="$2" '$2 == c && $3 == i {p = $1} END {print p}' "$PIDS"; }

inject_fault() {
  local target_rows deadline=$(( $(date +%s) + 900 ))
  target_rows=$(awk -v n="$N" -v f="$FAULT_AT" 'BEGIN {printf "%d", n * f}')
  until [ "$(pgcount)" -ge "$target_rows" ]; do
    [ "$(date +%s)" -gt "$deadline" ] && { echo "fault: gave up waiting for $target_rows rows"; return; }
    sleep 1
  done
  local at_rows injected desc
  at_rows=$(pgcount); injected=$(now_ms)
  case "$FAULT" in
    kill-consumer)
      kill -9 "$(pid_of enrich 1)" "$(pid_of ledger 1)" 2>/dev/null || true
      desc="enricher-1 and ledger-1 killed with SIGKILL (no clean shutdown, no offset commit)"
      echo "== FAULT: $desc at $at_rows rows"
      sleep "$RESTART_AFTER"
      start_stage enrich 1
      start_stage ledger 1 --expect="$N"
      ;;
    kill-broker)
      docker kill bench-kafka-2 >/dev/null
      desc="broker kafka-2 killed (docker kill); 2 of 3 replicas left, min.insync.replicas 2"
      echo "== FAULT: $desc at $at_rows rows"
      sleep "$RESTART_AFTER"
      docker start bench-kafka-2 >/dev/null
      ;;
    scale-out)
      local j=$((INSTANCES + 1))
      start_stage ledger "$j" --expect="$N"
      start_stage score "$j" --guarantee="$GUARANTEE"
      start_stage enrich "$j"
      desc="one more instance of every stage joined mid-run (consumer group rebalance)"
      echo "== FAULT: $desc at $at_rows rows"
      ;;
  esac
  cat > "$OUT/fault.json" <<JSON
{"type": "$FAULT", "target": "$desc", "injectedMs": $injected, "atRows": $at_rows, "restartedAfterSec": "$RESTART_AFTER"}
JSON
}

echo "== starting $INSTANCES instance(s) of each stage, $THREADS threads each"
for i in $(seq 1 "$INSTANCES"); do
  start_stage ledger "$i" --expect="$N"
  start_stage score "$i" --guarantee="$GUARANTEE"
  start_stage enrich "$i"
done
sleep 10   # let consumer groups join before data arrives

INJECTOR=""
if [ "$FAULT" != "none" ]; then inject_fault & INJECTOR=$!; fi

echo "== producing $N transactions"
java $JAVA_OPTS -jar $JAR generate --results="$OUT" --count="$N" --partitions="$PARTITIONS" --bootstrap="$BOOT" \
  --whale-share="$WHALE" --salt-buckets="$SALT" --rate="$RATE" | tee "$OUT/generator.log"

[ -n "$INJECTOR" ] && wait "$INJECTOR" || true
echo "== waiting for every stage to finish (the ledger writes DONE at $N rows; every stage stops on DONE)"
last_rows=-1; still=0
while :; do
  alive=0
  while read -r pid _; do kill -0 "$pid" 2>/dev/null && alive=1; done < "$PIDS"
  [ "$alive" = 0 ] && break
  rows=$(pgcount)
  [ "$rows" -ge "$N" ] && touch "$OUT/DONE"
  if [ "$rows" = "$last_rows" ]; then still=$((still + 2)); else still=0; last_rows=$rows; fi
  if [ "$still" -ge "${STALL_SEC:-180}" ] && [ ! -f "$OUT/DONE" ]; then
    echo "== STALLED: ledger stuck at $rows of $N rows for ${STALL_SEC:-180}s; stopping the run" | tee "$OUT/STALLED"
    touch "$OUT/DONE"
  fi
  sleep 2
done

java -jar $JAR report --dir="$OUT" --name="$NAME" --results=results > /dev/null
cat "$OUT/RESULTS.md"

# Clean up this run's data (results are already saved) so disk use stays flat across runs.
if [ "${KEEP_DATA:-0}" != "1" ]; then
  for t in transactions tx.enriched tx.decisions $(kt --list | grep -E '^scorer-' || true); do kt --delete --topic "$t" >/dev/null 2>&1 || true; done
  docker exec bench-postgres psql -U bench -d payments -qc "TRUNCATE ledger_entries" >/dev/null
  docker exec bench-postgres psql -U bench -d payments -qc "CHECKPOINT" >/dev/null
  rm -rf "$OUT"/state-*
fi
