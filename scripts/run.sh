#!/usr/bin/env bash
# One end-to-end benchmark run.
#   NAME=baseline N=2000000 PARTITIONS=12 THREADS=6 GUARANTEE=at_least_once WHALE=0 SALT=0 ./scripts/run.sh
set -euo pipefail
cd "$(dirname "$0")/.."

NAME=${NAME:-baseline}
N=${N:-2000000}
PARTITIONS=${PARTITIONS:-12}
THREADS=${THREADS:-6}
GUARANTEE=${GUARANTEE:-at_least_once}
WHALE=${WHALE:-0}
SALT=${SALT:-0}
RATE=${RATE:-0}   # 0 = as fast as possible (capacity run); e.g. 20000 = fixed-rate latency run
JAVA_OPTS=${JAVA_OPTS:--Xms512m -Xmx1g}
RUN_ID="$NAME-$(date +%s)"
OUT="results/runs/$NAME"
JAR=target/bench.jar
KT="docker exec bench-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"

[ -f "$JAR" ] || ./mvnw -q -B package -DskipTests

# Disk guard: a 2M run needs about 1.5 GB inside Docker while it runs.
FREE_GB=$(df -g "$HOME" | awk 'NR==2 {print $4}')
if [ "${FREE_GB:-0}" -lt "${MIN_FREE_GB:-3}" ]; then
  echo "Only ${FREE_GB} GB free on disk; need at least ${MIN_FREE_GB:-3} GB. Free space and retry." >&2
  exit 1
fi
docker compose up -d --wait >/dev/null

echo "== resetting topics ($PARTITIONS partitions) and state"
for t in transactions tx.enriched tx.decisions; do $KT --delete --topic "$t" >/dev/null 2>&1 || true; done
for t in $($KT --list | grep -E '^scorer-' || true); do $KT --delete --topic "$t" >/dev/null 2>&1 || true; done
sleep 3
for t in transactions tx.enriched tx.decisions; do
  $KT --create --topic "$t" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
done
rm -rf "$OUT" && mkdir -p "$OUT"

echo "== seeding Postgres and warming Redis"
java $JAVA_OPTS -jar $JAR seed --results="$OUT" | tee "$OUT/seed.log"

cat > "$OUT/run.json" <<JSON
{"transactions": "$N", "partitions": "$PARTITIONS", "threadsPerStage": "$THREADS", "scorerGuarantee": "$GUARANTEE",
 "whaleShare": "$WHALE", "saltBuckets": "$SALT", "generatorRate": "${RATE} tx/s (0 = unlimited)", "brokers": "1 (replication factor 1)",
 "machine": "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null) cores, $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 )) GB RAM",
 "kafka": "apache/kafka:3.8.0 (KRaft)", "redis": "redis:7-alpine", "postgres": "postgres:16-alpine"}
JSON

echo "== starting consumers"
COMMON="--run-id=$RUN_ID --results=$OUT --threads=$THREADS"
java $JAVA_OPTS -jar $JAR ledger $COMMON --expect="$N" > "$OUT/ledger.log" 2>&1 & LEDGER=$!
java $JAVA_OPTS -jar $JAR score  $COMMON --guarantee="$GUARANTEE" > "$OUT/scorer.log" 2>&1 & SCORER=$!
java $JAVA_OPTS -jar $JAR enrich $COMMON > "$OUT/enricher.log" 2>&1 & ENRICHER=$!
sleep 10   # let consumer groups join before data arrives

echo "== producing $N transactions"
java $JAVA_OPTS -jar $JAR generate --results="$OUT" --count="$N" --partitions="$PARTITIONS" \
  --whale-share="$WHALE" --salt-buckets="$SALT" --rate="$RATE" | tee "$OUT/generator.log"

echo "== waiting for the ledger to commit all $N"
wait $LEDGER
echo "== waiting for enricher and scorer to drain and exit"
wait $SCORER $ENRICHER || true

java -jar $JAR report --dir="$OUT" --name="$NAME" --results=results > /dev/null
cat "$OUT/RESULTS.md"

# Clean up this run's data (results are already saved) so disk use stays flat across runs.
if [ "${KEEP_DATA:-0}" != "1" ]; then
  for t in transactions tx.enriched tx.decisions $($KT --list | grep -E '^scorer-' || true); do $KT --delete --topic "$t" >/dev/null 2>&1 || true; done
  docker exec bench-postgres psql -U bench -d payments -qc "TRUNCATE ledger_entries" >/dev/null
  docker exec bench-postgres psql -U bench -d payments -qc "CHECKPOINT" >/dev/null
  rm -rf "$OUT/state"
fi
