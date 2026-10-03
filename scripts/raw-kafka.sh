#!/usr/bin/env bash
# LinkedIn-style raw Kafka throughput (same method as Jay Kreps' 2014 "2 million writes per second"):
# fixed-size 100-byte records, Kafka's own perf tools, no application logic.
#
#   N=2000000 ./scripts/raw-kafka.sh              # 1 broker
#   N=2000000 CLUSTER=1 ./scripts/raw-kafka.sh    # 3 brokers, replication factor 3 (LinkedIn's replication tests)
set -euo pipefail
cd "$(dirname "$0")/.."
N=${N:-2000000}
CLUSTER=${CLUSTER:-0}
TOPIC=raw-perf

if [ "$CLUSTER" = "1" ]; then
  COMPOSE=(docker compose -p bench-cluster -f docker-compose.cluster.yml)
  OTHER=(docker compose -p bench-single -f docker-compose.yml)
  BROKERS=(bench-kafka-1 bench-kafka-2 bench-kafka-3); INNER=kafka-1:29092,kafka-2:29092,kafka-3:29092; RF=3
  OUT=${OUT:-results/raw-cluster}
else
  COMPOSE=(docker compose -p bench-single -f docker-compose.yml)
  OTHER=(docker compose -p bench-cluster -f docker-compose.cluster.yml)
  BROKERS=(bench-kafka); INNER=localhost:9092; RF=1
  OUT=${OUT:-results/raw}
fi
KC=${BROKERS[0]}
BIN=/opt/kafka/bin
mkdir -p "$OUT" && rm -f "$OUT"/*.txt
"${OTHER[@]}" down >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --wait >/dev/null

reset() { # partitions
  docker exec "$KC" $BIN/kafka-topics.sh --bootstrap-server "$INNER" --delete --topic $TOPIC >/dev/null 2>&1 || true
  sleep 3
  docker exec "$KC" $BIN/kafka-topics.sh --bootstrap-server "$INNER" --create --topic $TOPIC \
    --partitions "$1" --replication-factor "$RF" >/dev/null
}

perf() { # container, records, producer props...
  local c=$1 n=$2; shift 2
  docker exec "$c" $BIN/kafka-producer-perf-test.sh --topic $TOPIC --num-records "$n" --record-size 100 --throughput -1 \
    --producer-props bootstrap.servers="$INNER" "$@" 2>/dev/null | tail -1
}

produce() { # name, partitions, producer props...
  local name=$1 parts=$2; shift 2
  reset "$parts"
  echo "== $name"
  perf "$KC" "$N" "$@" | tee "$OUT/$name.txt"
}

if [ "$CLUSTER" = "1" ]; then
  # LinkedIn's replication tests: "3x asynchronous replication" = acks=1, "3x synchronous replication" = acks=all
  produce "producer_rf3_acks1_async_replication" 6 acks=1 linger.ms=10 batch.size=262144
  produce "producer_rf3_acksall_sync_replication" 6 acks=all enable.idempotence=true linger.ms=10 batch.size=262144
  # LinkedIn's "three producers, 3x async replication": three producers at once, one per broker container
  reset 6
  echo "== three_producers_rf3_acks1 (3 producers in parallel)"
  for i in 0 1 2; do
    perf "${BROKERS[$i]}" "$N" acks=1 linger.ms=10 batch.size=262144 > "$OUT/three_producers_part$i.part" &
  done
  wait
  cat "$OUT"/three_producers_part*.part | tee "$OUT/three_producers_rf3_acks1.txt"
  rm -f "$OUT"/*.part
else
  produce "producer_acks1_nocompression_6p" 6 acks=1 linger.ms=10 batch.size=262144 compression.type=none
  produce "producer_acksall_idempotent_6p" 6 acks=all enable.idempotence=true linger.ms=10 batch.size=262144 compression.type=none
  produce "producer_acks1_lz4_6p" 6 acks=1 linger.ms=10 batch.size=262144 compression.type=lz4
  produce "producer_acks1_nocompression_1p" 1 acks=1 linger.ms=10 batch.size=262144 compression.type=none
fi

echo "== consumer_6p"
reset 6
perf "$KC" "$N" acks=1 linger.ms=10 batch.size=262144 >/dev/null
docker exec "$KC" $BIN/kafka-consumer-perf-test.sh --bootstrap-server "$INNER" --topic $TOPIC --messages "$N" --timeout 60000 2>/dev/null \
  | tail -1 | tee "$OUT/consumer_6p.txt"
docker exec "$KC" $BIN/kafka-topics.sh --bootstrap-server "$INNER" --delete --topic $TOPIC >/dev/null 2>&1 || true

N="$N" CLUSTER="$CLUSTER" python3 - "$OUT" <<'PY'
import json, os, re, sys
out = sys.argv[1]; tests = []
prod = re.compile(r'([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency.*?, (\d+) ms 99th')
for f in sorted(os.listdir(out)):
    if not f.endswith('.txt'): continue
    name = f[:-4]
    lines = [l.strip() for l in open(os.path.join(out, f)) if l.strip()]
    if name.startswith('consumer'):
        p = [x.strip() for x in lines[-1].split(',')]
        # start, end, MB consumed, MB/sec, records consumed, records/sec, ...
        if len(p) >= 6: tests.append({"name": name, "recordsPerSec": int(float(p[5])), "mbPerSec": float(p[3])})
        continue
    ms = [prod.search(l) for l in lines]
    ms = [m for m in ms if m]
    if not ms: continue
    t = {"name": name, "producers": len(ms),
         "recordsPerSec": int(sum(float(m.group(1)) for m in ms)),
         "mbPerSec": round(sum(float(m.group(2)) for m in ms), 1),
         "avgLatencyMs": round(sum(float(m.group(3)) for m in ms) / len(ms), 2),
         "p99LatencyMs": max(int(m.group(4)) for m in ms)}
    tests.append(t)
json.dump({"recordSizeBytes": 100, "recordsPerProducer": int(os.environ['N']),
           "brokers": 3 if os.environ['CLUSTER'] == '1' else 1,
           "replicationFactor": 3 if os.environ['CLUSTER'] == '1' else 1, "tests": tests},
          open(os.path.join(out, 'raw-kafka.json'), 'w'), indent=2)
for t in tests: print(f"{t['name']:42s} {t['recordsPerSec']:>12,} records/sec  {t['mbPerSec']:>8.1f} MB/s")
PY
