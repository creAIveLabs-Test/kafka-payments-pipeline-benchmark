#!/usr/bin/env bash
# LinkedIn-style raw Kafka throughput (same method as Jay Kreps' 2014 "2 million writes per second"):
# fixed-size 100-byte records, Kafka's own perf tools, no application logic.
#   N=2000000 OUT=results/raw ./scripts/raw-kafka.sh
set -euo pipefail
cd "$(dirname "$0")/.."
N=${N:-2000000}
OUT=${OUT:-results/raw}
mkdir -p "$OUT"
docker compose up -d --wait kafka >/dev/null
K="docker exec bench-kafka /opt/kafka/bin"
TOPIC=raw-perf

reset() {
  $K/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $TOPIC >/dev/null 2>&1 || true
  sleep 2
  $K/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $TOPIC --partitions "${1:-6}" --replication-factor 1 >/dev/null
}

produce() { # name, partitions, producer props...
  local name=$1 parts=$2; shift 2
  reset "$parts"
  echo "== $name"
  $K/kafka-producer-perf-test.sh --topic $TOPIC --num-records "$N" --record-size 100 --throughput -1 \
    --producer-props bootstrap.servers=localhost:9092 "$@" 2>/dev/null | tail -1 | tee "$OUT/$name.txt"
}

produce "producer_acks1_nocompression_6p" 6 acks=1 linger.ms=10 batch.size=262144 compression.type=none
produce "producer_acksall_idempotent_6p" 6 acks=all enable.idempotence=true linger.ms=10 batch.size=262144 compression.type=none
produce "producer_acks1_lz4_6p" 6 acks=1 linger.ms=10 batch.size=262144 compression.type=lz4
produce "producer_acks1_nocompression_1p" 1 acks=1 linger.ms=10 batch.size=262144 compression.type=none

echo "== consumer_6p"
$K/kafka-producer-perf-test.sh --topic $TOPIC --num-records 1 --record-size 100 --throughput -1 --producer-props bootstrap.servers=localhost:9092 >/dev/null 2>&1 || true
reset 6
$K/kafka-producer-perf-test.sh --topic $TOPIC --num-records "$N" --record-size 100 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=1 linger.ms=10 batch.size=262144 >/dev/null 2>&1
$K/kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic $TOPIC --messages "$N" --timeout 60000 2>/dev/null \
  | tail -1 | tee "$OUT/consumer_6p.txt"

python3 - "$OUT" <<'PY'
import json, os, re, sys
out = sys.argv[1]; tests = []
for f in sorted(os.listdir(out)):
    if not f.endswith('.txt'): continue
    line = open(os.path.join(out, f)).read().strip(); name = f[:-4]
    if name.startswith('producer'):
        m = re.search(r'([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency.*?, (\d+) ms 99th', line)
        if m: tests.append({"name": name, "recordsPerSec": int(float(m.group(1))), "mbPerSec": float(m.group(2)),
                            "avgLatencyMs": m.group(3), "p99LatencyMs": m.group(4)})
    else:
        p = [x.strip() for x in line.split(',')]
        # start, end, MB consumed, MB/sec, records consumed, records/sec, ...
        if len(p) >= 6: tests.append({"name": name, "recordsPerSec": int(float(p[5])), "mbPerSec": float(p[3])})
json.dump({"recordSizeBytes": 100, "records": int(os.environ.get('N', 2000000)), "tests": tests},
          open(os.path.join(out, 'raw-kafka.json'), 'w'), indent=2)
for t in tests: print(f"{t['name']:40s} {t['recordsPerSec']:>12,} records/sec  {t['mbPerSec']:>8.1f} MB/s")
PY
