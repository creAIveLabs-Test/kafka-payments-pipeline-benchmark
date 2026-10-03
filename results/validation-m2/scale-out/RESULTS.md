# Run: test-scale-out

| Setting | Value |
|---|---|
| transactions | 200000 |
| partitions | 12 |
| instancesPerStage | 2 |
| threadsPerInstance | 3 |
| scorerGuarantee | at_least_once |
| whaleShare | 0 |
| saltBuckets | 0 |
| generatorRate | 0 tx/s (0 = unlimited) |
| fault | scale-out |
| kafka | apache/kafka:3.8.0 (KRaft), 3 brokers, replication factor 3, min.insync.replicas 2 |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |
| machine | Apple M2, 8 logical cores, 16 GB RAM, macOS |

## End-to-end pipeline

- **200,000 transactions** committed to the ledger, from generator start to the last commit in **30.9 s** = **6,467 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 13751 ms, p95 18511 ms, p99 20095 ms, max 20607 ms
- Ledger rows: 200,000 of 200,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":151466,"DECLINE":4083,"REVIEW":44451}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 200,000 | 12,702 | 18,304 | hottest partition = 1.06x average |
| Enricher (Redis + Postgres) | 3 | 200,000 | 7,736 | 14,400 | cache hit ratio 0.0, 3 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 3 | 200,000 | 7,488 | 13,205 | at_least_once, 3 threads per instance |
| Ledger (Postgres batch insert) | 3 | 200,000 | 7,651 | 14,317 | 3 threads per instance |

Records per partition (generator): [16445,16505,16458,16535,17042,16186,16101,16657,16767,16754,16939,17611]

## Fault injection

| | |
|---|---|
| Fault | scale-out (one more instance of every stage joined mid-run (consumer group rebalance)) |
| Injected at | 69,948 ledger rows committed |
| Restarted after | 10 s |
| Baseline throughput before fault (ledger rows/sec) | 2,261 |
| Lowest throughput after fault | 7,189 rows/sec |
| Seconds with zero throughput | 0 |
| Recovered after | 0 s (throughput back above 50% of baseline and stayed there) |
| Data check | 200,000 rows of 200,000 expected, 0 duplicates absorbed |

