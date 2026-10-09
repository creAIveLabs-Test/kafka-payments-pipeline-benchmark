# Run: test-kill-broker

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
| fault | kill-broker |
| kafka | apache/kafka:3.8.0 (KRaft), 3 brokers, replication factor 3, min.insync.replicas 2 |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |
| machine | Apple M2, 8 logical cores, 16 GB RAM, macOS |

## End-to-end pipeline

- **200,000 transactions** committed to the ledger, from generator start to the last commit in **53.0 s** = **3,772 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 33759 ms, p95 40479 ms, p99 41535 ms, max 42367 ms
- Ledger rows: 200,000 of 200,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":151466,"DECLINE":4083,"REVIEW":44451}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 200,000 | 13,917 | 13,819 | hottest partition = 1.06x average |
| Enricher (Redis + Postgres) | 2 | 200,000 | 4,337 | 12,335 | cache hit ratio 0.955, 3 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 2 | 200,000 | 4,381 | 9,977 | at_least_once, 3 threads per instance |
| Ledger (Postgres batch insert) | 2 | 200,000 | 4,227 | 11,583 | 3 threads per instance |

Records per partition (generator): [16445,16505,16458,16535,17042,16186,16101,16657,16767,16754,16939,17611]

## Fault injection

| | |
|---|---|
| Fault | kill-broker (broker kafka-2 killed (docker kill); 2 of 3 replicas left, min.insync.replicas 2) |
| Injected at | 80,462 ledger rows committed |
| Restarted after | 10 s |
| Baseline throughput before fault (ledger, rows/sec) | 4,850 |
| Lowest throughput within 30 s | 0 rows/sec |
| Seconds with zero throughput (within 30 s) | 16 |
| Time to recover (80% of baseline) | 18 s |
| Data check | 200,000 rows of 200,000 expected, 0 duplicates absorbed |

