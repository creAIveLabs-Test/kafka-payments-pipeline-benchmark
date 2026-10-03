# Run: test-cluster-2inst

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
| fault | none |
| kafka | apache/kafka:3.8.0 (KRaft), 3 brokers, replication factor 3, min.insync.replicas 2 |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |
| machine | Apple M2, 8 logical cores, 16 GB RAM, macOS |

## End-to-end pipeline

- **200,000 transactions** committed to the ledger, from generator start to the last commit in **37.1 s** = **5,387 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 23247 ms, p95 29551 ms, p99 30271 ms, max 30623 ms
- Ledger rows: 200,000 of 200,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":151466,"DECLINE":4083,"REVIEW":44451}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 200,000 | 17,046 | 25,997 | hottest partition = 1.06x average |
| Enricher (Redis + Postgres) | 2 | 200,000 | 6,648 | 12,800 | cache hit ratio 0.954, 3 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 2 | 200,000 | 6,699 | 12,731 | at_least_once, 3 threads per instance |
| Ledger (Postgres batch insert) | 2 | 200,000 | 7,025 | 12,294 | 3 threads per instance |

Records per partition (generator): [16445,16505,16458,16535,17042,16186,16101,16657,16767,16754,16939,17611]

