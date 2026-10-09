# Run: single-partition

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 1 |
| instancesPerStage | 1 |
| threadsPerInstance | 6 |
| scorerGuarantee | at_least_once |
| whaleShare | 0 |
| saltBuckets | 0 |
| generatorRate | 0 tx/s (0 = unlimited) |
| fault | none |
| kafka | apache/kafka:3.8.0 (KRaft), 1 broker, replication factor 1 |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |
| machine | Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz, 12 logical cores, 31 GB RAM visible to Linux (WSL2) |

## End-to-end pipeline

- **2,000,000 transactions** committed to the ledger, from generator start to the last commit in **74.5 s** = **26,838 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 34879 ms, p95 62879 ms, p99 65599 ms, max 66111 ms
- Ledger rows: 2,000,000 of 2,000,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":1399282,"DECLINE":191397,"REVIEW":409321}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 2,000,000 | 251,509 | 183,644 | hottest partition = 1.0x average |
| Enricher (Redis + Postgres) | 1 | 2,000,000 | 89,226 | 104,930 | cache hit ratio 0.977, 6 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 1 | 2,000,000 | 40,895 | 45,632 | at_least_once, 6 threads per instance |
| Ledger (Postgres batch insert) | 1 | 2,000,000 | 27,173 | 31,282 | 6 threads per instance |

Records per partition (generator): [2000000]

