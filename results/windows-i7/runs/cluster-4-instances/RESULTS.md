# Run: cluster-4-instances

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 24 |
| instancesPerStage | 4 |
| threadsPerInstance | 4 |
| scorerGuarantee | at_least_once |
| whaleShare | 0 |
| saltBuckets | 0 |
| generatorRate | 0 tx/s (0 = unlimited) |
| fault | none |
| kafka | apache/kafka:3.8.0 (KRaft), 3 brokers, replication factor 3, min.insync.replicas 2 |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |
| machine | Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz, 12 logical cores, 31 GB RAM visible to Linux (WSL2) |

## End-to-end pipeline

- **2,000,000 transactions** committed to the ledger, from generator start to the last commit in **77.1 s** = **25,953 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 5023 ms, p95 14375 ms, p99 15991 ms, max 17663 ms
- Ledger rows: 2,000,000 of 2,000,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":1399282,"DECLINE":191397,"REVIEW":409321}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 2,000,000 | 30,787 | 68,131 | hottest partition = 1.12x average |
| Enricher (Redis + Postgres) | 4 | 2,000,000 | 30,604 | 72,267 | cache hit ratio 0.977, 4 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 4 | 2,000,000 | 30,552 | 71,186 | at_least_once, 4 threads per instance |
| Ledger (Postgres batch insert) | 4 | 2,000,000 | 26,633 | 70,747 | 4 threads per instance |

Records per partition (generator): [83424,83120,82704,83842,85177,83732,79401,81162,82898,86392,87604,93486,82117,80946,82055,82223,84363,80138,82300,82171,83871,81235,82655,82984]

