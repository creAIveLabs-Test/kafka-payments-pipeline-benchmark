# Run: cluster-2-instances

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 24 |
| instancesPerStage | 2 |
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

- **2,000,000 transactions** committed to the ledger, from generator start to the last commit in **53.3 s** = **37,491 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 11999 ms, p95 14823 ms, p99 15399 ms, max 16399 ms
- Ledger rows: 2,000,000 of 2,000,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":1399282,"DECLINE":191397,"REVIEW":409321}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 2,000,000 | 50,783 | 90,365 | hottest partition = 1.12x average |
| Enricher (Redis + Postgres) | 2 | 2,000,000 | 46,788 | 74,400 | cache hit ratio 0.976, 4 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 2 | 2,000,000 | 47,081 | 73,620 | at_least_once, 4 threads per instance |
| Ledger (Postgres batch insert) | 2 | 2,000,000 | 39,040 | 102,000 | 4 threads per instance |

Records per partition (generator): [83424,83120,82704,83842,85177,83732,79401,81162,82898,86392,87604,93486,82117,80946,82055,82223,84363,80138,82300,82171,83871,81235,82655,82984]

