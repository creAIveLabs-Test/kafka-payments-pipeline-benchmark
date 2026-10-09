# Run: ibm-1000

| Setting | Value |
|---|---|
| transactions | 1000 |
| source | ibm |
| partitions | 12 |
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

- **1,000 transactions** committed to the ledger, from generator start to the last commit in **1.4 s** = **703 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 690 ms, p95 718 ms, p99 722 ms, max 880 ms
- Ledger rows: 1,000 of 1,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":1000}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 1,000 | 10,000 | 0 | hottest partition = 12.0x average |
| Enricher (Redis + Postgres) | 1 | 1,000 | 9,174 | 0 | cache hit ratio 0.993, 6 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 1 | 1,000 | 2,639 | 16 | at_least_once, 6 threads per instance |
| Ledger (Postgres batch insert) | 1 | 1,000 | 4,673 | 0 | 6 threads per instance |

Records per partition (generator): [0,0,0,0,0,0,0,0,0,1000,0,0]

