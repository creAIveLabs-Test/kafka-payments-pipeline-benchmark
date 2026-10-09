# Run: ibm-100000

| Setting | Value |
|---|---|
| transactions | 100000 |
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

- **100,000 transactions** committed to the ledger, from generator start to the last commit in **6.2 s** = **16,116 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 1409 ms, p95 2619 ms, p99 2873 ms, max 2913 ms
- Ledger rows: 100,000 of 100,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":99933,"REVIEW":67}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 100,000 | 21,645 | 19,263 | hottest partition = 3.28x average |
| Enricher (Redis + Postgres) | 1 | 100,000 | 21,133 | 20,000 | cache hit ratio 0.999, 6 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 1 | 100,000 | 20,458 | 19,978 | at_least_once, 6 threads per instance |
| Ledger (Postgres batch insert) | 1 | 100,000 | 19,346 | 13,049 | 6 threads per instance |

Records per partition (generator): [18542,528,17109,15431,481,0,6861,12520,1197,27331,0,0]

