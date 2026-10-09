# Run: whale-30pct

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 12 |
| instancesPerStage | 1 |
| threadsPerInstance | 6 |
| scorerGuarantee | at_least_once |
| whaleShare | 0.3 |
| saltBuckets | 0 |
| generatorRate | 0 tx/s (0 = unlimited) |
| fault | none |
| kafka | apache/kafka:3.8.0 (KRaft), 1 broker, replication factor 1 |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |
| machine | Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz, 12 logical cores, 31 GB RAM visible to Linux (WSL2) |

## End-to-end pipeline

- **2,000,000 transactions** committed to the ledger, from generator start to the last commit in **49.5 s** = **40,399 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 6215 ms, p95 25967 ms, p99 26831 ms, max 27295 ms
- Ledger rows: 2,000,000 of 2,000,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":1001105,"DECLINE":706498,"REVIEW":292397}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 2,000,000 | 92,009 | 128,495 | hottest partition = 4.3x average |
| Enricher (Redis + Postgres) | 1 | 2,000,000 | 75,755 | 104,570 | cache hit ratio 0.977, 6 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 1 | 2,000,000 | 45,663 | 109,183 | at_least_once, 6 threads per instance |
| Ledger (Postgres batch insert) | 1 | 2,000,000 | 41,264 | 69,868 | 6 threads per instance |

Records per partition (generator): [115796,115354,115128,115799,118414,114994,112557,114339,717176,117537,119060,123846]

