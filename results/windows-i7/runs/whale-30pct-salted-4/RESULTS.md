# Run: whale-30pct-salted-4

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 12 |
| instancesPerStage | 1 |
| threadsPerInstance | 6 |
| scorerGuarantee | at_least_once |
| whaleShare | 0.3 |
| saltBuckets | 4 |
| generatorRate | 0 tx/s (0 = unlimited) |
| fault | none |
| kafka | apache/kafka:3.8.0 (KRaft), 1 broker, replication factor 1 |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |
| machine | Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz, 12 logical cores, 31 GB RAM visible to Linux (WSL2) |

## End-to-end pipeline

- **2,000,000 transactions** committed to the ledger, from generator start to the last commit in **44.1 s** = **45,329 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 6711 ms, p95 22047 ms, p99 22383 ms, max 22511 ms
- Ledger rows: 2,000,000 of 2,000,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":1001166,"DECLINE":706423,"REVIEW":292411}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 2,000,000 | 92,721 | 131,791 | hottest partition = 2.48x average |
| Enricher (Redis + Postgres) | 1 | 2,000,000 | 79,949 | 107,550 | cache hit ratio 0.977, 6 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 1 | 2,000,000 | 63,818 | 110,331 | at_least_once, 6 threads per instance |
| Ledger (Postgres batch insert) | 1 | 2,000,000 | 46,502 | 81,373 | 6 threads per instance |

Records per partition (generator): [266086,115354,115128,115799,118414,114994,412740,114339,116182,117537,269581,123846]

