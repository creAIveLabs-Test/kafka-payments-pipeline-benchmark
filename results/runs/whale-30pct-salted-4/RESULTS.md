# Run: whale-30pct-salted-4

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 12 |
| threadsPerStage | 6 |
| scorerGuarantee | at_least_once |
| whaleShare | 0.3 |
| saltBuckets | 4 |
| generatorRate | 0 tx/s (0 = unlimited) |
| brokers | 1 (replication factor 1) |
| machine | Apple M2, 8 cores, 16 GB RAM |
| kafka | apache/kafka:3.8.0 (KRaft) |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |

## End-to-end pipeline

- **2,000,000 transactions** from generator start to the last ledger commit in **61.0 s** = **32,813 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 19839 ms, p95 37023 ms, p99 37471 ms, max 37599 ms
- Ledger rows: 2,000,000 (duplicates skipped by ON CONFLICT: 0); decisions: {"APPROVE":1001166,"DECLINE":706423,"REVIEW":292411}

## Per stage

| Stage | Records | Avg records/sec | Peak 5 s records/sec | Notes |
|---|---|---|---|---|
| Generator (produce) | 2,000,000 | 41,958 | 62,913 | hottest partition = 2.48x average |
| Enricher (Redis + Postgres) | 2,000,000 | 37,086 | 54,000 | cache hit ratio 0.976, 6 threads |
| Scorer (Kafka Streams + RocksDB) | 2,000,000 | 34,562 | 53,860 | at_least_once, 6 threads |
| Ledger (Postgres batch insert) | 2,000,000 | 34,190 | 54,839 | 6 threads |

Records per partition (generator): [266086,115354,115128,115799,118414,114994,412740,114339,116182,117537,269581,123846]

