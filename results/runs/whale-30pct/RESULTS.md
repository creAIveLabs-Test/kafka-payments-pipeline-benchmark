# Run: whale-30pct

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 12 |
| threadsPerStage | 6 |
| scorerGuarantee | at_least_once |
| whaleShare | 0.3 |
| saltBuckets | 0 |
| generatorRate | 0 tx/s (0 = unlimited) |
| brokers | 1 (replication factor 1) |
| machine | Apple M2, 8 cores, 16 GB RAM |
| kafka | apache/kafka:3.8.0 (KRaft) |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |

## End-to-end pipeline

- **2,000,000 transactions** from generator start to the last ledger commit in **71.3 s** = **28,044 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 21215 ms, p95 50047 ms, p99 52639 ms, max 53119 ms
- Ledger rows: 2,000,000 (duplicates skipped by ON CONFLICT: 0); decisions: {"APPROVE":1001105,"DECLINE":706498,"REVIEW":292397}

## Per stage

| Stage | Records | Avg records/sec | Peak 5 s records/sec | Notes |
|---|---|---|---|---|
| Generator (produce) | 2,000,000 | 49,127 | 61,043 | hottest partition = 4.3x average |
| Enricher (Redis + Postgres) | 2,000,000 | 40,318 | 71,192 | cache hit ratio 0.976, 6 threads |
| Scorer (Kafka Streams + RocksDB) | 2,000,000 | 28,725 | 50,285 | at_least_once, 6 threads |
| Ledger (Postgres batch insert) | 2,000,000 | 28,799 | 47,025 | 6 threads |

Records per partition (generator): [115796,115354,115128,115799,118414,114994,112557,114339,717176,117537,119060,123846]

