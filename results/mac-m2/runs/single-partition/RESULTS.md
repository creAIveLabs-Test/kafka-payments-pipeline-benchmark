# Run: single-partition

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 1 |
| threadsPerStage | 6 |
| scorerGuarantee | at_least_once |
| whaleShare | 0 |
| saltBuckets | 0 |
| generatorRate | 0 tx/s (0 = unlimited) |
| brokers | 1 (replication factor 1) |
| machine | Apple M2, 8 cores, 16 GB RAM |
| kafka | apache/kafka:3.8.0 (KRaft) |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |

## End-to-end pipeline

- **2,000,000 transactions** from generator start to the last ledger commit in **67.9 s** = **29,471 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 34367 ms, p95 53183 ms, p99 54207 ms, max 54463 ms
- Ledger rows: 2,000,000 (duplicates skipped by ON CONFLICT: 0); decisions: {"APPROVE":1399282,"DECLINE":191397,"REVIEW":409321}

## Per stage

| Stage | Records | Avg records/sec | Peak 5 s records/sec | Notes |
|---|---|---|---|---|
| Generator (produce) | 2,000,000 | 155,678 | 125,563 | hottest partition = 1.0x average |
| Enricher (Redis + Postgres) | 2,000,000 | 42,299 | 55,142 | cache hit ratio 0.977, 6 threads |
| Scorer (Kafka Streams + RocksDB) | 2,000,000 | 36,198 | 49,886 | at_least_once, 6 threads |
| Ledger (Postgres batch insert) | 2,000,000 | 30,302 | 50,389 | 6 threads |

Records per partition (generator): [2000000]

