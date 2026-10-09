# Run: baseline-2M-12p

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 12 |
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

- **2,000,000 transactions** from generator start to the last ledger commit in **59.4 s** = **33,651 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 26175 ms, p95 39167 ms, p99 39871 ms, max 40255 ms
- Ledger rows: 2,000,000 (duplicates skipped by ON CONFLICT: 0); decisions: {"APPROVE":1399282,"DECLINE":191397,"REVIEW":409321}

## Per stage

| Stage | Records | Avg records/sec | Peak 5 s records/sec | Notes |
|---|---|---|---|---|
| Generator (produce) | 2,000,000 | 43,528 | 53,941 | hottest partition = 1.06x average |
| Enricher (Redis + Postgres) | 2,000,000 | 36,828 | 47,321 | cache hit ratio 0.976, 6 threads |
| Scorer (Kafka Streams + RocksDB) | 2,000,000 | 36,658 | 47,002 | at_least_once, 6 threads |
| Ledger (Postgres batch insert) | 2,000,000 | 34,751 | 42,000 | 6 threads |

Records per partition (generator): [165541,164066,164759,166065,169540,163870,161701,163333,166769,167627,170259,176470]

