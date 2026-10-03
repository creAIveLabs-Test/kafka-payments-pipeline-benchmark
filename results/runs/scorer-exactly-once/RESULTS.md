# Run: scorer-exactly-once

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 12 |
| threadsPerStage | 6 |
| scorerGuarantee | exactly_once_v2 |
| whaleShare | 0 |
| saltBuckets | 0 |
| generatorRate | 0 tx/s (0 = unlimited) |
| brokers | 1 (replication factor 1) |
| machine | Apple M2, 8 cores, 16 GB RAM |
| kafka | apache/kafka:3.8.0 (KRaft) |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |

## End-to-end pipeline

- **2,000,000 transactions** from generator start to the last ledger commit in **62.0 s** = **32,247 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 26047 ms, p95 42751 ms, p99 43359 ms, max 43871 ms
- Ledger rows: 2,000,000 (duplicates skipped by ON CONFLICT: 0); decisions: {"APPROVE":1399282,"DECLINE":191397,"REVIEW":409321}

## Per stage

| Stage | Records | Avg records/sec | Peak 5 s records/sec | Notes |
|---|---|---|---|---|
| Generator (produce) | 2,000,000 | 43,121 | 57,637 | hottest partition = 1.06x average |
| Enricher (Redis + Postgres) | 2,000,000 | 35,922 | 47,204 | cache hit ratio 0.976, 6 threads |
| Scorer (Kafka Streams + RocksDB) | 2,000,000 | 35,706 | 45,004 | exactly_once_v2, 6 threads |
| Ledger (Postgres batch insert) | 2,000,000 | 33,403 | 47,000 | 6 threads |

Records per partition (generator): [165541,164066,164759,166065,169540,163870,161701,163333,166769,167627,170259,176470]

