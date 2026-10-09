# Run: scorer-exactly-once

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 12 |
| instancesPerStage | 1 |
| threadsPerInstance | 6 |
| scorerGuarantee | exactly_once_v2 |
| whaleShare | 0 |
| saltBuckets | 0 |
| generatorRate | 0 tx/s (0 = unlimited) |
| fault | none |
| kafka | apache/kafka:3.8.0 (KRaft), 1 broker, replication factor 1 |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |
| machine | Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz, 12 logical cores, 31 GB RAM visible to Linux (WSL2) |

## End-to-end pipeline

- **2,000,000 transactions** committed to the ledger, from generator start to the last commit in **39.0 s** = **51,337 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 10807 ms, p95 16143 ms, p99 16527 ms, max 16767 ms
- Ledger rows: 2,000,000 of 2,000,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":1399282,"DECLINE":191397,"REVIEW":409321}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 2,000,000 | 90,596 | 118,489 | hottest partition = 1.06x average |
| Enricher (Redis + Postgres) | 1 | 2,000,000 | 87,789 | 118,764 | cache hit ratio 0.977, 6 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 1 | 2,000,000 | 71,005 | 111,240 | exactly_once_v2, 6 threads per instance |
| Ledger (Postgres batch insert) | 1 | 2,000,000 | 52,719 | 75,817 | 6 threads per instance |

Records per partition (generator): [165541,164066,164759,166065,169540,163870,161701,163333,166769,167627,170259,176470]

