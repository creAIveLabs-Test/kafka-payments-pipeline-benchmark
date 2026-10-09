# Run: smoke

| Setting | Value |
|---|---|
| transactions | 200000 |
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

- **200,000 transactions** committed to the ledger, from generator start to the last commit in **12.8 s** = **15,643 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 1957 ms, p95 3105 ms, p99 3495 ms, max 3763 ms
- Ledger rows: 200,000 of 200,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":151466,"DECLINE":4083,"REVIEW":44451}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 200,000 | 21,756 | 31,279 | hottest partition = 1.06x average |
| Enricher (Redis + Postgres) | 1 | 200,000 | 21,582 | 30,420 | cache hit ratio 0.957, 6 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 1 | 200,000 | 18,513 | 26,255 | at_least_once, 6 threads per instance |
| Ledger (Postgres batch insert) | 1 | 200,000 | 18,265 | 19,036 | 6 threads per instance |

Records per partition (generator): [16445,16505,16458,16535,17042,16186,16101,16657,16767,16754,16939,17611]

