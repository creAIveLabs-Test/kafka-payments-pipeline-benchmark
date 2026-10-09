# Run: ibm-all

| Setting | Value |
|---|---|
| transactions | 24386900 |
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

- **24,386,900 transactions** committed to the ledger, from generator start to the last commit in **364.8 s** = **66,847 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 54719 ms, p95 104511 ms, p99 116799 ms, max 121855 ms
- Ledger rows: 24,386,900 of 24,386,900 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":24383494,"DECLINE":1776,"REVIEW":1630}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 24,386,900 | 91,666 | 113,467 | hottest partition = 1.09x average |
| Enricher (Redis + Postgres) | 1 | 24,386,900 | 91,701 | 112,264 | cache hit ratio 0.999, 6 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 1 | 24,386,900 | 91,520 | 110,650 | at_least_once, 6 threads per instance |
| Ledger (Postgres batch insert) | 1 | 24,386,900 | 67,068 | 95,000 | 6 threads per instance |

Records per partition (generator): [2167574,1944436,2126376,1971009,1808071,1780035,2205035,2122929,1993080,1942011,2135528,2190816]

