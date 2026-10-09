# Run: cluster-fault-scale-out

| Setting | Value |
|---|---|
| transactions | 2000000 |
| partitions | 24 |
| instancesPerStage | 2 |
| threadsPerInstance | 4 |
| scorerGuarantee | at_least_once |
| whaleShare | 0 |
| saltBuckets | 0 |
| generatorRate | 0 tx/s (0 = unlimited) |
| fault | scale-out |
| kafka | apache/kafka:3.8.0 (KRaft), 3 brokers, replication factor 3, min.insync.replicas 2 |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |
| machine | Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz, 12 logical cores, 31 GB RAM visible to Linux (WSL2) |

## End-to-end pipeline

- **2,000,000 transactions** committed to the ledger, from generator start to the last commit in **61.3 s** = **32,602 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 12431 ms, p95 16607 ms, p99 18383 ms, max 19839 ms
- Ledger rows: 2,000,000 of 2,000,000 expected; duplicates skipped by ON CONFLICT: 15,306; decisions: {"APPROVE":1399282,"DECLINE":191397,"REVIEW":409321}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 2,000,000 | 41,888 | 89,577 | hottest partition = 1.12x average |
| Enricher (Redis + Postgres) | 3 | 2,010,306 | 35,862 | 62,453 | cache hit ratio 1.0, 4 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 3 | 2,010,306 | 36,007 | 67,090 | at_least_once, 4 threads per instance |
| Ledger (Postgres batch insert) | 3 | 2,015,306 | 34,156 | 95,940 | 4 threads per instance |

Records per partition (generator): [83424,83120,82704,83842,85177,83732,79401,81162,82898,86392,87604,93486,82117,80946,82055,82223,84363,80138,82300,82171,83871,81235,82655,82984]

## Fault injection

| | |
|---|---|
| Fault | scale-out (one more instance of every stage joined mid-run (consumer group rebalance)) |
| Injected at | 846,360 ledger rows committed |
| Restarted after | 10 s |
| Baseline throughput before fault (ledger rows/sec) | 30,000 |
| Lowest throughput after fault | 10,000 rows/sec |
| Seconds with zero throughput | 0 |
| Recovered after | 0 s (throughput back above 50% of baseline and stayed there) |
| Data check | 2,000,000 rows of 2,000,000 expected, 15,306 duplicates absorbed |

