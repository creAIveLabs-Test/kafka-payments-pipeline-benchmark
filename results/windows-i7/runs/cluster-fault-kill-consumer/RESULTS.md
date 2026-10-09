# Run: cluster-fault-kill-consumer

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
| fault | kill-consumer |
| kafka | apache/kafka:3.8.0 (KRaft), 3 brokers, replication factor 3, min.insync.replicas 2 |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |
| machine | Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz, 12 logical cores, 31 GB RAM visible to Linux (WSL2) |

## End-to-end pipeline

- **2,000,000 transactions** committed to the ledger, from generator start to the last commit in **64.9 s** = **30,801 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 12727 ms, p95 26223 ms, p99 27551 ms, max 31007 ms
- Ledger rows: 2,000,000 of 2,000,000 expected; duplicates skipped by ON CONFLICT: 0; decisions: {"APPROVE":1399282,"DECLINE":191397,"REVIEW":409321}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 2,000,000 | 49,587 | 87,595 | hottest partition = 1.12x average |
| Enricher (Redis + Postgres) | 2 | 1,198,277 | 22,856 | 38,800 | cache hit ratio 0.978, 4 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 2 | 2,000,000 | 37,926 | 75,799 | at_least_once, 4 threads per instance |
| Ledger (Postgres batch insert) | 2 | 1,570,616 | 25,021 | 96,755 | 4 threads per instance |

Records per partition (generator): [83424,83120,82704,83842,85177,83732,79401,81162,82898,86392,87604,93486,82117,80946,82055,82223,84363,80138,82300,82171,83871,81235,82655,82984]

## Fault injection

| | |
|---|---|
| Fault | kill-consumer (enricher-1 and ledger-1 killed with SIGKILL (no clean shutdown, no offset commit)) |
| Injected at | 873,022 ledger rows committed |
| Restarted after | 10 s |
| Baseline throughput before fault (ledger rows/sec) | 25,000 |
| Lowest throughput after fault | 0 rows/sec |
| Seconds with zero throughput | 1 |
| Recovered after | 10 s (throughput back above 50% of baseline and stayed there) |
| Data check | 2,000,000 rows of 2,000,000 expected, 0 duplicates absorbed |

