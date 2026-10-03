# Run: test-kill-consumer

| Setting | Value |
|---|---|
| transactions | 200000 |
| partitions | 12 |
| instancesPerStage | 2 |
| threadsPerInstance | 3 |
| scorerGuarantee | at_least_once |
| whaleShare | 0 |
| saltBuckets | 0 |
| generatorRate | 0 tx/s (0 = unlimited) |
| fault | kill-consumer |
| kafka | apache/kafka:3.8.0 (KRaft), 3 brokers, replication factor 3, min.insync.replicas 2 |
| redis | redis:7-alpine |
| postgres | postgres:16-alpine |
| machine | Apple M2, 8 logical cores, 16 GB RAM, macOS |

## End-to-end pipeline

- **200,000 transactions** committed to the ledger, from generator start to the last commit in **56.1 s** = **3,568 transactions/sec end to end**
- End-to-end latency (created to committed in Postgres): p50 20799 ms, p95 38847 ms, p99 39359 ms, max 39999 ms
- Ledger rows: 200,000 of 200,000 expected; duplicates skipped by ON CONFLICT: 2,861; decisions: {"APPROVE":151455,"DECLINE":4097,"REVIEW":44448}

## Per stage (all instances combined)

| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |
|---|---|---|---|---|---|
| Generator | 1 | 200,000 | 9,405 | 15,286 | hottest partition = 1.06x average |
| Enricher (Redis + Postgres) | 2 | 132,012 | 2,773 | 5,200 | cache hit ratio 0.955, 3 threads per instance |
| Scorer (Kafka Streams + RocksDB) | 2 | 202,000 | 4,198 | 11,782 | at_least_once, 3 threads per instance |
| Ledger (Postgres batch insert) | 2 | 151,540 | 3,182 | 6,217 | 3 threads per instance |

Records per partition (generator): [16445,16505,16458,16535,17042,16186,16101,16657,16767,16754,16939,17611]

## Fault injection

| | |
|---|---|
| Fault | kill-consumer (enricher-1 and ledger-1 killed with SIGKILL (no clean shutdown, no offset commit)) |
| Injected at | 96,021 ledger rows committed |
| Restarted after | 10 s |
| Baseline throughput before fault (ledger rows/sec) | 4,924 |
| Lowest throughput after fault | 0 rows/sec |
| Seconds with zero throughput | 10 |
| Recovered after | 19 s (throughput back above 50% of baseline and stayed there) |
| Data check | 200,000 rows of 200,000 expected, 2,861 duplicates absorbed |

