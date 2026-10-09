# kafka-payments-pipeline-benchmark

An end-to-end, reproducible benchmark of a real-time payments pipeline on Kafka, Kafka Streams, Redis and PostgreSQL, run on **2,000,000 synthetic card transactions**, plus a LinkedIn-style raw Kafka throughput test for comparison.

Every number in this README was measured by the code in this repository, on two laptops: an Apple M2 and a Windows i7 workstation. Re-run it and you get your own numbers.

## What it measures

```
                 transactions          tx.enriched            tx.decisions
 Generator ───► [ Kafka, 12 part ] ──► [ Kafka, 12 part ] ──► [ Kafka, 12 part ] ──► Ledger ──► PostgreSQL
 2M synthetic     key = account_id          ▲                       ▲            batch insert,     ledger_entries
 transactions                               │                       │            ON CONFLICT       (PK tx_id)
                                       Enricher                 Scorer           DO NOTHING
                                  Redis MGET per batch     Kafka Streams:
                                  cache-aside on Postgres  5-min velocity per account
                                  jittered TTL on refill   in RocksDB state store,
                                                           rules -> APPROVE/DECLINE/REVIEW
```

| Stage | What it does | Key techniques |
|---|---|---|
| **Generator** | Produces N transactions keyed by account | Idempotent producer, `acks=all`, lz4, 256 KB batches; power-law account and merchant popularity; optional "whale" account and salted keys |
| **Enricher** | Adds account tier/risk/country and merchant name/category/country | One Redis `MGET` per poll batch, Postgres `= ANY(?)` for misses, `SETEX` back with jittered TTL; output flushed before input offsets commit |
| **Scorer** | Velocity (transactions in the last 5 minutes per key) + rules | Kafka Streams Processor API, RocksDB state store with caching, changelog-backed; at-least-once or exactly-once v2 |
| **Ledger** | Writes final decisions to Postgres | One `INSERT ... SELECT FROM unnest(arrays)` per batch, `ON CONFLICT (tx_id) DO NOTHING`, DB commit before offset commit |

**How it's measured**
- **End-to-end throughput** = transactions ÷ (last ledger commit − generator start).
- **Per-stage throughput** = records ÷ (last − first record in that stage), plus the best 5-second window ("peak").
- **End-to-end latency** = time from the generator creating a transaction to the ledger committing it to Postgres (HdrHistogram).

## Results

Two machines, same code, same 2,000,000 seeded transactions:

| Machine | CPU / RAM | Setup |
|---|---|---|
| **Mac M2** | Apple M2, 8 cores, 16 GB | macOS, Docker Desktop |
| **Windows i7** | Intel Core i7-10750H, 12 logical cores, 31 GB visible to Linux | Windows 11, WSL2 (Ubuntu), Docker Desktop |

Everything (Kafka, Redis, Postgres and the Java stages) runs on the same machine, so all stages compete for the same cores. Kafka 3.8 (KRaft), Redis 7, PostgreSQL 16. The comparison is in [`results/SUMMARY.md`](results/SUMMARY.md); per-run details are in [`results/mac-m2/`](results/mac-m2) and [`results/windows-i7/`](results/windows-i7).

### End-to-end pipeline, 2M transactions, 1 broker

| Run | Mac M2 tx/sec | Windows i7 tx/sec | Mac p50 / p99 | Windows p50 / p99 | Hottest partition |
|---|---|---|---|---|---|
| **baseline (12 partitions)** | 33,651 | **56,638** | 26.2 s* / 39.9 s* | 9.9 s* / 11.9 s* | 1.06x |
| **fixed rate 20,000 tx/s** | 19,952 | 19,990 | **159 ms** / 3.5 s | **92 ms** / 2.9 s | 1.06x |
| single partition | 29,471 | 26,838 | 34.4 s* / 54.2 s* | 34.9 s* / 65.6 s* | 1.00x |
| whale account = 30% of traffic | 28,044 | 40,399 | 21.2 s* / 52.6 s* | 6.2 s* / 26.8 s* | **4.30x** |
| whale 30%, salted over 4 keys | 32,813 | 45,329 | 19.8 s* / 37.5 s* | 6.7 s* / 22.4 s* | **2.48x** |
| scorer exactly-once v2 | 32,247 | 51,337 | 26.0 s* / 43.4 s* | 10.8 s* / 16.5 s* | 1.06x |

\* In the capacity runs the generator produces as fast as it can, faster than the pipeline drains, so latency there measures **queueing backlog**, not processing time. The fixed-rate run is the one that shows processing latency.

All runs on both machines: 2,000,000 of 2,000,000 ledger rows, 0 duplicates.

Per stage on the Windows baseline: enricher 81,739/s, scorer 79,434/s, **ledger 58,770/s** (Redis cache hit ratio 0.976). Each transaction is written to three topics and read three times, so at 56.6K transactions/sec Kafka handles about **170K records written and 170K read per second** across the pipeline.

### 3-broker cluster and fault injection (Windows i7)

Kafka KRaft, 3 brokers, replication factor 3, `min.insync.replicas=2`, 24 partitions, 2M transactions per run.

| Run | Instances per stage | End-to-end tx/sec | p50 / p99 | Rows / expected | Duplicates absorbed |
|---|---|---|---|---|---|
| 1 instance | 1 | 37,943 | 22.9 s* / 33.0 s* | 2,000,000 / 2,000,000 | 0 |
| 2 instances | 2 | 37,491 | 12.0 s* / 15.4 s* | 2,000,000 / 2,000,000 | 0 |
| 4 instances | 4 | 25,953 | 5.0 s* / 16.0 s* | 2,000,000 / 2,000,000 | 0 |

| Fault (injected at about 42% of the run) | Before | Lowest after | Seconds at zero | Recovered in | Data check |
|---|---|---|---|---|---|
| `kill -9` an enricher and a ledger process | 25,000 rows/s | 0 | 1 | **10 s** | 2,000,000 / 2,000,000, 0 duplicates |
| `docker kill` 1 of 3 brokers | 32,674 rows/s | 0 | 7 | **8 s** | 2,000,000 / 2,000,000, 0 duplicates |
| add one instance to every stage (rebalance) | 30,000 rows/s | 10,000 | 0 | **0 s** | 2,000,000 / 2,000,000, **15,306 duplicates absorbed** |

"Recovered" = throughput back above 50% of the pre-fault rate and staying there. Killed processes and the broker restart after 10 s.

### Raw Kafka, LinkedIn-style

Same method as LinkedIn's 2014 benchmark ([Jay Kreps, "Benchmarking Apache Kafka: 2 Million Writes Per Second (On Three Cheap Machines)"](https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines)): 100-byte records, Kafka's own `kafka-producer-perf-test` / `kafka-consumer-perf-test`, no application logic. 2M records per producer.

| Test | Mac M2, 1 broker | Windows i7, 3 brokers, RF 3 | LinkedIn 2014 (3 brokers, 6 machines) |
|---|---|---|---|
| 1 producer, no replication | **935,891 records/s** (89.2 MB/s) | - | 821,557 records/s |
| 1 producer, 3x async replication (acks=1) | - | **790,826 records/s** (75.4 MB/s) | 786,980 records/s |
| 1 producer, 3x sync replication (acks=all) | 977,039 (no replicas to wait for) | **836,820 records/s** (79.8 MB/s) | 421,823 records/s |
| 3 producers at once | not tested | **1,496,167 records/s** (142.7 MB/s) | **2,024,032 records/s** |
| 1 consumer, 6 partitions | 329,489 records/s | 798,722 records/s | 940,521 records/s |

This is not a like-for-like comparison: LinkedIn used 2014 hardware and real networks between machines, while here all brokers share one laptop and replicate over localhost, so `acks=all` costs almost nothing. It shows the order of magnitude Kafka itself handles, not a record.

## What the results show

1. **Kafka is not the bottleneck; the work per record is.** Raw Kafka moves about 800K-935K records/s on these laptops; the full pipeline (JSON parsing, Redis lookups, RocksDB state, Postgres writes) does 34K-57K transactions/s.
2. **On the Windows i7 the ledger is the limit.** Enricher and scorer average about 80K/s, the Postgres ledger 59K/s, and end to end follows the slowest stage. That is the stage to scale next (bigger batches, a partitioned ledger table, Postgres on its own machine).
3. **The "50K transactions/sec" target is met on the Windows i7: 56,638 tx/s sustained end to end over 2M transactions,** with 0 lost and 0 duplicated. On the 8-core Mac it is 33,651.
4. **Partitions matter once there are spare cores.** On the Windows i7, 12 partitions ran 2.1x faster than 1 partition (56.6K vs 26.8K). On the 8-core Mac the gain was small (33.7K vs 29.5K) because the run was already CPU-bound.
5. **Hot keys are real, and salting fixes most of the damage.** Sending 30% of traffic from one account made one partition carry 4.3x the average. Salting it over 4 keys cut that to 2.48x and raised throughput +12% (Windows) and +17% (Mac). The trade-off: that account's events lose single-partition ordering, and velocity is counted per salt bucket.
6. **Exactly-once costs 4% (Mac) to 9% (Windows)** of throughput, from transactional commits every 200 ms.
7. **Latency depends on running below capacity.** At a steady 20K tx/s, p50 is 92 ms (Windows) / 159 ms (Mac). p99 of about 3 s points to periodic stalls (Kafka Streams cache flushes on commit, Postgres checkpoints, GC on a shared CPU).
8. **Failures cost seconds, never data.** Killing consumers or a broker mid-run stopped writes for 1-7 s and recovered in 8-10 s, with every one of the 2M transactions in the ledger exactly once. A rebalance replayed 15,306 records; `ON CONFLICT (tx_id) DO NOTHING` absorbed all of them.
9. **Replication factor 3 costs about a third of throughput on one machine** (56.6K with 1 broker vs 37.9K with 3 brokers): every write is stored three times on the same disk and CPU. In return a broker crash costs 8 seconds, not data.
10. **Adding instances on one machine cut latency but not throughput.** Going from 1 to 4 instances per stage lowered p50 from 22.9 s to 5.0 s (more consumers drain the backlog in parallel) but throughput fell from 37.9K to 26.0K, because 4 x 3 stages x 6 threads plus 3 brokers compete for 12 cores and one Postgres. Horizontal scaling needs more machines, not more processes on one.

## More documentation

- [docs/DATA.md](docs/DATA.md): where the data comes from (synthetic, seeded), record format and sizes, skew options
- [docs/HARDWARE.md](docs/HARDWARE.md): what a 100-byte record means, what machine you need, machine types for production, how to reach 50K+ sustained
- [docs/WINDOWS.md](docs/WINDOWS.md): running everything on Windows with WSL2 and Docker Desktop
- [docs/LESSONS.md](docs/LESSONS.md): real problems hit while building this (disk filling up, crashed consumers, rebalance errors) and how they were fixed

## Run it yourself

Requirements: Docker, Java 17+, Python 3, about 4 GB free disk (10 GB for the cluster experiments). macOS, Linux or Windows via WSL2 ([docs/WINDOWS.md](docs/WINDOWS.md)).

```bash
./mvnw -q package -DskipTests              # builds target/bench.jar
NAME=my-run N=2000000 ./scripts/run.sh     # one end-to-end run (prints RESULTS.md)
N=2000000 ./scripts/raw-kafka.sh           # LinkedIn-style raw Kafka test
./scripts/experiments.sh                   # every single-broker run in the table above + results/SUMMARY.md
./scripts/experiments-distributed.sh       # 3-broker cluster: scaling, crashes, rebalance, replication cost
docker compose down -v                     # remove containers and data
```

`run.sh` options (environment variables):

| Variable | Default | Meaning |
|---|---|---|
| `N` | 2000000 | transactions |
| `PARTITIONS` | 12 | partitions per topic |
| `THREADS` | 6 | consumer threads per stage |
| `RATE` | 0 | generator tx/s (0 = as fast as possible) |
| `GUARANTEE` | at_least_once | scorer: `at_least_once` or `exactly_once_v2` |
| `WHALE` | 0 | share of traffic from one huge account |
| `SALT` | 0 | spread the whale account over N keys |
| `INSTANCES` | 1 | processes per stage (enricher, scorer, ledger) |
| `CLUSTER` | 0 | 1 = 3 brokers, replication factor 3 |
| `FAULT` | none | `kill-consumer`, `kill-broker` or `scale-out` |
| `FAULT_AT` | 0.4 | inject the fault when this share of N is in the ledger |
| `RESTART_AFTER` | 10 | seconds before a killed process or broker comes back |
| `KEEP_DATA` | 0 | 1 = keep topics and ledger rows after the run |

The script refuses to start with less than 3 GB of free disk, and deletes each run's topics and ledger rows after saving results. A run ends when the ledger holds all N rows (it writes a `DONE` signal every stage watches); if the ledger stops growing for 3 minutes the run is marked `STALLED` instead of hanging.

## Reliability design (what keeps 2M = 2M)

- **No loss:** every stage writes its output (or commits to Postgres) before committing its input offsets.
- **No duplicates in the ledger:** primary key on `tx_id` plus `ON CONFLICT DO NOTHING`. A replayed batch changes nothing.
- **Ordering:** keyed by `account_id`, so one account's events stay on one partition and are processed in order by one task.
- **Producer:** idempotent, `acks=all`.
- **Consumer rebalancing:** cooperative sticky assignor, so a rebalance only pauses the partitions that move.
- **Failure detection:** consumer session timeout 10 s (Kafka's default is 45 s), so a crashed consumer's partitions move to a healthy one quickly.

## Limitations (read before quoting numbers)

- One machine per run: the brokers, Redis, Postgres and all stages share the same cores, so a real deployment spreads them out and scales differently. The 3-broker cluster replicates over localhost, not a real network.
- The single-broker runs use replication factor 1; production uses at least 3 brokers with `min.insync.replicas=2` (measured separately in the cluster runs).
- Synthetic data and simple rules: real fraud scoring does more work per event.
- Latency in the capacity runs is backlog. Use the fixed-rate run for processing latency.

## Project layout

```
src/main/java/bench/   Generator, Enricher, Scorer (Kafka Streams), Ledger, Seed, Report, Stats
scripts/run.sh         one end-to-end run
scripts/raw-kafka.sh   LinkedIn-style raw Kafka test
scripts/experiments.sh every experiment + summary
sql/schema.sql         accounts, merchants, ledger_entries
results/SUMMARY.md     both machines compared
results/mac-m2/        Apple M2 runs, raw Kafka results, 200K cluster validation
results/windows-i7/    Windows i7 runs, 3-broker cluster and fault runs, raw Kafka cluster results
```

## License

MIT. See [LICENSE](LICENSE).
