# payments-stream-benchmark

An end-to-end, reproducible benchmark of a real-time payments pipeline on Kafka, Kafka Streams, Redis and PostgreSQL, run on **2,000,000 synthetic card transactions**, plus a LinkedIn-style raw Kafka throughput test for comparison.

Every number in this README was measured by the code in this repository on one laptop. Re-run it and you get your own numbers.

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

Machine: **Apple M2, 8 cores, 16 GB RAM**. Everything (Kafka, Redis, Postgres and four JVMs) runs on the same laptop, so all stages compete for the same 8 cores. Kafka 3.8 (KRaft, 1 broker, replication factor 1), Redis 7, PostgreSQL 16. Per-run details are in [`results/runs/`](results/runs) and the comparison is in [`results/SUMMARY.md`](results/SUMMARY.md).

### End-to-end pipeline, 2M transactions

| Run | End-to-end tx/sec | Enricher avg / peak | Scorer avg / peak | Ledger avg / peak | p50 latency | p99 latency | Hottest partition |
|---|---|---|---|---|---|---|---|
| **baseline (12 partitions)** | **33,651** | 36,828 / 47,321 | 36,658 / 47,002 | 34,751 / 42,000 | 26.2 s* | 39.9 s* | 1.06x |
| **fixed rate 20,000 tx/s** | 19,952 | 20,256 / 21,910 | 20,317 / 26,180 | 20,386 / 26,432 | **159 ms** | 3.5 s | 1.06x |
| single partition | 29,471 | 42,299 / 55,142 | 36,198 / 49,886 | 30,302 / 50,389 | 34.4 s* | 54.2 s* | 1.00x |
| whale account = 30% of traffic | 28,044 | 40,318 / 71,192 | 28,725 / 50,285 | 28,799 / 47,025 | 21.2 s* | 52.6 s* | **4.30x** |
| whale 30%, salted over 4 keys | **32,813** | 37,086 / 54,000 | 34,562 / 53,860 | 34,190 / 54,839 | 19.8 s* | 37.5 s* | **2.48x** |
| scorer exactly-once v2 | 32,247 | 35,922 / 47,204 | 35,706 / 45,004 | 33,403 / 47,000 | 26.0 s* | 43.4 s* | 1.06x |

\* In the capacity runs the generator produces as fast as it can (about 38-60K/s), faster than the pipeline drains, so latency there measures **queueing backlog**, not processing time. The fixed-rate run is the one that shows processing latency.

All runs: 2,000,000 ledger rows, 0 duplicates.

Each transaction is written to three topics and read three times, plus state-store changelog writes. So at 33.6K transactions/sec, Kafka is handling **about 100K records written and 100K read per second** across the pipeline.

### Raw Kafka, LinkedIn-style

Same method as LinkedIn's 2014 benchmark ([Jay Kreps, "Benchmarking Apache Kafka: 2 Million Writes Per Second (On Three Cheap Machines)"](https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines)): 100-byte records, Kafka's own `kafka-producer-perf-test` / `kafka-consumer-perf-test`, no application logic. 2M records per test.

| Test | This laptop | LinkedIn 2014 (3 brokers, 6 machines) |
|---|---|---|
| 1 producer, no replication (acks=1, 6 partitions) | **935,891 records/s (89.2 MB/s)** | 821,557 records/s (78.3 MB/s) |
| 1 producer, acks=all + idempotence | 977,039 records/s (93.2 MB/s) | 3x async replication: 786,980 records/s |
| 1 producer, lz4 | 828,157 records/s (79.0 MB/s) | - |
| 1 producer, 1 partition | 866,175 records/s (82.6 MB/s) | - |
| 1 consumer, 6 partitions | 329,489 records/s (31.4 MB/s) | 940,521 records/s (89.7 MB/s) |
| 3 producers on 3 machines | not tested (one machine) | **2,024,032 records/s** (the "2 million writes/sec") |

With one broker, `acks=all` has no replicas to wait for, so it costs nothing here; on a real 3-broker cluster it does. The consumer number includes group-join time in a 2M-record test.

## What the results show

1. **Kafka is not the bottleneck; the work per record is.** Raw Kafka moves about 935K records/s on this laptop, while the full pipeline (JSON parsing, Redis lookups, RocksDB state, Postgres writes) does about 34K transactions/s. Throughput is set by per-record processing and shared CPU, not by the broker.
2. **On one machine, more partitions barely help (29.5K → 33.7K).** All stages share 8 cores, so the run is CPU-bound. Partitions are what let you add machines and consumers; their payoff shows up when you scale out.
3. **Hot keys are real, and salting fixes most of the damage.** Sending 30% of traffic from one account made one partition carry 4.3x the average and cut throughput to 28.0K/s with the worst tail. Salting that account over 4 keys brought the hottest partition to 2.48x and throughput back to 32.8K/s (+17%). The trade-off: that account's events are no longer in one order, and velocity is counted per salt bucket. (4 salted keys hashed onto 3 distinct partitions, which is why the hottest is still 2.48x.)
4. **Exactly-once costs about 4% here (33.7K → 32.2K)**, from transactional commits every 200 ms.
5. **Latency depends on running below capacity.** At a steady 20K tx/s (about 60% of capacity), p50 is 159 ms. p99 is 3.5 s, which points to periodic stalls (Kafka Streams cache flushes on commit, Postgres checkpoints, GC on a shared CPU). Investigating it is the obvious next step.
6. **Peaks above 50K/s per stage happen here** (enricher 71K, scorer 54K, ledger 55K in some runs), but **sustained end-to-end throughput on one laptop is about 34K tx/s.** Sustaining 50K+ end to end needs more hardware: separate broker machines, more consumer instances, Postgres on its own machine.

## More documentation

- [docs/DATA.md](docs/DATA.md): where the data comes from (synthetic, seeded), record format and sizes, skew options
- [docs/HARDWARE.md](docs/HARDWARE.md): what a 100-byte record means, what machine you need, machine types for production, how to reach 50K+ sustained

## Run it yourself

Requirements: Docker, Java 17+, about 4 GB free disk.

```bash
./mvnw -q package -DskipTests              # builds target/bench.jar
NAME=my-run N=2000000 ./scripts/run.sh     # one end-to-end run (prints RESULTS.md)
N=2000000 ./scripts/raw-kafka.sh           # LinkedIn-style raw Kafka test
./scripts/experiments.sh                   # every run in the table above + results/SUMMARY.md
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
| `KEEP_DATA` | 0 | 1 = keep topics and ledger rows after the run |

The script refuses to start with less than 3 GB of free disk, and deletes each run's topics and ledger rows after saving results.

## Reliability design (what keeps 2M = 2M)

- **No loss:** every stage writes its output (or commits to Postgres) before committing its input offsets.
- **No duplicates in the ledger:** primary key on `tx_id` plus `ON CONFLICT DO NOTHING`. A replayed batch changes nothing.
- **Ordering:** keyed by `account_id`, so one account's events stay on one partition and are processed in order by one task.
- **Producer:** idempotent, `acks=all`.
- **Consumer rebalancing:** cooperative sticky assignor, so a rebalance only pauses the partitions that move.

## Limitations (read before quoting numbers)

- One laptop: the broker, Redis, Postgres and all stages share 8 cores and 16 GB, so a real deployment spreads them out and scales differently.
- One broker, replication factor 1: production uses at least 3 brokers with `min.insync.replicas=2`.
- Synthetic data and simple rules: real fraud scoring does more work per event.
- Latency in the capacity runs is backlog. Use the fixed-rate run for processing latency.

## Project layout

```
src/main/java/bench/   Generator, Enricher, Scorer (Kafka Streams), Ledger, Seed, Report, Stats
scripts/run.sh         one end-to-end run
scripts/raw-kafka.sh   LinkedIn-style raw Kafka test
scripts/experiments.sh every experiment + summary
sql/schema.sql         accounts, merchants, ledger_entries
results/               per-run JSON and RESULTS.md, raw Kafka results, SUMMARY.md
```
