# Records, machines and scaling

## What a "100-byte record" means

In LinkedIn's benchmark, and in `scripts/raw-kafka.sh`, every Kafka message is **exactly 100 bytes of filler**: no JSON, no business logic. It's a deliberately small message, so the test measures **how many messages per second Kafka can move** (per-message overhead), not network bandwidth.

- 2,000,000 records x 100 bytes = 200 MB per test.
- Throughput in MB/s = records/s x record size. 935,891 records/s x 100 B ≈ 89 MB/s.
- Larger records mean fewer records/s but more MB/s. The pipeline's real transactions are 180-440 bytes of JSON, so record counts are naturally lower.

The raw test answers "how fast is Kafka itself?" The pipeline test answers "how fast is my whole system, with real work per record?"

## What you need to run this project

| | Minimum | Used for the published results |
|---|---|---|
| CPU | 4 cores | Apple M2, 8 cores |
| RAM | 8 GB (Docker gets about 4 GB) | 16 GB |
| Disk free | 4 GB | 4-8 GB (the run script refuses below 3 GB) |
| Software | Docker, Java 17+ | Docker Desktop 29, Java 23 (compiled for 17) |

On one machine, throughput is limited by **CPU cores**, because Kafka, Redis, Postgres and four Java processes share them. A machine with more cores gives higher numbers; results are only comparable on the same hardware.

## The machines LinkedIn used (2014)

Six machines, each with an Intel Xeon 2.5 GHz six-core CPU, six 7200 RPM SATA disks, 32 GB RAM and 1 Gb Ethernet. Three ran Kafka; the others ran ZooKeeper and the producer/consumer clients. Their 2 million writes/sec came from **three producers on three separate machines** writing to the three-broker cluster at the same time.

## Machine types for a real deployment

Each component needs a different kind of machine. Typical choices on AWS (Google Cloud and Azure have equivalents):

| Component | What limits it | Machine type | Examples |
|---|---|---|---|
| Kafka brokers | Disk throughput and network | Storage- or general-purpose, fast local or provisioned disk | `m6i` / `i4i` instances, or managed Amazon MSK |
| Stream processors (enricher, scorer) | CPU | Compute-optimized | `c6i` / `c7g` instances on EKS |
| Redis | Memory | Memory-optimized | ElastiCache `r6g` / `r7g` |
| PostgreSQL ledger | Disk IOPS and CPU for writes | Memory-optimized database instance with provisioned IOPS | RDS `r6g` with gp3 / io2 storage |

## How to go from about 34K to 50K+ transactions/sec sustained

What this benchmark measured on one laptop: about 34K transactions/sec end to end, with single stages peaking above 50K/sec. Sustaining 50K+ means giving each part its own resources:

1. **Separate the parts:** brokers, stream processors, Redis and Postgres each on their own machines, so they stop competing for CPU.
2. **Scale consumers by partitions:** with 12 partitions, each stage can run up to 12 consumer instances. Add partitions (24, 48) before adding more consumers than that.
3. **Use 3 brokers with replication factor 3** and `min.insync.replicas=2` for durability. This costs some producer latency (`acks=all` now waits for replicas), recovered by batching.
4. **Write the ledger in bigger batches,** or partition the ledger table, if Postgres becomes the limit.
5. **Measure, don't guess:** run this benchmark against the new setup and compare `results/SUMMARY.md`.

The instance types above are common starting points, not measured results. The right sizes come from benchmarking your own workload.
