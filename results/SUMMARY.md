# Results on two machines

Same code, same 2,000,000 seeded transactions, same settings. Each machine's full table is in its own folder:
- [`mac-m2/`](mac-m2/SUMMARY.md): Apple M2, 8 cores, 16 GB RAM (macOS, Docker Desktop)
- [`windows-i7/`](windows-i7/SUMMARY.md): Intel Core i7-10750H, 12 logical cores, 31 GB RAM visible to Linux (Windows 11, WSL2, Docker Desktop)

## Single broker (replication factor 1)

| Run | Mac M2 tx/sec | Windows i7 tx/sec | Mac p50 / p99 ms | Windows p50 / p99 ms | Rows / expected (both) | Duplicates |
|---|---|---|---|---|---|---|
| baseline-2M-12p | 33,651 | 56,638 | 26,175 / 39,871 | 9,887 / 11,903 | 2,000,000 / 2,000,000 of 2,000,000 | 0 / 0 |
| latency-at-20k-per-sec | 19,952 | 19,990 | 159 / 3,543 | 92 / 2,915 | 2,000,000 / 2,000,000 of 2,000,000 | 0 / 0 |
| single-partition | 29,471 | 26,838 | 34,367 / 54,207 | 34,879 / 65,599 | 2,000,000 / 2,000,000 of 2,000,000 | 0 / 0 |
| whale-30pct | 28,044 | 40,399 | 21,215 / 52,639 | 6,215 / 26,831 | 2,000,000 / 2,000,000 of 2,000,000 | 0 / 0 |
| whale-30pct-salted-4 | 32,813 | 45,329 | 19,839 / 37,471 | 6,711 / 22,383 | 2,000,000 / 2,000,000 of 2,000,000 | 0 / 0 |
| scorer-exactly-once | 32,247 | 51,337 | 26,047 / 43,359 | 10,807 / 16,527 | 2,000,000 / 2,000,000 of 2,000,000 | 0 / 0 |

## 3-broker cluster (replication factor 3, min.insync.replicas 2), Windows i7 only

| Run | Kafka | Instances per stage | Rows / expected | End-to-end tx/sec | Enricher / Scorer / Ledger avg | p50 ms | p99 ms | Hottest partition | Duplicates absorbed | Fault: lowest rate / recovery |
|---|---|---|---|---|---|---|---|---|---|---|
| cluster-1-instance | 3 brokers, RF 3 | 1 | 2,000,000 / 2,000,000 | **37,943** | 54,450 / 53,911 / 39,681 | 22895 | 33023 | 1.12x | 0 | - |
| cluster-2-instances | 3 brokers, RF 3 | 2 | 2,000,000 / 2,000,000 | **37,491** | 46,788 / 47,081 / 39,040 | 11999 | 15399 | 1.12x | 0 | - |
| cluster-4-instances | 3 brokers, RF 3 | 4 | 2,000,000 / 2,000,000 | **25,953** | 30,604 / 30,552 / 26,633 | 5023 | 15991 | 1.12x | 0 | - |
| cluster-fault-kill-consumer | 3 brokers, RF 3 | 2 | 2,000,000 / 2,000,000 | **30,801** | 22,856 / 37,926 / 25,021 | 12727 | 27551 | 1.12x | 0 | kill-consumer: 0 rows/sec / 10 s |
| cluster-fault-kill-broker | 3 brokers, RF 3 | 2 | 2,000,000 / 2,000,000 | **30,774** | 37,784 / 37,792 / 31,854 | 19119 | 24799 | 1.12x | 0 | kill-broker: 0 rows/sec / 8 s |
| cluster-fault-scale-out | 3 brokers, RF 3 | 3 | 2,000,000 / 2,000,000 | **32,602** | 35,862 / 36,007 / 34,156 | 12431 | 18383 | 1.12x | 15,306 | scale-out: 10,000 rows/sec / 0 s |

The Mac ran the cluster and fault code only as a 200,000-transaction validation ([`mac-m2/cluster-validation-200k/`](mac-m2/cluster-validation-200k/)); the full 2M-transaction cluster runs were done on the Windows machine, which has more memory and disk.
