# All runs: Intel Core i7-10750H, 12 logical cores, 31 GB RAM visible to Linux (Windows 11, WSL2, Docker Desktop)

| Run | Kafka | Instances per stage | Rows / expected | End-to-end tx/sec | Enricher / Scorer / Ledger avg | p50 ms | p99 ms | Hottest partition | Duplicates absorbed | Fault: lowest rate / recovery |
|---|---|---|---|---|---|---|---|---|---|---|
| smoke | 1 broker, RF 1 | 1 | 200,000 / 200,000 | **15,643** | 21,582 / 18,513 / 18,265 | 1957 | 3495 | 1.06x | 0 | - |
| baseline-2M-12p | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **56,638** | 81,739 / 79,434 / 58,770 | 9887 | 11903 | 1.06x | 0 | - |
| latency-at-20k-per-sec | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **19,990** | 20,140 / 20,158 / 20,194 | 92 | 2915 | 1.06x | 0 | - |
| single-partition | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **26,838** | 89,226 / 40,895 / 27,173 | 34879 | 65599 | 1.00x | 0 | - |
| whale-30pct | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **40,399** | 75,755 / 45,663 / 41,264 | 6215 | 26831 | 4.30x | 0 | - |
| whale-30pct-salted-4 | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **45,329** | 79,949 / 63,818 / 46,502 | 6711 | 22383 | 2.48x | 0 | - |
| scorer-exactly-once | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **51,337** | 87,789 / 71,005 / 52,719 | 10807 | 16527 | 1.06x | 0 | - |
| cluster-1-instance | 3 brokers, RF 3 | 1 | 2,000,000 / 2,000,000 | **37,943** | 54,450 / 53,911 / 39,681 | 22895 | 33023 | 1.12x | 0 | - |
| cluster-2-instances | 3 brokers, RF 3 | 2 | 2,000,000 / 2,000,000 | **37,491** | 46,788 / 47,081 / 39,040 | 11999 | 15399 | 1.12x | 0 | - |
| cluster-4-instances | 3 brokers, RF 3 | 4 | 2,000,000 / 2,000,000 | **25,953** | 30,604 / 30,552 / 26,633 | 5023 | 15991 | 1.12x | 0 | - |
| cluster-fault-kill-consumer | 3 brokers, RF 3 | 2 | 2,000,000 / 2,000,000 | **30,801** | 22,856 / 37,926 / 25,021 | 12727 | 27551 | 1.12x | 0 | kill-consumer: 0 rows/sec / 10 s |
| cluster-fault-kill-broker | 3 brokers, RF 3 | 2 | 2,000,000 / 2,000,000 | **30,774** | 37,784 / 37,792 / 31,854 | 19119 | 24799 | 1.12x | 0 | kill-broker: 0 rows/sec / 8 s |
| cluster-fault-scale-out | 3 brokers, RF 3 | 3 | 2,000,000 / 2,000,000 | **32,602** | 35,862 / 36,007 / 34,156 | 12431 | 18383 | 1.12x | 15,306 | scale-out: 10,000 rows/sec / 0 s |
