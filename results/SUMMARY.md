# All runs

| Run | Kafka | Instances per stage | Rows / expected | End-to-end tx/sec | Enricher / Scorer / Ledger avg | p50 ms | p99 ms | Hottest partition | Duplicates absorbed | Fault: lowest rate / recovery |
|---|---|---|---|---|---|---|---|---|---|---|
| baseline-2M-12p | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **33,651** | 36,828 / 36,658 / 34,751 | 26175 | 39871 | 1.06x | 0 | - |
| latency-at-20k-per-sec | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **19,952** | 20,256 / 20,317 / 20,386 | 159 | 3543 | 1.06x | 0 | - |
| single-partition | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **29,471** | 42,299 / 36,198 / 30,302 | 34367 | 54207 | 1.00x | 0 | - |
| whale-30pct | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **28,044** | 40,318 / 28,725 / 28,799 | 21215 | 52639 | 4.30x | 0 | - |
| whale-30pct-salted-4 | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **32,813** | 37,086 / 34,562 / 34,190 | 19839 | 37471 | 2.48x | 0 | - |
| scorer-exactly-once | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **32,247** | 35,922 / 35,706 / 33,403 | 26047 | 43359 | 1.06x | 0 | - |
