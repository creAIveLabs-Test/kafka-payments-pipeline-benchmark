# All runs

| Run | Kafka | Instances per stage | Rows / expected | End-to-end tx/sec | Enricher / Scorer / Ledger avg | p50 ms | p99 ms | Hottest partition | Duplicates absorbed | Fault: lowest rate / recovery |
|---|---|---|---|---|---|---|---|---|---|---|
| ibm-1000 | 1 broker, RF 1 | 1 | 1,000 / 1,000 | **703** | 9,174 / 2,639 / 4,673 | 690 | 722 | 12.00x | 0 | - |
| ibm-100000 | 1 broker, RF 1 | 1 | 100,000 / 100,000 | **16,116** | 21,133 / 20,458 / 19,346 | 1409 | 2873 | 3.28x | 0 | - |
| ibm-2000000 | 1 broker, RF 1 | 1 | 2,000,000 / 2,000,000 | **55,417** | 74,330 / 73,212 / 57,074 | 3749 | 10671 | 1.46x | 0 | - |
| ibm-all | 1 broker, RF 1 | 1 | 24,386,900 / 24,386,900 | **66,847** | 91,701 / 91,520 / 67,068 | 54719 | 116799 | 1.09x | 0 | - |
