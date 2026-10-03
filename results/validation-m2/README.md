# Validation runs (Apple M2 laptop, 200K transactions)

Small-scale runs used to verify the 3-broker cluster and fault-injection code on a laptop with limited Docker memory (brokers at 512 MB heap each). They prove correctness (every run ends with exactly 200,000 ledger rows), not peak throughput; throughput here is far below the single-broker results because three brokers and replication share 8 cores and about 8 GB of Docker memory.

| Run | Result |
|---|---|
| cluster-2inst | 200,000 / 200,000 rows, replication factor 3, 2 instances per stage |
| kill-broker | 1 of 3 brokers killed at 80K rows: 200,000 / 200,000 rows, 0 duplicates, recovered in 18 s |
| kill-consumer | enricher and ledger killed with SIGKILL: 200,000 / 200,000 rows, 2,861 replayed records absorbed by ON CONFLICT, recovered in 19 s |
| scale-out | extra instance of every stage joined mid-run: 200,000 / 200,000 rows, no throughput drop |

Full numbers are in each folder's RESULTS.md. Distributed results at full scale come from `scripts/experiments-distributed.sh` on a larger machine.
