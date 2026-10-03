# Problems found while building this benchmark

Real issues hit while building and running this project, with the cause and the fix. Each one is the kind of problem distributed systems produce in production.

## 1. The benchmark filled the disk

**Symptom:** after a few 2M-transaction runs, the laptop's free disk dropped from 8.8 GB to 1.4 GB.

**Cause:** three things kept data after each run:
- Kafka keeps topic data until retention expires (hours), and deleted topics are only removed from disk after a delay.
- PostgreSQL keeps write-ahead log (WAL) files up to `max_wal_size` (it was 2 GB).
- Docker's disk image grows and only shrinks later.

**Fix:**
- Postgres `max_wal_size=256MB`.
- Kafka `log.segment.delete.delay.ms` and `file.delete.delay.ms` set to 1 s.
- Each run deletes its own topics and ledger rows after saving results.
- `run.sh` refuses to start with less than 3 GB free.

**Lesson:** in a streaming system, retention and log settings are capacity decisions, not defaults to ignore.

## 2. A crashed consumer made the run stop early

**Symptom:** after `kill -9` on one consumer, the run ended with 174,028 of 200,000 rows in the ledger.

**Cause:** Kafka only notices a crashed consumer after its **session timeout** (45 s by default), and until then the dead consumer's partitions stay assigned to it. The ledger stage was written to exit after 30 s with no data, so it quit while those partitions were still waiting to be reassigned. The data was safe in Kafka, but nobody was left to read it.

**Fix:**
- Stages no longer stop on idleness. The ledger writes a `DONE` signal when the table holds all N rows, and every stage stops on that signal.
- The session timeout is lowered to 10 s (heartbeat 3 s), so a crashed consumer's partitions move in about 10 s instead of 45 s.

**Lesson:** any timeout in your own code must be longer than the platform's failure-detection time. Otherwise a normal recovery looks like "done".

## 3. A rebalance killed healthy consumers

**Symptom:** during the consumer-crash test, surviving ledger processes stopped with `RebalanceInProgressException`.

**Cause:** when consumers join or leave, the group rebalances. An offset commit made during the rebalance is rejected. The code treated any exception as fatal and shut the process down.

**Fix:** commit failures caused by a rebalance are expected, not fatal. The process keeps polling; the uncommitted batch is redelivered to whichever consumer now owns the partition; the ledger's `ON CONFLICT (tx_id) DO NOTHING` absorbs the repeats. Rejected commits are counted in each stage's report (`offsetCommitsRejectedByRebalance`).

**Result:** the data check showed all 200,000 rows and about 2,000 replayed records absorbed as duplicates. Exactly what at-least-once delivery plus an idempotent sink should do.

**Lesson:** at-least-once delivery means duplicates *will* arrive. Design the sink to be idempotent, and treat rebalance errors as part of normal operation.

## 4. Instances added mid-run never shut down

**Symptom:** in the scale-out test, the extra instances never exited, and the run hung.

**Cause:** they joined near the end, got no records, and the "exit when idle after processing something" rule never applied to them.

**Fix:** same `DONE` signal as above, plus a stall guard in `run.sh`. If the ledger stops growing for 3 minutes before reaching N, the run is marked `STALLED` and stopped instead of hanging.

## 5. Latency numbers were really queueing

**Symptom:** the first runs reported a p50 latency of 24-26 seconds.

**Cause:** the generator produces faster than the pipeline drains, so most of that time was transactions waiting in Kafka, not being processed.

**Fix:** a separate fixed-rate run (`RATE=20000`, about 60% of capacity). There, p50 is 159 ms. Capacity runs report throughput; fixed-rate runs report latency.

**Lesson:** latency measured at or above capacity is a backlog measurement. Always measure latency below saturation.
