# Replaying IBM TabFormer through the pipeline

[IBM TabFormer](https://github.com/IBM/TabFormer) is a public credit-card dataset: 24,386,900 transactions from
2,000 simulated US cardholders (6,139 cards, 100,343 merchants, 1991 to 2020), made by IBM's transaction
simulator. It is synthetic, like this repo's generator, but public, much larger, and labelled: 29,757 rows are
marked fraud. Replaying it gives a second workload anyone can reproduce, and a way to check the scoring rules
against labels the pipeline never sees.

## Run it

```bash
# 1. get the file (266 MB download, 2.35 GB unpacked)
curl -L -o transactions.tgz https://github.com/IBM/TabFormer/raw/main/data/credit_card/transactions.tgz
tar -xzf transactions.tgz            # -> card_transaction.v1.csv

# 2. replay 1K, 100K, 2M, then every row
IBM_CSV=$PWD/card_transaction.v1.csv ./scripts/run-ibm.sh
SIZES="1000 100000" IBM_CSV=... ./scripts/run-ibm.sh     # just some sizes
```

Each size replays the first N rows. The file is sorted by user, card and time, so a smaller size means fewer
cardholders with their full history. Results go to `results/ibm-tabformer/runs/ibm-<size>/`:
`RESULTS.md` (throughput, latency, per stage, as for every run) and `FRAUD.md` (below).

Disk: measured at about 34 MB per 100K rows while a run is in progress (Kafka + ledger), so about 8.5 GB for
all 24.4M rows. Data is deleted after each run.

## How the file maps to the pipeline

| File | Pipeline | Notes |
|---|---|---|
| row number | `txId` = UUID(0x1B4D000000000001, row) | Same row, same id: replays and redeliveries are absorbed by the ledger's primary key |
| `User` + `Card` | `accountId` (Kafka key) | All of a card's transactions stay in order on one partition |
| `Merchant Name` | `merchantId` | |
| `MCC` | merchant category | Real merchant category codes (7995 etc. = GAMBLING, 3000-3999 = TRAVEL, ...); see `Ibm.category` |
| `Merchant State` | merchant country | US state or online = US; a country name = that country (cross-border rule) |
| `Amount` | `amountCents` | Refunds (negative amounts) are kept and counted |
| `Year`..`Time` | `eventMs` | The Scorer's 5-minute velocity window uses this time, not the replay clock |
| `Use Chip`, `Errors?` | `channel`, `issuerError` | Carried on the message; rows with issuer errors are processed and counted |
| `Is Fraud?` | not sent | Read only after the run by `scripts/ibm_eval.py` |
| (not in the file) | account tier, risk flag | Derived from the id like the synthetic data, so those two rule inputs are not from the dataset |

## What FRAUD.md checks

1. **Source vs ledger:** every replayed row is in `ledger_entries` exactly once, matched by `txId`, not just a row count.
2. **Rules vs label:** flagged = DECLINE or REVIEW. Recall = share of labelled fraud flagged; precision = share of
   flagged rows that are labelled fraud, with a breakdown per rule.

The rules in `Scorer.java` were written for the synthetic generator, not tuned on this data. On the first 100K rows
(Mac M2 test run) they flagged 67 rows and caught none of the 126 labelled frauds. That is the baseline a learned
model would have to beat.
