## IBM TabFormer: source vs ledger

- Source rows replayed: **1,000**; in the ledger: **1,000**; missing: **0**; rows not from this replay: 0
- Refunds (negative amounts): 33; rows with an issuer error: 24 (both processed like any other row)

## Rules vs the fraud label (the pipeline never sees the label)

| | Labelled fraud | Labelled not fraud |
|---|---|---|
| Flagged (DECLINE or REVIEW) | 0 | 0 |
| Approved | 0 | 1,000 |

- **Recall 0.0%**: share of labelled fraud the rules flagged (0 of 0)
- **Precision 0.0%**: share of flagged transactions that are labelled fraud

| Rule | Flagged | Of which labelled fraud |
|---|---|---|
