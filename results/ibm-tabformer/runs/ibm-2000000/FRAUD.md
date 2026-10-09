## IBM TabFormer: source vs ledger

- Source rows replayed: **2,000,000**; in the ledger: **2,000,000**; missing: **0**; rows not from this replay: 0
- Refunds (negative amounts): 88,131; rows with an issuer error: 32,173 (both processed like any other row)

## Rules vs the fraud label (the pipeline never sees the label)

| | Labelled fraud | Labelled not fraud |
|---|---|---|
| Flagged (DECLINE or REVIEW) | 8 | 213 |
| Approved | 2,216 | 1,997,563 |

- **Recall 0.4%**: share of labelled fraud the rules flagged (8 of 2,224)
- **Precision 3.6%**: share of flagged transactions that are labelled fraud

| Rule | Flagged | Of which labelled fraud |
|---|---|---|
| high_amount_basic_tier | 218 | 6 |
| risk_account_gambling | 2 | 2 |
| cross_border_high_amount | 1 | 0 |
