## IBM TabFormer: source vs ledger

- Source rows replayed: **24,386,900**; in the ledger: **24,386,900**; missing: **0**; rows not from this replay: 0
- Refunds (negative amounts): 1,244,689; rows with an issuer error: 388,431 (both processed like any other row)

## Rules vs the fraud label (the pipeline never sees the label)

| | Labelled fraud | Labelled not fraud |
|---|---|---|
| Flagged (DECLINE or REVIEW) | 62 | 3,344 |
| Approved | 29,695 | 24,353,799 |

- **Recall 0.2%**: share of labelled fraud the rules flagged (62 of 29,757)
- **Precision 1.8%**: share of flagged transactions that are labelled fraud

| Rule | Flagged | Of which labelled fraud |
|---|---|---|
| risk_account_gambling | 1,776 | 6 |
| high_amount_basic_tier | 1,612 | 51 |
| cross_border_high_amount | 18 | 5 |
