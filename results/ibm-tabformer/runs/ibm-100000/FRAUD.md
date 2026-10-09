## IBM TabFormer: source vs ledger

- Source rows replayed: **100,000**; in the ledger: **100,000**; missing: **0**; rows not from this replay: 0
- Refunds (negative amounts): 2,581; rows with an issuer error: 1,707 (both processed like any other row)

## Rules vs the fraud label (the pipeline never sees the label)

| | Labelled fraud | Labelled not fraud |
|---|---|---|
| Flagged (DECLINE or REVIEW) | 0 | 67 |
| Approved | 126 | 99,807 |

- **Recall 0.0%**: share of labelled fraud the rules flagged (0 of 126)
- **Precision 0.0%**: share of flagged transactions that are labelled fraud

| Rule | Flagged | Of which labelled fraud |
|---|---|---|
| high_amount_basic_tier | 66 | 0 |
| cross_border_high_amount | 1 | 0 |
