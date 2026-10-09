#!/usr/bin/env python3
"""After an IBM TabFormer run: compare the ledger with the source file.

  python3 scripts/ibm_eval.py <card_transaction.v1.csv> <rows> <out_dir>

Checks every source row reached the ledger exactly once (tx_id = UUID(IBM_MSB, row index)),
then scores the pipeline's decisions against the "Is Fraud?" label the pipeline never saw:
flagged = DECLINE or REVIEW. Writes <out_dir>/FRAUD.md and <out_dir>/fraud.json.
"""
import csv
import json
import subprocess
import sys
from collections import Counter

IBM_MSB = "1b4d0000-0000-0001"
CODES = {"APPROVE": 1, "DECLINE": 2, "REVIEW": 3}
NAMES = {v: k for k, v in CODES.items()}

csv_path, rows, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]

decision = bytearray(rows)            # 0 = missing from the ledger
reason_of = {}                        # row -> reason, only for flagged rows
foreign = 0
psql = subprocess.Popen(
    ["docker", "exec", "bench-postgres", "psql", "-U", "bench", "-d", "payments", "-qAt",
     "-c", "COPY (SELECT tx_id, decision, reason FROM ledger_entries) TO STDOUT"],
    stdout=subprocess.PIPE, text=True, bufsize=1 << 20)
for line in psql.stdout:
    tx, dec, reason = line.rstrip("\n").split("\t")
    if not tx.startswith(IBM_MSB):
        foreign += 1
        continue
    row = int(tx.replace("-", "")[16:], 16)
    if row >= rows:
        foreign += 1
        continue
    decision[row] = CODES[dec]
    if dec != "APPROVE":
        reason_of[row] = reason
if psql.wait() != 0:
    sys.exit("could not read ledger_entries from bench-postgres")

tp = fp = fn = tn = 0
missing = refunds = issuer_errors = fraud_total = 0
caught_by = Counter()
flagged_by = Counter()
errors_seen = Counter()
with open(csv_path, newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)
    for i, rec in enumerate(reader):
        if i >= rows:
            break
        d = decision[i]
        if d == 0:
            missing += 1
            continue
        fraud = rec[14] == "Yes"
        flagged = d != CODES["APPROVE"]
        fraud_total += fraud
        if rec[6].startswith("$-"):
            refunds += 1
        if rec[13]:
            issuer_errors += 1
            errors_seen[rec[13].rstrip(",")] += 1
        if flagged:
            flagged_by[reason_of[i]] += 1
        if fraud and flagged:
            tp += 1
            caught_by[reason_of[i]] += 1
        elif flagged:
            fp += 1
        elif fraud:
            fn += 1
        else:
            tn += 1

in_ledger = rows - missing
precision = tp / (tp + fp) if tp + fp else 0.0
recall = tp / (tp + fn) if tp + fn else 0.0
result = {
    "sourceRows": rows, "ledgerRowsForSource": in_ledger, "missing": missing, "unexpectedLedgerRows": foreign,
    "refunds": refunds, "issuerErrors": issuer_errors, "issuerErrorTypes": dict(errors_seen.most_common()),
    "fraudLabelled": fraud_total, "flagged": tp + fp,
    "truePositive": tp, "falsePositive": fp, "falseNegative": fn, "trueNegative": tn,
    "precision": round(precision, 4), "recall": round(recall, 4),
    "flaggedByRule": dict(flagged_by.most_common()), "fraudCaughtByRule": dict(caught_by.most_common()),
}
with open(f"{out}/fraud.json", "w") as f:
    json.dump(result, f, indent=1)

md = [
    "## IBM TabFormer: source vs ledger",
    "",
    f"- Source rows replayed: **{rows:,}**; in the ledger: **{in_ledger:,}**; missing: **{missing:,}**; "
    f"rows not from this replay: {foreign:,}",
    f"- Refunds (negative amounts): {refunds:,}; rows with an issuer error: {issuer_errors:,} (both processed like any other row)",
    "",
    "## Rules vs the fraud label (the pipeline never sees the label)",
    "",
    "| | Labelled fraud | Labelled not fraud |",
    "|---|---|---|",
    f"| Flagged (DECLINE or REVIEW) | {tp:,} | {fp:,} |",
    f"| Approved | {fn:,} | {tn:,} |",
    "",
    f"- **Recall {recall:.1%}**: share of labelled fraud the rules flagged ({tp:,} of {fraud_total:,})",
    f"- **Precision {precision:.1%}**: share of flagged transactions that are labelled fraud",
    "",
    "| Rule | Flagged | Of which labelled fraud |",
    "|---|---|---|",
]
for rule, n in flagged_by.most_common():
    md.append(f"| {rule} | {n:,} | {caught_by[rule]:,} |")
with open(f"{out}/FRAUD.md", "w") as f:
    f.write("\n".join(md) + "\n")
print("\n".join(md))
if missing:
    sys.exit(f"{missing:,} source rows are missing from the ledger")
