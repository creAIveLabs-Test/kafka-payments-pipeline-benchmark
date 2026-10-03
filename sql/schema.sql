-- Reference data used for enrichment (cache-aside source of truth)
CREATE TABLE accounts (
  account_id  TEXT PRIMARY KEY,
  tier        TEXT NOT NULL,          -- BASIC | PLUS | PREMIUM | CORPORATE
  risk_flag   BOOLEAN NOT NULL,
  home_country TEXT NOT NULL
);

CREATE TABLE merchants (
  merchant_id TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  category    TEXT NOT NULL,          -- GROCERY | TRAVEL | GAMBLING | ...
  country     TEXT NOT NULL
);

-- Final state written by the ledger consumer.
-- Primary key on tx_id makes replays harmless (idempotent consumer).
CREATE TABLE ledger_entries (
  tx_id        UUID PRIMARY KEY,
  account_id   TEXT   NOT NULL,
  merchant_id  TEXT   NOT NULL,
  amount_cents BIGINT NOT NULL,
  decision     TEXT   NOT NULL,       -- APPROVE | DECLINE | REVIEW
  reason       TEXT   NOT NULL,
  velocity_5m  INT    NOT NULL,
  created_ms   BIGINT NOT NULL,       -- when the generator produced the transaction
  posted_ms    BIGINT NOT NULL        -- when the ledger committed it
);
