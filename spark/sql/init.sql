CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.accounts (
  account_id      BIGINT PRIMARY KEY,
  customer_id     BIGINT NOT NULL,
  account_type    TEXT NOT NULL,
  opened_date     DATE NOT NULL,
  status          TEXT NOT NULL
);

INSERT INTO bronze.accounts (account_id, customer_id, account_type, opened_date, status)
VALUES
  (1001, 501, 'checking', '2023-01-10', 'active'),
  (1002, 501, 'savings',  '2022-07-01', 'active'),
  (1003, 502, 'checking', '2024-02-14', 'active'),
  (1004, 503, 'credit',   '2021-11-30', 'active')
ON CONFLICT (account_id) DO NOTHING;
