-- =========================================================
-- Week 1 - Session 1 dataset (Banking-style, PostgreSQL)
-- =========================================================

DROP SCHEMA IF EXISTS de_week1 CASCADE;
CREATE SCHEMA de_week1;
SET search_path TO de_week1;

-- Customers: includes tags as TEXT[] (array)
CREATE TABLE customers (
  customer_id     INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  full_name       TEXT NOT NULL,
  email           TEXT UNIQUE NOT NULL,
  city            TEXT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  tags            TEXT[] NOT NULL DEFAULT '{}'::TEXT[]
);

-- Accounts: a customer can have multiple accounts
CREATE TABLE accounts (
  account_id      INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id     INT NOT NULL REFERENCES customers(customer_id),
  account_type    TEXT NOT NULL CHECK (account_type IN ('checking','savings','credit')),
  opened_at       TIMESTAMPTZ NOT NULL,
  status          TEXT NOT NULL CHECK (status IN ('active','frozen','closed'))
);

-- Merchants: where transactions happen
CREATE TABLE merchants (
  merchant_id     INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  merchant_name   TEXT NOT NULL,
  mcc             TEXT NOT NULL,          -- merchant category code (string for simplicity)
  city            TEXT NOT NULL
);

-- Transactions: includes JSONB metadata (device_id, ip, channel, etc.)
CREATE TABLE transactions (
  transaction_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_id      INT NOT NULL REFERENCES accounts(account_id),
  merchant_id     INT REFERENCES merchants(merchant_id),
  ts_event        TIMESTAMPTZ NOT NULL,
  amount          NUMERIC(12,2) NOT NULL,
  currency        TEXT NOT NULL DEFAULT 'NPR',
  status          TEXT NOT NULL CHECK (status IN ('posted','reversed','declined')),
  metadata        JSONB NOT NULL DEFAULT '{}'::JSONB
);

-- Helpful indexes (we won't focus on performance today, but these make demos smoother)
CREATE INDEX idx_transactions_account_ts ON transactions(account_id, ts_event);
CREATE INDEX idx_transactions_status_ts ON transactions(status, ts_event);

-- ---------------------------------------------------------
-- Insert sample data
-- ---------------------------------------------------------

INSERT INTO customers (full_name, email, city, created_at, tags) VALUES
('Aarav Shrestha',   'aarav@example.com',   'Kathmandu',  '2025-11-10 09:00:00+05:45', ARRAY['vip','salary']),
('Nisha Karki',      'nisha@example.com',   'Lalitpur',   '2025-10-02 10:20:00+05:45', ARRAY['student']),
('Rohit Gurung',     'rohit@example.com',   'Pokhara',    '2025-09-18 14:10:00+05:45', ARRAY['travel']),
('Sita Rai',         'sita@example.com',    'Biratnagar', '2025-12-01 08:15:00+05:45', ARRAY['vip','risk_high']),
('Bikash Thapa',     'bikash@example.com',  'Kathmandu',  '2025-08-21 16:45:00+05:45', ARRAY[]::TEXT[]),
('Anu Poudel',       'anu@example.com',     'Lalitpur',   '2025-07-11 11:05:00+05:45', ARRAY['salary']),
('Prakash Adhikari', 'prakash@example.com', 'Pokhara',    '2025-12-15 12:00:00+05:45', ARRAY['vip']),
('Mina Sharma',      'mina@example.com',    'Kathmandu',  '2025-06-30 09:30:00+05:45', ARRAY['student','travel']);

INSERT INTO accounts (customer_id, account_type, opened_at, status) VALUES
(1, 'checking', '2025-11-12 09:00:00+05:45', 'active'),
(1, 'savings',  '2025-11-12 09:05:00+05:45', 'active'),
(2, 'checking', '2025-10-03 10:00:00+05:45', 'active'),
(3, 'credit',   '2025-09-20 12:00:00+05:45', 'active'),
(4, 'checking', '2025-12-02 09:00:00+05:45', 'frozen'),
(5, 'checking', '2025-08-22 10:00:00+05:45', 'active'),
(6, 'savings',  '2025-07-12 11:00:00+05:45', 'active'),
(7, 'checking', '2025-12-16 09:00:00+05:45', 'active'),
(8, 'credit',   '2025-07-01 10:00:00+05:45', 'active'),
(8, 'checking', '2025-07-01 10:10:00+05:45', 'active');

INSERT INTO merchants (merchant_name, mcc, city) VALUES
('Himal Mart',     '5411', 'Kathmandu'),
('QuickFuel',      '5541', 'Lalitpur'),
('Skyline Hotel',  '7011', 'Pokhara'),
('Mero Online',    '5964', 'Kathmandu'),
('City Pharmacy',  '5912', 'Biratnagar'),
('TravelHub',      '4722', 'Kathmandu');

-- Transactions with realistic JSONB metadata:
-- channel: pos, atm, mobile, web
-- device_id and ip simulate fraud/anomaly analysis later
INSERT INTO transactions (account_id, merchant_id, ts_event, amount, currency, status, metadata) VALUES
(1, 1, '2026-01-01 09:10:00+05:45',  2500.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-101","ip":"103.10.28.1","note":"groceries"}'),
(1, 4, '2026-01-01 22:40:00+05:45',  7999.00, 'NPR', 'posted',   '{"channel":"web","device_id":"DVC-101","ip":"103.10.28.1","note":"online purchase"}'),
(1, 2, '2026-01-02 07:35:00+05:45',  2000.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-102","ip":"103.10.28.5","note":"fuel"}'),
(2, 6, '2026-01-02 18:20:00+05:45', 12000.00, 'NPR', 'posted',   '{"channel":"mobile","device_id":"DVC-201","ip":"27.34.12.9","note":"ticket booking"}'),
(2, 6, '2026-01-03 08:15:00+05:45',  9000.00, 'NPR', 'reversed', '{"channel":"mobile","device_id":"DVC-201","ip":"27.34.12.9","note":"reversal"}'),

(3, 1, '2026-01-01 12:00:00+05:45',  1500.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-301","ip":"110.44.120.8"}'),
(3, 4, '2026-01-02 21:05:00+05:45',  4999.00, 'NPR', 'posted',   '{"channel":"web","device_id":"DVC-301","ip":"110.44.120.8"}'),
(3, 4, '2026-01-03 21:10:00+05:45',  5999.00, 'NPR', 'declined', '{"channel":"web","device_id":"DVC-999","ip":"185.17.22.3","reason":"insufficient_funds"}'),

(4, 3, '2026-01-02 20:00:00+05:45', 18000.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-401","ip":"202.51.74.10","note":"hotel"}'),
(4, 3, '2026-01-04 08:30:00+05:45', 22000.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-401","ip":"202.51.74.10","note":"hotel"}'),
(4, 6, '2026-01-04 09:10:00+05:45', 35000.00, 'NPR', 'posted',   '{"channel":"mobile","device_id":"DVC-402","ip":"202.51.74.99","note":"travel booking"}'),

(5, 5, '2026-01-02 10:00:00+05:45',  1200.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-501","ip":"36.252.10.2","note":"pharmacy"}'),
(5, 5, '2026-01-02 10:05:00+05:45',  1300.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-501","ip":"36.252.10.2","note":"pharmacy"}'),
(5, 5, '2026-01-02 10:08:00+05:45',  1250.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-501","ip":"36.252.10.2","note":"pharmacy"}'),

(6, 1, '2026-01-03 09:00:00+05:45',  2600.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-601","ip":"103.1.92.77"}'),
(6, 2, '2026-01-03 09:30:00+05:45',  2100.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-601","ip":"103.1.92.77"}'),

(7, 4, '2026-01-04 23:50:00+05:45', 15000.00, 'NPR', 'posted',   '{"channel":"web","device_id":"DVC-777","ip":"185.17.22.3","note":"late-night purchase"}'),
(7, 4, '2026-01-05 00:05:00+05:45', 14000.00, 'NPR', 'posted',   '{"channel":"web","device_id":"DVC-777","ip":"185.17.22.3","note":"late-night purchase"}'),

(8, 6, '2026-01-03 16:40:00+05:45',  8000.00, 'NPR', 'posted',   '{"channel":"mobile","device_id":"DVC-801","ip":"49.244.88.19"}'),
(9, 2, '2026-01-04 07:20:00+05:45',  1900.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-802","ip":"49.244.88.19"}'),
(10, 1,'2026-01-04 19:10:00+05:45',  3100.00, 'NPR', 'posted',   '{"channel":"pos","device_id":"DVC-802","ip":"49.244.88.19"}');

-- Quick sanity checks
SELECT 'customers' AS table_name, count(*) AS rows FROM customers
UNION ALL
SELECT 'accounts', count(*) FROM accounts
UNION ALL
SELECT 'merchants', count(*) FROM merchants
UNION ALL
SELECT 'transactions', count(*) FROM transactions;
