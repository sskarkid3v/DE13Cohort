CREATE TABLE IF NOT EXISTS staging_customers (
  customer_id   INT PRIMARY KEY,
  full_name     TEXT NOT NULL,
  email         TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id   INT PRIMARY KEY,
  first_name    TEXT NOT NULL,
  last_name     TEXT NOT NULL,
  email         TEXT,
  created_at    TIMESTAMPTZ NOT NULL
);

INSERT INTO staging_customers (customer_id, full_name, email)
VALUES (1, 'Seed User', 'seed.user@example.com')
ON CONFLICT (customer_id) DO NOTHING;