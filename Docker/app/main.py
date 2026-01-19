import os
import logging
import pandas as pd
from sqlalchemy import text

from app.db import get_engine, wait_for_db

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("etl")

CSV_PATH = os.environ.get("CUSTOMERS_CSV", "/data/input/customers.csv")

def split_name(full_name: str) -> tuple[str, str]:
    parts = str(full_name).strip().split()
    if not parts:
        return ("Unknown", "Unknown")
    if len(parts) == 1:
        return (parts[0], "Unknown")
    return (parts[0], " ".join(parts[1:]))

def run():
    engine = get_engine()
    wait_for_db(engine)

    logger.info("Reading input CSV: %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH)

    required_cols = {"customer_id", "full_name", "email"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}")

    logger.info("Loading %s rows into staging_customers (replace load)", len(df))
    
    df.to_sql("staging_customers", engine, if_exists="replace", index=False)

    logger.info("Transforming staging -> dim_customer")
    dim = df.copy()
    dim[["first_name", "last_name"]] = dim["full_name"].apply(lambda x: pd.Series(split_name(x)))
    dim = dim[["customer_id", "first_name", "last_name", "email"]]

    
    upsert_sql = text("""
        INSERT INTO dim_customer (customer_id, first_name, last_name, email, created_at)
        VALUES (:customer_id, :first_name, :last_name, :email, NOW())
        ON CONFLICT (customer_id) DO UPDATE SET
          first_name = EXCLUDED.first_name,
          last_name  = EXCLUDED.last_name,
          email      = EXCLUDED.email;
    """)

    with engine.begin() as conn:
        for row in dim.to_dict(orient="records"):
            conn.execute(upsert_sql, row)

    logger.info("ETL complete. Verifying row counts...")
    with engine.connect() as conn:
        stg = conn.execute(text("SELECT COUNT(*) FROM staging_customers")).scalar_one()
        cur = conn.execute(text("SELECT COUNT(*) FROM dim_customer")).scalar_one()
    logger.info("staging_customers=%s, dim_customer=%s", stg, cur)

if __name__ == "__main__":
    run()