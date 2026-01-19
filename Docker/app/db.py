import os
import time
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

def build_db_url() -> str:
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    db = os.environ.get("DB_NAME", "de_demo")
    user = os.environ.get("DB_USER", "de_user")
    pwd = os.environ.get("DB_PASSWORD", "de_password")

    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)

def wait_for_db(engine: Engine, retries: int = 30, sleep_seconds: float = 1.0) -> None:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database is reachable.")
            return
        except Exception as e:
            last_err = e
            logger.info("Waiting for database... attempt %s/%s", attempt, retries)
            time.sleep(sleep_seconds)
    raise RuntimeError(f"Database not reachable after {retries} attempts. Last error: {last_err}")