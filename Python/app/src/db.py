from contextlib import contextmanager
import psycopg
from psycopg import Connection
from psycopg.errors import OperationalError
from .config import Config
import time
from .logger import setup_logger

logger = setup_logger()

def connect_with_retries(cfg: Config) -> Connection:
    attempt = 0
    while True:
        try:
            logger.info(f"Attempting to connect to the database (attempt {attempt + 1})")
            conn = psycopg.connect(cfg.dsn())
            logger.info("Database connection established")
            return conn
        except OperationalError as e:
            attempt += 1
            if attempt >= cfg.max_retries:
                logger.error(f"DB connection failed after {attempt} attempts: {e}")
                raise
            sleep_for = cfg.retry_backoff_seconds * attempt
            logger.warning(f"DB connection failed (attempt {attempt}): {e}. Retrying in {sleep_for} seconds...")
            time.sleep(sleep_for)

@contextmanager
def get_conn(cfg: Config) -> Connection:
    conn = psycopg.connect(cfg.dsn())
    try:
        yield conn
    finally:
        conn.close()


