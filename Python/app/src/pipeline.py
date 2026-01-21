import csv
import json
import time
from datetime import datetime, timedelta
from typing import Iterator, Tuple, Dict, Any, List
import io

from psycopg import Connection
from psycopg.errors import OperationalError

from .config import Config
from .logger import setup_logger

logger = setup_logger()

ALLOWED_CHANNELS = {"pos", "web", "mobile"}
ALLOWED_CURRENCY = {"NPR"}

def ensure_schema_and_tables(conn: Connection, cfg: Config) -> None:
    logger.info("Ensuring schema and tables exist")
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema};")

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {cfg.schema}.etl_runs (
            run_id        BIGSERIAL PRIMARY KEY,
            started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at   TIMESTAMPTZ,
            status        TEXT NOT NULL DEFAULT 'running',
            source_path   TEXT NOT NULL,
            rows_read     INT NOT NULL DEFAULT 0,
            rows_loaded   INT NOT NULL DEFAULT 0,
            bad_rows      INT NOT NULL DEFAULT 0,
            message       TEXT
        );
        """)

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {cfg.schema}.raw_transactions (
            id          BIGSERIAL PRIMARY KEY,
            txn_id      TEXT NOT NULL,
            account_id  INT NOT NULL,
            ts_event    TIMESTAMPTZ NOT NULL,
            amount      NUMERIC(12,2) NOT NULL,
            currency    TEXT NOT NULL,
            channel     TEXT NOT NULL,
            run_id      BIGINT NOT NULL REFERENCES {cfg.schema}.etl_runs(run_id),
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {cfg.schema}.bad_transactions (
            id          BIGSERIAL PRIMARY KEY,
            run_id      BIGINT NOT NULL REFERENCES {cfg.schema}.etl_runs(run_id),
            raw_row     JSONB NOT NULL,
            error       TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {cfg.schema}.clean_transactions (
            txn_id      TEXT PRIMARY KEY,
            account_id  INT NOT NULL,
            ts_event    TIMESTAMPTZ NOT NULL,
            amount      NUMERIC(12,2) NOT NULL,
            currency    TEXT NOT NULL,
            channel     TEXT NOT NULL,
            txn_day     DATE NOT NULL,
            last_run_id BIGINT NOT NULL REFERENCES {cfg.schema}.etl_runs(run_id)
        );
        """)
        
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {cfg.schema}.etl_state (
            job_name    TEXT PRIMARY KEY,
            last_success_ts TIMESTAMPTZ NOT NULL
        );
        """)
        
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {cfg.schema}.dq_results(
            id          BIGSERIAL PRIMARY KEY,
            run_id      BIGINT NOT NULL REFERENCES {cfg.schema}.etl_runs(run_id),
            check_name  TEXT NOT NULL,
            passed      BOOLEAN NOT NULL,
            metric      NUMERIC,
            details     TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)
        
        # Optional: prevent raw duplicates by txn_id per run
        cur.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_txn_per_run
        ON {cfg.schema}.raw_transactions (run_id, txn_id);
        """)

def start_run(conn: Connection, cfg: Config) -> int:
    logger.info("Creating etl_run record")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {cfg.schema}.etl_runs (source_path)
            VALUES (%s)
            RETURNING run_id;
            """,
            (cfg.csv_path,)
        )
        run_id = cur.fetchone()[0]
    logger.info(f"Run started with run_id={run_id}")
    return int(run_id)

def finish_run(conn: Connection, cfg: Config, run_id: int, status: str, rows_read: int, rows_loaded: int, bad_rows: int, message: str | None = None) -> None:
    logger.info(f"Finishing run_id={run_id} with status={status}")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {cfg.schema}.etl_runs
            SET finished_at = now(),
                status = %s,
                rows_read = %s,
                rows_loaded = %s,
                bad_rows = %s,
                message = %s
            WHERE run_id = %s;
            """,
            (status, rows_read, rows_loaded, bad_rows, message, run_id)
        )

def iter_csv_rows(cfg: Config, cutoff: datetime | None) -> Iterator[Dict[str, str]]:
    with open(cfg.csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if cutoff is None:  
                yield row
                continue
            
            ts_raw = row.get("ts_event", "").strip()
            if not ts_raw:
                yield row
                continue
            
            try:
                ts_val = datetime.fromisoformat(ts_raw)
                if ts_val >= cutoff:
                    yield row
            except Exception:
                yield row

def validate_row(row: Dict[str, str]) -> Tuple[bool, Dict[str, Any] | None, str | None]:
    try:
        txn_id = row["txn_id"].strip()
        if not txn_id:
            return False, None, "txn_id is empty"

        account_id = int(row["account_id"])
        ts_event = row["ts_event"].strip()
        amount = float(row["amount"])
        currency = row["currency"].strip().upper()
        channel = row["channel"].strip().lower()

        if amount <= 0:
            return False, None, "amount must be > 0"

        if currency not in ALLOWED_CURRENCY:
            return False, None, f"unsupported currency: {currency}"

        if channel not in ALLOWED_CHANNELS:
            return False, None, f"unsupported channel: {channel}"

        # Keep ts_event as string; Postgres will parse TIMESTAMPTZ.
        cleaned = {
            "txn_id": txn_id,
            "account_id": account_id,
            "ts_event": ts_event,
            "amount": amount,
            "currency": currency,
            "channel": channel,
        }
        return True, cleaned, None
    except Exception as e:
        return False, None, f"validation error: {e}"

def insert_bad_row(conn: Connection, cfg: Config, run_id: int, row: Dict[str, str], error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {cfg.schema}.bad_transactions (run_id, raw_row, error)
            VALUES (%s, %s::jsonb, %s);
            """,
            (run_id, json.dumps(row), error)
        )

def insert_raw_batch(conn: Connection, cfg: Config, run_id: int, batch: List[Dict[str, Any]]) -> int:
    if not batch:
        return 0

    values = [
        (
            r["txn_id"],
            r["account_id"],
            r["ts_event"],
            r["amount"],
            r["currency"],
            r["channel"],
            run_id
        )
        for r in batch
    ]

    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {cfg.schema}.raw_transactions
            (txn_id, account_id, ts_event, amount, currency, channel, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, txn_id) DO NOTHING;
            """,
            values
        )
    return len(batch)

def ingest_with_batching_and_quarantine(conn: Connection, cfg: Config, run_id: int, cutoff:datetime | None) -> Tuple[int, int, int]:
    logger.info(f"Starting ingestion with batch_size={cfg.batch_size}")
    rows_read = 0
    rows_loaded = 0
    bad_rows = 0
    batch: List[Dict[str, Any]] = []

    for row in iter_csv_rows(cfg, cutoff):
        rows_read += 1
        ok, cleaned, err = validate_row(row)

        if not ok:
            bad_rows += 1
            insert_bad_row(conn, cfg, run_id, row, err or "unknown error")
            continue

        batch.append(cleaned)

        if len(batch) >= cfg.batch_size:
            rows_loaded += insert_raw_batch_with_retries(conn, cfg, run_id, batch)
            batch.clear()

    if batch:
        rows_loaded += insert_raw_batch_with_retries(conn, cfg, run_id, batch)
        batch.clear()

    logger.info(f"Ingestion finished. rows_read={rows_read}, rows_loaded={rows_loaded}, bad_rows={bad_rows}")
    return rows_read, rows_loaded, bad_rows

def insert_raw_batch_with_retries(conn: Connection, cfg: Config, run_id: int, batch: List[Dict[str, Any]]) -> int:
    attempt = 0
    while True:
        try:
            if cfg.use_copy:
                return copy_raw_batch(conn, cfg, run_id, batch)
            return insert_raw_batch(conn, cfg, run_id, batch)
        except OperationalError as e:
            attempt += 1
            if attempt >= cfg.max_retries:
                logger.error(f"Batch insert failed after {attempt} attempts: {e}")
                raise
            sleep_for = cfg.retry_backoff_seconds * attempt
            logger.warning(f"Batch insert failed: {e}. Retrying in {sleep_for}s")
            time.sleep(sleep_for)

def transform_upsert_clean(conn: Connection, cfg: Config, run_id: int) -> int:
    logger.info("Transforming raw into clean with UPSERT")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {cfg.schema}.clean_transactions
            (txn_id, account_id, ts_event, amount, currency, channel, txn_day, last_run_id)
            SELECT
                txn_id,
                account_id,
                ts_event,
                amount,
                currency,
                channel,
                DATE(ts_event) AS txn_day,
                %s as last_run_id
            FROM {cfg.schema}.raw_transactions
            WHERE run_id = %s
            ON CONFLICT (txn_id)
            DO UPDATE SET
                account_id  = EXCLUDED.account_id,
                ts_event    = EXCLUDED.ts_event,
                amount      = EXCLUDED.amount,
                currency    = EXCLUDED.currency,
                channel     = EXCLUDED.channel,
                txn_day     = EXCLUDED.txn_day,
                last_run_id = EXCLUDED.last_run_id;
            """,
            (run_id, run_id)
        )

        cur.execute(f"SELECT COUNT(*) FROM {cfg.schema}.clean_transactions;")
        total = cur.fetchone()[0]

    logger.info(f"Transform complete. clean_transactions total rows={total}")
    return int(total)

def preview_run_summary(conn: Connection, cfg: Config, run_id: int) -> None:
    logger.info("Previewing run summary and a few clean rows")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT run_id, started_at, finished_at, status, rows_read, rows_loaded, bad_rows, source_path, message
            FROM {cfg.schema}.etl_runs
            WHERE run_id = %s;
            """,
            (run_id,)
        )
        print(cur.fetchone(), flush=True)

        cur.execute(
            f"""
            SELECT txn_id, account_id, ts_event, amount, currency, channel, txn_day, last_run_id
            FROM {cfg.schema}.clean_transactions
            ORDER BY ts_event
            LIMIT 10;
            """
        )
        rows = cur.fetchall()

    for r in rows:
        print(r, flush=True)

def get_watermark(conn: Connection, cfg: Config, job_name: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT last_success_ts
            FROM {cfg.schema}.etl_state
            WHERE job_name = %s;
            """,
            (job_name,)
        )
        result = cur.fetchone()
        if not result or result[0] is None:
            return None
    return str(result[0])

def set_watermark(conn: Connection, cfg: Config, job_name: str, ts: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {cfg.schema}.etl_state (job_name, last_success_ts)
            VALUES (%s, %s::timestamptz)
            ON CONFLICT (job_name)
            DO UPDATE SET last_success_ts = EXCLUDED.last_success_ts;
            """,
            (job_name, ts)
        )

def compute_cutoff_ts(watermark: str | None, lookback_hours: int) -> datetime | None:
    if watermark is None:
        return None
    wm = datetime.fromisoformat(watermark.replace(" ","T"))
    return wm - timedelta(hours=lookback_hours)

def copy_raw_batch(conn: Connection, cfg: Config, run_id: int, batch: List[Dict[str, Any]]) -> int:
    if not batch:
        return 0

    buffer = io.StringIO()
    
    for r in batch:
        line = (
            f"{r['txn_id']},{r['account_id']},{r['ts_event']},{r['amount']},{r['currency']},{r['channel']},{run_id}\n"
        )
        buffer.write(line)
    buffer.seek(0)
    
    copy_sql = f"""
    COPY {cfg.schema}.raw_transactions
    (txn_id, account_id, ts_event, amount, currency, channel, run_id)
    FROM STDIN WITH (FORMAT csv);
    """
    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            copy.write(buffer.getvalue())

    return len(batch)

def record_dq(conn: Connection, cfg: Config, run_id:int, check_name:str, passed:bool, metric:float | None, details:str | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {cfg.schema}.dq_results
            (run_id, check_name, passed, metric, details)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (run_id, check_name, passed, metric, details)
        )

def run_dq_checks(conn: Connection, cfg: Config, run_id: int, rows_loaded: int, bad_rows: int) -> None:
    logger.info("Running data quality checks")
    failed = False
    
    # if we read rows, we should load something unless everything was bad
    expected_min_loaded = 0 if rows_loaded==0 else 1
    passed = (rows_loaded >= expected_min_loaded)
    record_dq(
        conn, cfg, run_id,
        "min_rows_loaded_nonzero",
        passed,
        float(rows_loaded),
        f"Expected at least {expected_min_loaded} rows loaded, got {rows_loaded}"
    )
    if not passed and rows_loaded > 0:
        failed = True
        
    # No duplicates in clean by txn_id
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*) 
            FROM {cfg.schema}.clean_transactions;
            """
        )
        total = cur.fetchone()[0]
        cur.execute(
            f"""
            SELECT COUNT(DISTINCT txn_id) FROM {cfg.schema}.clean_transactions;
            """
        )
        distinct_total = cur.fetchone()[0]
        
        passed = (total == distinct_total)
        record_dq(
            conn, cfg, run_id,
            "no_duplicates_in_clean_transactions",
            passed,
            float(total - distinct_total),
            f"Total rows: {total}, Distinct txn_id: {distinct_total}"
        )
        if not passed:
            failed = True
            
    # No non-positive amounts in clean
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*) FROM {cfg.schema}.clean_transactions
            WHERE amount <= 0;
            """
        )
        non_positive_count = cur.fetchone()[0]
        
        passed = (non_positive_count == 0)
        record_dq(
            conn, cfg, run_id,
            "no_non_positive_amounts_in_clean",
            passed,
            float(non_positive_count),
            f"Non-positive amount count: {non_positive_count}"
        )
        if not passed:
            failed = True
            
    #recent run should have influenced clean if it loaded something
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*) FROM {cfg.schema}.clean_transactions
            WHERE last_run_id = %s;
            """,
            (run_id,)
        )
        influenced_count = cur.fetchone()[0]

        passed = (influenced_count > 0) if rows_loaded > 0 else True
        record_dq(
            conn, cfg, run_id,
            "recent_run_influenced_clean",
            passed,
            float(influenced_count),
            f"Expected at least some rows influenced by run_id={run_id}, got {influenced_count}"
        )
        if not passed:
            failed = True
            
        if failed:
            logger.error("DQ checks failed")
            raise RuntimeError("Data Quality checks failed")
        logger.info("All DQ checks passed")