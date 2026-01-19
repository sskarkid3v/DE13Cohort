from .config import Config
from .db import get_conn
from .logger import setup_logger
from .pipeline import (
    ensure_schema_and_tables,
    start_run,
    finish_run,
    ingest_with_batching_and_quarantine,
    transform_upsert_clean,
    preview_run_summary,
)

logger = setup_logger()

def main() -> None:
    cfg = Config()
    logger.info("Starting Session 2 ETL job (batching, upsert, retries, quality checks)")

    with get_conn(cfg) as conn:
        run_id = None
        rows_read = 0
        rows_loaded = 0
        bad_rows = 0

        try:
            ensure_schema_and_tables(conn, cfg)
            run_id = start_run(conn, cfg)

            rows_read, rows_loaded, bad_rows = ingest_with_batching_and_quarantine(conn, cfg, run_id)

            transform_upsert_clean(conn, cfg, run_id)

            finish_run(conn, cfg, run_id, "success", rows_read, rows_loaded, bad_rows, "OK")
            conn.commit()
            logger.info("Run committed successfully")

        except Exception as e:
            conn.rollback()
            logger.error(f"Pipeline failed. Rolled back. Error={e}")

            # Try to record failure if run_id exists, but don't crash if that fails.
            try:
                if run_id is not None:
                    finish_run(conn, cfg, run_id, "failed", rows_read, rows_loaded, bad_rows, str(e))
                    conn.commit()
            except Exception:
                pass

            raise

        preview_run_summary(conn, cfg, run_id)

    logger.info("ETL job completed")

if __name__ == "__main__":
    main()
