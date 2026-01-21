from .config import Config
from .db import get_conn
from .logger import setup_logger
import time
from .pipeline import (
    ensure_schema_and_tables,
    start_run,
    finish_run,
    ingest_with_batching_and_quarantine,
    transform_upsert_clean,
    preview_run_summary,
    get_watermark,
    set_watermark,
    compute_cutoff_ts,
    run_dq_checks,
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
            
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {cfg.statement_timeout_ms};")
                
            ensure_schema_and_tables(conn, cfg)
            run_id = start_run(conn, cfg)
            
            #incremental setup
            job_name ="CSV_transaction_etl_job"
            watermark = get_watermark(conn, cfg, job_name)
            cutoff = compute_cutoff_ts(watermark, cfg.lookback_hours)
            logger.info(f"Using watermark={watermark}, cutoff={cutoff} for incremental load, USE_COPY={cfg.use_copy}")
            
            #ingestion timing
            t0 = time.time()            
            rows_read, rows_loaded, bad_rows = ingest_with_batching_and_quarantine(conn, cfg, run_id, cutoff)
            t_ingest = time.time() - t0
            logger.info(f"Ingestion completed in {t_ingest:.2f} seconds: rows_read={rows_read}, rows_loaded={rows_loaded}, bad_rows={bad_rows}")

            #transformation timing
            t0 = time.time()
            transform_upsert_clean(conn, cfg, run_id)
            t_transform = time.time() - t0
            logger.info(f"Transformation completed in {t_transform:.2f} seconds")
            
            #DQ timing
            t0 = time.time()
            run_dq_checks(conn, cfg, run_id, rows_loaded, bad_rows)
            t_dq = time.time() - t0
            logger.info(f"Data quality checks completed in {t_dq:.2f} seconds")
            
            #update watermark only on success
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT max(ts_event) FROM {cfg.schema}.raw_transactions WHERE run_id= %s;",
                    (run_id,),
                )
                max_ts = cur.fetchone()[0]
                
            if max_ts is not None:
                set_watermark(conn, cfg, job_name, str(max_ts))
                logger.info(f"Watermark updated to {max_ts}")
            else:
                logger.info("No new data ingested, watermark not updated")                

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
