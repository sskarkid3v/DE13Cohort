from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = (
        SparkSession.builder
        .appName("lakehouse-iceberg-local")
        .master("local[*]")
        # Iceberg extensions = adds Iceberg SQL features (MERGE, time travel, etc.)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Define an Iceberg catalog called "local"
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        # Warehouse points to MinIO bucket
        .config("spark.sql.catalog.local.warehouse", "s3a://lakehouse/warehouse")
        # S3A settings for MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Classroom-friendly
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")


    print("\n--- 1) Create database (namespace) ---")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.demo")
    spark.sql("SHOW DATABASES").show(truncate=False)

    print("\n--- 2) Create an Iceberg table ---")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.demo.transactions (
            transaction_id STRING,
            customer_id INT,
            branch_id INT,
            amount DOUBLE,
            fee DOUBLE,
            txn_ts TIMESTAMP,
            channel STRING
        )
        USING iceberg
        PARTITIONED BY (days(txn_ts))
    """)

    print("\n--- 3) Insert some sample rows ---")
    spark.sql("""
        INSERT INTO local.demo.transactions VALUES
        ('TX-1', 10, 1, 1200.50, 3.20, TIMESTAMP '2025-01-10 10:10:10', 'mobile'),
        ('TX-2', 12, 1,  250.00, 0.80, TIMESTAMP '2025-01-10 11:20:00', 'web'),
        ('TX-3', 10, 2,  999.99, 2.50, TIMESTAMP '2025-01-11 09:05:00', 'atm'),
        ('TX-4', 50, 3, 5000.00,10.00, TIMESTAMP '2025-01-11 18:30:00', 'teller')
    """)

    print("\n--- 4) Query table ---")
    spark.sql("""
    SELECT to_date(txn_ts) AS txn_day, channel,
           COUNT(*) AS cnt,
           ROUND(SUM(amount), 2) AS total
    FROM local.demo.transactions
    GROUP BY to_date(txn_ts), channel
    ORDER BY txn_day, channel
    """).show(truncate=False)

    print("\n--- 5) Show Iceberg snapshots (time-travel foundation) ---")
    spark.sql("SELECT * FROM local.demo.transactions.snapshots ORDER BY committed_at").show(truncate=False)

    # Capture latest snapshot id for time travel demo
    latest_snapshot = spark.sql("""
        SELECT snapshot_id
        FROM local.demo.transactions.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """).collect()[0][0]

    print(f"\nLatest snapshot_id = {latest_snapshot}")

    print("\n--- 6) Update data using MERGE (warehouse-like behavior on files) ---")
    updates = spark.createDataFrame(
        [
            ("TX-2", 12, 1, 300.00, 0.90, "2025-01-10 11:20:00", "web"),     # update amount/fee
            ("TX-5", 77, 4, 700.00, 1.50, "2025-01-12 12:00:00", "mobile"), # new row
        ],
        ["transaction_id", "customer_id", "branch_id", "amount", "fee", "txn_ts", "channel"]
    ).withColumn("txn_ts", F.to_timestamp("txn_ts"))

    updates.createOrReplaceTempView("updates")

    spark.sql("""
        MERGE INTO local.demo.transactions t
        USING updates u
        ON t.transaction_id = u.transaction_id
        WHEN MATCHED THEN UPDATE SET
            t.customer_id = u.customer_id,
            t.branch_id   = u.branch_id,
            t.amount      = u.amount,
            t.fee         = u.fee,
            t.txn_ts      = u.txn_ts,
            t.channel     = u.channel
        WHEN NOT MATCHED THEN INSERT *
    """)

    print("\n--- 7) Snapshots after MERGE (new version committed) ---")
    spark.sql("SELECT * FROM local.demo.transactions.snapshots ORDER BY committed_at").show(truncate=False)

    print("\n--- 8) Time travel: query the table AS OF the earlier snapshot ---")
    spark.sql(f"""
        SELECT transaction_id, amount, fee, txn_ts
        FROM local.demo.transactions VERSION AS OF {latest_snapshot}
        ORDER BY transaction_id
    """).show(truncate=False)

    print("\n--- 9) Current view (after MERGE) ---")
    spark.sql("""
        SELECT transaction_id, amount, fee, txn_ts
        FROM local.demo.transactions
        ORDER BY transaction_id
    """).show(truncate=False)

    print("\n--- 10) Schema evolution: add a new column safely ---")
    spark.sql("ALTER TABLE local.demo.transactions ADD COLUMN currency STRING")
    spark.sql("""
        UPDATE local.demo.transactions
        SET currency = 'NPR'
        WHERE currency IS NULL
    """)

    spark.sql("DESCRIBE local.demo.transactions").show(truncate=False)
    spark.sql("SELECT transaction_id, amount, currency FROM local.demo.transactions ORDER BY transaction_id").show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()