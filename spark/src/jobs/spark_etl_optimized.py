import argparse
import time
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window


def build_spark(app_name: str, shuffle_partitions: int) -> SparkSession:
    """
    Production-minded SparkSession:
    - set shuffle partitions
    - enable AQE (Adaptive Query Execution)
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def read_transactions(spark: SparkSession, path: str):
    schema = T.StructType([
        T.StructField("transaction_id", T.StringType(), False),
        T.StructField("account_id", T.LongType(), False),
        T.StructField("amount", T.DoubleType(), True),
        T.StructField("currency", T.StringType(), True),
        T.StructField("merchant", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("txn_ts", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("channel", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
        T.StructField("country", T.StringType(), True),
    ])

    return (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(path)
    )


def transform_transactions(bronze_df):
    """
    Bronze -> Silver:
    - parse timestamps
    - keep approved
    - dedupe
    - add txn_date
    - basic data quality checks
    """
    df1 = bronze_df.withColumn("txn_time", F.to_timestamp("txn_ts", "yyyy-MM-dd HH:mm:ss"))

    df2 = (
        df1
        .filter(F.col("status") == F.lit("APPROVED"))
        .dropDuplicates(["transaction_id"])
        .withColumn("txn_date", F.to_date("txn_time"))
        .filter(F.col("amount").isNotNull())
        .filter(F.col("amount") > 0)
        .withColumn("merchant", F.trim(F.col("merchant")))
    )
    return df2


def daily_account_spend(silver_df):
    """
    Gold metric: daily spend per account.
    groupBy will cause shuffle (wide transformation).
    """
    return (
        silver_df
        .groupBy("account_id", "txn_date")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("total_amount")
        )
    )


def write_jdbc(df, jdbc_url, table, user, password, mode="append", batchsize=5000):
    """
    JDBC write with batchsize.
    Repartition outside this function to parallelize.
    """
    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", str(batchsize))
        .mode(mode)
        .save()
    )


def read_jdbc_simple(spark, jdbc_url, table, user, password):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def read_jdbc_parallel(spark, jdbc_url, table, user, password, partition_col, lower, upper, num_partitions):
    """
    Parallel JDBC read:
    Spark opens multiple JDBC connections and reads ranges in parallel.
    """
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("partitionColumn", partition_col)
        .option("lowerBound", str(lower))
        .option("upperBound", str(upper))
        .option("numPartitions", str(num_partitions))
        .load()
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--pg-url", required=True)
    p.add_argument("--pg-user", required=True)
    p.add_argument("--pg-password", required=True)
    p.add_argument("--shuffle-partitions", type=int, default=16)
    p.add_argument("--jdbc-parallelism", type=int, default=8)
    args = p.parse_args()

    spark = build_spark("banking_session2_optimized", args.shuffle_partitions)
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== Spark UI ===")
    print(spark.sparkContext.uiWebUrl)

    # 1) Bronze read
    bronze = read_transactions(spark, args.input)
    print("\n=== Bronze partitions ===")
    print(bronze.rdd.getNumPartitions())

    # 2) Silver transform
    silver = transform_transactions(bronze)

    # Key idea: cache because we reuse silver multiple times
    silver_cached = silver.cache()

    print("\n=== Materialize cache with an action (count) ===")
    t0 = time.perf_counter()
    silver_count = silver_cached.count()
    t1 = time.perf_counter()
    print(f"Silver rows: {silver_count} computed in {t1 - t0:.3f} seconds")

    # 3) Gold daily aggregation
    print("\n=== Build Gold daily spend (groupBy => shuffle) ===")
    t2 = time.perf_counter()
    gold_daily = daily_account_spend(silver_cached)
    _ = gold_daily.count()
    t3 = time.perf_counter()
    print(f"Gold daily computed in {t3 - t2:.3f} seconds")

    print("\n=== Gold daily explain (look for Exchange => shuffle) ===")
    gold_daily.explain(True)

    # 4) Write to Postgres with parallelism
    print("\n=== Writing to Postgres: parallelize by repartition ===")
    silver_out = silver_cached.repartition(args.jdbc_parallelism)
    gold_out = gold_daily.repartition(args.jdbc_parallelism)

    write_jdbc(silver_out, args.pg_url, "silver.transactions_clean", args.pg_user, args.pg_password, mode="overwrite")
    write_jdbc(gold_out, args.pg_url, "gold.daily_account_spend", args.pg_user, args.pg_password, mode="overwrite")

    # 5) Broadcast join
    print("\n=== Read small dimension table from Postgres ===")
    accounts = read_jdbc_simple(spark, args.pg_url, "bronze.accounts", args.pg_user, args.pg_password)

    print("\n=== Join optimization: broadcast(accounts) ===")
    customer_spend = (
        gold_daily.alias("g")
        .join(broadcast(accounts).alias("a"), on="account_id", how="left")
        .groupBy("customer_id")
        .agg(F.round(F.sum("total_amount"), 2).alias("customer_total_spend"))
        .orderBy(F.desc("customer_total_spend"))
    )

    print("\n=== Customer spend explain (look for BroadcastHashJoin) ===")
    customer_spend.explain(True)

    write_jdbc(customer_spend.repartition(args.jdbc_parallelism), args.pg_url, "gold.customer_spend", args.pg_user, args.pg_password, mode="overwrite")

    # 6) Window metric: rolling 7-day spend
    print("\n=== Window metric: rolling 7-day total spend per account ===")

    gold_daily2 = gold_daily.withColumn(
        "txn_day_epoch",
        F.unix_timestamp(F.col("txn_date").cast("timestamp"))
    )

    w = (
        Window.partitionBy("account_id")
        .orderBy(F.col("txn_day_epoch"))
        .rangeBetween(-6 * 86400, 0)  # last 7 days including today
    )

    gold_rolling_7d = (
        gold_daily2
        .withColumn("rolling_7d_amount", F.round(F.sum("total_amount").over(w), 2))
        .drop("txn_day_epoch")
    )

    gold_rolling_7d.show(20, truncate=False)

    write_jdbc(gold_rolling_7d.repartition(args.jdbc_parallelism), args.pg_url, "gold.account_rolling_7d", args.pg_user, args.pg_password, mode="overwrite")

    # 7) Parallel JDBC read demo
    print("\n=== Parallel JDBC read demo ===")
    bounds = accounts.agg(
        F.min("account_id").alias("min_id"),
        F.max("account_id").alias("max_id")
    ).collect()[0]

    min_id, max_id = int(bounds["min_id"]), int(bounds["max_id"])

    silver_parallel = read_jdbc_parallel(
        spark,
        args.pg_url,
        "silver.transactions_clean",
        args.pg_user,
        args.pg_password,
        partition_col="account_id",
        lower=min_id,
        upper=max_id,
        num_partitions=args.jdbc_parallelism
    )

    print("Parallel JDBC partitions:", silver_parallel.rdd.getNumPartitions())
    print("Parallel JDBC count:", silver_parallel.count())

    spark.stop()


if __name__ == "__main__":
    main()
        
    #spark-submit --master 'local[*]' --packages org.postgresql:postgresql:42.7.4 --conf 'spark.eventLog.enabled=true' --conf 'spark.ui.showConsoleProgress=false' src/jobs/spark_etl_optimized.py --input data/raw/transactions_big.csv --pg-url jdbc:postgresql://localhost:5432/abnk --pg-user de --pg-password de --shuffle-partitions 16 --jdbc-parallelism 8  