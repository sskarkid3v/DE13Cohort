import argparse
import time
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window


def build_spark(app_name: str) -> SparkSession:
    """
    Production-minded SparkSession:
    - set shuffle partitions
    - enable AQE (Adaptive Query Execution)
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def read_csv_transactions(spark: SparkSession, path: str):
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


def transform_to_silver(bronze_df):
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


def gold_daily_spend(silver_df):
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


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input-csv", required=True)
    p.add_argument("--bronze-paraquet", required=True)
    p.add_argument("--silver-paraquet", required=True)
    p.add_argument("--gold-paraquet", required=True)
    args = p.parse_args()

    spark = build_spark("paraquet_pipeline_demo")
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== Spark UI ===")
    print(spark.sparkContext.uiWebUrl)

    # 1) read csv transactions into Bronze
    bronze_csv = read_csv_transactions(spark, args.input_csv)
    print("\n=== Bronze partitions ===")
    print("CSV rows: ", bronze_csv.count())

    # 2) write bronze to Paraquet (optimized for Spark read)
    print("\n=== Write Bronze to Paraquet ===")
    bronze_csv.write.mode("overwrite").parquet(args.bronze_paraquet)
    
    # 3) read bronze from Paraquet (optimized for Spark read)
    print("\n=== Read Bronze from Paraquet ===")
    bronze_paraquet = spark.read.parquet(args.bronze_paraquet)
    print("Bronze Paraquet Schema: ")
    bronze_paraquet.printSchema()
    
    # 4) transform to silver and write to Paraquet
    print("\n=== Transform to Silver and write to Paraquet ===")
    silver = transform_to_silver(bronze_paraquet)
    print("Silver rows: ", silver.count())
    
    print("writing silver to parquet...")
    (silver.write.mode("overwrite").partitionBy("txn_date").parquet(args.silver_paraquet))
    
    # 5) gold aggregation and write to Paraquet
    print("\n=== Gold daily aggregation and write to Paraquet ===")
    gold = gold_daily_spend(silver)
    
    print("writing to gold parquet...")
    (gold.write.mode("overwrite").partitionBy("txn_date").parquet(args.gold_paraquet))

    spark.stop()


if __name__ == "__main__":
    main()
    
    
