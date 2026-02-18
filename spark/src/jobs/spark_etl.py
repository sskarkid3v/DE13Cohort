import argparse
from pyspark.sql import SparkSession, functions as F, types as T


#spark session entry point
def build_spark(app_name: str) -> SparkSession:
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions","8")
        .getOrCreate()
    )
    return spark

#read aw csv file and return dataframe
def read_transactions(spark: SparkSession, path: str):
    
    schema = T.StructType([
        T.StructField("transaction_id", T.StringType(), False),
        T.StructField("account_id", T.LongType(), False),
        T.StructField("amount", T.DoubleType(), False),
        T.StructField("currency", T.StringType(), False),
        T.StructField("merchant", T.StringType(), False),
        T.StructField("category", T.StringType(), False),
        T.StructField("txn_ts", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("channel", T.StringType(), False),
        T.StructField("city", T.StringType(), False),
        T.StructField("country", T.StringType(), False),
    ])
    
    df  = (
        spark.read.option("header", "true")
        .schema(schema)
        .csv(path)
    )
    
    return df

#silver layer transformation
def transform_transaction(bronze_df):
    
    df1 = bronze_df.withColumn("txn_ts", F.to_timestamp("txn_ts", "yyyy-MM-dd HH:mm:ss"))
    
    df2 = (
        df1.filter(F.col("status") == F.lit("APPROVED"))
        .dropDuplicates(["transaction_id"])
        .withColumn("txn_date", F.to_date("txn_ts"))
        .filter(F.col("amount").isNotNull())
        .filter(F.col("amount") > 0)
    )
    
    df3 = df2.withColumn("merchant", F.trim(F.col("merchant")))
    
    return df3
    
#gold layer transformation
def daily_account_spend(silver_df):
    
    df = (
        silver_df.groupBy("account_id", "txn_date")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_spend"),
        )
    )
    
    return df


# JDBC help functions to read and write from postgres
def write_jdbc(df, jdbc_url, table, user, password, mode="append"):
    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )
    

def read_jdbc(spark, jdbc_url, table, user, password):
    return(
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    
#main job orchestration
def main():
    parser = argparse.ArgumentParser(description="Spark ETL Job")
    parser.add_argument("--input", type=str, required=True, help="Path to input CSV file")
    parser.add_argument("--pg-url", type=str, required=True, help="JDBC URL for Postgres")
    parser.add_argument("--pg-user", type=str, required=True, help="Database user")
    parser.add_argument("--pg-password", type=str, required=True, help="Database password")
    
    args = parser.parse_args()
    
    spark = build_spark("Spark ETL Job")
    
    #load to bronze layer
    bronze_df = read_transactions(spark, args.input)
    bronze_df.printSchema()
    bronze_df.show(5, truncate=False)
    print(f"Number of partitions: {bronze_df.rdd.getNumPartitions()}")
    
    #load to silver layer
    silver_df = transform_transaction(bronze_df)
    silver_df.count()
    silver_df.show(5, truncate=False)
    silver_df.explain(True)
    
    #load to gold layer
    gold_df = daily_account_spend(silver_df)
    gold_df.show(5, truncate=False)
    gold_df.explain(True)
    
    #load to postgres
    write_jdbc(silver_df, args.pg_url, "silver.transactions_clean", args.pg_user, args.pg_password, mode="overwrite")
    write_jdbc(gold_df, args.pg_url, "gold.daily_account_spend", args.pg_user, args.pg_password, mode="overwrite")
    
    #read from bronze table and join with spark results
    accounts = read_jdbc(spark, args.pg_url, "bronze.accounts", args.pg_user, args.pg_password)
    
    customer_spend = (
        gold_df.alias("g")
        .join(accounts.alias("a"), on="account_id", how="left")
        .groupBy("customer_id")
        .agg(F.round(F.sum("total_spend"), 2).alias("customer_total_spend"))
        .orderBy(F.col("customer_total_spend").desc())                                                    
    )
    
    customer_spend.show(5, truncate=False)
    spark.stop()
    
if __name__ == "__main__":
    main()
    
#spark-submit --master 'local[*]' --packages org.postgresql:postgresql:42.7.4 src/jobs/spark_etl.py --input data/raw/transactions.csv --pg-url jdbc:postgresql://localhost:5432/abnk --pg-user de --pg-password de