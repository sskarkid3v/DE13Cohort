import argparse
from pyspark.sql import SparkSession, functions as F

def build_spark(app_name: str) -> SparkSession:
    """
    Production-minded SparkSession:
    - set shuffle partitions
    - enable AQE (Adaptive Query Execution)
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--silver-paraquet", required=True, help="Path to Silver Parquet files")
    p.add_argument("--txn-date", default="2025-12-01", help="date to filter transactions on")
    args = p.parse_args()
    
    spark = build_spark("Pruning Demo")
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n reading paraquet files from Silver layer")
    df = spark.read.parquet(args.silver_paraquet)
    
    print("\n dataset schema:")
    df.printSchema()
    
    print("\n partition pruning demo")
    print(f"filtering transactions on txn_date = {args.txn_date}")
    pruned = df.filter(F.col("txn_date") == F.lit(args.txn_date))
    
    print("\n exmplain plan for pruned query:")
    pruned.explain(True)
    
    print("\n count (action)")
    print(f"rows for date: {pruned.count()}")
    
    print("column pruning demo")
    pruned_cols = df.select("account_id", "amount")
    
    print("\n explain plan for column pruned query:")
    pruned_cols.explain(True)
    print("\n show(action)")
    pruned_cols.show(10, truncate=False)
    
    print("\n partition and column pruning demo")
    both = (
        df.filter(F.col("txn_date") == F.lit(args.txn_date))
        .select("account_id", "amount")
    )
    
    print("\n explain plan for partition and column pruned query:")
    both.explain(True)
    print("\n show(action)")
    both.show(10, truncate=False)
    spark.stop()
    
if __name__ == "__main__":
    main()