1. Use Apache spark as your compute layer
2. This will write paraquet files into minio
3. but instead of writing directly, we use iceberg and spark writes into iceber metadata files
4. iceberg reads metadata files, and figures out which paraquet files to read/write from


SPARK_LOCAL_IP=127.0.0.1 spark-submit \
    --conf "spark.eventLog.enabled=false" \
    --conf "spark.ui.showConsoleProgress=false" \
    --conf "spark.hadoop.fs.s3a.endpoint=http://localhost:9000" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
    --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
    --packages \
    org.apache.iceberg:oceberg-spark-runtime-4.0_2.13:1.10.1, \
    org.apache.hadoop:hadoop-aws:3.4.1, \
    software.amazon.awssdk:bundle:2.29.52 \
    lakehouse_iceberg.py