from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, when, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC = "clickstream"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/airflow"  # Using airflow DB for simplicity, or create a new one
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
POSTGRES_TABLE_ALERTS = "alerts"
POSTGRES_TABLE_AGG = "product_stats"

def get_spark_session():
    return SparkSession.builder \``
        .appName("ClickstreamAnalytics") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def write_to_postgres(df, epoch_id):
    # Write alerts
    df.persist()
    
    # Filter for alerts
    alerts_df = df.filter((col("total_views") > 100) & (col("total_purchases") < 5)) \
        .withColumn("alert_message", expr("'High Interest, Low Conversion: Flash Sale Recommended'")) \
        .withColumn("alert_time", current_timestamp())
    
    if alerts_df.count() > 0:
        print("!!! ALERTS GENERATED !!!")
        alerts_df.show()
        try:
            alerts_df.write \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", POSTGRES_TABLE_ALERTS) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        except Exception as e:
            print(f"Error writing alerts to Postgres: {e}")

    # Write all stats for reporting
    try:
        df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE_AGG) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error writing stats to Postgres: {e}")
    
    df.unpersist()

def write_raw_to_postgres(df, epoch_id):
    try:
        df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "activity_logs") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"Successfully wrote batch {epoch_id} to activity_logs")
    except Exception as e:
        print(f"Error writing raw logs to Postgres: {e}")

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Schema for incoming data
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("product_id", StringType()),
        StructField("event_type", StringType()),
        StructField("timestamp", StringType())  # Read as string first, then cast
    ])

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON and cast timestamp
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Watermark to handle late data (e.g., 1 minute)
    # Aggregate per product and window
    # Window: 10 minutes, sliding every 5 minutes
    windowed_counts = parsed_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "10 minutes", "5 minutes"),
            col("product_id")
        ) \
        .agg(
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases")
        )

    # Output to Console and Postgres (Aggregated)
    query = windowed_counts.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_postgres) \
        .start()

    # Output Raw Data to Postgres
    query_raw = parsed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_raw_to_postgres) \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
