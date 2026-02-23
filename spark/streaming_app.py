# Write your streaming app code here

"""
streaming_app.py — Main Spark Structured Streaming application.

Pipeline:
  1. Read raw strings from Kafka topic 'sensor-events'
  2. Try to parse JSON — unparseable lines go to invalid sink immediately
  3. Apply schema + validation rules (schema.py)
  4. Valid events   → windowed aggregations → console + file + Kafka
  5. Invalid events → console + file + Kafka topic 'invalid-events'

Run:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      spark/streaming_app.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from schema import EVENT_SCHEMA, add_parsed_timestamp, split_valid_invalid

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP  = "localhost:9092"
INPUT_TOPIC      = "sensor-events"
VALID_TOPIC      = "valid-events"
INVALID_TOPIC    = "invalid-events"
CHECKPOINT_BASE  = "C:/tmp/spark_checkpoints"
OUTPUT_BASE      = "C:/tmp/spark_output"
WATERMARK_DELAY  = "10 minutes"
WINDOW_DURATION  = "10 minutes"
WINDOW_SLIDE     = "5 minutes"





# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SensorStreamLab")
        # Required for local mode without a cluster
        .master("local[*]")
        # Kafka integration package is loaded via spark-submit --packages
        .config("spark.sql.shuffle.partitions", "4")   # keep small for local dev
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Source: Kafka → raw strings
# ---------------------------------------------------------------------------
def read_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        # Kafka gives us a 'value' column as binary — cast to string
        .selectExpr("CAST(value AS STRING) AS raw_json")
    )


# ---------------------------------------------------------------------------
# Parse JSON safely
# ---------------------------------------------------------------------------
def parse_json(raw_df):
    """
    from_json never throws — invalid JSON produces null fields.
    We keep the original raw_json so we can route truly unparseable rows.
    """
    parsed = raw_df.withColumn(
        "data",
        F.from_json(F.col("raw_json"), EVENT_SCHEMA)
    ).select(
        "raw_json",
        F.col("data.device_id").alias("device_id"),
        F.col("data.event_time").alias("event_time"),
        F.col("data.temperature").alias("temperature"),
        F.col("data.country").alias("country"),
    )
    return parsed


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------
def write_to_console(df, query_name: str, output_mode: str = "append", num_rows: int = 20):
    return (
        df.writeStream
        .queryName(query_name)
        .outputMode(output_mode)
        .format("console")
        .option("truncate", "false")
        .option("numRows", num_rows)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{query_name}_console")
        .start()
    )


def write_to_file(df, query_name: str, output_mode: str = "append", fmt: str = "json"):
    path = os.path.join(OUTPUT_BASE, query_name)
    return (
        df.writeStream
        .queryName(f"{query_name}_file")
        .outputMode(output_mode)
        .format(fmt)
        .option("path", path)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{query_name}_file")
        .start()
    )


def write_to_kafka(df, query_name: str, topic: str):
    """Serialize the whole row as JSON and send to a Kafka topic."""
    kafka_df = df.select(
        F.to_json(F.struct(*df.columns)).alias("value")
    )
    return (
        kafka_df.writeStream
        .queryName(f"{query_name}_kafka")
        .outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{query_name}_kafka")
        .start()
    )


# ---------------------------------------------------------------------------
# Windowed aggregations on valid events
# ---------------------------------------------------------------------------
def avg_temp_per_device(valid_df):
    """Average temperature per device_id over a sliding event-time window."""
    return (
        valid_df
        .withWatermark("event_ts", WATERMARK_DELAY)
        .groupBy(
            F.window("event_ts", WINDOW_DURATION, WINDOW_SLIDE),
            "device_id"
        )
        .agg(
            F.avg("temperature").alias("avg_temperature"),
            F.count("*").alias("event_count")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "device_id",
            F.round("avg_temperature", 2).alias("avg_temperature"),
            "event_count"
        )
    )


def events_per_country(valid_df):
    """Event count per country per event-time window."""
    return (
        valid_df
        .withWatermark("event_ts", WATERMARK_DELAY)
        .groupBy(
            F.window("event_ts", WINDOW_DURATION, WINDOW_SLIDE),
            "country"
        )
        .agg(F.count("*").alias("event_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "country",
            "event_count"
        )
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")  # reduce noise

    # 1. Read from Kafka
    raw_df = read_kafka(spark)

    # 2. Parse JSON (never crashes — bad JSON → null fields)
    parsed_df = parse_json(raw_df)

    # 3. Add proper timestamp column for event-time processing
    parsed_df = add_parsed_timestamp(parsed_df)

    # 4. Split into valid / invalid
    valid_df, invalid_df = split_valid_invalid(parsed_df)

    # ── Valid events pipeline ──────────────────────────────────────────────

    # 4a. Write raw valid events to console and file for inspection
    q_valid_console = write_to_console(valid_df, "valid_events")
    q_valid_file    = write_to_file(valid_df,    "valid_events", fmt="json")
    q_valid_kafka   = write_to_kafka(valid_df,   "valid_events", VALID_TOPIC)

    # 4b. Windowed aggregation: avg temp per device
    device_agg = avg_temp_per_device(valid_df)
    q_device_console = write_to_console(device_agg, "agg_device_temp", output_mode="update")
    q_device_file    = write_to_file(device_agg,    "agg_device_temp",
                                     output_mode="append", fmt="parquet")

    # 4c. Windowed aggregation: events per country
    country_agg = events_per_country(valid_df)
    q_country_console = write_to_console(country_agg, "agg_country_count", output_mode="update")
    q_country_file    = write_to_file(country_agg,    "agg_country_count",
                                      output_mode="append", fmt="parquet")

    # ── Invalid events pipeline ────────────────────────────────────────────

    q_invalid_console = write_to_console(invalid_df, "invalid_events")
    q_invalid_file    = write_to_file(invalid_df,    "invalid_events", fmt="json")
    q_invalid_kafka   = write_to_kafka(invalid_df,   "invalid_events", INVALID_TOPIC)

    # ── Wait for all queries ───────────────────────────────────────────────
    print("=" * 60)
    print("All streaming queries started. Press Ctrl+C to stop.")
    print("=" * 60)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()