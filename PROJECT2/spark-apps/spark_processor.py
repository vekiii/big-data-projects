#!/usr/bin/env python3
"""
Spark Structured Streaming - SUMO Madrid Emission Analyzer
===========================================================
Reads vehicle emission records from the 'madrid-emission' Kafka topic,
applies tumbling or sliding time windows, aggregates pollution metrics
per spatial grid cell, and writes results to 'madrid-results'.

Usage (inside spark-master container):
    /spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
        /app/spark_processor.py \
        <window_duration> <slide_duration> <pollutant> <vehicle_type>

Arguments:
    window_duration  : e.g. "60 seconds", "5 minutes"
    slide_duration   : e.g. "30 seconds" for sliding; use "0" for tumbling
    pollutant        : CO2 | CO | HC | NOx | PMx | noise | all
    vehicle_type     : all | veh_passenger | truck_truck

Examples:
    Tumbling 5-min window, CO2, all vehicles:
        spark_processor.py "5 minutes" "0" "CO2" "all"

    Sliding 5-min window every 1 min, all pollutants, passengers only:
        spark_processor.py "5 minutes" "1 minute" "all" "veh_passenger"
"""

import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, max as spark_max,
    round as spark_round, lit, to_json, struct,
    from_unixtime, to_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BROKER = "kafka-1:9092"          # Internal Docker broker address
INPUT_TOPIC = "madrid-emission"
OUTPUT_TOPIC = "madrid-results"
CHECKPOINT_DIR = "/tmp/spark-checkpoint"

# Spatial grid resolution: round lat/lon to N decimal places
# 3 decimals ≈ ~100m grid cells (good for city-level pollution heatmap)
GRID_PRECISION = 3

# All supported pollutant columns
ALL_POLLUTANTS = ["CO2", "CO", "HC", "NOx", "PMx", "noise"]

# Watermark tolerance for late data (based on simulated time)
WATERMARK_DELAY = "30 seconds"

# ─────────────────────────────────────────────────────────────────────────────
# Schema for incoming Kafka JSON messages
# ─────────────────────────────────────────────────────────────────────────────
EMISSION_SCHEMA = StructType([
    StructField("timestep", DoubleType()),
    StructField("vehicle_id", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("eclass", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("x", DoubleType()),
    StructField("y", DoubleType()),
    StructField("speed", DoubleType()),
    StructField("CO2", DoubleType()),
    StructField("CO", DoubleType()),
    StructField("HC", DoubleType()),
    StructField("NOx", DoubleType()),
    StructField("PMx", DoubleType()),
    StructField("fuel", DoubleType()),
    StructField("noise", DoubleType()),
    StructField("event_time_ms", LongType()),
])


def parse_args():
    if len(sys.argv) < 5:
        print(__doc__)
        sys.exit(1)

    window_duration = sys.argv[1]   # e.g. "5 minutes"
    slide_duration = sys.argv[2]    # e.g. "1 minute" or "0" for tumbling
    pollutant = sys.argv[3]         # e.g. "CO2" or "all"
    vehicle_type = sys.argv[4]      # e.g. "all" or "veh_passenger"

    return window_duration, slide_duration, pollutant, vehicle_type


def get_pollutant_columns(pollutant: str):
    """Return list of pollutant column names to aggregate."""
    if pollutant.lower() == "all":
        return ALL_POLLUTANTS
    elif pollutant in ALL_POLLUTANTS:
        return [pollutant]
    else:
        print(f"[ERROR] Unknown pollutant '{pollutant}'. Choose from: {ALL_POLLUTANTS} or 'all'")
        sys.exit(1)


def build_aggregation_exprs(pollutant_cols):
    """Build avg and max aggregation expressions for chosen pollutants."""
    agg_exprs = []
    for p in pollutant_cols:
        agg_exprs.append(spark_round(avg(col(p)), 4).alias(f"{p}_avg"))
        agg_exprs.append(spark_round(spark_max(col(p)), 4).alias(f"{p}_max"))
    # Always include vehicle count
    from pyspark.sql.functions import count
    agg_exprs.append(count("*").alias("vehicle_count"))
    return agg_exprs


def main():
    window_duration, slide_duration, pollutant_arg, vehicle_type = parse_args()
    pollutant_cols = get_pollutant_columns(pollutant_arg)
    use_sliding = slide_duration != "0"
    window_type_label = "sliding" if use_sliding else "tumbling"

    print(f"[Spark] Window type   : {window_type_label}")
    print(f"[Spark] Window size   : {window_duration}")
    if use_sliding:
        print(f"[Spark] Slide step    : {slide_duration}")
    print(f"[Spark] Pollutants    : {pollutant_cols}")
    print(f"[Spark] Vehicle filter: {vehicle_type}")

    # ── Spark Session ──────────────────────────────────────────────────────
    spark = SparkSession.builder \
        .appName("SUMO_Madrid_Emission_Streaming") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ── Read from Kafka ────────────────────────────────────────────────────
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # ── Parse JSON payload ─────────────────────────────────────────────────
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), EMISSION_SCHEMA).alias("data")
    ).select("data.*")

    # ── Convert event_time_ms → timestamp for windowing ───────────────────
    # We treat SUMO simulation seconds as a base time starting from epoch
    # (1970-01-01 00:00:00) so windows are deterministic and reproducible.
    timed_df = parsed_df.withColumn(
        "event_time",
        to_timestamp(from_unixtime(col("event_time_ms") / 1000.0))
    )

    # ── Optional: filter by vehicle type ──────────────────────────────────
    if vehicle_type.lower() != "all":
        timed_df = timed_df.filter(col("vehicle_type") == vehicle_type)

    # ── Spatial bucketing: snap lat/lon to grid cells ─────────────────────
    gridded_df = timed_df \
        .withColumn("lat_grid", spark_round(col("lat"), GRID_PRECISION)) \
        .withColumn("lon_grid", spark_round(col("lon"), GRID_PRECISION))

    # ── Apply watermark for late data tolerance ────────────────────────────
    watermarked_df = gridded_df.withWatermark("event_time", WATERMARK_DELAY)

    # ── Define window ──────────────────────────────────────────────────────
    if use_sliding:
        time_window = window(col("event_time"), window_duration, slide_duration)
    else:
        time_window = window(col("event_time"), window_duration)

    # ── Aggregate pollutants per grid cell per window ──────────────────────
    agg_exprs = build_aggregation_exprs(pollutant_cols)

    aggregated_df = watermarked_df \
        .groupBy(time_window, col("lat_grid"), col("lon_grid")) \
        .agg(*agg_exprs)

    # ── Build output payload ───────────────────────────────────────────────
    # Flatten window struct and add metadata
    output_cols = [
        col("window.start").cast("string").alias("window_start"),
        col("window.end").cast("string").alias("window_end"),
        col("lat_grid").alias("lat"),
        col("lon_grid").alias("lon"),
        col("vehicle_count"),
        lit(window_type_label).alias("window_type"),
        lit(window_duration).alias("window_duration"),
        lit(",".join(pollutant_cols)).alias("pollutants"),
    ]

    # Add all aggregated pollutant columns dynamically
    for p in pollutant_cols:
        output_cols.append(col(f"{p}_avg"))
        output_cols.append(col(f"{p}_max"))

    result_df = aggregated_df.select(*output_cols)

    # ── Serialize to JSON for Kafka output ────────────────────────────────
    kafka_output_df = result_df.select(
        to_json(struct([result_df[c] for c in result_df.columns])).alias("value")
    )

    # ── Write to Kafka madrid-results topic ───────────────────────────────
    query = kafka_output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()

    print(f"[Spark] Streaming started. Writing to topic: {OUTPUT_TOPIC}")
    print(f"[Spark] Checkpoint directory: {CHECKPOINT_DIR}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
