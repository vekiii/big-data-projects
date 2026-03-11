#!/usr/bin/env python3
"""
Spark Structured Streaming - SUMO Madrid Traffic Analyzer
==========================================================
Reads vehicle records from the 'madrid-emission' Kafka topic,
applies tumbling or sliding time windows, counts vehicles per
spatial grid cell, and writes results to 'madrid-traffic'.

Usage (inside spark-master container):
    /spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
        /app/spark_traffic.py \
        <window_duration> <slide_duration> <vehicle_type>

Arguments:
    window_duration  : e.g. "60 seconds", "5 minutes", "10 minutes"
    slide_duration   : e.g. "30 seconds", "2 minutes"; use "0" for tumbling
    vehicle_type     : all | veh_passenger | truck_truck

Examples:
    Tumbling 5-min window, all vehicles:
        spark_traffic.py "5 minutes" "0" "all"

    Sliding 10-min window every 2 min, passengers only:
        spark_traffic.py "10 minutes" "2 minutes" "veh_passenger"

    Sliding 5-min window every 1 min, trucks only:
        spark_traffic.py "5 minutes" "1 minute" "truck_truck"
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, approx_count_distinct,
    avg, round as spark_round, lit, to_json, struct,
    from_unixtime, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BROKER = "kafka-1:9092"
INPUT_TOPIC = "madrid-emission"
OUTPUT_TOPIC = "madrid-traffic"
CHECKPOINT_DIR = "/tmp/spark-traffic-checkpoint"

# Spatial grid resolution: round lat/lon to N decimal places
# 3 decimals ≈ ~100m grid cells
GRID_PRECISION = 3

# Watermark tolerance for late data
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
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)
    window_duration = sys.argv[1]
    slide_duration = sys.argv[2]
    vehicle_type = sys.argv[3]
    return window_duration, slide_duration, vehicle_type


def main():
    window_duration, slide_duration, vehicle_type = parse_args()
    use_sliding = slide_duration != "0"
    window_type_label = "sliding" if use_sliding else "tumbling"

    print(f"[Traffic] Window type   : {window_type_label}")
    print(f"[Traffic] Window size   : {window_duration}")
    if use_sliding:
        print(f"[Traffic] Slide step    : {slide_duration}")
    print(f"[Traffic] Vehicle filter: {vehicle_type}")

    # ── Spark Session ──────────────────────────────────────────────────────
    spark = SparkSession.builder \
        .appName("SUMO_Madrid_Traffic_Streaming") \
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

    # ── Convert event_time_ms → timestamp ─────────────────────────────────
    timed_df = parsed_df.withColumn(
        "event_time",
        to_timestamp(from_unixtime(col("event_time_ms") / 1000.0))
    )

    # ── Optional: filter by vehicle type ──────────────────────────────────
    if vehicle_type.lower() != "all":
        timed_df = timed_df.filter(col("vehicle_type") == vehicle_type)

    # ── Spatial bucketing ─────────────────────────────────────────────────
    gridded_df = timed_df \
        .withColumn("lat_grid", spark_round(col("lat"), GRID_PRECISION)) \
        .withColumn("lon_grid", spark_round(col("lon"), GRID_PRECISION))

    # ── Apply watermark ────────────────────────────────────────────────────
    watermarked_df = gridded_df.withWatermark("event_time", WATERMARK_DELAY)

    # ── Define window ──────────────────────────────────────────────────────
    if use_sliding:
        time_window = window(col("event_time"), window_duration, slide_duration)
    else:
        time_window = window(col("event_time"), window_duration)

    # ── Aggregate: count total records and distinct vehicles per cell ──────
    # - total_records: how many times any vehicle was seen in this cell
    # - unique_vehicles: how many distinct vehicles passed through
    # - avg_speed: average speed of vehicles in this cell
    aggregated_df = watermarked_df \
        .groupBy(time_window, col("lat_grid"), col("lon_grid")) \
        .agg(
            count("*").alias("total_records"),
            approx_count_distinct("vehicle_id").alias("unique_vehicles"),
            spark_round(avg("speed"), 2).alias("avg_speed_ms"),
            approx_count_distinct("vehicle_type").alias("vehicle_type_count")
        )

    # ── Build output payload ───────────────────────────────────────────────
    result_df = aggregated_df.select(
        col("window.start").cast("string").alias("window_start"),
        col("window.end").cast("string").alias("window_end"),
        col("lat_grid").alias("lat"),
        col("lon_grid").alias("lon"),
        col("total_records"),
        col("unique_vehicles"),
        col("avg_speed_ms"),
        col("vehicle_type_count"),
        lit(window_type_label).alias("window_type"),
        lit(window_duration).alias("window_duration"),
        lit(vehicle_type).alias("vehicle_filter"),
    )

    # ── Serialize to JSON for Kafka output ─────────────────────────────────
    kafka_output_df = result_df.select(
        to_json(struct([result_df[c] for c in result_df.columns])).alias("value")
    )

    # ── Write to Kafka madrid-traffic topic ───────────────────────────────
    query = kafka_output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()

    print(f"[Traffic] Streaming started. Writing to topic: {OUTPUT_TOPIC}")
    print(f"[Traffic] Checkpoint directory: {CHECKPOINT_DIR}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
