#!/usr/bin/env python3
"""
PyFlink Streaming - SUMO Madrid Traffic Analyzer
=================================================
Reads vehicle records from the 'madrid-emission' Kafka topic,
applies tumbling or sliding PROCESSING-time windows, counts vehicles
per spatial grid cell, and writes results to 'flink-traffic'.

Usage (inside flink-jobmanager container):
    flink run --python /opt/flink-apps/flink_traffic.py \
        -- <window_duration_seconds> <slide_duration_seconds> <vehicle_type>

Arguments:
    window_duration_seconds : window size in seconds, e.g. 300 (5 min)
    slide_duration_seconds  : slide step in seconds; 0 for tumbling
    vehicle_type            : all | veh_passenger | truck_truck

Examples:
    Tumbling 5-min window, all vehicles:
        -- 300 0 all

    Sliding 10-min window every 2 min:
        -- 600 120 all
"""

import sys
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
)
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows, Time
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.serialization import SimpleStringSchema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BROKER = "kafka-1:9092,kafka-2:9092"
INPUT_TOPIC = "madrid-emission"
OUTPUT_TOPIC = "flink-traffic"
GRID_PRECISION = 3


def parse_args():
    args = sys.argv[1:]
    if len(args) < 3:
        print(__doc__)
        sys.exit(1)
    window_seconds = int(args[0])
    slide_seconds = int(args[1])
    vehicle_type = args[2]
    return window_seconds, slide_seconds, vehicle_type


def round_grid(value: float, precision: int) -> float:
    return round(value, precision)


class TrafficWindowFunction(ProcessWindowFunction):
    """Process each window of vehicle records per grid cell."""
    def __init__(self, window_type_label, window_duration, vehicle_type):
        self.window_type_label = window_type_label
        self.window_duration = window_duration
        self.vehicle_type = vehicle_type

    def process(self, key, context, elements):
        records = list(elements)
        if not records:
            return

        lat_grid, lon_grid = key
        window = context.window()
        window_start = str(window.start)
        window_end = str(window.end)

        unique_vehicle_ids = set(r.get("vehicle_id") for r in records if r.get("vehicle_id"))
        total_records = len(records)
        speeds = [r.get("speed", 0.0) for r in records if r.get("speed") is not None]
        avg_speed = round(sum(speeds) / len(speeds), 2) if speeds else 0.0

        result = {
            "window_start": window_start,
            "window_end": window_end,
            "lat": lat_grid,
            "lon": lon_grid,
            "unique_vehicles": len(unique_vehicle_ids),
            "total_records": total_records,
            "avg_speed_ms": avg_speed,
            "window_type": self.window_type_label,
            "window_duration": self.window_duration,
            "vehicle_filter": self.vehicle_type,
            "engine": "flink",
        }

        yield json.dumps(result)


def main():
    window_seconds, slide_seconds, vehicle_type = parse_args()
    use_sliding = slide_seconds > 0
    window_type_label = "sliding" if use_sliding else "tumbling"
    window_duration_str = f"{window_seconds} seconds"

    logger.info(f"[Flink Traffic] Window type : {window_type_label}")
    logger.info(f"[Flink Traffic] Window size : {window_seconds}s")
    if use_sliding:
        logger.info(f"[Flink Traffic] Slide step  : {slide_seconds}s")
    logger.info(f"[Flink Traffic] Vehicle     : {vehicle_type}")

    # ── Environment ────────────────────────────────────────────────────────
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar")

    # ── Kafka Source ───────────────────────────────────────────────────────
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id("flink-traffic-consumer") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

    # ── Build stream ───────────────────────────────────────────────────────
    stream = env.from_source(
        kafka_source,
        watermark_strategy,
        "Kafka Emission Source Traffic"
    )

    # ── Parse JSON ─────────────────────────────────────────────────────────
    parsed_stream = stream.map(lambda s: json.loads(s))

    # ── Filter by vehicle type ─────────────────────────────────────────────
    if vehicle_type.lower() != "all":
        parsed_stream = parsed_stream.filter(
            lambda r: r.get("vehicle_type") == vehicle_type
        )

    # ── Spatial bucketing ──────────────────────────────────────────────────
    keyed_stream = parsed_stream.key_by(
        lambda r: (
            round_grid(r.get("lat", 0.0), GRID_PRECISION),
            round_grid(r.get("lon", 0.0), GRID_PRECISION)
        )
    )

    # ── Apply processing time window ───────────────────────────────────────
    if use_sliding:
        windowed_stream = keyed_stream.window(
            SlidingProcessingTimeWindows.of(
                Time.seconds(window_seconds),
                Time.seconds(slide_seconds)
            )
        )
    else:
        windowed_stream = keyed_stream.window(
            TumblingProcessingTimeWindows.of(Time.seconds(window_seconds))
        )

    # ── Process window ─────────────────────────────────────────────────────
    result_stream = windowed_stream.process(
        TrafficWindowFunction(
            window_type_label,
            window_duration_str,
            vehicle_type
        )
    ).map(lambda x: x if isinstance(x, str) else x.decode("utf-8"), output_type=Types.STRING())

    # ── Kafka Sink ─────────────────────────────────────────────────────────
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(OUTPUT_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    result_stream.sink_to(kafka_sink)

    logger.info(f"[Flink Traffic] Starting job. Writing to topic: {OUTPUT_TOPIC}")
    env.execute("SUMO_Madrid_Flink_Traffic")


if __name__ == "__main__":
    main()
