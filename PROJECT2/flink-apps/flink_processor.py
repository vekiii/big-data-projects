#!/usr/bin/env python3
"""
PyFlink Streaming - SUMO Madrid Emission Analyzer
==================================================
Reads vehicle emission records from the 'madrid-emission' Kafka topic,
applies tumbling or sliding PROCESSING-time windows, aggregates pollution
metrics per spatial grid cell, and writes results to 'flink-results'.

Usage (inside flink-jobmanager container):
    flink run --python /opt/flink-apps/flink_processor.py \
        -- <window_duration_seconds> <slide_duration_seconds> <pollutant> <vehicle_type>

Arguments:
    window_duration_seconds : window size in seconds, e.g. 300 (5 min)
    slide_duration_seconds  : slide step in seconds, e.g. 60 (1 min); 0 for tumbling
    pollutant               : CO2 | CO | HC | NOx | PMx | noise | all
    vehicle_type            : all | veh_passenger | truck_truck

Examples:
    Tumbling 5-min window, CO2, all vehicles:
        -- 300 0 CO2 all

    Sliding 10-min window every 2 min, all pollutants:
        -- 600 120 all all
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
OUTPUT_TOPIC = "flink-results"
GRID_PRECISION = 3
ALL_POLLUTANTS = ["CO2", "CO", "HC", "NOx", "PMx", "noise"]


def parse_args():
    args = sys.argv[1:]
    if len(args) < 4:
        print(__doc__)
        sys.exit(1)
    window_seconds = int(args[0])
    slide_seconds = int(args[1])
    pollutant = args[2]
    vehicle_type = args[3]
    return window_seconds, slide_seconds, pollutant, vehicle_type


def get_pollutant_columns(pollutant: str):
    if pollutant.lower() == "all":
        return ALL_POLLUTANTS
    elif pollutant in ALL_POLLUTANTS:
        return [pollutant]
    else:
        logger.error(f"Unknown pollutant '{pollutant}'.")
        sys.exit(1)


def round_grid(value: float, precision: int) -> float:
    return round(value, precision)


class EmissionWindowFunction(ProcessWindowFunction):
    """Process each window of emission records per grid cell."""
    def __init__(self, pollutant_cols, window_type_label, window_duration, vehicle_type):
        self.pollutant_cols = pollutant_cols
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

        result = {
            "window_start": window_start,
            "window_end": window_end,
            "lat": lat_grid,
            "lon": lon_grid,
            "vehicle_count": len(records),
            "window_type": self.window_type_label,
            "window_duration": self.window_duration,
            "pollutants": ",".join(self.pollutant_cols),
            "vehicle_filter": self.vehicle_type,
            "engine": "flink",
        }

        for p in self.pollutant_cols:
            values = [r.get(p, 0.0) for r in records if r.get(p) is not None]
            if values:
                result[f"{p}_avg"] = round(sum(values) / len(values), 4)
                result[f"{p}_max"] = round(max(values), 4)
            else:
                result[f"{p}_avg"] = 0.0
                result[f"{p}_max"] = 0.0

        yield json.dumps(result)


def main():
    window_seconds, slide_seconds, pollutant_arg, vehicle_type = parse_args()
    pollutant_cols = get_pollutant_columns(pollutant_arg)
    use_sliding = slide_seconds > 0
    window_type_label = "sliding" if use_sliding else "tumbling"
    window_duration_str = f"{window_seconds} seconds"

    logger.info(f"[Flink Pollution] Window type : {window_type_label}")
    logger.info(f"[Flink Pollution] Window size : {window_seconds}s")
    if use_sliding:
        logger.info(f"[Flink Pollution] Slide step  : {slide_seconds}s")
    logger.info(f"[Flink Pollution] Pollutants  : {pollutant_cols}")
    logger.info(f"[Flink Pollution] Vehicle     : {vehicle_type}")

    # ── Environment ────────────────────────────────────────────────────────
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar")

    # ── Kafka Source ───────────────────────────────────────────────────────
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id("flink-pollution-consumer") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # ── Use ingestion time watermark (processing time) ─────────────────────
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

    # ── Build stream ───────────────────────────────────────────────────────
    stream = env.from_source(
        kafka_source,
        watermark_strategy,
        "Kafka Emission Source"
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
        EmissionWindowFunction(
            pollutant_cols,
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

    logger.info(f"[Flink Pollution] Starting job. Writing to topic: {OUTPUT_TOPIC}")
    env.execute("SUMO_Madrid_Flink_Pollution")


if __name__ == "__main__":
    main()
