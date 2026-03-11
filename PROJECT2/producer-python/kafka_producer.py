#!/usr/bin/env python3
"""
Kafka Producer for SUMO emission data.
Parses emission_data.xml and streams JSON records to the 'madrid-emission' Kafka topic.

Uses the exact netOffset and UTM projection from osm.net.xml for precise
WGS84 lat/lon conversion (centimeter-level accuracy).

Usage:
    python kafka_producer.py --file /path/to/emission_data.xml [--broker localhost:9092] [--delay 0.005]

Dependencies:
    pip install kafka-python pyproj
"""

import argparse
import json
import time
import xml.etree.ElementTree as ET
from kafka import KafkaProducer
from pyproj import Proj, Transformer

# ─────────────────────────────────────────────────────────────────────────────
# Exact values extracted from osm.net.xml <location> tag:
#   netOffset="-436207.35,-4465685.32"
#   projParameter="+proj=utm +zone=30 +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
# ─────────────────────────────────────────────────────────────────────────────
NET_OFFSET_X = -436207.35
NET_OFFSET_Y = -4465685.32

# UTM Zone 30N → WGS84 transformer
_utm_proj = Proj(proj="utm", zone=30, ellps="WGS84", datum="WGS84", units="m")
_wgs84_transformer = Transformer.from_proj(_utm_proj, Proj(proj="latlong", datum="WGS84"))


def sumo_xy_to_latlon(x: float, y: float):
    """
    Convert SUMO local x/y (meters) to WGS84 lat/lon with full precision.

    Steps:
      1. Reverse the netOffset to get UTM Zone 30N coordinates
      2. Convert UTM → WGS84 using pyproj
    """
    utm_x = x - NET_OFFSET_X   # i.e. x + 436207.35
    utm_y = y - NET_OFFSET_Y   # i.e. y + 4465685.32
    lon, lat = _wgs84_transformer.transform(utm_x, utm_y)
    return round(lat, 6), round(lon, 6)


def parse_and_stream(xml_file: str, broker: str, delay: float):
    """
    Iteratively parse the emission XML using iterparse (memory-efficient),
    and send each vehicle record as a JSON message to Kafka.
    """
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
    )

    print(f"[Producer] Connected to Kafka broker: {broker}")
    print(f"[Producer] Streaming from: {xml_file}")

    # Quick sanity check: convert a known SUMO coordinate
    # truck0 starts at x=3701.62, y=9001.43 → should be somewhere in Madrid
    test_lat, test_lon = sumo_xy_to_latlon(3701.62, 9001.43)
    print(f"[Producer] Coordinate check: (3701.62, 9001.43) → ({test_lat}, {test_lon})")
    print(f"[Producer] Expected: somewhere around (40.37–40.45, -3.75–-3.58)")

    current_timestep = 0.0
    record_count = 0

    # iterparse is memory-efficient for large XML files
    context = ET.iterparse(xml_file, events=("start", "end"))

    for event, elem in context:
        if event == "start" and elem.tag == "timestep":
            current_timestep = float(elem.get("time", 0.0))

        elif event == "end" and elem.tag == "vehicle":
            x = float(elem.get("x", 0.0))
            y = float(elem.get("y", 0.0))
            lat, lon = sumo_xy_to_latlon(x, y)

            record = {
                "timestep":     current_timestep,
                "vehicle_id":   elem.get("id"),
                "vehicle_type": elem.get("type"),
                "eclass":       elem.get("eclass"),
                "lat":          lat,
                "lon":          lon,
                "speed":        float(elem.get("speed", 0.0)),
                "angle":        float(elem.get("angle", 0.0)),
                # Emission attributes
                "CO2":   float(elem.get("CO2",   0.0)),
                "CO":    float(elem.get("CO",    0.0)),
                "HC":    float(elem.get("HC",    0.0)),
                "NOx":   float(elem.get("NOx",   0.0)),
                "PMx":   float(elem.get("PMx",   0.0)),
                "fuel":  float(elem.get("fuel",  0.0)),
                "noise": float(elem.get("noise", 0.0)),
                # Use timestep as event timestamp (ms) for Spark watermarking
                "event_time_ms": int(current_timestep * 1000),
            }

            producer.send("madrid-emission", value=record)
            record_count += 1

            if record_count % 1000 == 0:
                print(f"[Producer] Sent {record_count} records | timestep={current_timestep:.1f}s")

            # Clear element to free memory
            elem.clear()

            # Throttle to simulate real-time streaming
            if delay > 0:
                time.sleep(delay)

    producer.flush()
    producer.close()
    print(f"[Producer] Done. Total records sent: {record_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SUMO Emission Kafka Producer")
    parser.add_argument("--file", required=True, help="Path to emission_data.xml")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--delay", type=float, default=0.005,
                        help="Delay between records in seconds (0 = as fast as possible)")
    args = parser.parse_args()

    parse_and_stream(args.file, args.broker, args.delay)
