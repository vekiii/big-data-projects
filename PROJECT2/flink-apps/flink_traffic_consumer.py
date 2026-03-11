#!/usr/bin/env python3
"""
Flink Traffic Map Consumer
===========================
Consumes aggregated traffic results from 'flink-traffic' Kafka topic
and renders an interactive HTML map using Folium.

Usage:
    python flink_traffic_consumer.py \
        --broker localhost:9092 \
        --output flink_traffic_map.html \
        --refresh 30

Dependencies:
    pip install kafka-python-ng folium
"""

import argparse
import json
import os
import time
import webbrowser
from kafka import KafkaConsumer
import folium

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
MADRID_CENTER = [40.4168, -3.7038]
DEFAULT_ZOOM = 14

COLOR_SCALE = [
    (0.0,  "#00cc44"),
    (0.25, "#aacc00"),
    (0.5,  "#ffaa00"),
    (0.75, "#ff5500"),
    (1.0,  "#cc0000"),
]

VEHICLE_COUNT_RANGE = (0, 50)
MIN_RADIUS = 4
MAX_RADIUS = 20


def normalize(value: float) -> float:
    lo, hi = VEHICLE_COUNT_RANGE
    if hi == lo:
        return 0.0
    return max(0.0, min(1.0, (value - lo) / (hi - lo)))


def value_to_color(norm: float) -> str:
    for i in range(len(COLOR_SCALE) - 1):
        t0, c0 = COLOR_SCALE[i]
        t1, c1 = COLOR_SCALE[i + 1]
        if t0 <= norm <= t1:
            ratio = (norm - t0) / (t1 - t0)
            r0, g0, b0 = int(c0[1:3], 16), int(c0[3:5], 16), int(c0[5:7], 16)
            r1, g1, b1 = int(c1[1:3], 16), int(c1[3:5], 16), int(c1[5:7], 16)
            r = int(r0 + ratio * (r1 - r0))
            g = int(g0 + ratio * (g1 - g0))
            b = int(b0 + ratio * (b1 - b0))
            return f"#{r:02x}{g:02x}{b:02x}"
    return COLOR_SCALE[-1][1]


def build_popup_html(record: dict) -> str:
    avg_speed_kmh = round(record.get("avg_speed_ms", 0.0) * 3.6, 1)
    lines = [
        f"<b>Grid Cell</b>: ({record['lat']:.3f}, {record['lon']:.3f})<br>",
        f"<b>Window</b>: {record.get('window_start', 'N/A')} → {record.get('window_end', 'N/A')}<br>",
        f"<b>Window type</b>: {record.get('window_type', 'N/A')} ({record.get('window_duration', 'N/A')})<br>",
        f"<b>Engine</b>: {record.get('engine', 'flink')}<br>",
        "<hr>",
        f"<b>Unique vehicles</b>: {record.get('unique_vehicles', 0)}<br>",
        f"<b>Total observations</b>: {record.get('total_records', 0)}<br>",
        f"<b>Avg speed</b>: {avg_speed_kmh} km/h<br>",
        f"<b>Vehicle filter</b>: {record.get('vehicle_filter', 'all')}<br>",
    ]
    return "".join(lines)


def render_map(grid_data, output_path, refresh_seconds, window_info):
    m = folium.Map(
        location=MADRID_CENTER,
        zoom_start=DEFAULT_ZOOM,
        tiles="CartoDB positron",
    )

    title_html = f"""
    <div style="position:fixed; top:10px; left:50%; transform:translateX(-50%);
                z-index:9999; background:white; padding:10px 20px;
                border-radius:8px; box-shadow:0 2px 6px rgba(0,0,0,0.3);
                font-family:Arial; font-size:14px;">
        <b>Madrid Traffic Monitor</b> &nbsp;|&nbsp;
        Engine: <b>Flink</b> &nbsp;|&nbsp;
        Vehicle count per grid cell &nbsp;|&nbsp;
        Window: <b>{window_info.get('duration', 'N/A')}</b> ({window_info.get('type', 'N/A')})
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    lo, hi = VEHICLE_COUNT_RANGE
    legend_html = f"""
    <div style="position:fixed; bottom:30px; right:20px; z-index:9999;
                background:white; padding:12px; border-radius:8px;
                box-shadow:0 2px 6px rgba(0,0,0,0.3); font-family:Arial; font-size:12px;">
        <b>Unique Vehicles</b><br>
        <div style="display:flex; align-items:center; margin-top:6px;">
            <div style="width:120px; height:14px;
                background:linear-gradient(to right, #00cc44, #aacc00, #ffaa00, #ff5500, #cc0000);
                border-radius:3px;"></div>
        </div>
        <div style="display:flex; justify-content:space-between; width:120px; font-size:10px;">
            <span>{lo}</span><span>{(lo+hi)//2}</span><span>{hi}+</span>
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    if refresh_seconds > 0:
        m.get_root().html.add_child(
            folium.Element(f'<meta http-equiv="refresh" content="{refresh_seconds}">')
        )

    plotted = 0
    for (lat, lon), record in grid_data.items():
        unique_vehicles = record.get("unique_vehicles", 0)
        norm = normalize(unique_vehicles)
        color = value_to_color(norm)
        radius = MIN_RADIUS + norm * (MAX_RADIUS - MIN_RADIUS)
        avg_speed_kmh = round(record.get("avg_speed_ms", 0.0) * 3.6, 1)
        popup_html = build_popup_html(record)

        folium.CircleMarker(
            location=[lat, lon],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.55,
            opacity=0.8,
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=f"Vehicles: {unique_vehicles} | Avg speed: {avg_speed_kmh} km/h",
        ).add_to(m)
        plotted += 1

    print(f"[Flink Traffic Map] Rendered {plotted} grid cells")
    m.save(output_path)
    print(f"[Flink Traffic Map] Saved → {output_path}")


def consume_and_visualize(broker, output_path, refresh_seconds):
    consumer = KafkaConsumer(
        "flink-traffic",
        bootstrap_servers=broker,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="flink-traffic-map-consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000,
    )

    print(f"[Flink Traffic Consumer] Connected to Kafka broker: {broker}")
    print(f"[Flink Traffic Consumer] Listening on topic: flink-traffic")

    grid_data = {}
    window_info = {}
    last_render_time = 0
    RENDER_INTERVAL = max(refresh_seconds, 5)
    browser_opened = False

    try:
        while True:
            try:
                batch_count = 0
                for msg in consumer:
                    record = msg.value
                    lat = record.get("lat")
                    lon = record.get("lon")
                    if lat is None or lon is None:
                        continue
                    grid_data[(lat, lon)] = record
                    batch_count += 1
                    if not window_info:
                        window_info = {
                            "type": record.get("window_type", "unknown"),
                            "duration": record.get("window_duration", "unknown"),
                        }

                if grid_data and (time.time() - last_render_time > RENDER_INTERVAL or batch_count > 0):
                    render_map(grid_data, output_path, refresh_seconds, window_info)
                    last_render_time = time.time()
                    if not browser_opened:
                        webbrowser.open(f"file://{os.path.abspath(output_path)}")
                        browser_opened = True
                elif not grid_data:
                    print("[Flink Traffic Consumer] Waiting for data from flink-traffic topic...")
                    time.sleep(2)

            except ValueError as e:
                if "Invalid file descriptor" in str(e):
                    print("[Flink Traffic Consumer] Connection reset, reconnecting...")
                    try:
                        consumer.close()
                    except Exception:
                        pass
                    time.sleep(2)
                    consumer = KafkaConsumer(
                        "flink-traffic",
                        bootstrap_servers=broker,
                        auto_offset_reset="latest",
                        enable_auto_commit=True,
                        group_id="flink-traffic-map-consumer",
                        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                        consumer_timeout_ms=5000,
                    )
                else:
                    raise

    except KeyboardInterrupt:
        print("\n[Flink Traffic Consumer] Stopped by user.")
    finally:
        consumer.close()
        print(f"[Flink Traffic Consumer] Final map saved at: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flink Traffic Map Consumer")
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--output", default="flink_traffic_map.html")
    parser.add_argument("--refresh", type=int, default=30)
    args = parser.parse_args()

    consume_and_visualize(args.broker, args.output, args.refresh)
