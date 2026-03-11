#!/usr/bin/env python3
"""
Kafka Consumer + Folium Traffic Map Visualizer
===============================================
Consumes aggregated traffic results from 'madrid-traffic' Kafka topic
and renders an interactive HTML map using Folium.

The map displays:
  - Color-coded CircleMarkers per grid cell (green→yellow→red by vehicle count)
  - Circle size proportional to number of unique vehicles
  - Popup with detailed traffic stats on click (vehicle count, avg speed, window)
  - Auto-refreshes every N seconds
  - Legend showing the vehicle count scale

Usage:
    python traffic_consumer.py \
        --broker localhost:9092 \
        --output traffic_map.html \
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

# Color thresholds for traffic circles (normalized 0–1)
# Green = low traffic, red = heavy traffic
COLOR_SCALE = [
    (0.0,  "#00cc44"),   # Very low  - green
    (0.25, "#aacc00"),   # Low       - yellow-green
    (0.5,  "#ffaa00"),   # Medium    - orange
    (0.75, "#ff5500"),   # High      - dark orange
    (1.0,  "#cc0000"),   # Very high - red
]

# Expected range of unique vehicles per grid cell per window
# Adjust based on your simulation density
VEHICLE_COUNT_RANGE = (0, 50)

# Circle radius scaling
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
        "<hr>",
        f"<b>Unique vehicles</b>: {record.get('unique_vehicles', 0)}<br>",
        f"<b>Total observations</b>: {record.get('total_records', 0)}<br>",
        f"<b>Avg speed</b>: {avg_speed_kmh} km/h<br>",
        f"<b>Vehicle filter</b>: {record.get('vehicle_filter', 'all')}<br>",
    ]
    return "".join(lines)


def render_map(
    grid_data: dict,
    output_path: str,
    refresh_seconds: int,
    window_info: dict,
):
    m = folium.Map(
        location=MADRID_CENTER,
        zoom_start=DEFAULT_ZOOM,
        tiles="CartoDB positron",
    )

    # ── Title box ─────────────────────────────────────────────────────────
    title_html = f"""
    <div style="position:fixed; top:10px; left:50%; transform:translateX(-50%);
                z-index:9999; background:white; padding:10px 20px;
                border-radius:8px; box-shadow:0 2px 6px rgba(0,0,0,0.3);
                font-family:Arial; font-size:14px;">
        <b>Madrid Traffic Monitor</b> &nbsp;|&nbsp;
        Vehicle count per grid cell &nbsp;|&nbsp;
        Window: <b>{window_info.get('duration', 'N/A')}</b> ({window_info.get('type', 'N/A')})
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    # ── Legend ────────────────────────────────────────────────────────────
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

    # ── Auto-refresh ──────────────────────────────────────────────────────
    if refresh_seconds > 0:
        refresh_html = f'<meta http-equiv="refresh" content="{refresh_seconds}">'
        m.get_root().html.add_child(folium.Element(refresh_html))

    # ── Plot CircleMarkers ─────────────────────────────────────────────────
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

    print(f"[Traffic Map] Rendered {plotted} grid cells")
    m.save(output_path)
    print(f"[Traffic Map] Saved → {output_path}")


def consume_and_visualize(broker: str, output_path: str, refresh_seconds: int):
    consumer = KafkaConsumer(
        "madrid-traffic",
        bootstrap_servers=broker,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="folium-traffic-consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000,
    )

    print(f"[Traffic Consumer] Connected to Kafka broker: {broker}")
    print(f"[Traffic Consumer] Listening on topic: madrid-traffic")

    grid_data: dict = {}
    window_info: dict = {}
    last_render_time = 0
    RENDER_INTERVAL = max(refresh_seconds, 5)
    browser_opened = False

    try:
        while True:
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
                    abs_path = os.path.abspath(output_path)
                    webbrowser.open(f"file://{abs_path}")
                    browser_opened = True

            elif not grid_data:
                print("[Traffic Consumer] Waiting for data from madrid-traffic topic...")
                time.sleep(2)

    except KeyboardInterrupt:
        print("\n[Traffic Consumer] Stopped by user.")
    finally:
        consumer.close()
        print(f"[Traffic Consumer] Final map saved at: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SUMO Traffic Map Consumer")
    parser.add_argument("--broker", default="localhost:9092",
                        help="Kafka broker address (default: localhost:9092)")
    parser.add_argument("--output", default="traffic_map.html",
                        help="Output HTML file path (default: traffic_map.html)")
    parser.add_argument("--refresh", type=int, default=30,
                        help="Map auto-refresh interval in seconds (default: 30)")
    args = parser.parse_args()

    consume_and_visualize(args.broker, args.output, args.refresh)
