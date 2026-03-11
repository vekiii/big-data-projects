#!/usr/bin/env python3
"""
Kafka Consumer + Folium Map Visualizer
=======================================
Consumes aggregated pollution results from 'madrid-results' Kafka topic
and renders an interactive HTML map using Folium.

The map displays:
  - Color-coded CircleMarkers per grid cell (green→yellow→red by intensity)
  - Popup with detailed pollution stats on click
  - Auto-refreshes every N seconds (configurable)
  - Legend showing the pollutant scale

Usage:
    python map_consumer.py \
        --broker localhost:9092 \
        --pollutant CO2 \
        --output /path/to/output/map.html \
        --refresh 15

Dependencies:
    pip install kafka-python folium
"""

import argparse
import json
import os
import time
import threading
import webbrowser
from collections import defaultdict
from kafka import KafkaConsumer
import folium
from folium.plugins import HeatMap

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
MADRID_CENTER = [40.4168, -3.7038]
DEFAULT_ZOOM = 14

# Color thresholds for pollution circles (normalized 0–1)
# Colors: green → yellow → orange → red
COLOR_SCALE = [
    (0.0,  "#00cc44"),   # Very low  - green
    (0.25, "#aacc00"),   # Low       - yellow-green
    (0.5,  "#ffaa00"),   # Medium    - orange
    (0.75, "#ff5500"),   # High      - dark orange
    (1.0,  "#cc0000"),   # Very high - red
]

# Known pollutant ranges for normalization (mg/s per vehicle approx.)
# Used to scale circle color and radius. Adjust based on your simulation.
POLLUTANT_RANGES = {
    "CO2":   (0,    5000),
    "CO":    (0,    50),
    "HC":    (0,    5),
    "NOx":   (0,    5),
    "PMx":   (0,    5),
    "noise": (40,   90),   # dB
    "fuel":  (0,    2000),
}

# Circle radius scaling (meters): min and max on the map
MIN_RADIUS = 30
MAX_RADIUS = 200


def normalize(value: float, pollutant: str) -> float:
    """Normalize a pollution value to [0, 1] using known ranges."""
    lo, hi = POLLUTANT_RANGES.get(pollutant, (0, 1))
    if hi == lo:
        return 0.0
    return max(0.0, min(1.0, (value - lo) / (hi - lo)))


def value_to_color(norm: float) -> str:
    """Interpolate between COLOR_SCALE entries based on normalized value."""
    for i in range(len(COLOR_SCALE) - 1):
        t0, c0 = COLOR_SCALE[i]
        t1, c1 = COLOR_SCALE[i + 1]
        if t0 <= norm <= t1:
            # Linear interpolation between two hex colors
            ratio = (norm - t0) / (t1 - t0)
            r0, g0, b0 = int(c0[1:3], 16), int(c0[3:5], 16), int(c0[5:7], 16)
            r1, g1, b1 = int(c1[1:3], 16), int(c1[3:5], 16), int(c1[5:7], 16)
            r = int(r0 + ratio * (r1 - r0))
            g = int(g0 + ratio * (g1 - g0))
            b = int(b0 + ratio * (b1 - b0))
            return f"#{r:02x}{g:02x}{b:02x}"
    return COLOR_SCALE[-1][1]


def build_popup_html(record: dict, pollutant: str) -> str:
    """Build an HTML popup string for a grid cell marker."""
    lines = [
        f"<b>Grid Cell</b>: ({record['lat']:.3f}, {record['lon']:.3f})<br>",
        f"<b>Window</b>: {record.get('window_start', 'N/A')} → {record.get('window_end', 'N/A')}<br>",
        f"<b>Vehicles</b>: {record.get('vehicle_count', 'N/A')}<br>",
        "<hr>",
    ]
    # Show all available pollutant stats
    all_p = ["CO2", "CO", "HC", "NOx", "PMx", "noise"]
    for p in all_p:
        avg_key = f"{p}_avg"
        max_key = f"{p}_max"
        if avg_key in record:
            unit = "dB" if p == "noise" else "mg/s"
            lines.append(
                f"<b>{p}</b>: avg={record[avg_key]:.2f} | max={record[max_key]:.2f} {unit}<br>"
            )
    return "".join(lines)


def render_map(
    grid_data: dict,
    pollutant: str,
    output_path: str,
    refresh_seconds: int,
    window_info: dict,
):
    """
    Render a Folium map with CircleMarkers for each grid cell.
    grid_data: { (lat, lon): latest_record_dict }
    """
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
        <b>Madrid Pollution Monitor</b> &nbsp;|&nbsp;
        Pollutant: <b>{pollutant}</b> &nbsp;|&nbsp;
        Window: <b>{window_info.get('duration', 'N/A')}</b> ({window_info.get('type', 'N/A')})
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    # ── Legend ────────────────────────────────────────────────────────────
    lo, hi = POLLUTANT_RANGES.get(pollutant, (0, 1))
    unit = "dB" if pollutant == "noise" else "mg/s"
    legend_html = f"""
    <div style="position:fixed; bottom:30px; right:20px; z-index:9999;
                background:white; padding:12px; border-radius:8px;
                box-shadow:0 2px 6px rgba(0,0,0,0.3); font-family:Arial; font-size:12px;">
        <b>{pollutant} Level ({unit})</b><br>
        <div style="display:flex; align-items:center; margin-top:6px;">
            <div style="width:120px; height:14px;
                background:linear-gradient(to right, #00cc44, #aacc00, #ffaa00, #ff5500, #cc0000);
                border-radius:3px;"></div>
        </div>
        <div style="display:flex; justify-content:space-between; width:120px; font-size:10px;">
            <span>{lo}</span><span>{(lo+hi)//2}</span><span>{hi}</span>
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # ── Auto-refresh meta tag ─────────────────────────────────────────────
    if refresh_seconds > 0:
        refresh_html = f'<meta http-equiv="refresh" content="{refresh_seconds}">'
        m.get_root().html.add_child(folium.Element(refresh_html))

    # ── Plot CircleMarkers ─────────────────────────────────────────────────
    avg_key = f"{pollutant}_avg"
    plotted = 0

    for (lat, lon), record in grid_data.items():
        value = record.get(avg_key, 0.0)
        norm = normalize(value, pollutant)
        color = value_to_color(norm)
        radius = MIN_RADIUS + norm * (MAX_RADIUS - MIN_RADIUS)

        popup_html = build_popup_html(record, pollutant)

        folium.CircleMarker(
            location=[lat, lon],
            radius=max(5, radius / 10),   # Folium radius is in pixels; divide for display
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.55,
            opacity=0.8,
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=f"{pollutant}: {value:.1f} {unit}",
        ).add_to(m)
        plotted += 1

    print(f"[Map] Rendered {plotted} grid cells | pollutant={pollutant}")
    m.save(output_path)
    print(f"[Map] Saved → {output_path}")


def consume_and_visualize(
    broker: str,
    pollutant: str,
    output_path: str,
    refresh_seconds: int,
):
    """
    Main loop: consume from madrid-results, accumulate latest record per
    grid cell, and re-render the Folium map on every batch.
    """
    consumer = KafkaConsumer(
        "madrid-results",
        bootstrap_servers=broker,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="folium-map-consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000,   # 5s poll timeout
    )

    print(f"[Consumer] Connected to Kafka broker: {broker}")
    print(f"[Consumer] Listening on topic: madrid-results")
    print(f"[Consumer] Visualizing pollutant: {pollutant}")

    # grid_data stores the latest result per (lat, lon) cell
    grid_data: dict = {}
    window_info: dict = {}
    last_render_time = 0
    RENDER_INTERVAL = max(refresh_seconds, 5)   # Minimum 5s between renders

    # Open the map in browser on first render
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

                # Capture window metadata for the map title
                if not window_info:
                    window_info = {
                        "type": record.get("window_type", "unknown"),
                        "duration": record.get("window_duration", "unknown"),
                    }

            if grid_data and (time.time() - last_render_time > RENDER_INTERVAL or batch_count > 0):
                render_map(grid_data, pollutant, output_path, refresh_seconds, window_info)
                last_render_time = time.time()

                if not browser_opened:
                    abs_path = os.path.abspath(output_path)
                    webbrowser.open(f"file://{abs_path}")
                    browser_opened = True

            elif not grid_data:
                print("[Consumer] Waiting for data from madrid-results topic...")
                time.sleep(2)

    except KeyboardInterrupt:
        print("\n[Consumer] Stopped by user.")
    finally:
        consumer.close()
        print(f"[Consumer] Final map saved at: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SUMO Pollution Map Consumer")
    parser.add_argument("--broker", default="localhost:9092",
                        help="Kafka broker address (default: localhost:9092)")
    parser.add_argument("--pollutant", default="CO2",
                        choices=["CO2", "CO", "HC", "NOx", "PMx", "noise", "all"],
                        help="Pollutant to visualize (default: CO2)")
    parser.add_argument("--output", default="pollution_map.html",
                        help="Output HTML file path (default: pollution_map.html)")
    parser.add_argument("--refresh", type=int, default=15,
                        help="Map auto-refresh interval in seconds (default: 15, 0=disable)")
    args = parser.parse_args()

    consume_and_visualize(args.broker, args.pollutant, args.output, args.refresh)
