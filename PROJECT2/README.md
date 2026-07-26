# PROJECT2 – Real-Time Madrid Traffic & Pollution Monitoring

**Author:** Vedran Mitić 2123

A real-time streaming pipeline that simulates Madrid city traffic using SUMO, streams vehicle emission and traffic data through Apache Kafka, processes it with both Apache Spark Structured Streaming and Apache Flink, and visualizes the results on an interactive Folium map of Madrid.

---

## Architecture

```
emission_data.xml ──► kafka_producer.py ──► madrid-emission (Kafka)
                                                      │
                    ┌─────────────────────────────────┼─────────────────────────────────┐
                    ▼                                 ▼                                 ▼
          spark_processor.py              spark_traffic.py              flink_processor.py
          ► madrid-results                ► madrid-traffic              ► flink-results
                    │                                 │                                 │
                    ▼                                 ▼                                 ▼
          map_consumer.py             traffic_consumer.py        flink_map_consumer.py
          (pollution map)             (traffic map)              (flink pollution map)
```

---

## Project Structure

```
PROJECT2/
├── docker/
│   └── docker-compose.yml          # Kafka, Spark, Flink cluster
├── flink-apps/
│   ├── flink_processor.py          # Flink pollution job (runs inside flink-jobmanager)
│   ├── flink_traffic.py            # Flink traffic job (runs inside flink-jobmanager)
│   ├── flink_map_consumer.py       # Flink pollution map visualizer (runs locally)
│   └── flink_traffic_consumer.py  # Flink traffic map visualizer (runs locally)
├── producer-python/
│   └── kafka_producer.py           # Merges XMLs, converts coords, streams to Kafka (runs locally)
├── spark-apps/
│   ├── spark_processor.py          # Spark pollution job (runs inside spark-master)
│   ├── spark_traffic.py            # Spark traffic job (runs inside spark-master)
│   ├── map_consumer.py             # Spark pollution map visualizer (runs locally)
│   └── traffic_consumer.py        # Spark traffic map visualizer (runs locally)
└── sumo-scenario/
    ├── emission_data.xml           # SUMO emission output (CO2, CO, HC, NOx, PMx, noise)
    └── fcd_data.xml                # SUMO floating car data (vehicle positions, speed)
```

> ⚠️ `emission_data.xml` (1.4 GB) and `fcd_data.xml` (686 MB) are excluded from this repository due to GitHub's file size limit. Generate them using SUMO or obtain them separately.

---

## Docker Cluster

| Container | Image | Ports |
|---|---|---|
| kafka-1 | apache/kafka:3.9.2 | 9092 |
| kafka-2 | apache/kafka:3.9.2 | 9094 |
| spark-master | bde2020/spark-master:3.1.1-hadoop3.2 | 8080, 7077 |
| spark-worker-1 | bde2020/spark-worker:3.1.1-hadoop3.2 | 8082 |
| spark-worker-2 | bde2020/spark-worker:3.1.1-hadoop3.2 | 8083 |
| flink-jobmanager | flink:2.2.0 | 8081 |
| flink-taskmanager | flink:2.2.0 | — |

All containers are on the external Docker network `bde`.

### Starting the cluster

```powershell
docker network create bde
docker compose up -d
```

---

## Setup

### 1. Install Python into clusters 

```powershell
docker exec -u root flink-jobmanager bash -c "apt-get update && apt-get install -y python3 python3-pip"
docker exec -u root flink-taskmanager bash -c "apt-get update && apt-get install -y python3 python3-pip"
```
**Install local Python dependencies**
```powershell
pip install kafka-python-ng pyproj folium
```

### 2. Install PyFlink inside Flink containers

```powershell
docker exec -u root flink-jobmanager pip3 install apache-flink --break-system-packages
docker exec -u root flink-taskmanager pip3 install apache-flink --break-system-packages
docker exec -u root flink-jobmanager ln -sf /usr/bin/python3 /usr/bin/python
docker exec -u root flink-taskmanager ln -sf /usr/bin/python3 /usr/bin/python
```

### 3. Download Kafka connector JAR for Flink (on both containers)

```powershell
docker exec flink-jobmanager wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar
docker exec flink-taskmanager wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar
```

### 4. Create Kafka topics

```powershell
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic madrid-emission --bootstrap-server kafka-1:9092 --partitions 8 --replication-factor 1
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic madrid-results --bootstrap-server kafka-1:9092 --partitions 8 --replication-factor 1
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic madrid-traffic --bootstrap-server kafka-1:9092 --partitions 8 --replication-factor 1
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic flink-results --bootstrap-server kafka-1:9092 --partitions 8 --replication-factor 1
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic flink-traffic --bootstrap-server kafka-1:9092 --partitions 8 --replication-factor 1
```

---

## Running the Pipeline

### Kafka Producer (runs locally)

```powershell
python producer-python/kafka_producer.py --file "sumo-scenario/emission_data.xml" --broker localhost:9092 --delay 0.05
```

### Spark – Pollution Job (runs inside spark-master)

```powershell
docker exec -it spark-master /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 /app/spark_processor.py "10 minutes" "2 minutes" "CO2" "all"
```

### Spark – Traffic Job (runs inside spark-master)

```powershell
docker exec -it spark-master /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 /app/spark_traffic.py "2 minutes" "30 seconds" "all"
```

### Flink – Pollution Job (runs inside flink-jobmanager)

```powershell
docker exec flink-jobmanager flink run --python /opt/flink-apps/flink_processor.py -- 600 120 CO2 all
```

### Flink – Traffic Job (runs inside flink-jobmanager)

```powershell
docker exec flink-jobmanager flink run --python /opt/flink-apps/flink_traffic.py -- 600 120 all
```

### Map Consumers (run locally)

```powershell
# Spark pollution map
python spark-apps/map_consumer.py --broker localhost:9092 --pollutant CO2 --output pollution_map.html --refresh 15

# Spark traffic map
python spark-apps/traffic_consumer.py --broker localhost:9092 --output traffic_map.html --refresh 30

# Flink pollution map
python flink-apps/flink_map_consumer.py --broker localhost:9092 --pollutant CO2 --output flink_pollution_map.html --refresh 30

# Flink traffic map
python flink-apps/flink_traffic_consumer.py --broker localhost:9092 --output flink_traffic_map.html --refresh 30
```

---

## Resetting Between Runs

```powershell
# Clear Spark checkpoints
docker exec spark-master rm -rf /tmp/spark-checkpoint
docker exec spark-master rm -rf /tmp/spark-traffic-checkpoint

# Delete and recreate topics (wait ~10 seconds between delete and create)
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --delete --topic madrid-emission --bootstrap-server kafka-1:9092
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --delete --topic flink-results --bootstrap-server kafka-1:9092
# ... then recreate as shown in Setup step 4
```

---

## Spark vs. Flink Comparison

| Metric | Flink | Spark |
|---|---|---|
| Processing model | Continuous streaming | Micro-batch |
| CPU pattern | Steady ~25% | Bursts up to ~97%, idle ~0.68% |
| Avg. CPU usage | 25.71% (TaskManager) | 25.5% |
| Memory usage | 1.391 GiB (18.3%) | 1.257 GiB (16.5%) |
| Latency | Low (record by record) | Higher (waits for batch trigger) |
| First result after | ~2 minutes | ~10 minutes (first full window) |
| Compression ratio | 11,706 → 2,465 records (~21%) | N/A |
| Network I/O (TaskManager) | 832 MB in / 58.4 MB out | N/A |

---

## Coordinate Conversion

SUMO outputs vehicle positions in a local Cartesian coordinate system. The producer converts them to WGS84 lat/lon using:

- `netOffset = (-436207.35, -4465685.32)` from `osm.net.xml`
- Projection: UTM Zone 30N via `pyproj`

Madrid simulation bounding box: `40.34°N–40.46°N`, `3.75°W–3.58°W`

---

## Dashboards

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Flink Dashboard | http://localhost:8081 |
| Kafka broker 1 | localhost:9092 |
| Kafka broker 2 | localhost:9094 |
