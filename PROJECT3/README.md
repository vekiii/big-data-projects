# BDS Project 3 – Spark MLlib on UQ Vital Signs Dataset
### Random Forest & GBT  ·  Classification + Regression  ·  Spark 3.1.1 / Hadoop 3.2

---

## Project structure

```
project3/                            ← project root (mapped as .:/app inside containers)
├── docker-compose.yml               ← 4 services: namenode, datanode, spark-master, spark-worker
├── prepare_data.py                  ← LOCAL script: flatten raw folders → dataset_ready_for_hdfs/
├── train_model.py                   ← Spark: load from HDFS, 70/15/15 split, train RF+GBT
├── predict.py                       ← Spark: inference across 8 parameter configs
├── visualize.py                     ← LOCAL: Matplotlib + Seaborn
├── requirements.txt
├── dataset_ready_for_hdfs/          ← created by prepare_data.py (mounted into namenode)
│   ├── uq_vsd_case01_fulldata_01.csv
│   ├── uq_vsd_case01_fulldata_02.csv
│   └── ...
└── results/                         ← pulled from HDFS after predict.py
    └── plots/                       ← created by visualize.py
```

---

## Services

| Container | Image | Port(s) | Purpose |
|---|---|---|---|
| `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | 9870, 9000 | HDFS NameNode |
| `datanode` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | – | HDFS DataNode |
| `spark-master` | `bde2020/spark-master:3.1.1-hadoop3.2` | 8080, 7077 | Spark Master |
| `spark-worker` | `bde2020/spark-worker:3.1.1-hadoop3.2` | 8081 | Spark Worker (4 cores / 8 GB) |

> **Internal HDFS address:** `hdfs://namenode:8020`  
> **Spark master address:** `spark://spark-master:7077`

---

## Step-by-step workflow

### Step 1 – Prepare data (local, no Docker needed)

Download the UQ Vital Signs dataset, unzip it, and follow the instructions from Project 1, so you have:
```
downloaded_data/
  case01/fulldata/uq_vsd_case01_fulldata_01.csv ...
  case02/fulldata/ ...
  ...
  case32/
```

Run the preparation script to flatten + sample to the desired size:
```bash
python prepare_data.py --source ./downloaded_data --target-gb 1.5
```

This creates `dataset_ready_for_hdfs/` with all selected fulldata CSVs in one flat folder
(sampled to 1.5 GB instead of the full 4.14 GB).

---

### Step 2 – Start the cluster

```bash
docker compose up -d
```

Verify at:
- Spark Master UI: http://localhost:8080
- HDFS NameNode UI: http://localhost:9870

> ⚠️ **Always use `docker compose stop` / `docker compose start`** to pause/resume.  
> Using `down` + `up` can trigger a NameNode re-format and Cluster ID mismatch,
> forcing a full re-upload of the dataset.

Install numpy in Spark:
docker exec spark-master2 apk add --no-cache py3-numpy 
docker exec spark-worker apk add --no-cache py3-numpy

---

### Step 3 – Upload data to HDFS

```bash
docker exec -it namenode hdfs dfs -mkdir -p /user/root
docker exec -it namenode hdfs dfs -put /data/incoming /user/root/dataset
```

Monitor progress at http://localhost:9870 → **DFS Used**.

---

### Step 4 – Train models

Since bde images are missing numpy, which is required for MLlib, you have to install it on Spark services. Run:

```bash

docker exec spark-master2 apk add --no-cache py3-numpy
docker exec spark-worker apk add --no-cache py3-numpy

```

```bash
docker exec -it spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 2G --executor-memory 4G \
    /app/train_model.py
```

This will:
1. Read `hdfs://namenode:8020/user/root/dataset/*.csv`
2. Clean and engineer features
3. Split **70% train / 15% val / 15% test** (random, seed=11)
4. Cross-validate RF and GBT for classification and regression
5. Save best models and test split to `hdfs://namenode:8020/user/root/models/`

You can monitor the job at localhost:4040.

---

### Step 5 – Run inference

```bash
docker exec -it spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 2G --executor-memory 4G \
    /app/predict.py
```

Evaluates across 8 configurations:
- Full test set
- Temporal: `early` / `mid` / `late` stage of the procedure
- Alarm state: `alarms_active` vs `no_alarms`

---

### Step 6 – Pull results & visualise

```bash
# Copy results from HDFS to local
docker exec namenode hdfs dfs -get /user/root/results /data/incoming/
docker cp namenode:/data/incoming/results ./results

# Generate plots (no Spark needed)
python visualize.py --results ./results --output ./results/plots
```

---

## ML Pipeline summary

```
HDFS CSV  →  clean & engineer features
              │
              randomSplit([0.70, 0.15, 0.15], seed=42)
              │
     ┌────────┴────────┐
  train (70%)     val (15%)    test (15%) → saved to HDFS as Parquet
     │
  Pipeline per model:
    Imputer(median) → VectorAssembler → StandardScaler
    │                                              │
  RF/GBT Classifier                    RF/GBT Regressor
  label: SpO2 < 95% → hypoxemia        label: HR (continuous)
  3-fold CV + ParamGrid                3-fold CV + ParamGrid
```

---

## References
- Liu D, Gorges M, Jenkins SA. *UQ Vital Signs Dataset*. Anesth Analg 2012; 114(3):584–9.
- Apache Spark MLlib 3.1.1: https://spark.apache.org/docs/3.1.1/ml-guide.html
