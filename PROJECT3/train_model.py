"""
train_model.py
──────────────
Reads the UQ Vital Signs fulldata CSVs from HDFS, splits them 70/15/15,
trains Random Forest and GBT models (classification + regression) with
cross-validation, and saves the best models back to HDFS.

Two ML tasks:
  • Classification : SpO2 < 95 % → hypoxemia (label=1), else normal (label=0)
  • Regression     : predict Heart Rate (HR) from the other vital-sign features

Run inside the cluster:
    docker exec -it spark-master /spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 2G --executor-memory 4G \
        /app/train_model.py

Run locally (for quick testing):
    python train_model.py --local
"""

import argparse
import json
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ── HDFS paths (matching Project 1 layout) ───────────────────────────────────
HDFS_NAMENODE  = "hdfs://namenode:8020"
HDFS_DATASET   = f"{HDFS_NAMENODE}/user/root/dataset/*.csv"
HDFS_MODELS    = f"{HDFS_NAMENODE}/user/root/models"
HDFS_RESULTS   = f"{HDFS_NAMENODE}/user/root/results"

# ── Feature columns (numerical vital signs) ──────────────────────────────────
FEATURE_COLS = [
    "HR", "Pulse", "Perf",
    "etCO2", "awRR",
    "NBP_Sys", "NBP_Dia", "NBP_Mean",
    "ART_Sys", "ART_Dias", "ART_Mean",
    "Temp", "BIS", "RR",
    "Tidal_Volume", "Minute_Volume",
    "etO2", "inO2",
    "MAC", "etSEV", "inSEV",
    "Num_Patient_Alarms",
    "pulse_pressure_art",
    "pulse_pressure_nbp",
]

CLASS_LABEL = "label_classification"   # SpO2 < 95 → 1, else 0
REG_LABEL   = "label_regression"       # Heart Rate (continuous)
SPO2_THRESHOLD = 95.0

# ── Train/val/test split ratios ───────────────────────────────────────────────
TRAIN_RATIO = 0.70
VAL_RATIO   = 0.15
TEST_RATIO  = 0.15


# ─────────────────────────────────────────────────────────────────────────────
def build_spark(local: bool) -> SparkSession:
    master = "local[*]" if local else "spark://spark-master:7077"
    return (
        SparkSession.builder
        .appName("BDS_Project3_VitalSigns")
        .master(master)
        .config("spark.sql.shuffle.partitions", "80")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
def load_and_clean(spark: SparkSession, hdfs_path: str):
    """
    Read all CSV files from HDFS into a single DataFrame.
    Adds case_name column (filename without path, matching Project 1 approach).
    Sanitises column names and casts to Double.
    """
    df = (
        spark.read
        .csv(hdfs_path, header=True, inferSchema=True)
        .withColumn("case_name", F.input_file_name())
    )

    # Clean case_name: keep only the filename, not the full HDFS path
    df = df.withColumn(
        "case_name",
        F.element_at(F.split(F.col("case_name"), "/"), -1)
    )

    # Rename columns with special characters to safe identifiers
    rename_map = {
        "NBP (Sys)":    "NBP_Sys",
        "NBP (Dia)":    "NBP_Dia",
        "NBP (Mean)":   "NBP_Mean",
        "NBP (Pulse)":  "NBP_Pulse",
        "NBP (Time Remaining)": "NBP_Time_Remaining",
        "ART (Sys)":    "ART_Sys",
        "ART (Dias)":   "ART_Dias",
        "ART (Mean)":   "ART_Mean",
        "ST-II":        "ST_II",
        "Set I:E ratio":"Set_IE_ratio",
        "Set Mechanical Ventilation": "Set_Mech_Vent",
        "Tidal Volume":             "Tidal_Volume",
        "Minute Volume":            "Minute_Volume",
        "Set Tidal Volume":         "Set_Tidal_Volume",
        "Set RR":                   "Set_RR",
        "Set PEEP":                 "Set_PEEP",
        "Set PAWmax":               "Set_PAWmax",
        "Set PAWmin":               "Set_PAWmin",
        "Tidal Volume Exp (Spiro)": "TV_Exp_Spiro",
        "Tidal Volume In (Spiro)":  "TV_In_Spiro",
        "Minute Volume Exp (Spiro)":"MV_Exp_Spiro",
        "Minute Volume In (Spiro)": "MV_In_Spiro",
        "Lung Compliance (Spiro)":  "Lung_Compliance",
        "Airway Resistance (Spiro)":"Airway_Resistance",
        "Max Inspiratory Pressure (Spiro)": "Max_Insp_Pressure",
        "Num Patient Alarms":       "Num_Patient_Alarms",
        "Num Technical Alarms":     "Num_Technical_Alarms",
        "AWP-Spiro":                "AWP_Spiro",
        "AWF-Spiro":                "AWF_Spiro",
        "AWV-Spiro":                "AWV_Spiro",
    }
    for orig, clean in rename_map.items():
        if orig in df.columns:
            df = df.withColumnRenamed(orig, clean)

    # Drop rows with invalid SpO2 or HR
    df = df.filter(
        F.col("SpO2").isNotNull() & F.col("HR").isNotNull() &
        (F.col("SpO2").cast("double") >= 0) &
        (F.col("SpO2").cast("double") <= 100) &
        (F.col("HR").cast("double") > 0) &
        (F.col("HR").cast("double") < 300)
    )

    # Cast key numeric columns to double
    for col in [c for c in df.columns
                if c not in ("Time", "Clock", "case_name") and
                   not c.startswith("Alarm")]:
        df = df.withColumn(col, F.col(col).cast("double"))

    # ── Engineered features ───────────────────────────────────────────────────
    df = df.withColumn(
        "pulse_pressure_art",
        F.when(F.col("ART_Sys").isNotNull() & F.col("ART_Dias").isNotNull(),
               F.col("ART_Sys") - F.col("ART_Dias"))
    )
    df = df.withColumn(
        "pulse_pressure_nbp",
        F.when(F.col("NBP_Sys").isNotNull() & F.col("NBP_Dia").isNotNull(),
               F.col("NBP_Sys") - F.col("NBP_Dia"))
    )

    # ── Labels ────────────────────────────────────────────────────────────────
    df = df.withColumn(
        CLASS_LABEL,
        F.when(F.col("SpO2") < SPO2_THRESHOLD, 1).otherwise(0).cast(IntegerType())
    )
    df = df.withColumn(REG_LABEL, F.col("HR").cast("double"))

    return df


# ─────────────────────────────────────────────────────────────────────────────
def preprocessing_stages(feature_cols: list) -> list:
    """Imputer → VectorAssembler → StandardScaler pipeline stages."""
    imputed = [c + "_imp" for c in feature_cols]
    imputer = Imputer(inputCols=feature_cols, outputCols=imputed, strategy="median")
    assembler = VectorAssembler(inputCols=imputed, outputCol="raw_features",
                                handleInvalid="skip")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                            withStd=True, withMean=False)
    return [imputer, assembler, scaler]


# ─────────────────────────────────────────────────────────────────────────────
#  CLASSIFICATION
# ─────────────────────────────────────────────────────────────────────────────
def train_rf_classifier(train_df, val_df, models_path: str) -> dict:
    print("\n[train] ── Random Forest Classifier ─────────────────────────")
    prep = preprocessing_stages(FEATURE_COLS)
    rf   = RandomForestClassifier(labelCol=CLASS_LABEL, featuresCol="features", seed=11)
    pipeline = Pipeline(stages=prep + [rf])

    grid = (ParamGridBuilder()
            .addGrid(rf.numTrees,             [50, 100])
            .addGrid(rf.maxDepth,             [5, 10])
            .addGrid(rf.minInstancesPerNode,  [1, 5])
            .build())

    cv = CrossValidator(
        estimator=pipeline, estimatorParamMaps=grid,
        evaluator=BinaryClassificationEvaluator(labelCol=CLASS_LABEL, metricName="areaUnderROC"),
        numFolds=3, seed=11, parallelism=2
    )

    t0 = time.time()
    best = cv.fit(train_df).bestModel
    elapsed = time.time() - t0

    preds = best.transform(val_df)
    metrics = _cls_metrics(preds, "RF_Classifier", elapsed)
    _print_cls(metrics)

    best.write().overwrite().save(f"{models_path}/rf_classifier")
    print(f"[train] Saved → {models_path}/rf_classifier")
    return metrics


def train_gbt_classifier(train_df, val_df, models_path: str) -> dict:
    print("\n[train] ── GBT Classifier ────────────────────────────────────")
    prep = preprocessing_stages(FEATURE_COLS)
    gbt  = GBTClassifier(labelCol=CLASS_LABEL, featuresCol="features", seed=11)
    pipeline = Pipeline(stages=prep + [gbt])

    grid = (ParamGridBuilder()
            .addGrid(gbt.maxIter,  [20, 50])
            .addGrid(gbt.maxDepth, [4, 6])
            .addGrid(gbt.stepSize, [0.1, 0.05])
            .build())

    cv = CrossValidator(
        estimator=pipeline, estimatorParamMaps=grid,
        evaluator=BinaryClassificationEvaluator(labelCol=CLASS_LABEL, metricName="areaUnderROC"),
        numFolds=3, seed=11, parallelism=2
    )

    t0 = time.time()
    best = cv.fit(train_df).bestModel
    elapsed = time.time() - t0

    preds = best.transform(val_df)
    metrics = _cls_metrics(preds, "GBT_Classifier", elapsed)
    _print_cls(metrics)

    best.write().overwrite().save(f"{models_path}/gbt_classifier")
    print(f"[train] Saved → {models_path}/gbt_classifier")
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
#  REGRESSION
# ─────────────────────────────────────────────────────────────────────────────
REG_FEATURE_COLS = [c for c in FEATURE_COLS if c != "HR"]


def train_rf_regressor(train_df, val_df, models_path: str) -> dict:
    print("\n[train] ── Random Forest Regressor ──────────────────────────")
    prep = preprocessing_stages(REG_FEATURE_COLS)
    rf   = RandomForestRegressor(labelCol=REG_LABEL, featuresCol="features", seed=11)
    pipeline = Pipeline(stages=prep + [rf])

    grid = (ParamGridBuilder()
            .addGrid(rf.numTrees, [50, 100])
            .addGrid(rf.maxDepth, [5, 10])
            .build())

    cv = CrossValidator(
        estimator=pipeline, estimatorParamMaps=grid,
        evaluator=RegressionEvaluator(labelCol=REG_LABEL, metricName="rmse"),
        numFolds=3, seed=11, parallelism=2
    )

    t0 = time.time()
    best = cv.fit(train_df).bestModel
    elapsed = time.time() - t0

    preds = best.transform(val_df)
    metrics = _reg_metrics(preds, "RF_Regressor", elapsed)
    _print_reg(metrics)

    best.write().overwrite().save(f"{models_path}/rf_regressor")
    print(f"[train] Saved → {models_path}/rf_regressor")
    return metrics


def train_gbt_regressor(train_df, val_df, models_path: str) -> dict:
    print("\n[train] ── GBT Regressor ─────────────────────────────────────")
    prep = preprocessing_stages(REG_FEATURE_COLS)
    gbt  = GBTRegressor(labelCol=REG_LABEL, featuresCol="features", seed=11)
    pipeline = Pipeline(stages=prep + [gbt])

    grid = (ParamGridBuilder()
            .addGrid(gbt.maxIter,  [20, 50])
            .addGrid(gbt.maxDepth, [4, 6])
            .addGrid(gbt.stepSize, [0.1, 0.05])
            .build())

    cv = CrossValidator(
        estimator=pipeline, estimatorParamMaps=grid,
        evaluator=RegressionEvaluator(labelCol=REG_LABEL, metricName="rmse"),
        numFolds=3, seed=11, parallelism=2
    )

    t0 = time.time()
    best = cv.fit(train_df).bestModel
    elapsed = time.time() - t0

    preds = best.transform(val_df)
    metrics = _reg_metrics(preds, "GBT_Regressor", elapsed)
    _print_reg(metrics)

    best.write().overwrite().save(f"{models_path}/gbt_regressor")
    print(f"[train] Saved → {models_path}/gbt_regressor")
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
#  Metric helpers
# ─────────────────────────────────────────────────────────────────────────────
def _cls_metrics(preds, model_name: str, elapsed: float) -> dict:
    bi = BinaryClassificationEvaluator(labelCol=CLASS_LABEL, metricName="areaUnderROC")
    mc = MulticlassClassificationEvaluator(labelCol=CLASS_LABEL, predictionCol="prediction")
    return dict(
        model=model_name, train_time_s=round(elapsed, 1),
        auc=round(bi.evaluate(preds), 4),
        accuracy=round(mc.setMetricName("accuracy").evaluate(preds), 4),
        f1=round(mc.setMetricName("f1").evaluate(preds), 4),
        precision=round(mc.setMetricName("weightedPrecision").evaluate(preds), 4),
        recall=round(mc.setMetricName("weightedRecall").evaluate(preds), 4),
    )


def _reg_metrics(preds, model_name: str, elapsed: float) -> dict:
    ev = RegressionEvaluator(labelCol=REG_LABEL, predictionCol="prediction")
    return dict(
        model=model_name, train_time_s=round(elapsed, 1),
        rmse=round(ev.setMetricName("rmse").evaluate(preds), 4),
        mae=round(ev.setMetricName("mae").evaluate(preds), 4),
        r2=round(ev.setMetricName("r2").evaluate(preds), 4),
    )


def _print_cls(m):
    print(f"[train] AUC={m['auc']}  Acc={m['accuracy']}  F1={m['f1']}  "
          f"P={m['precision']}  R={m['recall']}  time={m['train_time_s']}s")


def _print_reg(m):
    print(f"[train] RMSE={m['rmse']}  MAE={m['mae']}  R²={m['r2']}  "
          f"time={m['train_time_s']}s")


# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="BDS Project 3 – train ML models")
    parser.add_argument("--hdfs-dataset", default=HDFS_DATASET,
                        help="HDFS glob path to CSV files")
    parser.add_argument("--models",  default=HDFS_MODELS,
                        help="HDFS path to save trained models")
    parser.add_argument("--results", default=HDFS_RESULTS,
                        help="HDFS path to save metrics JSON")
    parser.add_argument("--task",  choices=["all", "classification", "regression"],
                        default="all")
    parser.add_argument("--local", action="store_true",
                        help="Use local[*] Spark master (for testing without cluster)")
    args = parser.parse_args()

    spark = build_spark(local=args.local)
    spark.sparkContext.setLogLevel("WARN")

    # ── Load ─────────────────────────────────────────────────────────────────
    print(f"[train] Loading data from: {args.hdfs_dataset}")
    df = load_and_clean(spark, args.hdfs_dataset)

    total = df.count()
    print(f"[train] Total valid rows: {total:,}")
    df.groupBy(CLASS_LABEL).count().orderBy(CLASS_LABEL).show()

    # ── 70 / 15 / 15 split ───────────────────────────────────────────────────
    print(f"[train] Splitting data: {int(TRAIN_RATIO*100)}% train / "
          f"{int(VAL_RATIO*100)}% val / {int(TEST_RATIO*100)}% test")
    train_df, val_df, test_df = df.randomSplit(
        [TRAIN_RATIO, VAL_RATIO, TEST_RATIO], seed=11
    )
    train_df.cache()
    val_df.cache()

    print(f"[train] Train: {train_df.count():,}  "
          f"Val: {val_df.count():,}  "
          f"Test: {test_df.count():,}")

    # Save test split to HDFS for use in predict.py
    test_df.write.mode("overwrite").parquet(f"{args.models}/test_split")
    print(f"[train] Test split saved → {args.models}/test_split")

    all_metrics = []

    # ── Classification ────────────────────────────────────────────────────────
    if args.task in ("all", "classification"):
        c_train = train_df.filter(F.col("SpO2").isNotNull())
        c_val   = val_df.filter(F.col("SpO2").isNotNull())
        all_metrics.append(train_rf_classifier(c_train, c_val, args.models))
        all_metrics.append(train_gbt_classifier(c_train, c_val, args.models))

    # ── Regression ───────────────────────────────────────────────────────────
    if args.task in ("all", "regression"):
        r_train = train_df.filter(F.col("HR").isNotNull())
        r_val   = val_df.filter(F.col("HR").isNotNull())
        all_metrics.append(train_rf_regressor(r_train, r_val, args.models))
        all_metrics.append(train_gbt_regressor(r_train, r_val, args.models))

    # ── Save metrics summary ──────────────────────────────────────────────────
    summary = json.dumps(all_metrics, indent=2)
    print("\n[train] ── Metrics Summary ──────────────────────────────────")
    print(summary)

    spark.sparkContext.parallelize([summary]) \
         .coalesce(1) \
         .saveAsTextFile(f"{args.results}/training_metrics")

    train_df.unpersist()
    val_df.unpersist()
    spark.stop()
    print("[train] Done.")


if __name__ == "__main__":
    main()
