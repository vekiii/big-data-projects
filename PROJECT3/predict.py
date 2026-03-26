"""
predict.py
──────────
Loads saved RF + GBT models from HDFS and runs inference on the held-out
test split across multiple parameter configurations:

  • Full test set
  • Temporal segments  : early / mid / late stage of the procedure
  • Alarm state        : active alarms vs no alarms

Results are written to HDFS as CSV + JSON.

Run inside the cluster:
    docker exec -it spark-master /spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 2G --executor-memory 4G \
        /app/predict.py
"""

import argparse
import json
import time

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

HDFS_NAMENODE = "hdfs://namenode:8020"
HDFS_MODELS   = f"{HDFS_NAMENODE}/user/root/models"
HDFS_RESULTS  = f"{HDFS_NAMENODE}/user/root/results"

CLASS_LABEL = "label_classification"
REG_LABEL   = "label_regression"

TEMPORAL_SEGMENTS = {
    "early": (0.00, 0.33),
    "mid":   (0.33, 0.67),
    "late":  (0.67, 1.00),
}


def build_spark(local: bool) -> SparkSession:
    master = "local[*]" if local else "spark://spark-master:7077"
    return (
        SparkSession.builder
        .appName("BDS_Project3_Predict")
        .master(master)
        .config("spark.sql.shuffle.partitions", "40")
        .getOrCreate()
    )


def add_time_norm(df):
    """Normalise RelativeTimeMilliseconds to [0, 1] per case_name."""
    win = Window.partitionBy("case_name")
    return df.withColumn(
        "time_norm",
        (F.col("RelativeTimeMilliseconds") - F.min("RelativeTimeMilliseconds").over(win)) /
        (F.max("RelativeTimeMilliseconds").over(win) -
         F.min("RelativeTimeMilliseconds").over(win) + 1e-6)
    )


# ── Evaluation helpers ────────────────────────────────────────────────────────
def eval_cls(preds, segment: str, model_name: str) -> dict:
    n = preds.count()
    if n == 0:
        return {}
    bi = BinaryClassificationEvaluator(labelCol=CLASS_LABEL, metricName="areaUnderROC")
    mc = MulticlassClassificationEvaluator(labelCol=CLASS_LABEL, predictionCol="prediction")
    return dict(
        model=model_name, segment=segment, n_rows=n,
        auc=round(bi.evaluate(preds), 4),
        accuracy=round(mc.setMetricName("accuracy").evaluate(preds), 4),
        f1=round(mc.setMetricName("f1").evaluate(preds), 4),
        precision=round(mc.setMetricName("weightedPrecision").evaluate(preds), 4),
        recall=round(mc.setMetricName("weightedRecall").evaluate(preds), 4),
    )


def eval_reg(preds, segment: str, model_name: str) -> dict:
    n = preds.count()
    if n == 0:
        return {}
    ev = RegressionEvaluator(labelCol=REG_LABEL, predictionCol="prediction")
    return dict(
        model=model_name, segment=segment, n_rows=n,
        rmse=round(ev.setMetricName("rmse").evaluate(preds), 4),
        mae=round(ev.setMetricName("mae").evaluate(preds), 4),
        r2=round(ev.setMetricName("r2").evaluate(preds), 4),
    )


# ── Experiment runners ────────────────────────────────────────────────────────
def run_classification(test_df, rf, gbt, results_path: str, spark):
    print("\n[predict] ── Classification Experiments ─────────────────────")
    df = add_time_norm(test_df)

    configs = {"full_test": df}
    for seg, (lo, hi) in TEMPORAL_SEGMENTS.items():
        configs[f"temporal_{seg}"] = df.filter(
            (F.col("time_norm") >= lo) & (F.col("time_norm") < hi)
        )
    configs["alarms_active"] = df.filter(F.col("Num_Patient_Alarms") > 0)
    configs["no_alarms"]     = df.filter(F.col("Num_Patient_Alarms") == 0)

    all_metrics, out_dfs = [], []

    for seg_name, seg_df in configs.items():
        seg_df = seg_df.cache()
        for model_name, model in [("RF_Classifier", rf), ("GBT_Classifier", gbt)]:
            preds = model.transform(seg_df)
            m = eval_cls(preds, seg_name, model_name)
            if m:
                all_metrics.append(m)
                print(f"  [{model_name}][{seg_name}]  AUC={m['auc']}  F1={m['f1']}  n={m['n_rows']:,}")
            out_dfs.append(
                preds.select("case_name", "RelativeTimeMilliseconds",
                             CLASS_LABEL, "prediction", "probability")
                     .withColumn("model",   F.lit(model_name))
                     .withColumn("segment", F.lit(seg_name))
            )
        seg_df.unpersist()

    from functools import reduce
    combined = reduce(lambda a, b: a.union(b), out_dfs)
    (combined.sample(fraction=0.1, seed=42).coalesce(4)
     .write.mode("overwrite").option("header", "true")
     .csv(f"{results_path}/cls_predictions"))

    spark.sparkContext.parallelize([json.dumps(all_metrics, indent=2)]) \
         .coalesce(1).saveAsTextFile(f"{results_path}/cls_metrics")
    print(f"[predict] Saved → {results_path}/cls_*")
    return all_metrics


def run_regression(test_df, rf, gbt, results_path: str, spark):
    print("\n[predict] ── Regression Experiments ────────────────────────")
    df = add_time_norm(test_df).filter(F.col("HR").isNotNull())

    configs = {"full_test": df}
    for seg, (lo, hi) in TEMPORAL_SEGMENTS.items():
        configs[f"temporal_{seg}"] = df.filter(
            (F.col("time_norm") >= lo) & (F.col("time_norm") < hi)
        )
    configs["alarms_active"] = df.filter(F.col("Num_Patient_Alarms") > 0)
    configs["no_alarms"]     = df.filter(F.col("Num_Patient_Alarms") == 0)

    all_metrics, out_dfs = [], []

    for seg_name, seg_df in configs.items():
        seg_df = seg_df.cache()
        for model_name, model in [("RF_Regressor", rf), ("GBT_Regressor", gbt)]:
            preds = model.transform(seg_df)
            m = eval_reg(preds, seg_name, model_name)
            if m:
                all_metrics.append(m)
                print(f"  [{model_name}][{seg_name}]  RMSE={m['rmse']}  MAE={m['mae']}  "
                      f"R²={m['r2']}  n={m['n_rows']:,}")
            out_dfs.append(
                preds.select("case_name", "RelativeTimeMilliseconds",
                             REG_LABEL, "prediction")
                     .withColumn("model",   F.lit(model_name))
                     .withColumn("segment", F.lit(seg_name))
            )
        seg_df.unpersist()

    from functools import reduce
    combined = reduce(lambda a, b: a.union(b), out_dfs)
    (combined.sample(fraction=0.1, seed=42).coalesce(4)
     .write.mode("overwrite").option("header", "true")
     .csv(f"{results_path}/reg_predictions"))

    spark.sparkContext.parallelize([json.dumps(all_metrics, indent=2)]) \
         .coalesce(1).saveAsTextFile(f"{results_path}/reg_metrics")
    print(f"[predict] Saved → {results_path}/reg_*")
    return all_metrics


# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="BDS Project 3 – inference")
    parser.add_argument("--models",  default=HDFS_MODELS)
    parser.add_argument("--results", default=HDFS_RESULTS)
    parser.add_argument("--local",   action="store_true")
    args = parser.parse_args()

    spark = build_spark(local=args.local)
    spark.sparkContext.setLogLevel("WARN")

    print(f"[predict] Loading test split from {args.models}/test_split …")
    test_df = spark.read.parquet(f"{args.models}/test_split").cache()
    print(f"[predict] Test rows: {test_df.count():,}")

    print("[predict] Loading models …")
    rf_cls  = PipelineModel.load(f"{args.models}/rf_classifier")
    gbt_cls = PipelineModel.load(f"{args.models}/gbt_classifier")
    rf_reg  = PipelineModel.load(f"{args.models}/rf_regressor")
    gbt_reg = PipelineModel.load(f"{args.models}/gbt_regressor")

    t0 = time.time()
    run_classification(test_df, rf_cls,  gbt_cls, args.results, spark)
    run_regression(    test_df, rf_reg,  gbt_reg, args.results, spark)
    print(f"\n[predict] Total inference time: {time.time() - t0:.1f}s")

    test_df.unpersist()
    spark.stop()
    print("[predict] Done.")


if __name__ == "__main__":
    main()
