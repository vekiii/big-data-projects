"""
visualize.py
────────────
Loads prediction CSV / JSON results produced by predict.py and generates
visualisation plots (Matplotlib + Seaborn + Plotly).

Run locally – no Spark needed:
    python visualize.py
    python visualize.py --results ./results --output ./results/plots
"""

import argparse
import glob
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_OK = True
except ImportError:
    PLOTLY_OK = False
    print("[viz] Plotly not installed – skipping HTML plots. pip install plotly")

sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)

PALETTE = {
    "RF_Classifier":  "#2196F3",
    "GBT_Classifier": "#FF5722",
    "RF_Regressor":   "#4CAF50",
    "GBT_Regressor":  "#9C27B0",
}

CLASS_LABEL = "label_classification"
REG_LABEL   = "label_regression"


# ── I/O helpers ───────────────────────────────────────────────────────────────
def load_csv_dir(folder: str) -> pd.DataFrame:
    files = glob.glob(os.path.join(folder, "*.csv")) + \
            glob.glob(os.path.join(folder, "part-*.csv"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


def load_json_metrics(folder: str) -> list:
    parts = sorted(glob.glob(os.path.join(folder, "part-*")))
    if not parts:
        return []
    with open(parts[0]) as fh:
        return json.load(fh)


def save(fig, path: str):
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[viz] → {path}")


# ── 1. Classification bar charts (AUC / F1 / Accuracy) ───────────────────────
def plot_cls_metrics(metrics: list, out: str):
    if not metrics:
        return
    df = pd.DataFrame(metrics)
    df = df[df["n_rows"] > 0]

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    for ax, metric in zip(axes, ["auc", "f1", "accuracy"]):
        if metric not in df.columns:
            continue
        pivot = df.pivot(index="segment", columns="model", values=metric)
        colors = [PALETTE.get(c, "grey") for c in pivot.columns]
        pivot.plot(kind="bar", ax=ax, color=colors, edgecolor="white", width=0.7)
        ax.set_title(metric.upper(), fontweight="bold")
        ax.set_ylim(0, 1.05)
        ax.set_xlabel("")
        ax.tick_params(axis="x", rotation=35)
        ax.legend(title="Model", fontsize=8)

    fig.suptitle("Classification – Performance by Segment", fontsize=14, y=1.01)
    plt.tight_layout()
    save(fig, os.path.join(out, "cls_metrics_by_segment.png"))


# ── 2. Regression bar charts (RMSE / MAE / R²) ───────────────────────────────
def plot_reg_metrics(metrics: list, out: str):
    if not metrics:
        return
    df = pd.DataFrame(metrics)
    df = df[df["n_rows"] > 0]

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    for ax, metric in zip(axes, ["rmse", "mae", "r2"]):
        if metric not in df.columns:
            continue
        pivot = df.pivot(index="segment", columns="model", values=metric)
        colors = [PALETTE.get(c, "grey") for c in pivot.columns]
        pivot.plot(kind="bar", ax=ax, color=colors, edgecolor="white", width=0.7)
        ax.set_title(metric.upper(), fontweight="bold")
        ax.set_xlabel("")
        ax.tick_params(axis="x", rotation=35)
        ax.legend(title="Model", fontsize=8)

    fig.suptitle("Regression (HR) – Performance by Segment", fontsize=14, y=1.01)
    plt.tight_layout()
    save(fig, os.path.join(out, "reg_metrics_by_segment.png"))


# ── 3. Radar chart – RF vs GBT on full test set ───────────────────────────────
def plot_radar(cls_metrics: list, out: str):
    if not cls_metrics:
        return
    df = pd.DataFrame(cls_metrics)
    full = df[df["segment"] == "full_test"]
    if full.empty:
        return

    keys   = ["auc", "accuracy", "f1", "precision", "recall"]
    labels = [k.upper() for k in keys]
    N      = len(labels)
    angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist() + [0]

    fig, ax = plt.subplots(figsize=(7, 7), subplot_kw={"polar": True})
    for _, row in full.iterrows():
        vals = [row[k] for k in keys] + [row[keys[0]]]
        color = PALETTE.get(row["model"], "grey")
        ax.plot(angles, vals, "o-", lw=2, color=color, label=row["model"])
        ax.fill(angles, vals, alpha=0.1, color=color)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, fontsize=11)
    ax.set_ylim(0, 1)
    ax.set_title("RF vs GBT – Full Test Set", fontweight="bold", pad=20)
    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.15))
    save(fig, os.path.join(out, "cls_radar.png"))


# ── 4. Predicted vs Actual scatter (regression) ───────────────────────────────
def plot_pred_vs_actual(df: pd.DataFrame, out: str):
    if df.empty:
        return
    full = df[df["segment"] == "full_test"] if "full_test" in df["segment"].values else df
    full = full.dropna(subset=[REG_LABEL, "prediction"])
    full = full.sample(n=min(5000, len(full)), random_state=42)

    models = full["model"].unique()
    fig, axes = plt.subplots(1, len(models), figsize=(7 * len(models), 6))
    if len(models) == 1:
        axes = [axes]

    for ax, m in zip(axes, models):
        sub = full[full["model"] == m]
        ax.scatter(sub[REG_LABEL], sub["prediction"],
                   alpha=0.3, s=8, color=PALETTE.get(m, "grey"))
        lo = min(sub[REG_LABEL].min(), sub["prediction"].min())
        hi = max(sub[REG_LABEL].max(), sub["prediction"].max())
        ax.plot([lo, hi], [lo, hi], "r--", lw=1.5, label="Ideal")
        ax.set_xlabel("Actual HR (bpm)")
        ax.set_ylabel("Predicted HR (bpm)")
        ax.set_title(f"{m}", fontweight="bold")
        ax.legend()

    plt.tight_layout()
    save(fig, os.path.join(out, "reg_pred_vs_actual.png"))


# ── 5. Residuals histogram ────────────────────────────────────────────────────
def plot_residuals(df: pd.DataFrame, out: str):
    if df.empty:
        return
    df = df[df["segment"] == "full_test"] if "full_test" in df["segment"].values else df
    df = df.dropna(subset=[REG_LABEL, "prediction"]).copy()
    df["residual"] = df["prediction"].astype(float) - df[REG_LABEL].astype(float)
    df = df.sample(n=min(10000, len(df)), random_state=42)

    models = df["model"].unique()[:2]
    fig, axes = plt.subplots(1, len(models), figsize=(14, 5))
    if len(models) == 1:
        axes = [axes]

    for ax, m in zip(axes, models):
        sub = df[df["model"] == m]["residual"]
        sns.histplot(sub, bins=60, ax=ax, color=PALETTE.get(m, "grey"), kde=True)
        ax.axvline(0, color="red", linestyle="--")
        ax.set_title(f"{m} – HR Residuals", fontweight="bold")
        ax.set_xlabel("Residual (bpm)")

    plt.tight_layout()
    save(fig, os.path.join(out, "reg_residuals.png"))


# ── 6. Time-series HR trace (one case) ────────────────────────────────────────
def plot_timeseries(df: pd.DataFrame, out: str):
    if df.empty or "RelativeTimeMilliseconds" not in df.columns:
        return
    df = df[df["segment"] == "full_test"] if "full_test" in df["segment"].values else df
    df = df.dropna(subset=["RelativeTimeMilliseconds", REG_LABEL, "prediction"])

    # Pick the case with the most rows
    case = df["case_name"].value_counts().index[0]
    sub  = df[df["case_name"] == case].sort_values("RelativeTimeMilliseconds")

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(sub["RelativeTimeMilliseconds"].astype(float) / 60000,
            sub[REG_LABEL].astype(float),
            label="Actual HR", color="steelblue", lw=1.5)

    for m in sub["model"].unique():
        ms = sub[sub["model"] == m]
        ax.plot(ms["RelativeTimeMilliseconds"].astype(float) / 60000,
                ms["prediction"].astype(float),
                label=f"Predicted ({m})", lw=1, alpha=0.75, linestyle="--",
                color=PALETTE.get(m, "orange"))

    ax.set_xlabel("Time (minutes)")
    ax.set_ylabel("Heart Rate (bpm)")
    ax.set_title(f"{case} – HR over Procedure Time", fontweight="bold")
    ax.legend()
    plt.tight_layout()
    save(fig, os.path.join(out, "reg_timeseries.png"))


# ── 7. Predicted probability histogram (classification) ───────────────────────
def plot_prob_hist(df: pd.DataFrame, out: str):
    if df.empty or "probability" not in df.columns:
        return
    df = df[df["segment"] == "full_test"] if "full_test" in df["segment"].values else df
    df = df.sample(n=min(20000, len(df)), random_state=42).copy()

    def p1(s):
        try:
            return float(str(s).strip("[]").split(",")[1])
        except Exception:
            return np.nan

    df["prob_pos"] = df["probability"].apply(p1)
    df = df.dropna(subset=["prob_pos", CLASS_LABEL])

    models = df["model"].unique()[:2]
    fig, axes = plt.subplots(1, len(models), figsize=(14, 5))
    if len(models) == 1:
        axes = [axes]

    for ax, m in zip(axes, models):
        sub = df[df["model"] == m]
        for lbl, grp in sub.groupby(CLASS_LABEL):
            sns.histplot(grp["prob_pos"], bins=50, ax=ax,
                         label=f"True={int(lbl)}", alpha=0.6, kde=False)
        ax.set_title(f"{m} – P(hypoxemia)", fontweight="bold")
        ax.set_xlabel("Predicted Probability")
        ax.legend()

    plt.tight_layout()
    save(fig, os.path.join(out, "cls_prob_hist.png"))


# ── 8. Plotly interactive HTML plots ──────────────────────────────────────────
def plotly_plots(cls_metrics: list, reg_metrics: list, out: str):
    if not PLOTLY_OK:
        return

    if cls_metrics:
        df = pd.DataFrame(cls_metrics)
        df = df[df["n_rows"] > 0]
        fig = px.bar(df, x="segment", y="auc", color="model", barmode="group",
                     title="Classification – AUC by Segment",
                     color_discrete_map=PALETTE,
                     hover_data=["f1", "accuracy", "n_rows"])
        fig.update_layout(xaxis_tickangle=-35)
        p = os.path.join(out, "cls_auc_interactive.html")
        fig.write_html(p)
        print(f"[viz] → {p}")

    if reg_metrics:
        df = pd.DataFrame(reg_metrics)
        df = df[df["n_rows"] > 0]
        fig = px.scatter(df, x="rmse", y="mae", color="model", symbol="segment",
                         size="n_rows", hover_name="segment",
                         title="Regression – RMSE vs MAE",
                         color_discrete_map=PALETTE)
        p = os.path.join(out, "reg_rmse_mae_interactive.html")
        fig.write_html(p)
        print(f"[viz] → {p}")

    if cls_metrics and reg_metrics:
        fig = make_subplots(rows=1, cols=2,
                            subplot_titles=("Classification AUC", "Regression R²"))
        for row in pd.DataFrame(cls_metrics).query("segment=='full_test'").itertuples():
            fig.add_trace(go.Bar(name=row.model, x=[row.model], y=[row.auc],
                                 marker_color=PALETTE.get(row.model, "grey"),
                                 showlegend=True), row=1, col=1)
        for row in pd.DataFrame(reg_metrics).query("segment=='full_test'").itertuples():
            fig.add_trace(go.Bar(name=row.model, x=[row.model], y=[row.r2],
                                 marker_color=PALETTE.get(row.model, "grey"),
                                 showlegend=False), row=1, col=2)
        fig.update_layout(title="Model Dashboard – Full Test Set", bargap=0.25)
        p = os.path.join(out, "dashboard.html")
        fig.write_html(p)
        print(f"[viz] → {p}")


# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="BDS Project 3 – visualisation")
    parser.add_argument("--results", default="./results",
                        help="Root folder containing cls_* and reg_* sub-folders")
    parser.add_argument("--output",  default="./results/plots",
                        help="Folder to write plot images into")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    cls_metrics = load_json_metrics(os.path.join(args.results, "cls_metrics"))
    reg_metrics = load_json_metrics(os.path.join(args.results, "reg_metrics"))
    cls_df      = load_csv_dir(os.path.join(args.results, "cls_predictions"))
    reg_df      = load_csv_dir(os.path.join(args.results, "reg_predictions"))

    for col in [CLASS_LABEL, "prediction"]:
        if col in cls_df.columns:
            cls_df[col] = pd.to_numeric(cls_df[col], errors="coerce")
    for col in [REG_LABEL, "prediction", "RelativeTimeMilliseconds"]:
        if col in reg_df.columns:
            reg_df[col] = pd.to_numeric(reg_df[col], errors="coerce")

    print("[viz] Generating static plots …")
    plot_cls_metrics(cls_metrics, args.output)
    plot_reg_metrics(reg_metrics, args.output)
    plot_radar(cls_metrics, args.output)
    plot_pred_vs_actual(reg_df, args.output)
    plot_residuals(reg_df, args.output)
    plot_timeseries(reg_df, args.output)
    plot_prob_hist(cls_df, args.output)

    print("[viz] Generating interactive HTML plots …")
    plotly_plots(cls_metrics, reg_metrics, args.output)

    print(f"\n[viz] All plots saved to {args.output}")


if __name__ == "__main__":
    main()
