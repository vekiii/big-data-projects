"""
prepare_data.py  –  LOCAL script (no Spark required)
─────────────────────────────────────────────────────
Mirrors the Project 1 workflow exactly:

  Downloaded structure (4.14 GB):
    <source_dir>/
      case01/
        fulldata/
          uq_vsd_case01_fulldata_01.csv
          uq_vsd_case01_fulldata_02.csv
          ...
        waveformplots/
        uq_vsd_case01_caseplot.png
        uq_vsd_case01_trenddata.csv
      case02/ ...
      ...
      case32/

  Output (dataset_ready_for_hdfs/):
    uq_vsd_case01_fulldata_01.csv
    uq_vsd_case01_fulldata_02.csv
    ...   ← all fulldata CSVs flattened into one folder,
              sampled to ~TARGET_GB

After this script finishes, upload to HDFS with:
    docker exec -it namenode hdfs dfs -mkdir -p /user/root
    docker exec -it namenode hdfs dfs -put /data/incoming /user/root/dataset

Usage:
    python prepare_data.py
    python prepare_data.py --source ./downloaded_data --target-gb 1.5
"""

import argparse
import os
import random
import shutil
import sys

# ── Defaults ─────────────────────────────────────────────────────────────────
DEFAULT_SOURCE   = "./downloaded_data"            # root of the unzipped dataset
DEFAULT_OUTPUT   = "./dataset_ready_for_hdfs"     # flat output folder (mounted into namenode)
TARGET_GB        = 1.1                           # desired output size in GB
BYTES_PER_GB     = 1024 ** 3


def collect_fulldata_csvs(source_dir: str) -> list[tuple[str, str]]:
    """
    Walk the source directory and collect every *fulldata*.csv file.
    Returns a list of (abs_src_path, dest_filename) tuples.

    Handles both layouts:
      case01/fulldata/uq_vsd_case01_fulldata_01.csv   ← standard download
      case01/uq_vsd_case01_fulldata_01.csv            ← some mirrors are flat
    """
    found = []
    for root, dirs, files in os.walk(source_dir):
        for fname in files:
            if "fulldata" in fname.lower() and fname.endswith(".csv"):
                src = os.path.join(root, fname)
                found.append((src, fname))

    if not found:
        print(f"[prepare_data] ERROR: No *fulldata*.csv files found under '{source_dir}'")
        print("  Make sure --source points to the folder that contains case01/, case02/, ...")
        sys.exit(1)

    found.sort(key=lambda x: x[1])          # deterministic order (case01 → case32)
    return found


def sample_files(files: list[tuple[str, str]], target_gb: float) -> list[tuple[str, str]]:
    """
    Randomly sample from `files` until we reach approximately `target_gb` GB.
    Uses actual file sizes so the estimate is accurate.
    """
    total_bytes = sum(os.path.getsize(src) for src, _ in files)
    total_gb    = total_bytes / BYTES_PER_GB
    print(f"[prepare_data] Total fulldata size : {total_gb:.2f} GB  ({len(files)} files)")

    if total_gb <= target_gb:
        print(f"[prepare_data] Dataset already ≤ {target_gb} GB – copying all files.")
        return files

    # Shuffle with a fixed seed for reproducibility, then take files until target is reached
    rng = random.Random(42)
    shuffled = list(files)
    rng.shuffle(shuffled)

    selected = []
    accumulated = 0
    target_bytes = int(target_gb * BYTES_PER_GB)

    for src, fname in shuffled:
        size = os.path.getsize(src)
        if accumulated + size > target_bytes:
            continue
        selected.append((src, fname))
        accumulated += size
        if accumulated >= target_bytes:
            break

    selected_gb = accumulated / BYTES_PER_GB
    print(f"[prepare_data] Selected {len(selected)} files ≈ {selected_gb:.2f} GB "
          f"(target {target_gb} GB)")
    return selected


def copy_to_output(files: list[tuple[str, str]], output_dir: str) -> None:
    """Copy sampled files into the flat output folder."""
    os.makedirs(output_dir, exist_ok=True)

    existing = set(os.listdir(output_dir))
    copied = skipped = 0

    for i, (src, fname) in enumerate(files, 1):
        dest = os.path.join(output_dir, fname)
        if fname in existing:
            skipped += 1
        else:
            shutil.copy2(src, dest)
            copied += 1

        if i % 50 == 0 or i == len(files):
            print(f"  [{i}/{len(files)}] copied={copied}  skipped={skipped}", end="\r")

    print()
    print(f"[prepare_data] Done – {copied} files copied, {skipped} already existed.")
    print(f"[prepare_data] Output folder: {os.path.abspath(output_dir)}")


def verify_output(output_dir: str) -> None:
    """Print a brief summary of what landed in the output folder."""
    csvs = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
    total_bytes = sum(os.path.getsize(os.path.join(output_dir, f)) for f in csvs)
    cases = sorted({f.split("_fulldata_")[0].split("case")[1][:2] for f in csvs
                    if "fulldata" in f and "case" in f})
    print(f"\n[verify] Files  : {len(csvs)}")
    print(f"[verify] Size   : {total_bytes / BYTES_PER_GB:.3f} GB")
    print(f"[verify] Cases  : {len(cases)}  ({cases[0]} → {cases[-1]})")
    print(f"\n[next steps]")
    print("  1. docker compose up -d")
    print("  2. docker exec -it namenode hdfs dfs -mkdir -p /user/root")
    print("  3. docker exec -it namenode hdfs dfs -put /data/incoming /user/root/dataset")
    print("  4. docker exec -it spark-master /spark/bin/spark-submit "
          "--driver-memory 2G --executor-memory 4G /app/train_model.py")


# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Flatten + sample UQ Vital Signs fulldata CSVs for HDFS upload"
    )
    parser.add_argument(
        "--source", default=DEFAULT_SOURCE,
        help=f"Root folder of the downloaded dataset (default: {DEFAULT_SOURCE})"
    )
    parser.add_argument(
        "--output", default=DEFAULT_OUTPUT,
        help=f"Flat output folder to mount into the namenode (default: {DEFAULT_OUTPUT})"
    )
    parser.add_argument(
        "--target-gb", type=float, default=TARGET_GB,
        help=f"Approximate target size in GB (default: {TARGET_GB})"
    )
    parser.add_argument(
        "--no-sample", action="store_true",
        help="Copy ALL fulldata CSVs without sampling (ignores --target-gb)"
    )
    args = parser.parse_args()

    print(f"[prepare_data] Source : {os.path.abspath(args.source)}")
    print(f"[prepare_data] Output : {os.path.abspath(args.output)}")
    print(f"[prepare_data] Target : {'ALL (no sampling)' if args.no_sample else f'{args.target_gb} GB'}")
    print()

    if not os.path.isdir(args.source):
        print(f"[prepare_data] ERROR: source directory does not exist: {args.source}")
        sys.exit(1)

    all_files = collect_fulldata_csvs(args.source)
    print(f"[prepare_data] Found {len(all_files)} fulldata CSV files.")

    if args.no_sample:
        selected = all_files
    else:
        selected = sample_files(all_files, args.target_gb)

    copy_to_output(selected, args.output)
    verify_output(args.output)


if __name__ == "__main__":
    main()
