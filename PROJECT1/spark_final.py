import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    # 1. Defining app atributes
    parser = argparse.ArgumentParser(description='BDS Project 1 - Spark Batch Processing')
    parser.add_argument('--task', type=int, required=True, help='1 for filtering, 2 for stats')
    parser.add_argument('--col', type=str, help='Filter (task 1) or stat (task 2) atribute')
    parser.add_argument('--val', type=float, help='Granična vrednost za filtriranje (task 1)')
    parser.add_argument('--group_by', type=str, default='case_name', help='Grouping atribute (task 2)')
    parser.add_argument('--sort_by', type=str, default='RelativeTimeMilliseconds', help='Sorting column (T1)')

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("BDS_Final_Project") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # 2. Loading data from HDFS-a
    # Adding 'case_name' column from file name
    hdfs_path = "hdfs://namenode:8020/user/root/dataset/*.csv"
    df = spark.read.csv(hdfs_path, header=True, inferSchema=True) \
        .withColumn("case_name", F.input_file_name())

    # Cleaning 'case_name' so that only the name os the file is left, and not the whole path
    df = df.withColumn("case_name", F.element_at(F.split(F.col("case_name"), "/"), -1))

    if args.task == 1:
        print(f"--- TASK 1: Filtering {args.col} > {args.val}, Sorted by: {args.sort_by} ---")
        result = df.filter(F.col(args.col) > args.val).orderBy(args.sort_by)
        result.select("case_name", "Time", args.col).show(20)
        print(f"Total number of samples: {result.count()}")

    elif args.task == 2:
        print(f"--- TASK 2: Stats for {args.col} grouped by {args.group_by} ---")
        stats = df.groupBy(args.group_by).agg(
            F.min(args.col).alias("Min"),
            F.max(args.col).alias("Max"),
            F.avg(args.col).alias("Mean"),
            F.stddev(args.col).alias("Std_Dev"),
            F.variance(args.col).alias("Variance"),
            F.percentile_approx(args.col, 0.5).alias("Median"),
            F.count("*").alias("Samples count")
        )
        stats.show()

    spark.stop()


if __name__ == "__main__":
    main()