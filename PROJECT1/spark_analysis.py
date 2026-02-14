from pyspark.sql import SparkSession


def main():
    # 1. Inicialization of Spark session
    spark = SparkSession.builder \
        .appName("Project1_Analysis") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    print("--- Spark session successfully created ---")

    # 2. File path for HDFS

    hdfs_path = "hdfs://namenode:8020/user/root/dataset/*.csv"

    print(f"--- Reading data from: {hdfs_path} ---")

    try:
        # 3. Loading CSV files
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(hdfs_path)


        print("--- First 5 rows: ---")
        df.show(5)

        print(f"--- Total number of samples: {df.count()} ---")

        print("--- Data scheme: ---")
        df.printSchema()

    except Exception as e:
        print(f"Error while loading the data: {e}")

    finally:
        # 5. Stoping the session
        spark.stop()
        print("--- Spark session closed ---")


if __name__ == "__main__":
    main()